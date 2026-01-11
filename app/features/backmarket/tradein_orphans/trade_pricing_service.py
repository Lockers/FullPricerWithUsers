from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError

from app.core.errors import NotFoundError
from app.features.backmarket.pricing.trade_pricing_service import (
    build_sell_anchor_index_for_user,
    compute_trade_pricing_for_doc,
    get_trade_pricing_settings_for_user,
)
from app.features.backmarket.pricing.trade_pricing_models import TradePricingGroupSettingsIn
from app.features.backmarket.tradein_orphans.repo import ORPHAN_COL, TradeinOrphansRepo


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def recompute_trade_pricing_for_orphan(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    orphan_id: str,
) -> Dict[str, Any]:
    repo = TradeinOrphansRepo(db)
    doc = await repo.get_one(user_id=user_id, orphan_id=orphan_id)
    if not doc:
        raise NotFoundError(
            code="orphan_not_found",
            message="Trade-in orphan not found",
            details={"user_id": user_id, "orphan_id": orphan_id},
        )

    user_settings = await get_trade_pricing_settings_for_user(db, user_id)
    sell_anchor_index = await build_sell_anchor_index_for_user(db, user_id)

    computed = compute_trade_pricing_for_doc(
        group=doc,
        user_settings=user_settings,
        sell_anchor_index=sell_anchor_index,
    )

    now = _now_utc()
    await repo.persist_trade_pricing_computed(user_id=user_id, orphan_id=orphan_id, computed=computed, updated_at=now)
    return computed


async def recompute_trade_pricing_for_orphans_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    orphan_ids: Optional[List[str]] = None,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    limit: Optional[int] = None,
    only_enabled: Optional[bool] = None,
    concurrency: int = 25,
) -> Dict[str, Any]:
    """Recompute trade_pricing.computed for orphan docs."""
    q: Dict[str, Any] = {"user_id": user_id}
    if only_enabled is True:
        q["enabled"] = True
    elif only_enabled is False:
        q["enabled"] = False

    if brand:
        q["brand"] = str(brand).strip().upper()
    if model:
        q["model"] = " ".join(str(model).strip().upper().split())

    if orphan_ids:
        oids: List[ObjectId] = []
        for x in orphan_ids:
            try:
                oids.append(ObjectId(str(x)))
            except (InvalidId, TypeError):
                continue
        if oids:
            q["_id"] = {"$in": oids}

    cur = db[ORPHAN_COL].find(q)
    if limit is not None:
        cur = cur.limit(int(limit))

    docs = await cur.to_list(length=None)

    user_settings = await get_trade_pricing_settings_for_user(db, user_id)
    sell_anchor_index = await build_sell_anchor_index_for_user(db, user_id)

    repo = TradeinOrphansRepo(db)

    sem = asyncio.Semaphore(max(1, int(concurrency)))
    ok = 0
    err = 0

    async def _worker(doc: Dict[str, Any]) -> None:
        nonlocal ok, err
        oid = doc.get("_id")
        if not oid:
            err += 1
            return
        async with sem:
            try:
                computed = compute_trade_pricing_for_doc(
                    group=doc,
                    user_settings=user_settings,
                    sell_anchor_index=sell_anchor_index,
                )
                await repo.persist_trade_pricing_computed(
                    user_id=user_id,
                    orphan_id=str(oid),
                    computed=computed,
                    updated_at=_now_utc(),
                )
                ok += 1
            except PyMongoError:
                err += 1

    await asyncio.gather(*[_worker(d) for d in docs])

    return {
        "user_id": user_id,
        "count": len(docs),
        "ok": ok,
        "errors": err,
        "collection": ORPHAN_COL,
        "updated_at": _now_utc().isoformat(),
    }


async def update_trade_pricing_settings_for_orphan(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    orphan_id: str,
    settings_in: TradePricingGroupSettingsIn,
) -> None:
    repo = TradeinOrphansRepo(db)

    patch = settings_in.model_dump(exclude_unset=True)
    if not patch:
        return

    settings_patch: Dict[str, Any] = {}
    if "required_profit" in patch:
        settings_patch["required_profit"] = None if patch["required_profit"] is None else float(patch["required_profit"])
    if "required_margin" in patch:
        settings_patch["required_margin"] = None if patch["required_margin"] is None else float(patch["required_margin"])

    now = _now_utc()
    ok = await repo.update_trade_pricing_settings(
        user_id=user_id,
        orphan_id=orphan_id,
        settings_patch=settings_patch,
        updated_at=now,
    )
    if not ok:
        raise NotFoundError(
            code="orphan_not_found",
            message="Trade-in orphan not found",
            details={"user_id": user_id, "orphan_id": orphan_id},
        )

