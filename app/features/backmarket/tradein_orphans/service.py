from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError

from app.core.errors import BadRequestError, NotFoundError
from app.features.backmarket.pricing_groups.sku import (
    make_group_key,
    parse_trade_sku_with_reason,
    target_sell_condition,
)
from app.features.backmarket.tradein_orphans.repo import ORPHAN_COL, TradeinOrphansRepo

logger = logging.getLogger(__name__)

ISSUES_COL = "pricing_bad_tradein_skus"
BM_TRADEIN_LISTINGS_COL = "bm_tradein_listings"

HARD_FAILURE_REASON_CODES: Set[str] = {
    "bad_request",
    "unauthorized",
    "not_found",
    "unprocessable_entity",
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _norm_str(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _derive_fields_from_trade_sku(
    *,
    trade_sku: Optional[str],
    tradein_grade_code: Optional[str],
    fallback_reason_details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    parsed, reason_code, reason_details = parse_trade_sku_with_reason(trade_sku)

    out: Dict[str, Any] = {
        "trade_sku": _norm_str(trade_sku),
        "trade_sku_parse_ok": bool(parsed is not None),
        "trade_sku_parse_reason_code": reason_code,
        "trade_sku_parse_reason_details": reason_details or {},
        "brand": None,
        "model": None,
        "storage_gb": None,
        "trade_sku_condition": None,
        "target_sell_condition": None,
        "group_key": None,
    }

    if parsed is not None:
        brand = parsed.brand
        model = parsed.model
        storage_gb = parsed.storage_gb
        cond = parsed.condition

        out.update(
            {
                "brand": brand,
                "model": model,
                "storage_gb": storage_gb,
                "trade_sku_condition": cond,
                "target_sell_condition": target_sell_condition(
                    tradein_grade_code=tradein_grade_code,
                    trade_sku_condition=cond,
                ),
                "group_key": make_group_key(
                    brand=brand,
                    model=model,
                    storage_gb=storage_gb,
                    tradein_grade_code=tradein_grade_code,
                ),
            }
        )
        return out

    # Parse failed; try to salvage what we can from reason_details.
    merged_details: Dict[str, Any] = {}
    if fallback_reason_details:
        merged_details.update(fallback_reason_details)
    if reason_details:
        merged_details.update(reason_details)

    brand = _norm_str(merged_details.get("brand"))
    model = _norm_str(merged_details.get("model"))
    storage_gb = merged_details.get("storage_gb")

    if isinstance(storage_gb, str):
        try:
            storage_gb = int(storage_gb)
        except (TypeError, ValueError):
            storage_gb = None
    if isinstance(storage_gb, float):
        storage_gb = int(storage_gb)
    if isinstance(storage_gb, int) and storage_gb <= 0:
        storage_gb = None

    out["brand"] = brand
    out["model"] = model
    out["storage_gb"] = storage_gb

    # We can still derive a target sell condition from grade code only.
    out["target_sell_condition"] = target_sell_condition(
        tradein_grade_code=tradein_grade_code,
        trade_sku_condition=None,
    )

    if brand and model and storage_gb is not None:
        out["group_key"] = make_group_key(
            brand=brand,
            model=model,
            storage_gb=storage_gb,
            tradein_grade_code=tradein_grade_code,
        )

    return out


async def sync_tradein_orphans_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    limit: Optional[int] = None,
    prune: bool = False,
) -> Dict[str, Any]:
    """Materialize trade-in orphans from `pricing_bad_tradein_skus`.

    This does NOT touch pricing_groups.
    """
    now = _now_utc()

    # Load existing manual overrides once (tradein_id -> manual_trade_sku).
    existing_manual: Dict[str, Optional[str]] = {}
    try:
        cur = db[ORPHAN_COL].find(
            {"user_id": user_id},
            {"_id": 0, "tradein_id": 1, "manual_trade_sku": 1},
        )
        async for d in cur:
            tid = _norm_str(d.get("tradein_id"))
            if tid:
                existing_manual[tid] = _norm_str(d.get("manual_trade_sku"))
    except PyMongoError:
        logger.exception("[tradein_orphans_sync] failed loading existing manual overrides user_id=%s", user_id)

    # 1) Read trade-in issues (exclude hard failures written by update endpoints).
    q: Dict[str, Any] = {
        "user_id": user_id,
        "reason_code": {"$nin": sorted(HARD_FAILURE_REASON_CODES)},
    }

    cursor = db[ISSUES_COL].find(q).sort("last_seen_at", -1)
    if limit is not None:
        cursor = cursor.limit(int(limit))

    issues: List[Dict[str, Any]] = await cursor.to_list(length=None)

    tradein_ids: List[str] = []
    for it in issues:
        tid = _norm_str(it.get("tradein_id"))
        if tid:
            tradein_ids.append(tid)

    # 2) Load BM trade-in listing docs for enrichment (markets/prices/sku).
    bm_map: Dict[str, Dict[str, Any]] = {}
    if tradein_ids:
        bm_cursor = db[BM_TRADEIN_LISTINGS_COL].find(
            {"user_id": user_id, "tradein_id": {"$in": tradein_ids}},
            {
                "_id": 0,
                "tradein_id": 1,
                "product_id": 1,
                "sku": 1,
                "aesthetic_grade_code": 1,
                "markets": 1,
                "prices": 1,
            },
        )
        async for d in bm_cursor:
            tid = _norm_str(d.get("tradein_id"))
            if tid:
                bm_map[tid] = d

    upserted = 0
    matched = 0

    for it in issues:
        tradein_id = _norm_str(it.get("tradein_id"))
        if not tradein_id:
            continue

        bm_doc = bm_map.get(tradein_id) or {}

        raw_sku = _norm_str(it.get("sku")) or _norm_str(bm_doc.get("sku"))
        grade_code = _norm_str(it.get("aesthetic_grade_code")) or _norm_str(bm_doc.get("aesthetic_grade_code"))

        manual_trade_sku = existing_manual.get(tradein_id)
        effective_trade_sku = manual_trade_sku or raw_sku
        trade_sku_source = "manual" if manual_trade_sku else "auto"

        derived = _derive_fields_from_trade_sku(
            trade_sku=effective_trade_sku,
            tradein_grade_code=grade_code,
            fallback_reason_details=(it.get("reason_details") or {}),
        )
        derived["trade_sku_source"] = trade_sku_source

        set_doc: Dict[str, Any] = {
            "schema_version": 1,
            "user_id": user_id,
            "tradein_id": tradein_id,
            "product_id": _norm_str(it.get("product_id")) or _norm_str(bm_doc.get("product_id")),
            "sku": raw_sku,
            "aesthetic_grade_code": grade_code,
            "tradein_grade_code": grade_code,
            "reason_code": _norm_str(it.get("reason_code")),
            "reason_details": it.get("reason_details") or {},
            "first_seen_at": it.get("first_seen_at"),
            "last_seen_at": it.get("last_seen_at"),
            "markets": bm_doc.get("markets") or [],
            "prices": bm_doc.get("prices") or {},
            "updated_at": now,
            # Minimal subset of pricing_groups.tradein_listing for consistency.
            "tradein_listing.tradein_id": tradein_id,
            "tradein_listing.sku": raw_sku,
            "tradein_listing.aesthetic_grade_code": grade_code,
        }
        set_doc.update(derived)

        res = await db[ORPHAN_COL].update_one(
            {"user_id": user_id, "tradein_id": tradein_id},
            {
                "$set": set_doc,
                "$setOnInsert": {
                    "created_at": now,
                    # Orphans are OFF by default until you explicitly enable them.
                    "enabled": False,
                },
            },
            upsert=True,
        )

        if res.upserted_id is not None:
            upserted += 1
        elif res.matched_count:
            matched += 1

    pruned = 0
    if prune:
        keep_ids = {str(tid) for tid in tradein_ids if tid}
        if keep_ids:
            del_res = await db[ORPHAN_COL].delete_many({"user_id": user_id, "tradein_id": {"$nin": sorted(keep_ids)}})
            pruned = int(del_res.deleted_count or 0)

    return {
        "ok": True,
        "user_id": user_id,
        "issues_found": len(issues),
        "orphans_upserted": upserted,
        "orphans_matched": matched,
        "orphans_pruned": pruned,
        "collection": ORPHAN_COL,
        "computed_at": now,
    }


async def list_orphans_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    limit: int = 5000,
    include_disabled: bool = True,
    reason_code: Optional[str] = None,
    enabled: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    repo = TradeinOrphansRepo(db)

    reason_codes = [reason_code] if reason_code else None
    docs = await repo.list_for_user(
        user_id=user_id,
        limit=limit,
        include_disabled=include_disabled,
        reason_codes=reason_codes,
    )

    if enabled is None:
        return docs

    return [d for d in docs if bool(d.get("enabled")) == bool(enabled)]


async def set_orphan_enabled(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    orphan_id: str,
    enabled: bool,
) -> bool:
    repo = TradeinOrphansRepo(db)
    now = _now_utc()

    updated = await repo.set_enabled(user_id=user_id, orphan_id=orphan_id, enabled=enabled, updated_at=now)
    if not updated:
        raise NotFoundError(
            code="orphan_not_found",
            message="Trade-in orphan not found",
            details={"user_id": user_id, "orphan_id": orphan_id},
        )
    return True


async def set_manual_trade_sku_for_orphan(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    orphan_id: str,
    manual_trade_sku: Optional[str],
) -> bool:
    repo = TradeinOrphansRepo(db)
    now = _now_utc()

    doc = await repo.get_one(user_id=user_id, orphan_id=orphan_id)
    if not doc:
        raise NotFoundError(
            code="orphan_not_found",
            message="Trade-in orphan not found",
            details={"user_id": user_id, "orphan_id": orphan_id},
        )

    m = _norm_str(manual_trade_sku)
    raw_sku = _norm_str(doc.get("sku"))
    grade_code = _norm_str(doc.get("tradein_grade_code")) or _norm_str(doc.get("aesthetic_grade_code"))

    effective_trade_sku = m or raw_sku
    trade_sku_source = "manual" if m else "auto"

    derived = _derive_fields_from_trade_sku(
        trade_sku=effective_trade_sku,
        tradein_grade_code=grade_code,
        fallback_reason_details=(doc.get("reason_details") or {}),
    )
    derived["trade_sku_source"] = trade_sku_source

    # If the user provided a manual trade_sku, it must be parseable.
    if m and not bool(derived.get("trade_sku_parse_ok")):
        raise BadRequestError(
            code="invalid_trade_sku",
            message="manual_trade_sku could not be parsed",
            details={
                "manual_trade_sku": m,
                "reason_code": derived.get("trade_sku_parse_reason_code"),
                "reason_details": derived.get("trade_sku_parse_reason_details"),
            },
        )

    updated = await repo.set_manual_trade_sku(
        user_id=user_id,
        orphan_id=orphan_id,
        manual_trade_sku=m,
        updated_at=now,
        derived_fields={**derived, "updated_at": now},
    )
    if not updated:
        raise NotFoundError(
            code="orphan_not_found",
            message="Trade-in orphan not found",
            details={"user_id": user_id, "orphan_id": orphan_id},
        )

    return True


async def set_manual_sell_anchor_for_orphan(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    orphan_id: str,
    manual_sell_anchor_gross: Optional[float],
) -> bool:
    repo = TradeinOrphansRepo(db)
    now = _now_utc()

    updated = await repo.set_manual_sell_anchor_gross(
        user_id=user_id,
        orphan_id=orphan_id,
        manual_sell_anchor_gross=manual_sell_anchor_gross,
        updated_at=now,
    )

    if not updated:
        raise NotFoundError(
            code="orphan_not_found",
            message="Trade-in orphan not found",
            details={"user_id": user_id, "orphan_id": orphan_id},
        )

    return True

