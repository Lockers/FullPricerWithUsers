from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError

from app.features.backmarket.tradein.competitors_service import (
    DEFAULT_CURRENCY,
    DEFAULT_MARKET,
    REASON_BAD_REQUEST,
    REASON_NOT_FOUND,
    REASON_UNAUTHORIZED,
    REASON_UNPROCESSABLE,
    fetch_tradein_competitor_snapshot_once,
    set_tradein_price_to_one,
)
from app.features.backmarket.tradein.repo import get_bad_tradein_ids_for_user, upsert_bad_tradein
from app.features.backmarket.tradein_orphans.repo import TradeinOrphansRepo

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrphanTradeinRef:
    orphan_id: str
    tradein_id: str
    trade_sku: Optional[str]
    enabled: bool


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _hard_failure_reason_for_update_status(status: int) -> Optional[str]:
    if status == 400:
        return REASON_BAD_REQUEST
    if status == 401:
        return REASON_UNAUTHORIZED
    if status == 404:
        return REASON_NOT_FOUND
    if status == 422:
        return REASON_UNPROCESSABLE
    return None


def _d2f(x: Optional[Decimal]) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _snapshot_to_latest_doc(snap: Any) -> Dict[str, Any]:
    return {
        "tradein_id": snap.tradein_id,
        "market": snap.market,
        "currency": snap.currency,
        "missing": bool(getattr(snap, "missing", False)),
        "fetched_at": snap.fetched_at,
        "is_winning": getattr(snap, "is_winning", None),
        "current_price": _d2f(getattr(snap, "current_price", None)),
        "gross_price_to_win": _d2f(getattr(snap, "gross_price_to_win", None)),
        "net_price_to_win": _d2f(getattr(snap, "net_price_to_win", None)),
        "error_status": getattr(snap, "error_status", None),
        "error_body_prefix": getattr(snap, "error_body_prefix", None),
        "cf_ray": getattr(snap, "cf_ray", None),
    }


def _snapshot_to_history_doc(snap: Any) -> Dict[str, Any]:
    return {
        "fetched_at": snap.fetched_at,
        "market": snap.market,
        "currency": snap.currency,
        "missing": bool(getattr(snap, "missing", False)),
        "is_winning": getattr(snap, "is_winning", None),
        "current_price": _d2f(getattr(snap, "current_price", None)),
        "gross_price_to_win": _d2f(getattr(snap, "gross_price_to_win", None)),
        "net_price_to_win": _d2f(getattr(snap, "net_price_to_win", None)),
        "error_status": getattr(snap, "error_status", None),
    }


async def _iter_orphan_refs(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str,
    limit: Optional[int],
    include_disabled: bool,
) -> List[OrphanTradeinRef]:
    q: Dict[str, Any] = {"user_id": user_id}

    mkt = str(market).upper().strip()
    if mkt:
        q["markets"] = mkt

    if not include_disabled:
        q["enabled"] = True

    proj = {"_id": 1, "tradein_id": 1, "trade_sku": 1, "enabled": 1}

    cur = db["pricing_tradein_orphans"].find(q, proj).sort("updated_at", -1)
    if limit is not None:
        cur = cur.limit(int(limit))

    refs: List[OrphanTradeinRef] = []
    async for d in cur:
        oid = d.get("_id")
        tid = d.get("tradein_id")
        if not oid or not tid:
            continue
        refs.append(
            OrphanTradeinRef(
                orphan_id=str(oid),
                tradein_id=str(tid),
                trade_sku=d.get("trade_sku"),
                enabled=bool(d.get("enabled", False)),
            )
        )

    return refs


async def run_tradein_orphan_competitor_refresh_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
    update_concurrency: int = 10,
    fetch_concurrency: int = 10,
    limit: Optional[int] = None,
    include_disabled: bool = False,
    wait_seconds: int = 60,
    fetch_attempts: int = 3,
    retry_wait_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    """Competitor refresh for trade-in orphans.

    1) set each trade-in to £1
    2) wait
    3) fetch competitors (round-level retries)

    Persists results into pricing_tradein_orphans.tradein_listing.competitor(+history).
    """
    start = time.perf_counter()
    mkt = str(market).upper().strip() or DEFAULT_MARKET
    cur = str(currency).upper().strip() or DEFAULT_CURRENCY

    update_sem = asyncio.Semaphore(max(1, int(update_concurrency)))
    fetch_sem = asyncio.Semaphore(max(1, int(fetch_concurrency)))

    attempts = max(1, int(fetch_attempts))
    wait_s = max(0, int(wait_seconds))
    retry_wait_s = wait_s if retry_wait_seconds is None else max(0, int(retry_wait_seconds))

    repo = TradeinOrphansRepo(db)

    # Respect user-disabled trade-ins (gb_amount <= 0 in bm_tradein_listings).
    disabled_ids: Set[str] = set()
    try:
        cursor = db["bm_tradein_listings"].find(
            {"user_id": user_id, "gb_amount": {"$lte": 0}},
            {"_id": 0, "tradein_id": 1},
        )
        async for doc in cursor:
            tid = doc.get("tradein_id")
            if tid:
                disabled_ids.add(str(tid))
    except PyMongoError:
        logger.exception("[tradein_orphans_competitors] failed loading disabled trade-ins user_id=%s", user_id)

    # Skip known hard failures (400/401/404/422 from prior runs).
    skip_ids: Set[str] = await get_bad_tradein_ids_for_user(
        db,
        user_id=user_id,
        reason_codes=[REASON_BAD_REQUEST, REASON_UNAUTHORIZED, REASON_NOT_FOUND, REASON_UNPROCESSABLE],
    )

    refs = await _iter_orphan_refs(db, user_id=user_id, market=mkt, limit=limit, include_disabled=include_disabled)

    stage1_ok_refs: List[OrphanTradeinRef] = []
    stage1_skipped_bad = 0
    stage1_skipped_disabled = 0
    stage1_hard_failed = 0
    stage1_failed = 0

    async def _set_one(ref: OrphanTradeinRef) -> None:
        nonlocal stage1_skipped_bad, stage1_skipped_disabled, stage1_hard_failed, stage1_failed

        if ref.tradein_id in disabled_ids:
            stage1_skipped_disabled += 1
            return
        if ref.tradein_id in skip_ids:
            stage1_skipped_bad += 1
            return

        async with update_sem:
            ok, status, error_body_prefix, _cf_ray = await set_tradein_price_to_one(
                db,
                user_id=user_id,
                tradein_id=ref.tradein_id,
                market=mkt,
                currency=cur,
            )

        if ok:
            stage1_ok_refs.append(ref)
            return

        stage1_failed += 1
        reason = _hard_failure_reason_for_update_status(int(status))
        if reason:
            stage1_hard_failed += 1
            try:
                await upsert_bad_tradein(
                    db,
                    user_id=user_id,
                    tradein_id=ref.tradein_id,
                    reason_code=reason,
                    status_code=int(status),
                    detail=error_body_prefix,
                )
            except PyMongoError:
                logger.exception("[tradein_orphans_competitors] upsert_bad_tradein failed tradein_id=%s", ref.tradein_id)

    await asyncio.gather(*[_set_one(r) for r in refs])

    if wait_s > 0 and stage1_ok_refs:
        await asyncio.sleep(float(wait_s))

    stage2_ok = 0
    stage2_unresolved = 0
    stage2_persist_failed = 0

    transient_statuses = {0, 404, 429, 500, 502, 503, 504}
    terminal_statuses = {400, 401, 422}

    async def _fetch_one(ref: OrphanTradeinRef) -> None:
        nonlocal stage2_ok, stage2_unresolved, stage2_persist_failed

        snap = None
        for attempt in range(1, attempts + 1):
            async with fetch_sem:
                snap = await fetch_tradein_competitor_snapshot_once(
                    db,
                    user_id=user_id,
                    tradein_id=ref.tradein_id,
                    market=mkt,
                    currency=cur,
                )

            status = getattr(snap, "error_status", None)

            if status is None and not bool(getattr(snap, "missing", False)):
                break

            if status is not None and int(status) in terminal_statuses:
                break

            if attempt < attempts:
                if status is None or int(status) in transient_statuses:
                    if retry_wait_s > 0:
                        await asyncio.sleep(float(retry_wait_s))
                    continue
                break

        if snap is None:
            stage2_unresolved += 1
            return

        latest_doc = _snapshot_to_latest_doc(snap)
        history_doc = _snapshot_to_history_doc(snap)

        try:
            await repo.persist_competitor_snapshot(
                user_id=user_id,
                orphan_id=ref.orphan_id,
                latest_doc=latest_doc,
                history_doc=history_doc,
                updated_at=_now_utc(),
            )
        except PyMongoError:
            stage2_persist_failed += 1
            logger.exception(
                "[tradein_orphans_competitors] persist failed orphan_id=%s tradein_id=%s",
                ref.orphan_id,
                ref.tradein_id,
            )
            return

        if getattr(snap, "error_status", None) is None and not bool(getattr(snap, "missing", False)):
            stage2_ok += 1
        else:
            stage2_unresolved += 1

    await asyncio.gather(*[_fetch_one(r) for r in stage1_ok_refs])

    elapsed = time.perf_counter() - start
    return {
        "user_id": user_id,
        "market": mkt,
        "currency": cur,
        "refs": len(refs),
        "stage1_ok": len(stage1_ok_refs),
        "stage1_failed": stage1_failed,
        "stage1_skipped_hard": stage1_hard_failed,
        "stage1_skipped": stage1_skipped_bad,
        "stage1_skipped_disabled": stage1_skipped_disabled,
        "stage2_ok": stage2_ok,
        "stage2_unresolved": stage2_unresolved,
        "stage2_persist_failed": stage2_persist_failed,
        "wait_seconds": wait_s,
        "fetch_attempts": attempts,
        "elapsed_seconds": round(float(elapsed), 3),
    }

