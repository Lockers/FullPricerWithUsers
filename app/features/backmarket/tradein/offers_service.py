from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.pricing.trade_pricing_service import recompute_trade_pricing_for_group
from app.features.backmarket.tradein.competitors_service import (
    DEFAULT_CURRENCY,
    DEFAULT_MARKET,
    REASON_BAD_REQUEST,
    REASON_NOT_FOUND,
    REASON_UNAUTHORIZED,
    REASON_UNPROCESSABLE,
    build_tradein_update_payload,
    hard_failure_reason_code,
)
from app.features.backmarket.tradein.repo import (
    get_bad_tradein_ids_for_user,
    persist_tradein_offer_update,
    upsert_bad_tradein,
)
from app.features.backmarket.transport.cache import get_bm_client_for_user
from app.features.backmarket.transport.exceptions import BMMaxRetriesError, BMRateLimited
from app.features.backmarket.transport.http_utils import parse_cf_ray, safe_text_prefix

logger = logging.getLogger(__name__)


PRICING_GROUPS_COL = "pricing_groups"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _get_nested(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _parse_object_id(s: str) -> ObjectId:
    try:
        return ObjectId(str(s))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Invalid ObjectId: {s}") from exc


@dataclass(frozen=True)
class OfferApplyRef:
    group_id: str
    trade_sku: str
    tradein_id: str
    final_update_price_gross: int


async def _update_tradein_offer(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    tradein_id: str,
    amount_gbp: int,
    market: str,
    currency: str,
    dry_run: bool,
) -> Tuple[bool, int, Optional[str], Optional[str]]:
    """Return (ok, status, cf_ray, error_body_prefix)."""

    if dry_run:
        return True, 0, None, None

    client = await get_bm_client_for_user(db, user_id)

    mkt = str(market).upper().strip() or DEFAULT_MARKET
    cur = str(currency).upper().strip() or DEFAULT_CURRENCY

    try:
        resp = await client.request(
            "PUT",
            f"/ws/buyback/v1/listings/{tradein_id}",
            endpoint_key="tradein_update",
            json_body=build_tradein_update_payload(
                market=mkt,
                currency=cur,
                amount=Decimal(int(amount_gbp)),
            ),
            raise_for_status=False,
        )
    except BMRateLimited as exc:
        return False, 0, None, f"BREAKER_OPEN: {exc!s}"
    except BMMaxRetriesError as exc:
        return False, 0, None, f"MAX_RETRIES: {exc!s}"

    status = int(resp.status_code)
    cf_ray = parse_cf_ray(resp)

    if status in (200, 202, 204):
        return True, status, cf_ray, None

    return False, status, cf_ray, safe_text_prefix(resp, limit=2000)


async def run_tradein_offer_update_for_group(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    group_id: str,
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
    dry_run: bool = True,
    recompute_pricing: bool = True,
    require_ok_to_update: bool = True,
) -> Dict[str, Any]:
    """Apply a single pricing_group.trade_pricing.final_update_price_gross to BM."""

    gid = _parse_object_id(group_id)

    group = await db[PRICING_GROUPS_COL].find_one({"_id": gid, "user_id": user_id})
    if not group:
        return {"error": "pricing_group_not_found", "user_id": user_id, "group_id": group_id}

    # Ensure trade_pricing is up to date (best-effort).
    if recompute_pricing:
        try:
            await recompute_trade_pricing_for_group(db, user_id=user_id, group_id=group_id)
            group = await db[PRICING_GROUPS_COL].find_one({"_id": gid, "user_id": user_id}) or group
        except Exception as exc:  # noqa: BLE001
            logger.exception("[tradein_offers] recompute_pricing_failed group_id=%s err=%r", group_id, exc)

    trade_sku = str(group.get("trade_sku") or "")
    tradein_id = str(_get_nested(group, "tradein_listing.tradein_id") or "")

    if not tradein_id:
        return {
            "error": "missing_tradein_id",
            "user_id": user_id,
            "group_id": group_id,
            "trade_sku": trade_sku,
        }

    ok_to_update = bool(group.get("trade_pricing", {}).get("ok_to_update"))
    if require_ok_to_update and not ok_to_update:
        return {
            "error": "not_ok_to_update",
            "user_id": user_id,
            "group_id": group_id,
            "trade_sku": trade_sku,
            "tradein_id": tradein_id,
            "not_ok_reasons": group.get("trade_pricing", {}).get("not_ok_reasons") or [],
        }

    amount = group.get("trade_pricing", {}).get("final_update_price_gross")
    try:
        amount_gross = int(amount)
    except Exception:
        return {
            "error": "missing_final_update_price",
            "user_id": user_id,
            "group_id": group_id,
            "trade_sku": trade_sku,
            "tradein_id": tradein_id,
        }

    sent_at = _now_utc()
    ok, status, cf_ray, err_prefix = await _update_tradein_offer(
        db,
        user_id=user_id,
        tradein_id=tradein_id,
        amount_gbp=amount_gross,
        market=market,
        currency=currency,
        dry_run=dry_run,
    )

    # Persist update snapshot (best-effort).
    # Persist update snapshot (best-effort).
    try:
        mkt = str(market).upper().strip() or DEFAULT_MARKET
        cur = str(currency).upper().strip() or DEFAULT_CURRENCY

        latest = {
            "market": mkt,
            "currency": cur,
            "amount": amount_gross,
            "ok": bool(ok),
            "status": int(status),
            "cf_ray": cf_ray,
            "error_body_prefix": err_prefix,
            "sent_at": sent_at,
            "dry_run": bool(dry_run),
            "computed_at": _get_nested(group, "trade_pricing.computed.computed_at"),
            "best_scenario_key": group.get("trade_pricing", {}).get("best_scenario_key"),
            "final_update_reason": group.get("trade_pricing", {}).get("final_update_reason"),
            "profit_at_final_update": group.get("trade_pricing", {}).get("profit_at_final_update"),
        }

        # Keep history the same shape as latest for now (makes auditing simpler).
        history = dict(latest)

        await persist_tradein_offer_update(
            db,
            user_id=user_id,
            trade_sku=trade_sku,
            snapshot_latest=latest,
            snapshot_history=history,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[tradein_offers] persist_offer_update_failed group_id=%s err=%r", group_id, exc)

    # Track hard failures.
    try:
        reason = hard_failure_reason_code(status)
        if reason:
            await upsert_bad_tradein(
                db,
                user_id=user_id,
                tradein_id=tradein_id,
                reason_code=reason,
                status_code=status,
                detail=err_prefix,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[tradein_offers] upsert_bad_tradein_failed tradein_id=%s err=%r", tradein_id, exc)

    return {
        "user_id": user_id,
        "group_id": group_id,
        "trade_sku": trade_sku,
        "tradein_id": tradein_id,
        "market": str(market).upper().strip() or DEFAULT_MARKET,
        "currency": str(currency).upper().strip() or DEFAULT_CURRENCY,
        "amount": amount_gross,
        "ok": bool(ok),
        "status": int(status),
        "cf_ray": cf_ray,
        "error_body_prefix": err_prefix,
        "sent_at": sent_at.isoformat(),
        "dry_run": bool(dry_run),
    }


async def run_tradein_offer_updates_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
    concurrency: int = 10,
    limit: Optional[int] = None,
    dry_run: bool = True,
    require_ok_to_update: bool = True,
    include_item_results: bool = False,
) -> Dict[str, Any]:
    """Apply offers for all pricing_groups for the user (optionally market-filtered)."""

    started = _now_utc()
    mkt = str(market).upper().strip() or DEFAULT_MARKET
    cur = str(currency).upper().strip() or DEFAULT_CURRENCY

    # Skip known hard failures (400/401/404/422 from prior runs).
    bad_ids = await get_bad_tradein_ids_for_user(
        db,
        user_id=user_id,
        reason_codes=[REASON_BAD_REQUEST, REASON_UNAUTHORIZED, REASON_NOT_FOUND, REASON_UNPROCESSABLE],
    )

    q: Dict[str, Any] = {
        "user_id": user_id,
        "tradein_listing.tradein_id": {"$exists": True, "$ne": None},
    }
    if require_ok_to_update:
        q["trade_pricing.ok_to_update"] = True

    # Prefer market-filtered if schema supports it; fall back if it yields zero.
    q_market = dict(q)
    q_market["$or"] = [{"markets": mkt}, {"tradein_listing.markets": mkt}]

    cursor = db[PRICING_GROUPS_COL].find(q_market, projection={"trade_sku": 1, "tradein_listing.tradein_id": 1, "trade_pricing.final_update_price_gross": 1})
    if limit is not None:
        cursor = cursor.limit(int(limit))

    refs = await cursor.to_list(length=None)

    if not refs:
        cursor2 = db[PRICING_GROUPS_COL].find(q, projection={"trade_sku": 1, "tradein_listing.tradein_id": 1, "trade_pricing.final_update_price_gross": 1})
        if limit is not None:
            cursor2 = cursor2.limit(int(limit))
        refs = await cursor2.to_list(length=None)

    apply_refs: List[OfferApplyRef] = []
    skipped_bad = 0
    skipped_missing_price = 0

    for d in refs:
        tradein_id = str(_get_nested(d, "tradein_listing.tradein_id") or "")
        if not tradein_id:
            continue
        if tradein_id in bad_ids:
            skipped_bad += 1
            continue

        trade_sku = str(d.get("trade_sku") or "")
        amt = d.get("trade_pricing", {}).get("final_update_price_gross")
        try:
            amt_i = int(amt)
        except Exception:
            skipped_missing_price += 1
            continue

        apply_refs.append(OfferApplyRef(group_id=str(d.get("_id")), trade_sku=trade_sku, tradein_id=tradein_id, final_update_price_gross=amt_i))

    sem = asyncio.Semaphore(max(1, int(concurrency)))

    results: List[Dict[str, Any]] = []
    ok_count = 0
    err_count = 0

    async def _worker(ref: OfferApplyRef) -> None:
        nonlocal ok_count, err_count
        async with sem:
            res = await run_tradein_offer_update_for_group(
                db,
                user_id=user_id,
                group_id=ref.group_id,
                market=mkt,
                currency=cur,
                dry_run=dry_run,
                recompute_pricing=False,
                require_ok_to_update=require_ok_to_update,
            )
        if res.get("ok"):
            ok_count += 1
        else:
            err_count += 1
        if include_item_results:
            results.append(res)

    start = time.perf_counter()
    await asyncio.gather(*[_worker(r) for r in apply_refs])
    elapsed = time.perf_counter() - start

    out: Dict[str, Any] = {
        "user_id": user_id,
        "market": mkt,
        "currency": cur,
        "dry_run": bool(dry_run),
        "require_ok_to_update": bool(require_ok_to_update),
        "attempted": len(apply_refs),
        "ok": ok_count,
        "errors": err_count,
        "skipped_bad_tradein_ids": skipped_bad,
        "skipped_missing_final_price": skipped_missing_price,
        "elapsed_seconds": round(float(elapsed), 3),
        "started_at": started.isoformat(),
        "finished_at": _now_utc().isoformat(),
    }
    if include_item_results:
        out["results"] = results
    return out
