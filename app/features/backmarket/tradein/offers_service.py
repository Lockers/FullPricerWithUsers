from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError

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
from app.features.backmarket.transport.exceptions import BMClientError, BMMaxRetriesError, BMRateLimited
from app.features.backmarket.transport.http_utils import parse_cf_ray, safe_text_prefix

logger = logging.getLogger(__name__)


PRICING_GROUPS_COL = "pricing_groups"


async def _persist_scenario_override(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    group_object_id: ObjectId,
    scenario_key: str,
    scenario_doc: Optional[Dict[str, Any]],
) -> None:
    """Persist the user's chosen scenario key onto the pricing_group.

    We store the key so future bulk runs (apply-all / price-all) use the same
    scenario until changed.

    Best-effort: DB failures shouldn't block the BM update.
    """

    now = _now_utc()
    set_doc: Dict[str, Any] = {
        "trade_pricing.override_scenario_key": scenario_key,
        "trade_pricing.override_updated_at": now,
        "updated_at": now,
    }

    # Keep convenience fields in sync so bulk workers relying on
    # trade_pricing.final_update_price_gross behave as expected.
    if isinstance(scenario_doc, dict):
        for k in (
            "max_trade_price_net",
            "max_trade_price_gross",
            "max_trade_offer_gross",
            "final_update_price_gross",
            "final_update_price_net",
            "final_update_reason",
            "profit_at_final_update",
            "margin_at_final_update",
            "vat_at_final_update",
            "valid",
        ):
            if scenario_doc.get(k) is not None:
                set_doc[f"trade_pricing.{k}"] = scenario_doc.get(k)

    try:
        await db[PRICING_GROUPS_COL].update_one(
            {"_id": group_object_id, "user_id": user_id},
            {"$set": set_doc},
        )
    except PyMongoError as exc:
        logger.exception(
            "[tradein_offers] persist_override_failed group_id=%s scenario_key=%s err=%r",
            str(group_object_id),
            scenario_key,
            exc,
        )


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
    except (InvalidId, TypeError) as exc:
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
    require_offer_enabled: bool = True,
    scenario_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Apply a single group's trade-in offer to Back Market.

    Default behaviour uses the persisted "best" scenario values.

    If scenario_key is provided, we use the matching scenario from
    trade_pricing.computed.scenarios (so the UI can force a specific route).
    """

    gid = _parse_object_id(group_id)

    group = await db[PRICING_GROUPS_COL].find_one({"_id": gid, "user_id": user_id})
    if not group:
        return {"error": "pricing_group_not_found", "user_id": user_id, "group_id": group_id}

    if require_offer_enabled:
        enabled = _get_nested(group, "trade_pricing.settings.offer_enabled")
        if enabled is not True:
            return {
                "error": "offer_disabled",
                "user_id": user_id,
                "group_id": group_id,
                "trade_sku": str(group.get("trade_sku") or ""),
                "offer_enabled": bool(enabled) if enabled is not None else False,
            }

    # Ensure trade_pricing is up to date (best-effort).
    if recompute_pricing:
        try:
            await recompute_trade_pricing_for_group(db, user_id=user_id, group_id=group_id)
            group = await db[PRICING_GROUPS_COL].find_one({"_id": gid, "user_id": user_id}) or group
        except (BMClientError, PyMongoError, ValueError) as exc:
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

    scenario_key_requested: Optional[str] = None
    if isinstance(scenario_key, str) and scenario_key.strip():
        scenario_key_requested = scenario_key.strip()

    trade_pricing_doc = group.get("trade_pricing")
    if not isinstance(trade_pricing_doc, dict):
        trade_pricing_doc = {}

    computed = trade_pricing_doc.get("computed")
    if not isinstance(computed, dict):
        computed = {}

    scenario_docs = computed.get("scenarios") if isinstance(computed.get("scenarios"), list) else None

    # Persisted per-group override so bulk runs keep using the same route.
    override_key: Optional[str] = None
    raw_override = trade_pricing_doc.get("override_scenario_key")
    if isinstance(raw_override, str) and raw_override.strip():
        override_key = raw_override.strip()

    scenario_used: Optional[Dict[str, Any]] = None

    # Default to persisted "best" selection; overridden below if requested/override exists.
    scenario_used_key: Optional[str] = trade_pricing_doc.get("best_scenario_key") or computed.get("best_scenario_key")
    scenario_selection_source = "best"

    if scenario_key_requested:
        scenario_used_key = scenario_key_requested
        scenario_selection_source = "requested"
    elif override_key:
        scenario_used_key = override_key
        scenario_selection_source = "override"

    if scenario_used_key and isinstance(scenario_docs, list):
        for s in scenario_docs:
            if isinstance(s, dict) and s.get("key") == scenario_used_key:
                scenario_used = s
                break

    # If the user requested a scenario, and we can see scenarios, but it doesn't exist, hard error.
    if scenario_key_requested and scenario_used is None and isinstance(scenario_docs, list):
        return {
            "error": "scenario_not_found",
            "user_id": user_id,
            "group_id": group_id,
            "trade_sku": trade_sku,
            "tradein_id": tradein_id,
            "scenario_key": scenario_key_requested,
        }

    # If an override is set but isn't present in the latest computed scenarios, block updates
    # rather than silently reverting to best.
    if (not scenario_key_requested) and override_key and scenario_used is None and isinstance(scenario_docs, list):
        return {
            "error": "scenario_override_not_found",
            "user_id": user_id,
            "group_id": group_id,
            "trade_sku": trade_sku,
            "tradein_id": tradein_id,
            "scenario_key": override_key,
        }

    # If the UI explicitly chose a scenario, persist it so future bulk runs use it.
    # Persist even on dry_run; this is a *selection* not a BM-side effect.
    if scenario_key_requested and (scenario_used is not None or not isinstance(scenario_docs, list)):
        await _persist_scenario_override(
            db,
            user_id=user_id,
            group_object_id=gid,
            scenario_key=scenario_key_requested,
            scenario_doc=scenario_used,
        )
        override_key = scenario_key_requested

    def _to_float(x: Any) -> Optional[float]:
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    # Enforce ok_to_update.
    # - If we are using a specific scenario (requested or override) and we have
    #   the scenario row, validate THAT scenario.
    # - Otherwise, fall back to the persisted group-level ok_to_update flag.
    validate_scenario = bool((scenario_key_requested or override_key) and isinstance(scenario_used, dict))

    if require_ok_to_update:
        if validate_scenario:
            not_ok_reasons: List[str] = []

            tp_currency = str(computed.get("currency") or trade_pricing_doc.get("currency") or currency).upper().strip()
            if tp_currency != "GBP":
                not_ok_reasons.append("unsupported_currency")

            if not (isinstance(scenario_used, dict) and bool(scenario_used.get("valid"))):
                not_ok_reasons.append("scenario_not_valid")

            final_reason = scenario_used.get("final_update_reason") if isinstance(scenario_used, dict) else None
            if final_reason == "max_trade_below_min_price":
                not_ok_reasons.append("max_trade_below_min_price")

            f_gross = _to_float(scenario_used.get("final_update_price_gross") if isinstance(scenario_used, dict) else None)
            f_cap = _to_float(scenario_used.get("max_trade_offer_gross") if isinstance(scenario_used, dict) else None)
            f_profit = _to_float(scenario_used.get("profit_at_final_update") if isinstance(scenario_used, dict) else None)

            if f_gross is None:
                not_ok_reasons.append("missing_final_update_price_gross")
            elif f_gross < 1.0:
                not_ok_reasons.append("final_update_below_min_price")

            if f_cap is None:
                not_ok_reasons.append("missing_max_trade_offer_gross")

            if f_gross is not None and f_cap is not None and f_gross > f_cap + 1e-9:
                not_ok_reasons.append("final_update_exceeds_max_trade_offer")

            required_profit = _to_float(_get_nested(group, "trade_pricing.computed.requirements.required_profit"))
            if required_profit is not None:
                if f_profit is None:
                    not_ok_reasons.append("missing_profit_at_final_update")
                elif f_profit < required_profit - 0.01:
                    not_ok_reasons.append("profit_below_required")

            if not_ok_reasons:
                return {
                    "error": "not_ok_to_update",
                    "user_id": user_id,
                    "group_id": group_id,
                    "trade_sku": trade_sku,
                    "tradein_id": tradein_id,
                    "scenario_key": scenario_used_key,
                    "scenario_selection_source": scenario_selection_source,
                    "not_ok_reasons": not_ok_reasons,
                }
        else:
            ok_to_update = bool(trade_pricing_doc.get("ok_to_update"))
            if not ok_to_update:
                return {
                    "error": "not_ok_to_update",
                    "user_id": user_id,
                    "group_id": group_id,
                    "trade_sku": trade_sku,
                    "tradein_id": tradein_id,
                    "not_ok_reasons": trade_pricing_doc.get("not_ok_reasons") or [],
                }

    if isinstance(scenario_used, dict) and scenario_used.get("final_update_price_gross") is not None:
        amount_obj: Any = scenario_used.get("final_update_price_gross")
        amount_source = "scenario"
    else:
        amount_obj = trade_pricing_doc.get("final_update_price_gross")
        amount_source = "best"

    try:
        amount_gross = int(amount_obj)
    except (TypeError, ValueError):
        return {
            "error": "missing_final_update_price",
            "user_id": user_id,
            "group_id": group_id,
            "trade_sku": trade_sku,
            "tradein_id": tradein_id,
            "scenario_key": scenario_used_key,
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
    try:
        mkt = str(market).upper().strip() or DEFAULT_MARKET
        cur = str(currency).upper().strip() or DEFAULT_CURRENCY

        used_reason = (
            scenario_used.get("final_update_reason")
            if isinstance(scenario_used, dict)
            else trade_pricing_doc.get("final_update_reason")
        )
        used_profit = (
            scenario_used.get("profit_at_final_update")
            if isinstance(scenario_used, dict)
            else trade_pricing_doc.get("profit_at_final_update")
        )

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
            "best_scenario_key": trade_pricing_doc.get("best_scenario_key"),
            "scenario_key_requested": scenario_key_requested,
            "scenario_key_used": scenario_used_key,
            "final_update_reason": used_reason,
            "profit_at_final_update": used_profit,
            "amount_source": amount_source,
        }

        history = dict(latest)

        await persist_tradein_offer_update(
            db,
            user_id=user_id,
            trade_sku=trade_sku,
            snapshot_latest=latest,
            snapshot_history=history,
        )
    except PyMongoError as exc:
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
    except PyMongoError as exc:
        logger.exception("[tradein_offers] upsert_bad_tradein_failed tradein_id=%s err=%r", tradein_id, exc)

    return {
        "user_id": user_id,
        "group_id": group_id,
        "trade_sku": trade_sku,
        "tradein_id": tradein_id,
        "market": str(market).upper().strip() or DEFAULT_MARKET,
        "currency": str(currency).upper().strip() or DEFAULT_CURRENCY,
        "amount": amount_gross,
        "amount_source": amount_source,
        "scenario_key_requested": scenario_key_requested,
        "scenario_key_used": scenario_used_key,
        "best_scenario_key": trade_pricing_doc.get("best_scenario_key") if isinstance(trade_pricing_doc, dict) else None,
        "ok": bool(ok),
        "status": int(status),
        "cf_ray": cf_ray,
        "error_body_prefix": err_prefix,
        "sent_at": sent_at.isoformat(),
        "dry_run": bool(dry_run),
    }


async def disable_tradein_offer_for_group(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    group_id: str,
    market: str = "GB",
    currency: str = "GBP",
    amount_gross: int = 0,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Disable a group's trade-in offer and push a disabling price to Back Market.

    This does two things:
      1) sets trade_pricing.settings.offer_enabled=false
      2) updates the trade-in offer amount on Back Market (default: 0).

    Notes:
      - If Back Market rejects amount_gross=0, we fall back to 1.
      - This endpoint is intentionally separate from the settings patch endpoint so the UI can
        guarantee a price is pushed when turning the toggle off.
    """

    gid = _parse_object_id(group_id)
    group = await db[PRICING_GROUPS_COL].find_one({"_id": gid, "user_id": user_id})
    if not group:
        return {
            "error": "group_not_found",
            "user_id": user_id,
            "group_id": group_id,
        }

    trade_sku = str(group.get("trade_sku") or "")
    tradein_id = _get_nested(group, "tradein_listing.tradein_id")
    if not tradein_id:
        return {
            "error": "missing_tradein_id",
            "user_id": user_id,
            "group_id": group_id,
            "trade_sku": trade_sku,
        }

    now = _now_utc()
    await db[PRICING_GROUPS_COL].update_one(
        {"_id": gid, "user_id": user_id},
        {
            "$set": {
                "trade_pricing.settings.offer_enabled": False,
                "trade_pricing.settings_updated_at": now,
                "updated_at": now,
            }
        },
    )

    requested_amount = int(amount_gross)
    used_amount = requested_amount
    res = await _update_tradein_offer(
        db,
        user_id=user_id,
        group_id=group_id,
        trade_sku=trade_sku,
        tradein_id=tradein_id,
        market=market,
        currency=currency,
        amount_gbp=used_amount,
        dry_run=dry_run,
    )

    # If 0 is rejected, try 1 as a best-effort fallback.
    if (not res.get("ok")) and used_amount == 0 and not dry_run:
        used_amount = 1
        res2 = await _update_tradein_offer(
            db,
            user_id=user_id,
            group_id=group_id,
            trade_sku=trade_sku,
            tradein_id=tradein_id,
            market=market,
            currency=currency,
            amount_gbp=used_amount,
            dry_run=dry_run,
        )
        res = {
            "ok": bool(res2.get("ok")),
            "fallback_from": 0,
            "fallback_to": 1,
            "first_attempt": res,
            "second_attempt": res2,
        }

    return {
        "user_id": user_id,
        "group_id": group_id,
        "trade_sku": trade_sku,
        "tradein_id": tradein_id,
        "disabled": True,
        "requested_amount_gross": requested_amount,
        "amount_gross": used_amount,
        "dry_run": bool(dry_run),
        "result": res,
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
    require_offer_enabled: bool = True,
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

    if require_offer_enabled:
        q["trade_pricing.settings.offer_enabled"] = True

    # Prefer market-filtered if schema supports it; fall back if it yields zero.
    q_market = dict(q)
    q_market["$or"] = [{"markets": mkt}, {"tradein_listing.markets": mkt}]

    cursor = db[PRICING_GROUPS_COL].find(
        q_market,
        projection={
            "trade_sku": 1,
            "tradein_listing.tradein_id": 1,
            "trade_pricing.final_update_price_gross": 1,
        },
    )
    if limit is not None:
        cursor = cursor.limit(int(limit))

    refs = await cursor.to_list(length=None)

    if not refs:
        cursor2 = db[PRICING_GROUPS_COL].find(
            q,
            projection={
                "trade_sku": 1,
                "tradein_listing.tradein_id": 1,
                "trade_pricing.final_update_price_gross": 1,
            },
        )
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
        except (TypeError, ValueError):
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
                require_offer_enabled=require_offer_enabled,
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
        "require_offer_enabled": bool(require_offer_enabled),
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
