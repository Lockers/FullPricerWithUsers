from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.tradein.competitors_service import (
    DEFAULT_CURRENCY,
    DEFAULT_MARKET,
    REASON_BAD_REQUEST,
    REASON_NOT_FOUND,
    REASON_UNAUTHORIZED,
    REASON_UNPROCESSABLE,
    build_tradein_update_payload,
)
from app.features.backmarket.tradein.repo import get_bad_tradein_ids_for_user, upsert_bad_tradein
from app.features.backmarket.transport.cache import get_bm_client_for_user
from app.features.backmarket.transport.http_utils import parse_cf_ray, safe_text_prefix
from app.features.backmarket.tradein_orphans.repo import ORPHAN_COL, TradeinOrphansRepo


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _hard_failure_reason_for_update_status(status_code: int) -> Optional[str]:
    if status_code == 400:
        return REASON_BAD_REQUEST
    if status_code == 401:
        return REASON_UNAUTHORIZED
    if status_code == 404:
        return REASON_NOT_FOUND
    if status_code == 422:
        return REASON_UNPROCESSABLE
    return None


def _get_nested(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(part)
        if cur is None:
            return default
    return cur


@dataclass(frozen=True)
class OrphanOfferRef:
    orphan_id: str
    tradein_id: str
    desired_amount: int
    computed_at: Optional[Any]
    final_update_reason: Optional[str]
    best_scenario_key: Optional[str]
    profit_at_final_update: Optional[float]


async def _apply_offer_update(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    ref: OrphanOfferRef,
    market: str,
    currency: str,
    dry_run: bool,
) -> Dict[str, Any]:
    sent_at = _now_utc()

    if dry_run:
        return {
            "ok": True,
            "status": 200,
            "cf_ray": None,
            "error_body_prefix": None,
            "sent_at": sent_at,
        }

    client = await get_bm_client_for_user(db, user_id)
    resp = await client.request(
        "PUT",
        f"/ws/buyback/v1/listings/{ref.tradein_id}",
        endpoint_key="tradein_update",
        json_body=build_tradein_update_payload(
            market=market,
            currency=currency,
            amount=Decimal(ref.desired_amount),
        ),
        raise_for_status=False,
    )

    status = int(getattr(resp, "status_code", 0) or 0)
    ok = status in (200, 202)

    body_prefix: Optional[str] = None
    if not ok:
        try:
            body_prefix = safe_text_prefix(resp)
        except Exception:  # noqa: BLE001
            body_prefix = None

    return {
        "ok": ok,
        "status": status,
        "cf_ray": parse_cf_ray(resp),
        "error_body_prefix": body_prefix,
        "sent_at": sent_at,
    }


async def run_tradein_orphan_offer_update_for_orphan(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    orphan_id: str,
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
    dry_run: bool = False,
    require_ok_to_update: bool = True,
) -> Dict[str, Any]:
    mkt = str(market).upper().strip() or DEFAULT_MARKET
    cur = str(currency).upper().strip() or DEFAULT_CURRENCY

    repo = TradeinOrphansRepo(db)
    d = await repo.get_one(user_id=user_id, orphan_id=orphan_id)
    if not d:
        return {"ok": False, "error": "not_found"}

    if require_ok_to_update and not bool(_get_nested(d, "trade_pricing.ok_to_update", False)):
        return {"ok": False, "error": "not_ok_to_update"}

    desired = int(_get_nested(d, "trade_pricing.final_update_price_gross", 0) or 0)
    if desired <= 0:
        return {"ok": False, "error": "missing_final_update_price"}

    tradein_id = str(_get_nested(d, "tradein_id") or _get_nested(d, "tradein_listing.tradein_id") or "").strip()
    if not tradein_id:
        return {"ok": False, "error": "missing_tradein_id"}

    ref = OrphanOfferRef(
        orphan_id=str(d.get("id") or orphan_id),
        tradein_id=tradein_id,
        desired_amount=desired,
        computed_at=_get_nested(d, "trade_pricing.computed.computed_at"),
        final_update_reason=_get_nested(d, "trade_pricing.final_update_reason"),
        best_scenario_key=_get_nested(d, "trade_pricing.best_scenario_key"),
        profit_at_final_update=_get_nested(d, "trade_pricing.profit_at_final_update"),
    )

    applied = await _apply_offer_update(db, user_id=user_id, ref=ref, market=mkt, currency=cur, dry_run=dry_run)
    status = int(applied.get("status") or 0)

    if not applied.get("ok"):
        reason = _hard_failure_reason_for_update_status(status)
        if reason:
            await upsert_bad_tradein(
                db,
                user_id=user_id,
                tradein_id=tradein_id,
                reason_code=reason,
                status_code=status,
                detail=applied.get("error_body_prefix"),
            )

    offer_doc: Dict[str, Any] = {
        "market": mkt,
        "currency": cur,
        "amount": desired,
        "ok": bool(applied.get("ok")),
        "status": status,
        "cf_ray": applied.get("cf_ray"),
        "error_body_prefix": applied.get("error_body_prefix"),
        "sent_at": applied.get("sent_at"),
        "final_update_reason": ref.final_update_reason,
        "best_scenario_key": ref.best_scenario_key,
        "profit_at_final_update": ref.profit_at_final_update,
        "computed_at": ref.computed_at,
        "dry_run": dry_run,
    }

    now = _now_utc()
    await repo.persist_offer_update(user_id=user_id, orphan_id=orphan_id, offer_update_doc=offer_doc, updated_at=now)

    return {
        "user_id": user_id,
        "orphan_id": orphan_id,
        "tradein_id": tradein_id,
        "desired_amount": desired,
        "dry_run": dry_run,
        "ok": bool(applied.get("ok")),
        "status": status,
        "updated_at": now.isoformat(),
    }


async def run_tradein_orphan_offer_updates_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
    dry_run: bool = False,
    only_enabled: bool = True,
    require_ok_to_update: bool = True,
    concurrency: int = 10,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    mkt = str(market).upper().strip() or DEFAULT_MARKET
    cur = str(currency).upper().strip() or DEFAULT_CURRENCY

    repo = TradeinOrphansRepo(db)

    hard_failed: Set[str] = await get_bad_tradein_ids_for_user(
        db,
        user_id=user_id,
        reason_codes=[REASON_BAD_REQUEST, REASON_UNAUTHORIZED, REASON_NOT_FOUND, REASON_UNPROCESSABLE],
    )

    q: Dict[str, Any] = {"user_id": user_id}
    if only_enabled:
        q["enabled"] = True

    cursor = db[ORPHAN_COL].find(
        q,
        {
            "_id": 1,
            "tradein_id": 1,
            "tradein_listing.tradein_id": 1,
            "enabled": 1,
            "trade_pricing.ok_to_update": 1,
            "trade_pricing.final_update_price_gross": 1,
            "trade_pricing.best_scenario_key": 1,
            "trade_pricing.final_update_reason": 1,
            "trade_pricing.profit_at_final_update": 1,
            "trade_pricing.computed.computed_at": 1,
        },
    ).sort("updated_at", -1)

    if limit is not None:
        cursor = cursor.limit(int(limit))

    docs = await cursor.to_list(length=None)

    refs: List[OrphanOfferRef] = []
    for d in docs:
        orphan_id = str(d.get("_id"))
        tradein_id = str(d.get("tradein_id") or _get_nested(d, "tradein_listing.tradein_id") or "").strip()
        if not tradein_id or tradein_id in hard_failed:
            continue

        if require_ok_to_update and not bool(_get_nested(d, "trade_pricing.ok_to_update", False)):
            continue

        desired = int(_get_nested(d, "trade_pricing.final_update_price_gross", 0) or 0)
        if desired <= 0:
            continue

        refs.append(
            OrphanOfferRef(
                orphan_id=orphan_id,
                tradein_id=tradein_id,
                desired_amount=desired,
                computed_at=_get_nested(d, "trade_pricing.computed.computed_at"),
                final_update_reason=_get_nested(d, "trade_pricing.final_update_reason"),
                best_scenario_key=_get_nested(d, "trade_pricing.best_scenario_key"),
                profit_at_final_update=_get_nested(d, "trade_pricing.profit_at_final_update"),
            )
        )

    sem = asyncio.Semaphore(max(1, int(concurrency)))

    ok = 0
    err = 0

    async def _worker(r: OrphanOfferRef) -> None:
        nonlocal ok, err
        async with sem:
            try:
                applied = await _apply_offer_update(db, user_id=user_id, ref=r, market=mkt, currency=cur, dry_run=dry_run)
                status = int(applied.get("status") or 0)

                if not applied.get("ok"):
                    reason = _hard_failure_reason_for_update_status(status)
                    if reason:
                        await upsert_bad_tradein(
                            db,
                            user_id=user_id,
                            tradein_id=r.tradein_id,
                            reason_code=reason,
                            status_code=status,
                            detail=applied.get("error_body_prefix"),
                        )
                    err += 1
                else:
                    ok += 1

                offer_doc: Dict[str, Any] = {
                    "market": mkt,
                    "currency": cur,
                    "amount": r.desired_amount,
                    "ok": bool(applied.get("ok")),
                    "status": status,
                    "cf_ray": applied.get("cf_ray"),
                    "error_body_prefix": applied.get("error_body_prefix"),
                    "sent_at": applied.get("sent_at"),
                    "final_update_reason": r.final_update_reason,
                    "best_scenario_key": r.best_scenario_key,
                    "profit_at_final_update": r.profit_at_final_update,
                    "computed_at": r.computed_at,
                    "dry_run": dry_run,
                }

                await repo.persist_offer_update(user_id=user_id, orphan_id=r.orphan_id, offer_update_doc=offer_doc, updated_at=_now_utc())
            except Exception:  # noqa: BLE001
                err += 1

    await asyncio.gather(*[_worker(r) for r in refs])

    return {
        "user_id": user_id,
        "market": mkt,
        "currency": cur,
        "dry_run": dry_run,
        "require_ok_to_update": require_ok_to_update,
        "only_enabled": only_enabled,
        "eligible": len(refs),
        "ok": ok,
        "errors": err,
        "collection": ORPHAN_COL,
        "updated_at": _now_utc().isoformat(),
    }


