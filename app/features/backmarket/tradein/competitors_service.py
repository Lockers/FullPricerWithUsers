from __future__ import annotations

"""Trade-in competitor pricing flow (two-stage, with a global wait + retry rounds).

Goal
----
For each pricing_groups doc for a user that has tradein_listing.tradein_id:

Stage 1 (set_to_one):
  - PUT /ws/buyback/v1/listings/{tradein_id}
  - Force market price to £1.00 (GB)
  - STRICT success: HTTP 202 only

Wait (global):
  - Sleep wait_seconds (default 60)

Stage 2 (fetch_competitors, multi-round):
  - GET /ws/buyback/v1/competitors/{tradein_id} (single request per round)
  - If response is missing competitor data (commonly 404) OR price_to_win missing:
      * Do NOT poll fast.
      * Retry in a later round after waiting retry_wait_seconds (default = wait_seconds).
      * Up to fetch_attempts rounds (default 3).
  - Persist into pricing_groups:
      * tradein_listing.competitor (latest)
      * tradein_listing.competitor_history (bounded)
      * Also updates pricing_groups.prices.{MARKET} when gross_price_to_win is available.

Unresolved after retries:
  - Upsert into pricing_unavailable_tradein_competitors for manual review.

Notes
-----
- 404 from competitors is treated as expected eventual consistency, not a crash.
- Circuit breaker / BMRateLimited is treated as transient: it becomes a "missing" snapshot
  for that round, and will be retried in a later round.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Set, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError

from app.features.backmarket.transport.cache import get_bm_client_for_user
from app.features.backmarket.transport.exceptions import BMMaxRetriesError, BMRateLimited
from app.features.backmarket.transport.http_utils import parse_cf_ray, safe_text_prefix

from app.features.backmarket.tradein.repo import (
    get_bad_tradein_ids_for_user,
    persist_tradein_competitor_snapshot,
    upsert_bad_tradein,
    upsert_unavailable_tradein_competitor,
)

from app.features.backmarket.pricing.trade_pricing_service import recompute_trade_pricing_for_user


logger = logging.getLogger(__name__)

DEFAULT_MARKET = "GB"
DEFAULT_CURRENCY = "GBP"

FORCED_MIN_PRICE_GBP = Decimal("1.00")

FEE_RATE = Decimal("0.10")  # 10%
DELIVERY_FEE_GBP = Decimal("10.90")  # fixed
TWOPLACES = Decimal("0.01")

# pricing_bad_tradein_skus reason codes (for UPDATE step hard failures)
REASON_BAD_REQUEST = "bad_request"  # 400
REASON_UNAUTHORIZED = "unauthorized"  # 401
REASON_NOT_FOUND = "not_found"  # 404 (update endpoint)
REASON_UNPROCESSABLE = "unprocessable_entity"  # 422


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_decimal(value: Any) -> Optional[Decimal]:
    """Parse a numeric into Decimal.

    Supports:
      - {"amount": "...", "currency": "..."} dict
      - str/int/float/Decimal
    """
    if value is None:
        return None

    if isinstance(value, dict):
        value = value.get("amount")

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (int, float)):
        return Decimal(str(value))

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return Decimal(s)
        except InvalidOperation:
            return None

    return None


def _fmt_money(d: Decimal) -> str:
    q = d.quantize(TWOPLACES, rounding=ROUND_HALF_UP)
    return format(q, "f")


def compute_net_price(gross_price_to_win: Decimal) -> Decimal:
    """Compute a net price from a gross price.

    Net rule (per spec):
      net = gross + (gross * 0.10) + 10.90

    Floored at 0.00.
    """
    net = gross_price_to_win + (gross_price_to_win * FEE_RATE) + DELIVERY_FEE_GBP
    if net < Decimal("0.00"):
        net = Decimal("0.00")
    return net.quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def build_tradein_update_payload(*, market: str, currency: str, amount: Decimal) -> Dict[str, Any]:
    """Build the buyback price update payload expected by Back Market."""
    mkt = str(market).upper().strip()
    cur = str(currency).upper().strip()
    return {"prices": {mkt: {"amount": _fmt_money(amount), "currency": cur}}}


@dataclass(frozen=True)
class TradeinRef:
    trade_sku: str
    tradein_id: str


@dataclass(frozen=True)
class TradeinCompetitorSnapshot:
    tradein_id: str
    market: str
    currency: str
    missing: bool
    fetched_at: datetime

    # From competitor response
    current_price: Optional[Decimal] = None
    gross_price_to_win: Optional[Decimal] = None
    is_winning: Optional[bool] = None

    # Derived
    net_price_to_win: Optional[Decimal] = None

    # Keep raw entry (GB row only) for debugging
    raw: Optional[Dict[str, Any]] = None

    # Error metadata (when non-200 or parse failure)
    error_status: Optional[int] = None
    error_body_prefix: Optional[str] = None
    cf_ray: Optional[str] = None


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


def _is_terminal_competitor_status(status: Optional[int]) -> bool:
    """Return True if competitors status is terminal (do not retry)."""
    if status is None:
        return False
    return int(status) in (400, 401, 422)


async def _iter_tradein_refs_from_pricing_groups(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str,
    limit: Optional[int],
) -> List[TradeinRef]:
    """Load (trade_sku, tradein_id) pairs from pricing_groups.

    Market filter:
      pricing_groups has not been fully consistent historically: market info may live in either:
        - pricing_groups.markets (top-level)
        - pricing_groups.tradein_listing.markets (nested)

    To avoid "found 0" when the schema differs, we:
      1) try a market-filtered query
      2) if it yields zero, fall back to a query without market filtering
    """
    mkt = str(market).upper().strip() or DEFAULT_MARKET

    q_base: Dict[str, Any] = {
        "user_id": user_id,
        "tradein_listing.tradein_id": {"$exists": True, "$ne": None},
    }

    q_market: Dict[str, Any] = dict(q_base)
    q_market["$or"] = [
        {"markets": mkt},
        {"tradein_listing.markets": mkt},
    ]

    proj = {"trade_sku": 1, "tradein_listing.tradein_id": 1}

    async def _run_query(q: Dict[str, Any]) -> List[TradeinRef]:
        out: List[TradeinRef] = []
        cursor = db["pricing_groups"].find(q, projection=proj)
        async for doc in cursor:
            trade_sku = doc.get("trade_sku")
            tradein_id = (doc.get("tradein_listing") or {}).get("tradein_id")

            if not isinstance(trade_sku, str) or not trade_sku:
                continue
            if not isinstance(tradein_id, str) or not tradein_id:
                continue

            out.append(TradeinRef(trade_sku=trade_sku, tradein_id=tradein_id))

            if limit is not None and len(out) >= int(limit):
                break
        return out

    refs = await _run_query(q_market)
    if not refs:
        logger.warning(
            "[tradein_refs] market-filter returned 0 rows; falling back to unfiltered query user_id=%s market=%s",
            user_id,
            mkt,
        )
        refs = await _run_query(q_base)

    return refs


async def set_tradein_price_to_one(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    tradein_id: str,
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
) -> Tuple[bool, int, Optional[str], Optional[str]]:
    """PUT /ws/buyback/v1/listings/{id} forcing price to £1.00.

    STRICT success rule: only 202 is success.
    """
    client = await get_bm_client_for_user(db, user_id)

    try:
        resp = await client.request(
            "PUT",
            f"/ws/buyback/v1/listings/{tradein_id}",
            endpoint_key="tradein_update",
            json_body=build_tradein_update_payload(market=market, currency=currency, amount=FORCED_MIN_PRICE_GBP),
            raise_for_status=False,
        )
    except BMRateLimited as exc:
        # Circuit breaker open: treat as transient failure (status 0) so the batch continues.
        return False, 0, f"BREAKER_OPEN: {exc!s}", None
    except BMMaxRetriesError as exc:
        return False, 0, f"MAX_RETRIES: {exc!s}", None

    status = int(resp.status_code)
    cf_ray = parse_cf_ray(resp)

    if status == 202:
        return True, status, None, cf_ray

    return False, status, safe_text_prefix(resp, limit=2000), cf_ray


def _pick_market_entry(data: List[Any], *, tradein_id: str, market: str) -> Optional[Dict[str, Any]]:
    """Pick a single market entry from a competitors response list."""
    mkt = str(market).upper().strip() or DEFAULT_MARKET

    for item in data:
        if (
            isinstance(item, dict)
            and str(item.get("listing_id")) == tradein_id
            and str(item.get("market") or "").upper().strip() == mkt
        ):
            return item

    for item in data:
        if isinstance(item, dict) and str(item.get("market") or "").upper().strip() == mkt:
            return item

    return None


async def fetch_tradein_competitor_snapshot_once(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    tradein_id: str,
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
) -> TradeinCompetitorSnapshot:
    """Single attempt: GET /ws/buyback/v1/competitors/{tradein_id}.

    IMPORTANT: No fast 404 polling here.
    Retries happen at the round level with a large wait.
    """
    client = await get_bm_client_for_user(db, user_id)

    mkt = str(market).upper().strip() or DEFAULT_MARKET
    cur = str(currency).upper().strip() or DEFAULT_CURRENCY

    try:
        resp = await client.request(
            "GET",
            f"/ws/buyback/v1/competitors/{tradein_id}",
            endpoint_key="tradein_competitors",
            raise_for_status=False,
        )
    except BMRateLimited as exc:
        return TradeinCompetitorSnapshot(
            tradein_id=tradein_id,
            market=mkt,
            currency=cur,
            missing=True,
            fetched_at=_now_utc(),
            error_status=0,
            error_body_prefix=f"BREAKER_OPEN: {exc!s}",
        )
    except BMMaxRetriesError as exc:
        return TradeinCompetitorSnapshot(
            tradein_id=tradein_id,
            market=mkt,
            currency=cur,
            missing=True,
            fetched_at=_now_utc(),
            error_status=0,
            error_body_prefix=f"MAX_RETRIES: {exc!s}",
        )

    status = int(resp.status_code)
    cf_ray = parse_cf_ray(resp)

    if status == 404:
        return TradeinCompetitorSnapshot(
            tradein_id=tradein_id,
            market=mkt,
            currency=cur,
            missing=True,
            fetched_at=_now_utc(),
            error_status=404,
            error_body_prefix=safe_text_prefix(resp, limit=2000),
            cf_ray=cf_ray,
        )

    if status != 200:
        return TradeinCompetitorSnapshot(
            tradein_id=tradein_id,
            market=mkt,
            currency=cur,
            missing=True,
            fetched_at=_now_utc(),
            error_status=status,
            error_body_prefix=safe_text_prefix(resp, limit=2000),
            cf_ray=cf_ray,
        )

    # 200: parse JSON list
    try:
        data = resp.json()
    except ValueError:
        return TradeinCompetitorSnapshot(
            tradein_id=tradein_id,
            market=mkt,
            currency=cur,
            missing=True,
            fetched_at=_now_utc(),
            error_status=200,
            error_body_prefix="JSON_DECODE_ERROR: " + (safe_text_prefix(resp, limit=500) or ""),
            cf_ray=cf_ray,
        )

    if not isinstance(data, list):
        return TradeinCompetitorSnapshot(
            tradein_id=tradein_id,
            market=mkt,
            currency=cur,
            missing=True,
            fetched_at=_now_utc(),
            error_status=200,
            error_body_prefix=f"UNEXPECTED_JSON_TYPE: {type(data).__name__}",
            cf_ray=cf_ray,
        )

    entry = _pick_market_entry(data, tradein_id=tradein_id, market=mkt)
    if entry is None:
        return TradeinCompetitorSnapshot(
            tradein_id=tradein_id,
            market=mkt,
            currency=cur,
            missing=True,
            fetched_at=_now_utc(),
            error_status=200,
            error_body_prefix="NO_MARKET_ENTRY",
            cf_ray=cf_ray,
        )

    current_price = _to_decimal(entry.get("price"))
    price_to_win = _to_decimal(entry.get("price_to_win"))
    is_winning = bool(entry.get("is_winning", False)) if "is_winning" in entry else None

    if price_to_win is None:
        return TradeinCompetitorSnapshot(
            tradein_id=tradein_id,
            market=mkt,
            currency=cur,
            missing=True,
            fetched_at=_now_utc(),
            current_price=current_price,
            gross_price_to_win=None,
            net_price_to_win=None,
            is_winning=is_winning,
            raw=entry,
            error_status=200,
            error_body_prefix="MISSING_PRICE_TO_WIN",
            cf_ray=cf_ray,
        )

    net_price = compute_net_price(price_to_win)

    return TradeinCompetitorSnapshot(
        tradein_id=tradein_id,
        market=mkt,
        currency=cur,
        missing=False,
        fetched_at=_now_utc(),
        current_price=current_price,
        gross_price_to_win=price_to_win,
        net_price_to_win=net_price,
        is_winning=is_winning,
        raw=entry,
        cf_ray=cf_ray,
    )


def _snapshot_to_latest_doc(snap: TradeinCompetitorSnapshot) -> Dict[str, Any]:
    def d2f(x: Optional[Decimal]) -> Optional[float]:
        return float(x) if x is not None else None

    return {
        "tradein_id": snap.tradein_id,
        "market": snap.market,
        "currency": snap.currency,
        "missing": bool(snap.missing),
        "fetched_at": snap.fetched_at,
        "is_winning": snap.is_winning,
        "current_price": d2f(snap.current_price),
        "gross_price_to_win": d2f(snap.gross_price_to_win),
        "net_price_to_win": d2f(snap.net_price_to_win),
        "error_status": snap.error_status,
        "error_body_prefix": snap.error_body_prefix,
    }


def _snapshot_to_history_doc(snap: TradeinCompetitorSnapshot) -> Dict[str, Any]:
    def d2f(x: Optional[Decimal]) -> Optional[float]:
        return float(x) if x is not None else None

    return {
        "fetched_at": snap.fetched_at,
        "market": snap.market,
        "currency": snap.currency,
        "missing": bool(snap.missing),
        "is_winning": snap.is_winning,
        "current_price": d2f(snap.current_price),
        "gross_price_to_win": d2f(snap.gross_price_to_win),
        "net_price_to_win": d2f(snap.net_price_to_win),
        "error_status": snap.error_status,
    }


def _reason_code_for_unavailable_snapshot(snap: TradeinCompetitorSnapshot) -> str:
    if snap.error_status == 404:
        return "competitors_404"
    if snap.error_status in (400, 401, 422):
        return f"competitors_{snap.error_status}"
    if snap.error_body_prefix:
        if snap.error_body_prefix.startswith("BREAKER_OPEN"):
            return "breaker_open"
        if snap.error_body_prefix.startswith("MAX_RETRIES"):
            return "max_retries"
        if snap.error_body_prefix == "MISSING_PRICE_TO_WIN":
            return "missing_price_to_win"
        if snap.error_body_prefix == "NO_MARKET_ENTRY":
            return "no_market_entry"
    if snap.error_status is None:
        return "unknown"
    return f"status_{int(snap.error_status)}"


def _chunked(items: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


# ---------------------------------------------------------------------------
# Stage 1: set ALL to £1
# ---------------------------------------------------------------------------

async def stage1_set_all_to_one(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
    concurrency: int = 10,
    limit: Optional[int] = None,
    include_results: bool = False,
) -> Tuple[Dict[str, Any], List[TradeinRef]]:
    """Stage 1: scan pricing_groups, set each tradein_id to £1.00."""
    start = time.perf_counter()
    mkt = str(market).upper().strip() or DEFAULT_MARKET
    cur = str(currency).upper().strip() or DEFAULT_CURRENCY
    max_conc = max(1, int(concurrency))

    # Respect user-disabled trade-in listings.
    # Convention: if the mirrored buyback listing GB price is 0, the user has
    # intentionally disabled it (Back Market treats 0 as "off").
    # When we run Stage 1 (set-to-one) to fetch competitors, we must NOT
    # re-enable those listings.
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
    except Exception:  # noqa: BLE001
        # Be defensive: if this lookup fails, do NOT break the run.
        # Worst case we set-to-one as before.
        logger.exception("[tradein_stage1] failed to load disabled trade-ins user_id=%s", user_id)


    # Skip trade-ins that are known hard failures.
    skip_ids: Set[str] = await get_bad_tradein_ids_for_user(
        db,
        user_id=user_id,
        reason_codes=[REASON_BAD_REQUEST, REASON_UNAUTHORIZED, REASON_NOT_FOUND, REASON_UNPROCESSABLE],
    )

    refs = await _iter_tradein_refs_from_pricing_groups(db, user_id=user_id, market=mkt, limit=limit)

    processed = 0
    skipped = 0
    ok_count = 0
    failed_count = 0
    skipped_disabled = 0


    ok_refs: List[TradeinRef] = []
    results: List[Dict[str, Any]] = []

    async def _run_batch(batch_refs: List[TradeinRef]) -> None:
        nonlocal ok_count, failed_count

        outs = await asyncio.gather(
            *[
                set_tradein_price_to_one(
                    db,
                    user_id=user_id,
                    tradein_id=batch_ref.tradein_id,
                    market=mkt,
                    currency=cur,
                )
                for batch_ref in batch_refs
            ],
            return_exceptions=True,
        )

        for batch_ref, out in zip(batch_refs, outs):
            if isinstance(out, Exception):
                failed_count += 1
                logger.exception("[tradein_stage1] update crashed tradein_id=%s", batch_ref.tradein_id, exc_info=out)
                if include_results:
                    results.append(
                        {
                            "trade_sku": batch_ref.trade_sku,
                            "tradein_id": batch_ref.tradein_id,
                            "ok": False,
                            "status": None,
                            "stage": "set_to_one",
                            "error": repr(out),
                        }
                    )
                continue

            ok, status, body, cf_ray = out
            if ok:
                ok_count += 1
                ok_refs.append(batch_ref)
            else:
                failed_count += 1

                reason = _hard_failure_reason_for_update_status(int(status))
                if reason is not None:
                    await upsert_bad_tradein(
                        db,
                        user_id=user_id,
                        tradein_id=batch_ref.tradein_id,
                        reason_code=reason,
                        status_code=int(status),
                        detail=body,
                    )

            if include_results:
                results.append(
                    {
                        "trade_sku": batch_ref.trade_sku,
                        "tradein_id": batch_ref.tradein_id,
                        "ok": bool(ok),
                        "status": int(status),
                        "cf_ray": cf_ray,
                        "stage": "set_to_one",
                    }
                )

    batch: List[TradeinRef] = []
    for trade_ref in refs:
        # Never touch user-disabled trade-ins (gb_amount <= 0 in bm_tradein_listings).
        if trade_ref.tradein_id in disabled_ids:
            skipped_disabled += 1
            continue

        # Skip trade-ins that are known hard failures.
        if trade_ref.tradein_id in skip_ids:
            skipped += 1
            continue

        batch.append(trade_ref)
        processed += 1

        if len(batch) >= max_conc:
            await _run_batch(batch)
            batch = []

    if batch:
        await _run_batch(batch)

    elapsed = time.perf_counter() - start

    summary: Dict[str, Any] = {
        "stage": "set_to_one",
        "user_id": user_id,
        "market": mkt,
        "currency": cur,
        "found": len(refs),
        "processed": processed,
        "skipped_hard_failed": skipped,
        "skipped_disabled": skipped_disabled,
        "ok": ok_count,
        "failed": failed_count,
        "elapsed_seconds": elapsed,
    }
    if include_results:
        summary["results"] = results

    return summary, ok_refs


# ---------------------------------------------------------------------------
# Stage 2: fetch competitors + persist gross/net (single round)
# ---------------------------------------------------------------------------

async def stage2_fetch_competitors_once(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    tradeins: List[TradeinRef],
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
    concurrency: int = 10,
    include_results: bool = False,
    round_number: int = 1,
) -> Tuple[Dict[str, Any], List[TradeinRef], List[TradeinRef], Dict[str, TradeinCompetitorSnapshot]]:
    """Stage 2 - single round competitor fetch + persist."""
    start = time.perf_counter()
    mkt = str(market).upper().strip() or DEFAULT_MARKET
    cur = str(currency).upper().strip() or DEFAULT_CURRENCY
    max_conc = max(1, int(concurrency))

    processed = 0
    persisted_ok = 0
    missing_count = 0
    persist_failed = 0
    retry_count = 0
    terminal_count = 0

    results: List[Dict[str, Any]] = []

    retry_refs: List[TradeinRef] = []
    terminal_refs: List[TradeinRef] = []
    snaps_by_id: Dict[str, TradeinCompetitorSnapshot] = {}

    for batch in _chunked(tradeins, max_conc):
        processed += len(batch)

        snaps = await asyncio.gather(
            *[
                fetch_tradein_competitor_snapshot_once(
                    db,
                    user_id=user_id,
                    tradein_id=batch_ref.tradein_id,
                    market=mkt,
                    currency=cur,
                )
                for batch_ref in batch
            ],
            return_exceptions=True,
        )

        for batch_ref, s in zip(batch, snaps):
            if isinstance(s, Exception):
                # Keep defensive so a single unexpected exception can't kill the batch.
                snap = TradeinCompetitorSnapshot(
                    tradein_id=batch_ref.tradein_id,
                    market=mkt,
                    currency=cur,
                    missing=True,
                    fetched_at=_now_utc(),
                    error_status=0,
                    error_body_prefix=f"UNCAUGHT_EXCEPTION: {repr(s)}",
                )
            else:
                snap = s

            snaps_by_id[batch_ref.tradein_id] = snap

            latest_doc = _snapshot_to_latest_doc(snap)
            history_doc = _snapshot_to_history_doc(snap)

            try:
                await persist_tradein_competitor_snapshot(
                    db,
                    user_id=user_id,
                    trade_sku=batch_ref.trade_sku,
                    snapshot_latest=latest_doc,
                    snapshot_history=history_doc,
                )
                persisted_ok += 1
            except PyMongoError as exc:
                persist_failed += 1
                logger.exception(
                    "[tradein_stage2] persist failed trade_sku=%s tradein_id=%s",
                    batch_ref.trade_sku,
                    batch_ref.tradein_id,
                    exc_info=exc,
                )
                if include_results:
                    results.append(
                        {
                            "trade_sku": batch_ref.trade_sku,
                            "tradein_id": batch_ref.tradein_id,
                            "ok": False,
                            "stage": "persist",
                            "error": repr(exc),
                            "round": round_number,
                        }
                    )
                # If we couldn't persist, we can't reliably mark it as done/retry.
                # Treat it as terminal for this run.
                terminal_refs.append(batch_ref)
                terminal_count += 1
                continue

            if snap.missing:
                missing_count += 1
                if _is_terminal_competitor_status(snap.error_status):
                    terminal_refs.append(batch_ref)
                    terminal_count += 1
                else:
                    retry_refs.append(batch_ref)
                    retry_count += 1

            if include_results:
                results.append(
                    {
                        "trade_sku": batch_ref.trade_sku,
                        "tradein_id": batch_ref.tradein_id,
                        "ok": True,
                        "missing": bool(snap.missing),
                        "gross_price_to_win": float(snap.gross_price_to_win)
                        if snap.gross_price_to_win is not None
                        else None,
                        "net_price_to_win": float(snap.net_price_to_win)
                        if snap.net_price_to_win is not None
                        else None,
                        "is_winning": snap.is_winning,
                        "error_status": snap.error_status,
                        "cf_ray": snap.cf_ray,
                        "stage": "fetch_competitors",
                        "round": round_number,
                    }
                )

    elapsed = time.perf_counter() - start

    out: Dict[str, Any] = {
        "stage": "fetch_competitors",
        "user_id": user_id,
        "market": mkt,
        "currency": cur,
        "round": int(round_number),
        "requested": len(tradeins),
        "processed": processed,
        "persisted": persisted_ok,
        "missing": missing_count,
        "retrying": retry_count,
        "terminal": terminal_count,
        "failed": persist_failed,
        "elapsed_seconds": elapsed,
    }
    if include_results:
        out["results"] = results



    return out, retry_refs, terminal_refs, snaps_by_id


# ---------------------------------------------------------------------------
# Stage 2: multi-round fetch (round waits) + unresolved collection
# ---------------------------------------------------------------------------

async def stage2_fetch_competitors_with_retries(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    tradeins: List[TradeinRef],
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
    concurrency: int = 10,
    include_results: bool = False,
    fetch_attempts: int = 3,
    retry_wait_seconds: int = 60,
) -> Dict[str, Any]:
    """Multi-round competitor fetch with round-level waits."""
    started = time.perf_counter()
    mkt = str(market).upper().strip() or DEFAULT_MARKET
    cur = str(currency).upper().strip() or DEFAULT_CURRENCY

    attempts = max(1, int(fetch_attempts))
    wait_s = max(0, int(retry_wait_seconds))

    rounds: List[Dict[str, Any]] = []
    last_snap_by_id: Dict[str, TradeinCompetitorSnapshot] = {}

    # Terminal items we should NOT retry, but still want to record as unresolved.
    terminal_by_id: Dict[str, TradeinRef] = {}

    pending: List[TradeinRef] = list(tradeins)

    for round_no in range(1, attempts + 1):
        if not pending:
            break

        summary, retry_refs, terminal_refs, snaps_by_id = await stage2_fetch_competitors_once(
            db,
            user_id=user_id,
            tradeins=pending,
            market=mkt,
            currency=cur,
            concurrency=concurrency,
            include_results=include_results,
            round_number=round_no,
        )
        rounds.append(summary)
        last_snap_by_id.update(snaps_by_id)

        for terminal_ref in terminal_refs:
            terminal_by_id[terminal_ref.tradein_id] = terminal_ref

        pending = retry_refs

        # Wait between rounds (but not after the last round).
        if pending and round_no < attempts and wait_s > 0:
            await asyncio.sleep(float(wait_s))

    unresolved: Dict[str, TradeinRef] = {**terminal_by_id, **{r.tradein_id: r for r in pending}}

    # Upsert unresolved into a separate collection for manual review.
    unresolved_saved = 0
    if unresolved:
        for unresolved_ref in unresolved.values():
            snap = last_snap_by_id.get(unresolved_ref.tradein_id)
            if snap is None:
                snap = TradeinCompetitorSnapshot(
                    tradein_id=unresolved_ref.tradein_id,
                    market=mkt,
                    currency=cur,
                    missing=True,
                    fetched_at=_now_utc(),
                    error_status=0,
                    error_body_prefix="MISSING_SNAPSHOT",
                )

            try:
                await upsert_unavailable_tradein_competitor(
                    db,
                    user_id=user_id,
                    trade_sku=unresolved_ref.trade_sku,
                    tradein_id=unresolved_ref.tradein_id,
                    market=mkt,
                    currency=cur,
                    reason_code=_reason_code_for_unavailable_snapshot(snap),
                    status_code=int(snap.error_status or 0),
                    detail=snap.error_body_prefix,
                    cf_ray=snap.cf_ray,
                    attempts=len(rounds),
                )
                unresolved_saved += 1
            except PyMongoError as exc:
                logger.exception(
                    "[tradein_stage2] unable-to-price upsert failed trade_sku=%s tradein_id=%s",
                    unresolved_ref.trade_sku,
                    unresolved_ref.tradein_id,
                    exc_info=exc,
                )

    elapsed = time.perf_counter() - started

    # Derive end-state stats (unique counts).
    total_targets = len(tradeins)
    unresolved_count = len(unresolved)
    resolved_count = total_targets - unresolved_count

    return {
        "stage": "fetch_competitors",
        "user_id": user_id,
        "market": mkt,
        "currency": cur,
        "targets": total_targets,
        "resolved": resolved_count,
        "unresolved": unresolved_count,
        "unresolved_saved": unresolved_saved,
        "attempts_configured": attempts,
        "rounds_executed": len(rounds),
        "retry_wait_seconds": wait_s,
        "rounds": rounds,
        "elapsed_seconds": elapsed,
    }


# ---------------------------------------------------------------------------
# Full run: stage1 -> wait -> stage2
# ---------------------------------------------------------------------------

async def run_tradein_competitor_refresh_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str = DEFAULT_MARKET,
    currency: str = DEFAULT_CURRENCY,
    update_concurrency: int = 10,
    fetch_concurrency: int = 10,
    wait_seconds: int = 60,
    fetch_attempts: int = 3,
    limit: Optional[int] = None,
    include_stage_results: bool = False,
    include_item_results: bool = False,
) -> Dict[str, Any]:
    """Full two-stage run: set-to-one -> wait -> fetch competitors with retries."""
    started = _now_utc()
    mkt = str(market).upper().strip() or DEFAULT_MARKET
    cur = str(currency).upper().strip() or DEFAULT_CURRENCY

    s1, ok_refs = await stage1_set_all_to_one(
        db,
        user_id=user_id,
        market=mkt,
        currency=cur,
        concurrency=update_concurrency,
        limit=limit,
        include_results=include_item_results,
    )

    ws = max(0, int(wait_seconds))
    if ok_refs and ws > 0:
        await asyncio.sleep(float(ws))

    s2 = await stage2_fetch_competitors_with_retries(
        db,
        user_id=user_id,
        tradeins=ok_refs,
        market=mkt,
        currency=cur,
        concurrency=fetch_concurrency,
        include_results=include_item_results,
        fetch_attempts=fetch_attempts,
        retry_wait_seconds=ws,
    )

    # Recompute trade pricing once competitor snapshots are persisted (best-effort).
    if ok_refs:
        trade_skus = sorted({r.trade_sku for r in ok_refs if isinstance(r.trade_sku, str) and r.trade_sku})
        if trade_skus:
            try:
                await recompute_trade_pricing_for_user(db, user_id, trade_skus=trade_skus)
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "[tradein_competitors] trade_pricing_recompute_failed user_id=%s err=%r",
                    user_id,
                    exc,
                )

    out: Dict[str, Any] = {
        "user_id": user_id,
        "market": mkt,
        "currency": cur,
        "wait_seconds": ws,
        "fetch_attempts": int(max(1, int(fetch_attempts))),
        "started_at": started.isoformat(),
        "finished_at": _now_utc().isoformat(),
        "stage1": s1
        if include_stage_results or include_item_results
        else {k: v for k, v in s1.items() if k != "results"},
        "stage2": s2
        if include_stage_results or include_item_results
        else {k: v for k, v in s2.items() if k != "rounds"},
    }
    if include_stage_results or include_item_results:
        out["stage2"] = s2

    return out


def hard_failure_reason_code(status: int) -> Optional[str]:
    """Public helper to map HTTP status -> hard-failure reason_code.

    Used by offers_service and other callers.
    """
    try:
        return _hard_failure_reason_for_update_status(int(status))
    except Exception:  # noqa: BLE001
        return None

