from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Any, Mapping

from motor.motor_asyncio import AsyncIOMotorDatabase

# ---------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------
# Generic coercion / dict helpers (used across pricing modules)
# ---------------------------------------------------------------------


def is_bool(v: Any) -> bool:
    # bool is a subclass of int; avoid silent coercion
    return isinstance(v, bool)


def to_float(v: Any) -> float | None:
    try:
        if v is None or is_bool(v):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def to_pos_float(v: Any) -> float | None:
    f = to_float(v)
    if f is None or f <= 0:
        return None
    return f


def to_rate(v: Any) -> float | None:
    """Accept 0.2 or 20 (normalize to 0.2). Reject bools and negatives."""
    f = to_float(v)
    if f is None:
        return None
    if 1.0 < f <= 100.0:
        f = f / 100.0
    if f < 0:
        return None
    return f


def get_nested(d: Mapping[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for key in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return cur if cur is not None else default


# ---------------------------------------------------------------------
# Money helpers (GB trade-in landed cost model + rounding)
# ---------------------------------------------------------------------

TWOPLACES = Decimal("0.01")

# Trade-in landed cost model (GB):
#   net = gross + (gross * 0.10) + 10.90
TRADEIN_FEE_RATE = Decimal("0.10")
TRADEIN_DELIVERY_FEE_GBP = Decimal("10.90")


def round2(x: float) -> float:
    return float(Decimal(str(x)).quantize(TWOPLACES, rounding=ROUND_HALF_UP))


def net_to_gross_tradein_gbp(net: float) -> float | None:
    """Invert net = gross + gross*0.10 + 10.90 => gross = (net - 10.90)/1.10."""
    try:
        d = Decimal(str(net))
    except InvalidOperation:
        return None

    denom = Decimal("1.00") + TRADEIN_FEE_RATE
    gross = (d - TRADEIN_DELIVERY_FEE_GBP) / denom
    if gross < Decimal("0.00"):
        gross = Decimal("0.00")
    return float(gross.quantize(TWOPLACES, rounding=ROUND_HALF_UP))


def gross_to_net_tradein_gbp(gross: float) -> float | None:
    """Forward net = gross + gross*0.10 + 10.90 (GB trade-in landed cost)."""
    try:
        d = Decimal(str(gross))
    except InvalidOperation:
        return None

    net = d + (d * TRADEIN_FEE_RATE) + TRADEIN_DELIVERY_FEE_GBP
    if net < Decimal("0.00"):
        net = Decimal("0.00")
    return float(net.quantize(TWOPLACES, rounding=ROUND_HALF_UP))


def floor_to_pounds(x: float) -> float | None:
    """Round DOWN to the nearest whole pound (<= x)."""
    try:
        d = Decimal(str(x))
    except InvalidOperation:
        return None
    return float(d.to_integral_value(rounding=ROUND_FLOOR))


def ceil_to_pounds(x: float) -> float | None:
    """Round UP to the nearest whole pound (>= x)."""
    try:
        d = Decimal(str(x))
    except InvalidOperation:
        return None
    return float(d.to_integral_value(rounding=ROUND_CEILING))


def normalize_competitor_tradein_price(
    *,
    currency: str,
    competitor_gross: float | None,
    competitor_net: float | None,
    min_gross_gbp: float = 10.0,
) -> tuple[float | None, float | None]:
    """Normalise BM competitor price-to-win anomalies for GBP.

    BM sometimes returns very small gross price_to_win (e.g. £2/£3).
    Treat anything under `min_gross_gbp` as equivalent to £1 ("no competitor")
    so downstream logic follows the correct path.
    """
    if str(currency or "").upper() != "GBP" or competitor_gross is None:
        return competitor_gross, competitor_net

    try:
        g = float(competitor_gross)
    except (TypeError, ValueError):
        g = 1.0

    if g < float(min_gross_gbp):
        return 1.0, gross_to_net_tradein_gbp(1.0)

    return competitor_gross, competitor_net


def compute_gb_tradein_update_price(
    *,
    max_buy_gross: float | None,
    competitor_gross: float | None,
    no_competitor_threshold_gross: float = 20.01,
) -> dict[str, Any]:
    """Compute the final GBP update price (whole £) for a scenario.

    Mirrors the logic previously in trade_pricing_service:
    - cap at floor(max_buy_gross) to keep profit-safe and whole-£
    - treat competitor <= threshold (or missing) as "no competitor"
    - otherwise update to ceil(competitor), capped at cap
    """
    max_trade_offer_gross: float | None = None
    competitor_to_win_rounded_gross: float | None = None
    final_update_price_gross: float | None = None
    final_update_reason: str | None = None

    if max_buy_gross is None:
        return {
            "max_trade_offer_gross": None,
            "competitor_to_win_rounded_gross": None,
            "final_update_price_gross": None,
            "final_update_reason": None,
        }

    cap = floor_to_pounds(float(max_buy_gross))
    if cap is None or cap < 1.0:
        return {
            "max_trade_offer_gross": None,
            "competitor_to_win_rounded_gross": None,
            "final_update_price_gross": None,
            "final_update_reason": "max_trade_below_min_price",
        }

    max_trade_offer_gross = cap

    try:
        comp = float(competitor_gross) if competitor_gross is not None else None
    except (TypeError, ValueError):
        comp = None

    # BM returns ~£1 when no competitor; treat <= threshold as no competitor
    if comp is None or comp <= float(no_competitor_threshold_gross):
        final_update_price_gross = cap
        final_update_reason = "no_competitor"
        return {
            "max_trade_offer_gross": max_trade_offer_gross,
            "competitor_to_win_rounded_gross": None,
            "final_update_price_gross": final_update_price_gross,
            "final_update_reason": final_update_reason,
        }

    competitor_to_win_rounded_gross = ceil_to_pounds(comp)
    if competitor_to_win_rounded_gross is not None and competitor_to_win_rounded_gross <= cap:
        final_update_price_gross = competitor_to_win_rounded_gross
        final_update_reason = "to_win"
    else:
        final_update_price_gross = cap
        final_update_reason = "capped_at_max_trade"

    return {
        "max_trade_offer_gross": max_trade_offer_gross,
        "competitor_to_win_rounded_gross": competitor_to_win_rounded_gross,
        "final_update_price_gross": final_update_price_gross,
        "final_update_reason": final_update_reason,
    }


def build_trade_pricing_item_result(
    *,
    group_id: str,
    trade_sku: Any,
    computed: Mapping[str, Any],
) -> dict[str, Any]:
    """Standard per-group result payload used by recompute endpoints/jobs."""
    return {
        "group_id": group_id,
        "trade_sku": trade_sku,
        "best_scenario_key": computed.get("best_scenario_key"),
        "max_trade_price_net": computed.get("max_trade_price_net"),
        "max_trade_price_gross": computed.get("max_trade_price_gross"),
        "max_trade_offer_gross": computed.get("max_trade_offer_gross"),
        "final_update_price_gross": computed.get("final_update_price_gross"),
        "final_update_reason": computed.get("final_update_reason"),
        "ok_to_update": computed.get("ok_to_update"),
        "not_ok_reasons": computed.get("not_ok_reasons"),
        "guardrails": computed.get("guardrails"),
    }


# ---------------------------------------------------------------------
# Existing helpers you already introduced earlier
# ---------------------------------------------------------------------


def normalize_computed_payload(trade_pricing: Mapping[str, Any]) -> dict[str, Any]:
    """Accept either a full trade_pricing object or the computed payload itself.

    - Newer code stores the calculated output under: pricing_groups.trade_pricing.computed
    - Some call sites historically passed the computed payload directly.
    """
    computed = trade_pricing.get("computed")
    if isinstance(computed, dict):
        return dict(computed)
    return dict(trade_pricing)


async def update_one_set_with_updated_at(
    db: AsyncIOMotorDatabase,
    *,
    collection: str,
    filter_doc: Mapping[str, Any],
    set_doc: Mapping[str, Any],
    now: datetime | None = None,
) -> int:
    """Update a document using $set and always include updated_at.

    Returns matched_count as an int (0 if no document matched).
    """
    ts = now or utc_now()
    res = await db[collection].update_one(
        dict(filter_doc),
        {"$set": {**dict(set_doc), "updated_at": ts}},
    )
    return int(getattr(res, "matched_count", 0))

