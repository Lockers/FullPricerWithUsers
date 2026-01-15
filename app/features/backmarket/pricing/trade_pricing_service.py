from __future__ import annotations

"""
Trade pricing calculations (max allowable buy price).

This module computes and persists trade-in pricing guidance onto each pricing_groups doc.

Core inputs:
- Net sell anchor per condition (sell_anchor.net_used preferred; else sell_anchor.lowest_net_sell_price)
- Repair costs (pricing_groups.repair_costs.costs)
- Required profit (per pricing_group override; else user default; else £75)
- VAT rate (user default; else 0.1667)

Key behavior:
- Scenarios like GOOD->EXCELLENT or FAIR->EXCELLENT MUST use the EXCELLENT sell anchor
  (not the group's own sell anchor if the group itself is GOOD/FAIR).
- Battery is assumed to be changed on every device. If no repair profile exists yet,
  battery defaults to £15 so we can still compute baseline scenarios.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import ValidationError
from pymongo.errors import PyMongoError

from app.features.backmarket.pricing.trade_pricing_models import (
    TradePricingGroupSettingsIn,
    TradePricingSettings,
    TradePricingSettingsIn,
)
from app.features.backmarket.transport.exceptions import BMClientError

logger = logging.getLogger(__name__)

PRICING_GROUPS_COL = "pricing_groups"
USER_SETTINGS_COL = "user_settings"

DEFAULT_REQUIRED_PROFIT_GBP = 75.0
DEFAULT_VAT_RATE = 0.1667

# If no repair profile exists yet, battery defaults to this so the base scenario can still compute.
DEFAULT_BATTERY_COST_GBP = 15.0

# Trade-in landed cost model (GB)
# NOTE: This mirrors the competitor flow implementation:
#   net = gross + (gross * 0.10) + 10.90
TRADEIN_FEE_RATE = Decimal("0.10")
TRADEIN_DELIVERY_FEE_GBP = Decimal("10.90")
TWOPLACES = Decimal("0.01")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _is_bool(v: Any) -> bool:
    # bool is a subclass of int; avoid silent coercion
    return isinstance(v, bool)


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or _is_bool(v):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_pos_float(v: Any) -> Optional[float]:
    f = _to_float(v)
    if f is None or f <= 0:
        return None
    return f


def _to_rate(v: Any) -> Optional[float]:
    """Accept 0.2 or 20 (normalize to 0.2). Reject bools and negatives."""
    f = _to_float(v)
    if f is None:
        return None
    if 1.0 < f <= 100.0:
        f = f / 100.0
    if f < 0:
        return None
    return f


def _round2(x: float) -> float:
    return float(Decimal(str(x)).quantize(TWOPLACES, rounding=ROUND_HALF_UP))


def _net_to_gross_tradein_gbp(net: float) -> Optional[float]:
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


def _gross_to_net_tradein_gbp(gross: float) -> Optional[float]:
    """Forward net = gross + gross*0.10 + 10.90 (GB trade-in landed cost)."""
    try:
        d = Decimal(str(gross))
    except InvalidOperation:
        return None

    net = d + (d * TRADEIN_FEE_RATE) + TRADEIN_DELIVERY_FEE_GBP
    if net < Decimal("0.00"):
        net = Decimal("0.00")
    return float(net.quantize(TWOPLACES, rounding=ROUND_HALF_UP))


def _floor_to_pounds(x: float) -> Optional[float]:
    """Round DOWN to the nearest whole pound (<= x)."""
    try:
        d = Decimal(str(x))
    except InvalidOperation:
        return None
    return float(d.to_integral_value(rounding=ROUND_FLOOR))


def _ceil_to_pounds(x: float) -> Optional[float]:
    """Round UP to the nearest whole pound (>= x)."""
    try:
        d = Decimal(str(x))
    except InvalidOperation:
        return None
    return float(d.to_integral_value(rounding=ROUND_CEILING))


def _parse_object_id(group_id: str) -> ObjectId:
    try:
        return ObjectId(group_id)
    except (InvalidId, TypeError) as exc:
        raise BMClientError(f"Invalid group_id ObjectId: {group_id}") from exc


def _infer_condition_from_trade_sku(trade_sku: Any) -> Optional[str]:
    if not isinstance(trade_sku, str) or not trade_sku.strip():
        return None
    parts = [p for p in trade_sku.strip().split("-") if p]
    if not parts:
        return None
    return parts[-1].strip().upper() or None


def _get_nested(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for key in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return cur if cur is not None else default


def _extract_net_sell_anchor(group: Dict[str, Any]) -> Optional[float]:
    sell_anchor = group.get("sell_anchor") or {}
    return _to_pos_float(sell_anchor.get("net_used")) or _to_pos_float(sell_anchor.get("lowest_net_sell_price"))


def _extract_gross_sell_anchor(group: Dict[str, Any]) -> Optional[float]:
    """Best-effort gross sell anchor used for display/debug.

    Prefer the sell_anchor.gross_used field (the actual anchor chosen by the sell-anchor
    selection logic). Fall back to lowest_gross_price_to_win if present.
    """
    sell_anchor = group.get("sell_anchor") or {}
    return _to_pos_float(sell_anchor.get("gross_used")) or _to_pos_float(sell_anchor.get("lowest_gross_price_to_win"))


def _extract_currency(group: Dict[str, Any]) -> str:
    sell_anchor = group.get("sell_anchor") or {}
    cur = sell_anchor.get("currency") or group.get("currency")
    if isinstance(cur, str) and cur.strip():
        return cur.strip().upper()
    return "GBP"


def _extract_competitor_net_price_to_win(group: Dict[str, Any]) -> Optional[float]:
    return _to_pos_float(_get_nested(group, "tradein_listing.competitor.net_price_to_win"))


def _extract_competitor_gross_price_to_win(group: Dict[str, Any]) -> Optional[float]:
    return _to_pos_float(_get_nested(group, "tradein_listing.competitor.gross_price_to_win"))


REPAIR_KEY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "battery": ("battery", "battery_new"),
    # "full_housing" exists in older docs; treat it as equivalent to "housing".
    "housing": ("housing", "housing_new", "full_housing", "housing_full"),
    # Some older repair profiles only store a single "screen_refurb" cost. When present,
    # use it for both in-house and external refurb scenarios.
    "screen_refurb_in_house": (
        "screen_refurb_in_house",
        "screen_refurb_internal",
        "screen_refurb_inhouse",
        "screen_refurb",
    ),
    "screen_refurb_external": (
        "screen_refurb_external",
        "screen_refurb_outsource",
        "screen_refurb_outsourced",
        "screen_refurb",
    ),
    "screen_replacement_in_house": (
        "screen_replacement_in_house",
        "screen_replacement",
        "screen_replace_in_house",
        "new_screen",
    ),
}


def _normalize_storage_gb(v: Any) -> Optional[str]:
    if v is None or _is_bool(v):
        return None
    if isinstance(v, (int, float)):
        try:
            i = int(v)
        except (TypeError, ValueError):
            return None
        return str(i)
    if isinstance(v, str) and v.strip():
        s = v.strip()
        # Keep numeric strings clean ("128" not "128.0").
        try:
            i = int(float(s))
            return str(i)
        except (TypeError, ValueError):
            return s
    return None


def _extract_device_key(group: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
    """Return a stable identity key for a pricing group: (brand, model, storage_gb).

    We prefer explicit fields, but fall back to parsing group_key if needed.
    """
    brand = group.get("brand")
    model = group.get("model")
    storage_gb = group.get("storage_gb")

    if isinstance(brand, str) and isinstance(model, str):
        storage_norm = _normalize_storage_gb(storage_gb)
        if storage_norm:
            return brand.strip().upper(), model.strip().upper(), storage_norm

    # Fallback: group_key is typically "BRAND|MODEL|STORAGE|TRADEIN_GRADE".
    gk = group.get("group_key")
    if isinstance(gk, str) and gk.strip() and "|" in gk:
        parts = [p.strip() for p in gk.split("|")]
        if len(parts) >= 3 and parts[0] and parts[1] and parts[2]:
            return parts[0].upper(), parts[1].upper(), parts[2]

    return None


def _extract_group_condition(group: Dict[str, Any]) -> Optional[str]:
    """Return the condition this group represents (used for sell-anchor lookup)."""
    for key in ("target_sell_condition", "trade_sku_condition", "sell_condition"):
        v = group.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    return _infer_condition_from_trade_sku(group.get("trade_sku"))


async def _build_sell_anchor_index_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
) -> Dict[Tuple[str, str, str], Dict[str, Dict[str, float]]]:
    """Build {device_key -> {condition -> {net, gross}}} for a user.

    Used so trade pricing scenarios like FAIR->EXCELLENT can use the EXCELLENT sell anchor
    (and for UI/debugging to show the *correct* gross/net sell anchor per scenario).
    """
    projection = {
        "brand": 1,
        "model": 1,
        "storage_gb": 1,
        "group_key": 1,
        "trade_sku": 1,
        "trade_sku_condition": 1,
        "target_sell_condition": 1,
        "sell_condition": 1,
        "sell_anchor": 1,
    }

    out: Dict[Tuple[str, str, str], Dict[str, Dict[str, float]]] = {}
    cur = db[PRICING_GROUPS_COL].find({"user_id": user_id}, projection=projection)
    async for doc in cur:
        dev = _extract_device_key(doc)
        if dev is None:
            continue
        cond = _extract_group_condition(doc)
        if not cond:
            continue
        net = _extract_net_sell_anchor(doc)
        if net is None:
            continue
        gross = _extract_gross_sell_anchor(doc)
        payload: Dict[str, float] = {"net": float(net)}
        if gross is not None:
            payload["gross"] = float(gross)
        out.setdefault(dev, {})[cond] = payload
    return out


# Public wrapper: used by trade-in orphan pricing.
async def build_sell_anchor_index_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
) -> Dict[Tuple[str, str, str], Dict[str, Dict[str, float]]]:
    """Public wrapper around the internal sell anchor index builder."""
    return await _build_sell_anchor_index_for_user(db, user_id=user_id)


def _extract_repair_costs(group: Dict[str, Any]) -> Dict[str, Optional[float]]:
    raw_costs = _get_nested(group, "repair_costs.costs", default={})
    costs: Dict[str, Optional[float]] = {k: None for k in REPAIR_KEY_ALIASES}

    if isinstance(raw_costs, dict):
        for canonical, aliases in REPAIR_KEY_ALIASES.items():
            for key in aliases:
                if key in raw_costs:
                    costs[canonical] = _to_pos_float(raw_costs.get(key))
                    break

    # Battery is always assumed (and is never "zero" per your spec).
    # If no repair profile exists yet, default it so trade pricing still works.
    if costs.get("battery") is None:
        costs["battery"] = float(DEFAULT_BATTERY_COST_GBP)

    return costs


@dataclass(frozen=True)
class TradePricingScenario:
    key: str
    route: str
    repair_cost_keys: Tuple[str, ...]


def _target_sell_conditions_for_buy_condition(buy_condition: str) -> Tuple[str, ...]:
    """
    Required scenarios (per spec):
      - GOOD: GOOD->GOOD and GOOD->EXCELLENT
      - FAIR: FAIR->FAIR and FAIR->EXCELLENT
      - EXCELLENT: EXCELLENT->EXCELLENT
    """
    buy = str(buy_condition or "").upper().strip()
    if not buy:
        return ("UNKNOWN",)

    if buy == "GOOD":
        return "GOOD", "EXCELLENT"
    if buy == "FAIR":
        return "FAIR", "EXCELLENT"
    if buy == "EXCELLENT":
        return ("EXCELLENT",)

    # keep legacy compatibility
    if buy == "USED":
        return "USED", "EXCELLENT"
    if buy == "CRACKED":
        return ("EXCELLENT",)

    return (buy,)


def _build_scenarios_for_route(buy_condition: str, sell_condition: str) -> List[TradePricingScenario]:
    buy = str(buy_condition or "").upper().strip()
    sell = str(sell_condition or "").upper().strip()
    route = f"{buy}->{sell}" if buy and sell else "UNKNOWN"

    if buy == "CRACKED" and sell == "EXCELLENT":
        return [
            TradePricingScenario(
                key="cracked_to_excellent_new_screen_new_housing_new_battery",
                route=route,
                repair_cost_keys=("screen_replacement_in_house", "housing", "battery"),
            ),
            TradePricingScenario(
                key="cracked_to_excellent_screen_refurb_in_house_new_housing_new_battery",
                route=route,
                repair_cost_keys=("screen_refurb_in_house", "housing", "battery"),
            ),
            TradePricingScenario(
                key="cracked_to_excellent_screen_refurb_external_new_housing_new_battery",
                route=route,
                repair_cost_keys=("screen_refurb_external", "housing", "battery"),
            ),
        ]

    # Spec: GOOD->EXCELLENT and FAIR->EXCELLENT are screen refurb (in-house/external)
    # + new housing + new battery.
    if buy in {"USED", "GOOD", "FAIR"} and sell == "EXCELLENT":
        buy_l = buy.lower()
        return [
            TradePricingScenario(
                key=f"{buy_l}_to_excellent_screen_refurb_in_house_new_housing_new_battery",
                route=route,
                repair_cost_keys=("screen_refurb_in_house", "housing", "battery"),
            ),
            TradePricingScenario(
                key=f"{buy_l}_to_excellent_screen_refurb_external_new_housing_new_battery",
                route=route,
                repair_cost_keys=("screen_refurb_external", "housing", "battery"),
            ),
        ]

    # Default: battery always required
    if buy and sell and buy == sell:
        return [TradePricingScenario(key="same_condition_new_battery", route=route, repair_cost_keys=("battery",))]

    return [TradePricingScenario(key="unknown_route_new_battery", route=route, repair_cost_keys=("battery",))]


def _compute_max_buy_net(
    *,
    net_sell: float,
    required_profit: float,
    repair_total: float,
    vat_rate: float,
) -> Optional[float]:
    # profit_after_vat = (net_sell - net_buy) * (1 - vat_rate) - repair_total
    one_minus_vat = 1.0 - float(vat_rate)
    if one_minus_vat <= 0:
        return None

    max_buy = float(net_sell) - (float(required_profit) + float(repair_total)) / one_minus_vat
    if max_buy < 0:
        max_buy = 0.0
    return _round2(max_buy)


def _compute_profit_at_buy_net(
    *,
    net_sell: float,
    net_buy: float,
    repair_total: float,
    vat_rate: float,
) -> float:
    margin = float(net_sell) - float(net_buy)
    vat = margin * float(vat_rate)
    profit = float(net_sell) - float(net_buy) - vat - float(repair_total)
    return _round2(profit)


def _default_trade_pricing_settings() -> TradePricingSettings:
    return TradePricingSettings(
        default_required_profit=DEFAULT_REQUIRED_PROFIT_GBP,
        default_required_margin=None,
        vat_rate=DEFAULT_VAT_RATE,
    )


async def get_trade_pricing_settings_for_user(db: AsyncIOMotorDatabase, user_id: str) -> Dict[str, Any]:
    doc = await db[USER_SETTINGS_COL].find_one(
        {"user_id": user_id},
        projection={"_id": 0, "trade_pricing": 1, "created_at": 1, "updated_at": 1},
    )

    raw = (doc or {}).get("trade_pricing") or {}

    settings: TradePricingSettings
    if isinstance(raw, dict):
        try:
            settings = TradePricingSettings.model_validate(raw)
        except ValidationError:
            settings = _default_trade_pricing_settings()
    else:
        settings = _default_trade_pricing_settings()

    return {
        "user_id": user_id,
        "default_required_profit": float(getattr(settings, "default_required_profit", DEFAULT_REQUIRED_PROFIT_GBP)),
        "default_required_margin": getattr(settings, "default_required_margin", None),
        "vat_rate": float(getattr(settings, "vat_rate", DEFAULT_VAT_RATE)),
        "created_at": (doc or {}).get("created_at"),
        "updated_at": (doc or {}).get("updated_at"),
    }


async def update_trade_pricing_settings_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    payload: TradePricingSettingsIn,
) -> Dict[str, Any]:
    now = _now_utc()

    try:
        settings = TradePricingSettings.model_validate(payload.model_dump())
    except ValidationError as exc:
        raise BMClientError(f"invalid trade pricing settings: {exc}") from exc

    await db[USER_SETTINGS_COL].update_one(
        {"user_id": user_id},
        {
            "$set": {
                "user_id": user_id,
                "trade_pricing": settings.model_dump(),
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )

    return await get_trade_pricing_settings_for_user(db, user_id)


async def update_trade_pricing_settings_for_group(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    group_id: str,
    payload: TradePricingGroupSettingsIn,
) -> Dict[str, Any]:
    """PATCH group overrides under pricing_groups.trade_pricing.settings.

    Patch semantics:
      - Only fields explicitly included in the payload are updated.
      - Explicit null clears.
    """
    gid = _parse_object_id(group_id)
    now = _now_utc()

    set_doc: Dict[str, Any] = {"trade_pricing.settings_updated_at": now}
    patch = payload.model_dump(exclude_unset=True)

    if "required_profit" in patch:
        set_doc["trade_pricing.settings.required_profit"] = (
            None if patch["required_profit"] is None else float(patch["required_profit"])
        )

    if "required_margin" in patch:
        set_doc["trade_pricing.settings.required_margin"] = (
            None if patch["required_margin"] is None else float(patch["required_margin"])
        )

    res = await db[PRICING_GROUPS_COL].update_one({"_id": gid, "user_id": user_id}, {"$set": set_doc})
    if res.matched_count <= 0:
        raise BMClientError(f"pricing_group not found: {group_id}")

    saved = await db[PRICING_GROUPS_COL].find_one(
        {"_id": gid, "user_id": user_id},
        projection={"_id": 0, "trade_pricing.settings": 1, "trade_pricing.settings_updated_at": 1},
    )
    return {
        "user_id": user_id,
        "group_id": group_id,
        "settings": _get_nested(saved or {}, "trade_pricing.settings", default={}),
        "settings_updated_at": _get_nested(saved or {}, "trade_pricing.settings_updated_at"),
    }


def _resolve_profit_and_margin(
    *,
    group: Dict[str, Any],
    user_settings: Dict[str, Any],
) -> Tuple[float, Optional[float], str, str]:
    """Return (required_profit, required_margin, profit_source, margin_source)."""

    group_profit = _to_float(_get_nested(group, "trade_pricing.settings.required_profit"))
    group_margin = _to_rate(_get_nested(group, "trade_pricing.settings.required_margin"))

    if group_profit is not None:
        required_profit = max(0.0, float(group_profit))
        profit_source = "group_override"
    else:
        default_profit = _to_float(user_settings.get("default_required_profit"))
        required_profit = max(0.0, float(default_profit if default_profit is not None else DEFAULT_REQUIRED_PROFIT_GBP))
        profit_source = "user_default"

    if group_margin is not None:
        required_margin = max(0.0, float(group_margin))
        margin_source = "group_override"
    else:
        default_margin = _to_rate(user_settings.get("default_required_margin"))
        required_margin = max(0.0, float(default_margin)) if default_margin is not None else None
        margin_source = "user_default"

    return required_profit, required_margin, profit_source, margin_source


def _compute_trade_pricing_for_group_doc(
    group: Dict[str, Any],
    *,
    user_settings: Dict[str, Any],
    sell_anchor_index: Optional[Dict[Tuple[str, str, str], Dict[str, Dict[str, float]]]] = None,
) -> Dict[str, Any]:
    computed_at = _now_utc()

    trade_sku = group.get("trade_sku")
    buy_condition = (
        (str(group.get("trade_sku_condition")).upper().strip() if isinstance(group.get("trade_sku_condition"), str) else None)
        or _infer_condition_from_trade_sku(trade_sku)
        or "UNKNOWN"
    )

    group_sell_condition = (
        (str(group.get("target_sell_condition")).upper().strip() if isinstance(group.get("target_sell_condition"), str) else None)
        or (str(group.get("sell_condition")).upper().strip() if isinstance(group.get("sell_condition"), str) else None)
        or "UNKNOWN"
    )

    # Optional user override: persist a scenario key on the pricing_group so the system
    # uses that route for the *effective* update price until the user changes it.
    trade_pricing_doc_any = group.get("trade_pricing")
    override_scenario_key: Optional[str] = None
    if isinstance(trade_pricing_doc_any, dict):
        raw_override = trade_pricing_doc_any.get("override_scenario_key")
        if isinstance(raw_override, str) and raw_override.strip():
            override_scenario_key = raw_override.strip()

    device_key = _extract_device_key(group)

    # Group-level sell anchor (used when scenario sell condition matches this group)
    group_net_sell = _extract_net_sell_anchor(group)
    group_gross_sell = _extract_gross_sell_anchor(group)
    currency = _extract_currency(group)

    competitor_net = _extract_competitor_net_price_to_win(group)
    competitor_gross = _extract_competitor_gross_price_to_win(group)

    # Back Market occasionally returns very small gross `price_to_win` values (e.g. £2/£3).
    # Treat anything under £10 as equivalent to £1 ("no competitor") so we follow the same pricing path.
    if currency == "GBP" and competitor_gross is not None:
        try:
            if float(competitor_gross) < 10.0:
                competitor_gross = 1.0
                competitor_net = _gross_to_net_tradein_gbp(1.0)
        except (TypeError, ValueError):
            competitor_gross = 1.0
            competitor_net = _gross_to_net_tradein_gbp(1.0)

    vat_rate = _to_rate(user_settings.get("vat_rate"))
    if vat_rate is None:
        vat_rate = DEFAULT_VAT_RATE

    required_profit, required_margin, profit_source, margin_source = _resolve_profit_and_margin(
        group=group,
        user_settings=user_settings,
    )

    repair_costs = _extract_repair_costs(group)

    # Build scenarios for ALL required sell-condition targets for this buy condition.
    scenarios: List[TradePricingScenario] = []
    for target_sell_condition in _target_sell_conditions_for_buy_condition(buy_condition):
        scenarios.extend(_build_scenarios_for_route(buy_condition, target_sell_condition))

    scenario_docs: List[Dict[str, Any]] = []
    any_missing_cost_keys: set[str] = set()
    missing_sell_anchor_conditions: set[str] = set()

    best_strict: Optional[Dict[str, Any]] = None

    for sc in scenarios:
        # Scenario sell anchor MUST match the scenario sell condition.
        sc_sell_condition = group_sell_condition
        if isinstance(sc.route, str) and "->" in sc.route:
            rhs = sc.route.split("->", 1)[1].strip().upper()
            if rhs:
                sc_sell_condition = rhs

        # Resolve the sell anchor for THIS scenario sell condition.
        net_sell: Optional[float] = None
        gross_sell: Optional[float] = None
        if sc_sell_condition == group_sell_condition:
            net_sell = group_net_sell
            gross_sell = group_gross_sell
        elif device_key is not None and sell_anchor_index is not None:
            entry = (sell_anchor_index.get(device_key) or {}).get(sc_sell_condition) or {}
            if isinstance(entry, dict):
                net_sell = _to_pos_float(entry.get("net"))
                gross_sell = _to_pos_float(entry.get("gross"))
            else:
                # Backwards-compatibility if index entries are floats (net only)
                net_sell = _to_pos_float(entry)

        if net_sell is None:
            missing_sell_anchor_conditions.add(sc_sell_condition)

        # Resolve repair total for this scenario.
        cost_parts: Dict[str, Optional[float]] = {k: repair_costs.get(k) for k in sc.repair_cost_keys}
        missing_cost_keys = [k for k, v in cost_parts.items() if v is None]
        any_missing_cost_keys.update(missing_cost_keys)

        strict_valid_inputs = bool(net_sell is not None and not missing_cost_keys)

        max_buy_net: Optional[float] = None
        max_buy_gross: Optional[float] = None

        profit_at_competitor: Optional[float] = None
        margin_at_competitor: Optional[float] = None
        vat_at_competitor: Optional[float] = None

        # Final update guidance for this scenario (what we should PUT back to BM).
        # NOTE: We always cap at max_trade_price_gross (profit-safe), but we may still
        # publish that capped value even if it doesn't currently win (so we become next
        # in line if competitors drop).
        max_trade_offer_gross: Optional[float] = None
        competitor_to_win_rounded_gross: Optional[float] = None
        final_update_price_gross: Optional[float] = None
        final_update_price_net: Optional[float] = None
        final_update_reason: Optional[str] = None
        profit_at_final_update: Optional[float] = None
        margin_at_final_update: Optional[float] = None
        vat_at_final_update: Optional[float] = None

        repair_total: Optional[float] = None

        if strict_valid_inputs and net_sell is not None:
            repair_total = sum(float(v) for v in cost_parts.values() if v is not None)

            max_buy_net = _compute_max_buy_net(
                net_sell=net_sell,
                required_profit=required_profit,
                repair_total=repair_total,
                vat_rate=vat_rate,
            )
            if max_buy_net is not None and currency == "GBP":
                max_buy_gross = _net_to_gross_tradein_gbp(max_buy_net)

            if competitor_net is not None:
                vat_at_competitor = _round2((float(net_sell) - float(competitor_net)) * float(vat_rate))
                profit_at_competitor = _compute_profit_at_buy_net(
                    net_sell=net_sell,
                    net_buy=competitor_net,
                    repair_total=repair_total,
                    vat_rate=vat_rate,
                )
                if net_sell > 0:
                    margin_at_competitor = _round2(float(profit_at_competitor) / float(net_sell))

            # --- Final update price logic (whole £ in GB) ---
            # Scenario 1: gross_price_to_win == £1.00 -> no competitors.
            # Scenario 2: gross_price_to_win > max_trade -> publish max_trade anyway (profit-safe, next-in-line).
            # Scenario 3: gross_price_to_win < max_trade -> publish just enough to win (rounded up to £1), capped at max_trade.
            if max_buy_gross is not None and currency == "GBP":
                cap = _floor_to_pounds(max_buy_gross)
                if cap is not None and cap >= 1.0:
                    max_trade_offer_gross = cap

                    # price_to_win is the price required to win; BM returns £1 when no competitor.
                    if competitor_gross is None or float(competitor_gross) <= 20.01:
                        final_update_price_gross = cap
                        final_update_reason = "no_competitor"
                    else:
                        competitor_to_win_rounded_gross = _ceil_to_pounds(float(competitor_gross))
                        if competitor_to_win_rounded_gross is not None and competitor_to_win_rounded_gross <= cap:
                            final_update_price_gross = competitor_to_win_rounded_gross
                            final_update_reason = "to_win"
                        else:
                            final_update_price_gross = cap
                            final_update_reason = "capped_at_max_trade"

                    if final_update_price_gross is not None:
                        final_update_price_net = _gross_to_net_tradein_gbp(final_update_price_gross)

                    if final_update_price_net is not None:
                        vat_at_final_update = _round2((float(net_sell) - float(final_update_price_net)) * float(vat_rate))
                        profit_at_final_update = _compute_profit_at_buy_net(
                            net_sell=net_sell,
                            net_buy=float(final_update_price_net),
                            repair_total=repair_total,
                            vat_rate=vat_rate,
                        )
                        if net_sell > 0 and profit_at_final_update is not None:
                            margin_at_final_update = _round2(float(profit_at_final_update) / float(net_sell))
                else:
                    # Cannot publish a profit-safe whole-£ price (min £1).
                    final_update_reason = "max_trade_below_min_price"

        scenario_valid = bool(strict_valid_inputs and max_buy_net is not None)

        scenario_doc = {
            "key": sc.key,
            "route": sc.route,
            "repair_cost_keys": list(sc.repair_cost_keys),
            "missing_cost_keys": missing_cost_keys,
            "repair_cost_total": _round2(repair_total) if repair_total is not None else None,
            "net_sell_anchor": _round2(net_sell) if net_sell is not None else None,
            "gross_sell_anchor": _round2(gross_sell) if gross_sell is not None else None,
            "max_trade_price_net": max_buy_net,
            "max_trade_price_gross": max_buy_gross,
            "max_trade_offer_gross": max_trade_offer_gross,
            "competitor_to_win_rounded_gross": competitor_to_win_rounded_gross,
            "final_update_price_gross": final_update_price_gross,
            "final_update_price_net": final_update_price_net,
            "final_update_reason": final_update_reason,
            "profit_at_final_update": profit_at_final_update,
            "margin_at_final_update": margin_at_final_update,
            "vat_at_final_update": vat_at_final_update,
            "profit_at_competitor": profit_at_competitor,
            "margin_at_competitor": margin_at_competitor,
            "vat_at_competitor": vat_at_competitor,
            "valid": scenario_valid,
        }

        scenario_docs.append(scenario_doc)

        if scenario_valid and max_buy_net is not None:
            if best_strict is None:
                best_strict = scenario_doc
            else:
                prev = best_strict.get("max_trade_price_net")
                if prev is None or float(max_buy_net) > float(prev):
                    best_strict = scenario_doc

    # ------------------------------------------------------------------
    # Determine the *effective* scenario
    #
    # - best_strict is the highest max_trade_price_net valid scenario
    # - override_scenario_key (if present) forces the system to use that scenario
    #   for final_update / ok_to_update until changed.
    # ------------------------------------------------------------------

    best_scenario_key = best_strict.get("key") if best_strict else None

    override_scenario: Optional[Dict[str, Any]] = None
    if override_scenario_key:
        for s in scenario_docs:
            if isinstance(s, dict) and s.get("key") == override_scenario_key:
                override_scenario = s
                break

    effective: Optional[Dict[str, Any]]
    effective_scenario_key: Optional[str]
    effective_source: str

    if override_scenario_key:
        # If an override is present but we can't find it in the recomputed scenarios,
        # treat the group as not OK to update (safer than silently falling back to best).
        if override_scenario is None:
            effective = None
            effective_scenario_key = override_scenario_key
            effective_source = "override_missing"
        else:
            effective = override_scenario
            effective_scenario_key = override_scenario_key
            effective_source = "override"
    else:
        effective = best_strict
        effective_scenario_key = best_scenario_key
        effective_source = "best"

    # Effective update fields (whole £ cap + final update price).
    max_trade_price_net = effective.get("max_trade_price_net") if effective else None
    max_trade_price_gross = effective.get("max_trade_price_gross") if effective else None
    max_trade_offer_gross = effective.get("max_trade_offer_gross") if effective else None
    final_update_price_gross = effective.get("final_update_price_gross") if effective else None
    final_update_price_net = effective.get("final_update_price_net") if effective else None
    final_update_reason = effective.get("final_update_reason") if effective else None
    profit_at_final_update = effective.get("profit_at_final_update") if effective else None
    margin_at_final_update = effective.get("margin_at_final_update") if effective else None
    vat_at_final_update = effective.get("vat_at_final_update") if effective else None

    competitor_within_strict: Optional[bool] = None
    delta_to_competitor: Optional[float] = None
    if competitor_net is not None and max_trade_price_net is not None:
        competitor_within_strict = bool(float(competitor_net) <= float(max_trade_price_net))
        delta_to_competitor = _round2(float(max_trade_price_net) - float(competitor_net))

    effective_route = (effective.get("route") if effective else None) or f"{buy_condition}->{group_sell_condition}"
    effective_sell_condition = (
        effective_route.split("->", 1)[1].strip().upper()
        if isinstance(effective_route, str) and "->" in effective_route
        else group_sell_condition
    )

    guardrails = {
        # Backwards-compatible: this flag only reflects the group-level sell anchor.
        "missing_net_sell_anchor": bool(group_net_sell is None),
        # New: scenarios like FAIR->EXCELLENT might be missing the EXCELLENT anchor.
        "missing_sell_anchor_conditions": sorted(missing_sell_anchor_conditions),
        "missing_repair_costs": bool(any_missing_cost_keys),
        "missing_cost_keys": sorted(any_missing_cost_keys),
        "competitor_missing": bool(_get_nested(group, "tradein_listing.competitor.missing", default=False)),
    }

    # Eligibility flag for automated BM buyback updates.
    # If ok_to_update is False, the cron/update worker MUST NOT send a PUT for this group.
    tradein_id = _get_nested(group, "tradein_listing.tradein_id")
    not_ok_reasons: List[str] = []

    if not (isinstance(tradein_id, str) and tradein_id.strip()):
        not_ok_reasons.append("missing_tradein_id")

    # Currently we only implement the GB landed-cost model end-to-end.
    if currency != "GBP":
        not_ok_reasons.append("unsupported_currency")

    if effective is None:
        if override_scenario_key and override_scenario is None:
            not_ok_reasons.append("override_scenario_not_found")
        else:
            not_ok_reasons.append("no_valid_scenarios")

    # If an override is present but the chosen scenario isn't valid, explicitly block updates.
    if effective is not None and override_scenario_key:
        if not bool(effective.get("valid")):
            not_ok_reasons.append("override_scenario_not_valid")

    if final_update_reason == "max_trade_below_min_price":
        not_ok_reasons.append("max_trade_below_min_price")

    if final_update_price_gross is None:
        not_ok_reasons.append("missing_final_update_price_gross")
    else:
        try:
            if float(final_update_price_gross) < 1.0:
                not_ok_reasons.append("final_update_below_min_price")
        except (TypeError, ValueError):
            not_ok_reasons.append("final_update_below_min_price")

    if max_trade_offer_gross is None:
        not_ok_reasons.append("missing_max_trade_offer_gross")

    # Sanity: never allow the final update price to exceed the computed cap.
    if final_update_price_gross is not None and max_trade_offer_gross is not None:
        try:
            if float(final_update_price_gross) > float(max_trade_offer_gross) + 1e-9:
                not_ok_reasons.append("final_update_exceeds_max_trade_offer")
        except (TypeError, ValueError):
            not_ok_reasons.append("final_update_exceeds_max_trade_offer")

    # Final sanity: profit must still meet required_profit after rounding/ceiling/flooring.
    if profit_at_final_update is None:
        not_ok_reasons.append("missing_profit_at_final_update")
    else:
        try:
            if float(profit_at_final_update) < float(required_profit) - 0.01:
                not_ok_reasons.append("profit_below_required")
        except (TypeError, ValueError):
            not_ok_reasons.append("profit_below_required")

    ok_to_update = len(not_ok_reasons) == 0

    return {
        "version": 1,
        "computed_at": computed_at,
        "trade_sku": trade_sku,
        "group_id": str(group.get("_id")) if group.get("_id") else None,
        "buy_condition": buy_condition,
        "sell_condition": effective_sell_condition,
        "route": effective_route,
        "currency": currency,
        "requirements": {
            "required_profit": _round2(required_profit),
            "required_margin": float(required_margin) if required_margin is not None else None,
            "required_profit_source": profit_source,
            "required_margin_source": margin_source,
            "vat_rate": float(vat_rate),
        },
        "inputs": {
            # Show the net sell anchor used by the *best* scenario (falls back to group anchor).
            "net_sell_anchor": (
                best_strict.get("net_sell_anchor")
                if isinstance(best_strict, dict) and best_strict.get("net_sell_anchor") is not None
                else (_round2(group_net_sell) if group_net_sell is not None else None)
            ),
            "gross_sell_anchor": (
                best_strict.get("gross_sell_anchor")
                if isinstance(best_strict, dict) and best_strict.get("gross_sell_anchor") is not None
                else (_round2(group_gross_sell) if group_gross_sell is not None else None)
            ),
            "competitor_net_price_to_win": _round2(competitor_net) if competitor_net is not None else None,
            "competitor_gross_price_to_win": _round2(competitor_gross) if competitor_gross is not None else None,
            "repair_costs": repair_costs,
        },
        "scenarios": scenario_docs,
        "best_scenario_key": best_scenario_key,
        "override_scenario_key": override_scenario_key,
        "effective_scenario_key": effective_scenario_key,
        "effective_scenario_source": effective_source,
        "max_trade_price_net": max_trade_price_net,
        "max_trade_price_gross": max_trade_price_gross,
        "max_trade_offer_gross": max_trade_offer_gross,
        "final_update_price_gross": final_update_price_gross,
        "final_update_price_net": final_update_price_net,
        "final_update_reason": final_update_reason,
        "profit_at_final_update": profit_at_final_update,
        "margin_at_final_update": margin_at_final_update,
        "vat_at_final_update": vat_at_final_update,
        "ok_to_update": ok_to_update,
        "not_ok_reasons": not_ok_reasons,
        "competitor_within_strict": competitor_within_strict,
        "delta_to_competitor_net": delta_to_competitor,
        "guardrails": guardrails,
    }


# Public wrapper: used by trade-in orphan pricing.
def compute_trade_pricing_for_doc(
    *,
    group: Dict[str, Any],
    user_settings: Dict[str, Any],
    sell_anchor_index: Dict[Tuple[str, str, str], Dict[str, Dict[str, float]]],
) -> Dict[str, Any]:
    """Public wrapper around the internal group trade pricing computation."""
    return _compute_trade_pricing_for_group_doc(
        group=group,
        user_settings=user_settings,
        sell_anchor_index=sell_anchor_index,
    )


async def _persist_trade_pricing_computed(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    group_object_id: ObjectId,
    computed: Dict[str, Any],
) -> None:
    now = _now_utc()
    await db[PRICING_GROUPS_COL].update_one(
        {"_id": group_object_id, "user_id": user_id},
        {
            "$set": {
                "trade_pricing.computed": computed,
                "trade_pricing.best_scenario_key": computed.get("best_scenario_key"),
                "trade_pricing.max_trade_price_net": computed.get("max_trade_price_net"),
                "trade_pricing.max_trade_price_gross": computed.get("max_trade_price_gross"),
                "trade_pricing.max_trade_offer_gross": computed.get("max_trade_offer_gross"),
                "trade_pricing.final_update_price_gross": computed.get("final_update_price_gross"),
                "trade_pricing.final_update_price_net": computed.get("final_update_price_net"),
                "trade_pricing.final_update_reason": computed.get("final_update_reason"),
                "trade_pricing.ok_to_update": computed.get("ok_to_update"),
                "trade_pricing.not_ok_reasons": computed.get("not_ok_reasons"),
                "trade_pricing.updated_at": now,
                "updated_at": now,
            }
        },
    )


async def recompute_trade_pricing_for_group(
    db: AsyncIOMotorDatabase,
    user_id: str,
    group_id: str,
) -> Dict[str, Any]:
    gid = _parse_object_id(group_id)

    group = await db[PRICING_GROUPS_COL].find_one({"_id": gid, "user_id": user_id})
    if not group:
        raise BMClientError(f"pricing_group not found: {group_id}")

    user_settings = await get_trade_pricing_settings_for_user(db, user_id)

    # Build a sell-anchor index so cross-condition scenarios (e.g. FAIR->EXCELLENT)
    # use the correct net sell anchor for the target sell condition.
    sell_anchor_index = await _build_sell_anchor_index_for_user(db, user_id=user_id)

    computed = _compute_trade_pricing_for_group_doc(
        group,
        user_settings=user_settings,
        sell_anchor_index=sell_anchor_index,
    )

    await _persist_trade_pricing_computed(db, user_id=user_id, group_object_id=gid, computed=computed)

    return {
        "user_id": user_id,
        "group_id": group_id,
        "trade_pricing": computed,
    }


async def recompute_trade_pricing_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    trade_skus: Optional[Iterable[str]] = None,
    groups_filter: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    max_parallel: int = 10,
    include_item_results: bool = False,
) -> Dict[str, Any]:
    """Recompute and persist trade_pricing for all matching pricing_groups."""
    t0 = time.perf_counter()
    user_settings = await get_trade_pricing_settings_for_user(db, user_id)

    # Pre-compute sell anchors for all groups so scenarios can look up anchors
    # for other conditions without extra round-trips per group.
    sell_anchor_index = await _build_sell_anchor_index_for_user(db, user_id=user_id)

    q: Dict[str, Any] = {"user_id": user_id}
    if groups_filter:
        q.update(groups_filter)

    if trade_skus is not None:
        sku_list = [str(s).strip() for s in trade_skus if str(s).strip()]
        if sku_list:
            q["trade_sku"] = {"$in": sku_list}

    # IMPORTANT: include identity fields so cross-condition sell anchor lookup works
    # when computing scenarios (GOOD->EXCELLENT etc).
    projection = {
        "_id": 1,
        "user_id": 1,
        "brand": 1,
        "model": 1,
        "storage_gb": 1,
        "group_key": 1,
        "trade_sku": 1,
        "trade_sku_condition": 1,
        "target_sell_condition": 1,
        "sell_condition": 1,
        "sell_anchor": 1,
        "tradein_listing": 1,
        "repair_costs": 1,
        "trade_pricing": 1,
    }

    cur = db[PRICING_GROUPS_COL].find(q, projection=projection)
    if limit is not None:
        cur = cur.limit(int(limit))

    groups: List[Dict[str, Any]] = []
    async for doc in cur:
        groups.append(doc)

    sem = asyncio.Semaphore(max(1, int(max_parallel)))

    updated = 0
    failed = 0
    item_results: List[Dict[str, Any]] = []

    async def _one(group_doc: Dict[str, Any]) -> None:
        nonlocal updated, failed

        gid = group_doc.get("_id")
        if not isinstance(gid, ObjectId):
            failed += 1
            return

        try:
            async with sem:
                computed = _compute_trade_pricing_for_group_doc(
                    group_doc,
                    user_settings=user_settings,
                    sell_anchor_index=sell_anchor_index,
                )
                await _persist_trade_pricing_computed(db, user_id=user_id, group_object_id=gid, computed=computed)
                updated += 1

                if include_item_results:
                    item_results.append(
                        {
                            "group_id": str(gid),
                            "trade_sku": group_doc.get("trade_sku"),
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
                    )

        except (BMClientError, KeyError, TypeError, ValueError, PyMongoError) as exc:
            failed += 1
            logger.exception(
                "[trade_pricing] group_recompute_failed user_id=%s group_id=%s err=%r",
                user_id,
                str(gid),
                exc,
            )

    await asyncio.gather(*[_one(g) for g in groups])

    elapsed = time.perf_counter() - t0

    out: Dict[str, Any] = {
        "user_id": user_id,
        "matched_groups": len(groups),
        "updated": updated,
        "failed": failed,
        "elapsed_seconds": round(elapsed, 3),
        "defaults": {
            "default_required_profit": user_settings.get("default_required_profit"),
            "default_required_margin": user_settings.get("default_required_margin"),
            "vat_rate": user_settings.get("vat_rate"),
            "default_battery_cost": DEFAULT_BATTERY_COST_GBP,
        },
    }
    if include_item_results:
        out["results"] = item_results

    logger.info(
        "[trade_pricing] user_recompute_done user_id=%s matched=%d updated=%d failed=%d elapsed=%.3fs",
        user_id,
        len(groups),
        updated,
        failed,
        elapsed,
    )

    return out




