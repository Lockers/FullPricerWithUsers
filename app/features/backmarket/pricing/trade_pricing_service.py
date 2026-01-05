from __future__ import annotations

"""
Trade pricing calculations (max allowable buy price).

This module computes and persists trade-in pricing guidance onto each pricing_groups doc.

It uses:
- Sell anchor net price (sell_anchor.net_used preferred; falls back to sell_anchor.lowest_net_sell_price)
- Repair costs (pricing_groups.repair_costs.costs)
- Required profit (per pricing_group override; falls back to user default; falls back to £75)
- Margin VAT (user default; falls back to 0.1667)

Outputs are persisted under:
  pricing_groups.trade_pricing

Strict vs estimated
-------------------
If a scenario is missing one or more required repair costs, it will:
- mark the scenario invalid (`valid=False`)
- still compute an *estimated* max price by treating missing costs as 0.00

This matches your "price but guardrail" requirement without accidentally treating
unknown repair costs as known.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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
    "housing": ("housing", "housing_new"),
    "screen_refurb_in_house": ("screen_refurb_in_house", "screen_refurb_internal", "screen_refurb_inhouse"),
    "screen_refurb_external": ("screen_refurb_external", "screen_refurb_outsource", "screen_refurb_outsourced"),
    "screen_replacement_in_house": (
        "screen_replacement_in_house",
        "screen_replacement",
        "screen_replace_in_house",
        "new_screen",
    ),
}


def _extract_repair_costs(group: Dict[str, Any]) -> Dict[str, Optional[float]]:
    raw_costs = _get_nested(group, "repair_costs.costs", default={})
    costs: Dict[str, Optional[float]] = {k: None for k in REPAIR_KEY_ALIASES}

    if not isinstance(raw_costs, dict):
        return costs

    for canonical, aliases in REPAIR_KEY_ALIASES.items():
        for key in aliases:
            if key in raw_costs:
                costs[canonical] = _to_pos_float(raw_costs.get(key))
                break

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
        return ("GOOD", "EXCELLENT")
    if buy == "FAIR":
        return ("FAIR", "EXCELLENT")
    if buy == "EXCELLENT":
        return ("EXCELLENT",)

    # keep legacy compatibility
    if buy == "USED":
        return ("USED", "EXCELLENT")
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

    # UPDATED: include FAIR and add the full "new screen replacement" path.
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

    # Default: battery always required (per your spec)
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
    # profit = (net_sell - net_buy) * (1 - vat_rate) - repair_total
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
    # Explicit defaults to keep type checkers happy (and to avoid relying on Pydantic Field defaults).
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

    # Validate + normalize before persisting.
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
) -> Dict[str, Any]:
    computed_at = _now_utc()

    trade_sku = group.get("trade_sku")
    buy_condition = (
        (str(group.get("trade_sku_condition")).upper().strip() if isinstance(group.get("trade_sku_condition"), str) else None)
        or _infer_condition_from_trade_sku(trade_sku)
        or "UNKNOWN"
    )
    sell_condition = (
        (str(group.get("target_sell_condition")).upper().strip() if isinstance(group.get("target_sell_condition"), str) else None)
        or (str(group.get("sell_condition")).upper().strip() if isinstance(group.get("sell_condition"), str) else None)
        or "UNKNOWN"
    )

    net_sell = _extract_net_sell_anchor(group)
    currency = _extract_currency(group)

    competitor_net = _extract_competitor_net_price_to_win(group)
    competitor_gross = _extract_competitor_gross_price_to_win(group)

    vat_rate = _to_rate(user_settings.get("vat_rate"))
    if vat_rate is None:
        vat_rate = DEFAULT_VAT_RATE

    required_profit, required_margin, profit_source, margin_source = _resolve_profit_and_margin(
        group=group,
        user_settings=user_settings,
    )

    repair_costs = _extract_repair_costs(group)
    # Build scenarios for ALL required sell-condition targets for this buy condition.
    # This ensures a GOOD entry persists both GOOD->GOOD and GOOD->EXCELLENT under computed.scenarios.
    scenarios: List[TradePricingScenario] = []
    for target_sell_condition in _target_sell_conditions_for_buy_condition(buy_condition):
        scenarios.extend(_build_scenarios_for_route(buy_condition, target_sell_condition))

    scenario_docs: List[Dict[str, Any]] = []
    any_missing_cost_keys: set[str] = set()

    best_strict: Optional[Dict[str, Any]] = None
    best_estimated: Optional[Dict[str, Any]] = None

    for sc in scenarios:
        cost_parts: Dict[str, Optional[float]] = {k: repair_costs.get(k) for k in sc.repair_cost_keys}
        missing_cost_keys = [k for k, v in cost_parts.items() if v is None]
        any_missing_cost_keys.update(missing_cost_keys)

        repair_total_assuming_zero = sum(float(v or 0.0) for v in cost_parts.values())

        strict_valid_inputs = bool(net_sell is not None and not missing_cost_keys)

        max_buy_net_strict: Optional[float] = None
        max_buy_gross_strict: Optional[float] = None
        max_buy_net_estimated: Optional[float] = None
        max_buy_gross_estimated: Optional[float] = None

        profit_at_competitor: Optional[float] = None
        margin_at_competitor: Optional[float] = None
        vat_at_competitor: Optional[float] = None

        if strict_valid_inputs and net_sell is not None:
            repair_total_strict = sum(float(v or 0.0) for v in cost_parts.values())
            max_buy_net_strict = _compute_max_buy_net(
                net_sell=net_sell,
                required_profit=required_profit,
                repair_total=repair_total_strict,
                vat_rate=vat_rate,
            )
            if max_buy_net_strict is not None and currency == "GBP":
                max_buy_gross_strict = _net_to_gross_tradein_gbp(max_buy_net_strict)

        if net_sell is not None:
            max_buy_net_estimated = _compute_max_buy_net(
                net_sell=net_sell,
                required_profit=required_profit,
                repair_total=repair_total_assuming_zero,
                vat_rate=vat_rate,
            )
            if max_buy_net_estimated is not None and currency == "GBP":
                max_buy_gross_estimated = _net_to_gross_tradein_gbp(max_buy_net_estimated)

        if net_sell is not None and competitor_net is not None:
            vat_at_competitor = _round2((float(net_sell) - float(competitor_net)) * float(vat_rate))
            profit_at_competitor = _compute_profit_at_buy_net(
                net_sell=net_sell,
                net_buy=competitor_net,
                repair_total=repair_total_assuming_zero,
                vat_rate=vat_rate,
            )
            if net_sell > 0:
                margin_at_competitor = _round2(float(profit_at_competitor) / float(net_sell))

        scenario_valid = bool(strict_valid_inputs and max_buy_net_strict is not None)

        scenario_doc = {
            "key": sc.key,
            "route": sc.route,
            "repair_cost_keys": list(sc.repair_cost_keys),
            "repair_cost_parts": cost_parts,
            "missing_cost_keys": missing_cost_keys,
            "repair_cost_total": (None if missing_cost_keys else _round2(repair_total_assuming_zero)),
            "repair_cost_total_assuming_zero": _round2(repair_total_assuming_zero),
            "required_profit": _round2(required_profit),
            "vat_rate": float(vat_rate),
            "net_sell_anchor": _round2(net_sell) if net_sell is not None else None,
            "max_trade_price_net": max_buy_net_strict,
            "max_trade_price_gross": max_buy_gross_strict,
            "max_trade_price_net_assuming_zero": max_buy_net_estimated,
            "max_trade_price_gross_assuming_zero": max_buy_gross_estimated,
            "competitor_net_price_to_win": _round2(competitor_net) if competitor_net is not None else None,
            "competitor_gross_price_to_win": _round2(competitor_gross) if competitor_gross is not None else None,
            "profit_at_competitor_assuming_zero": profit_at_competitor,
            "margin_at_competitor_assuming_zero": margin_at_competitor,
            "vat_at_competitor": vat_at_competitor,
            "valid": scenario_valid,
            "computed_at": computed_at,
        }

        scenario_docs.append(scenario_doc)

        if scenario_valid and max_buy_net_strict is not None:
            if best_strict is None:
                best_strict = scenario_doc
            else:
                prev = best_strict.get("max_trade_price_net")
                if prev is None or float(max_buy_net_strict) > float(prev):
                    best_strict = scenario_doc

        if max_buy_net_estimated is not None:
            if best_estimated is None:
                best_estimated = scenario_doc
            else:
                prev_est = best_estimated.get("max_trade_price_net_assuming_zero")
                if prev_est is None or float(max_buy_net_estimated) > float(prev_est):
                    best_estimated = scenario_doc

    max_trade_price_net = best_strict.get("max_trade_price_net") if best_strict else None
    max_trade_price_gross = best_strict.get("max_trade_price_gross") if best_strict else None

    max_trade_price_net_est = best_estimated.get("max_trade_price_net_assuming_zero") if best_estimated else None
    max_trade_price_gross_est = best_estimated.get("max_trade_price_gross_assuming_zero") if best_estimated else None

    competitor_within_strict: Optional[bool] = None
    delta_to_competitor: Optional[float] = None
    if competitor_net is not None and max_trade_price_net is not None:
        competitor_within_strict = bool(float(competitor_net) <= float(max_trade_price_net))
        delta_to_competitor = _round2(float(max_trade_price_net) - float(competitor_net))

    guardrails = {
        "missing_net_sell_anchor": bool(net_sell is None),
        "missing_repair_costs": bool(any_missing_cost_keys),
        "missing_cost_keys": sorted(any_missing_cost_keys),
        "competitor_missing": bool(_get_nested(group, "tradein_listing.competitor.missing", default=False)),
    }

    return {
        "version": 1,
        "computed_at": computed_at,
        "trade_sku": trade_sku,
        "group_id": str(group.get("_id")) if group.get("_id") else None,
        "buy_condition": buy_condition,
        "sell_condition": sell_condition,
        "route": f"{buy_condition}->{sell_condition}",
        "currency": currency,
        "requirements": {
            "required_profit": _round2(required_profit),
            "required_margin": float(required_margin) if required_margin is not None else None,
            "required_profit_source": profit_source,
            "required_margin_source": margin_source,
            "vat_rate": float(vat_rate),
        },
        "inputs": {
            "net_sell_anchor": _round2(net_sell) if net_sell is not None else None,
            "competitor_net_price_to_win": _round2(competitor_net) if competitor_net is not None else None,
            "competitor_gross_price_to_win": _round2(competitor_gross) if competitor_gross is not None else None,
            "repair_costs": repair_costs,
        },
        "scenarios": scenario_docs,
        "best_scenario_key": best_strict.get("key") if best_strict else None,
        "max_trade_price_net": max_trade_price_net,
        "max_trade_price_gross": max_trade_price_gross,
        "best_scenario_key_assuming_zero": best_estimated.get("key") if best_estimated else None,
        "max_trade_price_net_assuming_zero": max_trade_price_net_est,
        "max_trade_price_gross_assuming_zero": max_trade_price_gross_est,
        "competitor_within_strict": competitor_within_strict,
        "delta_to_competitor_net": delta_to_competitor,
        "guardrails": guardrails,
    }


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
                "trade_pricing.best_scenario_key_assuming_zero": computed.get("best_scenario_key_assuming_zero"),
                "trade_pricing.max_trade_price_net_assuming_zero": computed.get("max_trade_price_net_assuming_zero"),
                "trade_pricing.max_trade_price_gross_assuming_zero": computed.get("max_trade_price_gross_assuming_zero"),
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
    computed = _compute_trade_pricing_for_group_doc(group, user_settings=user_settings)

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

    q: Dict[str, Any] = {"user_id": user_id}
    if groups_filter:
        q.update(groups_filter)

    if trade_skus is not None:
        sku_list = [str(s).strip() for s in trade_skus if str(s).strip()]
        if sku_list:
            q["trade_sku"] = {"$in": sku_list}

    projection = {
        "_id": 1,
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
                computed = _compute_trade_pricing_for_group_doc(group_doc, user_settings=user_settings)
                await _persist_trade_pricing_computed(db, user_id=user_id, group_object_id=gid, computed=computed)
                updated += 1

                if include_item_results:
                    item_results.append(
                        {
                            "group_id": str(gid),
                            "trade_sku": group_doc.get("trade_sku"),
                            "max_trade_price_net": computed.get("max_trade_price_net"),
                            "max_trade_price_net_assuming_zero": computed.get("max_trade_price_net_assuming_zero"),
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


