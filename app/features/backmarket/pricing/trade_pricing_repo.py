# app/features/backmarket/pricing/trade_pricing_repo.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

PRICING_GROUPS_COL = "pricing_groups"
TRADE_PRICING_SETTINGS_COL = "pricing_trade_pricing_settings"


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def get_user_trade_pricing_settings_doc(db: AsyncIOMotorDatabase, user_id: str) -> Optional[Dict[str, Any]]:
    return await db[TRADE_PRICING_SETTINGS_COL].find_one({"user_id": user_id})


async def upsert_user_trade_pricing_settings_doc(
    db: AsyncIOMotorDatabase,
    user_id: str,
    update: Dict[str, Any],
) -> Dict[str, Any]:
    now = _now()
    await db[TRADE_PRICING_SETTINGS_COL].update_one(
        {"user_id": user_id},
        {"$set": {**update, "user_id": user_id, "updated_at": now}, "$setOnInsert": {"created_at": now}},
        upsert=True,
    )
    doc = await db[TRADE_PRICING_SETTINGS_COL].find_one({"user_id": user_id})
    return doc or {"user_id": user_id, **update, "created_at": None, "updated_at": now}


async def update_pricing_group_trade_overrides(
    db: AsyncIOMotorDatabase,
    user_id: str,
    group_id: ObjectId,
    overrides: Dict[str, Any],
) -> bool:
    """Stores per-group overrides used by trade pricing calc."""
    now = _now()
    res = await db[PRICING_GROUPS_COL].update_one(
        {"_id": group_id, "user_id": user_id},
        {"$set": {"trade_pricing_overrides": overrides, "updated_at": now}},
    )
    return bool(res.matched_count)


def _normalize_computed_payload(trade_pricing: Dict[str, Any]) -> Dict[str, Any]:
    """Accept either a full trade_pricing object or the computed payload itself.

    - Newer code stores the calculated output under: pricing_groups.trade_pricing.computed
    - Some call sites historically passed the computed payload directly.

    This helper keeps updates resilient and (crucially) avoids overwriting the entire
    `trade_pricing` object, which would delete nested fields like `trade_pricing.computed.scenarios`.
    """
    computed = trade_pricing.get("computed")
    return computed if isinstance(computed, dict) else trade_pricing


async def update_pricing_group_trade_pricing(
    db: AsyncIOMotorDatabase,
    user_id: str,
    group_id: ObjectId,
    trade_pricing: Dict[str, Any],
) -> bool:
    """Persist trade pricing results onto a pricing_groups doc without clobbering subfields.

    IMPORTANT:
    - Do NOT `$set: {"trade_pricing": ...}` here. That pattern overwrites the whole object and
      will wipe `trade_pricing.computed` (and its `scenarios`) if another service wrote it first.
    - Instead, write the computed payload into `trade_pricing.computed` and update derived fields.
    """
    now = _now()

    computed = _normalize_computed_payload(trade_pricing)

    # Prefer strict values; fall back to assuming_zero if strict is missing.
    max_net = computed.get("max_trade_price_net")
    if max_net is None:
        max_net = computed.get("max_trade_price_net_assuming_zero")

    max_gross = computed.get("max_trade_price_gross")
    if max_gross is None:
        max_gross = computed.get("max_trade_price_gross_assuming_zero")

    best_key = computed.get("best_scenario_key")
    if best_key is None:
        best_key = computed.get("best_scenario_key_assuming_zero")

    set_doc: Dict[str, Any] = {
        # Canonical storage (matches trade_pricing_service.py)
        "trade_pricing.computed": computed,
        "trade_pricing.best_scenario_key": computed.get("best_scenario_key"),
        "trade_pricing.max_trade_price_net": computed.get("max_trade_price_net"),
        "trade_pricing.max_trade_price_gross": computed.get("max_trade_price_gross"),
        "trade_pricing.best_scenario_key_assuming_zero": computed.get("best_scenario_key_assuming_zero"),
        "trade_pricing.max_trade_price_net_assuming_zero": computed.get("max_trade_price_net_assuming_zero"),
        "trade_pricing.max_trade_price_gross_assuming_zero": computed.get("max_trade_price_gross_assuming_zero"),
        "trade_pricing.updated_at": now,
        "updated_at": now,
        # Legacy convenience fields (keep if anything still depends on them)
        "trade_pricing_best_scenario_key": best_key,
        "trade_pricing_updated_at": now,
        "max_trade_price": max_net,  # legacy ambiguous field; keep as NET
    }

    res = await db[PRICING_GROUPS_COL].update_one(
        {"_id": group_id, "user_id": user_id},
        {"$set": set_doc},
    )
    return bool(res.matched_count)