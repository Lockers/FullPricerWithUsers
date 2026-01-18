# app/features/backmarket/pricing/trade_pricing_repo.py
from __future__ import annotations

from typing import Any, Dict, Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from .utils import normalize_computed_payload, update_one_set_with_updated_at, utc_now

PRICING_GROUPS_COL = "pricing_groups"
TRADE_PRICING_SETTINGS_COL = "pricing_trade_pricing_settings"


async def get_user_trade_pricing_settings_doc(db: AsyncIOMotorDatabase, user_id: str) -> Optional[Dict[str, Any]]:
    return await db[TRADE_PRICING_SETTINGS_COL].find_one({"user_id": user_id})


async def upsert_user_trade_pricing_settings_doc(
    db: AsyncIOMotorDatabase,
    user_id: str,
    update: Dict[str, Any],
) -> Dict[str, Any]:
    now = utc_now()
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
    now = utc_now()
    matched = await update_one_set_with_updated_at(
        db,
        collection=PRICING_GROUPS_COL,
        filter_doc={"_id": group_id, "user_id": user_id},
        set_doc={"trade_pricing_overrides": overrides},
        now=now,
    )
    return bool(matched)


async def update_pricing_group_trade_pricing(
    db: AsyncIOMotorDatabase,
    user_id: str,
    group_id: ObjectId,
    trade_pricing: Dict[str, Any],
) -> bool:
    """Persist trade pricing results onto a pricing_groups doc without clobbering subfields.

    IMPORTANT:
    - Only updates `trade_pricing.computed` (and timestamps).
    - Does NOT overwrite the entire `trade_pricing` object (avoids clobbering nested subfields).
    """
    now = utc_now()
    computed = normalize_computed_payload(trade_pricing)

    matched = await update_one_set_with_updated_at(
        db,
        collection=PRICING_GROUPS_COL,
        filter_doc={"_id": group_id, "user_id": user_id},
        set_doc={
            "trade_pricing.computed": computed,
            "trade_pricing.updated_at": now,
        },
        now=now,
    )
    return bool(matched)
