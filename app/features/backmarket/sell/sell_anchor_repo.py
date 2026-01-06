# app/features/backmarket/sell/sell_anchor_repo.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

PRICING_GROUPS_COL = "pricing_groups"
SELL_MAX_PRICES_COL = "pricing_sell_max_prices"
ANCHOR_SETTINGS_COL = "pricing_sell_anchor_settings"


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def get_user_settings_doc(db: AsyncIOMotorDatabase, user_id: str) -> Optional[Dict[str, Any]]:
    return await db[ANCHOR_SETTINGS_COL].find_one({"user_id": user_id})


async def upsert_user_settings_doc(db: AsyncIOMotorDatabase, user_id: str, update: Dict[str, Any]) -> Dict[str, Any]:
    now = _now()
    await db[ANCHOR_SETTINGS_COL].update_one(
        {"user_id": user_id},
        {"$set": {**update, "user_id": user_id, "updated_at": now}, "$setOnInsert": {"created_at": now}},
        upsert=True,
    )
    doc = await db[ANCHOR_SETTINGS_COL].find_one({"user_id": user_id})
    return doc or {"user_id": user_id, **update, "created_at": None, "updated_at": now}


async def get_pricing_group(db: AsyncIOMotorDatabase, user_id: str, group_id: ObjectId) -> Optional[Dict[str, Any]]:
    return await db[PRICING_GROUPS_COL].find_one({"_id": group_id, "user_id": user_id})


async def find_group_id_for_listing_ref(db: AsyncIOMotorDatabase, user_id: str, listing_ref: str) -> Optional[ObjectId]:
    doc = await db[PRICING_GROUPS_COL].find_one(
        {"user_id": user_id, "listings.bm_listing_id": listing_ref},
        projection={"_id": 1},
    )
    return doc.get("_id") if doc else None


async def update_pricing_group_sell_anchor(db: AsyncIOMotorDatabase, group_id: ObjectId, sell_anchor: Dict[str, Any]) -> None:
    now = _now()
    await db[PRICING_GROUPS_COL].update_one(
        {"_id": group_id},
        {"$set": {"sell_anchor": sell_anchor, "sell_anchor_updated_at": now, "updated_at": now}},
    )


async def update_pricing_group_manual_sell_anchor_gross(
    db: AsyncIOMotorDatabase,
    user_id: str,
    group_id: ObjectId,
    manual_gross: Optional[float],
) -> bool:
    """Set or clear the per-group manual sell anchor (gross).

    Stored at the pricing_group top-level as `manual_sell_anchor_gross`.
    `sell_anchor_service._build_sell_anchor_for_group_doc` reads this field when computing anchors.
    """
    now = _now()
    update: Dict[str, Any] = {"$set": {"updated_at": now}}

    if manual_gross is None:
        update["$unset"] = {"manual_sell_anchor_gross": ""}
    else:
        update["$set"]["manual_sell_anchor_gross"] = float(manual_gross)

    res = await db[PRICING_GROUPS_COL].update_one({"_id": group_id, "user_id": user_id}, update)
    return bool(res.matched_count)


async def list_pricing_groups_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    groups_filter: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    skip: Optional[int] = None,
) -> list[Dict[str, Any]]:
    q: Dict[str, Any] = {"user_id": user_id}
    if groups_filter:
        q.update(groups_filter)

    cur = db[PRICING_GROUPS_COL].find(q, projection=projection)
    if skip is not None:
        cur = cur.skip(int(skip))
    if limit is not None:
        cur = cur.limit(int(limit))

    out: list[Dict[str, Any]] = []
    async for g in cur:
        out.append(g)
    return out


async def get_sell_max_prices_for_listings(
    db: AsyncIOMotorDatabase,
    user_id: str,
    listing_ids: list[str],
) -> Dict[str, float]:
    if not listing_ids:
        return {}

    cur = db[SELL_MAX_PRICES_COL].find(
        {"user_id": user_id, "bm_listing_id": {"$in": listing_ids}},
        projection={"bm_listing_id": 1, "max_price": 1},
    )

    out: Dict[str, float] = {}
    async for doc in cur:
        lid = str(doc.get("bm_listing_id") or "").strip()
        mp = doc.get("max_price")
        if lid and isinstance(mp, (int, float)) and mp > 0:
            out[lid] = float(mp)
    return out



