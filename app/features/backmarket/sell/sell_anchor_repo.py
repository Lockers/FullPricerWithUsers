# app/features/backmarket/sell/sell_anchor_repo.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase


SETTINGS_COL = "sell_anchor_settings"
PRICING_GROUPS_COL = "pricing_groups"
SELL_LISTINGS_COL = "bm_sell_listings"


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def get_user_settings_doc(db: AsyncIOMotorDatabase, user_id: str) -> Optional[Dict[str, Any]]:
    return await db[SETTINGS_COL].find_one({"user_id": user_id}, projection={"_id": 0})


async def upsert_user_settings_doc(db: AsyncIOMotorDatabase, user_id: str, doc: Dict[str, Any]) -> Dict[str, Any]:
    now = _now()
    to_set = dict(doc)
    to_set["updated_at"] = now

    await db[SETTINGS_COL].update_one(
        {"user_id": user_id},
        {
            "$set": to_set,
            "$setOnInsert": {"user_id": user_id, "created_at": now},
        },
        upsert=True,
    )
    saved = await get_user_settings_doc(db, user_id)
    return saved or {"user_id": user_id, **doc, "created_at": now, "updated_at": now}


async def find_group_id_for_listing_ref(
    db: AsyncIOMotorDatabase,
    user_id: str,
    listing_ref: str,
) -> Optional[ObjectId]:
    doc = await db[PRICING_GROUPS_COL].find_one(
        {"user_id": user_id, "listings.bm_listing_id": listing_ref},
        projection={"_id": 1},
    )
    return doc.get("_id") if doc else None


async def get_pricing_group(
    db: AsyncIOMotorDatabase,
    user_id: str,
    group_id: ObjectId,
) -> Optional[Dict[str, Any]]:
    return await db[PRICING_GROUPS_COL].find_one({"_id": group_id, "user_id": user_id})


async def update_pricing_group_sell_anchor(
    db: AsyncIOMotorDatabase,
    group_id: ObjectId,
    *,
    sell_anchor: Dict[str, Any],
) -> None:
    await db[PRICING_GROUPS_COL].update_one(
        {"_id": group_id},
        {
            "$set": {
                "sell_anchor": sell_anchor,
                "updated_at": _now(),
            }
        },
    )


async def list_pricing_groups_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    mongo_filter: Optional[Dict[str, Any]] = None,
    limit: int = 25,
    skip: int = 0,
) -> List[Dict[str, Any]]:
    q: Dict[str, Any] = {"user_id": user_id}
    if mongo_filter:
        q.update(mongo_filter)

    projection = {
        "_id": 1,
        "trade_sku": 1,
        "group_key": 1,
        "brand": 1,
        "model": 1,
        "storage_gb": 1,
        "listings_count": 1,
        "updated_at": 1,
        "sell_anchor.gross_used": 1,
        "sell_anchor.net_used": 1,
        "sell_anchor.source_used": 1,
        "sell_anchor.reason": 1,
        "sell_anchor.needs_manual_anchor": 1,
    }

    cur = (
        db[PRICING_GROUPS_COL]
        .find(q, projection=projection)
        .sort("updated_at", -1)
        .skip(int(skip))
        .limit(int(limit))
    )

    out: List[Dict[str, Any]] = []
    async for doc in cur:
        doc["_id"] = str(doc["_id"])
        out.append(doc)
    return out


async def get_sell_max_prices_for_listings(
    db: AsyncIOMotorDatabase,
    user_id: str,
    listing_ids: List[str],
) -> Dict[str, float]:
    if not listing_ids:
        return {}

    cur = db[SELL_LISTINGS_COL].find(
        {"user_id": user_id, "listing_id": {"$in": listing_ids}},
        projection={"listing_id": 1, "raw.max_price": 1},
    )

    out: Dict[str, float] = {}
    async for doc in cur:
        lid = str(doc.get("listing_id") or "").strip()
        raw = doc.get("raw") or {}
        mp = raw.get("max_price")
        try:
            if lid and mp is not None:
                out[lid] = float(mp)
        except (TypeError, ValueError):
            continue
    return out

