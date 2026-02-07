# app/features/pricing_groups/repo.py
"""
Mongo persistence for pricing_groups.

We upsert groups by (user_id, trade_sku).
This guarantees each user has isolated grouping and avoids duplicates.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne


@dataclass(frozen=True)
class BulkUpsertResult:
    attempted: int
    upserted: int
    matched: int
    modified: int
    elapsed_seconds: float


class PricingGroupsRepo:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db["pricing_groups"]

    async def bulk_upsert(self, *, user_id: str, group_docs: List[Dict[str, Any]]) -> BulkUpsertResult:
        """
        Upsert many groups efficiently.

        Each group doc MUST contain:
        - user_id
        - trade_sku
        """
        start = time.perf_counter()
        now = datetime.now(timezone.utc)

        ops: List[UpdateOne] = []

        for g in group_docs:
            trade_sku = g.get("trade_sku")
            if not trade_sku:
                continue

            # Ensure consistent timestamps:
            g = dict(g)
            g["user_id"] = user_id
            g["updated_at"] = now

            ops.append(
                UpdateOne(
                    {"user_id": user_id, "trade_sku": trade_sku},
                    {
                        "$set": g,
                        "$setOnInsert": {
                            "created_at": now,
                            # Safety default: offers must be explicitly enabled per group.
                            "trade_pricing": {
                                "settings": {"offer_enabled": False},
                                "settings_updated_at": now,
                            },
                        },
                    },
                    upsert=True,
                )
            )

        if not ops:
            return BulkUpsertResult(
                attempted=0,
                upserted=0,
                matched=0,
                modified=0,
                elapsed_seconds=time.perf_counter() - start,
            )

        res = await self._col.bulk_write(ops, ordered=False)

        return BulkUpsertResult(
            attempted=len(ops),
            upserted=int(res.upserted_count),
            matched=int(res.matched_count),
            modified=int(res.modified_count),
            elapsed_seconds=time.perf_counter() - start,
        )

    async def delete_missing_trade_skus(self, *, user_id: str, keep_trade_skus: List[str]) -> int:
        """
        Optional cleanup:
        Delete groups for user_id that are not in keep_trade_skus.
        """
        if not keep_trade_skus:
            return 0
        res = await self._col.delete_many(
            {"user_id": user_id, "trade_sku": {"$nin": keep_trade_skus}}
        )
        return int(res.deleted_count)

    async def list_for_user(self, user_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        cursor = self._col.find({"user_id": user_id})
        if limit is not None:
            cursor = cursor.limit(int(limit))
        return [doc async for doc in cursor]

    async def get_by_object_id(self, oid) -> Optional[Dict[str, Any]]:
        return await self._col.find_one({"_id": oid})
