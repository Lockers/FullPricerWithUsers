# app/features/backmarket/sell/listings_repo.py
"""
Persistence for BM sell listings.

Storage model (one doc per listing per user):
{
  user_id: str,
  listing_id: str,
  raw: { ... BM listing payload ... },
  first_seen_at: datetime,
  last_seen_at: datetime,
}
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne


@dataclass(frozen=True)
class UpsertManyResult:
    attempted: int
    skipped_missing_id: int
    upserted: int
    matched: int
    modified: int
    elapsed_seconds: float


def _extract_listing_id(raw: Dict[str, Any]) -> Optional[str]:
    """
    BM listing payloads typically include `id`. Keep this defensive.
    """
    val = raw.get("id") or raw.get("listing_id") or raw.get("uuid")
    if val is None:
        return None
    s = str(val).strip()
    return s or None


class SellListingsRepo:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db["bm_sell_listings"]

    async def upsert_many(self, *, user_id: str, listings: List[Dict[str, Any]]) -> UpsertManyResult:
        start = time.perf_counter()
        now = datetime.now(timezone.utc)

        ops: List[UpdateOne] = []
        skipped = 0

        for raw in listings:
            listing_id = _extract_listing_id(raw)
            if not listing_id:
                skipped += 1
                continue

            ops.append(
                UpdateOne(
                    {"user_id": user_id, "listing_id": listing_id},
                    {
                        "$set": {
                            "user_id": user_id,
                            "listing_id": listing_id,
                            "raw": raw,
                            "last_seen_at": now,
                        },
                        "$setOnInsert": {"first_seen_at": now},
                    },
                    upsert=True,
                )
            )

        if not ops:
            return UpsertManyResult(
                attempted=len(listings),
                skipped_missing_id=skipped,
                upserted=0,
                matched=0,
                modified=0,
                elapsed_seconds=time.perf_counter() - start,
            )

        res = await self._col.bulk_write(ops, ordered=False)

        return UpsertManyResult(
            attempted=len(listings),
            skipped_missing_id=skipped,
            upserted=int(res.upserted_count),
            matched=int(res.matched_count),
            modified=int(res.modified_count),
            elapsed_seconds=time.perf_counter() - start,
        )

    async def count_for_user(self, user_id: str) -> int:
        return int(await self._col.count_documents({"user_id": user_id}))

    async def sample_for_user(self, user_id: str, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Returns stored docs (without _id) so you can verify saving via Swagger.
        """
        cursor = (
            self._col.find({"user_id": user_id}, {"_id": 0})
            .sort("last_seen_at", -1)
            .limit(int(limit))
        )
        return [doc async for doc in cursor]
