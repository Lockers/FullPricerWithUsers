# app/features/pricing_groups/issues_repo.py
"""
Pricing grouping issue persistence.

We store "things we skipped" so they can be fixed manually later.

Collections
----------
pricing_bad_sell_skus
  - one doc per (user_id, listing_id)
pricing_bad_tradein_skus
  - one doc per (user_id, tradein_id)

Why not store full raw payload here?
-----------------------------------
Because it duplicates data you already store in:
- bm_sell_listings
- bm_tradein_listings

Instead, we store:
- identifiers
- sku string
- reason_code + reason_details
- timestamps + seen_count
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import UpdateOne


@dataclass(frozen=True)
class BulkIssueUpsertResult:
    attempted: int
    upserted: int
    matched: int
    modified: int
    elapsed_seconds: float


ExtraDocFieldsFn = Callable[[Dict[str, Any]], Dict[str, Any]]


class PricingIssuesRepo:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._sell: AsyncIOMotorCollection = db["pricing_bad_sell_skus"]
        self._tradein: AsyncIOMotorCollection = db["pricing_bad_tradein_skus"]

    @staticmethod
    async def _bulk_upsert_issues(
        *,
        coll: AsyncIOMotorCollection,
        user_id: str,
        issues: List[Dict[str, Any]],
        id_key: str,
        ref_collection: str,
        extra_doc_fields: Optional[ExtraDocFieldsFn] = None,
    ) -> BulkIssueUpsertResult:
        start = time.perf_counter()
        now = datetime.now(timezone.utc)
        extra_doc_fields = extra_doc_fields or (lambda _it: {})

        ops: List[UpdateOne] = []
        for it in issues:
            raw_id = it.get(id_key)
            if not raw_id:
                continue

            str_id = str(raw_id)

            doc: Dict[str, Any] = {
                "user_id": user_id,
                id_key: str_id,
                "sku": it.get("sku"),
                "reason_code": it.get("reason_code"),
                "reason_details": it.get("reason_details") or {},
                "ref_collection": ref_collection,
                "last_seen_at": now,
            }
            doc.update(extra_doc_fields(it) or {})

            ops.append(
                UpdateOne(
                    {"user_id": user_id, id_key: str_id},
                    {
                        "$set": doc,
                        "$setOnInsert": {"first_seen_at": now},
                        "$inc": {"seen_count": 1},
                    },
                    upsert=True,
                )
            )

        if not ops:
            return BulkIssueUpsertResult(0, 0, 0, 0, time.perf_counter() - start)

        res = await coll.bulk_write(ops, ordered=False)
        return BulkIssueUpsertResult(
            attempted=len(ops),
            upserted=int(res.upserted_count),
            matched=int(res.matched_count),
            modified=int(res.modified_count),
            elapsed_seconds=time.perf_counter() - start,
        )

    @staticmethod
    async def _list_issues(
        *,
        coll: AsyncIOMotorCollection,
        user_id: str,
        reason_code: Optional[str],
        limit: int,
    ) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {"user_id": user_id}
        if reason_code:
            q["reason_code"] = reason_code

        cursor = coll.find(q, {"_id": 0}).sort("last_seen_at", -1).limit(int(limit))
        return [doc async for doc in cursor]

    async def upsert_sell_issues(self, *, user_id: str, issues: List[Dict[str, Any]]) -> BulkIssueUpsertResult:
        """
        issues items must include:
          - listing_id (str)
          - sku (str|None)
          - reason_code (str)
          - reason_details (dict)
        """
        return await self._bulk_upsert_issues(
            coll=self._sell,
            user_id=user_id,
            issues=issues,
            id_key="listing_id",
            ref_collection="bm_sell_listings",
        )

    async def upsert_tradein_issues(self, *, user_id: str, issues: List[Dict[str, Any]]) -> BulkIssueUpsertResult:
        """
        issues items must include:
          - tradein_id (str)
          - sku (str|None)
          - reason_code (str)
          - reason_details (dict)
        """
        return await self._bulk_upsert_issues(
            coll=self._tradein,
            user_id=user_id,
            issues=issues,
            id_key="tradein_id",
            ref_collection="bm_tradein_listings",
            extra_doc_fields=lambda it: {
                "product_id": it.get("product_id"),
                "aesthetic_grade_code": it.get("aesthetic_grade_code"),
            },
        )

    async def list_sell_issues(
        self,
        *,
        user_id: str,
        reason_code: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        return await self._list_issues(
            coll=self._sell,
            user_id=user_id,
            reason_code=reason_code,
            limit=limit,
        )

    async def list_tradein_issues(
        self,
        *,
        user_id: str,
        reason_code: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        return await self._list_issues(
            coll=self._tradein,
            user_id=user_id,
            reason_code=reason_code,
            limit=limit,
        )


