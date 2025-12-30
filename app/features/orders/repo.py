# app/features/orders/repo.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne


def _parse_rfc3339(dt_str: Any) -> Optional[datetime]:
    """
    Parse RFC3339-ish strings from BM.
    - "2022-04-29T09:35:24Z"
    - "2022-04-29T09:35:24.123456Z"
    - sometimes without tz: "2022-04-29T09:35:24"
    """
    if not dt_str:
        return None
    if isinstance(dt_str, datetime):
        return dt_str if dt_str.tzinfo else dt_str.replace(tzinfo=timezone.utc)

    s = str(dt_str).strip()
    if not s:
        return None

    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class BmOrdersRepo:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db["bm_orders"]

    async def bulk_upsert_orders(self, *, user_id: str, orders: List[Dict[str, Any]]) -> Dict[str, int]:
        if not orders:
            return {"matched": 0, "modified": 0, "upserted": 0}

        now = datetime.now(timezone.utc)
        ops: List[UpdateOne] = []

        for order in orders:
            oid = order.get("order_id")
            if oid is None:
                continue

            order_id = int(oid)

            # Keep the entire BM payload unchanged in bm_raw.
            # Store a few parsed dates for filtering/sync.
            doc: Dict[str, Any] = {
                "user_id": user_id,
                "order_id": order_id,
                "state": order.get("state"),
                "currency": order.get("currency"),
                "country_code": order.get("country_code"),

                "date_creation": _parse_rfc3339(order.get("date_creation")),
                "date_modification": _parse_rfc3339(order.get("date_modification")),
                "date_payment": _parse_rfc3339(order.get("date_payment")),
                "date_shipping": _parse_rfc3339(order.get("date_shipping")),

                # Full raw payload (includes addresses, IMEI, etc.)
                "bm_raw": order,

                "updated_at": now,
            }

            ops.append(
                UpdateOne(
                    {"user_id": user_id, "order_id": order_id},
                    {"$set": doc, "$setOnInsert": {"created_at": now}},
                    upsert=True,
                )
            )

        if not ops:
            return {"matched": 0, "modified": 0, "upserted": 0}

        res = await self._col.bulk_write(ops, ordered=False)
        return {
            "matched": int(getattr(res, "matched_count", 0) or 0),
            "modified": int(getattr(res, "modified_count", 0) or 0),
            "upserted": int(len(getattr(res, "upserted_ids", {}) or {})),
        }

    async def latest_date_modification(self, *, user_id: str) -> Optional[datetime]:
        doc = await self._col.find_one(
            {"user_id": user_id, "date_modification": {"$ne": None}},
            sort=[("date_modification", -1)],
            projection={"_id": 0, "date_modification": 1},
        )
        if not doc:
            return None
        dt = doc.get("date_modification")
        if isinstance(dt, datetime):
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        return None
