from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.core.errors import NotFoundError


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class RepairCostsRepo:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db["pricing_repair_costs"]

    async def upsert(
        self,
        *,
        user_id: str,
        market: str,
        currency: str,
        brand: str,
        model: str,
        costs: Dict[str, Any],
        notes: Optional[str],
    ) -> Dict[str, Any]:
        now = _now_utc()

        filt = {"user_id": user_id, "market": market, "brand": brand, "model": model}

        update = {
            "$set": {
                "user_id": user_id,
                "market": market,
                "currency": currency,
                "brand": brand,
                "model": model,
                "costs": costs,
                "notes": notes,
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        }

        try:
            doc = await self._col.find_one_and_update(
                filt,
                update,
                upsert=True,
                return_document=ReturnDocument.AFTER,
                projection={"_id": 0},
            )
            assert doc is not None
            return doc
        except DuplicateKeyError:
            # Rare race: another upsert inserted first; just read back.
            existing = await self._col.find_one(filt, projection={"_id": 0})
            if existing:
                return existing
            raise

    async def get_one(self, *, user_id: str, market: str, brand: str, model: str) -> Optional[Dict[str, Any]]:
        return await self._col.find_one(
            {"user_id": user_id, "market": market, "brand": brand, "model": model},
            projection={"_id": 0},
        )

    async def list_for_user(
        self,
        *,
        user_id: str,
        market: Optional[str] = None,
        brand: Optional[str] = None,
        model: Optional[str] = None,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {"user_id": user_id}
        if market is not None:
            q["market"] = market
        if brand is not None:
            q["brand"] = brand
        if model is not None:
            q["model"] = model

        cursor = (
            self._col.find(q, projection={"_id": 0})
            .sort([("brand", 1), ("model", 1)])
            .limit(int(limit))
        )
        return [doc async for doc in cursor]

    async def list_keys_for_user(
        self,
        *,
        user_id: str,
        market: str,
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """
        Return map: (brand, model) -> {updated_at, currency}
        """
        out: Dict[Tuple[str, str], Dict[str, Any]] = {}

        cursor = self._col.find(
            {"user_id": user_id, "market": market},
            projection={"_id": 0, "brand": 1, "model": 1, "updated_at": 1, "currency": 1},
        )

        async for doc in cursor:
            b = doc.get("brand")
            m = doc.get("model")
            if isinstance(b, str) and b and isinstance(m, str) and m:
                out[(b, m)] = {"updated_at": doc.get("updated_at"), "currency": doc.get("currency")}
        return out

    async def patch_one(
        self,
        *,
        user_id: str,
        market: str,
        brand: str,
        model: str,
        update_fields: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not update_fields:
            doc = await self.get_one(user_id=user_id, market=market, brand=brand, model=model)
            if not doc:
                raise NotFoundError(
                    code="repair_cost_not_found",
                    message="Repair cost not found",
                    details={"user_id": user_id, "market": market, "brand": brand, "model": model},
                )
            return doc

        update_fields = dict(update_fields)
        update_fields["updated_at"] = _now_utc()

        doc = await self._col.find_one_and_update(
            {"user_id": user_id, "market": market, "brand": brand, "model": model},
            {"$set": update_fields},
            return_document=ReturnDocument.AFTER,
            projection={"_id": 0},
        )
        if not doc:
            raise NotFoundError(
                code="repair_cost_not_found",
                message="Repair cost not found",
                details={"user_id": user_id, "market": market, "brand": brand, "model": model},
            )
        return doc

    async def delete_one(self, *, user_id: str, market: str, brand: str, model: str) -> bool:
        res = await self._col.delete_one({"user_id": user_id, "market": market, "brand": brand, "model": model})
        return res.deleted_count == 1

