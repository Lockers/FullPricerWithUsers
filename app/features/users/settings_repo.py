# app/features/users/settings_repo.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ReturnDocument

from app.core.errors import NotFoundError


class UserSettingsRepo:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db["user_settings"]

    async def get(self, user_id: str) -> Optional[Dict[str, Any]]:
        return await self._col.find_one({"user_id": user_id}, {"_id": 0})

    async def upsert(self, user_id: str, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upsert settings for a user.

        Rules:
        - created_at is set ONLY on insert
        - updated_at is set on every write
        """
        now = datetime.now(timezone.utc)

        doc = dict(doc)
        doc["user_id"] = user_id

        # Remove timestamps from $set to avoid conflicts with $setOnInsert
        created_at = doc.pop("created_at", None)
        doc.pop("updated_at", None)

        update_doc = {
            "$set": {**doc, "updated_at": now},
            "$setOnInsert": {"created_at": created_at or now},
        }

        out = await self._col.find_one_and_update(
            {"user_id": user_id},
            update_doc,
            upsert=True,
            return_document=ReturnDocument.AFTER,
            projection={"_id": 0},
        )
        assert out is not None
        return out

    async def update(self, user_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        patch = {k: v for k, v in patch.items() if v is not None}
        patch["updated_at"] = datetime.now(timezone.utc)

        out = await self._col.find_one_and_update(
            {"user_id": user_id},
            {"$set": patch},
            return_document=ReturnDocument.AFTER,
            projection={"_id": 0},
        )
        if not out:
            raise NotFoundError(code="user_settings_not_found", message="User settings not found", details={"user_id": user_id})
        return out

    async def delete(self, user_id: str) -> bool:
        res = await self._col.delete_one({"user_id": user_id})
        return res.deleted_count == 1
