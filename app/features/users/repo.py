"""
UsersRepo: CRUD for users collection.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.core.errors import ConflictError, NotFoundError


class UsersRepo:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db["users"]

    async def create(self, *, email: str, name: str, company_name: str) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        doc = {
            "id": str(uuid4()),
            "email": email,
            "name": name,
            "company_name": company_name,
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }
        try:
            await self._col.insert_one(doc)
        except DuplicateKeyError as exc:
            raise ConflictError(code="user_exists", message="User with this email already exists") from exc

        doc.pop("_id", None)
        return doc

    async def get(self, user_id: str) -> Optional[Dict[str, Any]]:
        doc = await self._col.find_one({"id": user_id}, {"_id": 0})
        return doc

    async def list(self, limit: int = 100) -> list[Dict[str, Any]]:
        cursor = self._col.find({}, {"_id": 0}).limit(int(limit))
        return [doc async for doc in cursor]

    async def update(self, user_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        patch = {k: v for k, v in patch.items() if v is not None}
        patch["updated_at"] = datetime.now(timezone.utc)

        doc = await self._col.find_one_and_update(
            {"id": user_id},
            {"$set": patch},
            return_document=ReturnDocument.AFTER,
            projection={"_id": 0},
        )
        if not doc:
            raise NotFoundError(code="user_not_found", message="User not found", details={"user_id": user_id})
        return doc

    async def delete(self, user_id: str) -> bool:
        res = await self._col.delete_one({"id": user_id})
        return res.deleted_count == 1
