# app/features/pricing_groups/router.py
"""
/pricing-groups endpoints.

These endpoints build and inspect the grouping "backbone" used by pricing.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.errors import BadRequestError, NotFoundError
from app.db.mongo import get_db
from app.features.backmarket.pricing_groups.repo import PricingGroupsRepo
from app.features.backmarket.pricing_groups.service import build_pricing_groups_for_user

router = APIRouter(prefix="/pricing-groups", tags=["pricing-groups"])


def _parse_object_id(group_id: str) -> ObjectId:
    try:
        return ObjectId(group_id)
    except Exception as exc:  # noqa: BLE001
        raise BadRequestError(code="invalid_group_id", message="Invalid group_id") from exc


def _stringify_id(doc: Dict[str, Any]) -> Dict[str, Any]:
    raw_id = doc.get("_id")
    if isinstance(raw_id, ObjectId):
        doc["_id"] = str(raw_id)
    doc["id"] = doc.get("_id")
    return doc


@router.post("/build/{user_id}")
async def build_groups_for_user(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    prune_missing: bool = Query(False),
    max_children_per_group: int = Query(500, ge=1, le=5000),
) -> Dict[str, Any]:
    """
    Rebuild pricing_groups for a user from bm_tradein_listings + bm_sell_listings.

    Structural only. Safe to call repeatedly.
    """
    return await build_pricing_groups_for_user(
        db,
        user_id,
        prune_missing=prune_missing,
        max_children_per_group=max_children_per_group,
    )


@router.get("/user/{user_id}")
async def list_groups_for_user(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    limit: Optional[int] = Query(None, ge=1, le=5000),
) -> List[Dict[str, Any]]:
    repo = PricingGroupsRepo(db)
    docs = await repo.list_for_user(user_id, limit=limit)
    return [_stringify_id(d) for d in docs]


@router.get("/group/{group_id}")
async def get_group_by_id(
    group_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> Dict[str, Any]:
    oid = _parse_object_id(group_id)
    repo = PricingGroupsRepo(db)
    doc = await repo.get_by_object_id(oid)
    if not doc:
        raise NotFoundError(code="pricing_group_not_found", message="pricing_group not found")
    return _stringify_id(doc)

