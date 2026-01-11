"""
/pricing-groups endpoints.

These endpoints build and inspect the grouping "backbone" used by pricing.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError

from app.core.errors import BadRequestError, NotFoundError
from app.db.mongo import get_db
from app.features.backmarket.pricing_groups.repo import PricingGroupsRepo
from app.features.backmarket.pricing_groups.service import build_pricing_groups_for_user

router = APIRouter(prefix="/pricing-groups", tags=["pricing-groups"])


def _parse_object_id(group_id: str) -> ObjectId:
    try:
        return ObjectId(group_id)
    except (InvalidId, TypeError) as exc:
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


# ---------------------------------------------------------------------------
# Diagnostics / quick fixes (indexes)
# ---------------------------------------------------------------------------

def _to_jsonable_key_spec(key_spec: Any) -> List[List[Any]]:
    out: List[List[Any]] = []
    if isinstance(key_spec, list):
        for it in key_spec:
            if isinstance(it, (list, tuple)) and len(it) == 2:
                out.append([it[0], int(it[1])])
    return out


async def _pricing_groups_index_diag(db: AsyncIOMotorDatabase) -> Dict[str, Any]:
    col = db["pricing_groups"]
    info = await col.index_information()

    indexes: List[Dict[str, Any]] = []
    bad_unique: List[Dict[str, Any]] = []

    for name, spec in info.items():
        key_spec = _to_jsonable_key_spec(spec.get("key"))
        unique = bool(spec.get("unique", False))

        indexes.append({"name": name, "key": key_spec, "unique": unique})

        if name != "_id_" and unique:
            fields = [k for k, _v in key_spec]
            if "user_id" not in fields:
                bad_unique.append({"name": name, "key": key_spec})

    has_expected_unique = any(
        (idx["unique"] is True and idx["key"] == [["user_id", 1], ["trade_sku", 1]]) for idx in indexes
    )

    return {
        "collection": "pricing_groups",
        "indexes": indexes,
        "bad_unique": bad_unique,
        "has_expected_unique_user_trade_sku": has_expected_unique,
    }


@router.get("/diag/indexes")
async def pricing_groups_indexes(db: AsyncIOMotorDatabase = Depends(get_db)) -> Dict[str, Any]:
    return await _pricing_groups_index_diag(db)


@router.post("/diag/fix-indexes")
async def pricing_groups_fix_indexes(db: AsyncIOMotorDatabase = Depends(get_db)) -> Dict[str, Any]:
    col = db["pricing_groups"]
    info = await col.index_information()

    dropped: List[str] = []
    errors: List[Dict[str, str]] = []

    for name, spec in info.items():
        if name == "_id_":
            continue
        if not spec.get("unique", False):
            continue

        key_spec = spec.get("key") or []
        fields = [k[0] for k in key_spec] if isinstance(key_spec, list) else []

        if "user_id" not in fields:
            try:
                await col.drop_index(name)
                dropped.append(name)
            except PyMongoError as exc:
                errors.append({"action": "drop_index", "name": name, "error": str(exc)})

    # Ensure expected unique index exists (do not force name to avoid options conflicts)
    try:
        await col.create_index([("user_id", 1), ("trade_sku", 1)], unique=True)
    except PyMongoError as exc:
        errors.append({"action": "create_index", "name": "(user_id,trade_sku) unique", "error": str(exc)})

    return {"ok": True, "dropped": dropped, "errors": errors, "diag": await _pricing_groups_index_diag(db)}


