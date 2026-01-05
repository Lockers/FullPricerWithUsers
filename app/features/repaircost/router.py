from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongo import get_db
from app.features.repaircost.schemas import (
    RepairCostPatchRequest,
    RepairCostRead,
    RepairCostUpsert,
    RepairModelsResponse,
)
from app.features.repaircost.service import (
    delete_repair_cost,
    get_repair_cost_one,
    list_models_status,
    list_repair_costs,
    patch_repair_cost,
    upsert_repair_cost,
)

router = APIRouter(prefix="/repair-costs", tags=["backmarket:repaircost"])


@router.get("/{user_id}", response_model=list[RepairCostRead])
async def list_repair_costs_endpoint(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    market: str | None = Query(None, min_length=2, max_length=4),
    brand: str | None = Query(None),
    model: str | None = Query(None),
    limit: int = Query(5000, ge=1, le=20000),
):
    return await list_repair_costs(db, user_id, market=market, brand=brand, model=model, limit=limit)


@router.get("/{user_id}/one", response_model=RepairCostRead)
async def get_repair_cost_one_endpoint(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    market: str = Query("GB", min_length=2, max_length=4),
    brand: str = Query(..., min_length=1),
    model: str = Query(..., min_length=1),
):
    return await get_repair_cost_one(db, user_id, market=market, brand=brand, model=model)


@router.put("/{user_id}", response_model=RepairCostRead)
async def upsert_repair_cost_endpoint(
    user_id: str,
    payload: RepairCostUpsert,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await upsert_repair_cost(db, user_id, payload)


@router.patch("/{user_id}", response_model=RepairCostRead)
async def patch_repair_cost_endpoint(
    user_id: str,
    payload: RepairCostPatchRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await patch_repair_cost(db, user_id, payload)


@router.delete("/{user_id}")
async def delete_repair_cost_endpoint(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    market: str = Query("GB", min_length=2, max_length=4),
    brand: str = Query(..., min_length=1),
    model: str = Query(..., min_length=1),
):
    await delete_repair_cost(db, user_id, market=market, brand=brand, model=model)
    return {"ok": True}


@router.get("/{user_id}/models", response_model=RepairModelsResponse)
async def list_models_status_endpoint(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    market: str = Query("GB", min_length=2, max_length=4),
    limit: int = Query(5000, ge=1, le=20000),
):
    return await list_models_status(db, user_id, market=market, limit=limit)


