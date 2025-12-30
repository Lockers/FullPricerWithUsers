"""
/users endpoints:
- CRUD user identity
- init user with BM settings
- CRUD user settings
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongo import get_db
from app.features.users.schemas import UserCreate, UserInitRequest, UserInitResponse, UserRead, UserUpdate
from app.features.users.service import create_user, delete_user, get_user_by_id, list_users, update_user
from app.features.users.settings_schemas import TradeinConfigUpdate, UserSettingsRead, UserSettingsUpdate
from app.features.users.settings_service import (
    get_user_settings,
    init_user_with_bm,
    update_tradein_config,
    update_user_settings,
)

router = APIRouter(prefix="/users", tags=["users"])


@router.post("", response_model=UserRead, status_code=201)
async def create_user_endpoint(payload: UserCreate, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await create_user(db, payload)


@router.get("", response_model=list[UserRead])
async def list_users_endpoint(
    db: AsyncIOMotorDatabase = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
):
    return await list_users(db, limit=limit)


@router.get("/{user_id}", response_model=UserRead)
async def get_user_endpoint(user_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await get_user_by_id(db, user_id)


@router.patch("/{user_id}", response_model=UserRead)
async def update_user_endpoint(user_id: str, payload: UserUpdate, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await update_user(db, user_id, payload)


@router.delete("/{user_id}")
async def delete_user_endpoint(user_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    await delete_user(db, user_id)
    return {"ok": True}


@router.post("/init", response_model=UserInitResponse, status_code=201)
async def init_user_endpoint(payload: UserInitRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await init_user_with_bm(db, payload)


@router.get("/{user_id}/settings", response_model=UserSettingsRead)
async def get_settings_endpoint(user_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await get_user_settings(db, user_id)


@router.patch("/{user_id}/settings", response_model=UserSettingsRead)
async def patch_settings_endpoint(user_id: str, payload: UserSettingsUpdate, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await update_user_settings(db, user_id, payload)


@router.patch("/{user_id}/tradein-config", response_model=UserSettingsRead)
async def patch_tradein_config_endpoint(
    user_id: str, payload: TradeinConfigUpdate, db: AsyncIOMotorDatabase = Depends(get_db)
):
    return await update_tradein_config(db, user_id, payload)

