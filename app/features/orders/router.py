# app/features/orders/router.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongo import get_db
from app.features.orders.service import sync_bm_orders_for_user

router = APIRouter(prefix="/users/{user_id}/orders", tags=["orders"])


@router.post("/sync")
async def sync_orders_endpoint(
    user_id: str,
    full: bool = Query(False, description="If true: ignore cursor and fetch all pages."),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    # Returns only a summary, not the order data.
    return await sync_bm_orders_for_user(db, user_id=user_id, full=full)
