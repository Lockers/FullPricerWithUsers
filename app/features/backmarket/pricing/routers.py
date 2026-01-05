from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongo import get_db
from app.features.backmarket.pricing.trade_pricing_models import (
    TradePricingGroupSettingsIn,
    TradePricingSettingsIn,
)
from app.features.backmarket.pricing.trade_pricing_service import (
    get_trade_pricing_settings_for_user,
    recompute_trade_pricing_for_user,
    update_trade_pricing_settings_for_group,
    update_trade_pricing_settings_for_user,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bm/pricing", tags=["backmarket:pricing"])


@router.get("/trade/{user_id}/settings")
async def get_trade_pricing_settings(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> Dict[str, Any]:
    return await get_trade_pricing_settings_for_user(db, user_id)


@router.put("/trade/{user_id}/settings")
async def put_trade_pricing_settings(
    user_id: str,
    payload: TradePricingSettingsIn,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> Dict[str, Any]:
    return await update_trade_pricing_settings_for_user(db, user_id, payload)


@router.patch("/trade/{user_id}/groups/{group_id}/settings")
async def patch_trade_pricing_group_settings(
    user_id: str,
    group_id: str,
    payload: TradePricingGroupSettingsIn,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> Dict[str, Any]:
    # NOTE: service signature accepts `payload=` (so this call is valid)
    return await update_trade_pricing_settings_for_group(db, user_id=user_id, group_id=group_id, payload=payload)


@router.post("/trade/{user_id}/recompute")
async def post_trade_pricing_recompute(
    user_id: str,
    limit: Optional[int] = Query(None, ge=1),
    max_parallel: int = Query(10, ge=1, le=50),
    include_item_results: bool = Query(False),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> Dict[str, Any]:
    """Recompute trade pricing (max_trade_price) for a user.

    Notes:
    - Intended as a manual/debug endpoint. The normal flow is:
        sell/pricing_cycle and tradein/competitors_service each call this at the end.
    """
    return await recompute_trade_pricing_for_user(
        db,
        user_id,
        limit=limit,
        max_parallel=max_parallel,
        include_item_results=include_item_results,
    )




