# app/features/backmarket/sell/pricing_cycle_router.py
"""
Endpoints to run the sell pricing cycle.

Supports:
- full pricing cycle (no selectors)
- selected-model pricing cycle (selectors provided)

This endpoint is intentionally thin; all logic lives in pricing_select_service.py.
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Body, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongo import get_db
from app.features.backmarket.sell.pricing_select_schemas import SellPricingCycleRequest
from app.features.backmarket.sell.pricing_select_service import run_sell_pricing_cycle_selected

router = APIRouter(prefix="/pricing-cycle", tags=["bm-sell-pricing"])


@router.post("/run/{user_id}")
async def run_sell_pricing_cycle(
    user_id: str,
    payload: SellPricingCycleRequest = Body(default=SellPricingCycleRequest()),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> Dict[str, Any]:
    """
    Run the sell pricing cycle.

    Body examples:

    Full run (all groups):
      {}

    Selected model run:
      {
        "selectors": [{"brand": "APPLE", "model": "iPhone 13 Pro"}],
        "match_mode": "exact",
        "wait_seconds": 180
      }
    """
    return await run_sell_pricing_cycle_selected(db, user_id, payload)
