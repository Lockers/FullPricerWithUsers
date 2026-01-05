# app/features/backmarket/pricing/pricing_overrides_schemas.py

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ManualSellAnchorGrossUpdate(BaseModel):
    """Set/clear manual gross sell anchor for a pricing group.

    - If manual_sell_anchor_gross is provided (>0), it overrides auto sell anchors.
    - If null, the manual override is cleared.
    """

    manual_sell_anchor_gross: Optional[float] = Field(default=None, gt=0)
    market: str = Field(default="GB", min_length=2, max_length=4)
    currency: str = Field(default="GBP", min_length=3, max_length=3)


class ProfitTargetUpdate(BaseModel):
    """Set/clear profit target (GBP) for a pricing group.

    - If profit_target_gbp is provided (>0), it overrides the global default.
    - If null, the group will fall back to the global default in the pricing calc.
    """

    profit_target_gbp: Optional[float] = Field(default=None, gt=0)
    market: str = Field(default="GB", min_length=2, max_length=4)
    currency: str = Field(default="GBP", min_length=3, max_length=3)
