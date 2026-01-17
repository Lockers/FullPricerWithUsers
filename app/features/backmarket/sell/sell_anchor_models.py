from __future__ import annotations

from typing import Literal, Optional, List
from pydantic import BaseModel, Field, model_validator


FeeType = Literal["percent", "fixed"]


AnchorMode = Literal["manual_only", "auto"]


class FeeItem(BaseModel):
    key: str
    type: FeeType

    # Optional so fixed items don't need rate, and percent items doesn't need amount.
    rate: Optional[float] = None
    amount: Optional[float] = None

    @model_validator(mode="after")
    def _validate_fee_item(self) -> "FeeItem":
        if self.type == "percent":
            if self.rate is None:
                raise ValueError("FeeItem.rate is required when type='percent'")
            if self.rate < 0:
                raise ValueError("FeeItem.rate must be >= 0")
            if self.amount is not None:
                raise ValueError("FeeItem.amount must be omitted when type='percent'")

        if self.type == "fixed":
            if self.amount is None:
                raise ValueError("FeeItem.amount is required when type='fixed'")
            if self.amount < 0:
                raise ValueError("FeeItem.amount must be >= 0")
            if self.rate is not None:
                raise ValueError("FeeItem.rate must be omitted when type='fixed'")

        return self


class FeeConfig(BaseModel):
    currency: str = "GBP"
    items: List[FeeItem] = Field(default_factory=list)


class SellAnchorSettings(BaseModel):
    anchor_mode: AnchorMode = Field(
        default="manual_only",
        description=(
            "Anchor selection strategy. 'manual_only' requires a manual_sell_anchor_gross for the group "
            "(otherwise we mark needs_manual_anchor and pricing will not update offers). "
            "'auto' uses recent sold prices (if available) then robust Backbox-derived anchors, with manual as fallback."
        ),
    )

    safe_ratio: float = 0.85
    fee_config: FeeConfig = Field(default_factory=FeeConfig)

    # Orders / realised sales
    recent_sold_days: int = Field(
        default=21,
        ge=1,
        le=365,
        description="Use BM order prices within this lookback window (days) as the highest-weight sell signal.",
    )
    recent_sold_min_samples: int = Field(
        default=1,
        ge=1,
        le=50,
        description="Minimum number of sales in the lookback window before we treat sales as a primary anchor.",
    )

    # Backbox robustness
    backbox_lookback_days: int = Field(
        default=14,
        ge=1,
        le=365,
        description="Use Backbox history within this lookback window (days) when deriving auto anchors.",
    )
    backbox_outlier_pct: float = Field(
        default=0.20,
        ge=0.0,
        le=0.9,
        description="If the lowest candidate price is more than this % below the rolling average/median, skip to the next lowest.",
    )

    # Minimum number of viable child prices required for auto pricing.
    # (Previously a constant in sell_anchor_service.py)
    min_viable_child_prices: int = Field(default=5, ge=1, le=50)

    @model_validator(mode="after")
    def _validate_settings(self) -> "SellAnchorSettings":
        # keep it simple: allow 0..1.0
        if not (0 < float(self.safe_ratio) <= 1.0):
            raise ValueError("safe_ratio must be in (0, 1]")
        if int(self.recent_sold_days) < 1:
            raise ValueError("recent_sold_days must be >= 1")
        if int(self.backbox_lookback_days) < 1:
            raise ValueError("backbox_lookback_days must be >= 1")
        if int(self.min_viable_child_prices) < 1:
            raise ValueError("min_viable_child_prices must be >= 1")
        return self

