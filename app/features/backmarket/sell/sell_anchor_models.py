from __future__ import annotations

from typing import Literal, Optional, List
from pydantic import BaseModel, Field, model_validator


FeeType = Literal["percent", "fixed"]


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
    safe_ratio: float = 0.85
    fee_config: FeeConfig = Field(default_factory=FeeConfig)

    @model_validator(mode="after")
    def _validate_settings(self) -> "SellAnchorSettings":
        # keep it simple: allow 0..1.0
        if not (0 < float(self.safe_ratio) <= 1.0):
            raise ValueError("safe_ratio must be in (0, 1]")
        return self

