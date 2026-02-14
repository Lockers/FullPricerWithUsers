from __future__ import annotations

from typing import Literal, Optional, List

from pydantic import BaseModel, Field, model_validator


FeeType = Literal["percent", "fixed"]

# How the sell anchor is selected/applied.
AnchorMode = Literal["manual_only", "auto"]

# How we aggregate per-listing Backbox history within the lookback window.
BackboxMetric = Literal["min", "median", "ewma"]

# Optional guardrail around manual anchors.
ManualGuardrailMode = Literal["off", "clamp", "block"]


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
        default="auto",
        description=(
            "Anchor selection strategy. 'manual_only' requires a manual_sell_anchor_gross for the group "
            "(otherwise we mark needs_manual_anchor and pricing will not update offers). "
            "'auto' derives a suggested anchor from recent sold + Backbox + depreciation, and may require manual "
            "input when data is sparse."
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
        description="Minimum number of sales in the lookback window before we treat sales as a viable signal.",
    )

    # Backbox robustness
    backbox_lookback_days: int = Field(
        default=14,
        ge=1,
        le=365,
        description="Use Backbox history within this lookback window (days) when deriving Backbox signals.",
    )
    backbox_outlier_pct: float = Field(
        default=0.20,
        ge=0.0,
        le=0.9,
        description="Used for UI/debug. Historically used to skip extreme low outliers for a 'min' anchor.",
    )

    backbox_metric: BackboxMetric = Field(
        default="median",
        description=(
            "Aggregation method for Backbox history within backbox_lookback_days. "
            "'min' is most aggressive (lowest recent value), 'median' is most robust, "
            "and 'ewma' emphasizes the most recent samples."
        ),
    )
    backbox_ewma_alpha: float = Field(
        default=0.30,
        ge=0.05,
        le=0.95,
        description="EWMA alpha (0..1) used when backbox_metric='ewma'. Higher => more weight on the latest value.",
    )

    # Minimum number of viable child prices required before we trust Backbox-derived values for auto.
    min_viable_child_prices: int = Field(default=5, ge=1, le=50)

    # Blend weights (used when multiple signals exist).
    # These do NOT need to sum to 1; they are normalized in code.
    sold_weight: float = Field(default=0.60, ge=0.0, le=1.0)
    backbox_weight: float = Field(default=0.35, ge=0.0, le=1.0)
    depreciation_weight: float = Field(default=0.05, ge=0.0, le=1.0)

    # Baseline guard: prevent suggested/used sell anchors from being too far above the lowest-ever observed value.
    baseline_cap_enabled: bool = Field(default=True)
    baseline_max_above_lowest_pct: float = Field(
        default=0.25,
        ge=0.0,
        le=5.0,
        description="Cap is lowest_ever * (1 + baseline_max_above_lowest_pct).",
    )

    # Manual baseline rule for sparse Backbox coverage (< min_viable_child_prices).
    require_manual_below_min_backboxes: bool = Field(default=True)
    manual_baseline_weight: float = Field(
        default=0.60,
        ge=0.0,
        le=1.0,
        description="When manual baseline is applied: used = w*manual + (1-w)*suggested.",
    )

    # Manual guardrail (optional) - keeps auto-derived anchors within a band around manual.
    manual_guardrail_mode: ManualGuardrailMode = Field(default="off")
    manual_guardrail_pct: float = Field(default=0.25, ge=0.0, le=5.0)

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

        w_sum = float(self.sold_weight) + float(self.backbox_weight) + float(self.depreciation_weight)
        if w_sum <= 0.0:
            raise ValueError("At least one of sold_weight/backbox_weight/depreciation_weight must be > 0")

        return self
