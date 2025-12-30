# app/features/backmarket/sell/pricing_select_schemas.py
"""
Schemas for running a SELL pricing cycle on a subset of models.

Selection is applied to pricing_groups (NOT bm_sell_listings directly).

Why pricing_groups?
- It's your canonical “which listings matter” graph.
- It already encodes your model parsing + child attachment logic.
"""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class SellPricingSelector(BaseModel):
    """
    Select a device "family" to price.

    Matches pricing_groups fields:
      - brand (stored uppercase)
      - model (stored uppercase, derived from SKU parsing)
      - storage_gb (int, optional)

    model is required (e.g. "iPhone 13 Pro").
    brand is optional but recommended (e.g. "APPLE") to avoid accidental matches.
    """
    brand: Optional[str] = Field(default=None, min_length=1)
    model: str = Field(..., min_length=1)
    storage_gb: Optional[int] = Field(default=None, ge=0)


class SellPricingCycleRequest(BaseModel):
    """
    Run the sell pricing cycle.

    If selectors is omitted/empty => full run (all pricing_groups).
    If selectors provided         => only pricing_groups matching those selectors are used.
    """
    selectors: Optional[List[SellPricingSelector]] = Field(default=None)

    match_mode: Literal["exact", "contains"] = "exact"

    wait_seconds: int = Field(default=180, ge=0, le=3600)
    market: str = Field(default="GB", min_length=2, max_length=2)

    max_backbox_attempts: int = Field(default=3, ge=1, le=10)
    max_parallel_backbox: int = Field(default=4, ge=1, le=64)
    max_parallel_deactivate: int = Field(default=8, ge=1, le=64)
