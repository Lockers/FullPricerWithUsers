from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class TradePricingSettings(BaseModel):
    """User-level defaults."""
    default_required_profit: float = Field(75.0, ge=0)
    default_required_margin: Optional[float] = Field(None, ge=0, le=1)
    vat_rate: float = Field(0.1667, ge=0, le=1)


class TradePricingSettingsIn(BaseModel):
    """PUT body for user settings."""
    default_required_profit: float = Field(75.0, ge=0)
    default_required_margin: Optional[float] = Field(None, ge=0, le=1)
    vat_rate: float = Field(0.1667, ge=0, le=1)


class TradePricingGroupSettingsIn(BaseModel):
    """PATCH body for group overrides.

    Patch semantics are handled in the service via `exclude_unset=True`:
      - omit field => no change
      - explicit null => clear
    """
    required_profit: Optional[float] = Field(None, ge=0)
    required_margin: Optional[float] = Field(None, ge=0, le=1)

