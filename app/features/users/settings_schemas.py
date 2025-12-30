"""
User settings schemas.

This collection stores:
- BackMarket credentials (bm_api_key token stored RAW, no 'Basic ')
- integration_name + user_agent
- market_language
- limits
- tradein_config guardrails
"""

from __future__ import annotations

from typing import Optional, Annotated

from pydantic import BaseModel, Field


class UserSettingsBase(BaseModel):
    bm_api_key: Optional[str] = None  # RAW token, no "Basic "
    bm_user_id: Optional[str] = None
    bm_seller_id: Optional[str] = None

    integration_name: Optional[str] = None
    user_agent: Optional[str] = None

    market_language: Optional[str] = None

    daily_trade_limit: Optional[int] = None
    daily_spend_limit: Optional[float] = None
    auto_pause_on_limit: bool = False


class UserSettingsUpdate(UserSettingsBase):
    """PATCH model - all optional."""
    pass


class TradeinConfig(BaseModel):
    sell_realism_ratio: Annotated[float, Field(ge=0.0, le=2.0)] = 0.8
    min_trusted_sell_count: Annotated[int, Field(ge=0)] = 5

    default_min_net_margin_pct: Annotated[float, Field(ge=0.0, le=1.0)] = 0.2
    default_min_profit_abs: Annotated[float, Field(ge=0.0)] = 50.0

    default_min_net_margin_pct_cracked: Annotated[float, Field(ge=0.0, le=1.0)] = 0.25
    default_min_profit_abs_cracked: Annotated[float, Field(ge=0.0)] = 60.0

    manual_guardrail_stale_days: Annotated[int, Field(ge=0)] = 7
    cracked_repair_mix_ratio: Annotated[float, Field(ge=0.0, le=1.0)] = 0.6


class TradeinConfigUpdate(BaseModel):
    """
    PATCH model for trade-in guardrails. All fields optional.
    """
    sell_realism_ratio: Annotated[Optional[float], Field(ge=0.0, le=2.0)] = None
    min_trusted_sell_count: Annotated[Optional[int], Field(ge=0)] = None

    default_min_net_margin_pct: Annotated[Optional[float], Field(ge=0.0, le=1.0)] = None
    default_min_profit_abs: Annotated[Optional[float], Field(ge=0.0)] = None

    default_min_net_margin_pct_cracked: Annotated[Optional[float], Field(ge=0.0, le=1.0)] = None
    default_min_profit_abs_cracked: Annotated[Optional[float], Field(ge=0.0)] = None

    manual_guardrail_stale_days: Annotated[Optional[int], Field(ge=0)] = None
    cracked_repair_mix_ratio: Annotated[Optional[float], Field(ge=0.0, le=1.0)] = None


class UserSettingsRead(BaseModel):
    """
    SAFE read model: do not return raw bm_api_key.
    """
    user_id: str
    bm_api_key_set: bool
    bm_api_key_masked: str

    bm_user_id: Optional[str] = None
    bm_seller_id: Optional[str] = None

    integration_name: Optional[str] = None
    user_agent: Optional[str] = None
    market_language: Optional[str] = None

    daily_trade_limit: Optional[int] = None
    daily_spend_limit: Optional[float] = None
    auto_pause_on_limit: bool = False

    tradein_config: TradeinConfig = Field(default_factory=TradeinConfig)
