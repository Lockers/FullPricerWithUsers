"""
User API schemas.

This defines "who the user/company is" (identity + status).
Back Market credentials live in user_settings (separate collection).
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr


class UserBase(BaseModel):
    email: EmailStr
    name: str
    company_name: str  # each user == one company


class UserCreate(UserBase):
    pass


class UserUpdate(BaseModel):
    """PATCH model (all optional)."""
    name: Optional[str] = None
    company_name: Optional[str] = None
    is_active: Optional[bool] = None


class UserRead(UserBase):
    id: str
    is_active: bool = True
    created_at: datetime
    updated_at: datetime


class UserInitRequest(BaseModel):
    """
    One-shot user initialisation:
    - creates user
    - creates user_settings (BM credentials + UA + defaults)
    """

    email: EmailStr
    name: str
    company_name: str

    bm_api_key: str
    bm_user_id: str
    bm_seller_id: str

    program_name: Optional[str] = None
    market_language: Optional[str] = "en-gb"


class UserInitResponse(UserRead):
    bm_credentials_set: bool
    market_language: Optional[str] = None
    integration_name: Optional[str] = None
    user_agent: Optional[str] = None

