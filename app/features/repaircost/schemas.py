from __future__ import annotations

from datetime import datetime
from typing import Optional, Annotated

from pydantic import BaseModel, Field

Money = Annotated[float, Field(ge=0.0)]


class RepairCosts(BaseModel):
    screen_refurb: Money
    screen_replacement: Money
    battery: Money
    rear_cover: Money
    full_housing: Money


class RepairCostsPatch(BaseModel):
    screen_refurb: Optional[Money] = None
    screen_replacement: Optional[Money] = None
    battery: Optional[Money] = None
    rear_cover: Optional[Money] = None
    full_housing: Optional[Money] = None


class RepairCostUpsert(BaseModel):
    """
    Upsert payload for a SINGLE device family.
    Key is (user_id from path, market, brand, model).
    """
    market: Annotated[str, Field(default="GB", min_length=2, max_length=4)]
    currency: Annotated[str, Field(default="GBP", min_length=3, max_length=3)]
    brand: Annotated[str, Field(min_length=1, max_length=64)]
    model: Annotated[str, Field(min_length=1, max_length=64)]

    costs: RepairCosts
    notes: Optional[str] = None


class RepairCostPatchRequest(BaseModel):
    """
    PATCH payload: identify record by (market, brand, model) and patch fields.
    """
    market: Annotated[str, Field(default="GB", min_length=2, max_length=4)]
    brand: Annotated[str, Field(min_length=1, max_length=64)]
    model: Annotated[str, Field(min_length=1, max_length=64)]

    currency: Optional[Annotated[str, Field(min_length=3, max_length=3)]] = None
    costs: Optional[RepairCostsPatch] = None
    notes: Optional[str] = None


class RepairCostRead(BaseModel):
    user_id: str
    market: str
    currency: str
    brand: str
    model: str
    costs: RepairCosts
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class RepairModelStatus(BaseModel):
    brand: str
    model: str
    groups_count: int
    has_repair_cost: bool
    repair_cost_updated_at: Optional[datetime] = None
    currency: Optional[str] = None


class RepairModelsResponse(BaseModel):
    user_id: str
    market: str
    count: int
    items: list[RepairModelStatus]

