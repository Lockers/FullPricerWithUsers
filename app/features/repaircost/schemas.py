from __future__ import annotations

"""
Repair cost schemas.

Costs are per:
  (user_id, market, brand, model)

Model here is the family string already used in pricing_groups.model
(e.g. "16", "15 PRO MAX") and is NOT dependent on color/sim/storage.

Cost categories:
- screen_replacement
- screen_refurb_in_house
- screen_refurb_external
- battery
- housing
"""

from datetime import datetime
from typing import Optional, Annotated

from pydantic import BaseModel, Field

Money = Annotated[float, Field(ge=0.0)]
Minutes = Annotated[float, Field(ge=0.0)]


class RepairCosts(BaseModel):
    screen_replacement: Money
    screen_refurb_in_house: Money
    screen_refurb_external: Money
    battery: Money
    housing: Money


class RepairTimes(BaseModel):
    """Estimated time to perform each repair action, in minutes."""

    screen_replacement: Minutes
    screen_refurb_in_house: Minutes
    screen_refurb_external: Minutes
    battery: Minutes
    housing: Minutes


class RepairCostsPatch(BaseModel):
    screen_replacement: Optional[Money] = None
    screen_refurb_in_house: Optional[Money] = None
    screen_refurb_external: Optional[Money] = None
    battery: Optional[Money] = None
    housing: Optional[Money] = None


class RepairTimesPatch(BaseModel):
    """PATCH variant of RepairTimes (minutes)."""

    screen_replacement: Optional[Minutes] = None
    screen_refurb_in_house: Optional[Minutes] = None
    screen_refurb_external: Optional[Minutes] = None
    battery: Optional[Minutes] = None
    housing: Optional[Minutes] = None


class RepairCostUpsert(BaseModel):
    """
    Upsert payload for a single device family.
    Keyed by: (user_id from path, market, brand, model)
    """
    market: Annotated[str, Field(default="GB", min_length=2, max_length=4)]
    currency: Annotated[str, Field(default="GBP", min_length=3, max_length=3)]
    brand: Annotated[str, Field(min_length=1, max_length=64)]
    model: Annotated[str, Field(min_length=1, max_length=64)]

    costs: RepairCosts
    # Optional for backwards compatibility; if omitted, defaults to all zeros.
    times: Optional[RepairTimes] = None
    notes: Optional[str] = None


class RepairCostPatchRequest(BaseModel):
    """
    PATCH payload: identify a record by (market, brand, model) then patch fields.
    """
    market: Annotated[str, Field(default="GB", min_length=2, max_length=4)]
    brand: Annotated[str, Field(min_length=1, max_length=64)]
    model: Annotated[str, Field(min_length=1, max_length=64)]

    currency: Optional[Annotated[str, Field(min_length=3, max_length=3)]] = None
    costs: Optional[RepairCostsPatch] = None
    times: Optional[RepairTimesPatch] = None
    notes: Optional[str] = None


class RepairCostRead(BaseModel):
    user_id: str
    market: str
    currency: str
    brand: str
    model: str
    costs: RepairCosts
    times: RepairTimes
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


