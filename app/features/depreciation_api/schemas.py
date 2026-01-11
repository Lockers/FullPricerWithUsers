from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict

from .types import Grade, MultiplierScope, Segment


class DepreciationModelUpsertRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    user_id: str
    market: str = Field(default="GB", description="Market code, e.g. GB")
    brand: str
    model: str
    storage_gb: int = Field(ge=1)

    release_date: date
    msrp_amount: float = Field(gt=0, description="Launch MSRP in local currency (GBP for GB)")
    currency: str = Field(default="GBP")

    segment: Optional[Segment] = Field(
        default=None,
        description="Optional override; if omitted we derive from brand/model.",
    )


class DepreciationModelResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    id: str = Field(description="Mongo ObjectId as string")
    user_id: str
    market: str
    brand: str
    model: str
    storage_gb: int

    release_date: date
    msrp_amount: float
    currency: str
    segment: Segment


class DepreciationEstimateRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    user_id: str
    market: str = "GB"

    brand: str
    model: str
    storage_gb: int = Field(ge=1)

    # If you want to override catalog for testing.
    release_date: Optional[date] = None
    msrp_amount: Optional[float] = Field(default=None, gt=0)

    grade: Grade = Grade.EXCELLENT
    as_of_date: Optional[date] = None

    # If you want a forced segment override for debugging.
    segment: Optional[Segment] = None

    # Optional: force multiplier scope selection
    scope: Optional[MultiplierScope] = None


class DepreciationEstimateResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    market: str
    brand: str
    model: str
    storage_gb: int

    segment: Segment
    grade: Grade

    release_date: date
    as_of_date: date
    age_months: float

    msrp_amount: float
    currency: str

    curve_version: str
    retention_excellent: float

    multiplier: float
    multiplier_source: str

    predicted_excellent: float
    predicted_good: float
    predicted_fair: float
    predicted_for_grade: float

    depreciation_cost_for_grade: float


class ComputePricingGroupDepreciationRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    # Optional: verify group belongs to this user
    user_id: Optional[str] = None

    as_of_date: Optional[date] = None
    persist: bool = True
    scope: Optional[MultiplierScope] = None


class ComputePricingGroupDepreciationResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    pricing_group_id: str
    trade_sku: str
    user_id: str
    market: str

    stored_field: str
    persisted: bool

    estimate: DepreciationEstimateResponse


class DepreciationObserveRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    user_id: str
    market: str = "GB"

    # Provide either a sku string OR brand/model/storage
    sku: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    storage_gb: Optional[int] = Field(default=None, ge=1)

    observed_price: float = Field(gt=0)
    observed_at: Optional[date] = None
    grade: Grade = Grade.EXCELLENT

    scope: Optional[MultiplierScope] = None
    weight: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class DepreciationObserveResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    key: str
    scope: MultiplierScope

    previous_multiplier: float
    updated_multiplier: float

    n: int
    applied_weight: float

    target_multiplier: float

    # Debug / trace
    base_pred_excellent: float
    observed_excellent_equiv: float
    segment: Segment


class ObserveFromOrderlineRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    user_id: str
    market: str = "GB"
    scope: Optional[MultiplierScope] = None
    weight: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class ObserveFromOrderlineResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    orderline_id: str
    listing_sku: str
    observed_price: float
    observed_at: datetime

    result: DepreciationObserveResponse


class ObserveFromOrderRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    user_id: str
    market: str = "GB"
    scope: Optional[MultiplierScope] = None
    weight: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class ObserveFromOrderResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    order_id: str
    observed_at: datetime
    results: list[DepreciationObserveResponse]
