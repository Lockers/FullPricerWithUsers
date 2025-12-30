from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


def _clean_str(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip()
    if not s:
        return None
    if s.lower() == "string":
        return None
    return s


def _clean_str_list(v: Optional[List[str]]) -> Optional[List[str]]:
    if not v:
        return None
    out = [_clean_str(x) for x in v]
    out2 = [x for x in out if x]
    return out2 or None


class PricingGroupsFilterIn(BaseModel):
    """
    Filter used to select which pricing_groups to price.

    IMPORTANT:
    - All fields are optional.
    - We deliberately treat Swagger placeholder values ("string", 0) as "not set".
    """

    brand: Optional[str] = Field(None)
    model: Optional[str] = Field(None)
    models: Optional[List[str]] = Field(None)
    storage_gb: Optional[int] = Field(None)
    trade_sku: Optional[str] = Field(None)
    group_keys: Optional[List[str]] = Field(None)

    def to_mongo_filter(self) -> Dict[str, Any]:
        q: Dict[str, Any] = {}

        brand = _clean_str(self.brand)
        model = _clean_str(self.model)
        trade_sku = _clean_str(self.trade_sku)
        models = _clean_str_list(self.models)
        group_keys = _clean_str_list(self.group_keys)

        storage_gb = self.storage_gb
        if storage_gb is not None and int(storage_gb) <= 0:
            storage_gb = None

        if brand:
            q["brand"] = brand
        if storage_gb is not None:
            q["storage_gb"] = int(storage_gb)
        if trade_sku:
            q["trade_sku"] = trade_sku

        merged_models: List[str] = []
        if model:
            merged_models.append(model)
        if models:
            merged_models.extend(models)

        merged_models = sorted(set(merged_models))
        if len(merged_models) == 1:
            q["model"] = merged_models[0]
        elif len(merged_models) > 1:
            q["model"] = {"$in": merged_models}

        # We don’t know if your DB uses group_key or group_keys.
        # Support both safely via $or (only if provided).
        if group_keys:
            q["$or"] = [
                {"group_key": {"$in": group_keys}},
                {"group_keys": {"$in": group_keys}},
            ]

        return q
