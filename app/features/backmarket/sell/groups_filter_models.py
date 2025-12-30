# app/features/backmarket/sell/groups_filter_models.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


def _clean_str(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # Swagger placeholder guard. If someone *really* has sku == "string" they can still pass it by disabling this.
    if s.lower() == "string":
        return None
    return s


def _clean_str_list(v: Optional[List[str]]) -> List[str]:
    out: List[str] = []
    for x in v or []:
        s = _clean_str(x)
        if s:
            out.append(s)
    # de-dupe preserving order
    seen = set()
    uniq: List[str] = []
    for s in out:
        if s not in seen:
            uniq.append(s)
            seen.add(s)
    return uniq


class GroupsFilterRequest(BaseModel):
    """
    Filter for pricing_groups queries.

    Note:
    - storage_gb is Optional[int] with ge=1 (so swagger won't default to 0 and accidentally filter out everything)
    - model + models get merged into a single $in filter
    - group_keys maps to pricing_groups.group_key
    """

    brand: Optional[str] = None
    model: Optional[str] = None
    models: List[str] = Field(default_factory=list)

    storage_gb: Optional[int] = Field(default=None, ge=1)

    trade_sku: Optional[str] = None
    group_keys: List[str] = Field(default_factory=list)

    def to_mongo_filter(self) -> Dict[str, Any]:
        q: Dict[str, Any] = {}

        brand = _clean_str(self.brand)
        if brand:
            q["brand"] = brand.upper()

        trade_sku = _clean_str(self.trade_sku)
        if trade_sku:
            q["trade_sku"] = trade_sku.upper()

        # model + models -> $in
        ms: List[str] = []
        m1 = _clean_str(self.model)
        if m1:
            ms.append(m1.upper())
        ms.extend([m.upper() for m in _clean_str_list(self.models)])

        # de-dupe again after upper()
        ms = list(dict.fromkeys(ms))
        if len(ms) == 1:
            q["model"] = ms[0]
        elif len(ms) > 1:
            q["model"] = {"$in": ms}

        if self.storage_gb is not None:
            q["storage_gb"] = int(self.storage_gb)

        gks = [g.upper() for g in _clean_str_list(self.group_keys)]
        if len(gks) == 1:
            q["group_key"] = gks[0]
        elif len(gks) > 1:
            q["group_key"] = {"$in": gks}

        return q
