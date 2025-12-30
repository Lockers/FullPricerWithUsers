from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.features.backmarket.sell.schemas import PricingGroupsFilterIn


def _norm_upper(s: Optional[str]) -> Optional[str]:
    """
    Normalize user input to match pricing_groups storage:

      - strip
      - uppercase
      - collapse repeated whitespace

    Example:
      "  iPhone   13 Pro " -> "IPHONE 13 PRO"
    """
    if not s:
        return None
    return " ".join(str(s).strip().upper().split())


def groups_filter_from_selection(selection: Optional[PricingGroupsFilterIn]) -> Optional[Dict[str, Any]]:
    """
    Convert SellPricingSelection -> Mongo filter (excluding user_id).

    Notes:
    - If selection is None or empty, returns None (meaning "no filtering").
    - We only generate simple filters (and $in when multiple models/keys are supplied).
    - The activation/pricing services will merge this with {"user_id": user_id}.
    """
    if selection is None:
        return None

    f: Dict[str, Any] = {}

    brand = _norm_upper(selection.brand)
    if brand:
        f["brand"] = brand

    models: List[str] = []
    if selection.model:
        models.append(selection.model)
    if selection.models:
        models.extend(selection.models)

    models_u = [_norm_upper(m) for m in models]
    models_u = [m for m in models_u if m]

    if models_u:
        f["model"] = models_u[0] if len(models_u) == 1 else {"$in": models_u}

    if selection.storage_gb is not None:
        f["storage_gb"] = int(selection.storage_gb)

    if selection.trade_sku:
        f["trade_sku"] = str(selection.trade_sku).strip()

    if selection.group_keys:
        keys = [str(k).strip() for k in selection.group_keys if str(k).strip()]
        if keys:
            f["group_key"] = keys[0] if len(keys) == 1 else {"$in": keys}

    return f or None
