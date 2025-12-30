# app/features/backmarket/sell/pricing_select_service.py
"""
Run a sell pricing cycle for a subset of models.

This is a thin wrapper around run_sell_pricing_cycle_for_user() that builds a
pricing_groups filter from (brand/model/storage) selectors.

Important:
- Selection is applied at the *pricing_groups* level.
- Activation session is built only from the selected groups’ children.
- All Back Market calls still use endpoint_key => rate limiter + learner apply.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.sell.pricing_cycle import run_sell_pricing_cycle_for_user
from app.features.backmarket.sell.pricing_select_schemas import SellPricingCycleRequest, SellPricingSelector


def _upper(s: Optional[str]) -> str:
    return (s or "").strip().upper()


def _model_regex(model: str, *, match_mode: str) -> Dict[str, Any]:
    """
    Build a case-insensitive regex for pricing_groups.model.

    We allow whitespace/hyphen variance:
      "iPhone 13 Pro" matches "IPHONE-13-PRO" or "IPHONE 13 PRO"

    match_mode:
      - exact:    anchors ^...$
      - contains: no anchors
    """
    tokens = [t for t in re.split(r"[\s\-]+", model.strip()) if t]
    # allow one-or-more spaces/hyphens between tokens
    sep = r"[\s\-]+"
    pat = sep.join(re.escape(t) for t in tokens)
    if match_mode == "exact":
        pat = f"^{pat}$"

    return {"$regex": pat, "$options": "i"}


def build_pricing_groups_filter(
    selectors: List[SellPricingSelector],
    *,
    match_mode: str,
) -> Dict[str, Any]:
    """
    Convert selectors into a Mongo filter fragment for pricing_groups.

    Returns a filter fragment (does NOT include user_id).
    """
    clauses: List[Dict[str, Any]] = []

    for sel in selectors:
        clause: Dict[str, Any] = {}

        if sel.brand:
            clause["brand"] = _upper(sel.brand)

        clause["model"] = _model_regex(sel.model, match_mode=match_mode)

        if sel.storage_gb is not None:
            clause["storage_gb"] = int(sel.storage_gb)

        clauses.append(clause)

    if len(clauses) == 1:
        return clauses[0]

    return {"$or": clauses}


async def run_sell_pricing_cycle_selected(
    db: AsyncIOMotorDatabase,
    user_id: str,
    req: SellPricingCycleRequest,
) -> Dict[str, Any]:
    """
    Run pricing cycle using selection if provided.

    If req.selectors is falsy => full run.
    """
    groups_filter: Optional[Dict[str, Any]] = None
    selection_meta: Optional[Dict[str, Any]] = None

    if req.selectors:
        groups_filter = build_pricing_groups_filter(req.selectors, match_mode=req.match_mode)
        selection_meta = {
            "match_mode": req.match_mode,
            "selectors": [s.model_dump() for s in req.selectors],
            "groups_filter": groups_filter,
        }

    res = await run_sell_pricing_cycle_for_user(
        db,
        user_id,
        wait_seconds=req.wait_seconds,
        market=req.market,
        max_backbox_attempts=req.max_backbox_attempts,
        max_parallel_backbox=req.max_parallel_backbox,
        max_parallel_deactivate=req.max_parallel_deactivate,
        groups_filter=groups_filter,
    )

    # attach selection info for debugging (non-breaking)
    res["selection"] = selection_meta
    return res
