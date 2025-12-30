from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict

from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _norm_upper(s: str) -> str:
    return str(s).strip().upper()


def _norm_model(s: str) -> str:
    # Match the canonicalization you use in repaircost.service
    return " ".join(str(s).strip().upper().split())


async def apply_repair_cost_to_pricing_groups(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str,
    brand: str,
    model: str,
    repair_cost_doc: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Set pricing_groups.repair_costs snapshot for ALL matching pricing_groups docs.

    Attempts market-filtered update first; if it matches 0 docs, falls back to
    user+brand+model without market filtering (useful if markets field is missing).
    """
    mkt = _norm_upper(market)
    b = _norm_upper(brand)
    md = _norm_model(model)

    now = _now_utc()

    costs = (repair_cost_doc.get("costs") or {})
    currency = _norm_upper(repair_cost_doc.get("currency") or "GBP")

    snapshot = {
        "market": mkt,
        "currency": currency,
        "costs": costs,
        "source_updated_at": repair_cost_doc.get("updated_at"),
        "applied_at": now,
    }

    update = {
        "$set": {
            "repair_costs": snapshot,
            "updated_at": now,
        }
    }

    q_market = {
        "user_id": user_id,
        "brand": b,
        "model": md,
        "$or": [{"markets": mkt}, {"tradein_listing.markets": mkt}],
    }

    res = await db["pricing_groups"].update_many(q_market, update)

    if int(res.matched_count or 0) == 0:
        logger.warning(
            "[repair_costs_apply] market-filter matched 0; falling back to unfiltered user_id=%s brand=%s model=%s market=%s",
            user_id,
            b,
            md,
            mkt,
        )
        q_fallback = {"user_id": user_id, "brand": b, "model": md}
        res2 = await db["pricing_groups"].update_many(q_fallback, update)
        return {
            "matched": int(res2.matched_count or 0),
            "modified": int(res2.modified_count or 0),
            "fallback_used": True,
        }

    return {
        "matched": int(res.matched_count or 0),
        "modified": int(res.modified_count or 0),
        "fallback_used": False,
    }


async def clear_repair_cost_from_pricing_groups(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str,
    brand: str,
    model: str,
) -> Dict[str, Any]:
    """
    Remove pricing_groups.repair_costs for matching docs (used on delete).
    """
    mkt = _norm_upper(market)
    b = _norm_upper(brand)
    md = _norm_model(model)

    now = _now_utc()

    update = {
        "$unset": {"repair_costs": ""},
        "$set": {"updated_at": now},
    }

    q_market = {
        "user_id": user_id,
        "brand": b,
        "model": md,
        "$or": [{"markets": mkt}, {"tradein_listing.markets": mkt}],
    }

    res = await db["pricing_groups"].update_many(q_market, update)

    if int(res.matched_count or 0) == 0:
        q_fallback = {"user_id": user_id, "brand": b, "model": md}
        res2 = await db["pricing_groups"].update_many(q_fallback, update)
        return {
            "matched": int(res2.matched_count or 0),
            "modified": int(res2.modified_count or 0),
            "fallback_used": True,
        }

    return {
        "matched": int(res.matched_count or 0),
        "modified": int(res.modified_count or 0),
        "fallback_used": False,
    }
