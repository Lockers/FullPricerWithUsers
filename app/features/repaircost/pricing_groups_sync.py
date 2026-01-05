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
    return " ".join(str(s).strip().upper().split())


async def apply_repair_cost_snapshot_to_pricing_groups(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    repair_cost_doc: Dict[str, Any],
) -> None:
    """
    Apply repair cost snapshot to ALL pricing_groups rows for (user_id, brand, model).
    Called after upsert/patch of pricing_repair_costs.
    """
    now = _now_utc()

    market = _norm_upper(repair_cost_doc.get("market") or "GB")
    currency = _norm_upper(repair_cost_doc.get("currency") or "GBP")
    brand = _norm_upper(repair_cost_doc.get("brand") or "")
    model = _norm_model(repair_cost_doc.get("model") or "")

    if not brand or not model:
        logger.warning("[repair_costs_apply] missing brand/model user_id=%s", user_id)
        return

    snapshot = {
        "market": market,
        "currency": currency,
        "costs": (repair_cost_doc.get("costs") or {}),
        "source_updated_at": repair_cost_doc.get("updated_at"),
        "applied_at": now,
    }

    res = await db["pricing_groups"].update_many(
        {"user_id": user_id, "brand": brand, "model": model},
        {"$set": {"repair_costs": snapshot, "updated_at": now}},
    )

    logger.info(
        "[repair_costs_apply] user_id=%s brand=%s model=%s matched=%s modified=%s",
        user_id,
        brand,
        model,
        int(res.matched_count or 0),
        int(res.modified_count or 0),
    )


async def clear_repair_cost_snapshot_from_pricing_groups(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    brand: str,
    model: str,
) -> None:
    """
    Remove repair_costs snapshot from pricing_groups rows for (user_id, brand, model).
    Called after delete of pricing_repair_costs.
    """
    now = _now_utc()
    b = _norm_upper(brand)
    md = _norm_model(model)

    res = await db["pricing_groups"].update_many(
        {"user_id": user_id, "brand": b, "model": md},
        {"$unset": {"repair_costs": ""}, "$set": {"updated_at": now}},
    )

    logger.info(
        "[repair_costs_clear] user_id=%s brand=%s model=%s matched=%s modified=%s",
        user_id,
        b,
        md,
        int(res.matched_count or 0),
        int(res.modified_count or 0),
    )

