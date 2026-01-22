from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase

from .types import MultiplierScope


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc_datetime(v: Any) -> datetime:
    """
    Mongo BSON cannot encode datetime.date. Convert to timezone-aware datetime.
    Accepts:
      - datetime (kept, ensured tz)
      - date (converted to 00:00:00 UTC)
      - ISO string (YYYY-MM-DD or full ISO datetime)
    """
    if isinstance(v, datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=timezone.utc)

    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)

    if isinstance(v, str):
        # Handles "YYYY-MM-DD" and full ISO strings
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

    raise TypeError(f"Unsupported release_date type: {type(v)!r}")


def default_weight(n: int) -> float:
    w = 1.0 / (n + 1)
    return max(0.05, min(1.0, w))


async def upsert_depreciation_model(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str,
    brand: str,
    model: str,
    storage_gb: int,
    release_date: Any,
    msrp_amount: float,
    currency: str,
    segment: str,
) -> Dict[str, Any]:
    col = db["depreciation_models"]
    now = _now()

    filt = {
        "user_id": user_id,
        "market": market,
        "brand": brand,
        "model": model,
        "storage_gb": storage_gb,
    }

    release_dt = _as_utc_datetime(release_date)

    update = {
        "$set": {
            "release_date": release_dt,  # <-- datetime, BSON-safe
            "msrp_amount": float(msrp_amount),
            "currency": currency,
            "segment": segment,
            "updated_at": now,
        },
        "$setOnInsert": {"created_at": now},
    }

    await col.update_one(filt, update, upsert=True)
    doc = await col.find_one(filt)
    return doc  # type: ignore[return-value]


async def get_depreciation_model(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str,
    brand: str,
    model: str,
    storage_gb: int,
) -> Optional[Dict[str, Any]]:
    return await db["depreciation_models"].find_one(
        {
            "user_id": user_id,
            "market": market,
            "brand": brand,
            "model": model,
            "storage_gb": storage_gb,
        }
    )


def build_multiplier_keys(
    *,
    market: str,
    brand: str,
    model: str,
    storage_gb: int,
    segment: str,
) -> List[str]:
    sku_key = f"sku:{market}:{brand}|{model}|{storage_gb}"
    model_key = f"model:{market}:{brand}|{model}"
    segment_key = f"segment:{market}:{segment}"
    global_key = f"global:{market}"
    return [sku_key, model_key, segment_key, global_key]


async def resolve_multiplier(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str,
    keys: List[str],
) -> Tuple[float, str]:
    col = db["depreciation_multipliers"]
    docs = await col.find({"user_id": user_id, "market": market, "key": {"$in": keys}}).to_list(
        length=len(keys)
    )
    by_key = {d["key"]: d for d in docs}

    for k in keys:
        d = by_key.get(k)
        if d:
            log_m = float(d.get("log_m", 0.0))
            return float(math.exp(log_m)), k

    return 1.0, "none"


async def update_multiplier_log_ewma(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str,
    key: str,
    scope: MultiplierScope,
    target_multiplier: float,
    weight: Optional[float],
) -> Dict[str, Any]:
    col = db["depreciation_multipliers"]
    now = _now()

    doc = await col.find_one({"user_id": user_id, "market": market, "key": key})
    old_log = float(doc.get("log_m", 0.0)) if doc else 0.0
    old_n = int(doc.get("n", 0)) if doc else 0

    w = float(weight) if weight is not None else default_weight(old_n)
    w = max(0.0, min(1.0, w))

    target_multiplier = max(float(target_multiplier), 1e-9)
    log_target = math.log(target_multiplier)

    new_log = (1 - w) * old_log + w * log_target
    new_n = old_n + 1

    await col.update_one(
        {"user_id": user_id, "market": market, "key": key},
        {
            "$set": {
                "scope": scope.value,
                "log_m": float(new_log),
                "n": int(new_n),
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )

    return {
        "previous_multiplier": float(math.exp(old_log)),
        "updated_multiplier": float(math.exp(new_log)),
        "n": new_n,
        "applied_weight": w,
        "target_multiplier": target_multiplier,
    }

