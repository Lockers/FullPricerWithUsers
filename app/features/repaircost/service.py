from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.errors import NotFoundError
from app.features.repaircost.pricing_groups_sync import (
    apply_repair_cost_snapshot_to_pricing_groups,
    clear_repair_cost_snapshot_from_pricing_groups,
)
from app.features.repaircost.repo import RepairCostsRepo
from app.features.repaircost.schemas import (
    RepairCostPatchRequest,
    RepairCostRead,
    RepairCostUpsert,
    RepairModelStatus,
    RepairModelsResponse,
)


def _norm_upper(s: str) -> str:
    return str(s).strip().upper()


def _norm_market(s: str) -> str:
    return _norm_upper(s)


def _norm_currency(s: str) -> str:
    return _norm_upper(s)


def _norm_brand(s: str) -> str:
    return _norm_upper(s)


def _norm_model(s: str) -> str:
    # Canonicalize spacing + uppercase so joins are stable.
    return " ".join(str(s).strip().upper().split())


def _to_float(x: Any, default: float = 0.0) -> float:
    if isinstance(x, (int, float)):
        v = float(x)
        return v if math.isfinite(v) else default
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return default
        try:
            v = float(s)
            return v if math.isfinite(v) else default
        except ValueError:
            return default
    return default


def _coerce_costs_for_read(costs: Any) -> Dict[str, float]:
    """
    Ensure the costs dict contains ALL required keys for RepairCosts.
    This prevents older docs (from previous schemas) from breaking reads.

    Minimal mapping:
      - housing <- housing OR full_housing
      - screen_refurb_in_house <- screen_refurb_in_house OR screen_refurb
    """
    c = costs if isinstance(costs, dict) else {}

    out = {
        "screen_replacement": _to_float(c.get("screen_replacement"), 0.0),
        "screen_refurb_in_house": _to_float(
            c.get("screen_refurb_in_house", c.get("screen_refurb")), 0.0
        ),
        "screen_refurb_external": _to_float(c.get("screen_refurb_external"), 0.0),
        "battery": _to_float(c.get("battery"), 0.0),
        "housing": _to_float(c.get("housing", c.get("full_housing")), 0.0),
    }
    return out


def _normalize_doc_for_read(doc: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(doc)

    d["market"] = _norm_market(d.get("market") or "GB")
    d["currency"] = _norm_currency(d.get("currency") or "GBP")
    d["brand"] = _norm_brand(d.get("brand") or "")
    d["model"] = _norm_model(d.get("model") or "")
    d["notes"] = d.get("notes")

    d["costs"] = _coerce_costs_for_read(d.get("costs"))

    return d


async def upsert_repair_cost(db: AsyncIOMotorDatabase, user_id: str, payload: RepairCostUpsert) -> RepairCostRead:
    repo = RepairCostsRepo(db)

    mkt = _norm_market(payload.market)
    cur = _norm_currency(payload.currency)
    brand = _norm_brand(payload.brand)
    model = _norm_model(payload.model)

    doc = await repo.upsert(
        user_id=user_id,
        market=mkt,
        currency=cur,
        brand=brand,
        model=model,
        costs=payload.costs.model_dump(),
        notes=payload.notes,
    )

    # Push snapshot into pricing_groups immediately (per user, per model family)
    await apply_repair_cost_snapshot_to_pricing_groups(db, user_id=user_id, repair_cost_doc=doc)

    return RepairCostRead(**_normalize_doc_for_read(doc))


async def list_repair_costs(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    market: Optional[str],
    brand: Optional[str],
    model: Optional[str],
    limit: int,
) -> List[RepairCostRead]:
    repo = RepairCostsRepo(db)

    mkt = _norm_market(market) if market is not None else None
    b = _norm_brand(brand) if brand is not None else None
    md = _norm_model(model) if model is not None else None

    docs = await repo.list_for_user(user_id=user_id, market=mkt, brand=b, model=md, limit=limit)
    return [RepairCostRead(**_normalize_doc_for_read(d)) for d in docs]


async def get_repair_cost_one(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    market: str,
    brand: str,
    model: str,
) -> RepairCostRead:
    repo = RepairCostsRepo(db)

    mkt = _norm_market(market)
    b = _norm_brand(brand)
    md = _norm_model(model)

    doc = await repo.get_one(user_id=user_id, market=mkt, brand=b, model=md)
    if not doc:
        raise NotFoundError(
            code="repair_cost_not_found",
            message="Repair cost not found",
            details={"user_id": user_id, "market": mkt, "brand": b, "model": md},
        )
    return RepairCostRead(**_normalize_doc_for_read(doc))


async def patch_repair_cost(db: AsyncIOMotorDatabase, user_id: str, payload: RepairCostPatchRequest) -> RepairCostRead:
    repo = RepairCostsRepo(db)

    mkt = _norm_market(payload.market)
    b = _norm_brand(payload.brand)
    md = _norm_model(payload.model)

    update: Dict[str, Any] = {}

    if payload.currency is not None:
        update["currency"] = _norm_currency(payload.currency)

    if payload.notes is not None:
        update["notes"] = payload.notes

    if payload.costs is not None:
        costs_patch = payload.costs.model_dump(exclude_unset=True, exclude_none=True)
        for k, v in costs_patch.items():
            update[f"costs.{k}"] = v

    doc = await repo.patch_one(user_id=user_id, market=mkt, brand=b, model=md, update_fields=update)

    # Push snapshot into pricing_groups immediately
    await apply_repair_cost_snapshot_to_pricing_groups(db, user_id=user_id, repair_cost_doc=doc)

    return RepairCostRead(**_normalize_doc_for_read(doc))


async def delete_repair_cost(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    market: str,
    brand: str,
    model: str,
) -> None:
    repo = RepairCostsRepo(db)

    mkt = _norm_market(market)
    b = _norm_brand(brand)
    md = _norm_model(model)

    ok = await repo.delete_one(user_id=user_id, market=mkt, brand=b, model=md)
    if not ok:
        raise NotFoundError(
            code="repair_cost_not_found",
            message="Repair cost not found",
            details={"user_id": user_id, "market": mkt, "brand": b, "model": md},
        )

    # Remove snapshot from pricing_groups
    await clear_repair_cost_snapshot_from_pricing_groups(db, user_id=user_id, brand=b, model=md)


# -----------------------------------------------------------------------------
# Helper: list all distinct (brand, model) pairs present in pricing_groups
# and show if repair costs exist for each.
# -----------------------------------------------------------------------------

async def _list_models_from_pricing_groups(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    market: str,
    limit: int,
) -> List[Dict[str, Any]]:
    """
    Returns docs like:
      {"brand": "...", "model": "...", "groups_count": N}

    Uses a market-filtered aggregation first; if that returns 0, falls back to
    unfiltered (to tolerate schema drift / missing markets).
    """
    mkt = _norm_market(market)

    base_match: Dict[str, Any] = {
        "user_id": user_id,
        "brand": {"$exists": True, "$ne": None},
        "model": {"$exists": True, "$ne": None},
    }

    market_match = dict(base_match)
    market_match["$or"] = [
        {"markets": mkt},
        {"tradein_listing.markets": mkt},
    ]

    def pipeline(match: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            {"$match": match},
            {"$group": {"_id": {"brand": "$brand", "model": "$model"}, "groups_count": {"$sum": 1}}},
            {"$project": {"_id": 0, "brand": "$_id.brand", "model": "$_id.model", "groups_count": 1}},
            {"$sort": {"brand": 1, "model": 1}},
            {"$limit": int(limit)},
        ]

    out: List[Dict[str, Any]] = [doc async for doc in db["pricing_groups"].aggregate(pipeline(market_match))]
    if out:
        return out

    return [doc async for doc in db["pricing_groups"].aggregate(pipeline(base_match))]


async def list_models_status(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    market: str,
    limit: int = 5000,
) -> RepairModelsResponse:
    repo = RepairCostsRepo(db)

    mkt = _norm_market(market)
    lim = max(1, min(int(limit), 20000))

    models = await _list_models_from_pricing_groups(db, user_id=user_id, market=mkt, limit=lim)
    repair_keys = await repo.list_keys_for_user(user_id=user_id, market=mkt)

    items: List[RepairModelStatus] = []
    for row in models:
        b_raw = row.get("brand")
        m_raw = row.get("model")
        if not isinstance(b_raw, str) or not b_raw:
            continue
        if not isinstance(m_raw, str) or not m_raw:
            continue

        b = _norm_brand(b_raw)
        md = _norm_model(m_raw)

        meta = repair_keys.get((b, md))
        has_cost = meta is not None

        items.append(
            RepairModelStatus(
                brand=b,
                model=md,
                groups_count=int(row.get("groups_count") or 0),
                has_repair_cost=has_cost,
                repair_cost_updated_at=meta.get("updated_at") if meta else None,
                currency=meta.get("currency") if meta else None,
            )
        )

    return RepairModelsResponse(user_id=user_id, market=mkt, count=len(items), items=items)


