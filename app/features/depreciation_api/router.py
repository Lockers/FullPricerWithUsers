from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongo import get_db

from .curves import CURVE_VERSION, GRADE_MULTIPLIERS, SEED_CURVES_EXCELLENT
from .repository import (
    build_multiplier_keys,
    get_depreciation_model,
    resolve_multiplier,
    update_multiplier_log_ewma,
    upsert_depreciation_model,
)
from .schemas import (
    ComputePricingGroupDepreciationRequest,
    ComputePricingGroupDepreciationResponse,
    DepreciationEstimateRequest,
    DepreciationEstimateResponse,
    DepreciationModelResponse,
    DepreciationModelUpsertRequest,
    DepreciationObserveRequest,
    DepreciationObserveResponse,
    ObserveFromOrderlineRequest,
    ObserveFromOrderlineResponse,
    ObserveFromOrderRequest,
    ObserveFromOrderResponse,
)
from .service import (
    derive_segment,
    grade_multiplier,
    months_since,
    normalize_grade,
    parse_sku,
    retention_at,
    round_gbp,
)
from .types import Grade, MultiplierScope, Segment

router = APIRouter(prefix="/depreciation", tags=["depreciation"])


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _oid_str(oid: Any) -> str:
    try:
        return str(oid)
    except TypeError:
        return "unknown"


def _u(v: Any) -> str:
    """Uppercase + strip; always returns a string."""
    return str(v or "").strip().upper()


def _parse_dt_best_effort(v: Any, *, default: Optional[datetime] = None) -> datetime:
    """
    Parse a datetime from common BM formats.
    Accepts ISO strings with 'Z' and datetime objects. Falls back to default/now.
    """
    if default is None:
        default = _now_dt()

    if isinstance(v, datetime):
        return v

    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            return default

    return default


def _scope_or_default(scope: Optional[MultiplierScope]) -> MultiplierScope:
    return scope if scope is not None else MultiplierScope.SKU


def _try_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _require_float(v: Any, *, field: str) -> float:
    f = _try_float(v)
    if f is None:
        raise HTTPException(status_code=422, detail=f"Missing or invalid {field}")
    return f


def _extract_listing_sku(d: Dict[str, Any]) -> str:
    # tolerate slightly different field names
    sku = d.get("listing") or d.get("listing_sku") or d.get("listingSku") or ""
    return str(sku or "").strip()


def _extract_price(d: Dict[str, Any]) -> Optional[float]:
    # BM payload uses `price` on orderline; your bridge sometimes uses `unit_price`
    raw = d.get("price")
    if raw is None:
        raw = d.get("unit_price")
    return _try_float(raw)


def _extract_observed_dt(d: Dict[str, Any], *, fallback: datetime) -> datetime:
    dt = d.get("date_creation") or d.get("date_payment") or d.get("created_at")
    return _parse_dt_best_effort(dt, default=fallback)


def _iter_orderlines(v: Any) -> Iterable[Dict[str, Any]]:
    """
    Docs say `orderlines` can be:
      - list[dict]
      - dict keyed by SKU (value is dict)
    Normalize to yielding dicts; for dict form, inject listing=sku_key if missing.
    """
    if isinstance(v, list):
        for x in v:
            if isinstance(x, dict):
                yield x
        return

    if isinstance(v, dict):
        for sku_key, val in v.items():
            if not isinstance(val, dict):
                continue
            d = dict(val)
            d.setdefault("listing", sku_key)
            yield d
        return

    return


def _segment_from_doc_value(seg_val: Any, *, fallback: Segment) -> Segment:
    """Turn unknown/Any segment values into a Segment enum safely."""
    if seg_val is None:
        return fallback
    try:
        return Segment(str(seg_val))
    except (ValueError, TypeError):
        return fallback


async def _require_dep_doc(
    *,
    db: AsyncIOMotorDatabase,
    user_id: str,
    market: str,
    brand: str,
    model: str,
    storage_gb: int,
) -> Dict[str, Any]:
    dep_doc = await get_depreciation_model(
        db,
        user_id=user_id,
        market=market,
        brand=brand,
        model=model,
        storage_gb=storage_gb,
    )
    if not dep_doc:
        raise HTTPException(
            status_code=422,
            detail="Missing depreciation model data (release_date/msrp). Upsert it via PUT /depreciation/models.",
        )
    return dep_doc


async def _resolve_dep_inputs(
    *,
    db: AsyncIOMotorDatabase,
    user_id: str,
    market: str,
    brand: str,
    model: str,
    storage_gb: int,
    release_date: Optional[date],
    msrp_amount: Optional[float],
    currency_hint: str = "GBP",
    segment: Optional[Segment],
) -> Tuple[date, float, str, Segment]:
    """
    Resolve (release_date, msrp_amount, currency, segment) from request values
    and/or stored depreciation model.
    """
    dep_doc: Optional[Dict[str, Any]] = None
    if release_date is None or msrp_amount is None or segment is None:
        dep_doc = await get_depreciation_model(
            db,
            user_id=user_id,
            market=market,
            brand=brand,
            model=model,
            storage_gb=storage_gb,
        )

    if release_date is None:
        if not dep_doc:
            dep_doc = await _require_dep_doc(
                db=db, user_id=user_id, market=market, brand=brand, model=model, storage_gb=storage_gb
            )
        rd = dep_doc.get("release_date")
        if isinstance(rd, datetime):
            release_date = rd.date()
        elif isinstance(rd, date):
            release_date = rd
        else:
            raise HTTPException(status_code=422, detail="Missing or invalid release_date in depreciation model")

    if msrp_amount is None:
        if not dep_doc:
            dep_doc = await _require_dep_doc(
                db=db, user_id=user_id, market=market, brand=brand, model=model, storage_gb=storage_gb
            )
        msrp_amount = _require_float(dep_doc.get("msrp_amount"), field="msrp_amount")

    currency = currency_hint
    if dep_doc and dep_doc.get("currency"):
        currency = str(dep_doc.get("currency") or currency)

    if segment is None:
        fallback_seg = derive_segment(brand, model)
        segment = _segment_from_doc_value(dep_doc.get("segment") if dep_doc else None, fallback=fallback_seg)

    return release_date, msrp_amount, currency, segment


@router.put("/models", response_model=DepreciationModelResponse)
async def upsert_model(req: DepreciationModelUpsertRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    brand = _u(req.brand)
    model = _u(req.model)
    market = _u(req.market)

    seg = req.segment or derive_segment(brand, model)

    doc = await upsert_depreciation_model(
        db,
        user_id=req.user_id,
        market=market,
        brand=brand,
        model=model,
        storage_gb=req.storage_gb,
        release_date=req.release_date,
        msrp_amount=req.msrp_amount,
        currency=str(req.currency),
        segment=str(seg.value),
    )
    if not doc:
        raise HTTPException(status_code=500, detail="Failed to upsert depreciation model")

    seg_out = _segment_from_doc_value(doc.get("segment"), fallback=seg)

    return DepreciationModelResponse(
        id=_oid_str(doc.get("_id")),
        user_id=str(doc["user_id"]),
        market=str(doc["market"]),
        brand=str(doc["brand"]),
        model=str(doc["model"]),
        storage_gb=int(doc["storage_gb"]),
        release_date=doc["release_date"],
        msrp_amount=float(doc["msrp_amount"]),
        currency=str(doc.get("currency", "GBP")),
        segment=seg_out,
    )


@router.post("/estimate", response_model=DepreciationEstimateResponse)
async def estimate(req: DepreciationEstimateRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    market = _u(req.market)
    brand = _u(req.brand)
    model = _u(req.model)

    release_date, msrp_amount, currency, seg = await _resolve_dep_inputs(
        db=db,
        user_id=req.user_id,
        market=market,
        brand=brand,
        model=model,
        storage_gb=req.storage_gb,
        release_date=req.release_date,
        msrp_amount=req.msrp_amount,
        currency_hint="GBP",
        segment=req.segment,
    )

    as_of = req.as_of_date or date.today()
    age_m = months_since(release_date, as_of)

    curve = SEED_CURVES_EXCELLENT[seg]
    ret_ex = retention_at(age_m, curve)

    keys = build_multiplier_keys(
        market=market,
        brand=brand,
        model=model,
        storage_gb=req.storage_gb,
        segment=str(seg.value),
    )
    mult, mult_source = await resolve_multiplier(db, user_id=req.user_id, market=market, keys=keys)

    base_excellent = float(msrp_amount) * ret_ex
    pred_ex = base_excellent * mult
    pred_good = pred_ex * float(GRADE_MULTIPLIERS[Grade.GOOD])
    pred_fair = pred_ex * float(GRADE_MULTIPLIERS[Grade.FAIR])
    pred_for_grade = pred_ex * grade_multiplier(req.grade)

    pred_ex_r = round_gbp(pred_ex)
    pred_good_r = round_gbp(pred_good)
    pred_fair_r = round_gbp(pred_fair)
    pred_grade_r = round_gbp(pred_for_grade)

    depreciation_cost = max(float(msrp_amount) - pred_grade_r, 0.0)

    return DepreciationEstimateResponse(
        market=market,
        brand=brand,
        model=model,
        storage_gb=req.storage_gb,
        segment=seg,
        grade=req.grade,
        release_date=release_date,
        as_of_date=as_of,
        age_months=round(age_m, 2),
        msrp_amount=float(msrp_amount),
        currency=currency,
        curve_version=CURVE_VERSION,
        retention_excellent=round(ret_ex, 4),
        multiplier=round(mult, 6),
        multiplier_source=mult_source,
        predicted_excellent=pred_ex_r,
        predicted_good=pred_good_r,
        predicted_fair=pred_fair_r,
        predicted_for_grade=pred_grade_r,
        depreciation_cost_for_grade=round_gbp(depreciation_cost),
    )


@router.post("/pricing-groups/{pricing_group_id}/compute", response_model=ComputePricingGroupDepreciationResponse)
async def compute_for_pricing_group(
    pricing_group_id: str,
    req: ComputePricingGroupDepreciationRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    try:
        oid = ObjectId(pricing_group_id)
    except (InvalidId, TypeError):
        raise HTTPException(status_code=422, detail="Invalid pricing_group_id (expected ObjectId hex)")

    pg = await db["pricing_groups"].find_one({"_id": oid})
    if not pg:
        raise HTTPException(status_code=404, detail="pricing_group not found")

    if req.user_id and pg.get("user_id") != req.user_id:
        raise HTTPException(status_code=403, detail="pricing_group does not belong to user_id")

    user_id = pg.get("user_id")
    if not isinstance(user_id, str) or not user_id:
        raise HTTPException(status_code=422, detail="pricing_group missing user_id")

    trade_sku = str(pg.get("trade_sku", "") or "")

    market = (pg.get("markets") or ["GB"])[0]
    market = _u(market)

    brand = _u(pg.get("brand", ""))
    model = _u(pg.get("model", ""))
    storage_gb = int(pg.get("storage_gb") or 0)

    if (not brand or not model or not storage_gb) and trade_sku:
        b, m, sg, _g = parse_sku(trade_sku)
        brand = brand or b
        model = model or m
        storage_gb = storage_gb or sg

    if not brand or not model or not storage_gb:
        raise HTTPException(
            status_code=422,
            detail="pricing_group missing brand/model/storage_gb and trade_sku not parseable",
        )

    target_condition = pg.get("target_sell_condition") or pg.get("trade_sku_condition") or ""
    target_grade = normalize_grade(str(target_condition), default=Grade.GOOD)

    release_date, msrp_amount, currency, seg = await _resolve_dep_inputs(
        db=db,
        user_id=user_id,
        market=market,
        brand=brand,
        model=model,
        storage_gb=storage_gb,
        release_date=None,
        msrp_amount=None,
        currency_hint="GBP",
        segment=None,
    )

    as_of = req.as_of_date or date.today()
    age_m = months_since(release_date, as_of)
    curve = SEED_CURVES_EXCELLENT[seg]
    ret_ex = retention_at(age_m, curve)

    keys = build_multiplier_keys(
        market=market,
        brand=brand,
        model=model,
        storage_gb=storage_gb,
        segment=str(seg.value),
    )
    mult, mult_source = await resolve_multiplier(db, user_id=user_id, market=market, keys=keys)

    base_excellent = msrp_amount * ret_ex
    pred_ex = base_excellent * mult
    pred_good = pred_ex * float(GRADE_MULTIPLIERS[Grade.GOOD])
    pred_fair = pred_ex * float(GRADE_MULTIPLIERS[Grade.FAIR])
    pred_target = pred_ex * grade_multiplier(target_grade)

    dep_estimate = DepreciationEstimateResponse(
        market=market,
        brand=brand,
        model=model,
        storage_gb=storage_gb,
        segment=seg,
        grade=target_grade,
        release_date=release_date,
        as_of_date=as_of,
        age_months=round(age_m, 2),
        msrp_amount=msrp_amount,
        currency=currency,
        curve_version=CURVE_VERSION,
        retention_excellent=round(ret_ex, 4),
        multiplier=round(mult, 6),
        multiplier_source=mult_source,
        predicted_excellent=round_gbp(pred_ex),
        predicted_good=round_gbp(pred_good),
        predicted_fair=round_gbp(pred_fair),
        predicted_for_grade=round_gbp(pred_target),
        depreciation_cost_for_grade=round_gbp(max(msrp_amount - round_gbp(pred_target), 0.0)),
    )

    stored_field = "depreciation_anchor"
    persisted = False
    if req.persist:
        payload: Dict[str, Any] = {
            "market": market,
            "brand": brand,
            "model": model,
            "storage_gb": storage_gb,
            "target_condition": str(target_condition),
            "target_grade": target_grade.value,
            "release_date": release_date,
            "msrp_amount": msrp_amount,
            "currency": currency,
            "as_of_date": as_of,
            "age_months": dep_estimate.age_months,
            "curve_version": CURVE_VERSION,
            "retention_excellent": dep_estimate.retention_excellent,
            "multiplier": dep_estimate.multiplier,
            "multiplier_source": dep_estimate.multiplier_source,
            "predicted_excellent": dep_estimate.predicted_excellent,
            "predicted_good": dep_estimate.predicted_good,
            "predicted_fair": dep_estimate.predicted_fair,
            "predicted_target": dep_estimate.predicted_for_grade,
            "computed_at": _now_dt(),
        }
        await db["pricing_groups"].update_one({"_id": oid}, {"$set": {stored_field: payload}})
        persisted = True

    return ComputePricingGroupDepreciationResponse(
        pricing_group_id=pricing_group_id,
        trade_sku=trade_sku,
        user_id=user_id,
        market=market,
        stored_field=stored_field,
        persisted=persisted,
        estimate=dep_estimate,
    )


@router.post("/observe", response_model=DepreciationObserveResponse)
async def observe(req: DepreciationObserveRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    market = _u(req.market)

    if req.sku:
        brand, model, storage_gb, parsed_grade = parse_sku(str(req.sku))
        grade = req.grade or parsed_grade
    else:
        if not (req.brand and req.model and req.storage_gb):
            raise HTTPException(status_code=422, detail="Provide either sku or brand+model+storage_gb")
        brand = _u(req.brand)
        model = _u(req.model)
        storage_gb = int(req.storage_gb)
        grade = req.grade

    dep_doc = await _require_dep_doc(
        db=db, user_id=req.user_id, market=market, brand=brand, model=model, storage_gb=storage_gb
    )

    release_date_raw = dep_doc.get("release_date")
    if isinstance(release_date_raw, datetime):
        release_date = release_date_raw.date()
    elif isinstance(release_date_raw, date):
        release_date = release_date_raw
    else:
        raise HTTPException(status_code=422, detail="Missing or invalid release_date in depreciation model")

    msrp_amount = _require_float(dep_doc.get("msrp_amount"), field="msrp_amount")
    seg = _segment_from_doc_value(dep_doc.get("segment"), fallback=derive_segment(brand, model))

    observed_at = req.observed_at or date.today()
    age_m = months_since(release_date, observed_at)
    curve = SEED_CURVES_EXCELLENT[seg]
    ret_ex = retention_at(age_m, curve)

    base_pred_ex = msrp_amount * ret_ex
    observed_excellent_equiv = float(req.observed_price) / max(grade_multiplier(grade), 1e-9)
    target_multiplier = observed_excellent_equiv / max(base_pred_ex, 1e-9)

    scope = _scope_or_default(req.scope)

    if scope == MultiplierScope.SKU:
        key = f"sku:{market}:{brand}|{model}|{storage_gb}"
    elif scope == MultiplierScope.MODEL:
        key = f"model:{market}:{brand}|{model}"
    elif scope == MultiplierScope.SEGMENT:
        key = f"segment:{market}:{seg.value}"
    else:
        key = f"global:{market}"

    update = await update_multiplier_log_ewma(
        db,
        user_id=req.user_id,
        market=market,
        key=key,
        scope=scope,
        target_multiplier=target_multiplier,
        weight=req.weight,
    )

    return DepreciationObserveResponse(
        key=key,
        scope=scope,
        previous_multiplier=round(update["previous_multiplier"], 6),
        updated_multiplier=round(update["updated_multiplier"], 6),
        n=int(update["n"]),
        applied_weight=round(float(update["applied_weight"]), 4),
        target_multiplier=round(float(update["target_multiplier"]), 6),
        base_pred_excellent=round_gbp(base_pred_ex),
        observed_excellent_equiv=round_gbp(observed_excellent_equiv),
        segment=seg,
    )


async def _observe_from_line(
    *,
    user_id: str,
    market: str,
    line: Dict[str, Any],
    order_dt_fallback: datetime,
    scope: MultiplierScope,
    weight: Optional[float],
    db: AsyncIOMotorDatabase,
) -> Tuple[Optional[DepreciationObserveResponse], Optional[datetime]]:
    """Shared logic for observe-from-order and observe-from-orderline.

    Returns (result, observed_at_dt_used).
    """
    listing_sku = _extract_listing_sku(line)
    if not listing_sku:
        return None, None

    observed_price = _extract_price(line)
    if observed_price is None:
        return None, None

    observed_at_dt = _extract_observed_dt(line, fallback=order_dt_fallback)

    try:
        brand, model, storage_gb, grade = parse_sku(listing_sku)
    except ValueError:
        return None, None

    obs_req = DepreciationObserveRequest(
        user_id=user_id,
        market=market,
        brand=brand,
        model=model,
        storage_gb=storage_gb,
        observed_price=observed_price,
        observed_at=observed_at_dt.date(),
        grade=grade,
        scope=scope,
        weight=weight,
    )
    return await observe(obs_req, db), observed_at_dt


@router.post("/observe/from-orderline/{orderline_id}", response_model=ObserveFromOrderlineResponse)
async def observe_from_orderline(
    orderline_id: str,
    req: ObserveFromOrderlineRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    col = db["bm_orderlines"]

    doc = await col.find_one({"user_id": req.user_id, "orderline_id": orderline_id})
    if not doc:
        try:
            doc = await col.find_one({"user_id": req.user_id, "orderline_id": int(orderline_id)})
        except (TypeError, ValueError):
            doc = None

    if not doc:
        raise HTTPException(status_code=404, detail="bm_orderline not found")

    line: Dict[str, Any] = dict(doc)

    # Prefer these common keys if present
    listing_sku = str(line.get("listing_sku") or line.get("listing") or "")
    if listing_sku:
        line.setdefault("listing", listing_sku)

    order_dt = _parse_dt_best_effort(line.get("date_creation") or line.get("date_payment") or line.get("created_at"))

    market = _u(req.market)
    scope = _scope_or_default(req.scope)

    res, observed_at_dt = await _observe_from_line(
        user_id=req.user_id,
        market=market,
        line=line,
        order_dt_fallback=order_dt,
        scope=scope,
        weight=req.weight,
        db=db,
    )

    if res is None:
        raise HTTPException(status_code=422, detail="bm_orderline missing listing/price or sku not parseable")

    used_sku = _extract_listing_sku(line)
    used_price = _extract_price(line)

    return ObserveFromOrderlineResponse(
        orderline_id=orderline_id,
        listing_sku=used_sku,
        observed_price=float(used_price or 0.0),
        observed_at=observed_at_dt or order_dt,
        result=res,
    )


@router.post("/observe/from-order/{order_id}", response_model=ObserveFromOrderResponse)
async def observe_from_order(
    order_id: str,
    req: ObserveFromOrderRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Reads bm_orders and calibrates multipliers from each orderline.

    Supports both storage shapes:
      - bm_orders.bm_raw.orderlines (current)
      - bm_orders.orderlines (legacy)
    """
    col = db["bm_orders"]

    doc = await col.find_one({"user_id": req.user_id, "order_id": order_id})
    if not doc:
        try:
            doc = await col.find_one({"user_id": req.user_id, "order_id": int(order_id)})
        except (TypeError, ValueError):
            doc = None

    if not doc:
        raise HTTPException(status_code=404, detail="bm_order not found")

    order_dt = _parse_dt_best_effort(doc.get("date_creation") or doc.get("date_payment") or doc.get("created_at"))

    raw = doc.get("bm_raw") or {}
    lines_any = doc.get("orderlines")
    if lines_any is None:
        lines_any = raw.get("orderlines")

    market = _u(req.market)
    scope = _scope_or_default(req.scope)

    results: List[DepreciationObserveResponse] = []
    for ol in _iter_orderlines(lines_any or []):
        res, _dt_used = await _observe_from_line(
            user_id=req.user_id,
            market=market,
            line=ol,
            order_dt_fallback=order_dt,
            scope=scope,
            weight=req.weight,
            db=db,
        )
        if res is not None:
            results.append(res)

    return ObserveFromOrderResponse(
        order_id=str(order_id),
        observed_at=order_dt,
        results=results,
    )



