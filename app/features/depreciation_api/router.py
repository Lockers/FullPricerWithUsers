from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict

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


@router.put("/models", response_model=DepreciationModelResponse)
async def upsert_model(req: DepreciationModelUpsertRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    brand = req.brand.strip().upper()
    model = req.model.strip().upper()
    market = req.market.strip().upper()

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
        currency=req.currency,
        segment=seg.value,
    )
    if not doc:
        raise HTTPException(status_code=500, detail="Failed to upsert depreciation model")

    return DepreciationModelResponse(
        id=_oid_str(doc.get("_id")),
        user_id=doc["user_id"],
        market=doc["market"],
        brand=doc["brand"],
        model=doc["model"],
        storage_gb=int(doc["storage_gb"]),
        release_date=doc["release_date"],
        msrp_amount=float(doc["msrp_amount"]),
        currency=doc.get("currency", "GBP"),
        segment=Segment(doc.get("segment", seg.value)),
    )


@router.post("/estimate", response_model=DepreciationEstimateResponse)
async def estimate(req: DepreciationEstimateRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    market = req.market.strip().upper()
    brand = req.brand.strip().upper()
    model = req.model.strip().upper()

    release_date = req.release_date
    msrp_amount = req.msrp_amount
    currency = "GBP"

    dep_doc = None
    if release_date is None or msrp_amount is None or req.segment is None:
        dep_doc = await get_depreciation_model(
            db,
            user_id=req.user_id,
            market=market,
            brand=brand,
            model=model,
            storage_gb=req.storage_gb,
        )

    if release_date is None or msrp_amount is None:
        if not dep_doc:
            raise HTTPException(
                status_code=422,
                detail="Missing depreciation model data (release_date/msrp). Upsert it via PUT /depreciation/models.",
            )
        release_date = release_date or dep_doc["release_date"]
        msrp_amount = msrp_amount or float(dep_doc["msrp_amount"])
        currency = dep_doc.get("currency", currency)

    seg = req.segment
    if seg is None:
        if dep_doc and dep_doc.get("segment"):
            seg = Segment(dep_doc["segment"])
        else:
            seg = derive_segment(brand, model)

    as_of = req.as_of_date or date.today()
    age_m = months_since(release_date, as_of)

    curve = SEED_CURVES_EXCELLENT[seg]
    ret_ex = retention_at(age_m, curve)

    keys = build_multiplier_keys(market=market, brand=brand, model=model, storage_gb=req.storage_gb, segment=seg.value)
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
    trade_sku = pg.get("trade_sku", "")

    market = (pg.get("markets") or ["GB"])[0]
    market = str(market).upper()

    brand = str(pg.get("brand", "")).upper().strip()
    model = str(pg.get("model", "")).upper().strip()
    storage_gb = int(pg.get("storage_gb") or 0)

    if (not brand or not model or not storage_gb) and trade_sku:
        b, m, sg, _g = parse_sku(trade_sku)
        brand = brand or b
        model = model or m
        storage_gb = storage_gb or sg

    if not brand or not model or not storage_gb:
        raise HTTPException(status_code=422, detail="pricing_group missing brand/model/storage_gb and trade_sku not parseable")

    target_condition = pg.get("target_sell_condition") or pg.get("trade_sku_condition") or ""
    target_grade = normalize_grade(str(target_condition), default=Grade.GOOD)

    dep = await get_depreciation_model(
        db, user_id=user_id, market=market, brand=brand, model=model, storage_gb=storage_gb
    )
    if not dep:
        raise HTTPException(
            status_code=422,
            detail="Missing depreciation model data (release_date/msrp). Upsert it via PUT /depreciation/models.",
        )

    release_date = dep["release_date"]
    msrp_amount = float(dep["msrp_amount"])
    currency = dep.get("currency", "GBP")

    seg = Segment(dep.get("segment")) if dep.get("segment") else derive_segment(brand, model)

    as_of = req.as_of_date or date.today()
    age_m = months_since(release_date, as_of)
    curve = SEED_CURVES_EXCELLENT[seg]
    ret_ex = retention_at(age_m, curve)

    keys = build_multiplier_keys(market=market, brand=brand, model=model, storage_gb=storage_gb, segment=seg.value)
    mult, mult_source = await resolve_multiplier(db, user_id=user_id, market=market, keys=keys)

    base_excellent = msrp_amount * ret_ex
    pred_ex = base_excellent * mult
    pred_good = pred_ex * float(GRADE_MULTIPLIERS[Grade.GOOD])
    pred_fair = pred_ex * float(GRADE_MULTIPLIERS[Grade.FAIR])
    pred_target = pred_ex * grade_multiplier(target_grade)

    estimate = DepreciationEstimateResponse(
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
            "age_months": estimate.age_months,
            "curve_version": CURVE_VERSION,
            "retention_excellent": estimate.retention_excellent,
            "multiplier": estimate.multiplier,
            "multiplier_source": estimate.multiplier_source,
            "predicted_excellent": estimate.predicted_excellent,
            "predicted_good": estimate.predicted_good,
            "predicted_fair": estimate.predicted_fair,
            "predicted_target": estimate.predicted_for_grade,
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
        estimate=estimate,
    )


@router.post("/observe", response_model=DepreciationObserveResponse)
async def observe(req: DepreciationObserveRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    market = req.market.strip().upper()

    if req.sku:
        brand, model, storage_gb, parsed_grade = parse_sku(req.sku)
        grade = req.grade or parsed_grade
    else:
        if not (req.brand and req.model and req.storage_gb):
            raise HTTPException(status_code=422, detail="Provide either sku or brand+model+storage_gb")
        brand = req.brand.strip().upper()
        model = req.model.strip().upper()
        storage_gb = int(req.storage_gb)
        grade = req.grade

    dep = await get_depreciation_model(
        db, user_id=req.user_id, market=market, brand=brand, model=model, storage_gb=storage_gb
    )
    if not dep:
        raise HTTPException(
            status_code=422,
            detail="Missing depreciation model data (release_date/msrp). Upsert it via PUT /depreciation/models.",
        )
    release_date = dep["release_date"]
    msrp_amount = float(dep["msrp_amount"])
    seg = Segment(dep.get("segment")) if dep.get("segment") else derive_segment(brand, model)

    observed_at = req.observed_at or date.today()
    age_m = months_since(release_date, observed_at)
    curve = SEED_CURVES_EXCELLENT[seg]
    ret_ex = retention_at(age_m, curve)

    base_pred_ex = msrp_amount * ret_ex
    observed_excellent_equiv = float(req.observed_price) / max(grade_multiplier(grade), 1e-9)
    target_multiplier = observed_excellent_equiv / max(base_pred_ex, 1e-9)

    scope = req.scope or MultiplierScope.SKU
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

    listing_sku = doc.get("listing_sku") or doc.get("listing") or ""
    if not listing_sku:
        raise HTTPException(status_code=422, detail="bm_orderline missing listing_sku/listing")

    price_raw = doc.get("price") or doc.get("unit_price")
    try:
        observed_price = float(price_raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="bm_orderline price not parseable")

    dt = doc.get("date_creation") or doc.get("date_payment") or doc.get("created_at")
    if isinstance(dt, str):
        try:
            observed_at_dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            observed_at_dt = _now_dt()
    elif isinstance(dt, datetime):
        observed_at_dt = dt
    else:
        observed_at_dt = _now_dt()

    try:
        brand, model, storage_gb, grade = parse_sku(listing_sku)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"Could not parse listing sku: {e}")

    obs_req = DepreciationObserveRequest(
        user_id=req.user_id,
        market=req.market,
        brand=brand,
        model=model,
        storage_gb=storage_gb,
        observed_price=observed_price,
        observed_at=observed_at_dt.date(),
        grade=grade,
        scope=req.scope,
        weight=req.weight,
    )
    res = await observe(obs_req, db)

    return ObserveFromOrderlineResponse(
        orderline_id=orderline_id,
        listing_sku=listing_sku,
        observed_price=observed_price,
        observed_at=observed_at_dt,
        result=res,
    )


@router.post("/observe/from-order/{order_id}", response_model=ObserveFromOrderResponse)
async def observe_from_order(
    order_id: str,
    req: ObserveFromOrderRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    # Reads bm_orders (nested orderlines) and calibrates multipliers from each orderline.
    # Expected fields (best effort):
    #   bm_orders.orderlines[].listing  (sku-like string)
    #   bm_orders.orderlines[].price
    #   bm_orders.orderlines[].date_creation / date_payment
    col = db["bm_orders"]

    # order_id may be numeric; try both
    doc = await col.find_one({"user_id": req.user_id, "order_id": order_id})
    if not doc:
        try:
            doc = await col.find_one({"user_id": req.user_id, "order_id": int(order_id)})
        except (TypeError, ValueError):
            doc = None
    if not doc:
        raise HTTPException(status_code=404, detail="bm_order not found")

    # Determine an order-level observed_at fallback
    dt = doc.get("date_creation") or doc.get("date_payment") or doc.get("created_at")
    if isinstance(dt, str):
        try:
            order_dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            order_dt = _now_dt()
    elif isinstance(dt, datetime):
        order_dt = dt
    else:
        order_dt = _now_dt()

    results: list[DepreciationObserveResponse] = []
    for ol in doc.get("orderlines", []) or []:
        listing_sku = ol.get("listing") or ol.get("listing_sku") or ""
        if not listing_sku:
            continue

        price_raw = ol.get("price") or ol.get("unit_price")
        try:
            observed_price = float(price_raw)
        except (TypeError, ValueError):
            continue

        dt2 = ol.get("date_creation") or ol.get("date_payment")
        if isinstance(dt2, str):
            try:
                observed_at_dt = datetime.fromisoformat(dt2.replace("Z", "+00:00"))
            except ValueError:
                observed_at_dt = order_dt
        elif isinstance(dt2, datetime):
            observed_at_dt = dt2
        else:
            observed_at_dt = order_dt

        try:
            brand, model, storage_gb, grade = parse_sku(listing_sku)
        except ValueError:
            continue

        obs_req = DepreciationObserveRequest(
            user_id=req.user_id,
            market=req.market,
            brand=brand,
            model=model,
            storage_gb=storage_gb,
            observed_price=observed_price,
            observed_at=observed_at_dt.date(),
            grade=grade,
            scope=req.scope,
            weight=req.weight,
        )
        res = await observe(obs_req, db)
        results.append(res)

    return ObserveFromOrderResponse(
        order_id=str(order_id),
        observed_at=order_dt,
        results=results,
    )
