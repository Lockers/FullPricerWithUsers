from __future__ import annotations

"""
Trade-in endpoints under /tradein/*

Existing endpoints:
- GET  /tradein/listings/{user_id}         (live fetch from BM)
- POST /tradein/listings/{user_id}/sync    (sync to bm_tradein_listings)
- GET  /tradein/listings/{user_id}/stored  (sample from Mongo)

Added endpoints:
- POST /tradein/competitors/{user_id}/run
  Full 2-stage flow:
    set all to £1 -> wait -> fetch competitor -> persist gross/net

(These can be long-running depending on number of groups; they are intended
for controlled ops / cron / diagnostics.)
"""

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from bson import ObjectId
from bson.errors import InvalidId

from app.core.errors import BadRequestError, ConflictError, NotFoundError
from app.features.backmarket.pricing.trade_pricing_service import recompute_trade_pricing_for_group
from app.features.backmarket.transport.exceptions import BMClientError


from app.db.mongo import get_db
from app.features.backmarket.tradein.repo import TradeinListingsRepo
from app.features.backmarket.tradein.tradein_listings import (
    fetch_all_tradein_listings,
    sync_tradein_listings_to_db,
)
from app.features.backmarket.tradein.competitors_service import (
    run_tradein_competitor_refresh_for_user, stage1_set_all_to_one,
)
from app.features.backmarket.tradein.offers_service import (
    disable_tradein_offer_for_group,
    run_tradein_offer_update_for_group,
    run_tradein_offer_updates_for_user,
)



router = APIRouter(prefix="/tradein", tags=["backmarket:tradein"])


@router.get("/listings/{user_id}")
async def get_tradein_listings_live(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    page_size: int = Query(100, ge=1, le=200),
    max_pages: int = Query(500, ge=1, le=5000),
    max_results: int | None = Query(200, ge=1, le=100000),
    sample_size: int = Query(25, ge=0, le=500),
):
    res = await fetch_all_tradein_listings(
        db,
        user_id,
        page_size=page_size,
        max_pages=max_pages,
        max_results=max_results,
    )
    return {
        "user_id": user_id,
        "count": len(res.listings),
        "pages_fetched": res.pages_fetched,
        "elapsed_seconds": res.elapsed_seconds,
        "sample": res.listings[: int(sample_size)],
    }


@router.post("/listings/{user_id}/sync")
async def sync_tradein_listings(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    page_size: int = Query(100, ge=1, le=200),
    max_pages: int = Query(500, ge=1, le=5000),
    max_results: int | None = Query(500, ge=1, le=100000),
):
    return await sync_tradein_listings_to_db(
        db,
        user_id,
        page_size=page_size,
        max_pages=max_pages,
        max_results=max_results,
    )


@router.get("/listings/{user_id}/stored")
async def get_tradein_listings_stored(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    limit: int = Query(25, ge=1, le=500),
):
    repo = TradeinListingsRepo(db)
    count = await repo.count_for_user(user_id)
    sample = await repo.sample_for_user(user_id, limit=limit)
    return {"user_id": user_id, "count": count, "sample": sample}


# ---------------------------------------------------------------------------
# Full pricing getter flow (trade-in competitors)
# ---------------------------------------------------------------------------

@router.post("/competitors/{user_id}/run")
async def run_tradein_competitor_refresh(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    market: str = Query("GB", min_length=2, max_length=4),
    currency: str = Query("GBP", min_length=3, max_length=3),
    wait_seconds: int = Query(60, ge=0, le=600),
    update_concurrency: int = Query(10, ge=1, le=200),
    fetch_concurrency: int = Query(10, ge=1, le=200),
    limit: int | None = Query(None, ge=1, le=100000),
    include_stage_results: bool = Query(False),
    include_item_results: bool = Query(False),
):
    """
    Full flow:
      1) set all trade-in listings to £1.00
      2) wait 60s
      3) fetch competitor price_to_win
      4) store gross + computed net in pricing_groups
    """
    return await run_tradein_competitor_refresh_for_user(
        db,
        user_id=user_id,
        market=market,
        currency=currency,
        update_concurrency=update_concurrency,
        fetch_concurrency=fetch_concurrency,
        wait_seconds=wait_seconds,
        limit=limit,
        include_stage_results=include_stage_results,
        include_item_results=include_item_results,
    )


@router.get("/competitors/{user_id}/diag")
async def tradein_competitors_diag(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    market: str = Query("GB", min_length=2, max_length=4),
):
    mkt = str(market).upper().strip()

    total_user = await db["pricing_groups"].count_documents({"user_id": user_id})
    with_tradein = await db["pricing_groups"].count_documents(
        {
            "user_id": user_id,
            "tradein_listing.tradein_id": {"$exists": True, "$ne": None},
        }
    )
    with_tradein_and_market = await db["pricing_groups"].count_documents(
        {
            "user_id": user_id,
            "tradein_listing.tradein_id": {"$exists": True, "$ne": None},
            "markets": {"$in": [mkt]},
        }
    )

    sample = await db["pricing_groups"].find_one(
        {"user_id": user_id},
        projection={"_id": 0, "trade_sku": 1, "markets": 1, "tradein_listing": 1},
    )

    return {
        "mongo_db": db.name,
        "market": mkt,
        "counts": {
            "pricing_groups_total_for_user": int(total_user),
            "with_tradein_listing_tradein_id": int(with_tradein),
            "with_tradein_id_and_market": int(with_tradein_and_market),
        },
        "sample": sample,
    }

@router.post("/competitors/{user_id}/set-to-one")
async def tradein_set_all_to_one(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    market: str = Query("GB", min_length=2, max_length=4),
    currency: str = Query("GBP", min_length=3, max_length=3),
    concurrency: int = Query(10, ge=1, le=200),
    limit: int | None = Query(None, ge=1, le=100000),
    include_results: bool = Query(False),
):
    summary, _ok_refs = await stage1_set_all_to_one(
        db,
        user_id=user_id,
        market=market,
        currency=currency,
        concurrency=concurrency,
        limit=limit,
        include_results=include_results,
    )
    return summary


# ---------------------------------------------------------------------------
# Stage 3: apply final trade-in offers (PUT back to BM)
# ---------------------------------------------------------------------------

@router.post("/offers/{user_id}/apply")
async def apply_tradein_offers(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    market: str = Query("GB", min_length=2, max_length=4),
    currency: str = Query("GBP", min_length=3, max_length=3),
    concurrency: int = Query(10, ge=1, le=200),
    limit: int | None = Query(None, ge=1, le=100000),
    dry_run: bool = Query(False),
    include_item_results: bool = Query(False),
    require_ok_to_update: bool = Query(True),
):
    """Apply the profit-safe final_update_price_gross to Back Market trade-in listings."""
    return await run_tradein_offer_updates_for_user(
        db,
        user_id=user_id,
        market=market,
        currency=currency,
        concurrency=concurrency,
        limit=limit,
        dry_run=dry_run,
        include_item_results=include_item_results,
        require_ok_to_update=require_ok_to_update,
    )

@router.post("/offers/{user_id}/apply-group/{group_id}")
async def apply_tradein_offer_for_group(
    user_id: str,
    group_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    market: str = Query("GB", min_length=2, max_length=4),
    currency: str = Query("GBP", min_length=3, max_length=3),
    dry_run: bool = Query(False),
    require_ok_to_update: bool = Query(True),
    recompute: bool = Query(True),
    scenario_key: str | None = Query(None),
):
    """Apply a single group's profit-safe trade-in offer to Back Market (drawer action)."""
    try:
        gid = ObjectId(group_id)
    except (InvalidId, TypeError) as exc:
        raise BadRequestError(code="invalid_group_id", message="Invalid group_id") from exc

    exists = await db["pricing_groups"].find_one({"_id": gid, "user_id": user_id}, projection={"_id": 1})
    if not exists:
        raise NotFoundError(code="pricing_group_not_found", message="pricing_group not found")

    trade_pricing = None
    if recompute:
        try:
            recomputed = await recompute_trade_pricing_for_group(db, user_id, group_id)
            trade_pricing = recomputed.get("trade_pricing")
        except BMClientError as exc:
            raise NotFoundError(code="pricing_group_not_found", message="pricing_group not found") from exc

    applied = await run_tradein_offer_update_for_group(
        db,
        user_id=user_id,
        group_id=group_id,
        market=market,
        currency=currency,
        dry_run=dry_run,
        recompute_pricing=False,
        require_ok_to_update=require_ok_to_update,
        scenario_key=scenario_key,
    )

    err = applied.get("error")
    if err:
        # Map common service-level errors to proper HTTP status codes so the UI
        # doesn't treat failures as "success".
        if err == "pricing_group_not_found":
            raise NotFoundError(code="pricing_group_not_found", message="pricing_group not found")
        if err == "scenario_not_found":
            raise BadRequestError(
                code="scenario_not_found",
                message="scenario_key not found in computed scenarios",
                details=applied,
            )
        if err in {"missing_tradein_id", "missing_final_update_price"}:
            raise BadRequestError(code=str(err), message=str(err).replace("_", " "), details=applied)
        if err == "offer_disabled":
            raise ConflictError(code="offer_disabled", message="Offer updates are disabled for this group", details=applied)
        if err == "not_ok_to_update":
            raise ConflictError(code="not_ok_to_update", message="Not OK to update trade-in offer", details=applied)

        # Fallback: treat unknown service errors as a bad request.
        raise BadRequestError(code=str(err), message="Offer update failed", details=applied)

    return {
        "user_id": user_id,
        "group_id": group_id,
        "recomputed": bool(recompute),
        "trade_pricing": trade_pricing,
        "apply": applied,
    }


@router.post("/offers/{user_id}/disable-group/{group_id}")
async def disable_tradein_offer_group(
    user_id: str,
    group_id: str,
    market: Optional[str] = Query(None, description="Market code"),
    currency: Optional[str] = Query(None, description="Currency code"),
    amount_gross: int = Query(0, ge=0, description="Gross amount to set when disabling"),
    dry_run: bool = Query(False, description="If true, do not actually update Back Market"),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Disable offer updates for a single group and push a 0 (or fallback) trade-in amount to Back Market."""

    mkt = market or "GB"
    cur = currency or "GBP"
    res = await disable_tradein_offer_for_group(
        db,
        user_id=user_id,
        group_id=group_id,
        market=mkt,
        currency=cur,
        amount_gbp=int(amount_gross),
        dry_run=dry_run,
    )
    if res.get("error"):
        err = res.get("error")
        if err in {"group_not_found", "missing_tradein_id"}:
            raise NotFoundError(code=str(err), message=str(err).replace("_", " "), details=res)
        raise BadRequestError(code=str(err), message=str(err).replace("_", " "), details=res)
    return res
