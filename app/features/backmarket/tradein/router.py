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

from app.db.mongo import get_db
from app.features.backmarket.tradein.repo import TradeinListingsRepo
from app.features.backmarket.tradein.tradein_listings import (
    fetch_all_tradein_listings,
    sync_tradein_listings_to_db,
)
from app.features.backmarket.tradein.competitors_service import (
    run_tradein_competitor_refresh_for_user,
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

