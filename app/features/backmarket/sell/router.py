from __future__ import annotations

from bson import ObjectId

from fastapi import APIRouter, Body, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.mongo import get_db
from app.features.backmarket.pricing.trade_pricing_service import recompute_trade_pricing_for_user
from app.features.backmarket.sell import sell_anchor_repo
from app.features.backmarket.sell.activation import (
    activate_session,
    create_activation_session_for_user,
    deactivate_all_active_for_user,
    deactivate_session,
)
from app.features.backmarket.sell.groups_filter_models import GroupsFilterRequest
from app.features.backmarket.sell.listings_repo import SellListingsRepo
from app.features.backmarket.sell.pricing_cycle import run_sell_pricing_cycle_for_user
from app.features.backmarket.sell.schemas import PricingGroupsFilterIn
from app.features.backmarket.sell.sell_listings import fetch_all_sell_listings, sync_sell_listings_to_db
from app.features.backmarket.sell.sell_anchor_models import SellAnchorSettings
from app.features.backmarket.sell.sell_anchor_service import (
    get_sell_anchor_settings_for_user,
    update_sell_anchor_settings_for_user,
    recompute_sell_anchor_for_group, recompute_sell_anchors_for_user,
)
from app.features.backmarket.sell.service import run_price_all_for_user

router = APIRouter(prefix="/bm/sell", tags=["backmarket:sell"])


@router.get("/listings/{user_id}")
async def get_sell_listings(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    page_size: int = Query(50, ge=1, le=200),
    max_results: int | None = Query(None, ge=1, le=100000),
):
    result = await fetch_all_sell_listings(
        db,
        user_id,
        page_size=page_size,
        max_results=max_results,
    )
    return {
        "user_id": user_id,
        "count": len(result.listings),
        "pages_fetched": result.pages_fetched,
        "elapsed_seconds": result.elapsed_seconds,
        "sample": result.listings[:25],
    }


@router.post("/listings/{user_id}/sync")
async def sync_sell_listings(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    page_size: int = Query(50, ge=1, le=200),
    max_results: int | None = Query(500, ge=1, le=100000),
):
    return await sync_sell_listings_to_db(
        db,
        user_id,
        page_size=page_size,
        max_results=max_results,
    )


@router.get("/listings/{user_id}/stored")
async def get_stored_sell_listings(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    limit: int = Query(25, ge=1, le=5000),
):
    repo = SellListingsRepo(db)
    count = await repo.count_for_user(user_id)
    sample = await repo.sample_for_user(user_id, limit=limit)
    return {"user_id": user_id, "count": count, "sample": sample}


@router.post("/activation/session/{user_id}")
async def create_activation_session(
    user_id: str,
    wait_seconds: int = Query(180, ge=0, le=1800),
    groups_filter: PricingGroupsFilterIn | None = Body(None),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    mongo_filter = groups_filter.to_mongo_filter() if groups_filter else None
    return await create_activation_session_for_user(
        db,
        user_id,
        wait_seconds=wait_seconds,
        do_sell_sync=True,
        groups_filter=mongo_filter,
    )


@router.post("/activation/{user_id}/{session_id}/activate")
async def activate(
    user_id: str,
    session_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await activate_session(db, user_id, session_id)


@router.post("/activation/{user_id}/{session_id}/deactivate")
async def deactivate(
    user_id: str,
    session_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await deactivate_session(db, user_id, session_id)


@router.post("/activation/deactivate-all/{user_id}")
async def deactivate_all(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await deactivate_all_active_for_user(db, user_id)


@router.post("/pricing-cycle/{user_id}")
async def sell_pricing_cycle(
    user_id: str,
    wait_seconds: int = Query(180, ge=0, le=1800),
    max_backbox_attempts: int = Query(3, ge=1, le=10),
    groups_filter: PricingGroupsFilterIn | None = Body(None),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    mongo_filter = groups_filter.to_mongo_filter() if groups_filter else None
    return await run_sell_pricing_cycle_for_user(
        db,
        user_id,
        wait_seconds=wait_seconds,
        max_backbox_attempts=max_backbox_attempts,
        groups_filter=mongo_filter,
    )

# 1) list pricing groups (so you can SEE group_id if you want)
@router.get("/pricing-groups/{user_id}")
async def list_pricing_groups(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    limit: int = Query(25, ge=1, le=500),
    skip: int = Query(0, ge=0, le=100000),
):
    groups = await sell_anchor_repo.list_pricing_groups_for_user(db, user_id, limit=limit, skip=skip)
    return {"user_id": user_id, "count": len(groups), "groups": groups}


# 2) recompute sell anchors for ALL groups (or filtered) - NO group_id required
@router.post("/pricing-groups/{user_id}/recompute-sell-anchor")
async def recompute_sell_anchor_for_user(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
    limit: int | None = Query(None, ge=1, le=5000),
    body: GroupsFilterRequest | None = Body(default=None),
):
    groups_filter = body.to_mongo_filter() if body else None
    result = await recompute_sell_anchors_for_user(db, user_id, groups_filter=groups_filter, limit=limit)
    await recompute_trade_pricing_for_user(db, user_id, groups_filter=groups_filter, limit=limit)
    return result


# 3) keep the single-group recompute (uses group_id, when you have it)
@router.post("/pricing-groups/{user_id}/{group_id}/recompute-sell-anchor")
async def recompute_one_group_sell_anchor(
    user_id: str,
    group_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    result = await recompute_sell_anchor_for_group(db, user_id, group_id)
    groups_filter = None
    gid = ObjectId(group_id)
    group_doc = await sell_anchor_repo.get_pricing_group(db, user_id, gid)
    if group_doc:
        gf = {}
        if group_doc.get("brand") is not None:
            gf["brand"] = group_doc.get("brand")
        if group_doc.get("model") is not None:
            gf["model"] = group_doc.get("model")
        if group_doc.get("storage_gb") is not None:
            gf["storage_gb"] = group_doc.get("storage_gb")
        groups_filter = gf or None
    await recompute_trade_pricing_for_user(db, user_id, groups_filter=groups_filter)
    return result


# 4) anchor settings: optionally recompute after saving (so PUT "does something")
@router.get("/anchor-settings/{user_id}")
async def get_anchor_settings(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await get_sell_anchor_settings_for_user(db, user_id)


@router.put("/anchor-settings/{user_id}")
async def put_anchor_settings(
    user_id: str,
    settings: SellAnchorSettings,
    db: AsyncIOMotorDatabase = Depends(get_db),
    recompute: bool = Query(False),
):
    saved = await update_sell_anchor_settings_for_user(db, user_id, settings)
    if not recompute:
        return saved
    recomputed = await recompute_sell_anchors_for_user(db, user_id)
    await recompute_trade_pricing_for_user(db, user_id)
    return {"settings": saved, "recompute": recomputed}

@router.post("/pricing-cycle/{user_id}/all")
async def price_all(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),

    sell_wait_seconds: int = Query(180, ge=0, le=1800),
    sell_max_backbox_attempts: int = Query(3, ge=1, le=10),

    tradein_market: str = Query("GB", min_length=2, max_length=4),
    tradein_currency: str = Query("GBP", min_length=3, max_length=3),
    tradein_wait_seconds: int = Query(60, ge=0, le=600),

    include_stage_results: bool = Query(False),
    include_item_results: bool = Query(False),
):
    return await run_price_all_for_user(
        db,
        user_id=user_id,
        sell_wait_seconds=sell_wait_seconds,
        sell_max_backbox_attempts=sell_max_backbox_attempts,
        tradein_market=tradein_market,
        tradein_currency=tradein_currency,
        tradein_wait_seconds=tradein_wait_seconds,
        include_stage_results=include_stage_results,
        include_item_results=include_item_results,
    )
