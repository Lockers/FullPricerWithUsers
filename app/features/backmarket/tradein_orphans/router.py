from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, Field

from app.db.mongo import get_db
from app.features.backmarket.pricing.pricing_overrides_schemas import ManualSellAnchorGrossUpdate
from app.features.backmarket.pricing.trade_pricing_models import TradePricingGroupSettingsIn
from app.features.backmarket.tradein_orphans.competitors_service import run_tradein_orphan_competitor_refresh_for_user
from app.features.backmarket.tradein_orphans.offers_service import (
    run_tradein_orphan_offer_update_for_orphan,
    run_tradein_orphan_offer_updates_for_user,
)
from app.features.backmarket.tradein_orphans.repo import TradeinOrphansRepo
from app.features.backmarket.tradein_orphans.sell_anchor_service import recompute_sell_anchor_for_orphan
from app.features.backmarket.tradein_orphans.service import (
    list_orphans_for_user,
    set_manual_sell_anchor_for_orphan,
    set_manual_trade_sku_for_orphan,
    set_orphan_enabled,
    sync_tradein_orphans_for_user,
)
from app.features.backmarket.tradein_orphans.trade_pricing_service import (
    recompute_trade_pricing_for_orphan,
    recompute_trade_pricing_for_orphans_for_user,
    update_trade_pricing_settings_for_orphan,
)

router = APIRouter(prefix="/tradein/orphans", tags=["backmarket:tradein-orphans"])


class OrphanEnabledUpdate(BaseModel):
    enabled: bool = Field(..., description="Whether this orphan can be updated on Back Market")


class ManualTradeSkuUpdate(BaseModel):
    manual_trade_sku: Optional[str] = Field(
        default=None,
        description=(
            "Manual override for trade SKU (MAKE-MODEL-STORAGE-CONDITION). "
            "Set null to clear."
        ),
    )


@router.post("/sync/{user_id}")
async def sync_orphans(
    user_id: str,
    limit: Optional[int] = None,
    prune: bool = False,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Materialize trade-in orphans from `pricing_bad_tradein_skus` into `pricing_tradein_orphans`."""
    return await sync_tradein_orphans_for_user(db, user_id=user_id, limit=limit, prune=prune)


@router.get("/{user_id}")
async def list_orphans(
    user_id: str,
    limit: int = 5000,
    include_disabled: bool = True,
    reason_code: Optional[str] = None,
    enabled: Optional[bool] = None,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    docs = await list_orphans_for_user(
        db,
        user_id=user_id,
        limit=limit,
        include_disabled=include_disabled if enabled is None else True,
        reason_code=reason_code,
        enabled=enabled,
    )
    return {"user_id": user_id, "count": len(docs), "items": docs}


@router.get("/{user_id}/{orphan_id}")
async def get_orphan(
    user_id: str,
    orphan_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    repo = TradeinOrphansRepo(db)
    doc = await repo.get_one(user_id=user_id, orphan_id=orphan_id)
    return {"user_id": user_id, "orphan": doc}


@router.patch("/{user_id}/{orphan_id}/enabled")
async def patch_orphan_enabled(
    user_id: str,
    orphan_id: str,
    body: OrphanEnabledUpdate,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    updated = await set_orphan_enabled(db, user_id=user_id, orphan_id=orphan_id, enabled=body.enabled)
    return {"user_id": user_id, "orphan_id": orphan_id, "updated": updated}


@router.patch("/{user_id}/{orphan_id}/manual-trade-sku")
async def patch_manual_trade_sku(
    user_id: str,
    orphan_id: str,
    body: ManualTradeSkuUpdate,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    updated = await set_manual_trade_sku_for_orphan(
        db,
        user_id=user_id,
        orphan_id=orphan_id,
        manual_trade_sku=body.manual_trade_sku,
    )

    # Best-effort recompute chain.
    await recompute_sell_anchor_for_orphan(db, user_id=user_id, orphan_id=orphan_id)
    await recompute_trade_pricing_for_orphan(db, user_id=user_id, orphan_id=orphan_id)

    return {"user_id": user_id, "orphan_id": orphan_id, "updated": updated}


@router.patch("/{user_id}/{orphan_id}/manual-sell-anchor")
async def patch_manual_sell_anchor(
    user_id: str,
    orphan_id: str,
    body: ManualSellAnchorGrossUpdate,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    await set_manual_sell_anchor_for_orphan(
        db,
        user_id=user_id,
        orphan_id=orphan_id,
        manual_sell_anchor_gross=body.manual_sell_anchor_gross,
    )

    sell_anchor = await recompute_sell_anchor_for_orphan(db, user_id=user_id, orphan_id=orphan_id)
    pricing = await recompute_trade_pricing_for_orphan(db, user_id=user_id, orphan_id=orphan_id)

    return {"user_id": user_id, "orphan_id": orphan_id, "sell_anchor": sell_anchor, "trade_pricing": pricing}


@router.patch("/{user_id}/{orphan_id}/trade-pricing-settings")
async def patch_trade_pricing_settings(
    user_id: str,
    orphan_id: str,
    body: TradePricingGroupSettingsIn,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    await update_trade_pricing_settings_for_orphan(db, user_id=user_id, orphan_id=orphan_id, settings_in=body)
    pricing = await recompute_trade_pricing_for_orphan(db, user_id=user_id, orphan_id=orphan_id)
    return {"user_id": user_id, "orphan_id": orphan_id, "trade_pricing": pricing}


@router.post("/recompute/{user_id}")
async def recompute_orphans(
    user_id: str,
    limit: Optional[int] = None,
    include_disabled: bool = True,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await recompute_trade_pricing_for_orphans_for_user(
        db,
        user_id=user_id,
        limit=limit,
        only_enabled=None if include_disabled else True,
    )


@router.post("/competitors/{user_id}/refresh")
async def refresh_orphan_competitors(
    user_id: str,
    market: str = "GB",
    currency: str = "GBP",
    limit: Optional[int] = None,
    include_disabled: bool = True,
    update_concurrency: int = 10,
    fetch_concurrency: int = 10,
    wait_seconds: int = 60,
    fetch_attempts: int = 3,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await run_tradein_orphan_competitor_refresh_for_user(
        db,
        user_id=user_id,
        market=market,
        currency=currency,
        limit=limit,
        include_disabled=include_disabled,
        update_concurrency=update_concurrency,
        fetch_concurrency=fetch_concurrency,
        wait_seconds=wait_seconds,
        fetch_attempts=fetch_attempts,
    )


@router.post("/offers/{user_id}/apply")
async def apply_orphan_offers(
    user_id: str,
    market: str = "GB",
    currency: str = "GBP",
    dry_run: bool = True,
    require_ok_to_update: bool = True,
    only_enabled: bool = True,
    limit: Optional[int] = None,
    concurrency: int = 10,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await run_tradein_orphan_offer_updates_for_user(
        db,
        user_id=user_id,
        market=market,
        currency=currency,
        dry_run=dry_run,
        require_ok_to_update=require_ok_to_update,
        only_enabled=only_enabled,
        limit=limit,
        concurrency=concurrency,
    )


@router.post("/offers/{user_id}/apply/{orphan_id}")
async def apply_orphan_offer_one(
    user_id: str,
    orphan_id: str,
    market: str = "GB",
    currency: str = "GBP",
    dry_run: bool = True,
    require_ok_to_update: bool = True,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await run_tradein_orphan_offer_update_for_orphan(
        db,
        user_id=user_id,
        orphan_id=orphan_id,
        market=market,
        currency=currency,
        dry_run=dry_run,
        require_ok_to_update=require_ok_to_update,
    )

