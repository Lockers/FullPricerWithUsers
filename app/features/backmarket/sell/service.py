from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.sell.activation import create_activation_session_for_user
from app.features.backmarket.sell.filters import groups_filter_from_selection
from app.features.backmarket.sell.pricing_cycle import run_sell_pricing_cycle_for_user
from app.features.backmarket.sell.schemas import PricingGroupsFilterIn
from app.features.backmarket.tradein.competitors_service import run_tradein_competitor_refresh_for_user

logger = logging.getLogger(__name__)


async def create_activation_session_for_user_with_selection(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    wait_seconds: int = 180,
    do_sell_sync: bool = True,
    selection: Optional[PricingGroupsFilterIn] = None,
) -> Dict[str, Any]:
    """
    Router-facing wrapper that keeps routers dumb.

    Converts a typed selection payload into the Mongo `groups_filter`
    expected by `create_activation_session_for_user(...)`.
    """
    groups_filter = groups_filter_from_selection(selection)

    logger.info(
        "[sell_service] create_activation_session user_id=%s wait_seconds=%s groups_filter=%s",
        user_id,
        wait_seconds,
        groups_filter,
    )

    return await create_activation_session_for_user(
        db,
        user_id,
        wait_seconds=wait_seconds,
        do_sell_sync=do_sell_sync,
        groups_filter=groups_filter,
    )


async def run_sell_pricing_cycle_for_user_with_selection(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    wait_seconds: int = 180,
    max_backbox_attempts: int = 3,
    selection: Optional[PricingGroupsFilterIn] = None,
) -> Dict[str, Any]:
    """
    Router-facing wrapper that keeps routers dumb.

    Converts a typed selection payload into the Mongo `groups_filter`
    expected by `run_sell_pricing_cycle_for_user(...)`.
    """
    groups_filter = groups_filter_from_selection(selection)

    logger.info(
        "[sell_service] pricing_cycle user_id=%s wait_seconds=%s max_backbox_attempts=%s groups_filter=%s",
        user_id,
        wait_seconds,
        max_backbox_attempts,
        groups_filter,
    )

    return await run_sell_pricing_cycle_for_user(
        db,
        user_id,
        wait_seconds=wait_seconds,
        max_backbox_attempts=max_backbox_attempts,
        groups_filter=groups_filter,
    )

async def run_price_all_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    sell_wait_seconds: int = 180,
    sell_max_backbox_attempts: int = 3,
    tradein_market: str = "GB",
    tradein_currency: str = "GBP",
    tradein_wait_seconds: int = 60,
    include_stage_results: bool = False,
    include_item_results: bool = False,
) -> Dict[str, Any]:
    """
    Orchestrator for "Price all".
    - Keeps router dumb.
    - Best-effort: attempts both sell + trade-in and returns both results.
    """
    t0 = time.perf_counter()

    out: Dict[str, Any] = {
        "user_id": user_id,
        "sell": None,
        "tradein": None,
    }

    try:
        out["sell"] = await run_sell_pricing_cycle_for_user(
            db,
            user_id,
            wait_seconds=sell_wait_seconds,
            max_backbox_attempts=sell_max_backbox_attempts,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[price_all] sell failed user_id=%s", user_id)
        out["sell"] = {"ok": False, "error": str(exc), "error_type": exc.__class__.__name__}

    try:
        out["tradein"] = await run_tradein_competitor_refresh_for_user(
            db,
            user_id=user_id,
            market=tradein_market,
            currency=tradein_currency,
            wait_seconds=tradein_wait_seconds,
            include_stage_results=include_stage_results,
            include_item_results=include_item_results,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[price_all] tradein failed user_id=%s", user_id)
        out["tradein"] = {"ok": False, "error": str(exc), "error_type": exc.__class__.__name__}

    out["elapsed_seconds"] = time.perf_counter() - t0
    return out
