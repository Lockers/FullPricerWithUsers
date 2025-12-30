from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.sell.activation import create_activation_session_for_user
from app.features.backmarket.sell.filters import groups_filter_from_selection
from app.features.backmarket.sell.pricing_cycle import run_sell_pricing_cycle_for_user
from app.features.backmarket.sell.schemas import PricingGroupsFilterIn

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
