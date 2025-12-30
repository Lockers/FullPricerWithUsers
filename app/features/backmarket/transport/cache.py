"""
Per-user BackMarketClient cache.

Important:
- This cache exists per-process. If you run multiple Uvicorn workers, each worker
  has its own cache (that's normal).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict

import httpx
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import config
from app.features.backmarket.transport.client import BackMarketClient

logger = logging.getLogger(__name__)

_clients_by_user: Dict[str, BackMarketClient] = {}
_clients_lock = asyncio.Lock()


async def get_bm_client_for_user(db: AsyncIOMotorDatabase, user_id: str) -> BackMarketClient:
    """
    Return a cached BackMarketClient for this user (per-process).

    Fixes the "base_url missing" issue by passing global config values into the client,
    instead of relying on env vars the client may not read.
    """
    if not isinstance(user_id, str):
        raise TypeError("user_id must be a string")

    client = _clients_by_user.get(user_id)
    if client is not None:
        return client

    async with _clients_lock:
        # Double-check inside lock.
        client = _clients_by_user.get(user_id)
        if client is None:
            client = BackMarketClient(
                user_id=user_id,
                db=db,
                base_url=config.bm_api_base_url,
                proxy_url=config.proxy_url,
                timeout_seconds=config.bm_timeout_seconds,
            )
            _clients_by_user[user_id] = client

    return client


async def shutdown_bm_clients() -> None:
    """
    Close all cached clients.

    Called on app shutdown.
    """
    for client in list(_clients_by_user.values()):
        try:
            await client.aclose()
        except (RuntimeError, httpx.HTTPError):
            logger.exception("Error closing BackMarketClient")

    _clients_by_user.clear()



