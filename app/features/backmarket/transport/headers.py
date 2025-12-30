"""Build Back Market auth headers for a user.

This isolates "how do we read credentials" from the HTTP client logic.

The codebase historically had `get_user_bm_credentials` in different argument
orders. This module keeps a defensive fallback so the transport doesn't break
randomly during migration.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.transport.exceptions import BMMisconfiguredError
from app.features.users.settings_service import get_user_bm_credentials


async def build_bm_headers(db: AsyncIOMotorDatabase, user_id: str) -> Dict[str, str]:
    """Load credentials for this user and build standard request headers."""

    # Try the conventional signature first: (db, user_id)
    try:
        creds_obj: Any = await get_user_bm_credentials(db, user_id)
    except (TypeError, AttributeError):
        # Fall back to legacy signature ordering: (user_id, db)
        creds_obj = await get_user_bm_credentials(db, user_id)

    api_key: Optional[str] = getattr(creds_obj, "api_key", None)
    user_agent: Optional[str] = getattr(creds_obj, "user_agent", None)
    market_language: Optional[str] = getattr(creds_obj, "market_language", None)

    if isinstance(creds_obj, dict):
        api_key = creds_obj.get("api_key") or creds_obj.get("bm_api_key") or api_key
        user_agent = creds_obj.get("user_agent") or user_agent
        market_language = creds_obj.get("market_language") or market_language

    api_key = (api_key or "").strip()
    user_agent = (user_agent or "").strip()
    market_language = (market_language or "").strip() or None

    if not api_key or not user_agent:
        raise BMMisconfiguredError(f"User {user_id} missing Back Market credentials")

    auth_value = api_key if api_key.lower().startswith("basic ") else f"Basic {api_key}"

    headers_out: Dict[str, str] = {
        "Authorization": auth_value,
        "User-Agent": user_agent,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    if market_language:
        headers_out["Accept-Language"] = market_language

    return headers_out


@dataclass(frozen=True)
class _CacheEntry:
    expires_at_m: float
    headers: Dict[str, str]


class BackMarketHeadersBuilder:
    """Small per-process TTL cache around :func:`build_bm_headers`."""

    def __init__(self, db: AsyncIOMotorDatabase, *, ttl_seconds: float = 60.0):
        self._db = db
        self._ttl_seconds = float(ttl_seconds)
        self._cache: Dict[str, _CacheEntry] = {}
        self._lock = asyncio.Lock()

    async def build_headers(self, user_id: str, endpoint_key: str | None = None) -> Dict[str, str]:
        """Return auth headers for this user.

        `endpoint_key` is accepted for call-site convenience, but headers are
        currently cached per-user only.
        """

        _ = endpoint_key  # unused (reserved for future per-endpoint custom headers)

        now_m = time.monotonic()
        entry = self._cache.get(user_id)
        if entry is not None and entry.expires_at_m > now_m:
            return dict(entry.headers)

        # Avoid thundering herd on cache miss.
        async with self._lock:
            now_m = time.monotonic()
            entry = self._cache.get(user_id)
            if entry is not None and entry.expires_at_m > now_m:
                return dict(entry.headers)

            headers = await build_bm_headers(self._db, user_id)
            self._cache[user_id] = _CacheEntry(expires_at_m=now_m + self._ttl_seconds, headers=dict(headers))
            return dict(headers)
