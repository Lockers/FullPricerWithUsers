"""
Rate controller (glue code).

Owns:
- per-endpoint in-memory limiters
- cached EndpointRateState objects
- Mongo persistence via repo
- learner policy application

BackMarketClient should call this instead of directly manipulating state/DB.

Concurrency note
----------------
The state object is mutable. If multiple concurrent requests for the same
endpoint mutate it without coordination, you can get:

- exaggerated step-downs (e.g. many concurrent 429s decrement repeatedly)
- odd success streak accounting
- racey persistence

This controller exposes `endpoint_lock()` so the transport client can serialize
state updates *per endpoint* without blocking unrelated endpoints.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.transport.endpoints import endpoint_config
from app.features.backmarket.rate.learner import RateLearner
from app.features.backmarket.rate.limiter import SimpleRateLimiter
from app.features.backmarket.rate.repo import BmRateStateRepository
from app.features.backmarket.rate.state import EndpointRateState

logger = logging.getLogger(__name__)


class RateController:
    """
    Per-user rate controller.

    This class is intended to be created per user_id and reused for many calls.
    """

    def __init__(
        self,
        *,
        user_id: str,
        db: AsyncIOMotorDatabase,
        refresh_interval_seconds: float = 60.0,
    ):
        self.user_id = user_id
        self._repo = BmRateStateRepository(db)
        self._learner = RateLearner()

        # In-memory caches
        self._states: Dict[str, EndpointRateState] = {}
        self._limiters: Dict[str, SimpleRateLimiter] = {}

        # Per-endpoint state mutation locks
        self._locks: Dict[str, asyncio.Lock] = {}

        # Optional periodic refresh so multiple worker processes converge.
        self._refresh_interval = float(refresh_interval_seconds)
        self._last_refresh: Dict[str, float] = {}

    def endpoint_lock(self, endpoint_key: str) -> asyncio.Lock:
        """
        Get/create the per-endpoint lock.

        Transport should use:
            async with rate_controller.endpoint_lock(endpoint_key):
                ... mutate state + persist ...
        """
        lock = self._locks.get(endpoint_key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[endpoint_key] = lock
        return lock

    async def prepare(self, endpoint_key: str) -> Tuple[EndpointRateState, SimpleRateLimiter]:
        """
        Load state (cached) and return the limiter (cached) for this endpoint.

        Refresh behavior:
        - State is cached in-memory for performance.
        - Every `refresh_interval_seconds`, we re-load from Mongo to pick up
          changes from other worker processes.
        """
        # Per-endpoint lock ensures only one coroutine at a time loads/refreshes.
        async with self.endpoint_lock(endpoint_key):
            state = self._states.get(endpoint_key)

            now_m = time.monotonic()
            last = self._last_refresh.get(endpoint_key, 0.0)
            needs_refresh = (state is None) or (
                    0 < self._refresh_interval <= (now_m - last)
            )

            if needs_refresh:
                cfg = endpoint_config(endpoint_key)
                state = await self._repo.load_or_init(user_id=self.user_id, endpoint_key=endpoint_key, cfg=cfg)
                self._states[endpoint_key] = state
                self._last_refresh[endpoint_key] = now_m

                logger.info(
                    "[rate_controller] state_load user_id=%s endpoint=%s current_rps=%s locked_rps=%s "
                    "rps_float=%.3f cooldown_until=%s updated_at=%s",
                    self.user_id,
                    endpoint_key,
                    state.current_rps,
                    state.locked_rps,
                    float(state.rps_float),
                    state.cooldown_until,
                    state.updated_at,
                )

            limiter = self._limiters.get(endpoint_key)
            if limiter is None:
                limiter = SimpleRateLimiter(max_calls=state.current_rps, period=1.0, name=endpoint_key)
                self._limiters[endpoint_key] = limiter
            else:
                limiter.update_max_calls(state.current_rps)

            return state, limiter

    async def persist(self, state: EndpointRateState) -> None:
        """Persist the state to Mongo."""
        await self._repo.save(state)

    def _apply(self, state: EndpointRateState, *, reason: str) -> None:
        """
        Clamp/recompute state and propagate changes into the in-memory limiter.

        Logs at INFO only when the integer RPS changes (to avoid spam).
        """
        old_rps = int(state.current_rps)

        state.clamp_and_recompute()

        new_rps = int(state.current_rps)
        limiter = self._limiters.get(state.endpoint_key)
        if limiter is not None:
            limiter.update_max_calls(new_rps)

        if new_rps != old_rps:
            logger.info(
                "[rate_controller] rps_change user_id=%s endpoint=%s reason=%s %d -> %d locked_rps=%s rps_float=%.3f",
                state.user_id,
                state.endpoint_key,
                reason,
                old_rps,
                new_rps,
                state.locked_rps,
                float(state.rps_float),
            )

    def on_success(self, state: EndpointRateState, status_code: int) -> None:
        self._learner.on_success(state, status_code)
        self._apply(state, reason="success")

    def on_rate_limited(self, state: EndpointRateState, status_code: int) -> None:
        self._learner.on_rate_limited(state, status_code)
        self._apply(state, reason="rate_limited")

    def on_server_error(self, state: EndpointRateState, status_code: int) -> None:
        self._learner.on_server_error(state, status_code)
        self._apply(state, reason="server_error")

    def on_client_error(self, state: EndpointRateState, status_code: int) -> None:
        self._learner.on_client_error(state, status_code)
        self._apply(state, reason="client_error")

    # ---------------------------------------------------------------------
    # Compatibility helpers (older call sites used these names)
    # ---------------------------------------------------------------------

    async def get_state(self, user_id: str, endpoint_key: str, *args) -> EndpointRateState:
        """
        Backwards-compatible alias for older call sites.

        Notes:
        - RateController is per-user, so `user_id` is redundant; we accept it to
          avoid breaking older code.
        """
        _ = args
        if user_id != self.user_id:
            logger.debug("[rate_controller] get_state user_id mismatch got=%s expected=%s", user_id, self.user_id)
        state, _limiter = await self.prepare(endpoint_key)
        return state

    async def get_limiter(self, user_id: str, endpoint_key: str, *args) -> SimpleRateLimiter:
        """Backwards-compatible alias for older call sites."""
        _ = args
        if user_id != self.user_id:
            logger.debug("[rate_controller] get_limiter user_id mismatch got=%s expected=%s", user_id, self.user_id)
        _state, limiter = await self.prepare(endpoint_key)
        return limiter

    def update_limiter(self, endpoint_key: str) -> None:
        """
        Backwards-compatible helper used by some older code paths.

        Re-syncs limiter max_calls from the cached state (if present).
        """
        state = self._states.get(endpoint_key)
        limiter = self._limiters.get(endpoint_key)
        if state is None or limiter is None:
            return
        limiter.update_max_calls(int(state.current_rps))

