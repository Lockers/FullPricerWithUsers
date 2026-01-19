"""Rate controller (glue code).

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

import anyio
from anyio import CapacityLimiter
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.transport.endpoints import endpoint_config
from app.features.backmarket.rate.learner import RateLearner
from app.features.backmarket.rate.limiter import SimpleRateLimiter
from app.features.backmarket.rate.repo import BmRateStateRepository
from app.features.backmarket.rate.state import EndpointRateState

logger = logging.getLogger(__name__)


class RateController:
    """Per-user adaptive rate controller."""

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
        self._concurrency_limiters: Dict[str, CapacityLimiter] = {}

        # Per-endpoint state mutation locks
        self._locks: Dict[str, asyncio.Lock] = {}

        # Optional periodic refresh so multiple worker processes converge.
        self._refresh_interval = float(refresh_interval_seconds)
        self._last_refresh: Dict[str, float] = {}

    def endpoint_lock(self, endpoint_key: str) -> asyncio.Lock:
        """Get/create the per-endpoint lock."""

        lock = self._locks.get(endpoint_key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[endpoint_key] = lock
        return lock

    async def prepare(self, endpoint_key: str) -> Tuple[EndpointRateState, SimpleRateLimiter, CapacityLimiter]:
        """Load state (cached) and return the limiters (cached) for this endpoint.

        Refresh behavior:
        - State is cached in-memory for performance.
        - Every `refresh_interval_seconds`, we re-load from Mongo to pick up
          changes from other worker processes.
        """

        async with self.endpoint_lock(endpoint_key):
            state = self._states.get(endpoint_key)

            now_m = time.monotonic()
            last = self._last_refresh.get(endpoint_key, 0.0)
            needs_refresh = (state is None) or (0 < self._refresh_interval <= (now_m - last))

            if needs_refresh:
                cfg = endpoint_config(endpoint_key)
                state = await self._repo.load_or_init(user_id=self.user_id, endpoint_key=endpoint_key, cfg=cfg)
                self._states[endpoint_key] = state
                self._last_refresh[endpoint_key] = now_m

                logger.info(
                    "[rate_controller] state_load user_id=%s endpoint=%s rps=%s locked_rps=%s rps_float=%.3f "
                    "conc=%s locked_conc=%s conc_float=%.3f cooldown_until=%s updated_at=%s",
                    self.user_id,
                    endpoint_key,
                    state.current_rps,
                    state.locked_rps,
                    float(state.rps_float),
                    state.current_concurrency,
                    state.locked_concurrency,
                    float(state.concurrency_float),
                    state.cooldown_until,
                    state.updated_at,
                )

            # RPS limiter
            limiter = self._limiters.get(endpoint_key)
            if limiter is None:
                limiter = SimpleRateLimiter(max_calls=float(state.current_rps), period=1.0, name=endpoint_key)
                self._limiters[endpoint_key] = limiter
            else:
                limiter.update_max_calls(float(state.current_rps))

            # Concurrency limiter (anyio CapacityLimiter)
            conc = self._concurrency_limiters.get(endpoint_key)
            if conc is None:
                conc = anyio.CapacityLimiter(max(1, int(state.current_concurrency)))
                self._concurrency_limiters[endpoint_key] = conc
            else:
                # If borrowed_tokens > total_tokens, anyio allows shrinking; new
                # borrowers will block until releases bring it under the cap.
                conc.total_tokens = max(1, int(state.current_concurrency))

            return state, limiter, conc

    async def persist(self, state: EndpointRateState) -> None:
        """Persist the state to Mongo."""

        await self._repo.save(state)

    def _apply(self, state: EndpointRateState, *, reason: str) -> None:
        """Clamp/recompute state and propagate changes into in-memory limiters."""

        old_rps = float(state.current_rps)
        old_conc = int(state.current_concurrency)

        state.clamp_and_recompute()

        new_rps = float(state.current_rps)
        new_conc = int(state.current_concurrency)

        limiter = self._limiters.get(state.endpoint_key)
        if limiter is not None:
            limiter.update_max_calls(float(new_rps))

        conc = self._concurrency_limiters.get(state.endpoint_key)
        if conc is not None:
            conc.total_tokens = max(1, int(new_conc))

        if abs(new_rps - old_rps) > 1e-9:
            logger.info(
                "[rate_controller] rps_change user_id=%s endpoint=%s reason=%s %.3f -> %.3f locked_rps=%s rps_float=%.3f",
                state.user_id,
                state.endpoint_key,
                reason,
                old_rps,
                new_rps,
                state.locked_rps,
                float(state.rps_float),
            )

        if new_conc != old_conc:
            logger.info(
                "[rate_controller] conc_change user_id=%s endpoint=%s reason=%s %d -> %d locked_conc=%s conc_float=%.3f",
                state.user_id,
                state.endpoint_key,
                reason,
                old_conc,
                new_conc,
                state.locked_concurrency,
                float(state.concurrency_float),
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
        """Backwards-compatible alias for older call sites."""

        _ = args
        if user_id != self.user_id:
            logger.debug("[rate_controller] get_state user_id mismatch got=%s expected=%s", user_id, self.user_id)
        state, _limiter, _conc = await self.prepare(endpoint_key)
        return state

    async def get_limiter(self, user_id: str, endpoint_key: str, *args) -> SimpleRateLimiter:
        """Backwards-compatible alias for older call sites."""

        _ = args
        if user_id != self.user_id:
            logger.debug("[rate_controller] get_limiter user_id mismatch got=%s expected=%s", user_id, self.user_id)
        _state, limiter, _conc = await self.prepare(endpoint_key)
        return limiter

    async def get_concurrency_limiter(self, endpoint_key: str) -> CapacityLimiter:
        """Return the per-endpoint concurrency limiter.

        Prefer using `prepare()` in the transport client (it creates the limiter
        if missing).
        """

        _state, _rps_limiter, conc = await self.prepare(endpoint_key)
        return conc

    def update_limiter(self, endpoint_key: str) -> None:
        """Backwards-compatible helper used by some older code paths.

        Re-syncs limiter max_calls + concurrency from the cached state (if present).
        """

        state = self._states.get(endpoint_key)
        if state is None:
            return

        limiter = self._limiters.get(endpoint_key)
        if limiter is not None:
            limiter.update_max_calls(float(state.current_rps))

        conc = self._concurrency_limiters.get(endpoint_key)
        if conc is not None:
            conc.total_tokens = max(1, int(state.current_concurrency))
