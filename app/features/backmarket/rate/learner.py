"""Adaptive rate + concurrency learning policy.

This policy controls how `EndpointRateState.rps_float` and
`EndpointRateState.concurrency_float` change over time.

Key requirements
----------------
- Increase slowly: sustained 2xx responses should gradually raise RPS and
  (more cautiously) concurrency.

- Decrease quickly: if we see repeated 429 / Cloudflare-generated 503 responses,
  we step down quickly. In practice, many "patterned" rate-limit storms are
  **concurrency-triggered** (too many in-flight requests), so we step down
  concurrency immediately and only step down RPS after a short streak.

- Only throttle on rate-limit signals: 429 and Cloudflare-generated 503 are
  treated as rate-limit signals. Other 4xx/5xx responses are logged and retried
  by the transport client, but they should not directly change the learned RPS
  or concurrency.

Terminology
-----------
- Unlocked: we have not yet observed rate limiting for this endpoint. We can
  explore upward more aggressively.

- Locked: we have observed rate limiting. We keep persistent ceilings
  (`locked_rps`, `locked_concurrency`) and only increase them cautiously.
"""

from __future__ import annotations

from app.features.backmarket.rate.state import EndpointRateState


class RateLearner:
    """Policy object that mutates an :class:`EndpointRateState` in-place."""

    # --------------------
    # RPS policy
    # --------------------

    # How often to step up after successes.
    # NOTE: these are consecutive successes since the last non-success.
    UNLOCKED_STEP_UP_EVERY_SUCCESSES = 50
    LOCKED_STEP_UP_EVERY_SUCCESSES = 100

    # How much to change the underlying float RPS by per step.
    STEP_UP_DELTA = 0.25
    STEP_DOWN_DELTA = 0.25

    # How many consecutive 429/CF503 responses are required before we step down RPS.
    RATE_LIMIT_STREAK_TO_STEP_DOWN = 5

    # --------------------
    # Concurrency policy
    # --------------------

    # Concurrency is typically the first lever to pull to stop 429 storms.
    CONC_STEP_DOWN_DELTA = 1.0

    # Step up concurrency much more slowly than RPS.
    # Uses *total* success_count so it keeps creeping up over long stable periods.
    UNLOCKED_CONC_STEP_UP_EVERY_SUCCESSES = 100
    LOCKED_CONC_STEP_UP_EVERY_SUCCESSES = 200
    CONC_STEP_UP_DELTA = 1.0

    # --------------------
    # Cooldown
    # --------------------

    # Cooldown after a rate-limit signal. While in cooldown we do not step up.
    COOLDOWN_SECONDS_ON_RATE_LIMIT = 20.0

    @staticmethod
    def on_success(state: EndpointRateState, status_code: int) -> None:
        state.last_status = int(status_code)

        # Success breaks a rate-limit streak.
        state.consecutive_429s = 0

        # Count successes.
        state.consecutive_successes += 1
        state.success_count += 1

        # Do not step up while cooling down.
        if state.in_cooldown():
            return

        # ---- RPS step up (consecutive successes) ----
        step_every_rps = (
            RateLearner.LOCKED_STEP_UP_EVERY_SUCCESSES
            if state.locked_rps is not None
            else RateLearner.UNLOCKED_STEP_UP_EVERY_SUCCESSES
        )

        if state.consecutive_successes >= step_every_rps:
            # Reset the streak counter *before* mutation so repeated calls don't run away.
            state.consecutive_successes = 0

            state.rps_float = float(state.rps_float) + float(RateLearner.STEP_UP_DELTA)

            # In locked mode, also move the ceiling upward (conservatively).
            if state.locked_rps is not None:
                state.locked_rps = float(state.locked_rps) + float(RateLearner.STEP_UP_DELTA)

        # ---- Concurrency step up (total successes) ----
        step_every_c = (
            RateLearner.LOCKED_CONC_STEP_UP_EVERY_SUCCESSES
            if state.locked_concurrency is not None
            else RateLearner.UNLOCKED_CONC_STEP_UP_EVERY_SUCCESSES
        )

        if step_every_c > 0 and (int(state.success_count) % int(step_every_c) == 0):
            state.concurrency_float = float(state.concurrency_float) + float(RateLearner.CONC_STEP_UP_DELTA)

            # In locked mode, also move the ceiling upward (very cautiously).
            if state.locked_concurrency is not None:
                state.locked_concurrency = float(state.locked_concurrency) + float(RateLearner.CONC_STEP_UP_DELTA)

    @staticmethod
    def on_rate_limited(state: EndpointRateState, status_code: int) -> None:
        state.last_status = int(status_code)

        # Any rate-limit signal breaks success streak.
        state.consecutive_successes = 0

        # Track streak of consecutive rate limits.
        state.consecutive_429s += 1
        state.error_count += 1

        # Enter cooldown immediately.
        state.set_cooldown(RateLearner.COOLDOWN_SECONDS_ON_RATE_LIMIT)

        # First time we see rate limiting, lock ceilings at the current values.
        if state.locked_rps is None:
            state.locked_rps = float(state.rps_float)
        if state.locked_concurrency is None:
            state.locked_concurrency = float(state.concurrency_float)

        # Step down concurrency immediately (most 429 storms are concurrency-triggered).
        state.concurrency_float = float(state.concurrency_float) - float(RateLearner.CONC_STEP_DOWN_DELTA)
        if state.locked_concurrency is not None:
            state.locked_concurrency = float(state.locked_concurrency) - float(RateLearner.CONC_STEP_DOWN_DELTA)

        # Only step down RPS once per N consecutive rate limits.
        if state.consecutive_429s < RateLearner.RATE_LIMIT_STREAK_TO_STEP_DOWN:
            return

        state.consecutive_429s = 0

        state.rps_float = float(state.rps_float) - float(RateLearner.STEP_DOWN_DELTA)

        if state.locked_rps is not None:
            state.locked_rps = float(state.locked_rps) - float(RateLearner.STEP_DOWN_DELTA)

    @staticmethod
    def on_server_error(state: EndpointRateState, status_code: int) -> None:
        """Record a server error without changing the learned RPS or concurrency."""

        state.last_status = int(status_code)
        state.consecutive_successes = 0
        state.consecutive_429s = 0
        state.error_count += 1

    @staticmethod
    def on_client_error(state: EndpointRateState, status_code: int) -> None:
        """Record a client error without changing the learned RPS or concurrency."""

        state.last_status = int(status_code)
        state.consecutive_successes = 0
        state.consecutive_429s = 0
        state.error_count += 1
