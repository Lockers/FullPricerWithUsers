"""Adaptive rate learning policy.

This policy controls how :attr:`EndpointRateState.rps_float` changes over time.

Key requirements
----------------
- Increase slowly: sustained 2xx responses should gradually raise RPS, but the
  step-up cadence must be conservative to avoid sudden rate-limit storms.

- Decrease quickly: if we see repeated 429 / Cloudflare-generated 503 responses,
  we must step down rapidly.

- Only throttle on rate-limit signals: 429 and Cloudflare-generated 503 are
  treated as rate-limit signals. Other 4xx/5xx responses are logged and retried
  by the transport client, but they should not directly change the learned RPS.

Terminology
-----------
- Unlocked: we have not yet observed rate limiting for this endpoint. We can
  explore upward more aggressively.

- Locked: we have observed rate limiting. We keep a persistent ``locked_rps``
  ceiling and only increase it cautiously.
"""

from __future__ import annotations

from app.features.backmarket.rate.state import EndpointRateState


class RateLearner:
    """Policy object that mutates an :class:`EndpointRateState` in-place.

    The methods are deliberately static: the learner has no per-instance state.
    """

    # How often to step up after successes.
    # NOTE: these are consecutive successes since the last non-success.
    UNLOCKED_STEP_UP_EVERY_SUCCESSES = 50
    LOCKED_STEP_UP_EVERY_SUCCESSES = 100

    # How much to change the underlying float RPS by per step.
    STEP_UP_DELTA = 0.5
    STEP_DOWN_DELTA = 0.5

    # How many consecutive 429/CF503 responses are required before we step down.
    RATE_LIMIT_STREAK_TO_STEP_DOWN = 3

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

        step_every = (
            RateLearner.LOCKED_STEP_UP_EVERY_SUCCESSES
            if state.locked_rps is not None
            else RateLearner.UNLOCKED_STEP_UP_EVERY_SUCCESSES
        )

        if state.consecutive_successes < step_every:
            return

        # Reset the streak counter *before* mutation so repeated calls don't run away.
        state.consecutive_successes = 0

        # Step up.
        state.rps_float = float(state.rps_float) + float(RateLearner.STEP_UP_DELTA)

        # In locked mode, also move the ceiling upward (conservatively).
        if state.locked_rps is not None:
            state.locked_rps = float(state.locked_rps) + float(RateLearner.STEP_UP_DELTA)

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

        # First time we see rate limiting, lock the ceiling at the current value.
        if state.locked_rps is None:
            state.locked_rps = float(state.rps_float)

        if state.consecutive_429s < RateLearner.RATE_LIMIT_STREAK_TO_STEP_DOWN:
            return

        # Step down once per N consecutive rate limits.
        state.consecutive_429s = 0

        state.rps_float = float(state.rps_float) - float(RateLearner.STEP_DOWN_DELTA)

        if state.locked_rps is not None:
            state.locked_rps = float(state.locked_rps) - float(RateLearner.STEP_DOWN_DELTA)

    @staticmethod
    def on_server_error(state: EndpointRateState, status_code: int) -> None:
        """Record a server error without changing the learned RPS.

        Requirement: only throttle on 429 / CF503.
        """
        state.last_status = int(status_code)
        state.consecutive_successes = 0
        state.consecutive_429s = 0
        state.error_count += 1

    @staticmethod
    def on_client_error(state: EndpointRateState, status_code: int) -> None:
        """Record a client error without changing the learned RPS."""
        state.last_status = int(status_code)
        state.consecutive_successes = 0
        state.consecutive_429s = 0
        state.error_count += 1
