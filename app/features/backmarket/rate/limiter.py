"""In-process async rate limiter.

The original implementation used a rolling 1-second window with a simple call
counter. That design tends to produce bursts:

- N concurrent coroutines can all pass immediately until the per-second counter
  is exhausted.
- Once exhausted, the remaining coroutines sleep until the same window reset
  boundary, then all resume at once.

Cloudflare / bot protection often rate-limits based on **burstiness** and/or
**concurrency**, not just average RPS. To reduce patterned 429 storms, this
limiter spaces requests evenly using a simple "next allowed time" schedule
(leaky-bucket style).

Fractional RPS
--------------
Some Back Market endpoints appear to require < 1 request/second. This limiter
therefore supports fractional rates (e.g. 0.75 rps).

Interface
---------
- `update_max_calls(rate_per_sec: float)` updates the target rate.
- `acquire()` awaits until the caller is permitted to proceed and returns the
  time slept.

Scope
-----
- Per-process only (no shared state across workers)
- Per-endpoint instances are owned by `RateController`
"""

from __future__ import annotations

import asyncio
import math
import time


def _safe_rate(value: float) -> float:
    """Coerce a rate to a finite positive float.

    We never allow <= 0 because that would stall the pipeline indefinitely.
    """

    try:
        r = float(value)
    except (TypeError, ValueError):
        r = 1.0

    if not math.isfinite(r) or r <= 0:
        r = 1.0

    # Absolute floor so interval doesn't become absurdly huge.
    return max(0.01, r)


class SimpleRateLimiter:
    """A lightweight smoothed async rate limiter.

    This enforces a *smoothed* rate_per_sec (= max_calls/period). It does not
    allow large bursts; instead, it schedules each permit at least
    (period / rate_per_sec) after the previous permit.
    """

    def __init__(self, *, max_calls: float, period: float, name: str = ""):
        # `period` is kept for backwards compatibility; callers use period=1.0.
        self._period = max(0.0001, float(period))
        self._name = str(name)

        # Treat `max_calls` as "calls per period" which equals RPS when period=1.
        self._rate = _safe_rate(float(max_calls))
        self._interval = self._period / float(self._rate)

        # Earliest time the next call is allowed to start.
        self._next_allowed = time.monotonic()
        self._lock = asyncio.Lock()

    def update_max_calls(self, max_calls: float) -> None:
        # Backwards-compatible name; value is treated as "calls per period".
        self._rate = _safe_rate(float(max_calls))
        self._interval = self._period / float(self._rate)

        # Keep next_allowed from drifting too far into the past.
        now = time.monotonic()
        if self._next_allowed < now:
            self._next_allowed = now

    async def acquire(self) -> float:
        """Acquire permission to proceed.

        Returns
        -------
        float
            The amount of time slept (seconds) while waiting for availability.
        """

        async with self._lock:
            now = time.monotonic()
            if now >= self._next_allowed:
                # We can proceed immediately.
                self._next_allowed = now + self._interval
                sleep_for = 0.0
            else:
                # We need to wait; reserve our slot in the schedule.
                sleep_for = self._next_allowed - now
                self._next_allowed = self._next_allowed + self._interval

        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
        return float(sleep_for)
