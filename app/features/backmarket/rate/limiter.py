"""Simple in-process rate limiter.

This limiter is intentionally lightweight:
- Per-process only (no shared state across workers)
- Uses a rolling one-second window

The controller adjusts `max_calls` dynamically as the learner changes the
learned integer RPS.
"""

from __future__ import annotations

import asyncio
import time


class SimpleRateLimiter:
    """A simple asynchronous rate limiter for per-second request limiting."""

    def __init__(self, *, max_calls: int, period: float, name: str = ""):

        self._max_calls = max(1, int(max_calls))
        self._period = float(period)
        self._calls = 0
        self._window_start = time.monotonic()
        self._lock = asyncio.Lock()
        self._name = name

    def update_max_calls(self, max_calls: int) -> None:
        self._max_calls = max(1, int(max_calls))

    async def acquire(self) -> float | None:
        """Acquire permission to proceed.

        Returns
        -------
        float
            The amount of time slept (seconds) while waiting for availability.
        """

        slept_total = 0.0

        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._window_start

                # Reset window if period elapsed.
                if elapsed >= self._period:
                    self._calls = 0
                    self._window_start = now

                if self._calls < self._max_calls:
                    self._calls += 1
                    return slept_total

                # Need to wait until window resets.
                sleep_for = max(0.0, self._period - elapsed)

            # Sleep outside the lock.
            if sleep_for <= 0:
                # Busy loop protection: yield control.
                await asyncio.sleep(0)
                continue

            slept_total += sleep_for
            await asyncio.sleep(sleep_for)
