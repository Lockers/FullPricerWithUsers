"""Per-endpoint circuit breaker.

The circuit breaker protects Back Market (and our own workers) from a thundering
herd when an endpoint is consistently failing due to upstream issues.

Important behaviour:
- The breaker opens only on transport errors and 5xx responses (as decided by
  the transport client).
- Rate limiting (429 / Cloudflare 503) should *not* count as a breaker failure.

When the breaker is open, :meth:`before_call` raises :class:`BMRateLimited` with
a retry_after value so the transport can sleep and retry.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from app.features.backmarket.transport.exceptions import BMRateLimited


@dataclass
class _BreakerState:
    failures: int = 0
    open_until_m: float = 0.0


class CircuitBreaker:
    """Simple async-friendly circuit breaker."""

    def __init__(
        self,
        *,
        name: str = "default",
        failure_threshold: int = 5,
        cooldown_seconds: float = 10.0,
    ):
        self.name = str(name)
        self.failure_threshold = max(1, int(failure_threshold))
        self.cooldown_seconds = max(0.0, float(cooldown_seconds))
        self._state = _BreakerState()

    async def before_call(self) -> None:
        """Raise if the breaker is open."""

        now = time.monotonic()
        if now < self._state.open_until_m:
            retry_after = max(0.0, self._state.open_until_m - now)
            raise BMRateLimited(f"Circuit breaker open for {retry_after:.1f}s", retry_after=retry_after)

    async def after_success(self) -> None:
        """Reset failure streak on success."""

        self._state.failures = 0
        self._state.open_until_m = 0.0

    async def after_failure(self) -> None:
        """Record a failure and open the breaker if threshold reached."""

        self._state.failures += 1
        if self._state.failures >= self.failure_threshold:
            self._state.open_until_m = time.monotonic() + self.cooldown_seconds
            self._state.failures = 0
