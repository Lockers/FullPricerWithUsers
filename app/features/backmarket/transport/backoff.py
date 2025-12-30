"""
Retry / backoff timings.

Keep these pure so they are testable and reusable.
"""

from __future__ import annotations

import random
from typing import Optional

MIN_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 8.0


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff with jitter."""
    base = MIN_BACKOFF_SECONDS * (2 ** (attempt - 1))
    base = min(base, MAX_BACKOFF_SECONDS)
    jitter = random.uniform(0.0, 0.25)
    return base + jitter


def client_error_sleep_seconds(attempt: int) -> float:
    """Short backoff for 4xx (non-429) retries."""
    base = min(2.0, 0.5 + 0.25 * (attempt - 1))
    jitter = random.uniform(0.0, 0.15)
    return base + jitter


def rate_limit_sleep_seconds(attempt: int, retry_after: Optional[float]) -> float:
    """
    Sleep after a rate-limit response.

    - Prefer Retry-After if provided.
    - Otherwise use a small increasing delay.
    """
    if retry_after is not None:
        return max(0.0, retry_after) + random.uniform(0.0, 0.25)

    base = min(10.0, 1.5 + 0.75 * (attempt - 1))
    return base + random.uniform(0.0, 0.25)
