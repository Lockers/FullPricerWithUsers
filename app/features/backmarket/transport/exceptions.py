"""
Exceptions for Back Market transport + rate modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


class BMClientError(RuntimeError):
    """
    Base class for Back Market client failures.

    Domain code often catches BMClientError to handle "expected" client/runtime failures
    without crashing a whole batch.
    """


class BMTransportError(BMClientError):
    """Base class for Back Market transport errors (HTTP/retries/rate/circuit)."""


class BMMisconfiguredError(BMTransportError):
    """Raised when required Back Market configuration/credentials are missing."""


class BMMaxRetriesError(BMTransportError):
    """Raised when the client exhausts configured retries without a successful response."""


@dataclass
class BMRateLimited(BMTransportError):
    """
    Raised when we must wait before retrying.

    Used for:
      - HTTP 429
      - Cloudflare-generated 503
      - Circuit breaker open

    `retry_after` is seconds (if known).
    """

    message: str
    retry_after: Optional[float] = None

    def __post_init__(self) -> None:
        # Populate Exception args for nicer logging/tracebacks.
        super().__init__(self.message)

    def __str__(self) -> str:  # pragma: no cover
        return self.message


