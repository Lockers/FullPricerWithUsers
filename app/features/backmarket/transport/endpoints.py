"""Back Market endpoint configuration.

Endpoints are referenced by a stable key (string) throughout the transport/rate
system. Each endpoint has a base/min/max RPS which drives the adaptive learner.

If an endpoint has non-standard "successful" statuses (e.g. 404 meaning
"competitor missing"), define it in :data:`ENDPOINT_ALLOWED_STATUSES`.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EndpointConfig:
    base_rps: int
    min_rps: int = 1
    max_rps: int = 50


# Default endpoint configurations.
# Tune these based on production observations.
ENDPOINTS: dict[str, EndpointConfig] = {
    "sell_listings_get": EndpointConfig(base_rps=10, min_rps=1, max_rps=40),
    "sell_listing_update": EndpointConfig(base_rps=4, min_rps=1, max_rps=10),
    "sell_orders_get": EndpointConfig(base_rps=6, min_rps=1, max_rps=20),
}


# Some endpoints treat certain 4xx statuses as a normal outcome.
# Example: a competitor listing missing might be represented as 404.
ENDPOINT_ALLOWED_STATUSES: dict[str, set[int]] = {
    # "sell_competitor_lookup": {404},
}


def endpoint_config(endpoint_key: str) -> EndpointConfig:
    """Return the EndpointConfig for the given endpoint key."""

    cfg = ENDPOINTS.get(endpoint_key)
    if cfg is None:
        # Safe defaults for unknown endpoints.
        return EndpointConfig(base_rps=5, min_rps=1, max_rps=15)
    return cfg
