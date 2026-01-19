"""Back Market endpoint configuration.

Endpoints are referenced by a stable key (string) throughout the transport/rate
system. Each endpoint has:
- base/min/max RPS
- base/min/max concurrency (max in-flight requests)

The adaptive learner persists state per (user_id, endpoint_key) and gradually
adjusts both dimensions to find a sweet spot that avoids 429/Cloudflare blocks.

If an endpoint has non-standard "successful" statuses (e.g. 404 meaning
"competitor missing"), define it in `ENDPOINT_ALLOWED_STATUSES`.
"""

from __future__ import annotations

from app.features.backmarket.rate.state import EndpointConfig


# Default endpoint configurations.
# Tune these based on production observations.
ENDPOINTS: dict[str, EndpointConfig] = {
    # SELL
    "sell_listings_get": EndpointConfig(
        base_rps=15,
        min_rps=5,
        max_rps=40,
        base_concurrency=5,
        min_concurrency=1,
        max_concurrency=16,
    ),
    "sell_listing_update": EndpointConfig(
        base_rps=5,
        min_rps=4,
        max_rps=8,
        base_concurrency=8,
        min_concurrency=5,
        max_concurrency=20,
    ),
    "sell_orders_get": EndpointConfig(
        base_rps=6,
        min_rps=3,
        max_rps=10,
        base_concurrency=2,
        min_concurrency=1,
        max_concurrency=6,
    ),

    # SELL: competitor/backbox endpoint tends to be stricter. Keep concurrency low.
    "backbox_competitors": EndpointConfig(
        # Many bot-protected endpoints behave better below 1 rps.
        # Start at 0.75 rps and let the learner explore upward cautiously.
        base_rps=1,
        min_rps=1,
        max_rps=7,
        base_concurrency=1,
        min_concurrency=1,
        max_concurrency=18,
    ),

    # TRADE-IN
    "tradein_listings": EndpointConfig(
        base_rps=5,
        min_rps=1,
        max_rps=15,
        base_concurrency=2,
        min_concurrency=1,
        max_concurrency=6,
    ),
    # Update listings (set quantity, etc.)
    "tradein_update": EndpointConfig(
        base_rps=1,
        min_rps=1,
        max_rps=8,
        base_concurrency=1,
        min_concurrency=1,
        max_concurrency=16,
    ),
    # Fetch competitor prices for trade-in listings.
    "tradein_competitors": EndpointConfig(
        base_rps=1,
        min_rps=1,
        max_rps=8,
        base_concurrency=1,
        min_concurrency=1,
        max_concurrency=12,
    ),
}


ENDPOINT_ALLOWED_STATUSES: dict[str, set[int]] = {
    # Treat "no competitors / no entry" as a normal outcome for rate learning.
    "backbox_competitors": {404},
    "tradein_competitors": {404},
}


def endpoint_config(endpoint_key: str) -> EndpointConfig:
    """Return the EndpointConfig for the given endpoint key."""

    cfg = ENDPOINTS.get(endpoint_key)
    if cfg is None:
        # Safe defaults for unknown endpoints.
        return EndpointConfig(
            base_rps=5.0,
            min_rps=1.0,
            max_rps=15.0,
            base_concurrency=2,
            min_concurrency=1,
            max_concurrency=8,
        )
    return cfg
