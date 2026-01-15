# app/core/config.py
"""
This file holds *global* app settings (things that are not per-user).

Think of it like the "settings panel" for the backend:
- MongoDB connection details
- Proxy URL (if you use one)
- Back Market base URL and timeout
- Global circuit breaker defaults

IMPORTANT:
Some parts of the code (older versions) may expect:
    config.backmarket_base_url

Newer code expects:
    config.bm_api_base_url

To prevent crashes, we keep BOTH, and make backmarket_base_url an alias.
"""
from __future__ import annotations

import os
from dotenv import load_dotenv
from pydantic import BaseModel

# Loads environment variables from a local ".env" file (if present).
# This makes local development easier (you can keep secrets out of code).
load_dotenv()


class AppConfig(BaseModel):
    """
    AppConfig is a structured container for environment-based settings.

    If you set environment variables (Windows: setx / PowerShell: $env:..., Linux/macOS: export ...),
    those values override the defaults below.

    Notes for non-coders:
    - "base_url" just means "the website root address" we attach API paths to.
    - "timeout" means "how long we wait for Back Market before giving up".
    """

    # -----------------------------
    # General app settings
    # -----------------------------
    app_name: str = "BackMarket Pricer Backend"
    environment: str = os.getenv("APP_ENV", "dev")  # dev / staging / prod
    debug: bool = os.getenv("DEBUG", "true").lower() == "true"

    # -----------------------------
    # MongoDB settings
    # -----------------------------
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db: str = os.getenv("MONGO_DB", "test_pricer")

    # -----------------------------
    # Global proxy (same for all users)
    # -----------------------------
    # If you have a proxy service, set:
    #   PROXY_URL=http://user:pass@host:port
    proxy_url: str | None = os.getenv("RES_PROXY_URL")

    # -----------------------------
    # Back Market API settings
    # -----------------------------
    # We support *both* env var names just in case older deployments used BACKMARKET_BASE_URL.
    # Priority:
    #   BM_API_BASE_URL first, else BACKMARKET_BASE_URL, else default.
    bm_api_base_url: str = (
        os.getenv("BM_API_BASE_URL")
        or os.getenv("BACKMARKET_BASE_URL")
        or "https://www.backmarket.co.uk"
    )

    # How long we wait for a BM response (seconds)
    bm_timeout_seconds: float = float(os.getenv("BM_TIMEOUT_SECONDS", "15"))

    # -----------------------------
    # Global rate limiting defaults
    # -----------------------------
    # This is a conservative default. Your smarter "per-endpoint learner" logic
    # will tune per endpoint in bm_rate_state, but this provides safe fallbacks.
    bm_rate_max_per_second: int = int(os.getenv("BM_RATE_MAX_PER_SECOND", "5"))

    # -----------------------------
    # Circuit breaker defaults
    # -----------------------------
    # If BM is failing repeatedly, we "pause" briefly to avoid hammering them.
    bm_circuit_fail_threshold: int = int(os.getenv("BM_CIRCUIT_FAIL_THRESHOLD", "5"))
    bm_circuit_reset_seconds: int = int(os.getenv("BM_CIRCUIT_RESET_SECONDS", "60"))

    # -----------------------------
    # Backwards compatibility aliases
    # -----------------------------
    @property
    def backmarket_base_url(self) -> str:
        """
        Backwards-compatible name.

        Some older code refers to:
            config.backmarket_base_url

        Newer code refers to:
            config.bm_api_base_url

        They mean the same thing, so we map the old name to the new setting.
        """
        return self.bm_api_base_url


# Global singleton used throughout the app
config = AppConfig()