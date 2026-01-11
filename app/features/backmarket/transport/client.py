"""Async HTTP client for Back Market.

This module contains :class:`BackMarketClient`, a thin transport wrapper around
:class:`httpx.AsyncClient`.

Responsibilities
----------------
- Build Back Market auth headers for a user
- Apply per-endpoint rate limiting (via :class:`RateController`)
- Apply circuit breaking (per endpoint) to avoid thundering-herd failures
- Retry transient failures with backoff
- Log enough context to debug rate behaviour (RPS, limiter wait, cf-ray, ...)

Non-responsibilities
--------------------
- Business logic: interpreting specific API payloads
- Deciding how to handle non-2xx responses in domain code

The client returns an :class:`httpx.Response` whenever a response was received,
even for 4xx/5xx. This keeps error handling explicit at call sites.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import ssl
import time
import uuid
from typing import Any, Dict, Optional

import anyio
import httpx
from pymongo.errors import PyMongoError

from app.features.backmarket.rate.circuit_breaker import CircuitBreaker
from app.features.backmarket.rate.controller import RateController
from app.features.backmarket.transport.backoff import backoff_seconds
from app.features.backmarket.transport.endpoints import ENDPOINT_ALLOWED_STATUSES, endpoint_config
from app.features.backmarket.transport.exceptions import BMMaxRetriesError, BMRateLimited
from app.features.backmarket.transport.headers import BackMarketHeadersBuilder
from app.features.backmarket.transport.http_utils import (
    is_cloudflare_503,
    parse_retry_after_seconds,
    read_response_snippet,
    redact_headers,
)

logger = logging.getLogger(__name__)


_RE_BREAKER_SECONDS = re.compile(r"(?P<secs>\d+(?:\.\d+)?)s")


def _breaker_wait_seconds(exc: Exception) -> Optional[float]:
    """Best-effort extraction of breaker open duration from an exception."""

    retry_after = getattr(exc, "retry_after", None)
    if isinstance(retry_after, (int, float)):
        return max(0.0, float(retry_after))

    m = _RE_BREAKER_SECONDS.search(str(exc))
    if not m:
        return None

    try:
        return max(0.0, float(m.group("secs")))
    except ValueError:
        return None


class BackMarketClient:
    """Back Market HTTP client bound to a specific user."""

    def __init__(
        self,
        *,
        user_id: str,
        db: Any,
        base_url: Optional[str] = None,
        proxy_url: Optional[str] = None,
        timeout_seconds: float = 30.0,
        max_attempts: int = 5,
        refresh_rates_seconds: float = 60.0,
    ):
        self.user_id = user_id
        self._db = db

        # Prefer explicit base_url, else env var, else raise.
        # Prefer explicit base_url, else env var, else sane default.
        resolved_base_url = (
                (base_url or "").strip()
                or (os.environ.get("BM_API_BASE_URL") or "").strip()
                or (os.environ.get("BACKMARKET_BASE_URL") or "").strip()
                or (os.environ.get("BACKMARKET_API_BASE_URL") or "").strip()
                or "https://www.backmarket.co.uk"
        )

        self._base_url = resolved_base_url.rstrip("/")
        self._timeout_seconds = float(timeout_seconds)
        self._max_attempts = max(1, int(max_attempts))

        # Headers builder (with small TTL cache).
        self._headers = BackMarketHeadersBuilder(db)

        # Per-user rate controller (per endpoint limiters + Mongo persistence).
        self._rates = RateController(user_id=user_id, db=db, refresh_interval_seconds=refresh_rates_seconds)

        # Per-endpoint circuit breakers.
        self._breakers: Dict[str, CircuitBreaker] = {}

        client_kwargs: Dict[str, Any] = {"base_url": self._base_url, "timeout": self._timeout_seconds,
                                         "limits": httpx.Limits(max_keepalive_connections=0)}

        # Disable keepalive connections (helps with rotating proxies).

        if proxy_url:
            # httpx has changed proxy kwargs across versions.
            proxies = {
                "http://": proxy_url,
                "https://": proxy_url,
            }
            client_kwargs["proxies"] = proxies

        try:
            self._client = httpx.AsyncClient(**client_kwargs)
        except TypeError as exc:
            # Fallback for newer httpx versions using `proxy=`.
            if not proxy_url:
                raise

            msg = str(exc)
            if "proxies" not in msg and "unexpected keyword argument" not in msg:
                raise

            client_kwargs.pop("proxies", None)
            client_kwargs["proxy"] = proxy_url
            self._client = httpx.AsyncClient(**client_kwargs)

    def _breaker_for(self, endpoint_key: str) -> CircuitBreaker:
        breaker = self._breakers.get(endpoint_key)
        if breaker is None:
            breaker = CircuitBreaker(name=f"bm:{endpoint_key}")
            self._breakers[endpoint_key] = breaker
        return breaker

    async def aclose(self) -> None:
        await self._client.aclose()

    async def close(self) -> None:
        """Compatibility alias for older call sites."""
        await self.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        endpoint_key: str,
        json_body: Any = None,
        params: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[float] = None,
        max_attempts: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        raise_for_status: bool = False,
    ) -> httpx.Response:
        """Send a request with rate limiting, retries, and circuit breaking."""

        # Ensure the endpoint exists (also prevents typos silently using defaults).
        endpoint_config(endpoint_key)

        attempts_total = max(1, int(max_attempts or self._max_attempts))
        req_id = uuid.uuid4().hex

        # ✅ Guard: rate controller prepare must never hang forever
        try:
            state, limiter = await asyncio.wait_for(
                self._rates.prepare(endpoint_key),
                timeout=15.0,
            )
        except asyncio.TimeoutError as exc:
            logger.error(
                "[bm_request] RATE_PREPARE_TIMEOUT req_id=%s user_id=%s endpoint=%s method=%s path=%s timeout=15s",
                req_id,
                self.user_id,
                endpoint_key,
                method,
                path,
            )
            raise BMMaxRetriesError(
                f"Back Market request aborted: rate.prepare timeout (endpoint={endpoint_key})"
            ) from exc

        async def _safe_persist(where: str) -> None:
            try:
                await self._rates.persist(state)
            except PyMongoError as persist_exc:
                logger.warning(
                    "[bm_request] RATE_PERSIST_FAILED req_id=%s user_id=%s endpoint=%s where=%s err=%r",
                    req_id,
                    self.user_id,
                    endpoint_key,
                    where,
                    persist_exc,
                )

        breaker = self._breaker_for(endpoint_key)

        http_attempt = 0
        last_exc: Optional[Exception] = None

        while http_attempt < attempts_total:
            # Circuit breaker gate: if open, sleep and retry *without consuming an HTTP attempt*.
            try:
                await asyncio.wait_for(breaker.before_call(), timeout=10.0)
            except asyncio.TimeoutError as exc:
                last_exc = exc
                wait_s = backoff_seconds(max(1, http_attempt + 1))
                logger.error(
                    "[bm_request] BREAKER_BEFORE_CALL_TIMEOUT req_id=%s user_id=%s endpoint=%s method=%s path=%s "
                    "attempt=%d/%d wait=%.3fs",
                    req_id,
                    self.user_id,
                    endpoint_key,
                    method,
                    path,
                    http_attempt + 1,
                    attempts_total,
                    wait_s,
                )
                await asyncio.sleep(wait_s)
                continue

            except BMRateLimited as exc:
                wait_s = _breaker_wait_seconds(exc)
                if wait_s is None:
                    # Use backoff based on the *next* attempt number for stability.
                    wait_s = backoff_seconds(max(1, http_attempt + 1))

                logger.warning(
                    "[bm_request] BREAKER_OPEN req_id=%s user_id=%s endpoint=%s method=%s path=%s "
                    "attempt=%d/%d wait=%.3fs err=%r",
                    req_id,
                    self.user_id,
                    endpoint_key,
                    method,
                    path,
                    http_attempt + 1,
                    attempts_total,
                    wait_s,
                    exc,
                )
                await asyncio.sleep(wait_s)
                continue

            # Rate limiter gate.
            try:
                limiter_wait = await asyncio.wait_for(limiter.acquire(), timeout=60.0)
            except asyncio.TimeoutError:
                # ✅ Avoid silent freeze if limiter is wedged or rps is effectively 0.
                logger.error(
                    "[bm_request] LIMITER_ACQUIRE_TIMEOUT req_id=%s user_id=%s endpoint=%s method=%s path=%s "
                    "attempt=%d/%d -> bypassing limiter",
                    req_id,
                    self.user_id,
                    endpoint_key,
                    method,
                    path,
                    http_attempt + 1,
                    attempts_total,
                )
                limiter_wait = 0.0

            http_attempt += 1

            # Build headers.
            auth_headers = await self._headers.build_headers(self.user_id, endpoint_key)
            headers: Dict[str, str] = dict(auth_headers)
            if extra_headers:
                headers.update(extra_headers)

            timeout = float(timeout_seconds or self._timeout_seconds)

            t0 = time.monotonic()
            try:
                resp = await self._client.request(
                    method=method,
                    url=path,
                    json=json_body,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                )
            except (httpx.TransportError, ssl.SSLError, anyio.ClosedResourceError) as exc:
                # No response. Treat as retryable.
                last_exc = exc

                await breaker.after_failure()

                # Track as server_error with status_code 0.
                async with self._rates.endpoint_lock(endpoint_key):
                    self._rates.on_server_error(state, 0)
                    await _safe_persist("transport_error")

                wait_s = backoff_seconds(http_attempt)
                logger.warning(
                    "[bm_request] TRANSPORT_ERROR req_id=%s user_id=%s endpoint=%s method=%s path=%s "
                    "attempt=%d/%d rps=%s locked=%s limiter_wait=%.3fs wait=%.3fs err=%r headers=%s",
                    req_id,
                    self.user_id,
                    endpoint_key,
                    method,
                    path,
                    http_attempt,
                    attempts_total,
                    state.current_rps,
                    state.locked_rps,
                    limiter_wait,
                    wait_s,
                    exc,
                    redact_headers(headers),
                )
                await asyncio.sleep(wait_s)
                continue

            elapsed = time.monotonic() - t0

            status = resp.status_code
            cf_ray = resp.headers.get("cf-ray") or resp.headers.get("CF-Ray")

            allowed = ENDPOINT_ALLOWED_STATUSES.get(endpoint_key)
            is_success = (allowed is not None and status in allowed) or (200 <= status < 300)

            if is_success:
                async with self._rates.endpoint_lock(endpoint_key):
                    self._rates.on_success(state, status)
                    await _safe_persist("success")

                await breaker.after_success()

                logger.info(
                    "[bm_request] OK req_id=%s user_id=%s endpoint=%s method=%s path=%s status=%s "
                    "attempt=%d/%d rps=%s locked=%s limiter_wait=%.3fs elapsed=%.3fs cf_ray=%s",
                    req_id,
                    self.user_id,
                    endpoint_key,
                    method,
                    path,
                    status,
                    http_attempt,
                    attempts_total,
                    state.current_rps,
                    state.locked_rps,
                    limiter_wait,
                    elapsed,
                    cf_ray,
                )
                return resp

            # Rate-limited: only 429 or Cloudflare 503.
            if status == 429 or is_cloudflare_503(resp):
                async with self._rates.endpoint_lock(endpoint_key):
                    self._rates.on_rate_limited(state, status)
                    await _safe_persist("rate_limited")

                # Do NOT count rate limiting as a circuit breaker failure.
                await breaker.after_success()

                # Sleep: prefer Retry-After, else exponential backoff.
                wait_s = parse_retry_after_seconds(resp.headers) or backoff_seconds(http_attempt)

                snippet = read_response_snippet(resp)
                logger.warning(
                    "[bm_request] RATE_LIMITED req_id=%s user_id=%s endpoint=%s method=%s path=%s status=%s "
                    "attempt=%d/%d rps=%s locked=%s limiter_wait=%.3fs elapsed=%.3fs wait=%.3fs cf_ray=%s "
                    "body=%s",
                    req_id,
                    self.user_id,
                    endpoint_key,
                    method,
                    path,
                    status,
                    http_attempt,
                    attempts_total,
                    state.current_rps,
                    state.locked_rps,
                    limiter_wait,
                    elapsed,
                    wait_s,
                    cf_ray,
                    snippet,
                )

                await asyncio.sleep(wait_s)
                continue

            # Server error: retryable (but should not affect the rate learner policy).
            if 500 <= status < 600:
                async with self._rates.endpoint_lock(endpoint_key):
                    self._rates.on_server_error(state, status)
                    await _safe_persist("server_error")

                await breaker.after_failure()

                wait_s = backoff_seconds(http_attempt)
                snippet = read_response_snippet(resp)

                logger.warning(
                    "[bm_request] SERVER_ERROR req_id=%s user_id=%s endpoint=%s method=%s path=%s status=%s "
                    "attempt=%d/%d rps=%s locked=%s limiter_wait=%.3fs elapsed=%.3fs wait=%.3fs cf_ray=%s "
                    "body=%s",
                    req_id,
                    self.user_id,
                    endpoint_key,
                    method,
                    path,
                    status,
                    http_attempt,
                    attempts_total,
                    state.current_rps,
                    state.locked_rps,
                    limiter_wait,
                    elapsed,
                    wait_s,
                    cf_ray,
                    snippet,
                )

                await asyncio.sleep(wait_s)
                continue

            # Client error: usually not retryable. Track it, but don't open the breaker.
            async with self._rates.endpoint_lock(endpoint_key):
                self._rates.on_client_error(state, status)
                await _safe_persist("client_error")

            await breaker.after_success()

            snippet = read_response_snippet(resp)
            logger.warning(
                "[bm_request] CLIENT_ERROR req_id=%s user_id=%s endpoint=%s method=%s path=%s status=%s "
                "attempt=%d/%d rps=%s locked=%s limiter_wait=%.3fs elapsed=%.3fs cf_ray=%s body=%s",
                req_id,
                self.user_id,
                endpoint_key,
                method,
                path,
                status,
                http_attempt,
                attempts_total,
                state.current_rps,
                state.locked_rps,
                limiter_wait,
                elapsed,
                cf_ray,
                snippet,
            )

            # For most 4xx, return immediately (or raise if the caller asked).
            if raise_for_status:
                resp.raise_for_status()
            return resp

        # Exhausted attempts.
        if last_exc is not None:
            raise BMMaxRetriesError(
                f"Back Market request failed after {attempts_total} attempts: {method} {path} ({endpoint_key})"
            ) from last_exc

        raise BMMaxRetriesError(
            f"Back Market request failed after {attempts_total} attempts: {method} {path} ({endpoint_key})"
        )

    async def post_json(
        self,
        *,
        endpoint_key: str,
        path: str,
        payload: Any,
        params: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[float] = None,
    ) -> httpx.Response:
        return await self.request(
            endpoint_key=endpoint_key,
            method="POST",
            path=path,
            json_body=payload,
            params=params,
            timeout_seconds=timeout_seconds,
        )

    async def get(
        self,
        *,
        endpoint_key: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[float] = None,
    ) -> httpx.Response:
        return await self.request(
            endpoint_key=endpoint_key,
            method="GET",
            path=path,
            params=params,
            timeout_seconds=timeout_seconds,
        )
