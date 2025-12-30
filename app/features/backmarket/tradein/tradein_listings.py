"""
Fetch + sync BackMarket trade-in (buyback) listings.

Endpoint
--------
/ws/buyback/v1/listings

Pagination model
----------------
Cursor-based pagination:
- You send: pageSize=...
- Response contains: results + next (URL with ?cursor=...)

Why this module exists
----------------------
This is the trade-in equivalent of sell listings sync:
- proves BackMarketClient works for buyback endpoints
- persists into Mongo, keyed by user_id so each user stays isolated
- provides the raw inputs needed for later grouping/pricing
"""

from __future__ import annotations

import logging
import time
import urllib.parse as u
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.transport.cache import get_bm_client_for_user
from app.features.backmarket.transport.exceptions import BMClientError
from app.features.backmarket.tradein.repo import TradeinListingsRepo

logger = logging.getLogger(__name__)

ENDPOINT_KEY = "tradein_listings"
PATH = "/ws/buyback/v1/listings"


@dataclass(frozen=True)
class TradeinFetchResult:
    listings: List[Dict[str, Any]]
    pages_fetched: int
    elapsed_seconds: float


async def fetch_all_tradein_listings(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    page_size: int = 100,
    max_pages: int = 500,
    max_results: Optional[int] = None,
) -> TradeinFetchResult:
    """
    Fetch all trade-in listings for a user.

    Safety guards:
    - max_pages prevents infinite loops if BM returns a broken `next`
    - max_results allows limiting output for Swagger/testing
    """
    client = await get_bm_client_for_user(db, user_id)

    params: Dict[str, Any] = {"pageSize": int(page_size)}
    all_results: List[Dict[str, Any]] = []
    pages_fetched = 0

    overall_start = time.perf_counter()
    logger.info("[tradein_listings] start user_id=%s page_size=%s", user_id, page_size)

    while True:
        if pages_fetched >= int(max_pages):
            raise BMClientError(
                f"Trade-in pagination exceeded max_pages={max_pages} "
                f"cursor={params.get('cursor')!r} total={len(all_results)}"
            )

        page_start = time.perf_counter()
        cursor = params.get("cursor")
        logger.info("[tradein_listings] requesting cursor=%r", cursor)

        resp = await client.request(
            "GET",
            PATH,
            endpoint_key=ENDPOINT_KEY,
            params=params,
        )
        pages_fetched += 1

        if resp.status_code != 200:
            body_prefix = (resp.text or "")[:500]
            cf_ray = resp.headers.get("cf-ray") or resp.headers.get("CF-Ray")
            extra = f" cf-ray={cf_ray}" if cf_ray else ""
            raise BMClientError(f"Unexpected trade-in status {resp.status_code}:{extra} {body_prefix}")

        try:
            data = resp.json()
        except ValueError as exc:
            body_prefix = (resp.text or "")[:500]
            raise BMClientError(f"Trade-in returned non-JSON body: {body_prefix}") from exc

        results = data.get("results") or []
        if not isinstance(results, list):
            raise BMClientError("Trade-in response invalid: 'results' is not a list")

        all_results.extend(results)

        elapsed = time.perf_counter() - page_start
        logger.info(
            "[tradein_listings] page=%s results=%s total=%s elapsed=%.3fs",
            pages_fetched,
            len(results),
            len(all_results),
            elapsed,
        )

        if max_results is not None and len(all_results) >= int(max_results):
            logger.info(
                "[tradein_listings] early stop max_results=%s reached total=%s",
                max_results,
                len(all_results),
            )
            break

        next_url = data.get("next")
        if not next_url:
            break

        parsed = u.urlparse(next_url)
        query = u.parse_qs(parsed.query)
        next_cursor = (query.get("cursor") or [None])[0]

        if not next_cursor:
            logger.warning("[tradein_listings] next url missing cursor, stopping next_url=%r", next_url)
            break

        params["cursor"] = next_cursor

    total_elapsed = time.perf_counter() - overall_start
    logger.info(
        "[tradein_listings] done user_id=%s pages=%s total=%s elapsed=%.3fs",
        user_id,
        pages_fetched,
        len(all_results),
        total_elapsed,
    )

    return TradeinFetchResult(
        listings=all_results,
        pages_fetched=pages_fetched,
        elapsed_seconds=total_elapsed,
    )


async def sync_tradein_listings_to_db(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    page_size: int = 100,
    max_pages: int = 500,
    max_results: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Fetch + upsert all trade-in listings into Mongo.

    Output is intentionally small so Swagger doesn't explode.
    """
    fetch_res = await fetch_all_tradein_listings(
        db,
        user_id,
        page_size=page_size,
        max_pages=max_pages,
        max_results=max_results,
    )

    repo = TradeinListingsRepo(db)
    save_res = await repo.upsert_many(user_id=user_id, listings=fetch_res.listings)
    stored = await repo.count_for_user(user_id)

    return {
        "user_id": user_id,
        "endpoint_key": ENDPOINT_KEY,
        "fetched_count": len(fetch_res.listings),
        "pages_fetched": fetch_res.pages_fetched,
        "fetch_elapsed_seconds": fetch_res.elapsed_seconds,
        "save": {
            "attempted": save_res.attempted,
            "skipped_missing_id": save_res.skipped_missing_id,
            "upserted": save_res.upserted,
            "matched": save_res.matched,
            "modified": save_res.modified,
            "elapsed_seconds": save_res.elapsed_seconds,
            "stored_total_for_user": stored,
        },
    }



