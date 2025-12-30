# app/features/backmarket/sell/sell_listings.py
"""
Fetch all SELL listings from Back Market (/ws/listings) for one user.

Goals
-----
- Validate BackMarketClient.request() end-to-end
- rate limiter applied per endpoint_key
- learner state loaded/updated/persisted during pagination

Error handling
--------------
This module is "fetch only" but it DOES support a "partial" result:
- If a transient/network/proxy/max-retries issue happens mid-pagination, we stop
  and return what we have so far with partial=True and error metadata.
- We do NOT swallow programmer errors (we do not catch bare Exception).

Logging
-------
- INFO: page-by-page progress + summary timings
- DEBUG: next-url parsing details and parameters
"""

from __future__ import annotations

import logging
import time
import urllib.parse as u
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.sell.listings_repo import SellListingsRepo
from app.features.backmarket.transport.cache import get_bm_client_for_user
from app.features.backmarket.transport.exceptions import BMClientError, BMMaxRetriesError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SellListingsFetchResult:
    listings: List[Dict[str, Any]]
    pages_fetched: int
    elapsed_seconds: float
    partial: bool
    failed_page: Optional[int]
    error: Optional[str]
    last_status: Optional[int]


async def fetch_all_sell_listings(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    page_size: int = 50,
    min_quantity: Optional[int] = None,
    max_quantity: Optional[int] = None,
    publication_state: Optional[int] = None,
    max_pages: int = 500,
    max_results: Optional[int] = None,
) -> SellListingsFetchResult:
    client = await get_bm_client_for_user(db, user_id)

    path = "/ws/listings"
    params: Dict[str, Any] = {"page": 1, "page-size": int(page_size)}

    if min_quantity is not None:
        params["min_quantity"] = int(min_quantity)
    if max_quantity is not None:
        params["max_quantity"] = int(max_quantity)
    if publication_state is not None:
        params["publication_state"] = int(publication_state)

    all_results: List[Dict[str, Any]] = []
    pages_fetched = 0

    partial = False
    failed_page: Optional[int] = None
    error: Optional[str] = None
    last_status: Optional[int] = None

    overall_start = time.perf_counter()

    logger.info(
        "[sell_listings] START user_id=%s page_size=%s filters={min_quantity:%s,max_quantity:%s,publication_state:%s} "
        "max_pages=%s max_results=%s",
        user_id,
        page_size,
        min_quantity,
        max_quantity,
        publication_state,
        max_pages,
        max_results,
    )

    while True:
        if pages_fetched >= int(max_pages):
            partial = True
            failed_page = int(params.get("page") or 0)
            error = (
                f"pagination_exceeded_max_pages max_pages={max_pages} "
                f"last_page={params.get('page')} total={len(all_results)}"
            )
            logger.warning(
                "[sell_listings] PARTIAL_STOP user_id=%s page=%s reason=%s",
                user_id,
                failed_page,
                error,
            )
            break

        page = int(params["page"])
        page_start = time.perf_counter()

        logger.info("[sell_listings] REQUEST user_id=%s page=%s params=%s", user_id, page, params)

        try:
            resp = await client.request(
                "GET",
                path,
                endpoint_key="sell_listings_get",
                params=params,
            )
        except (BMMaxRetriesError, BMClientError, httpx.HTTPError) as exc:
            # Transient/transport-ish failures should not crash the entire pricing cycle.
            partial = True
            failed_page = page
            error = f"{exc.__class__.__name__}: {exc}"
            logger.warning(
                "[sell_listings] PARTIAL_STOP user_id=%s page=%s err=%r total_so_far=%s",
                user_id,
                page,
                exc,
                len(all_results),
            )
            break

        pages_fetched += 1
        req_elapsed = time.perf_counter() - page_start
        last_status = int(resp.status_code)

        if resp.status_code != 200:
            body_prefix = (resp.text or "")[:500]
            logger.error(
                "[sell_listings] ERROR user_id=%s page=%s status=%s elapsed=%.3fs body_prefix=%r",
                user_id,
                page,
                resp.status_code,
                req_elapsed,
                body_prefix,
            )
            # Partial stop: we keep what we already fetched
            partial = True
            failed_page = page
            error = f"BM status {resp.status_code}: {body_prefix}"
            break

        try:
            data = resp.json()
        except (ValueError, TypeError) as exc:
            body_prefix = (resp.text or "")[:500]
            logger.error(
                "[sell_listings] ERROR user_id=%s page=%s elapsed=%.3fs json_decode_failed body_prefix=%r",
                user_id,
                page,
                req_elapsed,
                body_prefix,
            )
            partial = True
            failed_page = page
            error = f"json_decode_failed: {exc.__class__.__name__}: {exc}"
            break

        results = data.get("results") or []
        if not isinstance(results, list):
            logger.error(
                "[sell_listings] ERROR user_id=%s page=%s invalid_results_type=%s",
                user_id,
                page,
                type(results),
            )
            partial = True
            failed_page = page
            error = f"invalid_results_type={type(results)}"
            break

        all_results.extend(results)

        logger.info(
            "[sell_listings] PAGE_DONE user_id=%s page=%s results=%s total=%s elapsed=%.3fs",
            user_id,
            page,
            len(results),
            len(all_results),
            req_elapsed,
        )

        if max_results is not None and len(all_results) >= int(max_results):
            logger.info(
                "[sell_listings] EARLY_STOP user_id=%s max_results=%s reached total=%s pages=%s",
                user_id,
                max_results,
                len(all_results),
                pages_fetched,
            )
            break

        next_url = data.get("next")
        if not next_url:
            logger.info("[sell_listings] END_PAGINATION user_id=%s page=%s next=null", user_id, page)
            break

        parsed = u.urlparse(str(next_url))
        query = u.parse_qs(parsed.query)
        next_page_raw = (query.get("page") or [None])[0]

        logger.debug("[sell_listings] next_url=%r parsed_query=%s next_page_raw=%r", next_url, query, next_page_raw)

        if not next_page_raw:
            # Not an error, but we can't continue safely.
            partial = True
            failed_page = page
            error = f"next_url_missing_page_param next_url={next_url!r}"
            logger.warning("[sell_listings] PARTIAL_STOP user_id=%s reason=%s", user_id, error)
            break

        try:
            params["page"] = int(str(next_page_raw))
        except (TypeError, ValueError) as exc:
            partial = True
            failed_page = page
            error = f"invalid_next_page_value value={next_page_raw!r} err={exc.__class__.__name__}: {exc}"
            logger.warning("[sell_listings] PARTIAL_STOP user_id=%s reason=%s", user_id, error)
            break

    total_elapsed = time.perf_counter() - overall_start
    logger.info(
        "[sell_listings] DONE user_id=%s pages=%s total=%s partial=%s failed_page=%s last_status=%s error=%r elapsed=%.3fs",
        user_id,
        pages_fetched,
        len(all_results),
        partial,
        failed_page,
        last_status,
        error,
        total_elapsed,
    )

    return SellListingsFetchResult(
        listings=all_results,
        pages_fetched=pages_fetched,
        elapsed_seconds=total_elapsed,
        partial=partial,
        failed_page=failed_page,
        error=error,
        last_status=last_status,
    )


async def sync_sell_listings_to_db(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    page_size: int = 50,
    min_quantity: Optional[int] = None,
    max_quantity: Optional[int] = None,
    publication_state: Optional[int] = None,
    max_pages: int = 500,
    max_results: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Fetch sell listings from BM and upsert into bm_sell_listings.

    If fetch is partial, we still upsert what we got, and return partial/error metadata
    so the caller (pricing_cycle) can decide whether to treat this as soft failure.
    """
    t_total = time.perf_counter()
    logger.info(
        "[sell_listings_sync] START user_id=%s page_size=%s max_pages=%s max_results=%s",
        user_id,
        page_size,
        max_pages,
        max_results,
    )

    t0 = time.perf_counter()
    fetch_result = await fetch_all_sell_listings(
        db,
        user_id,
        page_size=page_size,
        min_quantity=min_quantity,
        max_quantity=max_quantity,
        publication_state=publication_state,
        max_pages=max_pages,
        max_results=max_results,
    )
    fetch_elapsed = time.perf_counter() - t0

    logger.info(
        "[sell_listings_sync] FETCH_DONE user_id=%s fetched=%s pages=%s partial=%s failed_page=%s last_status=%s elapsed=%.3fs",
        user_id,
        len(fetch_result.listings),
        fetch_result.pages_fetched,
        fetch_result.partial,
        fetch_result.failed_page,
        fetch_result.last_status,
        fetch_elapsed,
    )

    repo = SellListingsRepo(db)

    t0 = time.perf_counter()
    save_result = await repo.upsert_many(user_id=user_id, listings=fetch_result.listings)
    save_elapsed = time.perf_counter() - t0

    stored_count = await repo.count_for_user(user_id)

    total_elapsed = time.perf_counter() - t_total
    logger.info(
        "[sell_listings_sync] DONE user_id=%s fetched=%s stored_total=%s partial=%s "
        "save={attempted:%s,skipped_missing_id:%s,upserted:%s,matched:%s,modified:%s,elapsed:%.3fs} total_elapsed=%.3fs",
        user_id,
        len(fetch_result.listings),
        stored_count,
        fetch_result.partial,
        save_result.attempted,
        save_result.skipped_missing_id,
        save_result.upserted,
        save_result.matched,
        save_result.modified,
        save_elapsed,
        total_elapsed,
    )

    return {
        "user_id": user_id,
        "fetched_count": len(fetch_result.listings),
        "pages_fetched": fetch_result.pages_fetched,
        "fetch_elapsed_seconds": fetch_result.elapsed_seconds,
        "partial": fetch_result.partial,
        "failed_page": fetch_result.failed_page,
        "error": fetch_result.error,
        "last_status": fetch_result.last_status,
        "save": {
            "attempted": save_result.attempted,
            "skipped_missing_id": save_result.skipped_missing_id,
            "upserted": save_result.upserted,
            "matched": save_result.matched,
            "modified": save_result.modified,
            "elapsed_seconds": save_result.elapsed_seconds,
            "stored_total_for_user": stored_count,
        },
    }


# Backwards compatible alias (your other modules import this name)
async def sync_sell_listings_for_user(db: AsyncIOMotorDatabase, user_id: str, **kwargs: Any) -> Dict[str, Any]:
    return await sync_sell_listings_to_db(db, user_id, **kwargs)




