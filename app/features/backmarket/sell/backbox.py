# app/features/backmarket/sell/backbox.py
"""
Backbox subsystem.

Purpose
-------
Fetch competitor / buybox signals from:
  GET /ws/backbox/v1/competitors/{listingId}

Persist results into pricing_groups.listings[*] so pricing/UI do not need to
re-call Back Market constantly.

Golden price rule (your requirement)
------------------------------------
- We treat price_to_win as the key pricing signal.
- We persist every observation into backbox_history (timestamped).
- We also maintain a "best so far" (lowest ever) price_to_win per listing child:
      listings[*].backbox_best_price_to_win
  This is updated ONLY when a new observed price_to_win is lower.

Storage
-------
- listings[*].backbox                 : latest snapshot (trimmed; no raw payload)
- listings[*].backbox_history (<= 50) : rolling observations
- listings[*].backbox_best_price_to_win: lowest observed price_to_win

Notes
-----
- Per-request detail is logged by transport/client.py when DEBUG is enabled.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne

from app.features.backmarket.transport.cache import get_bm_client_for_user
from app.features.backmarket.transport.exceptions import BMClientError


logger = logging.getLogger(__name__)

GROUPS_COL = "pricing_groups"
JOBS_COL = "backbox_jobs"

DEFAULT_MARKET = "GB"
DEFAULT_MAX_PARALLEL_CALLS = 4

ENDPOINT_KEY_BACKBOX_COMPETITORS = "backbox_competitors"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_amount(m: Any) -> Optional[float]:
    """
    Backbox money fields are typically:
      {"amount": "123.45", "currency": "GBP"}

    Sometimes numbers/strings directly.
    """
    if m is None:
        return None
    if isinstance(m, (int, float)):
        return float(m)
    if isinstance(m, str):
        try:
            return float(m.strip())
        except (TypeError, ValueError):
            return None
    if isinstance(m, dict):
        amt = m.get("amount")
        if amt is None:
            return None
        try:
            return float(str(amt))
        except (TypeError, ValueError):
            return None
    return None


def _parse_currency(m: Any) -> Optional[str]:
    if isinstance(m, dict):
        c = m.get("currency")
        return str(c) if c else None
    return None


def _child_listing_ref(child: Dict[str, Any]) -> Optional[str]:
    for k in ("bm_uuid", "listing_uuid", "uuid", "bm_listing_id", "listing_id", "bm_id"):
        v = child.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


async def fetch_backbox_for_listing(
    db: AsyncIOMotorDatabase,
    user_id: str,
    listing_ref: str,
    *,
    market: str = DEFAULT_MARKET,
) -> Optional[Dict[str, Any]]:
    """
    Fetch Backbox competitors for one listing.

    Returns:
      - None if endpoint returns 404 or no entry for requested market.
      - Snapshot dict otherwise.

    Snapshot includes parsed numeric fields (price_to_win, winner_price, etc.).
    """
    client = await get_bm_client_for_user(db, user_id)
    path = f"/ws/backbox/v1/competitors/{listing_ref}"

    resp = await client.request(
        "GET",
        path,
        endpoint_key=ENDPOINT_KEY_BACKBOX_COMPETITORS,
    )

    if resp.status_code == 404:
        logger.debug("[backbox] 404 user_id=%s listing_ref=%s", user_id, listing_ref)
        return None

    if resp.status_code != 200:
        raise BMClientError(
            f"Backbox status {resp.status_code} for listing {listing_ref}: {resp.text[:500]}"
        )

    try:
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        raise BMClientError(f"Invalid JSON from Backbox for listing {listing_ref}") from exc

    if not isinstance(data, list):
        raise BMClientError(f"Unexpected Backbox JSON type for listing {listing_ref}: {type(data)}")

    own_entry: Optional[Dict[str, Any]] = None

    # Prefer our own listing entry in the requested market
    for item in data:
        if not isinstance(item, dict):
            continue
        if str(item.get("listing_id")) == str(listing_ref) and item.get("market") == market:
            own_entry = item
            break

    # Fallback: any entry in that market
    if own_entry is None:
        for item in data:
            if isinstance(item, dict) and item.get("market") == market:
                own_entry = item
                break

    if own_entry is None:
        return None

    price_to_win = _parse_amount(own_entry.get("price_to_win"))
    currency = _parse_currency(own_entry.get("price_to_win")) or _parse_currency(own_entry.get("price")) or "GBP"

    return {
        "market": own_entry.get("market"),
        "currency": currency,
        "is_winning": bool(own_entry.get("is_winning", False)),
        "price_to_win": price_to_win,
        "winner_price": _parse_amount(own_entry.get("winner_price")),
        "min_price": _parse_amount(own_entry.get("min_price")),
    }


async def persist_backbox_snapshot(
    *,
    db: AsyncIOMotorDatabase,
    user_id: str,
    listing_ref: str,
    backbox: Optional[Dict[str, Any]],
    fetched_at: datetime,
    market: str = DEFAULT_MARKET,
) -> Dict[str, Any]:
    """
    Persist latest backbox snapshot + append history into pricing_groups children.

    Also updates:
      listings[*].backbox_best_price_to_win = MIN(previous_best, new_price_to_win)

    Returns:
      {"listing_ref": ..., "pricing_groups_modified": int}
    """
    groups_col = db[GROUPS_COL]

    if backbox is None:
        snapshot: Dict[str, Any] = {"missing": True, "market": market, "fetched_at": fetched_at}
        history_entry: Dict[str, Any] = {"missing": True, "market": market, "fetched_at": fetched_at}
        price_to_win = None
    else:
        # Defensive: never persist raw payload blobs into pricing_groups.
        backbox_clean = {k: v for k, v in backbox.items() if k != "raw"}
        snapshot = {**backbox_clean, "missing": False, "fetched_at": fetched_at}
        price_to_win = backbox.get("price_to_win")
        history_entry = {
            "fetched_at": fetched_at,
            "market": backbox.get("market") or market,
            "missing": False,
            "price_to_win": price_to_win,
            "winner_price": backbox.get("winner_price"),
            "min_price": backbox.get("min_price"),
            "is_winning": backbox.get("is_winning", False),
        }

    update_doc: Dict[str, Any] = {
        "$set": {"listings.$[elem].backbox": snapshot},
        "$push": {"listings.$[elem].backbox_history": {"$each": [history_entry], "$slice": -50}},
    }

    if isinstance(price_to_win, (int, float)):
        update_doc["$min"] = {"listings.$[elem].backbox_best_price_to_win": float(price_to_win)}

    # Primary match: bm_uuid
    res1 = await groups_col.update_many(
        {"user_id": user_id, "listings.bm_uuid": listing_ref},
        update_doc,
        array_filters=[{"elem.bm_uuid": listing_ref}],
    )

    modified = int(res1.modified_count)

    # Fallback: bm_listing_id
    if modified == 0:
        res2 = await groups_col.update_many(
            {"user_id": user_id, "listings.bm_listing_id": listing_ref},
            update_doc,
            array_filters=[{"elem.bm_listing_id": listing_ref}],
        )
        modified = int(res2.modified_count)

    logger.debug(
        "[backbox] persist user_id=%s listing_ref=%s modified=%d price_to_win=%s",
        user_id,
        listing_ref,
        modified,
        price_to_win,
    )

    return {"listing_ref": listing_ref, "pricing_groups_modified": modified}


async def enqueue_backbox_jobs_for_user(db: AsyncIOMotorDatabase, user_id: str) -> Dict[str, Any]:
    """
    Reset (or create) backbox_jobs for all unique listing refs in pricing_groups.
    """
    groups_col = db[GROUPS_COL]
    jobs_col = db[JOBS_COL]
    now = _now()

    groups = await groups_col.find({"user_id": user_id}, {"_id": 0, "listings": 1}).to_list(length=None)

    refs: Set[str] = set()
    for g in groups:
        for child in (g.get("listings") or []):
            if not isinstance(child, dict):
                continue
            ref = _child_listing_ref(child)
            if ref:
                refs.add(ref)

    refs_list = sorted(list(refs))
    if not refs_list:
        return {"user_id": user_id, "total_unique_listings": 0, "jobs_created_or_reset": 0, "run_at": now.isoformat()}

    ops: List[UpdateOne] = []
    for ref in refs_list:
        ops.append(
            UpdateOne(
                {"user_id": user_id, "bm_uuid": ref},
                {
                    "$set": {
                        "user_id": user_id,
                        "bm_uuid": ref,
                        "status": "pending",
                        "attempts": 0,
                        "last_error": None,
                        "updated_at": now,
                    },
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
        )

    res = await jobs_col.bulk_write(ops, ordered=False)
    touched = int(res.matched_count) + int(res.upserted_count)

    logger.info(
        "[backbox_jobs] enqueue user_id=%s groups=%d unique_listings=%d touched=%d",
        user_id,
        len(groups),
        len(refs_list),
        touched,
    )

    return {"user_id": user_id, "total_unique_listings": len(refs_list), "jobs_created_or_reset": touched, "run_at": now.isoformat()}


async def process_backbox_jobs_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    max_attempts: int = 3,
    max_parallel_calls: int = DEFAULT_MAX_PARALLEL_CALLS,
    market: str = DEFAULT_MARKET,
) -> Dict[str, Any]:
    """
    Process pending backbox jobs (queue-based approach).

    NOTE: Uses persist_backbox_snapshot so the same "golden price" logic applies
    as the session-driven pricing cycle.
    """
    jobs_col = db[JOBS_COL]
    now = _now()

    jobs = await jobs_col.find(
        {"user_id": user_id, "status": "pending", "attempts": {"$lt": int(max_attempts)}}
    ).to_list(length=None)

    if not jobs:
        return {"user_id": user_id, "processed": 0, "success": 0, "failed": 0, "run_at": now.isoformat()}

    sem = asyncio.Semaphore(max(1, int(max_parallel_calls)))

    success = 0
    failed = 0

    async def _process_job(job: Dict[str, Any]) -> None:
        nonlocal success, failed

        job_id = job["_id"]
        ref = str(job.get("bm_uuid") or "").strip()

        if not ref:
            await jobs_col.update_one(
                {"_id": job_id},
                {"$set": {"status": "failed", "last_error": "missing bm_uuid", "updated_at": _now()}},
            )
            failed += 1
            return

        async with sem:
            try:
                bb = await fetch_backbox_for_listing(db, user_id, ref, market=market)
            except BMClientError as exc:
                new_attempts = int(job.get("attempts") or 0) + 1
                status = "failed" if new_attempts >= int(max_attempts) else "pending"
                await jobs_col.update_one(
                    {"_id": job_id},
                    {"$set": {"attempts": new_attempts, "status": status, "last_error": str(exc), "updated_at": _now()}},
                )
                if status == "failed":
                    failed += 1
                return

        fetched_at = _now()
        new_attempts = int(job.get("attempts") or 0) + 1

        # Persist snapshot/history (even if bb is None -> missing marker)
        await persist_backbox_snapshot(
            db=db,
            user_id=user_id,
            listing_ref=ref,
            backbox=bb,
            fetched_at=fetched_at,
            market=market,
        )

        await jobs_col.update_one(
            {"_id": job_id},
            {"$set": {"status": "success", "attempts": new_attempts, "last_error": None, "updated_at": fetched_at}},
        )
        success += 1

    await asyncio.gather(*[_process_job(j) for j in jobs])

    return {"user_id": user_id, "processed": len(jobs), "success": success, "failed": failed, "run_at": now.isoformat()}




