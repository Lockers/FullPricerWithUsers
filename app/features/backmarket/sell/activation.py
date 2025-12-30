# app/features/backmarket/sell/activation.py
"""
Activation subsystem (Sell-side pricing prerequisite).

This module creates an activation "session" and then performs activation/deactivation
in a safe, auditable way.

Safety rules (must never break)
-------------------------------
- Listings with original_quantity > 0 are considered REAL ACTIVE stock and are NEVER modified.
- Deactivation only targets listings that:
    - were originally inactive (original_quantity == 0)
    - AND were successfully activated in THIS session

Logging
-------
Very chatty at INFO for progress and timings.
Per-request detail is in transport/client.py (DEBUG) and update_listing_quantity (DEBUG).
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Set, Tuple, TypeVar

from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.sell.sell_listings import sync_sell_listings_for_user  # type: ignore
from app.features.backmarket.transport.cache import get_bm_client_for_user
from app.features.backmarket.transport.exceptions import BMClientError

logger = logging.getLogger(__name__)

SELL_COL = "bm_sell_listings"
GROUPS_COL = "pricing_groups"
SESSIONS_COL = "activation_sessions"

DEFAULT_MAX_PARALLEL_UPDATES = 16

# Must match endpoint config key (rate learner/limiter):
ENDPOINT_KEY_LISTING_UPDATE = "sell_listing_update"

UpdateMode = Literal["activate", "deactivate"]

T = TypeVar("T")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_cast(v: Any, cast: Callable[[Any], T]) -> Optional[T]:
    try:
        return cast(v)
    except (TypeError, ValueError):
        return None


def _as_int(v: Any, default: int = 0) -> int:
    out = _safe_cast(v, int)
    return out if out is not None else default


def _as_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    return _safe_cast(v, float)


def _progress_every(total: int) -> int:
    if total <= 50:
        return 5
    if total <= 500:
        return 25
    if total <= 5000:
        return 100
    return 250


def _parse_object_id(oid: str) -> ObjectId:
    try:
        return ObjectId(oid)
    except (InvalidId, TypeError) as exc:
        raise BMClientError(f"Invalid session_id ObjectId: {oid}") from exc


def _child_listing_ref(child: Dict[str, Any]) -> Optional[str]:
    """
    Return the identifier used for:
      - POST /ws/listings/{id}
      - GET  /ws/backbox/v1/competitors/{id}

    We prefer bm_uuid, but support fallbacks for older stored shapes.
    """
    for k in ("bm_uuid", "listing_uuid", "uuid", "bm_listing_id", "listing_id", "bm_id"):
        v = child.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _raw(doc: Dict[str, Any]) -> Dict[str, Any]:
    r = doc.get("raw")
    return r if isinstance(r, dict) else {}


def _sell_currency(doc: Dict[str, Any]) -> str:
    r = _raw(doc)
    return str(doc.get("currency") or r.get("currency") or "GBP")


def _sell_quantity(doc: Dict[str, Any]) -> int:
    r = _raw(doc)
    v = doc.get("quantity")
    if v is None:
        v = r.get("quantity")
    return _as_int(v, 0)


def _sell_max_price(doc: Dict[str, Any]) -> Optional[float]:
    r = _raw(doc)
    v = doc.get("max_price")
    if v is None:
        v = r.get("max_price") or r.get("maxPrice")
    return _as_float(v)


def _sell_bm_uuid(doc: Dict[str, Any]) -> Optional[str]:
    """
    Canonical-ish listing identifier from stored sell docs.

    Your storage should ideally normalize this to doc["bm_uuid"].
    """
    for k in ("bm_uuid", "uuid", "listing_id", "bm_listing_id"):
        v = doc.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()

    r = _raw(doc)
    for k in ("uuid", "id", "listingId", "listing_id"):
        v = r.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()

    return None


def _build_mixed_refs(refs: List[str]) -> List[Any]:
    """
    Some collections store IDs as ints, some as strings. We query with both when possible.
    """
    out: List[Any] = []
    for r in refs:
        out.append(r)
        if r.isdigit():
            out.append(_as_int(r))
    return out


async def _load_session_listings(
    db: AsyncIOMotorDatabase, user_id: str, session_id: str
) -> Tuple[Any, ObjectId, List[Dict[str, Any]]]:
    """
    Shared session loader for activate_session / deactivate_session.
    Returns (sessions_col, oid, listings).
    """
    sessions_col = db[SESSIONS_COL]
    oid = _parse_object_id(session_id)

    session = await sessions_col.find_one({"_id": oid})
    if not session:
        raise BMClientError(f"Activation session not found: {session_id}")
    if session.get("user_id") != user_id:
        raise BMClientError("Activation session user mismatch")

    listings = session.get("listings") or []
    if not isinstance(listings, list):
        listings = []

    return sessions_col, oid, listings


async def update_listing_quantity(
    *,
    db: AsyncIOMotorDatabase,
    user_id: str,
    listing_ref: str,
    quantity: int,
    max_price: float,
    currency: str,
) -> Tuple[bool, Optional[str]]:
    """
    Low-level listing update call.

    IMPORTANT:
    - Always sends min_price=None so we never leave a Backbox override behind.
    - Uses endpoint_key so limiter/learner apply.

    Returns:
      (ok, error_message)
    """
    t0 = time.perf_counter()

    client = await get_bm_client_for_user(db, user_id)
    payload = {
        "price": str(max_price),
        "currency": currency,
        "quantity": int(quantity),
        "min_price": None,
    }
    path = f"/ws/listings/{listing_ref}"

    logger.debug(
        "[listing_update] REQ user_id=%s ref=%s quantity=%s max_price=%s currency=%s path=%s payload=%s",
        user_id,
        listing_ref,
        quantity,
        max_price,
        currency,
        path,
        payload,
    )

    try:
        resp = await client.request(
            "POST",
            path,
            endpoint_key=ENDPOINT_KEY_LISTING_UPDATE,
            json_body=payload,
        )
    except BMClientError as exc:
        elapsed = time.perf_counter() - t0
        logger.warning(
            "[listing_update] EXC user_id=%s ref=%s quantity=%s elapsed=%.3fs err=%s",
            user_id,
            listing_ref,
            quantity,
            elapsed,
            exc,
        )
        return False, str(exc)

    elapsed = time.perf_counter() - t0

    if resp.status_code not in (200, 201, 202):
        body_prefix = (resp.text or "")[:300]
        logger.warning(
            "[listing_update] FAIL user_id=%s ref=%s quantity=%s status=%s elapsed=%.3fs body_prefix=%r",
            user_id,
            listing_ref,
            quantity,
            resp.status_code,
            elapsed,
            body_prefix,
        )
        return False, f"{resp.status_code}: {body_prefix}"

    logger.debug(
        "[listing_update] OK user_id=%s ref=%s quantity=%s status=%s elapsed=%.3fs",
        user_id,
        listing_ref,
        quantity,
        resp.status_code,
        elapsed,
    )
    return True, None


async def create_activation_session_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    wait_seconds: int = 180,
    do_sell_sync: bool = True,
    groups_filter: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Create an activation session document.

    groups_filter:
      Optional Mongo filter fragment applied to pricing_groups (in addition to user_id).
      This enables "price only selected models".
    """
    logger.info(
        "[activation] create_session START user_id=%s do_sell_sync=%s wait_seconds=%s groups_filter=%s",
        user_id,
        do_sell_sync,
        int(wait_seconds),
        groups_filter,
    )

    t_total = time.perf_counter()

    if do_sell_sync:
        t0 = time.perf_counter()
        res = await sync_sell_listings_for_user(db, user_id)
        logger.info(
            "[activation] sell_sync DONE user_id=%s elapsed=%.3fs summary=%s",
            user_id,
            time.perf_counter() - t0,
            {k: res.get(k) for k in ("fetched_count", "pages_fetched", "fetch_elapsed_seconds") if isinstance(res, dict)},
        )

    groups_col = db[GROUPS_COL]
    sell_col = db[SELL_COL]
    sessions_col = db[SESSIONS_COL]

    now = _now()

    base = {"user_id": user_id}
    if groups_filter:
        # If filter is an operator doc, AND it with base.
        if any(str(k).startswith("$") for k in groups_filter.keys()):
            query = {"$and": [base, groups_filter]}
        else:
            query = {**base, **groups_filter}
    else:
        query = base

    # 1) Load groups (only need child refs)
    t0 = time.perf_counter()
    groups = await groups_col.find(query, {"_id": 0, "listings": 1}).to_list(length=None)
    t_groups = time.perf_counter() - t0

    # 2) Collect unique listing refs
    listing_refs: Set[str] = set()
    child_snapshot_by_ref: Dict[str, Dict[str, Any]] = {}

    for g in groups:
        for child in (g.get("listings") or []):
            if not isinstance(child, dict):
                continue
            ref = _child_listing_ref(child)
            if not ref:
                continue
            listing_refs.add(ref)
            child_snapshot_by_ref.setdefault(ref, child)

    refs_list = sorted(list(listing_refs))

    logger.info(
        "[activation] groups_loaded user_id=%s groups=%d unique_listings=%d elapsed=%.3fs",
        user_id,
        len(groups),
        len(refs_list),
        t_groups,
    )

    # 3) Load sell docs for those refs
    t0 = time.perf_counter()
    sell_docs: List[Dict[str, Any]] = []

    if refs_list:
        mixed_refs = _build_mixed_refs(refs_list)
        sell_docs = await sell_col.find(
            {
                "user_id": user_id,
                "$or": [
                    {"bm_uuid": {"$in": mixed_refs}},
                    {"listing_id": {"$in": mixed_refs}},
                    {"bm_listing_id": {"$in": mixed_refs}},
                    {"uuid": {"$in": mixed_refs}},
                ],
            },
            {"_id": 0},
        ).to_list(length=None)

    t_sell = time.perf_counter() - t0
    logger.info(
        "[activation] sell_docs_loaded user_id=%s sell_docs=%d elapsed=%.3fs",
        user_id,
        len(sell_docs),
        t_sell,
    )

    # Index sell docs by any usable identifier for robust lookups
    sell_index: Dict[str, Dict[str, Any]] = {}
    for d in sell_docs:
        for k in ("bm_uuid", "uuid", "listing_id", "bm_listing_id"):
            v = d.get(k)
            if v is not None:
                sell_index[str(v)] = d
        rv = _sell_bm_uuid(d)
        if rv:
            sell_index.setdefault(rv, d)

    # 4) Build session entries
    session_listings: List[Dict[str, Any]] = []

    needs_activation = 0
    missing_sell = 0
    missing_price_data = 0

    for ref in refs_list:
        sdoc = sell_index.get(ref)
        fallback_child = child_snapshot_by_ref.get(ref, {})

        currency = "GBP"
        max_price: Optional[float] = None

        if sdoc is not None:
            original_quantity = _sell_quantity(sdoc)
            currency = _sell_currency(sdoc)
            max_price = _sell_max_price(sdoc)
        else:
            missing_sell += 1
            original_quantity = _as_int(fallback_child.get("quantity"), 0)
            currency = str(fallback_child.get("currency") or currency)
            max_price = _as_float(fallback_child.get("max_price")) or max_price

        if max_price is None:
            missing_price_data += 1

        entry: Dict[str, Any] = {
            "bm_uuid": ref,
            "original_quantity": int(original_quantity),
            "currency": currency,
            "max_price": max_price,
            # activation/deactivation flags
            "activated": False,
            "activation_attempts": 0,
            "activation_success": None,
            "deactivated": False,
            "deactivate_attempts": 0,
            "deactivate_success": None,
            # backbox bookkeeping (populated later)
            "backbox_attempts": 0,
            "backbox_success": None,
            "backbox_error": None,
            "backbox_fetched_at": None,
            "last_error": None,
        }

        if original_quantity == 0:
            needs_activation += 1

        session_listings.append(entry)

    session_doc: Dict[str, Any] = {
        "user_id": user_id,
        "status": "pending_activation",
        "started_at": now,
        "wait_seconds": int(wait_seconds),
        "groups_filter": groups_filter or None,
        "listings": session_listings,
        "created_at": now,
        "updated_at": now,
        "error": None,
    }

    t0 = time.perf_counter()
    res = await sessions_col.insert_one(session_doc)
    insert_elapsed = time.perf_counter() - t0
    session_id = str(res.inserted_id)

    logger.info(
        "[activation] session_created user_id=%s session_id=%s total=%d needs_activation=%d missing_sell=%d missing_price=%d "
        "insert_elapsed=%.3fs total_elapsed=%.3fs",
        user_id,
        session_id,
        len(session_listings),
        needs_activation,
        missing_sell,
        missing_price_data,
        insert_elapsed,
        time.perf_counter() - t_total,
    )

    return {
        "user_id": user_id,
        "session_id": session_id,
        "total_listings": len(session_listings),
        "needs_activation": needs_activation,
        "missing_sell_docs": missing_sell,
        "missing_price_data": missing_price_data,
        "wait_seconds": int(wait_seconds),
        "created_at": now.isoformat(),
    }


async def _apply_session_updates(
    *,
    db: AsyncIOMotorDatabase,
    user_id: str,
    session_id: str,
    listings: List[Dict[str, Any]],
    mode: UpdateMode,
    target_quantity: int,
    max_parallel_updates: int,
) -> Dict[str, Any]:
    """
    Shared implementation for activate_session / deactivate_session.

    mode="activate":
      - only touches entries where original_quantity == 0 and activated == False
      - sets activated + activation_success

    mode="deactivate":
      - only touches entries where original_quantity == 0 and activated == True and deactivated == False
      - sets deactivated + deactivate_success
    """
    sem = asyncio.Semaphore(max(1, int(max_parallel_updates)))

    # Field mapping
    if mode == "activate":
        flag_field = "activated"
        attempts_field = "activation_attempts"
        success_field = "activation_success"
    else:
        flag_field = "deactivated"
        attempts_field = "deactivate_attempts"
        success_field = "deactivate_success"

    attempted = 0
    ok_count = 0
    skipped = 0
    failed = 0

    skip_reasons: Dict[str, int] = {}

    async def _one(entry: Dict[str, Any]) -> Dict[str, int]:
        nonlocal attempted, ok_count, skipped, failed

        original_qty = _as_int(entry.get("original_quantity"), 0)

        # Never touch real stock listings.
        if original_qty > 0:
            reason = "originally_active"
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
            entry.setdefault("last_error", None)
            return {"attempted": 0, "ok": 0, "skipped": 1, "failed": 0}

        listing_ref = str(entry.get("bm_uuid") or "").strip()
        max_price = _as_float(entry.get("max_price"))
        currency = str(entry.get("currency") or "GBP")

        if not listing_ref or max_price is None:
            reason = "missing_bm_uuid_or_max_price"
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
            entry["last_error"] = reason
            return {"attempted": 0, "ok": 0, "skipped": 1, "failed": 0}

        if mode == "activate":
            if bool(entry.get("activated")):
                reason = "already_activated"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                return {"attempted": 0, "ok": 0, "skipped": 1, "failed": 0}
        else:
            if not bool(entry.get("activated")):
                reason = "not_activated"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                return {"attempted": 0, "ok": 0, "skipped": 1, "failed": 0}
            if bool(entry.get("deactivated")):
                reason = "already_deactivated"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                return {"attempted": 0, "ok": 0, "skipped": 1, "failed": 0}

        attempted += 1
        entry[attempts_field] = _as_int(entry.get(attempts_field), 0) + 1

        async with sem:
            ok, err = await update_listing_quantity(
                db=db,
                user_id=user_id,
                listing_ref=listing_ref,
                quantity=int(target_quantity),
                max_price=float(max_price),
                currency=currency,
            )

        if not ok:
            entry[success_field] = False
            entry["last_error"] = err
            failed += 1
            return {"attempted": 1, "ok": 0, "skipped": 0, "failed": 1}

        entry[flag_field] = True
        entry[success_field] = True
        entry["last_error"] = None
        ok_count += 1
        return {"attempted": 1, "ok": 1, "skipped": 0, "failed": 0}

    tasks = [asyncio.create_task(_one(entry)) for entry in listings]
    total = len(tasks)
    every = _progress_every(total)
    t0 = time.perf_counter()

    done = 0
    for fut in asyncio.as_completed(tasks):
        res = await fut
        done += 1
        skipped += res["skipped"]

        if done % every == 0 or done == total:
            logger.info(
                "[activation] %s progress user_id=%s session_id=%s done=%d/%d attempted=%d ok=%d failed=%d skipped=%d elapsed=%.3fs",
                mode,
                user_id,
                session_id,
                done,
                total,
                attempted,
                ok_count,
                failed,
                skipped,
                time.perf_counter() - t0,
            )

    return {
        "user_id": user_id,
        "session_id": session_id,
        "attempted": attempted,
        "ok": ok_count,
        "failed": failed,
        "skipped": skipped,
        "skip_reasons": skip_reasons,
        "elapsed_seconds": time.perf_counter() - t0,
    }


async def activate_session(
    db: AsyncIOMotorDatabase,
    user_id: str,
    session_id: str,
    *,
    max_parallel_updates: int = DEFAULT_MAX_PARALLEL_UPDATES,
) -> Dict[str, Any]:
    """
    Activate listings where original_quantity == 0.

    Activation = quantity=1 at max_price.
    """
    sessions_col, oid, listings = await _load_session_listings(db, user_id, session_id)

    if not listings:
        return {"user_id": user_id, "session_id": session_id, "attempted": 0, "activated": 0, "skipped": 0, "failed": 0}

    to_activate = [e for e in listings if _as_int(e.get("original_quantity"), 0) == 0 and not bool(e.get("activated"))]

    logger.info(
        "[activation] activate START user_id=%s session_id=%s total=%d to_activate=%d max_parallel=%d",
        user_id,
        session_id,
        len(listings),
        len(to_activate),
        int(max_parallel_updates),
    )

    now = _now()
    res = await _apply_session_updates(
        db=db,
        user_id=user_id,
        session_id=session_id,
        listings=listings,
        mode="activate",
        target_quantity=1,
        max_parallel_updates=max_parallel_updates,
    )

    await sessions_col.update_one({"_id": oid}, {"$set": {"status": "activated", "listings": listings, "updated_at": now}})

    logger.info(
        "[activation] activate DONE user_id=%s session_id=%s attempted=%d activated=%d failed=%d skipped=%d elapsed=%.3fs skip_reasons=%s",
        user_id,
        session_id,
        res["attempted"],
        res["ok"],
        res["failed"],
        res["skipped"],
        res["elapsed_seconds"],
        res["skip_reasons"],
    )

    return {
        "user_id": user_id,
        "session_id": session_id,
        "attempted": res["attempted"],
        "activated": res["ok"],
        "skipped": res["skipped"],
        "failed": res["failed"],
        "skip_reasons": res["skip_reasons"],
        "status": "activated",
        "updated_at": now.isoformat(),
    }


async def deactivate_session(
    db: AsyncIOMotorDatabase,
    user_id: str,
    session_id: str,
    *,
    max_parallel_updates: int = DEFAULT_MAX_PARALLEL_UPDATES,
) -> Dict[str, Any]:
    """
    Deactivate ONLY listings we activated in this session.

    Safe by design:
      - never touches original_quantity > 0 listings
      - never touches entries not activated by us
    """
    sessions_col, oid, listings = await _load_session_listings(db, user_id, session_id)

    if not listings:
        return {"user_id": user_id, "session_id": session_id, "attempted": 0, "deactivated": 0, "skipped": 0, "failed": 0}

    to_deactivate = [
        e
        for e in listings
        if _as_int(e.get("original_quantity"), 0) == 0 and bool(e.get("activated")) and not bool(e.get("deactivated"))
    ]

    logger.info(
        "[activation] deactivate START user_id=%s session_id=%s total=%d to_deactivate=%d max_parallel=%d",
        user_id,
        session_id,
        len(listings),
        len(to_deactivate),
        int(max_parallel_updates),
    )

    now = _now()
    res = await _apply_session_updates(
        db=db,
        user_id=user_id,
        session_id=session_id,
        listings=listings,
        mode="deactivate",
        target_quantity=0,
        max_parallel_updates=max_parallel_updates,
    )

    await sessions_col.update_one({"_id": oid}, {"$set": {"status": "deactivated", "listings": listings, "updated_at": now}})

    logger.info(
        "[activation] deactivate DONE user_id=%s session_id=%s attempted=%d deactivated=%d failed=%d skipped=%d elapsed=%.3fs skip_reasons=%s",
        user_id,
        session_id,
        res["attempted"],
        res["ok"],
        res["failed"],
        res["skipped"],
        res["elapsed_seconds"],
        res["skip_reasons"],
    )

    return {
        "user_id": user_id,
        "session_id": session_id,
        "attempted": res["attempted"],
        "deactivated": res["ok"],
        "skipped": res["skipped"],
        "failed": res["failed"],
        "skip_reasons": res["skip_reasons"],
        "status": "deactivated",
        "updated_at": now.isoformat(),
    }


async def deactivate_all_active_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    max_parallel_updates: int = DEFAULT_MAX_PARALLEL_UPDATES,
) -> Dict[str, Any]:
    """
    Emergency hard reset:
      - Sync sell listings
      - Set quantity=0 for ALL listings with quantity > 0

    This ignores activation sessions and pricing groups.
    Use with care: will switch off real stock listings too.
    """
    logger.warning("[deactivate_all] START user_id=%s", user_id)

    t0 = time.perf_counter()
    await sync_sell_listings_for_user(db, user_id)
    logger.info("[deactivate_all] sell_sync DONE user_id=%s elapsed=%.3fs", user_id, time.perf_counter() - t0)

    sell_col = db[SELL_COL]
    now = _now()

    docs = await sell_col.find({"user_id": user_id, "quantity": {"$gt": 0}}, {"_id": 0}).to_list(length=None)
    if not docs:
        all_docs = await sell_col.find({"user_id": user_id}, {"_id": 0}).to_list(length=None)
        docs = [d for d in all_docs if _sell_quantity(d) > 0]

    logger.warning(
        "[deactivate_all] user_id=%s active_listings=%d max_parallel=%d", user_id, len(docs), int(max_parallel_updates)
    )

    sem = asyncio.Semaphore(max(1, int(max_parallel_updates)))

    attempted = 0
    deactivated = 0
    skipped = 0
    failed = 0

    async def _deactivate_one(doc: Dict[str, Any]) -> None:
        nonlocal attempted, deactivated, skipped, failed

        listing_ref = _sell_bm_uuid(doc)
        if not listing_ref:
            skipped += 1
            return

        max_price = _sell_max_price(doc)
        currency = _sell_currency(doc)

        # Fallback to current price if max_price missing.
        if max_price is None:
            r = _raw(doc)
            max_price = _as_float(doc.get("price") or r.get("price"))

        if max_price is None:
            skipped += 1
            return

        attempted += 1
        async with sem:
            ok, _err = await update_listing_quantity(
                db=db,
                user_id=user_id,
                listing_ref=listing_ref,
                quantity=0,
                max_price=float(max_price),
                currency=currency,
            )

        if ok:
            deactivated += 1
        else:
            failed += 1

    await asyncio.gather(*[asyncio.create_task(_deactivate_one(d)) for d in docs])

    logger.warning(
        "[deactivate_all] DONE user_id=%s attempted=%d deactivated=%d skipped=%d failed=%d",
        user_id,
        attempted,
        deactivated,
        skipped,
        failed,
    )

    return {
        "user_id": user_id,
        "attempted": attempted,
        "deactivated": deactivated,
        "skipped": skipped,
        "failed": failed,
        "run_at": now.isoformat(),
    }





