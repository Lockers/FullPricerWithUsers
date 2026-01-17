# app/features/backmarket/sell/pricing_cycle.py
"""
Sell pricing cycle (Backbox pull) - SESSION-DRIVEN, PER-LISTING DEACTIVATION.

Required flow
-------------
  1) Sync sell listings (refresh quantity/max_price/currency in Mongo)
  2) Create activation session (snapshot original quantities + required metadata)
  3) Activate qty=0 listings (quantity=1) using max_price (safety)
  4) Wait N seconds (~3 minutes)
  5) For each listing in the session:
        - fetch Backbox competitors
        - persist snapshot + history into pricing_groups.listings[*]
        - immediately deactivate (quantity=0) IF AND ONLY IF we activated it in this run
  6) Recompute group-level sell anchors (lowest_gross_price_to_win -> net after fees)

Safety rules
------------
- Listings active before run (original_quantity > 0) are NEVER deactivated.
- Listings we failed to activate are NEVER deactivated.
- If per-listing deactivation fails, we run a final cleanup sweep (deactivate_session).

Rate limiting / learning
------------------------
Every Back Market call includes an endpoint_key so limiter + learner apply automatically.
Per-request logs (headers/body/status/retries) are produced by transport/client.py when DEBUG enabled.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError

from app.features.backmarket.sell.activation import (
    activate_session,
    create_activation_session_for_user,
    deactivate_session,
    update_listing_quantity,
)
from app.features.backmarket.sell.backbox import (
    fetch_backbox_for_listing,
    persist_backbox_snapshot,
)
from app.features.backmarket.sell.sell_anchor_service import recompute_sell_anchors_for_user
from app.features.backmarket.pricing.trade_pricing_service import recompute_trade_pricing_for_user
from app.features.backmarket.sell.sell_listings import sync_sell_listings_for_user
from app.features.backmarket.transport.exceptions import BMClientError
from app.features.orders.service import sync_bm_orders_for_user

logger = logging.getLogger(__name__)

DEFAULT_WAIT_SECONDS = 30
DEFAULT_MARKET = "GB"

DEFAULT_MAX_PARALLEL_BACKBOX = 5
DEFAULT_MAX_PARALLEL_DEACTIVATE = 10

SESSIONS_COL = "activation_sessions"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_object_id(session_id: str) -> ObjectId:
    try:
        return ObjectId(session_id)
    except (InvalidId, TypeError) as exc:
        raise BMClientError(f"Invalid session_id ObjectId: {session_id}") from exc


def _as_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _progress_every(total: int) -> int:
    if total <= 50:
        return 5
    if total <= 500:
        return 25
    if total <= 5000:
        return 100
    return 250


async def _backbox_then_deactivate_per_listing(
    db: AsyncIOMotorDatabase,
    user_id: str,
    session_id: str,
    *,
    market: str,
    max_backbox_attempts: int,
    max_parallel_backbox: int,
    max_parallel_deactivate: int,
) -> Dict[str, Any]:
    """
    Core step:
      for each listing:
        fetch backbox -> persist -> deactivate immediately (if needed)

    Updates activation_sessions.listings[] with backbox/deactivation bookkeeping.
    """
    sessions_col = db[SESSIONS_COL]
    oid = _parse_object_id(session_id)

    session = await sessions_col.find_one({"_id": oid})
    if not session:
        raise BMClientError(f"Activation session not found: {session_id}")
    if session.get("user_id") != user_id:
        raise BMClientError("Activation session user mismatch")

    listings: List[Dict[str, Any]] = session.get("listings") or []
    if not listings:
        return {
            "session_id": session_id,
            "total_listings": 0,
            "processed": 0,
            "skipped_inactive": 0,
            "backbox_success": 0,
            "backbox_failed": 0,
            "backbox_missing": 0,
            "deactivated": 0,
            "deactivate_failed": 0,
        }

    backbox_sem = asyncio.Semaphore(max(1, int(max_parallel_backbox)))
    deact_sem = asyncio.Semaphore(max(1, int(max_parallel_deactivate)))

    total = len(listings)
    every = _progress_every(total)
    logger.info(
        "[sell_pricing_cycle] backbox_then_deactivate START user_id=%s session_id=%s total=%d max_parallel_backbox=%d max_parallel_deactivate=%d",
        user_id,
        session_id,
        total,
        int(max_parallel_backbox),
        int(max_parallel_deactivate),
    )


    processed = 0
    skipped_inactive = 0
    backbox_success = 0
    backbox_failed = 0
    backbox_missing = 0
    deactivated = 0
    deactivate_failed = 0

    t_total = time.perf_counter()

    async def _process_entry(entry: Dict[str, Any]) -> Dict[str, int]:
        """
        Returns per-entry counters so caller can aggregate safely (no nonlocal races).
        """
        listing_ref = str(entry.get("bm_uuid") or "").strip()
        original_qty = _as_int(entry.get("original_quantity"), 0)
        was_activated = bool(entry.get("activated"))

        # Only fetch backbox if listing is active now:
        # - originally active OR activated by us
        should_fetch = (original_qty > 0) or was_activated

        if not listing_ref:
            entry["backbox_success"] = False
            entry["backbox_error"] = "missing bm_uuid"
            return {
                "processed": 1,
                "skipped": 0,
                "bb_ok": 0,
                "bb_fail": 1,
                "bb_missing": 0,
                "deact_ok": 0,
                "deact_fail": 0,
            }

        if not should_fetch:
            entry["backbox_success"] = False
            entry["backbox_error"] = "skipped_inactive_not_activated"
            return {
                "processed": 1,
                "skipped": 1,
                "bb_ok": 0,
                "bb_fail": 0,
                "bb_missing": 0,
                "deact_ok": 0,
                "deact_fail": 0,
            }

        fetched_at = _now()
        entry["backbox_fetched_at"] = fetched_at

        deactivate_needed = (original_qty == 0) and was_activated

        currency = str(entry.get("currency") or "GBP")
        max_price = _as_float(entry.get("max_price"))

        bb: Optional[Dict[str, Any]] = None
        fetch_error: Optional[str] = None

        # ---- Fetch Backbox (retry loop) ----
        for attempt in range(1, max(1, int(max_backbox_attempts)) + 1):
            entry["backbox_attempts"] = attempt
            try:
                async with backbox_sem:
                    bb = await fetch_backbox_for_listing(db, user_id, listing_ref, market=market)
                fetch_error = None
                break
            except BMClientError as exc:
                fetch_error = str(exc)
                if attempt >= int(max_backbox_attempts):
                    break
                await asyncio.sleep(0.75 * attempt)

        # ---- Persist snapshot/history (always attempt, even if bb=None) ----
        persist_error: Optional[str] = None
        try:
            await persist_backbox_snapshot(
                db=db,
                user_id=user_id,
                listing_ref=listing_ref,
                backbox=bb,
                fetched_at=fetched_at,
                market=market,
            )
        except PyMongoError as exc:
            # Persistence failure is serious, but we must still proceed to deactivation to stay safe.
            persist_error = str(exc)

        # ---- Session bookkeeping ----
        if fetch_error is None and persist_error is None:
            entry["backbox_success"] = True
            entry["backbox_error"] = None
            bb_ok = 1
            bb_fail = 0
        else:
            entry["backbox_success"] = False
            entry["backbox_error"] = fetch_error or f"persist_error: {persist_error}"
            bb_ok = 0
            bb_fail = 1

        bb_missing = 1 if (bb is None and fetch_error is None) else 0

        # ---- Immediate deactivation (only if we activated it) ----
        if not deactivate_needed:
            return {
                "processed": 1,
                "skipped": 0,
                "bb_ok": bb_ok,
                "bb_fail": bb_fail,
                "bb_missing": bb_missing,
                "deact_ok": 0,
                "deact_fail": 0,
            }

        if max_price is None:
            entry["deactivate_success"] = False
            entry["last_error"] = "cannot_deactivate_missing_max_price"
            return {
                "processed": 1,
                "skipped": 0,
                "bb_ok": bb_ok,
                "bb_fail": bb_fail,
                "bb_missing": bb_missing,
                "deact_ok": 0,
                "deact_fail": 1,
            }

        entry["deactivate_attempts"] = _as_int(entry.get("deactivate_attempts"), 0) + 1

        async with deact_sem:
            ok, err = await update_listing_quantity(
                db=db,
                user_id=user_id,
                listing_ref=listing_ref,
                quantity=0,
                max_price=float(max_price),
                currency=currency,
            )

        if not ok:
            entry["deactivate_success"] = False
            entry["last_error"] = err
            return {
                "processed": 1,
                "skipped": 0,
                "bb_ok": bb_ok,
                "bb_fail": bb_fail,
                "bb_missing": bb_missing,
                "deact_ok": 0,
                "deact_fail": 1,
            }

        entry["deactivated"] = True
        entry["deactivate_success"] = True
        entry["last_error"] = None
        return {
            "processed": 1,
            "skipped": 0,
            "bb_ok": bb_ok,
            "bb_fail": bb_fail,
            "bb_missing": bb_missing,
            "deact_ok": 1,
            "deact_fail": 0,
        }

    tasks = [asyncio.create_task(_process_entry(entry)) for entry in listings]

    done = 0
    for fut in asyncio.as_completed(tasks):
        r = await fut
        done += 1

        processed += r["processed"]
        skipped_inactive += r["skipped"]
        backbox_success += r["bb_ok"]
        backbox_failed += r["bb_fail"]
        backbox_missing += r["bb_missing"]
        deactivated += r["deact_ok"]
        deactivate_failed += r["deact_fail"]

        if done % every == 0 or done == total:
            logger.info(
                "[sell_pricing_cycle] progress user_id=%s session_id=%s done=%d/%d "
                "bb_ok=%d bb_fail=%d bb_missing=%d deact_ok=%d deact_fail=%d skipped_inactive=%d elapsed=%.3fs",
                user_id,
                session_id,
                done,
                total,
                backbox_success,
                backbox_failed,
                backbox_missing,
                deactivated,
                deactivate_failed,
                skipped_inactive,
                time.perf_counter() - t_total,
            )

    await sessions_col.update_one(
        {"_id": oid},
        {"$set": {"status": "backbox_completed", "listings": listings, "updated_at": _now()}},
    )

    logger.info(
        "[sell_pricing_cycle] backbox_then_deactivate DONE user_id=%s session_id=%s total=%d processed=%d "
        "bb_ok=%d bb_fail=%d bb_missing=%d deact_ok=%d deact_fail=%d skipped_inactive=%d elapsed=%.3fs",
        user_id,
        session_id,
        total,
        processed,
        backbox_success,
        backbox_failed,
        backbox_missing,
        deactivated,
        deactivate_failed,
        skipped_inactive,
        time.perf_counter() - t_total,
    )

    return {
        "session_id": session_id,
        "total_listings": total,
        "processed": processed,
        "skipped_inactive": skipped_inactive,
        "backbox_success": backbox_success,
        "backbox_failed": backbox_failed,
        "backbox_missing": backbox_missing,
        "deactivated": deactivated,
        "deactivate_failed": deactivate_failed,
    }


async def run_sell_pricing_cycle_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    wait_seconds: int = DEFAULT_WAIT_SECONDS,
    market: str = DEFAULT_MARKET,
    max_backbox_attempts: int = 3,
    max_parallel_backbox: int = DEFAULT_MAX_PARALLEL_BACKBOX,
    max_parallel_deactivate: int = DEFAULT_MAX_PARALLEL_DEACTIVATE,
    groups_filter: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    started_at_dt = _now()
    start_total = time.perf_counter()

    timings: Dict[str, float] = {}

    logger.info(
        "[sell_pricing_cycle] START user_id=%s wait_seconds=%s market=%s max_backbox_attempts=%s "
        "max_parallel_backbox=%s max_parallel_deactivate=%s groups_filter=%s",
        user_id,
        int(wait_seconds),
        market,
        int(max_backbox_attempts),
        int(max_parallel_backbox),
        int(max_parallel_deactivate),
        groups_filter,
    )

    # 1) Sync sell listings
    t0 = time.perf_counter()
    try:
        sell_sync_res = await sync_sell_listings_for_user(db, user_id)
    except Exception as exc:  # last-resort: never 500 the whole cycle for sell sync
        sell_sync_res = {"ok": False, "error": str(exc), "error_type": exc.__class__.__name__}
        logger.error("[sell_pricing_cycle] sell_sync ERROR user_id=%s err=%r", user_id, exc)
    timings["sell_sync"] = time.perf_counter() - t0
    logger.info("[sell_pricing_cycle] sell_sync DONE user_id=%s elapsed=%.3fs", user_id, timings["sell_sync"])

    # 2) Create activation session
    t0 = time.perf_counter()
    session_info = await create_activation_session_for_user(
        db,
        user_id,
        wait_seconds=wait_seconds,
        do_sell_sync=False,
        groups_filter=groups_filter,
    )
    timings["create_session"] = time.perf_counter() - t0
    session_id = str(session_info["session_id"])
    logger.info(
        "[sell_pricing_cycle] create_session DONE user_id=%s session_id=%s elapsed=%.3fs session_info=%s",
        user_id,
        session_id,
        timings["create_session"],
        session_info,
    )

    # 3) Activate
    t0 = time.perf_counter()
    activation_res = await activate_session(db, user_id, session_id)
    timings["activate"] = time.perf_counter() - t0
    logger.info(
        "[sell_pricing_cycle] activate DONE user_id=%s session_id=%s elapsed=%.3fs activation=%s",
        user_id,
        session_id,
        timings["activate"],
        {k: activation_res.get(k) for k in ("attempted", "activated", "failed", "skipped")},
    )

    # 4) Wait once (global) so Backbox can stabilize
    if wait_seconds > 0:
        logger.info("[sell_pricing_cycle] wait START user_id=%s seconds=%d", user_id, int(wait_seconds))
        t0 = time.perf_counter()
        await asyncio.sleep(int(wait_seconds))
        timings["wait"] = time.perf_counter() - t0
        logger.info("[sell_pricing_cycle] wait DONE user_id=%s elapsed=%.3fs", user_id, timings["wait"])
    else:
        timings["wait"] = 0.0

    # 5) Backbox per listing -> immediate deactivate
    t0 = time.perf_counter()
    cleanup_res: Optional[Dict[str, Any]] = None

    try:
        backbox_res = await _backbox_then_deactivate_per_listing(
            db,
            user_id,
            session_id,
            market=market,
            max_backbox_attempts=max(1, int(max_backbox_attempts)),
            max_parallel_backbox=max_parallel_backbox,
            max_parallel_deactivate=max_parallel_deactivate,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "[sell_pricing_cycle] ERROR during backbox/deactivate user_id=%s session_id=%s err=%r",
            user_id,
            session_id,
            exc,
        )
        # Safety fuse: deactivate any still-active session-activated entries
        try:
            cleanup_res = await deactivate_session(db, user_id, session_id)
            logger.warning(
                "[sell_pricing_cycle] cleanup_deactivate_session DONE user_id=%s session_id=%s cleanup=%s",
                user_id,
                session_id,
                {k: cleanup_res.get(k) for k in ("attempted", "deactivated", "failed", "skipped")},
            )
        except Exception as cleanup_exc:  # noqa: BLE001
            logger.error(
                "[sell_pricing_cycle] cleanup_deactivate_session ERROR user_id=%s session_id=%s err=%r",
                user_id,
                session_id,
                cleanup_exc,
            )
        raise
    finally:
        timings["backbox_then_deactivate"] = time.perf_counter() - t0

    # Optional cleanup sweep if any deactivations failed (rare)
    if int(backbox_res.get("deactivate_failed") or 0) > 0:
        logger.warning(
            "[sell_pricing_cycle] deactivation_failures user_id=%s session_id=%s count=%s -> running cleanup sweep",
            user_id,
            session_id,
            backbox_res.get("deactivate_failed"),
        )
        t0 = time.perf_counter()
        cleanup_res = await deactivate_session(db, user_id, session_id)
        timings["cleanup_deactivate_session"] = time.perf_counter() - t0
        logger.warning(
            "[sell_pricing_cycle] cleanup_deactivate_session DONE user_id=%s session_id=%s elapsed=%.3fs cleanup=%s",
            user_id,
            session_id,
            timings["cleanup_deactivate_session"],
            {k: cleanup_res.get(k) for k in ("attempted", "deactivated", "failed", "skipped")},
        )
    else:
        timings["cleanup_deactivate_session"] = 0.0

    # 6) Sync orders (incremental) + apply to pricing_groups
    #
    # IMPORTANT:
    # We do this BEFORE recomputing sell anchors / trade pricing so the most recent
    # realised sale prices can influence the anchor selection in the same cycle.
    t0 = time.perf_counter()
    try:
        orders_sync_res = await sync_bm_orders_for_user(
            db,
            user_id=user_id,
            full=False,          # incremental default
            page_size=50,        # default in service
            overlap_seconds=300, # default in service
        )
        logger.info(
            "[sell_pricing_cycle] orders_sync DONE user_id=%s pages=%s fetched=%s upserted_new=%s elapsed=%.3fs",
            user_id,
            orders_sync_res.get("pages"),
            orders_sync_res.get("fetched_orders"),
            orders_sync_res.get("upserted_new"),
            time.perf_counter() - t0,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("[sell_pricing_cycle] orders_sync ERROR user_id=%s err=%r", user_id, exc)
        orders_sync_res = {"ok": False, "error": str(exc), "error_type": exc.__class__.__name__}

    timings["orders_sync"] = time.perf_counter() - t0

    # 7) Recompute group-level sell anchors (lowest net sell price, viability, etc.)
    t0 = time.perf_counter()
    try:
        sell_anchor_recompute_res = await recompute_sell_anchors_for_user(
            db,
            user_id,
            groups_filter=groups_filter,
        )
        timings["sell_anchor_recompute"] = time.perf_counter() - t0
        logger.info(
            "[sell_pricing_cycle] sell_anchor_recompute DONE user_id=%s session_id=%s matched=%s updated=%s failed=%s elapsed=%.3fs",
            user_id,
            session_id,
            sell_anchor_recompute_res.get("matched_groups"),
            sell_anchor_recompute_res.get("updated"),
            sell_anchor_recompute_res.get("failed"),
            timings["sell_anchor_recompute"],
        )
    except Exception as exc:  # noqa: BLE001
        timings["sell_anchor_recompute"] = time.perf_counter() - t0
        logger.error(
            "[sell_pricing_cycle] sell_anchor_recompute ERROR user_id=%s session_id=%s err=%r",
            user_id,
            session_id,
            exc,
        )
        sell_anchor_recompute_res = {"error": str(exc)}

    # 8) Trade pricing recompute (max_trade_price)
    t0 = time.perf_counter()
    try:
        trade_pricing_recompute_res = await recompute_trade_pricing_for_user(
            db,
            user_id,
            groups_filter=groups_filter,
        )
        timings["trade_pricing_recompute"] = time.perf_counter() - t0
        logger.info(
            "[sell_pricing_cycle] trade_pricing_recompute DONE user_id=%s matched=%s updated=%s failed=%s elapsed=%.3fs",
            user_id,
            trade_pricing_recompute_res.get("matched_groups"),
            trade_pricing_recompute_res.get("updated"),
            trade_pricing_recompute_res.get("failed"),
            timings["trade_pricing_recompute"],
        )
    except Exception as exc:  # noqa: BLE001
        timings["trade_pricing_recompute"] = time.perf_counter() - t0
        logger.exception(
            "[sell_pricing_cycle] trade_pricing_recompute ERROR user_id=%s err=%r",
            user_id,
            exc,
        )
        trade_pricing_recompute_res = {"error": str(exc)}

    timings["total"] = time.perf_counter() - start_total
    finished_at_dt = _now()

    logger.info(
        "[sell_pricing_cycle] DONE user_id=%s session_id=%s total_elapsed=%.3fs timings=%s summary=%s",
        user_id,
        session_id,
        timings["total"],
        timings,
        {
            "activation": {k: activation_res.get(k) for k in ("attempted", "activated", "failed", "skipped")},
            "backbox": {
                k: backbox_res.get(k)
                for k in (
                    "processed",
                    "backbox_success",
                    "backbox_failed",
                    "backbox_missing",
                    "deactivated",
                    "deactivate_failed",
                    "skipped_inactive",
                )
            },
            "sell_anchor_recompute": (
                {k: sell_anchor_recompute_res.get(k) for k in ("matched_groups", "updated", "failed")}
                if isinstance(sell_anchor_recompute_res, dict)
                else sell_anchor_recompute_res
            ),
        },
    )

    return {
        "user_id": user_id,
        "session_id": session_id,
        "groups_filter": groups_filter,
        "sell_sync": sell_sync_res,
        "activation_session": session_info,
        "activation": activation_res,
        "backbox_then_deactivate": backbox_res,
        "cleanup_deactivate_session": cleanup_res,
        "sell_anchor_recompute": sell_anchor_recompute_res,
        "trade_pricing_recompute": trade_pricing_recompute_res,
        "orders_sync": orders_sync_res,
        "timings": timings,
        "started_at": started_at_dt.isoformat(),
        "finished_at": finished_at_dt.isoformat(),
    }






