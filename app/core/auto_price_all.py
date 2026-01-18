from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import cast

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorDatabase
from starlette.datastructures import State

from app.features.backmarket.sell.service import run_price_all_for_user

logger = logging.getLogger(__name__)


class _AppState(State):
    # Type-only attributes for app.state to satisfy IDEs/type checkers.
    db: AsyncIOMotorDatabase
    auto_price_all_lock: asyncio.Lock
    auto_price_all_stop: asyncio.Event | None
    auto_price_all_task: asyncio.Task | None


class _FastAPIWithState(FastAPI):
    state: _AppState


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int, *, min_value: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        value = default
    else:
        try:
            value = int(raw.strip())
        except ValueError:
            value = default

    if min_value is not None:
        value = max(min_value, value)
    return value


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    return raw if raw else default


def _env_csv(name: str) -> tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None:
        return ()
    parts = [p.strip() for p in raw.split(",")]
    return tuple([p for p in parts if p])


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class AutoPriceAllSettings:
    enabled: bool
    interval_seconds: int
    user_ids: tuple[str, ...]  # can include special token "ALL"
    run_on_startup: bool

    # Parameters passed to run_price_all_for_user (same defaults as router)
    sell_wait_seconds: int
    sell_max_backbox_attempts: int
    tradein_market: str
    tradein_currency: str
    tradein_wait_seconds: int
    apply_tradein_offers: bool
    offers_concurrency: int
    offers_limit: int | None
    offers_dry_run: bool
    offers_require_ok_to_update: bool
    include_stage_results: bool
    include_item_results: bool


def load_auto_price_all_settings() -> AutoPriceAllSettings:
    enabled = _env_bool("AUTO_PRICE_ALL_ENABLED", False)
    interval_minutes = _env_int("AUTO_PRICE_ALL_INTERVAL_MINUTES", 90, min_value=1)
    interval_seconds = int(interval_minutes * 60)

    # Prefer explicit list; support legacy single-id env var.
    user_ids = _env_csv("AUTO_PRICE_ALL_USER_IDS")
    if not user_ids:
        single = os.getenv("AUTO_PRICE_ALL_USER_ID")
        if single and single.strip():
            user_ids = (single.strip(),)

    run_on_startup = _env_bool("AUTO_PRICE_ALL_RUN_ON_STARTUP", False)

    offers_limit_raw = os.getenv("AUTO_PRICE_ALL_OFFERS_LIMIT")
    offers_limit: int | None
    if offers_limit_raw and offers_limit_raw.strip():
        try:
            offers_limit = int(offers_limit_raw.strip())
        except ValueError:
            offers_limit = None
    else:
        offers_limit = None

    return AutoPriceAllSettings(
        enabled=enabled,
        interval_seconds=interval_seconds,
        user_ids=user_ids,
        run_on_startup=run_on_startup,
        sell_wait_seconds=_env_int("AUTO_PRICE_ALL_SELL_WAIT_SECONDS", 180, min_value=0),
        sell_max_backbox_attempts=_env_int("AUTO_PRICE_ALL_SELL_MAX_BACKBOX_ATTEMPTS", 3, min_value=1),
        tradein_market=_env_str("AUTO_PRICE_ALL_TRADEIN_MARKET", "GB").upper(),
        tradein_currency=_env_str("AUTO_PRICE_ALL_TRADEIN_CURRENCY", "GBP").upper(),
        tradein_wait_seconds=_env_int("AUTO_PRICE_ALL_TRADEIN_WAIT_SECONDS", 60, min_value=0),
        apply_tradein_offers=_env_bool("AUTO_PRICE_ALL_APPLY_TRADEIN_OFFERS", True),
        offers_concurrency=_env_int("AUTO_PRICE_ALL_OFFERS_CONCURRENCY", 10, min_value=1),
        offers_limit=offers_limit,
        offers_dry_run=_env_bool("AUTO_PRICE_ALL_OFFERS_DRY_RUN", False),
        offers_require_ok_to_update=_env_bool("AUTO_PRICE_ALL_OFFERS_REQUIRE_OK_TO_UPDATE", True),
        include_stage_results=_env_bool("AUTO_PRICE_ALL_INCLUDE_STAGE_RESULTS", False),
        include_item_results=_env_bool("AUTO_PRICE_ALL_INCLUDE_ITEM_RESULTS", False),
    )


async def _resolve_user_ids(db: AsyncIOMotorDatabase, configured: tuple[str, ...]) -> tuple[str, ...]:
    # Support a special token to run for all active users.
    if any(x.strip().upper() == "ALL" for x in configured):
        cursor = db["users"].find({"is_active": True}, {"_id": 0, "id": 1})
        docs = await cursor.to_list(length=None)
        ids = [str(d.get("id") or "").strip() for d in docs]
        return tuple([x for x in ids if x])
    return tuple([x for x in configured if x.strip()])


async def _run_once(db: AsyncIOMotorDatabase, user_id: str, s: AutoPriceAllSettings) -> None:
    logger.info("[auto_price_all] start user_id=%s at=%s", user_id, _now_utc_iso())
    res = await run_price_all_for_user(
        db,
        user_id=user_id,
        sell_wait_seconds=s.sell_wait_seconds,
        sell_max_backbox_attempts=s.sell_max_backbox_attempts,
        tradein_market=s.tradein_market,
        tradein_currency=s.tradein_currency,
        tradein_wait_seconds=s.tradein_wait_seconds,
        apply_tradein_offers=s.apply_tradein_offers,
        offers_concurrency=s.offers_concurrency,
        offers_limit=s.offers_limit,
        offers_dry_run=s.offers_dry_run,
        offers_require_ok_to_update=s.offers_require_ok_to_update,
        include_stage_results=s.include_stage_results,
        include_item_results=s.include_item_results,
    )
    elapsed = res.get("elapsed_seconds")
    logger.info(
        "[auto_price_all] done user_id=%s elapsed_seconds=%s at=%s",
        user_id,
        elapsed,
        _now_utc_iso(),
    )


async def _auto_price_all_loop(app: FastAPI, stop: asyncio.Event, s: AutoPriceAllSettings) -> None:
    app = cast(_FastAPIWithState, app)

    # This lock prevents overlapping runs if something calls start twice (or you add a manual trigger).
    lock: asyncio.Lock = getattr(app.state, "auto_price_all_lock", asyncio.Lock())
    setattr(app.state, "auto_price_all_lock", lock)

    # Optional: run immediately.
    if s.run_on_startup and not stop.is_set():
        # noinspection PyBroadException
        try:
            async with lock:
                db: AsyncIOMotorDatabase = app.state.db
                for uid in await _resolve_user_ids(db, s.user_ids):
                    await _run_once(db, uid, s)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001  # pylint: disable=broad-except
            logger.exception("[auto_price_all] run_on_startup failed")

    while not stop.is_set():
        try:
            # Wait for the interval, but wake early on shutdown.
            await asyncio.wait_for(stop.wait(), timeout=float(s.interval_seconds))
        except asyncio.TimeoutError:
            pass

        if stop.is_set():
            break

        # noinspection PyBroadException
        try:
            async with lock:
                db = app.state.db
                user_ids = await _resolve_user_ids(db, s.user_ids)
                if not user_ids:
                    logger.warning(
                        "[auto_price_all] enabled but no users resolved (AUTO_PRICE_ALL_USER_IDS=%r)",
                        s.user_ids,
                    )
                    continue
                for uid in user_ids:
                    await _run_once(db, uid, s)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001  # pylint: disable=broad-except
            logger.exception("[auto_price_all] loop iteration failed")


def start_auto_price_all(app: FastAPI) -> None:
    """Start the background scheduler if enabled via env vars.

    This is safe for single-worker uvicorn. If you run uvicorn with multiple workers
    (or use --reload), you may see duplicate schedulers.
    """
    app = cast(_FastAPIWithState, app)

    s = load_auto_price_all_settings()
    if not s.enabled:
        logger.info("[auto_price_all] disabled (AUTO_PRICE_ALL_ENABLED!=true)")
        return

    if not s.user_ids:
        logger.warning(
            "[auto_price_all] AUTO_PRICE_ALL_ENABLED=true but no AUTO_PRICE_ALL_USER_IDS set; not starting"
        )
        return

    # Prevent double-start.
    existing = getattr(app.state, "auto_price_all_task", None)
    if existing is not None and not getattr(existing, "done", lambda: True)():
        logger.warning("[auto_price_all] task already running; skipping start")
        return

    stop = asyncio.Event()
    task = asyncio.create_task(_auto_price_all_loop(app, stop, s), name="auto_price_all")

    setattr(app.state, "auto_price_all_stop", stop)
    setattr(app.state, "auto_price_all_task", task)
    logger.info(
        "[auto_price_all] started interval_seconds=%s user_ids=%s run_on_startup=%s",
        s.interval_seconds,
        s.user_ids,
        s.run_on_startup,
    )


async def stop_auto_price_all(app: FastAPI) -> None:
    app = cast(_FastAPIWithState, app)

    stop: asyncio.Event | None = getattr(app.state, "auto_price_all_stop", None)
    task: asyncio.Task | None = getattr(app.state, "auto_price_all_task", None)

    if stop is None or task is None:
        return

    stop.set()
    task.cancel()

    # noinspection PyBroadException
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:  # noqa: BLE001  # pylint: disable=broad-except
        logger.exception("[auto_price_all] task shutdown failed")
    finally:
        setattr(app.state, "auto_price_all_stop", None)
        setattr(app.state, "auto_price_all_task", None)
        logger.info("[auto_price_all] stopped")
