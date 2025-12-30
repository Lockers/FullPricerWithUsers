from __future__ import annotations

"""
Trade-in listings + trade-in competitor persistence (Mongo).

This module contains:

1) Trade-in listings mirror (bm_tradein_listings)
   - One document per trade-in listing per user.
   - Bounded history of GB price changes.

2) Trade-in competitor snapshot persistence (pricing_groups)
   - Stores latest competitor snapshot + bounded history under:
       tradein_listing.competitor
       tradein_listing.competitor_history
   - When a gross_price_to_win is available, we also update the pricing_groups "prices"
     field for that market (normally GB). This lets the rest of the pricer treat the
     "trade-in price" as the competitor-derived price_to_win.

3) Hard-failure bookkeeping (pricing_bad_tradein_skus)
   - Stores tradein_ids that consistently fail due to bad auth/payload/not-found
   - Batch runs can skip them.

4) Competitor-unavailable bookkeeping (pricing_unavailable_tradein_competitors)
   - Stores tradein_ids that repeatedly returned no competitor / missing price_to_win
   - Intended for manual review.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

HISTORY_MAX_POINTS = 100
UNAVAILABLE_COMPETITORS_COLLECTION = "pricing_unavailable_tradein_competitors"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Trade-in listings mirror: bm_tradein_listings
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TradeinUpsertResult:
    attempted: int
    skipped_missing_id: int
    upserted: int
    matched: int
    modified: int
    elapsed_seconds: float


def _extract_tradein_id(raw: Dict[str, Any]) -> Optional[str]:
    """
    BackMarket trade-in listing payload usually has `id`.
    Keep defensive so we don't crash on odd payloads.
    """
    val = raw.get("id") or raw.get("tradein_id") or raw.get("uuid")
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _extract_product_id(raw: Dict[str, Any]) -> Optional[str]:
    val = raw.get("productId") or raw.get("product_id")
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _parse_gb_price(prices: Any) -> tuple[Optional[float], Optional[str]]:
    """
    prices is usually like:
    {
      "GB": {"amount": 123.45, "currency": "GBP"},
      "FR": ...
    }
    """
    if not isinstance(prices, dict):
        return None, None

    gb = prices.get("GB")
    if not isinstance(gb, dict):
        return None, None

    amount_raw = gb.get("amount")
    currency = gb.get("currency")

    amount: Optional[float] = None
    if amount_raw is not None:
        try:
            amount = float(amount_raw)
        except (TypeError, ValueError):
            amount = None

    currency_out = str(currency).strip() if currency is not None else None
    return amount, currency_out


class TradeinListingsRepo:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db["bm_tradein_listings"]

    async def upsert_many(self, *, user_id: str, listings: List[Dict[str, Any]]) -> TradeinUpsertResult:
        """
        Upsert many listings efficiently.

        Implementation detail:
        - Uses bulk_write with update pipelines so we can:
          - keep first_seen_at stable (ifNull)
          - append to tradein_history only if GB price changed
          - cap history length
        """
        start = time.perf_counter()
        now = _now_utc()

        ops: List[UpdateOne] = []
        skipped = 0

        for raw in listings:
            tradein_id = _extract_tradein_id(raw)
            if not tradein_id:
                skipped += 1
                continue

            product_id = _extract_product_id(raw)
            sku = raw.get("sku")
            aesthetic_grade_code = raw.get("aestheticGradeCode")
            markets = raw.get("markets") or []
            prices = raw.get("prices") or {}

            gb_amount, gb_currency = _parse_gb_price(prices)

            # Capture previous gb_amount BEFORE we set the new one (pipeline stage 1)
            update_pipeline = [
                {"$set": {"_old_gb_amount": "$gb_amount"}},
                {
                    "$set": {
                        "user_id": user_id,
                        "tradein_id": tradein_id,
                        "product_id": product_id,
                        "sku": sku,
                        "aesthetic_grade_code": aesthetic_grade_code,
                        "markets": markets,
                        "prices": prices,
                        "gb_amount": gb_amount,
                        "gb_currency": gb_currency,
                        "last_seen_at": now,
                    }
                },
                {
                    "$set": {
                        "first_seen_at": {"$ifNull": ["$first_seen_at", now]},
                        "tradein_history": {
                            "$let": {
                                "vars": {"hist": {"$ifNull": ["$tradein_history", []]}},
                                "in": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$ne": [gb_amount, None]},
                                                {"$ne": ["$_old_gb_amount", gb_amount]},
                                            ]
                                        },
                                        {
                                            "$slice": [
                                                {
                                                    "$concatArrays": [
                                                        "$$hist",
                                                        [
                                                            {
                                                                "gb_amount": gb_amount,
                                                                "gb_currency": gb_currency,
                                                                "timestamp": now,
                                                            }
                                                        ],
                                                    ]
                                                },
                                                -HISTORY_MAX_POINTS,
                                            ]
                                        },
                                        "$$hist",
                                    ]
                                },
                            }
                        },
                    }
                },
                {"$unset": ["_old_gb_amount"]},
            ]

            ops.append(
                UpdateOne(
                    {"user_id": user_id, "tradein_id": tradein_id},
                    update_pipeline,
                    upsert=True,
                )
            )

        if not ops:
            return TradeinUpsertResult(
                attempted=len(listings),
                skipped_missing_id=skipped,
                upserted=0,
                matched=0,
                modified=0,
                elapsed_seconds=time.perf_counter() - start,
            )

        res = await self._col.bulk_write(ops, ordered=False)

        return TradeinUpsertResult(
            attempted=len(listings),
            skipped_missing_id=skipped,
            upserted=int(res.upserted_count),
            matched=int(res.matched_count),
            modified=int(res.modified_count),
            elapsed_seconds=time.perf_counter() - start,
        )

    async def count_for_user(self, user_id: str) -> int:
        return int(await self._col.count_documents({"user_id": user_id}))

    async def sample_for_user(self, user_id: str, limit: int = 25) -> List[Dict[str, Any]]:
        cursor = self._col.find({"user_id": user_id}, {"_id": 0}).sort("last_seen_at", -1).limit(int(limit))
        return [doc async for doc in cursor]


# ---------------------------------------------------------------------------
# Batch-safety: pricing_bad_tradein_skus
# ---------------------------------------------------------------------------

async def upsert_bad_tradein(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    tradein_id: str,
    reason_code: str,
    status_code: int,
    detail: Optional[str] = None,
) -> None:
    """
    Record a "hard failure" trade-in listing so future runs can skip it.

    Indexes (created in mongo_lifespan):
      - pricing_bad_tradein_skus: (user_id, tradein_id) unique
      - pricing_bad_tradein_skus: (user_id, reason_code)
    """
    now = _now_utc()
    doc = {
        "user_id": user_id,
        "tradein_id": tradein_id,
        "reason_code": reason_code,
        "last_status": int(status_code),
        "detail": detail,
        "updated_at": now,
    }

    try:
        await db["pricing_bad_tradein_skus"].update_one(
            {"user_id": user_id, "tradein_id": tradein_id},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    except PyMongoError:
        # Best-effort: do not crash jobs if "bad sku" bookkeeping fails.
        logger.exception(
            "[tradein_bad] upsert failed user_id=%s tradein_id=%s reason=%s status=%s",
            user_id,
            tradein_id,
            reason_code,
            status_code,
        )


async def get_bad_tradein_ids_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    reason_codes: Iterable[str],
) -> Set[str]:
    """
    Return tradein_ids that should be skipped (known hard failures).
    """
    reasons = [r for r in dict.fromkeys(reason_codes) if isinstance(r, str) and r.strip()]
    if not reasons:
        return set()

    out: Set[str] = set()
    cursor = db["pricing_bad_tradein_skus"].find(
        {"user_id": user_id, "reason_code": {"$in": reasons}},
        projection={"tradein_id": 1},
    )
    async for doc in cursor:
        tid = doc.get("tradein_id")
        if isinstance(tid, str) and tid:
            out.add(tid)
    return out


# ---------------------------------------------------------------------------
# Competitor-unavailable: pricing_unavailable_tradein_competitors
# ---------------------------------------------------------------------------

async def upsert_unavailable_tradein_competitor(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    trade_sku: str,
    tradein_id: str,
    market: str,
    currency: str,
    reason_code: str,
    status_code: int,
    detail: Optional[str] = None,
    cf_ray: Optional[str] = None,
    attempts: int = 0,
) -> None:
    """
    Store trade-ins that couldn't get a competitor price after retries.

    This is not necessarily permanent; it's intended for manual review / dashboards.
    """
    now = _now_utc()
    doc = {
        "user_id": user_id,
        "trade_sku": trade_sku,
        "tradein_id": tradein_id,
        "market": str(market).upper().strip(),
        "currency": str(currency).upper().strip(),
        "reason_code": str(reason_code).strip() or "unknown",
        "last_status": int(status_code),
        "detail": detail,
        "cf_ray": cf_ray,
        "attempts": int(attempts),
        "updated_at": now,
    }

    try:
        await db[UNAVAILABLE_COMPETITORS_COLLECTION].update_one(
            {"user_id": user_id, "tradein_id": tradein_id},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    except PyMongoError:
        logger.exception(
            "[tradein_unavailable] upsert failed user_id=%s trade_sku=%s tradein_id=%s reason=%s status=%s",
            user_id,
            trade_sku,
            tradein_id,
            reason_code,
            status_code,
        )


# ---------------------------------------------------------------------------
# pricing_groups competitor snapshot persistence
# ---------------------------------------------------------------------------

async def persist_tradein_competitor_snapshot(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    trade_sku: str,
    snapshot_latest: Dict[str, Any],
    snapshot_history: Dict[str, Any],
    history_max: int = 250,
) -> None:
    """
    Persist:
      - pricing_groups.tradein_listing.competitor (latest, includes raw)
      - pricing_groups.tradein_listing.competitor_history (append, bounded)

    Additionally, if the snapshot contains a gross_price_to_win, we update pricing_groups.prices
    for that market (normally GB). This makes the competitor-derived price available as the
    "trade-in price" for the rest of the pipeline.

    Single atomic update.
    """
    now = _now_utc()

    update: Dict[str, Any] = {
        "$set": {
            "tradein_listing.competitor": snapshot_latest,
            "tradein_listing.competitor_updated_at": now,
            "updated_at": now,
        },
        "$push": {
            "tradein_listing.competitor_history": {
                "$each": [snapshot_history],
                "$slice": -int(history_max),
            }
        },
    }

    # Optional: promote gross_price_to_win to group-level "prices".
    mkt = str(snapshot_latest.get("market") or "").upper().strip()
    cur = str(snapshot_latest.get("currency") or "").upper().strip()

    gross_amount_str_raw = snapshot_latest.get("gross_price_to_win_amount")
    gross_amount_str: Optional[str] = None
    if gross_amount_str_raw is not None:
        s = str(gross_amount_str_raw).strip()
        if s:
            gross_amount_str = s

    gross_amount_float_raw = snapshot_latest.get("gross_price_to_win")
    gross_amount_float: Optional[float] = None
    if gross_amount_float_raw is not None:
        try:
            gross_amount_float = float(gross_amount_float_raw)
        except (TypeError, ValueError):
            gross_amount_float = None

    if mkt and cur and gross_amount_str is not None and gross_amount_float is not None:
        update["$set"].update(
            {
                f"prices.{mkt}.amount": gross_amount_str,
                f"prices.{mkt}.currency": cur,
                "prices.updated_at": now,
            }
        )

        # Back-compat fields used elsewhere in the codebase (GB-only).
        if mkt == "GB":
            update["$set"].update(
                {
                    "prices.gb_amount": float(gross_amount_float),
                    "prices.gb_currency": cur,
                }
            )

    res = await db["pricing_groups"].update_one(
        {"user_id": user_id, "trade_sku": trade_sku},
        update,
    )

    if res.matched_count == 0:
        logger.warning(
            "[tradein_comp] pricing_groups doc not found user_id=%s trade_sku=%s",
            user_id,
            trade_sku,
        )






