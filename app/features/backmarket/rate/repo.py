"""Mongo persistence for bm_rate_state.

This isolates Motor/PyMongo details away from the learner + client logic.

Why we add sorting + update_many
--------------------------------
If there is **no unique index** on (user_id, endpoint_key), concurrent upserts can
create duplicate documents. In that case:

- `find_one_and_update(...)` may return an arbitrary matching doc.
- `update_one(...)` updates an arbitrary matching doc.

That can produce symptoms like:
- runtime logs show low RPS (loaded from one duplicate doc)
- but you inspect another duplicate doc in Mongo and see a different RPS

This repo therefore:
- uses a deterministic sort when selecting a document (prefer newest updated_at)
- uses update_many when saving, to keep duplicates in sync (and logs a warning)

You SHOULD still create a unique index on (user_id, endpoint_key) once the data
is cleaned:
    db.bm_rate_state.createIndex({user_id: 1, endpoint_key: 1}, {unique: true})
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

from app.features.backmarket.rate.state import EndpointConfig, EndpointRateState

logger = logging.getLogger(__name__)


class BmRateStateRepository:
    """Persistence wrapper for the bm_rate_state collection."""

    def __init__(self, db: AsyncIOMotorDatabase):
        self._db = db
        self._col = db["bm_rate_state"]

    async def ensure_indexes(self) -> None:
        """Best-effort index creation.

        Notes
        -----
        - Creating a unique index will FAIL if duplicates exist.
        - Call this from an admin script / startup task once duplicates are removed.
        """

        try:
            await self._col.create_index([("user_id", 1), ("endpoint_key", 1)], unique=True)
        except PyMongoError:
            # Common causes: duplicates exist, insufficient privileges, transient
            # connectivity issues, etc. This is best-effort.
            logger.exception("[bm_rate_repo] ensure_indexes failed (duplicates may exist)")

    async def load_or_init(
        self, *, user_id: str, endpoint_key: str, cfg: EndpointConfig
    ) -> EndpointRateState:
        """Load the existing state for (user_id, endpoint_key) or insert defaults.

        Implementation detail:
        - Uses $setOnInsert so an existing record is never overwritten by defaults.
        - Uses a deterministic `sort` (prefer newest updated_at) to reduce weirdness
          if duplicates exist.

        IMPORTANT:
        - Mongo does not allow updating the same field in both $setOnInsert and $set.
          Therefore, any fields we want to *always* sync from code config MUST NOT
          also appear in $setOnInsert.
        """

        now = datetime.now(timezone.utc)

        defaults = EndpointRateState(
            user_id=user_id,
            endpoint_key=endpoint_key,

            # RPS
            base_rps=cfg.base_rps,
            min_rps=cfg.min_rps,
            max_rps=cfg.max_rps,

            # Concurrency
            base_concurrency=cfg.base_concurrency,
            min_concurrency=cfg.min_concurrency,
            max_concurrency=cfg.max_concurrency,

            # Learned
            rps_float=float(cfg.base_rps),
            concurrency_float=float(cfg.base_concurrency),
            current_rps=float(cfg.base_rps),
            current_concurrency=int(cfg.base_concurrency),

            locked_rps=None,
            locked_concurrency=None,
            cooldown_until=None,
            consecutive_successes=0,
            consecutive_429s=0,
            success_count=0,
            error_count=0,
            last_status=None,
            updated_at=now,
        )
        defaults.clamp_and_recompute()

        # Build the insert-only doc, but REMOVE config fields that we also $set below.
        insert_doc = defaults.to_mongo()
        for k in (
            "base_rps",
            "min_rps",
            "max_rps",
            "base_concurrency",
            "min_concurrency",
            "max_concurrency",
        ):
            insert_doc.pop(k, None)

        # Prefer most recently updated doc if duplicates exist.
        sort = [("updated_at", -1), ("_id", -1)]

        try:
            doc = await self._col.find_one_and_update(
                {"user_id": user_id, "endpoint_key": endpoint_key},
                {
                    # Insert defaults only on first creation.
                    "$setOnInsert": insert_doc,

                    # Always keep the static config fields in sync with code.
                    # This avoids stale floors/ceilings preventing adaptation
                    # (especially important for fractional RPS experiments).
                    "$set": {
                        "base_rps": float(cfg.base_rps),
                        "min_rps": float(cfg.min_rps),
                        "max_rps": float(cfg.max_rps),
                        "base_concurrency": int(cfg.base_concurrency),
                        "min_concurrency": int(cfg.min_concurrency),
                        "max_concurrency": int(cfg.max_concurrency),
                    },
                },
                upsert=True,
                sort=sort,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError:
            # Happens only if you DO have a unique index and two writers raced.
            doc = await self._col.find_one(
                {"user_id": user_id, "endpoint_key": endpoint_key},
                sort=sort,
            )

        if not doc:
            return defaults

        return EndpointRateState.from_mongo(user_id, endpoint_key, doc, cfg)

    async def save(self, state: EndpointRateState) -> None:
        """Persist the state back to Mongo.

        Uses update_many so if duplicates exist they are kept consistent.
        Logs a warning if multiple docs were matched.
        """

        state.updated_at = datetime.now(timezone.utc)

        res = await self._col.update_many(
            {"user_id": state.user_id, "endpoint_key": state.endpoint_key},
            {"$set": state.to_mongo()},
            upsert=True,
        )

        matched_raw = getattr(res, "matched_count", 0) or 0
        matched = int(matched_raw) if isinstance(matched_raw, int) else 0

        if matched > 1:
            logger.warning(
                "[bm_rate_repo] duplicate bm_rate_state rows detected user_id=%s endpoint=%s matched=%d "
                "(add unique index after cleanup)",
                state.user_id,
                state.endpoint_key,
                matched,
            )

