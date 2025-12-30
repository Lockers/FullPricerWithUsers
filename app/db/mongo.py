# app/db/mongo.py
"""
MongoDB connection + FastAPI dependency.

- Connects once at app startup (lifespan).
- Stores db on app.state.db
- Creates required indexes in an idempotent way.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import timezone

from fastapi import FastAPI, Request
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import OperationFailure

from app.core.config import config

logger = logging.getLogger(__name__)


async def _ensure_index(col, keys, **kwargs) -> None:
    """
    Create an index if it doesn't exist.

    If an index with the same keys already exists under a different name,
    Mongo raises code=85 (IndexOptionsConflict). In that case we keep the
    existing index and continue.
    """
    try:
        await col.create_index(keys, **kwargs)
    except OperationFailure as e:
        if getattr(e, "code", None) == 85:
            logger.warning(
                "Index conflict on %s keys=%s name=%s; keeping existing index",
                col.name,
                keys,
                kwargs.get("name"),
            )
            return
        raise


@asynccontextmanager
async def mongo_lifespan(fastapi_app: FastAPI):
    client = AsyncIOMotorClient(
        config.mongo_uri,
        tz_aware=True,
        tzinfo=timezone.utc,
    )
    db = client[config.mongo_db]

    logger.info("Mongo connected: uri=%s db=%s", config.mongo_uri, db.name)

    # Store on app.state (avoid IDE/type checker complaining about FastAPI.state)
    state = getattr(fastapi_app, "state")
    setattr(state, "mongo_client", client)
    setattr(state, "db", db)

    # ---- Indexes ----
    await _ensure_index(db["users"], [("id", 1)], unique=True, name="uniq_users_id")
    await _ensure_index(db["users"], [("email", 1)], unique=True, name="uniq_users_email")

    await _ensure_index(db["user_settings"], [("user_id", 1)], unique=True, name="uniq_user_settings_user_id")

    await _ensure_index(
        db["bm_rate_state"],
        [("user_id", 1), ("endpoint_key", 1)],
        unique=True,
        name="uniq_bm_rate_state_user_endpoint",
    )

    await _ensure_index(
        db["bm_sell_listings"],
        [("user_id", 1), ("listing_id", 1)],
        unique=True,
        name="uniq_bm_sell_listings_user_listing",
    )
    await _ensure_index(
        db["bm_tradein_listings"],
        [("user_id", 1), ("tradein_id", 1)],
        unique=True,
        name="uniq_bm_tradein_listings_user_tradein",
    )

    await _ensure_index(
        db["bm_tradein_listings"],
        [("user_id", 1), ("product_id", 1)],
        unique=False,
        name="idx_bm_tradein_listings_user_product",
    )
    await _ensure_index(
        db["bm_tradein_listings"],
        [("user_id", 1), ("sku", 1)],
        unique=False,
        name="idx_bm_tradein_listings_user_sku",
    )

    await _ensure_index(
        db["pricing_groups"],
        [("user_id", 1), ("trade_sku", 1)],
        unique=True,
        name="uniq_pricing_groups_user_trade_sku",
    )
    await _ensure_index(
        db["pricing_groups"],
        [("user_id", 1), ("brand", 1), ("model", 1), ("storage_gb", 1)],
        unique=False,
        name="idx_pricing_groups_user_device",
    )

    await _ensure_index(
        db["pricing_bad_sell_skus"],
        [("user_id", 1), ("listing_id", 1)],
        unique=True,
        name="uniq_pricing_bad_sell_user_listing",
    )
    await _ensure_index(
        db["pricing_bad_sell_skus"],
        [("user_id", 1), ("reason_code", 1)],
        unique=False,
        name="idx_pricing_bad_sell_user_reason",
    )

    await _ensure_index(
        db["pricing_bad_tradein_skus"],
        [("user_id", 1), ("tradein_id", 1)],
        unique=True,
        name="uniq_pricing_bad_tradein_user_tradein",
    )
    await _ensure_index(
        db["pricing_bad_tradein_skus"],
        [("user_id", 1), ("reason_code", 1)],
        unique=False,
        name="idx_pricing_bad_tradein_user_reason",
    )

    await _ensure_index(
        db["pricing_unavailable_tradein_competitors"],
        [("user_id", 1), ("tradein_id", 1)],
        unique=True,
        name="uniq_pricing_unavail_tradein_user_tradein",
    )
    await _ensure_index(
        db["pricing_unavailable_tradein_competitors"],
        [("user_id", 1), ("reason_code", 1)],
        unique=False,
        name="idx_pricing_unavail_tradein_user_reason",
    )
    await _ensure_index(
        db["pricing_repair_costs"],
        [("user_id", 1), ("market", 1), ("brand", 1), ("model", 1)],
        unique=True,
        name="uniq_pricing_repair_costs_user_market_brand_model",
    )
    await _ensure_index(
        db["pricing_repair_costs"],
        [("user_id", 1), ("market", 1)],
        unique=False,
        name="idx_pricing_repair_costs_user_market",
    )
    await _ensure_index(
        db["pricing_repair_costs"],
        [("user_id", 1), ("brand", 1), ("model", 1)],
        unique=False,
        name="idx_pricing_repair_costs_user_brand_model",
    )

    # bm_orders
    await _ensure_index(
        db["bm_orders"],
        [("user_id", 1), ("order_id", 1)],
        unique=True,
        name="uniq_bm_orders_user_order",
    )
    await _ensure_index(
        db["bm_orders"],
        [("user_id", 1), ("date_modification", -1)],
        unique=False,
        name="idx_bm_orders_user_date_mod",
    )

    # bm_orderlines (optional but recommended)
    await _ensure_index(
        db["bm_orderlines"],
        [("user_id", 1), ("orderline_id", 1)],
        unique=True,
        name="uniq_bm_orderlines_user_orderline",
    )
    await _ensure_index(
        db["bm_orderlines"],
        [("user_id", 1), ("listing_sku", 1)],
        unique=False,
        name="idx_bm_orderlines_user_listing_sku",
    )
    await _ensure_index(
        db["bm_orderlines"],
        [("user_id", 1), ("imei", 1)],
        unique=False,
        name="idx_bm_orderlines_user_imei",
    )

    await _ensure_index(
        db["depreciation_models"],
        [("user_id", 1), ("market", 1), ("brand", 1), ("model", 1), ("storage_gb", 1)],
        unique=True,
        name="uniq_depr_models_user_market_device",
    )
    await _ensure_index(
        db["depreciation_models"],
        [("user_id", 1), ("market", 1), ("brand", 1), ("model", 1)],
        unique=False,
        name="idx_depr_models_user_market_brand_model",
    )
    await _ensure_index(
        db["depreciation_multipliers"],
        [("user_id", 1), ("market", 1), ("key", 1)],
        unique=True,
        name="uniq_depr_mult_user_market_key",
    )
    await _ensure_index(
        db["depreciation_multipliers"],
        [("user_id", 1), ("market", 1), ("scope", 1)],
        unique=False,
        name="idx_depr_mult_user_market_scope",
    )

    try:
        yield
    finally:
        client.close()


async def get_db(request: Request) -> AsyncIOMotorDatabase:
    return request.app.state.db




