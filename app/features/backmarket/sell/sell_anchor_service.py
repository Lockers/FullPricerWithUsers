# app/features/backmarket/sell/sell_anchor_service.py
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import ValidationError
from pymongo.errors import PyMongoError

from app.features.backmarket.sell.sell_anchor_models import FeeConfig, FeeItem, SellAnchorSettings
from app.features.backmarket.sell import sell_anchor_repo as repo
from app.features.backmarket.transport.exceptions import BMClientError

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _is_bool(v: Any) -> bool:
    # bool is a subclass of int, so float(True) == 1.0 (can silently corrupt pricing)
    return isinstance(v, bool)


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or _is_bool(v):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_pos_float(v: Any) -> Optional[float]:
    f = _to_float(v)
    if f is None or f <= 0:
        return None
    return f


def _to_rate(v: Any) -> Optional[float]:
    """
    Accept:
      - 0.11  (11%)
      - 11    (normalize to 0.11)

    Reject bools, negatives.
    """
    f = _to_float(v)
    if f is None:
        return None
    if 1.0 < f <= 100.0:
        f = f / 100.0
    if f < 0:
        return None
    return f


DEFAULT_FEE_CONFIG = FeeConfig(
    currency="GBP",
    items=[
        FeeItem(key="bm_commission", type="percent", rate=0.11),
        FeeItem(key="ccbm", type="fixed", amount=5.49),
        FeeItem(key="payment_processor", type="percent", rate=0.03),
        FeeItem(key="shipping", type="fixed", amount=15.0),
    ],
)

DEFAULT_SETTINGS = SellAnchorSettings(
    safe_ratio=0.85,
    fee_config=DEFAULT_FEE_CONFIG,
)

REQUIRED_FEE_KEYS = {"bm_commission", "ccbm", "payment_processor", "shipping"}


def _fee_config_or_default(raw: Any) -> FeeConfig:
    """
    Defensive parse/validate:
    - If user settings are missing/broken/incomplete, fall back to DEFAULT_FEE_CONFIG.
    - Prevent configs that accidentally drop fee items (net becomes nonsense).
    """
    if not raw:
        return DEFAULT_FEE_CONFIG

    try:
        cfg = raw if isinstance(raw, FeeConfig) else FeeConfig(**raw)
    except (TypeError, ValueError, ValidationError) as exc:
        logger.warning("[sell_anchor] invalid fee_config -> using default err=%r raw=%r", exc, raw)
        return DEFAULT_FEE_CONFIG

    items = getattr(cfg, "items", None) or []
    keys = {str(getattr(i, "key", "")).strip() for i in items if getattr(i, "key", None)}
    if not REQUIRED_FEE_KEYS.issubset(keys):
        logger.warning(
            "[sell_anchor] incomplete fee_config keys=%s (required=%s) -> using default",
            sorted(keys),
            sorted(REQUIRED_FEE_KEYS),
        )
        return DEFAULT_FEE_CONFIG

    return cfg


def _extract_best_price_to_win(listing: Dict[str, Any]) -> Optional[float]:
    raw = listing.get("raw") or {}
    backbox = listing.get("backbox") or {}

    candidates = [
        listing.get("backbox_best_price_to_win"),
        raw.get("backbox_best_price_to_win"),
        backbox.get("price_to_win"),
        (raw.get("price_to_win") or {}).get("amount"),
    ]

    for c in candidates:
        f = _to_pos_float(c)
        if f is not None:
            return f
    return None


def _extract_currency(group: Dict[str, Any], source_listing: Optional[Dict[str, Any]], fee_currency: str) -> str:
    if source_listing:
        for c in (
            source_listing.get("currency"),
            (source_listing.get("backbox") or {}).get("currency"),
            ((source_listing.get("raw") or {}).get("price") or {}).get("currency"),
        ):
            if isinstance(c, str) and c.strip():
                return c.strip()

    for l in (group.get("listings") or []):
        c = l.get("currency")
        if isinstance(c, str) and c.strip():
            return c.strip()

    if isinstance(fee_currency, str) and fee_currency.strip():
        return fee_currency.strip()

    return "GBP"


def _compute_net(gross: float, fee_config: FeeConfig) -> Tuple[float, float, list[dict]]:
    total = 0.0
    breakdown: list[dict] = []

    for item in (fee_config.items or []):
        item_type = getattr(item, "type", None)

        if item_type == "percent":
            rate = _to_rate(getattr(item, "rate", None)) or 0.0
            fee = gross * rate
            breakdown.append({"key": item.key, "type": "percent", "rate": rate, "fee": round(fee, 2)})
        else:
            amt = _to_float(getattr(item, "amount", None)) or 0.0
            fee = amt
            breakdown.append({"key": item.key, "type": "fixed", "amount": amt, "fee": round(fee, 2)})

        total += fee

    net = gross - total
    return round(net, 2), round(total, 2), breakdown


def _parse_object_id(group_id: str) -> ObjectId:
    try:
        return ObjectId(group_id)
    except (InvalidId, TypeError) as exc:
        raise BMClientError(f"Invalid group_id ObjectId: {group_id}") from exc


async def get_sell_anchor_settings_for_user(db: AsyncIOMotorDatabase, user_id: str) -> Dict[str, Any]:
    doc = await repo.get_user_settings_doc(db, user_id)
    if not doc:
        s = DEFAULT_SETTINGS
        return {
            "user_id": user_id,
            "safe_ratio": s.safe_ratio,
            "fee_config": s.fee_config.model_dump(),
            "created_at": None,
            "updated_at": None,
        }

    safe_ratio = _to_float(doc.get("safe_ratio")) or DEFAULT_SETTINGS.safe_ratio
    fee_cfg = _fee_config_or_default(doc.get("fee_config"))

    settings = SellAnchorSettings(safe_ratio=safe_ratio, fee_config=fee_cfg)

    return {
        "user_id": user_id,
        "safe_ratio": settings.safe_ratio,
        "fee_config": settings.fee_config.model_dump(),
        "created_at": doc.get("created_at"),
        "updated_at": doc.get("updated_at"),
    }


async def update_sell_anchor_settings_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    settings: SellAnchorSettings,
) -> Dict[str, Any]:
    saved = await repo.upsert_user_settings_doc(
        db,
        user_id,
        {
            "safe_ratio": float(settings.safe_ratio),
            "fee_config": settings.fee_config.model_dump(),
        },
    )
    return {
        "user_id": user_id,
        "safe_ratio": float(saved.get("safe_ratio") or settings.safe_ratio),
        "fee_config": (saved.get("fee_config") or settings.fee_config.model_dump()),
        "created_at": saved.get("created_at"),
        "updated_at": saved.get("updated_at"),
    }


def _build_sell_anchor_for_group_doc(
    group: Dict[str, Any],
    *,
    safe_ratio: float,
    fee_config: FeeConfig,
    max_price_map: Dict[str, float],
) -> Dict[str, Any]:
    listings = group.get("listings") or []

    auto_gross: Optional[float] = None
    source_listing: Optional[Dict[str, Any]] = None

    for l in listings:
        p = _extract_best_price_to_win(l)
        if p is None:
            continue
        if auto_gross is None or p < auto_gross:
            auto_gross = p
            source_listing = l

    currency = _extract_currency(group, source_listing, fee_config.currency)

    auto_net: Optional[float] = None
    auto_fees_total: Optional[float] = None
    auto_breakdown: list[dict] = []
    if auto_gross is not None:
        auto_net, auto_fees_total, auto_breakdown = _compute_net(auto_gross, fee_config)

    # safest: min(max_price) across children
    group_max_price: Optional[float] = None
    for l in listings:
        lid = str(l.get("bm_listing_id") or "").strip()
        mp = max_price_map.get(lid)
        if mp is None or mp <= 0:
            continue
        if group_max_price is None or mp < group_max_price:
            group_max_price = mp

    reason: Optional[str] = None
    auto_viable = False
    if auto_gross is None:
        reason = "missing_gross_anchor"
    elif group_max_price is None:
        reason = "missing_max_price"
    elif auto_gross > (group_max_price * safe_ratio):
        reason = "gross_above_safe_ratio"
    elif auto_net is None or auto_net <= 0:
        # Avoid silently accepting broken fee configs (net hits 0/negative)
        reason = "net_non_positive"
    else:
        auto_viable = True

    manual_gross = _to_pos_float(group.get("manual_sell_anchor_gross"))
    manual_net: Optional[float] = None
    manual_fees_total: Optional[float] = None
    manual_breakdown: list[dict] = []
    if manual_gross is not None:
        manual_net, manual_fees_total, manual_breakdown = _compute_net(manual_gross, fee_config)

    gross_used: Optional[float] = None
    net_used: Optional[float] = None
    source_used: Optional[str] = None
    needs_manual_anchor = False

    if auto_viable and auto_gross is not None and auto_net is not None and auto_net > 0:
        gross_used = auto_gross
        net_used = auto_net
        source_used = "auto"
    elif manual_gross is not None and manual_net is not None and manual_net > 0:
        gross_used = manual_gross
        net_used = manual_net
        source_used = "manual"
    else:
        needs_manual_anchor = True

    return {
        "lowest_gross_price_to_win": auto_gross,
        "lowest_net_sell_price": auto_net,
        "auto_fees_total": auto_fees_total,
        "auto_fee_breakdown": auto_breakdown,
        "source_bm_listing_id": (source_listing or {}).get("bm_listing_id"),
        "source_full_sku": (source_listing or {}).get("full_sku"),
        "safe_ratio": safe_ratio,
        "max_price": group_max_price,
        "auto_viable": auto_viable,
        "reason": reason,
        "manual_sell_anchor_gross": manual_gross,
        "manual_net_sell_price": manual_net,
        "manual_fees_total": manual_fees_total,
        "manual_fee_breakdown": manual_breakdown,
        "gross_used": gross_used,
        "net_used": net_used,
        "source_used": source_used,
        "needs_manual_anchor": needs_manual_anchor,
        "currency": currency,
        "fees": fee_config.model_dump(),
        "computed_at": _now(),
    }


async def recompute_sell_anchor_for_group(
    db: AsyncIOMotorDatabase,
    user_id: str,
    group_id: str,
) -> Dict[str, Any]:
    gid = _parse_object_id(group_id)
    group = await repo.get_pricing_group(db, user_id, gid)
    if not group:
        raise BMClientError(f"pricing_group not found: {group_id}")

    settings_doc = await get_sell_anchor_settings_for_user(db, user_id)
    safe_ratio = float(settings_doc["safe_ratio"])
    fee_config = _fee_config_or_default(settings_doc.get("fee_config"))

    listing_ids = [
        str((l.get("bm_listing_id") or "")).strip()
        for l in (group.get("listings") or [])
        if l.get("bm_listing_id")
    ]
    max_price_map = await repo.get_sell_max_prices_for_listings(db, user_id, listing_ids)

    sell_anchor = _build_sell_anchor_for_group_doc(
        group,
        safe_ratio=safe_ratio,
        fee_config=fee_config,
        max_price_map=max_price_map,
    )

    await repo.update_pricing_group_sell_anchor(db, gid, sell_anchor=sell_anchor)
    logger.info(
        "[sell_anchor] group_updated user_id=%s group_id=%s used=%s gross=%s net=%s reason=%s",
        user_id,
        group_id,
        sell_anchor.get("source_used"),
        sell_anchor.get("gross_used"),
        sell_anchor.get("net_used"),
        sell_anchor.get("reason"),
    )
    return {"user_id": user_id, "group_id": group_id, "sell_anchor": sell_anchor}


async def recompute_sell_anchor_for_listing_ref(
    db: AsyncIOMotorDatabase,
    user_id: str,
    listing_ref: str,
) -> Dict[str, Any]:
    """
    Convenience helper used by backbox persistence flows.

    Finds the pricing_group containing this bm_listing_id, recomputes sell_anchor for that group,
    and persists it.

    Returns a small status payload.
    """
    listing_ref = str(listing_ref or "").strip()
    if not listing_ref:
        raise BMClientError("missing listing_ref")

    gid = await repo.find_group_id_for_listing_ref(db, user_id, listing_ref)
    if not gid:
        return {
            "user_id": user_id,
            "listing_ref": listing_ref,
            "group_id": None,
            "updated": False,
            "reason": "group_not_found_for_listing_ref",
        }

    res = await recompute_sell_anchor_for_group(db, user_id, str(gid))
    return {
        "user_id": user_id,
        "listing_ref": listing_ref,
        "group_id": res["group_id"],
        "updated": True,
        "sell_anchor": res["sell_anchor"],
    }


async def recompute_sell_anchors_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    groups_filter: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    max_parallel: int = 10,
) -> Dict[str, Any]:
    """
    Recompute & persist sell_anchor for all pricing_groups matching groups_filter.

    No group_id needed.
    """
    t0 = time.perf_counter()

    settings_doc = await get_sell_anchor_settings_for_user(db, user_id)
    safe_ratio = float(settings_doc["safe_ratio"])
    fee_config = _fee_config_or_default(settings_doc.get("fee_config"))

    q: Dict[str, Any] = {"user_id": user_id}
    if groups_filter:
        q.update(groups_filter)

    projection = {
        "_id": 1,
        "trade_sku": 1,
        "group_key": 1,
        "manual_sell_anchor_gross": 1,
        "listings": 1,
    }

    cur = db[repo.PRICING_GROUPS_COL].find(q, projection=projection)
    if limit is not None:
        cur = cur.limit(int(limit))

    groups: list[Dict[str, Any]] = []
    listing_ids_set: set[str] = set()

    async for group_doc in cur:
        groups.append(group_doc)
        for l in (group_doc.get("listings") or []):
            lid = str(l.get("bm_listing_id") or "").strip()
            if lid:
                listing_ids_set.add(lid)

    max_price_map = await repo.get_sell_max_prices_for_listings(db, user_id, list(listing_ids_set))

    sem = asyncio.Semaphore(max(1, int(max_parallel)))

    updated = 0
    failed = 0

    async def _one(group_doc: Dict[str, Any]) -> None:
        nonlocal updated, failed

        gid = group_doc.get("_id")
        if not isinstance(gid, ObjectId):
            failed += 1
            logger.error(
                "[sell_anchor] group_recompute_failed user_id=%s reason=missing_or_invalid__id _id=%r",
                user_id,
                gid,
            )
            return

        try:
            async with sem:
                sell_anchor = _build_sell_anchor_for_group_doc(
                    group_doc,
                    safe_ratio=safe_ratio,
                    fee_config=fee_config,
                    max_price_map=max_price_map,
                )
                await repo.update_pricing_group_sell_anchor(db, gid, sell_anchor=sell_anchor)
                updated += 1
        except (BMClientError, KeyError, TypeError, ValueError, PyMongoError) as exc:
            failed += 1
            logger.error(
                "[sell_anchor] group_recompute_failed user_id=%s group_id=%s err=%r",
                user_id,
                str(gid),
                exc,
            )

    await asyncio.gather(*[_one(group_doc) for group_doc in groups])

    elapsed = time.perf_counter() - t0
    logger.info(
        "[sell_anchor] user_recompute_done user_id=%s matched=%d updated=%d failed=%d elapsed=%.3fs",
        user_id,
        len(groups),
        updated,
        failed,
        elapsed,
    )

    return {
        "user_id": user_id,
        "matched_groups": len(groups),
        "updated": updated,
        "failed": failed,
        "elapsed_seconds": round(elapsed, 3),
    }




