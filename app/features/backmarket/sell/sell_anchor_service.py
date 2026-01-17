# app/features/backmarket/sell/sell_anchor_service.py
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
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

# Default minimum number of "safe" child prices required before accepting auto anchor.
# This is now configurable per-user via SellAnchorSettings.min_viable_child_prices, but
# we keep the constant for backwards-compatibility and for other modules that import it.
MIN_VIABLE_CHILD_PRICES = 5


def _as_dt(v: Any) -> Optional[datetime]:
    """Best-effort parse for datetimes stored in Mongo (datetime) or strings."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        s = str(v).strip()
    except Exception:
        return None
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _median(xs: list[float]) -> Optional[float]:
    if not xs:
        return None
    ys = sorted(float(x) for x in xs)
    n = len(ys)
    mid = n // 2
    if n % 2 == 1:
        return ys[mid]
    return (ys[mid - 1] + ys[mid]) / 2.0



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


def _extract_recent_best_price_to_win(
    listing: Dict[str, Any],
    *,
    now: datetime,
    lookback_days: int,
) -> Optional[float]:
    """Return the best (lowest) Backbox price_to_win within a recent time window.

    The current implementation of backbox persistence keeps:
      - `backbox_history`: up to the last 50 snapshots
      - `backbox_best_price_to_win`: the *lowest ever* snapshot

    We want to avoid anchoring off a single historic outlier, so this function first
    looks at the recent history window and only falls back to the older fields.
    """
    cutoff = now - timedelta(days=max(1, int(lookback_days)))
    hist = listing.get("backbox_history") or []
    prices: list[float] = []

    if isinstance(hist, list):
        for h in hist:
            if not isinstance(h, dict):
                continue
            ts = _as_dt(h.get("fetched_at") or h.get("ts") or h.get("timestamp") or h.get("at"))
            if ts is None or ts < cutoff:
                continue
            p = _to_pos_float(h.get("price_to_win"))
            if p is not None:
                prices.append(float(p))

    if prices:
        return min(prices)

    # Fallback to best-ever / latest snapshot.
    return _extract_best_price_to_win(listing)


def _extract_recent_sold_prices(
    listing: Dict[str, Any],
    *,
    now: datetime,
    lookback_days: int,
) -> list[dict]:
    """Extract recent sales from pricing_groups.listings[*].sold_history."""
    cutoff = now - timedelta(days=max(1, int(lookback_days)))
    out: list[dict] = []

    hist = listing.get("sold_history") or []
    if not isinstance(hist, list):
        return out

    for e in hist:
        if not isinstance(e, dict):
            continue
        sold_at = _as_dt(e.get("sold_at") or e.get("date_payment") or e.get("date_creation"))
        if sold_at is None or sold_at < cutoff:
            continue

        # Normalised by app/features/orders/pricing_groups_bridge.py
        price = e.get("unit_price")
        if price is None:
            price = e.get("price")
        if isinstance(price, dict):
            price = price.get("amount")

        p = _to_pos_float(price)
        if p is None:
            continue

        out.append(
            {
                "price": float(p),
                "sold_at": sold_at,
                "order_id": e.get("order_id"),
                "orderline_id": e.get("orderline_id"),
                "currency": e.get("currency"),
            }
        )

    return out


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
            "anchor_mode": s.anchor_mode,
            "safe_ratio": s.safe_ratio,
            "fee_config": s.fee_config.model_dump(),
            "recent_sold_days": s.recent_sold_days,
            "recent_sold_min_samples": s.recent_sold_min_samples,
            "backbox_lookback_days": s.backbox_lookback_days,
            "backbox_outlier_pct": s.backbox_outlier_pct,
            "min_viable_child_prices": s.min_viable_child_prices,
            "created_at": None,
            "updated_at": None,
        }

    anchor_mode = str(doc.get("anchor_mode") or DEFAULT_SETTINGS.anchor_mode).strip().lower()
    if anchor_mode not in {"manual_only", "auto"}:
        anchor_mode = DEFAULT_SETTINGS.anchor_mode

    safe_ratio = _to_float(doc.get("safe_ratio")) or DEFAULT_SETTINGS.safe_ratio
    fee_cfg = _fee_config_or_default(doc.get("fee_config"))

    recent_sold_days = int(doc.get("recent_sold_days") or DEFAULT_SETTINGS.recent_sold_days)
    recent_sold_min_samples = int(doc.get("recent_sold_min_samples") or DEFAULT_SETTINGS.recent_sold_min_samples)
    backbox_lookback_days = int(doc.get("backbox_lookback_days") or DEFAULT_SETTINGS.backbox_lookback_days)
    backbox_outlier_pct = _to_float(doc.get("backbox_outlier_pct"))
    if backbox_outlier_pct is None:
        backbox_outlier_pct = float(DEFAULT_SETTINGS.backbox_outlier_pct)
    min_viable_child_prices = int(doc.get("min_viable_child_prices") or DEFAULT_SETTINGS.min_viable_child_prices)

    settings = SellAnchorSettings(
        anchor_mode=anchor_mode,
        safe_ratio=safe_ratio,
        fee_config=fee_cfg,
        recent_sold_days=recent_sold_days,
        recent_sold_min_samples=recent_sold_min_samples,
        backbox_lookback_days=backbox_lookback_days,
        backbox_outlier_pct=backbox_outlier_pct,
        min_viable_child_prices=min_viable_child_prices,
    )

    return {
        "user_id": user_id,
        "anchor_mode": settings.anchor_mode,
        "safe_ratio": settings.safe_ratio,
        "fee_config": settings.fee_config.model_dump(),
        "recent_sold_days": settings.recent_sold_days,
        "recent_sold_min_samples": settings.recent_sold_min_samples,
        "backbox_lookback_days": settings.backbox_lookback_days,
        "backbox_outlier_pct": settings.backbox_outlier_pct,
        "min_viable_child_prices": settings.min_viable_child_prices,
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
            "anchor_mode": str(settings.anchor_mode),
            "safe_ratio": float(settings.safe_ratio),
            "fee_config": settings.fee_config.model_dump(),
            "recent_sold_days": int(settings.recent_sold_days),
            "recent_sold_min_samples": int(settings.recent_sold_min_samples),
            "backbox_lookback_days": int(settings.backbox_lookback_days),
            "backbox_outlier_pct": float(settings.backbox_outlier_pct),
            "min_viable_child_prices": int(settings.min_viable_child_prices),
        },
    )
    return {
        "user_id": user_id,
        "anchor_mode": str(saved.get("anchor_mode") or settings.anchor_mode),
        "safe_ratio": float(saved.get("safe_ratio") or settings.safe_ratio),
        "fee_config": (saved.get("fee_config") or settings.fee_config.model_dump()),
        "recent_sold_days": int(saved.get("recent_sold_days") or settings.recent_sold_days),
        "recent_sold_min_samples": int(saved.get("recent_sold_min_samples") or settings.recent_sold_min_samples),
        "backbox_lookback_days": int(saved.get("backbox_lookback_days") or settings.backbox_lookback_days),
        "backbox_outlier_pct": float(saved.get("backbox_outlier_pct") or settings.backbox_outlier_pct),
        "min_viable_child_prices": int(saved.get("min_viable_child_prices") or settings.min_viable_child_prices),
        "created_at": saved.get("created_at"),
        "updated_at": saved.get("updated_at"),
    }


def _build_sell_anchor_for_group_doc(
    group: Dict[str, Any],
    *,
    anchor_mode: str,
    safe_ratio: float,
    fee_config: FeeConfig,
    recent_sold_days: int,
    recent_sold_min_samples: int,
    backbox_lookback_days: int,
    backbox_outlier_pct: float,
    min_viable_child_prices: int,
) -> Dict[str, Any]:
    now = _now()
    listings = group.get("listings") or []

    # 0) Safest: min(max_price) across children
    group_max_price: Optional[float] = None
    for l in listings:
        mp = _to_pos_float(l.get("max_price"))
        if mp is None:
            continue
        if group_max_price is None or mp < group_max_price:
            group_max_price = mp

    safe_threshold: Optional[float] = None
    if group_max_price is not None:
        safe_threshold = group_max_price * float(safe_ratio)

    # 1) Orders / realised sales (highest weight)
    sold_entries: list[tuple[float, datetime, Dict[str, Any]]] = []
    sold_last_at: Optional[datetime] = None
    sold_last_listing: Optional[Dict[str, Any]] = None

    for l in listings:
        for e in _extract_recent_sold_prices(l, now=now, lookback_days=recent_sold_days):
            p = _to_pos_float(e.get("price"))
            dt = e.get("sold_at")
            if p is None or not isinstance(dt, datetime):
                continue
            sold_entries.append((float(p), dt, l))
            if sold_last_at is None or dt > sold_last_at:
                sold_last_at = dt
                sold_last_listing = l

    sold_recent_count = len(sold_entries)
    sold_recent_median_gross: Optional[float] = None
    sold_net: Optional[float] = None
    sold_fees_total: Optional[float] = None
    sold_fee_breakdown: list[dict] = []
    sold_viable = False

    if sold_recent_count >= max(1, int(recent_sold_min_samples)):
        sold_recent_median_gross = _median([p for (p, _dt, _l) in sold_entries])
        if sold_recent_median_gross is not None:
            sold_net, sold_fees_total, sold_fee_breakdown = _compute_net(sold_recent_median_gross, fee_config)
            sold_viable = bool(sold_net is not None and sold_net > 0)

    # 2) Backbox (auto) - use recent window, and ignore extreme low outliers
    backbox_candidates: list[tuple[float, Dict[str, Any]]] = []
    for l in listings:
        p = _extract_recent_best_price_to_win(l, now=now, lookback_days=backbox_lookback_days)
        if p is None:
            continue
        if safe_threshold is not None and p > safe_threshold:
            continue
        backbox_candidates.append((float(p), l))

    auto_viable_listings_count = len(backbox_candidates)

    auto_gross: Optional[float] = None
    source_listing: Optional[Dict[str, Any]] = None
    backbox_outlier_skipped = False
    backbox_median: Optional[float] = None
    backbox_threshold_low: Optional[float] = None

    if backbox_candidates:
        backbox_candidates.sort(key=lambda t: t[0])
        prices = [p for (p, _l) in backbox_candidates]
        backbox_median = _median(prices)
        if backbox_median is not None:
            backbox_threshold_low = backbox_median * (1.0 - max(0.0, float(backbox_outlier_pct)))

        # If we have "enough" viable prices, apply outlier skipping.
        chosen_price: Optional[float] = None
        chosen_listing: Optional[Dict[str, Any]] = None

        if (
            backbox_threshold_low is not None
            and float(backbox_outlier_pct) > 0
            and len(backbox_candidates) >= max(2, int(min_viable_child_prices))
        ):
            for p, l in backbox_candidates:
                if p >= backbox_threshold_low:
                    chosen_price = p
                    chosen_listing = l
                    break
            if chosen_price is None:
                # everything is below the threshold (rare); fall back to the min
                chosen_price, chosen_listing = backbox_candidates[0]
            if chosen_price != backbox_candidates[0][0]:
                backbox_outlier_skipped = True
        else:
            chosen_price, chosen_listing = backbox_candidates[0]

        auto_gross = chosen_price
        source_listing = chosen_listing

    currency = _extract_currency(group, source_listing or sold_last_listing, fee_config.currency)

    auto_net: Optional[float] = None
    auto_fees_total: Optional[float] = None
    auto_breakdown: list[dict] = []
    if auto_gross is not None:
        auto_net, auto_fees_total, auto_breakdown = _compute_net(auto_gross, fee_config)

    reason: Optional[str] = None
    auto_viable = False
    if auto_gross is None:
        reason = "missing_gross_anchor"
    elif group_max_price is None:
        reason = "missing_max_price"
    elif auto_viable_listings_count < int(min_viable_child_prices):
        reason = "insufficient_viable_prices"
    elif safe_threshold is not None and auto_gross > safe_threshold:
        reason = "gross_above_safe_ratio"
    elif auto_net is None or auto_net <= 0:
        reason = "net_non_positive"
    else:
        auto_viable = True

    # 3) Manual fallback
    manual_gross = _to_pos_float(group.get("manual_sell_anchor_gross"))
    manual_net: Optional[float] = None
    manual_fees_total: Optional[float] = None
    manual_breakdown: list[dict] = []
    if manual_gross is not None:
        manual_net, manual_fees_total, manual_breakdown = _compute_net(manual_gross, fee_config)

    # 4) Select which anchor to use
    gross_used: Optional[float] = None
    net_used: Optional[float] = None
    source_used: Optional[str] = None
    needs_manual_anchor = False
    used_listing: Optional[Dict[str, Any]] = None

    mode = str(anchor_mode or "").strip().lower()
    if mode not in {"manual_only", "auto"}:
        mode = "manual_only"

    if mode == "manual_only":
        # Safety-first: only use the manual anchor. If missing, mark group as requiring manual setup.
        if manual_gross is not None and manual_net is not None and manual_net > 0:
            gross_used = float(manual_gross)
            net_used = float(manual_net)
            source_used = "manual"
            used_listing = None
            reason = "manual_only"
        else:
            needs_manual_anchor = True
            source_used = "manual_only"
            reason = "manual_only_missing_manual_anchor"
    else:
        # Auto: realised sales > backbox-derived anchor > manual fallback
        if sold_viable and sold_recent_median_gross is not None and sold_net is not None and sold_net > 0:
            gross_used = float(sold_recent_median_gross)
            net_used = float(sold_net)
            source_used = "sold_recent"
            used_listing = sold_last_listing
            reason = "sold_recent"
        elif auto_viable and auto_gross is not None and auto_net is not None and auto_net > 0:
            gross_used = float(auto_gross)
            net_used = float(auto_net)
            source_used = "auto"
            used_listing = source_listing
        elif manual_gross is not None and manual_net is not None and manual_net > 0:
            gross_used = float(manual_gross)
            net_used = float(manual_net)
            source_used = "manual"
            used_listing = None
        else:
            needs_manual_anchor = True

    return {
        "anchor_mode": mode,
        # Backbox-derived fields (auto)
        "lowest_gross_price_to_win": auto_gross,
        "lowest_net_sell_price": auto_net,
        "auto_fees_total": auto_fees_total,
        "auto_fee_breakdown": auto_breakdown,
        "backbox_lookback_days": int(backbox_lookback_days),
        "backbox_outlier_pct": float(backbox_outlier_pct),
        "backbox_outlier_skipped": bool(backbox_outlier_skipped),
        "backbox_median": backbox_median,
        "backbox_threshold_low": backbox_threshold_low,

        # Orders / sales-derived fields
        "recent_sold_days": int(recent_sold_days),
        "recent_sold_min_samples": int(recent_sold_min_samples),
        "sold_recent_count": int(sold_recent_count),
        "sold_recent_median_gross": sold_recent_median_gross,
        "sold_recent_last_at": sold_last_at,
        "sold_net_sell_price": sold_net,
        "sold_fees_total": sold_fees_total,
        "sold_fee_breakdown": sold_fee_breakdown,
        "sold_viable": sold_viable,

        # Shared / selection
        "source_bm_listing_id": (used_listing or {}).get("bm_listing_id"),
        "source_full_sku": (used_listing or {}).get("full_sku"),
        "safe_ratio": float(safe_ratio),
        "safe_gross_threshold": safe_threshold,
        "auto_viable_listings_count": int(auto_viable_listings_count),
        "auto_viable_listings_min_required": int(min_viable_child_prices),
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
        "computed_at": now,
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

    recent_sold_days = int(settings_doc.get("recent_sold_days") or DEFAULT_SETTINGS.recent_sold_days)
    recent_sold_min_samples = int(
        settings_doc.get("recent_sold_min_samples") or DEFAULT_SETTINGS.recent_sold_min_samples
    )
    backbox_lookback_days = int(settings_doc.get("backbox_lookback_days") or DEFAULT_SETTINGS.backbox_lookback_days)
    backbox_outlier_pct = float(settings_doc.get("backbox_outlier_pct") or DEFAULT_SETTINGS.backbox_outlier_pct)
    min_viable_child_prices = int(
        settings_doc.get("min_viable_child_prices") or DEFAULT_SETTINGS.min_viable_child_prices
    )

    anchor_mode = str(settings_doc.get("anchor_mode") or DEFAULT_SETTINGS.anchor_mode)

    sell_anchor = _build_sell_anchor_for_group_doc(
        group,
        anchor_mode=anchor_mode,
        safe_ratio=safe_ratio,
        fee_config=fee_config,
        recent_sold_days=recent_sold_days,
        recent_sold_min_samples=recent_sold_min_samples,
        backbox_lookback_days=backbox_lookback_days,
        backbox_outlier_pct=backbox_outlier_pct,
        min_viable_child_prices=min_viable_child_prices,
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

    recent_sold_days = int(settings_doc.get("recent_sold_days") or DEFAULT_SETTINGS.recent_sold_days)
    recent_sold_min_samples = int(
        settings_doc.get("recent_sold_min_samples") or DEFAULT_SETTINGS.recent_sold_min_samples
    )
    backbox_lookback_days = int(settings_doc.get("backbox_lookback_days") or DEFAULT_SETTINGS.backbox_lookback_days)
    backbox_outlier_pct = float(settings_doc.get("backbox_outlier_pct") or DEFAULT_SETTINGS.backbox_outlier_pct)
    min_viable_child_prices = int(
        settings_doc.get("min_viable_child_prices") or DEFAULT_SETTINGS.min_viable_child_prices
    )

    anchor_mode = str(settings_doc.get("anchor_mode") or DEFAULT_SETTINGS.anchor_mode)

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

    async for group_doc in cur:
        groups.append(group_doc)

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
                    anchor_mode=anchor_mode,
                    safe_ratio=safe_ratio,
                    fee_config=fee_config,
                    recent_sold_days=recent_sold_days,
                    recent_sold_min_samples=recent_sold_min_samples,
                    backbox_lookback_days=backbox_lookback_days,
                    backbox_outlier_pct=backbox_outlier_pct,
                    min_viable_child_prices=min_viable_child_prices,
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





