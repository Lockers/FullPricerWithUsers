# app/features/backmarket/sell/sell_anchor_service.py
from __future__ import annotations

import asyncio
import logging
import statistics
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


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _confidence_label(conf: Optional[float]) -> Optional[str]:
    if conf is None:
        return None
    if conf >= 0.75:
        return "HIGH"
    if conf >= 0.50:
        return "MED"
    return "LOW"



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
    metric: str = "min",
    ewma_alpha: float = 0.30,
    fallback_to_best_ever: bool = True,
) -> Optional[float]:
    """Return a Backbox price_to_win anchor within a recent time window.

    Important:
    - When fallback_to_best_ever=False, we ONLY consider samples inside the lookback window.
      If there are no recent samples, we return None.
    - When fallback_to_best_ever=True, we fall back to the persisted best-ever/current fields.

    The DB schema typically has:
      - listings[*].backbox_history (last ~50 snapshots)
      - listings[*].backbox (latest snapshot)
      - listings[*].backbox_best_price_to_win (lowest-ever snapshot)
    """

    cutoff = now - timedelta(days=max(1, int(lookback_days)))

    samples: list[tuple[datetime, float]] = []

    # 1) Backbox history samples
    hist = listing.get("backbox_history") or []
    if isinstance(hist, list):
        for h in hist:
            if not isinstance(h, dict):
                continue
            ts = _as_dt(h.get("fetched_at") or h.get("ts") or h.get("timestamp") or h.get("at"))
            if ts is None or ts < cutoff:
                continue
            p = _to_pos_float(h.get("price_to_win"))
            if p is not None:
                samples.append((ts, float(p)))

    # 2) Current snapshot (if present + in-window)
    bb = listing.get("backbox") or {}
    if isinstance(bb, dict):
        ts = _as_dt(bb.get("fetched_at") or bb.get("ts") or bb.get("timestamp") or bb.get("at"))
        if ts is not None and ts >= cutoff:
            p = _to_pos_float(bb.get("price_to_win"))
            if p is not None:
                samples.append((ts, float(p)))

    if samples:
        metric_norm = (metric or "min").strip().lower()
        if metric_norm not in {"min", "median", "ewma"}:
            metric_norm = "min"

        # Sort by timestamp for EWMA.
        samples.sort(key=lambda x: x[0])
        prices = [p for _, p in samples]

        if metric_norm == "min":
            return float(min(prices))
        if metric_norm == "median":
            return float(statistics.median(prices))
        if metric_norm == "ewma":
            a = float(ewma_alpha)
            if not (0.0 < a < 1.0):
                a = 0.30
            ewma = prices[0]
            for p in prices[1:]:
                ewma = (a * p) + ((1.0 - a) * ewma)
            return float(ewma)

    if not fallback_to_best_ever:
        return None

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
            "backbox_metric": s.backbox_metric,
            "backbox_ewma_alpha": s.backbox_ewma_alpha,
            "min_viable_child_prices": s.min_viable_child_prices,
            "sold_weight": s.sold_weight,
            "backbox_weight": s.backbox_weight,
            "depreciation_weight": s.depreciation_weight,
            "baseline_cap_enabled": s.baseline_cap_enabled,
            "baseline_max_above_lowest_pct": s.baseline_max_above_lowest_pct,
            "require_manual_below_min_backboxes": s.require_manual_below_min_backboxes,
            "manual_baseline_weight": s.manual_baseline_weight,
            "manual_guardrail_mode": s.manual_guardrail_mode,
            "manual_guardrail_pct": s.manual_guardrail_pct,
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

    backbox_metric = str(doc.get("backbox_metric") or DEFAULT_SETTINGS.backbox_metric).strip().lower()
    if backbox_metric not in {"min", "median", "ewma"}:
        backbox_metric = DEFAULT_SETTINGS.backbox_metric

    backbox_ewma_alpha = _to_float(doc.get("backbox_ewma_alpha"))
    if backbox_ewma_alpha is None:
        backbox_ewma_alpha = float(DEFAULT_SETTINGS.backbox_ewma_alpha)

    sold_weight = _to_float(doc.get("sold_weight"))
    if sold_weight is None:
        sold_weight = float(DEFAULT_SETTINGS.sold_weight)

    backbox_weight = _to_float(doc.get("backbox_weight"))
    if backbox_weight is None:
        backbox_weight = float(DEFAULT_SETTINGS.backbox_weight)

    depreciation_weight = _to_float(doc.get("depreciation_weight"))
    if depreciation_weight is None:
        depreciation_weight = float(DEFAULT_SETTINGS.depreciation_weight)

    baseline_cap_enabled = doc.get("baseline_cap_enabled")
    if baseline_cap_enabled is None:
        baseline_cap_enabled = bool(DEFAULT_SETTINGS.baseline_cap_enabled)
    else:
        baseline_cap_enabled = bool(baseline_cap_enabled)

    baseline_max_above_lowest_pct = _to_float(doc.get("baseline_max_above_lowest_pct"))
    if baseline_max_above_lowest_pct is None:
        baseline_max_above_lowest_pct = float(DEFAULT_SETTINGS.baseline_max_above_lowest_pct)

    require_manual_below_min_backboxes = doc.get("require_manual_below_min_backboxes")
    if require_manual_below_min_backboxes is None:
        require_manual_below_min_backboxes = bool(DEFAULT_SETTINGS.require_manual_below_min_backboxes)
    else:
        require_manual_below_min_backboxes = bool(require_manual_below_min_backboxes)

    manual_baseline_weight = _to_float(doc.get("manual_baseline_weight"))
    if manual_baseline_weight is None:
        manual_baseline_weight = float(DEFAULT_SETTINGS.manual_baseline_weight)

    manual_guardrail_mode = str(doc.get("manual_guardrail_mode") or DEFAULT_SETTINGS.manual_guardrail_mode).strip().lower()
    if manual_guardrail_mode not in {"off", "clamp", "block"}:
        manual_guardrail_mode = str(DEFAULT_SETTINGS.manual_guardrail_mode)

    manual_guardrail_pct = _to_float(doc.get("manual_guardrail_pct"))
    if manual_guardrail_pct is None:
        manual_guardrail_pct = float(DEFAULT_SETTINGS.manual_guardrail_pct)


    settings = SellAnchorSettings(
        anchor_mode=anchor_mode,
        safe_ratio=safe_ratio,
        fee_config=fee_cfg,
        recent_sold_days=recent_sold_days,
        recent_sold_min_samples=recent_sold_min_samples,
        backbox_lookback_days=backbox_lookback_days,
        backbox_outlier_pct=backbox_outlier_pct,
        backbox_metric=backbox_metric,
        backbox_ewma_alpha=backbox_ewma_alpha,
        min_viable_child_prices=min_viable_child_prices,
        sold_weight=sold_weight,
        backbox_weight=backbox_weight,
        depreciation_weight=depreciation_weight,
        baseline_cap_enabled=baseline_cap_enabled,
        baseline_max_above_lowest_pct=baseline_max_above_lowest_pct,
        require_manual_below_min_backboxes=require_manual_below_min_backboxes,
        manual_baseline_weight=manual_baseline_weight,
        manual_guardrail_mode=manual_guardrail_mode,
        manual_guardrail_pct=manual_guardrail_pct,
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
        "backbox_metric": settings.backbox_metric,
        "backbox_ewma_alpha": settings.backbox_ewma_alpha,
        "min_viable_child_prices": settings.min_viable_child_prices,
        "sold_weight": settings.sold_weight,
        "backbox_weight": settings.backbox_weight,
        "depreciation_weight": settings.depreciation_weight,
        "baseline_cap_enabled": settings.baseline_cap_enabled,
        "baseline_max_above_lowest_pct": settings.baseline_max_above_lowest_pct,
        "require_manual_below_min_backboxes": settings.require_manual_below_min_backboxes,
        "manual_baseline_weight": settings.manual_baseline_weight,
        "manual_guardrail_mode": settings.manual_guardrail_mode,
        "manual_guardrail_pct": settings.manual_guardrail_pct,
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
            "backbox_metric": str(settings.backbox_metric),
            "backbox_ewma_alpha": float(settings.backbox_ewma_alpha),
            "min_viable_child_prices": int(settings.min_viable_child_prices),
            "sold_weight": float(settings.sold_weight),
            "backbox_weight": float(settings.backbox_weight),
            "depreciation_weight": float(settings.depreciation_weight),
            "baseline_cap_enabled": bool(settings.baseline_cap_enabled),
            "baseline_max_above_lowest_pct": float(settings.baseline_max_above_lowest_pct),
            "require_manual_below_min_backboxes": bool(settings.require_manual_below_min_backboxes),
            "manual_baseline_weight": float(settings.manual_baseline_weight),
            "manual_guardrail_mode": str(settings.manual_guardrail_mode),
            "manual_guardrail_pct": float(settings.manual_guardrail_pct),

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
        "backbox_metric": str(saved.get("backbox_metric") or settings.backbox_metric),
        "backbox_ewma_alpha": float(saved.get("backbox_ewma_alpha") or settings.backbox_ewma_alpha),
        "min_viable_child_prices": int(saved.get("min_viable_child_prices") or settings.min_viable_child_prices),
        "sold_weight": float(saved.get("sold_weight") or settings.sold_weight),
        "backbox_weight": float(saved.get("backbox_weight") or settings.backbox_weight),
        "depreciation_weight": float(saved.get("depreciation_weight") or settings.depreciation_weight),
        "baseline_cap_enabled": bool(saved.get("baseline_cap_enabled") if saved.get("baseline_cap_enabled") is not None else settings.baseline_cap_enabled),
        "baseline_max_above_lowest_pct": float(saved.get("baseline_max_above_lowest_pct") or settings.baseline_max_above_lowest_pct),
        "require_manual_below_min_backboxes": bool(saved.get("require_manual_below_min_backboxes") if saved.get("require_manual_below_min_backboxes") is not None else settings.require_manual_below_min_backboxes),
        "manual_baseline_weight": float(saved.get("manual_baseline_weight") or settings.manual_baseline_weight),
        "manual_guardrail_mode": str(saved.get("manual_guardrail_mode") or settings.manual_guardrail_mode),
        "manual_guardrail_pct": float(saved.get("manual_guardrail_pct") or settings.manual_guardrail_pct),
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
    backbox_metric: str,
    backbox_ewma_alpha: float,
    min_viable_child_prices: int,
    # Blend strategy
    sold_weight: float,
    backbox_weight: float,
    depreciation_weight: float,
    # Baseline guard
    baseline_cap_enabled: bool,
    baseline_max_above_lowest_pct: float,
    # Manual baseline/guardrail
    require_manual_below_min_backboxes: bool,
    manual_baseline_weight: float,
    manual_guardrail_mode: str,
    manual_guardrail_pct: float,
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
            group_max_price = float(mp)

    safe_threshold: Optional[float] = None
    if group_max_price is not None:
        safe_threshold = float(group_max_price) * float(safe_ratio)

    # 1) Orders / realised sales (high weight)
    sold_entries: list[tuple[float, datetime, Dict[str, Any]]] = []
    sold_last_at: Optional[datetime] = None
    sold_last_listing: Optional[Dict[str, Any]] = None

    sold_lowest_ever_gross: Optional[float] = None
    sold_ever_count = 0

    for l in listings:
        # Baseline: lowest ever sold (all history)
        hist_all = l.get("sold_history") or []
        if isinstance(hist_all, list):
            for e in hist_all:
                if not isinstance(e, dict):
                    continue
                p_all = _to_pos_float(e.get("price"))
                if p_all is None:
                    continue
                sold_ever_count += 1
                if sold_lowest_ever_gross is None or float(p_all) < sold_lowest_ever_gross:
                    sold_lowest_ever_gross = float(p_all)

        # Recent sold entries for the primary sold signal
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

    if sold_recent_count >= int(max(1, recent_sold_min_samples)):
        sold_recent_median_gross = _median([p for p, _, _ in sold_entries])

    if sold_recent_median_gross is not None:
        sold_net, sold_fees_total, sold_fee_breakdown = _compute_net(sold_recent_median_gross, fee_config)

    sold_viable = bool(sold_recent_median_gross is not None and sold_net is not None and sold_net > 0)

    # 2) Backbox: compute a per-listing signal within the recent window.
    bb_cutoff = now - timedelta(days=max(1, int(backbox_lookback_days)))

    def _lowest_ever_backbox(listing: Dict[str, Any]) -> Optional[float]:
        # Prefer persisted lowest-ever.
        p = _to_pos_float(listing.get("backbox_best_price_to_win"))
        if p is not None:
            return float(p)

        # Fallback: compute min from stored history/current snapshot.
        best: Optional[float] = None
        hist = listing.get("backbox_history") or []
        if isinstance(hist, list):
            for h in hist:
                if not isinstance(h, dict):
                    continue
                ph = _to_pos_float(h.get("price_to_win"))
                if ph is None:
                    continue
                if best is None or float(ph) < best:
                    best = float(ph)
        bb = listing.get("backbox") or {}
        if isinstance(bb, dict):
            pb = _to_pos_float(bb.get("price_to_win"))
            if pb is not None:
                if best is None or float(pb) < best:
                    best = float(pb)
        return best

    backbox_lowest_ever_gross: Optional[float] = None

    backbox_candidates: list[tuple[float, Dict[str, Any]]] = []

    # Stats within the lookback window
    backbox_samples_total = 0
    backbox_last_at: Optional[datetime] = None

    for l in listings:
        # Baseline: lowest ever backbox
        pever = _lowest_ever_backbox(l)
        if pever is not None:
            if backbox_lowest_ever_gross is None or float(pever) < backbox_lowest_ever_gross:
                backbox_lowest_ever_gross = float(pever)

        # Recent Backbox signal (no fallback)
        p = _extract_recent_best_price_to_win(
            l,
            now=now,
            lookback_days=int(backbox_lookback_days),
            metric=str(backbox_metric),
            ewma_alpha=float(backbox_ewma_alpha),
            fallback_to_best_ever=False,
        )
        if p is not None:
            p = float(p)
            if safe_threshold is not None and p > float(safe_threshold):
                # Ignore prices that exceed the safety threshold.
                pass
            else:
                backbox_candidates.append((p, l))

        # Backbox sample stats
        hist = l.get("backbox_history") or []
        if isinstance(hist, list):
            for h in hist:
                if not isinstance(h, dict):
                    continue
                dt = _as_dt(h.get("fetched_at") or h.get("ts") or h.get("timestamp") or h.get("at"))
                if dt is None or dt < bb_cutoff:
                    continue
                ph = _to_pos_float(h.get("price_to_win"))
                if ph is None:
                    continue
                backbox_samples_total += 1
                if backbox_last_at is None or dt > backbox_last_at:
                    backbox_last_at = dt

        bb = l.get("backbox") or {}
        if isinstance(bb, dict):
            dt = _as_dt(bb.get("fetched_at") or bb.get("ts") or bb.get("timestamp") or bb.get("at"))
            if dt is not None and dt >= bb_cutoff:
                ph = _to_pos_float(bb.get("price_to_win"))
                if ph is not None:
                    backbox_samples_total += 1
                    if backbox_last_at is None or dt > backbox_last_at:
                        backbox_last_at = dt

    auto_viable_listings_count = len(backbox_candidates)

    backbox_prices = [p for p, _ in backbox_candidates]
    backbox_current_min: Optional[float] = min(backbox_prices) if backbox_prices else None
    backbox_median: Optional[float] = _median(backbox_prices)
    backbox_current_max: Optional[float] = max(backbox_prices) if backbox_prices else None

    # Legacy-ish behaviour: pick a "lowest" robust price, optionally skipping extreme low outliers.
    threshold_low: Optional[float] = None
    backbox_outlier_skipped = False
    auto_gross: Optional[float] = None
    source_listing: Optional[Dict[str, Any]] = None

    if backbox_median is not None:
        threshold_low = float(backbox_median) * (1.0 - float(backbox_outlier_pct))

    if backbox_candidates:
        backbox_candidates_sorted = sorted(backbox_candidates, key=lambda x: x[0])
        auto_gross, source_listing = backbox_candidates_sorted[0]
        if threshold_low is not None and auto_gross is not None and auto_gross < float(threshold_low) and len(backbox_candidates_sorted) > 1:
            # pick the first price above the threshold
            for p, l in backbox_candidates_sorted[1:]:
                if p >= float(threshold_low):
                    auto_gross, source_listing = p, l
                    backbox_outlier_skipped = True
                    break

    auto_net: Optional[float] = None
    auto_fees_total: Optional[float] = None
    auto_fee_breakdown: list[dict] = []
    if auto_gross is not None:
        auto_net, auto_fees_total, auto_fee_breakdown = _compute_net(auto_gross, fee_config)

    auto_viable = bool(
        auto_gross is not None
        and auto_net is not None
        and auto_net > 0
        and (safe_threshold is None or float(auto_gross) <= float(safe_threshold))
        and auto_viable_listings_count >= int(max(1, min_viable_child_prices))
    )

    # 3) Depreciation (low weight)
    dep_anchor = group.get("depreciation_anchor") or {}
    dep_gross = _to_pos_float(
        dep_anchor.get("predicted_target_gross")
        or dep_anchor.get("predicted_target")
        or dep_anchor.get("target_gross")
        or dep_anchor.get("gross")
    )

    dep_net: Optional[float] = None
    dep_fees_total: Optional[float] = None
    dep_breakdown: list[dict] = []
    dep_viable = False
    if dep_gross is not None:
        dep_net, dep_fees_total, dep_breakdown = _compute_net(dep_gross, fee_config)
        dep_viable = bool(dep_net is not None and dep_net > 0)

    # 4) Manual anchor
    manual_gross = _to_pos_float(group.get("manual_sell_anchor_gross"))
    manual_net: Optional[float] = None
    manual_fees_total: Optional[float] = None
    manual_breakdown: list[dict] = []
    if manual_gross is not None:
        manual_net, manual_fees_total, manual_breakdown = _compute_net(manual_gross, fee_config)

    # 5) Currency
    currency = _extract_currency(group, source_listing or sold_last_listing, fee_config.currency)

    # 6) Confidence inputs
    sold_full_n = max(5, int(recent_sold_min_samples))
    conf_sold = _clamp01(float(sold_recent_count) / float(sold_full_n)) if sold_recent_count else 0.0

    conf_backbox_listings = _clamp01(float(auto_viable_listings_count) / float(max(1, int(min_viable_child_prices))))
    expected_samples = max(1.0, float(max(1, int(auto_viable_listings_count))) * 5.0)
    conf_backbox_samples = _clamp01(float(backbox_samples_total) / expected_samples) if backbox_samples_total else 0.0
    conf_backbox = conf_backbox_listings * max(0.5, conf_backbox_samples)

    conf_dep = 1.0 if dep_viable else 0.0

    # Agreement penalty (when multiple viable signals exist)
    viable_gross_signals: list[float] = []
    if sold_viable and sold_recent_median_gross is not None:
        viable_gross_signals.append(float(sold_recent_median_gross))
    if backbox_median is not None:
        viable_gross_signals.append(float(backbox_median))
    if dep_viable and dep_gross is not None:
        viable_gross_signals.append(float(dep_gross))

    spread_pct: Optional[float] = None
    penalty = 1.0
    if len(viable_gross_signals) >= 2:
        mx = max(viable_gross_signals)
        mn = min(viable_gross_signals)
        if mx > 0:
            spread_pct = (mx - mn) / mx
            if spread_pct > 0.40:
                penalty = 0.70
            elif spread_pct > 0.25:
                penalty = 0.85

    # 7) Baseline guard
    baseline_lowest_ever_gross: Optional[float] = None
    baseline_lowest_ever_source: Optional[str] = None

    # Consider both sold lowest ever and backbox lowest ever.
    for src, val in (
        ("backbox_lowest_ever", backbox_lowest_ever_gross),
        ("sold_lowest_ever", sold_lowest_ever_gross),
    ):
        if val is None:
            continue
        if baseline_lowest_ever_gross is None or float(val) < baseline_lowest_ever_gross:
            baseline_lowest_ever_gross = float(val)
            baseline_lowest_ever_source = src

    baseline_cap_gross: Optional[float] = None
    if baseline_cap_enabled and baseline_lowest_ever_gross is not None:
        baseline_cap_gross = float(baseline_lowest_ever_gross) * (1.0 + float(baseline_max_above_lowest_pct))

    def _apply_cap(gross: Optional[float]) -> tuple[Optional[float], bool]:
        if gross is None or baseline_cap_gross is None:
            return gross, False
        if float(gross) > float(baseline_cap_gross):
            return float(baseline_cap_gross), True
        return gross, False

    # 8) Suggested anchor (blend of available signals)
    signals: dict[str, float] = {}
    confs: dict[str, float] = {}
    base_w: dict[str, float] = {}

    if sold_viable and sold_recent_median_gross is not None and float(sold_weight) > 0.0:
        signals["sold_recent"] = float(sold_recent_median_gross)
        confs["sold_recent"] = float(conf_sold)
        base_w["sold_recent"] = float(sold_weight)

    if backbox_median is not None and float(backbox_weight) > 0.0:
        signals["backbox_median"] = float(backbox_median)
        confs["backbox_median"] = float(conf_backbox)
        base_w["backbox_median"] = float(backbox_weight)

    if dep_viable and dep_gross is not None and float(depreciation_weight) > 0.0:
        signals["depreciation"] = float(dep_gross)
        confs["depreciation"] = float(conf_dep)
        base_w["depreciation"] = float(depreciation_weight)

    eff_w: dict[str, float] = {k: float(base_w[k]) * float(confs.get(k, 0.0)) for k in base_w}
    sum_eff = sum(eff_w.values())

    suggested_gross_raw: Optional[float] = None
    suggested_source: Optional[str] = None
    blend_norm_w: dict[str, float] = {}

    if sum_eff > 0.0:
        blend_norm_w = {k: (eff_w[k] / sum_eff) for k in eff_w}
        suggested_gross_raw = 0.0
        for k, w in blend_norm_w.items():
            suggested_gross_raw += float(w) * float(signals[k])

        # Source label
        active = [k for k, w in blend_norm_w.items() if w > 0.0]
        if len(active) >= 2:
            suggested_source = "blend"
        elif len(active) == 1:
            suggested_source = active[0]

    suggested_gross: Optional[float] = None
    suggested_net: Optional[float] = None
    suggested_cap_applied = False

    if suggested_gross_raw is not None:
        suggested_gross, suggested_cap_applied = _apply_cap(round(float(suggested_gross_raw), 2))
        if suggested_gross is not None:
            suggested_net, _, _ = _compute_net(suggested_gross, fee_config)

    # Suggested confidence uses base weights across available signals (not effective weights), then applies penalty.
    suggested_confidence: Optional[float] = None
    if suggested_gross is not None and base_w:
        bw_sum = sum(base_w.values())
        if bw_sum > 0.0:
            bw_norm = {k: (base_w[k] / bw_sum) for k in base_w}
            suggested_confidence = 0.0
            for k, w in bw_norm.items():
                suggested_confidence += float(w) * float(confs.get(k, 0.0))
            suggested_confidence = _clamp01(float(suggested_confidence) * float(penalty))

    suggested_confidence_label = _confidence_label(suggested_confidence)

    # 9) Determine used anchor
    needs_manual_anchor = False
    gross_used: Optional[float] = None
    net_used: Optional[float] = None
    source_used: Optional[str] = None
    reason: str

    manual_required = bool(require_manual_below_min_backboxes and auto_viable_listings_count < int(max(1, min_viable_child_prices)))

    if str(anchor_mode).strip() == "manual_only":
        if manual_gross is None:
            needs_manual_anchor = True
            reason = "manual_only_missing_manual_anchor"
            source_used = "manual_only"
        else:
            gross_used = float(manual_gross)
            source_used = "manual"
            reason = "manual_only"

    else:
        # Auto mode
        if manual_required:
            if manual_gross is None:
                needs_manual_anchor = True
                reason = "manual_required_below_min_backboxes"
                source_used = "manual_required"
            else:
                # Manual baseline dominates when backbox coverage is sparse.
                base = float(manual_baseline_weight)
                base = 0.0 if base < 0.0 else (1.0 if base > 1.0 else base)
                other = float(suggested_gross) if suggested_gross is not None else float(manual_gross)
                gross_used = (base * float(manual_gross)) + ((1.0 - base) * other)
                source_used = "manual_baseline"
                reason = "manual_baseline_below_min_backboxes"
        else:
            if suggested_gross is not None:
                gross_used = float(suggested_gross)
                source_used = str(suggested_source or "blend")
                reason = f"auto_{source_used}"
            elif manual_gross is not None:
                gross_used = float(manual_gross)
                source_used = "manual_fallback"
                reason = "manual_fallback_no_signals"
            else:
                needs_manual_anchor = True
                reason = "missing_signals_needs_manual_anchor"
                source_used = None

    # Manual guardrail (optional) - only applies in auto-ish sources.
    manual_guardrail_applied = False
    manual_guardrail_action: Optional[str] = None
    manual_guardrail_low: Optional[float] = None
    manual_guardrail_high: Optional[float] = None

    if (
        manual_gross is not None
        and gross_used is not None
        and str(anchor_mode).strip() != "manual_only"
        and str(manual_guardrail_mode or "off").strip().lower() in {"clamp", "block"}
        and source_used not in {"manual", "manual_fallback", "manual_baseline"}
    ):
        pct = float(manual_guardrail_pct)
        if pct < 0.0:
            pct = 0.0
        manual_guardrail_low = float(manual_gross) * (1.0 - pct)
        manual_guardrail_high = float(manual_gross) * (1.0 + pct)

        if gross_used < manual_guardrail_low or gross_used > manual_guardrail_high:
            manual_guardrail_applied = True
            mode = str(manual_guardrail_mode).strip().lower()
            if mode == "block":
                gross_used = float(manual_gross)
                source_used = "manual_guardrail_block"
                manual_guardrail_action = "blocked"
            else:
                # clamp
                gross_used = min(max(gross_used, manual_guardrail_low), manual_guardrail_high)
                manual_guardrail_action = "clamped"

    # Baseline cap applied to used auto-derived anchors (not to explicit manual-only/manual-fallback/manual-baseline).
    used_cap_applied = False
    if baseline_cap_gross is not None and gross_used is not None:
        if source_used not in {"manual", "manual_only", "manual_fallback", "manual_baseline", "manual_guardrail_block"}:
            gross_used_capped, used_cap_applied = _apply_cap(float(gross_used))
            gross_used = gross_used_capped

    if gross_used is not None:
        net_used, _, _ = _compute_net(float(gross_used), fee_config)

    # 10) Choose a source listing for UI/debug
    used_listing: Optional[Dict[str, Any]] = None
    if source_used == "sold_recent":
        used_listing = sold_last_listing
    elif source_used in {"backbox_median", "backbox"}:
        # Listing closest to the median
        if backbox_candidates and backbox_median is not None:
            used_listing = min(backbox_candidates, key=lambda x: abs(float(x[0]) - float(backbox_median)))[1]

    # 11) Manual-vs-suggested diagnostics
    manual_delta_gross: Optional[float] = None
    manual_delta_pct: Optional[float] = None
    manual_far_from_suggested = False
    if manual_gross is not None and suggested_gross is not None and suggested_source not in {None, "manual"}:
        try:
            manual_delta_gross = abs(float(manual_gross) - float(suggested_gross))
            if float(suggested_gross) > 0:
                manual_delta_pct = float(manual_delta_gross) / float(suggested_gross)
                manual_far_from_suggested = bool(manual_delta_pct > 0.25)
        except Exception:
            manual_delta_gross = None
            manual_delta_pct = None
            manual_far_from_suggested = False

    # Used confidence
    used_confidence: Optional[float] = None
    if not needs_manual_anchor and gross_used is not None:
        if source_used in {"manual", "manual_fallback", "manual_guardrail_block"}:
            used_confidence = 0.85
            if manual_far_from_suggested:
                used_confidence = min(float(used_confidence), 0.55)
        elif source_used == "manual_baseline":
            base = float(manual_baseline_weight)
            base = 0.0 if base < 0.0 else (1.0 if base > 1.0 else base)
            used_confidence = (base * 0.85) + ((1.0 - base) * float(suggested_confidence or 0.0))
            used_confidence = _clamp01(float(used_confidence))
        else:
            used_confidence = suggested_confidence

    used_confidence_label = _confidence_label(used_confidence)

    # 12) Backbox + sold fee breakdowns for transparency
    auto_net_sell_price = auto_net
    lowest_net_sell_price = auto_net

    return {
        # Strategy
        "anchor_mode": str(anchor_mode),

        # Backbox "low" anchor (legacy-friendly)
        "lowest_gross_price_to_win": auto_gross,
        "lowest_net_sell_price": lowest_net_sell_price,
        "auto_fees_total": auto_fees_total,
        "auto_fee_breakdown": auto_fee_breakdown,

        # Backbox stats
        "backbox_lookback_days": int(backbox_lookback_days),
        "backbox_outlier_pct": float(backbox_outlier_pct),
        "backbox_outlier_skipped": bool(backbox_outlier_skipped),
        "backbox_median": backbox_median,
        "backbox_threshold_low": threshold_low,
        "backbox_current_min_gross": backbox_current_min,
        "backbox_current_max_gross": backbox_current_max,
        "backbox_lowest_ever_gross": backbox_lowest_ever_gross,

        # Sold stats
        "recent_sold_days": int(recent_sold_days),
        "recent_sold_min_samples": int(recent_sold_min_samples),
        "sold_recent_count": int(sold_recent_count),
        "sold_recent_median_gross": sold_recent_median_gross,
        "sold_recent_last_at": sold_last_at,
        "sold_net_sell_price": sold_net,
        "sold_fees_total": sold_fees_total,
        "sold_fee_breakdown": sold_fee_breakdown,
        "sold_viable": sold_viable,
        "sold_lowest_ever_gross": sold_lowest_ever_gross,
        "sold_ever_count": int(sold_ever_count),

        # Baseline guard
        "baseline_cap_enabled": bool(baseline_cap_enabled),
        "baseline_lowest_ever_gross": baseline_lowest_ever_gross,
        "baseline_lowest_ever_source": baseline_lowest_ever_source,
        "baseline_cap_gross": baseline_cap_gross,
        "baseline_cap_applied_to_suggested": bool(suggested_cap_applied),
        "baseline_cap_applied_to_used": bool(used_cap_applied),

        # Manual baseline/guardrail settings (for UI)
        "require_manual_below_min_backboxes": bool(require_manual_below_min_backboxes),
        "manual_required_below_min_backboxes": bool(manual_required),
        "manual_baseline_weight": float(manual_baseline_weight),
        "manual_guardrail_mode": str(manual_guardrail_mode),
        "manual_guardrail_pct": float(manual_guardrail_pct),
        "manual_guardrail_applied": bool(manual_guardrail_applied),
        "manual_guardrail_action": manual_guardrail_action,
        "manual_guardrail_low": manual_guardrail_low,
        "manual_guardrail_high": manual_guardrail_high,

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

        # Blend details (for UI/debug)
        "blend": {
            "signals": signals,
            "base_weights": base_w,
            "confidences": {k: round(float(confs.get(k, 0.0)), 4) for k in confs},
            "effective_weights": {k: round(float(eff_w.get(k, 0.0)), 6) for k in eff_w},
            "normalized_weights": {k: round(float(blend_norm_w.get(k, 0.0)), 6) for k in blend_norm_w},
        },

        # Suggested (best available) anchor, independent of anchor_mode.
        "suggested_source": suggested_source,
        "suggested_gross": suggested_gross,
        "suggested_net": suggested_net,
        "suggested_confidence": suggested_confidence,
        "suggested_confidence_label": suggested_confidence_label,

        # Confidence for the anchor actually used (None when not used / blocked)
        "confidence": used_confidence,
        "confidence_label": used_confidence_label,
        "confidence_spread_pct": spread_pct,
        "confidence_penalty": penalty,
        "confidence_components": {
            "sold": round(float(conf_sold), 4),
            "backbox": round(float(conf_backbox), 4),
            "depreciation": float(conf_dep),
            "backbox_samples_total": int(backbox_samples_total),
        },
        "backbox_samples_total": int(backbox_samples_total),
        "backbox_last_at": backbox_last_at,

        # Manual-vs-suggested diagnostics
        "manual_delta_gross": manual_delta_gross,
        "manual_delta_pct": manual_delta_pct,
        "manual_far_from_suggested": bool(manual_far_from_suggested),

        "manual_sell_anchor_gross": manual_gross,
        "manual_net_sell_price": manual_net,
        "manual_fees_total": manual_fees_total,
        "manual_fee_breakdown": manual_breakdown,

        # Depreciation fields
        "depreciation_predicted_target_gross": dep_gross,
        "depreciation_net_sell_price": dep_net,
        "depreciation_fees_total": dep_fees_total,
        "depreciation_fee_breakdown": dep_breakdown,
        "depreciation_viable": bool(dep_viable),
        "depreciation_computed_at": dep_anchor.get("computed_at"),
        "depreciation_as_of_date": dep_anchor.get("as_of_date"),
        "depreciation_curve_version": dep_anchor.get("curve_version"),
        "depreciation_multiplier_source": dep_anchor.get("multiplier_source"),

        # Final used
        "gross_used": gross_used,
        "net_used": net_used,
        "source_used": source_used,
        "needs_manual_anchor": needs_manual_anchor,
        "currency": currency,
        "fees": fee_config.model_dump(),
        "computed_at": now,

        # Strategy weights echoed
        "sold_weight": float(sold_weight),
        "backbox_weight": float(backbox_weight),
        "depreciation_weight": float(depreciation_weight),
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
    backbox_metric = str(settings_doc.get("backbox_metric") or DEFAULT_SETTINGS.backbox_metric).strip().lower()
    if backbox_metric not in {"min", "median", "ewma"}:
        backbox_metric = DEFAULT_SETTINGS.backbox_metric
    backbox_ewma_alpha = float(settings_doc.get("backbox_ewma_alpha") or DEFAULT_SETTINGS.backbox_ewma_alpha)
    min_viable_child_prices = int(
        settings_doc.get("min_viable_child_prices") or DEFAULT_SETTINGS.min_viable_child_prices
    )


    sold_weight = float(settings_doc.get("sold_weight") or DEFAULT_SETTINGS.sold_weight)
    backbox_weight = float(settings_doc.get("backbox_weight") or DEFAULT_SETTINGS.backbox_weight)
    depreciation_weight = float(settings_doc.get("depreciation_weight") or DEFAULT_SETTINGS.depreciation_weight)

    baseline_cap_enabled = (
        bool(settings_doc.get("baseline_cap_enabled"))
        if settings_doc.get("baseline_cap_enabled") is not None
        else DEFAULT_SETTINGS.baseline_cap_enabled
    )
    baseline_max_above_lowest_pct = float(
        settings_doc.get("baseline_max_above_lowest_pct") or DEFAULT_SETTINGS.baseline_max_above_lowest_pct
    )

    require_manual_below_min_backboxes = (
        bool(settings_doc.get("require_manual_below_min_backboxes"))
        if settings_doc.get("require_manual_below_min_backboxes") is not None
        else DEFAULT_SETTINGS.require_manual_below_min_backboxes
    )
    manual_baseline_weight = float(
        settings_doc.get("manual_baseline_weight") or DEFAULT_SETTINGS.manual_baseline_weight
    )

    manual_guardrail_mode = str(
        settings_doc.get("manual_guardrail_mode") or DEFAULT_SETTINGS.manual_guardrail_mode
    ).strip().lower()
    if manual_guardrail_mode not in {"off", "clamp", "block"}:
        manual_guardrail_mode = DEFAULT_SETTINGS.manual_guardrail_mode
    manual_guardrail_pct = float(settings_doc.get("manual_guardrail_pct") or DEFAULT_SETTINGS.manual_guardrail_pct)

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
        backbox_metric=backbox_metric,
        backbox_ewma_alpha=backbox_ewma_alpha,
        min_viable_child_prices=min_viable_child_prices,
                    sold_weight=sold_weight,
                    backbox_weight=backbox_weight,
                    depreciation_weight=depreciation_weight,
                    baseline_cap_enabled=baseline_cap_enabled,
                    baseline_max_above_lowest_pct=baseline_max_above_lowest_pct,
                    require_manual_below_min_backboxes=require_manual_below_min_backboxes,
                    manual_baseline_weight=manual_baseline_weight,
                    manual_guardrail_mode=manual_guardrail_mode,
                    manual_guardrail_pct=manual_guardrail_pct,
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
    backbox_metric = str(settings_doc.get("backbox_metric") or DEFAULT_SETTINGS.backbox_metric).strip().lower()
    if backbox_metric not in {"min", "median", "ewma"}:
        backbox_metric = DEFAULT_SETTINGS.backbox_metric
    backbox_ewma_alpha = float(settings_doc.get("backbox_ewma_alpha") or DEFAULT_SETTINGS.backbox_ewma_alpha)
    min_viable_child_prices = int(
        settings_doc.get("min_viable_child_prices") or DEFAULT_SETTINGS.min_viable_child_prices
    )


    sold_weight = float(settings_doc.get("sold_weight") or DEFAULT_SETTINGS.sold_weight)
    backbox_weight = float(settings_doc.get("backbox_weight") or DEFAULT_SETTINGS.backbox_weight)
    depreciation_weight = float(settings_doc.get("depreciation_weight") or DEFAULT_SETTINGS.depreciation_weight)

    baseline_cap_enabled = (
        bool(settings_doc.get("baseline_cap_enabled"))
        if settings_doc.get("baseline_cap_enabled") is not None
        else DEFAULT_SETTINGS.baseline_cap_enabled
    )
    baseline_max_above_lowest_pct = float(
        settings_doc.get("baseline_max_above_lowest_pct") or DEFAULT_SETTINGS.baseline_max_above_lowest_pct
    )

    require_manual_below_min_backboxes = (
        bool(settings_doc.get("require_manual_below_min_backboxes"))
        if settings_doc.get("require_manual_below_min_backboxes") is not None
        else DEFAULT_SETTINGS.require_manual_below_min_backboxes
    )
    manual_baseline_weight = float(
        settings_doc.get("manual_baseline_weight") or DEFAULT_SETTINGS.manual_baseline_weight
    )

    manual_guardrail_mode = str(
        settings_doc.get("manual_guardrail_mode") or DEFAULT_SETTINGS.manual_guardrail_mode
    ).strip().lower()
    if manual_guardrail_mode not in {"off", "clamp", "block"}:
        manual_guardrail_mode = DEFAULT_SETTINGS.manual_guardrail_mode
    manual_guardrail_pct = float(settings_doc.get("manual_guardrail_pct") or DEFAULT_SETTINGS.manual_guardrail_pct)

    anchor_mode = str(settings_doc.get("anchor_mode") or DEFAULT_SETTINGS.anchor_mode)

    q: Dict[str, Any] = {"user_id": user_id}
    if groups_filter:
        q.update(groups_filter)

    projection = {
        "_id": 1,
        "trade_sku": 1,
        "group_key": 1,
        "manual_sell_anchor_gross": 1,
        "depreciation_anchor": 1,
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
                    backbox_metric=backbox_metric,
                    backbox_ewma_alpha=backbox_ewma_alpha,
                    min_viable_child_prices=min_viable_child_prices,
                    sold_weight=sold_weight,
                    backbox_weight=backbox_weight,
                    depreciation_weight=depreciation_weight,
                    baseline_cap_enabled=baseline_cap_enabled,
                    baseline_max_above_lowest_pct=baseline_max_above_lowest_pct,
                    require_manual_below_min_backboxes=require_manual_below_min_backboxes,
                    manual_baseline_weight=manual_baseline_weight,
                    manual_guardrail_mode=manual_guardrail_mode,
                    manual_guardrail_pct=manual_guardrail_pct,
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





