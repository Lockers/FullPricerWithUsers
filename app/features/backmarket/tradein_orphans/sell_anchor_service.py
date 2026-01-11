from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.errors import NotFoundError
from app.features.backmarket.sell.sell_anchor_models import FeeConfig
from app.features.backmarket.sell.sell_anchor_service import (
    DEFAULT_FEE_CONFIG,
    MIN_VIABLE_CHILD_PRICES,
    REQUIRED_FEE_KEYS,
    get_sell_anchor_settings_for_user,
)
from app.features.backmarket.tradein_orphans.repo import TradeinOrphansRepo


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fee_config_or_default(raw: Any) -> FeeConfig:
    """Parse/validate fee_config similarly to sell_anchor_service.

    We avoid importing private helpers from sell_anchor_service.
    """
    if not raw:
        return DEFAULT_FEE_CONFIG
    try:
        cfg = raw if isinstance(raw, FeeConfig) else FeeConfig(**raw)
    except Exception:  # noqa: BLE001
        return DEFAULT_FEE_CONFIG

    keys = {str(i.key).strip() for i in (cfg.items or []) if getattr(i, "key", None)}
    if not REQUIRED_FEE_KEYS.issubset(keys):
        return DEFAULT_FEE_CONFIG

    return cfg


def _compute_net(gross: float, fee_config: FeeConfig) -> Tuple[float, float, List[Dict[str, Any]]]:
    """Return (net, fees_total, breakdown) for a gross price."""
    total = 0.0
    breakdown: List[Dict[str, Any]] = []

    for item in (fee_config.items or []):
        t = str(item.type or "").strip()
        if t == "percent":
            rate = float(item.rate or 0.0)
            fee = round(float(gross) * rate, 2)
            breakdown.append({"key": item.key, "type": "percent", "rate": rate, "fee": fee})
        else:
            amt = float(item.amount or 0.0)
            fee = round(amt, 2)
            breakdown.append({"key": item.key, "type": "fixed", "amount": amt, "fee": fee})
        total += fee

    total = round(total, 2)
    net = round(float(gross) - total, 2)
    return net, total, breakdown


async def recompute_sell_anchor_for_orphan(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    orphan_id: str,
) -> Dict[str, Any]:
    """Compute + persist sell_anchor for an orphan doc.

    Orphans typically have no sell children, so they rely on manual_sell_anchor_gross.
    """
    repo = TradeinOrphansRepo(db)
    doc = await repo.get_one(user_id=user_id, orphan_id=orphan_id)
    if not doc:
        raise NotFoundError(
            code="orphan_not_found",
            message="Trade-in orphan not found",
            details={"user_id": user_id, "orphan_id": orphan_id},
        )

    settings_doc = await get_sell_anchor_settings_for_user(db, user_id)
    safe_ratio = float(settings_doc.get("safe_ratio") or 0.85)
    fee_config = _fee_config_or_default(settings_doc.get("fee_config"))

    manual_gross_raw = doc.get("manual_sell_anchor_gross")
    manual_gross: Optional[float] = None
    try:
        if manual_gross_raw is not None:
            manual_gross = float(manual_gross_raw)
            if manual_gross <= 0:
                manual_gross = None
    except (TypeError, ValueError):
        manual_gross = None

    manual_net: Optional[float] = None
    manual_fees_total: Optional[float] = None
    manual_breakdown: List[Dict[str, Any]] = []

    if manual_gross is not None:
        manual_net, manual_fees_total, manual_breakdown = _compute_net(manual_gross, fee_config)

    gross_used: Optional[float] = None
    net_used: Optional[float] = None
    source_used: Optional[str] = None
    needs_manual_anchor = False

    if manual_gross is not None and manual_net is not None and manual_net > 0:
        gross_used = manual_gross
        net_used = manual_net
        source_used = "manual"
    else:
        needs_manual_anchor = True

    # Orphans have no auto anchor.
    sell_anchor: Dict[str, Any] = {
        "lowest_gross_price_to_win": None,
        "lowest_net_sell_price": None,
        "auto_fees_total": None,
        "auto_fee_breakdown": [],
        "source_bm_listing_id": None,
        "source_full_sku": None,
        "safe_ratio": safe_ratio,
        "safe_gross_threshold": None,
        "auto_viable_listings_count": 0,
        "auto_viable_listings_min_required": MIN_VIABLE_CHILD_PRICES,
        "max_price": None,
        "auto_viable": False,
        "reason": "missing_gross_anchor",
        "manual_sell_anchor_gross": manual_gross,
        "manual_net_sell_price": manual_net,
        "manual_fees_total": manual_fees_total,
        "manual_fee_breakdown": manual_breakdown,
        "gross_used": gross_used,
        "net_used": net_used,
        "source_used": source_used,
        "needs_manual_anchor": needs_manual_anchor,
        "currency": fee_config.currency,
        "fees": fee_config.model_dump(),
        "computed_at": _now_utc(),
    }

    now = _now_utc()
    await repo.persist_sell_anchor(user_id=user_id, orphan_id=orphan_id, sell_anchor=sell_anchor, updated_at=now)
    return sell_anchor

