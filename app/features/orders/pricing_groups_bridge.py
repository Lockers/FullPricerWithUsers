from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Iterable, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateMany


def _parse_rfc3339(dt_str: Any) -> Optional[datetime]:
    if not dt_str:
        return None
    if isinstance(dt_str, datetime):
        return dt_str if dt_str.tzinfo else dt_str.replace(tzinfo=timezone.utc)

    s = str(dt_str).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _money_float(v: Any) -> Optional[float]:
    """Normalize money values to 2dp float for stable comparisons + $min."""
    if v is None:
        return None
    try:
        d = Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return float(d)
    except (InvalidOperation, ValueError, TypeError):
        return None


def _iter_orderlines(order: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Docs say orderlines can be 'indexed by SKU', but examples show a list.
    Handle both list and dict defensively.
    """
    lines = order.get("orderlines") or []
    if isinstance(lines, list):
        for x in lines:
            if isinstance(x, dict):
                yield x
        return

    if isinstance(lines, dict):
        for sku_key, val in lines.items():
            if not isinstance(val, dict):
                continue
            d = dict(val)
            d.setdefault("listing", sku_key)
            yield d
        return


async def apply_orders_to_pricing_groups(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    orders: List[Dict[str, Any]],
) -> Dict[str, int]:
    """
    For each BM orderline, match:
        orderline.listing  -> pricing_groups.listings.full_sku

    Then:
      - add sale record to listings.$.sold_history (idempotent)
      - maintain listings.$.sold_best_price (min)
      - maintain listings.$.sold_last_at (max)
    """
    if not orders:
        return {"ops": 0, "matched_docs": 0, "modified_docs": 0}

    col = db["pricing_groups"]
    now = datetime.now(timezone.utc)

    ops = []

    for order in orders:
        order_id = order.get("order_id")
        if order_id is None:
            continue

        # Pick a stable "sold_at" timestamp.
        sold_at = (
            _parse_rfc3339(order.get("date_payment"))
            or _parse_rfc3339(order.get("date_creation"))
            or _parse_rfc3339(order.get("date_modification"))
        )

        for line in _iter_orderlines(order):
            sku = (line.get("listing") or "").strip()
            if not sku:
                continue

            orderline_id = line.get("id")
            if orderline_id is None:
                continue

            unit_price = _money_float(line.get("price"))
            unit_ship = _money_float(line.get("shipping_price"))
            qty_raw = line.get("quantity")
            try:
                qty = int(qty_raw) if qty_raw is not None else 1
            except (TypeError, ValueError):
                qty = 1
            qty = max(1, qty)

            currency = (line.get("currency") or order.get("currency") or "").strip() or None

            # This object must be stable across runs so $addToSet prevents duplicates.
            sale_entry: Dict[str, Any] = {
                "order_id": int(order_id),
                "orderline_id": int(orderline_id),
                "sold_at": sold_at,
                "currency": currency,
                "unit_price": unit_price,
                "unit_shipping_price": unit_ship,
                "quantity": qty,
            }

            # Build update doc (only include $min/$max if we have values)
            update: Dict[str, Any] = {
                "$addToSet": {"listings.$[l].sold_history": sale_entry},
                "$set": {
                    # keep your document timestamps consistent
                    "updated_at": now,
                },
            }

            if unit_price is not None:
                update["$min"] = {"listings.$[l].sold_best_price": unit_price}

            if sold_at is not None:
                update.setdefault("$max", {})
                update["$max"]["listings.$[l].sold_last_at"] = sold_at

            # Use UpdateMany to be safe in case the same full_sku exists in more than one doc.
            ops.append(
                UpdateMany(
                    {"user_id": user_id, "listings.full_sku": sku},
                    update,
                    array_filters=[{"l.full_sku": sku}],
                )
            )

    if not ops:
        return {"ops": 0, "matched_docs": 0, "modified_docs": 0}

    res = await col.bulk_write(ops, ordered=False)
    return {
        "ops": len(ops),
        "matched_docs": int(getattr(res, "matched_count", 0) or 0),
        "modified_docs": int(getattr(res, "modified_count", 0) or 0),
    }
