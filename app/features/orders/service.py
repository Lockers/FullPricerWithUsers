# app/features/orders/service.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.transport.cache import get_bm_client_for_user
from app.features.orders.repo import BmOrdersRepo
from app.features.orders.pricing_groups_bridge import apply_orders_to_pricing_groups



def _to_rfc3339(dt: datetime) -> str:
    dtu = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return dtu.isoformat().replace("+00:00", "Z")


async def sync_bm_orders_for_user(
    db: AsyncIOMotorDatabase,
    *,
    user_id: str,
    full: bool = False,
    page_size: int = 50,
    overlap_seconds: int = 300,  # 5m overlap to avoid missing boundary updates
) -> Dict[str, Any]:
    repo = BmOrdersRepo(db)
    client = await get_bm_client_for_user(db, user_id)

    # Optional incremental mode (default): fetch orders modified since last sync.
    since: Optional[datetime] = None
    if not full:
        last = await repo.latest_date_modification(user_id=user_id)
        if last:
            since = last - timedelta(seconds=int(overlap_seconds))

    fetched = 0
    pages = 0
    upserted_total = 0

    page = 1
    page_size = min(50, max(1, int(page_size)))

    while True:
        params: Dict[str, Any] = {"page": page, "page-size": page_size}
        if since is not None:
            params["date_modification"] = _to_rfc3339(since)

        resp = await client.get(
            endpoint_key="sell_orders_get",
            path="/ws/orders",
            params=params,
        )

        if resp.status_code != 200:
            # Transport already retried; keep this failure explicit.
            body = (resp.text or "")[:500]
            raise RuntimeError(f"/ws/orders failed status={resp.status_code} body={body}")

        data = resp.json() or {}
        results = data.get("results") or []
        if not results:
            break

        pages += 1
        fetched += len(results)

        w = await repo.bulk_upsert_orders(user_id=user_id, orders=results)
        upserted_total += int(w.get("upserted", 0))
        await apply_orders_to_pricing_groups(db, user_id=user_id, orders=results)

        # stop conditions
        count = data.get("count")
        if isinstance(count, int) and fetched >= count:
            break
        if len(results) < page_size:
            break

        page += 1

    return {
        "user_id": user_id,
        "mode": ("full" if full else "incremental"),
        "since": since,
        "pages": pages,
        "fetched_orders": fetched,
        "upserted_new": upserted_total,
    }
