# app/features/pricing_groups/service.py
"""
Build pricing groups from stored BackMarket listings.

Inputs (stored per user):
- bm_sell_listings
- bm_tradein_listings

Outputs:
- pricing_groups

Additionally, logs skipped/invalid data into:
- pricing_bad_sell_skus
- pricing_bad_tradein_skus

Why issues are persisted
------------------------
If we just "continue" on malformed SKUs or missing matches, we lose visibility.
Persisting issues makes the pipeline deterministic, debuggable, and fixable
without guesswork.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, DefaultDict, Dict, List, Optional, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.backmarket.pricing_groups.constants import ALLOWED_TRADEIN_GRADE_CODES
from app.features.backmarket.pricing_groups.issues_repo import PricingIssuesRepo
from app.features.backmarket.pricing_groups.repo import PricingGroupsRepo
from app.features.backmarket.pricing_groups.sku import (
    ParsedSellSku,
    make_group_key,
    parse_sell_sku_with_reason,
    parse_trade_sku_with_reason,
    target_sell_condition,
)

logger = logging.getLogger(__name__)

SELL_COL = "bm_sell_listings"
TRADEIN_COL = "bm_tradein_listings"
GROUPS_COL = "pricing_groups"


def _upper(s: Optional[str]) -> str:
    return (s or "").strip().upper()


def _sell_doc_sku(doc: Dict[str, Any]) -> Optional[str]:
    """
    Sell docs are stored as:
      { user_id, listing_id, raw: {...}, ... }

    We prefer raw.sku but support older shapes defensively.
    """
    sku = doc.get("sku")
    if sku:
        return str(sku)

    raw = doc.get("raw") or {}
    if isinstance(raw, dict):
        v = raw.get("sku") or raw.get("full_sku") or raw.get("fullSku")
        return str(v) if v else None

    return None


def _sell_doc_id(doc: Dict[str, Any]) -> Optional[str]:
    """
    Prefer our stored listing_id.
    """
    v = doc.get("listing_id") or doc.get("bm_listing_id")
    if v is not None:
        return str(v)

    raw = doc.get("raw") or {}
    if isinstance(raw, dict):
        rv = raw.get("id") or raw.get("listingId")
        return str(rv) if rv is not None else None

    return None


def _tradein_id(doc: Dict[str, Any]) -> Optional[str]:
    """
    Trade-in docs are stored as:
      { user_id, tradein_id, sku, aesthetic_grade_code, ... }
    Keep defensive for older shapes.
    """
    v = doc.get("tradein_id") or doc.get("id")
    return str(v) if v is not None else None


def _tradein_grade(doc: Dict[str, Any]) -> Optional[str]:
    v = doc.get("aesthetic_grade_code") or doc.get("aestheticGradeCode")
    return str(v).strip().upper() if v else None


def _tradein_product_id(doc: Dict[str, Any]) -> Optional[str]:
    v = doc.get("product_id") or doc.get("productId")
    return str(v) if v is not None else None


def _sell_child_snapshot(doc: Dict[str, Any], parsed: ParsedSellSku) -> Dict[str, Any]:
    """
    Lightweight embed of sell listing into the pricing group.

    Keep this small so pricing_groups documents remain safe (Mongo 16MB cap).
    If you want more fields later, add them here deliberately.
    """
    raw = doc.get("raw") or {}
    if not isinstance(raw, dict):
        raw = {}

    qty_raw = (
        raw.get("quantity")
        or raw.get("availableQuantity")
        or raw.get("available_quantity")
        or doc.get("quantity")
    )
    try:
        qty = int(qty_raw) if qty_raw is not None else 0
    except (TypeError, ValueError):
        qty = 0


    return {
        "bm_listing_id": _sell_doc_id(doc),
        "full_sku": _sell_doc_sku(doc),
        "colour": parsed.colour,
        "storage_gb": parsed.storage_gb,
        "sim_type": parsed.sim_type,
        "condition": _upper(parsed.condition),
        # Keep this minimal; backbox fields are appended later by backbox.py
        "quantity": qty,
        "max_price": raw.get("max_price") or doc.get("max_price"),
    }


def _choose_better_tradein(existing: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    """
    If multiple trade-in listings share the same trade SKU, pick one.

    Rule:
    - prefer higher gb_amount (if present)
    """
    ex = existing.get("gb_amount")
    ca = candidate.get("gb_amount")

    try:
        exv = float(ex) if ex is not None else None
    except (TypeError, ValueError):
        exv = None

    try:
        cav = float(ca) if ca is not None else None
    except (TypeError, ValueError):
        cav = None

    if exv is None and cav is not None:
        return candidate
    if exv is not None and cav is not None and cav > exv:
        return candidate
    return existing


# Listing subfields that are written by other stages (backbox snapshot persistence).
# Fields inside pricing_groups.listings[*] that are updated by background/side pipelines
# (Backbox, Orders, etc.) and must be preserved when we rebuild pricing_groups from the
# canonical sell-listings snapshot.
_LISTING_DYNAMIC_FIELDS = (
    # Backbox (competitor signal)
    "backbox",
    "backbox_history",
    "backbox_best_price_to_win",

    # Orders (real sales)
    "sold_history",
    "sold_best_price",
    "sold_last_at",
)


def _child_snapshot_ids(child: Dict[str, Any]) -> List[str]:
    """Return all plausible IDs for a stored child listing snapshot.

    We keep this liberal because older docs used different keys (bm_uuid vs bm_listing_id).
    """
    ids: List[str] = []
    for k in ("bm_listing_id", "bm_uuid", "listing_id", "bm_id", "uuid", "listing_uuid"):
        v = child.get(k)
        if v is not None and str(v).strip():
            ids.append(str(v).strip())
    return ids


def _merge_listing_dynamic_fields(new_child: Dict[str, Any], existing_child: Dict[str, Any]) -> None:
    """Preserve dynamic subfields that are maintained by other pipelines."""
    for k in _LISTING_DYNAMIC_FIELDS:
        if k in existing_child:
            new_child[k] = existing_child[k]


def _merge_tradein_dynamic_fields(new_tradein: Dict[str, Any], existing_tradein: Dict[str, Any]) -> None:
    """Preserve competitor snapshots/history when re-building group structure."""
    for k in ("competitor", "competitor_history", "competitor_updated_at"):
        if k in existing_tradein:
            new_tradein[k] = existing_tradein[k]

async def build_pricing_groups_for_user(
    db: AsyncIOMotorDatabase,
    user_id: str,
    *,
    prune_missing: bool = False,
    max_children_per_group: int = 500,
) -> Dict[str, Any]:
    """
    Build (upsert) pricing groups for a user and persist skipped items to issue DBs.

    prune_missing:
      If True, delete pricing_groups for this user whose trade_sku was not built this run.

    max_children_per_group:
      Safety cap to prevent massive group docs.
    """
    now = datetime.now(timezone.utc)

    groups_repo = PricingGroupsRepo(db)
    issues_repo = PricingIssuesRepo(db)

    # Collected issues to write at end (bulk upsert)
    sell_issues: List[Dict[str, Any]] = []
    tradein_issues: List[Dict[str, Any]] = []

    # -------------------------
    # 1) Load sell listings + index children
    # -------------------------
    sell_col = db[SELL_COL]

    # index: (BRAND, MODEL, STORAGE_GB, CONDITION) -> [child_snapshots...]
    sells_index: DefaultDict[Tuple[str, str, Optional[int], str], List[Dict[str, Any]]] = defaultdict(list)

    # secondary index: (BRAND, MODEL, STORAGE_GB) -> { condition -> count }
    available_conditions: DefaultDict[Tuple[str, str, Optional[int]], Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    sell_seen = 0
    sell_parsed = 0

    async for sdoc in sell_col.find({"user_id": user_id}, {"_id": 0}):
        sell_seen += 1

        listing_id = _sell_doc_id(sdoc)
        sku = _sell_doc_sku(sdoc)

        if not listing_id:
            # Can't store issue without stable identifier (by design).
            # If this happens, your sell sync is storing bad data.
            continue

        parsed, reason_code, reason_details = parse_sell_sku_with_reason(sku)
        if not parsed:
            sell_issues.append(
                {
                    "listing_id": listing_id,
                    "sku": sku,
                    "reason_code": reason_code or "malformed_sell_sku",
                    "reason_details": {"source_collection": SELL_COL, **(reason_details or {})},
                }
            )
            continue

        sell_parsed += 1

        brand_u = _upper(parsed.brand)
        model_u = _upper(parsed.model)
        cond_u = _upper(parsed.condition)

        key = (brand_u, model_u, parsed.storage_gb, cond_u)
        sells_index[key].append(_sell_child_snapshot(sdoc, parsed))

        dev_key = (brand_u, model_u, parsed.storage_gb)
        available_conditions[dev_key][cond_u] += 1

    # -------------------------
    # 2) Load trade-ins (filter + dedupe by trade_sku)
    # -------------------------
    tradein_col = db[TRADEIN_COL]

    tradeins_considered = 0
    tradeins_by_trade_sku: Dict[str, Dict[str, Any]] = {}

    async for ti in tradein_col.find({"user_id": user_id}, {"_id": 0}):
        tid = _tradein_id(ti)
        sku = ti.get("sku")
        grade = _tradein_grade(ti)

        if not tid:
            # can't store issue without stable tradein_id
            continue

        if not grade:
            tradein_issues.append(
                {
                    "tradein_id": tid,
                    "sku": str(sku) if sku else None,
                    "product_id": _tradein_product_id(ti),
                    "aesthetic_grade_code": None,
                    "reason_code": "missing_grade_code",
                    "reason_details": {},
                }
            )
            continue

        if grade not in ALLOWED_TRADEIN_GRADE_CODES:
            tradein_issues.append(
                {
                    "tradein_id": tid,
                    "sku": str(sku) if sku else None,
                    "product_id": _tradein_product_id(ti),
                    "aesthetic_grade_code": grade,
                    "reason_code": "grade_not_allowed",
                    "reason_details": {"allowed": sorted(ALLOWED_TRADEIN_GRADE_CODES)},
                }
            )
            continue

        if not sku:
            tradein_issues.append(
                {
                    "tradein_id": tid,
                    "sku": None,
                    "product_id": _tradein_product_id(ti),
                    "aesthetic_grade_code": grade,
                    "reason_code": "missing_sku",
                    "reason_details": {"source_collection": TRADEIN_COL},
                }
            )
            continue

        tradeins_considered += 1
        trade_sku = str(sku)

        # Deduplicate: one "best" tradein per trade_sku
        existing = tradeins_by_trade_sku.get(trade_sku)
        if existing is None:
            tradeins_by_trade_sku[trade_sku] = ti
        else:
            tradeins_by_trade_sku[trade_sku] = _choose_better_tradein(existing, ti)


    # Prefetch existing groups so "rebuild" is truly structural-only:
    # we preserve dynamic fields that are written by other stages
    # (e.g. backbox snapshots, competitor snapshots).
    existing_by_trade_sku: Dict[str, Dict[str, Any]] = {}
    if tradeins_by_trade_sku:
        cur_existing = db[GROUPS_COL].find(
            {"user_id": user_id, "trade_sku": {"$in": list(tradeins_by_trade_sku.keys())}},
            projection={"trade_sku": 1, "listings": 1, "tradein_listing": 1},
        )
        async for g in cur_existing:
            ts = g.get("trade_sku")
            if isinstance(ts, str) and ts.strip():
                existing_by_trade_sku[ts.strip()] = g

    # -------------------------
    # 3) Build group docs + record "no match" + malformed trade SKU
    # -------------------------
    groups_to_upsert: List[Dict[str, Any]] = []
    skipped_bad_trade_sku = 0
    skipped_no_children = 0

    for trade_sku, ti in tradeins_by_trade_sku.items():
        tid = _tradein_id(ti)  # safe: should exist here
        grade = _tradein_grade(ti)

        parsed_trade, treason_code, treason_details = parse_trade_sku_with_reason(trade_sku)
        if not parsed_trade:
            skipped_bad_trade_sku += 1
            if tid:
                tradein_issues.append(
                    {
                        "tradein_id": tid,
                        "sku": trade_sku,
                        "product_id": _tradein_product_id(ti),
                        "aesthetic_grade_code": grade,
                        "reason_code": treason_code or "malformed_trade_sku",
                        "reason_details": treason_details or {},
                    }
                )
            continue

        brand_u = _upper(parsed_trade.brand)
        model_u = _upper(parsed_trade.model)
        storage_gb = parsed_trade.storage_gb

        target_cond = target_sell_condition(
            tradein_grade_code=grade,
            trade_sku_condition=_upper(parsed_trade.condition),
        )
        target_cond_u = _upper(target_cond)

        sell_key = (brand_u, model_u, storage_gb, target_cond_u)
        children = sells_index.get(sell_key, [])

        if not children:
            skipped_no_children += 1
            if tid:
                dev_key = (brand_u, model_u, storage_gb)
                avail = available_conditions.get(dev_key, {})
                tradein_issues.append(
                    {
                        "tradein_id": tid,
                        "sku": trade_sku,
                        "product_id": _tradein_product_id(ti),
                        "aesthetic_grade_code": grade,
                        "reason_code": "no_sell_children",
                        "reason_details": {
                            "brand": brand_u,
                            "model": model_u,
                            "storage_gb": storage_gb,
                            "target_sell_condition": target_cond_u,
                            "available_sell_conditions": dict(avail),
                        },
                    }
                )
            continue

        if 0 < max_children_per_group < len(children):
            children = children[:max_children_per_group]

        existing_group = existing_by_trade_sku.get(trade_sku)

        # Copy children so we don't mutate the global sells_index cache.
        children_out: List[Dict[str, Any]] = [dict(c) for c in children]

        # Preserve listing dynamic fields written by other stages (backbox snapshots + history).
        if existing_group:
            existing_children = existing_group.get("listings") or []
            if isinstance(existing_children, list) and existing_children:
                existing_child_by_id: Dict[str, Dict[str, Any]] = {}
                for ex_child in existing_children:
                    if not isinstance(ex_child, dict):
                        continue
                    for cid in _child_snapshot_ids(ex_child):
                        if cid not in existing_child_by_id:
                            existing_child_by_id[cid] = ex_child

                for c in children_out:
                    for cid in _child_snapshot_ids(c):
                        ex = existing_child_by_id.get(cid)
                        if ex:
                            _merge_listing_dynamic_fields(c, ex)
                            break

        tradein_listing_doc: Dict[str, Any] = {
            "tradein_id": tid,
            "sku": ti.get("sku"),
            "aesthetic_grade_code": grade,
        }

        # Preserve competitor snapshot/history if this group already exists.
        if existing_group:
            existing_tradein = existing_group.get("tradein_listing") or {}
            if isinstance(existing_tradein, dict):
                _merge_tradein_dynamic_fields(tradein_listing_doc, existing_tradein)

        group_doc = {
            "schema_version": 2,
            "user_id": user_id,
            "group_key": make_group_key(
                brand=brand_u,
                model=model_u,
                storage_gb=storage_gb,
                tradein_grade_code=grade,
            ),
            "trade_sku": trade_sku,
            "brand": brand_u,
            "model": model_u,
            "storage_gb": storage_gb,
            "trade_sku_condition": _upper(parsed_trade.condition),
            "tradein_grade_code": grade,
            "target_sell_condition": target_cond_u,
            "listings": children_out,
            "listings_count": len(children_out),
            "tradein_listing": tradein_listing_doc,
            "updated_at": now,
        }
        groups_to_upsert.append(group_doc)
    # -------------------------
    # 4) Persist groups
    # -------------------------
    upsert_res = await groups_repo.bulk_upsert(user_id=user_id, group_docs=groups_to_upsert)

    deleted = 0
    if prune_missing:
        keep = [g["trade_sku"] for g in groups_to_upsert if g.get("trade_sku")]
        deleted = await groups_repo.delete_missing_trade_skus(user_id=user_id, keep_trade_skus=keep)

    # -------------------------
    # 5) Persist issues (bulk upsert)
    # -------------------------
    sell_issues_res = await issues_repo.upsert_sell_issues(user_id=user_id, issues=sell_issues)
    tradein_issues_res = await issues_repo.upsert_tradein_issues(user_id=user_id, issues=tradein_issues)

    logger.info(
        "[pricing_groups] user_id=%s groups=%s sell_issues=%s tradein_issues=%s",
        user_id,
        len(groups_to_upsert),
        len(sell_issues),
        len(tradein_issues),
    )

    return {
        "user_id": user_id,
        "source_collections": {"sell": SELL_COL, "tradein": TRADEIN_COL},
        "sell_seen": sell_seen,
        "sell_parsed": sell_parsed,
        "tradeins_considered": tradeins_considered,
        "groups_built": len(groups_to_upsert),
        "groups_skipped_bad_trade_sku": skipped_bad_trade_sku,
        "groups_skipped_no_children": skipped_no_children,
        "db_groups_upsert": {
            "attempted": upsert_res.attempted,
            "created": upsert_res.upserted,
            "matched": upsert_res.matched,
            "modified": upsert_res.modified,
            "elapsed_seconds": upsert_res.elapsed_seconds,
        },
        "db_issues_upsert": {
            "sell": {
                "attempted": sell_issues_res.attempted,
                "created": sell_issues_res.upserted,
                "matched": sell_issues_res.matched,
                "modified": sell_issues_res.modified,
                "elapsed_seconds": sell_issues_res.elapsed_seconds,
            },
            "tradein": {
                "attempted": tradein_issues_res.attempted,
                "created": tradein_issues_res.upserted,
                "matched": tradein_issues_res.matched,
                "modified": tradein_issues_res.modified,
                "elapsed_seconds": tradein_issues_res.elapsed_seconds,
            },
        },
        "prune_missing": prune_missing,
        "deleted": deleted,
        "run_at": now.isoformat(),
    }


