#!/usr/bin/env python
"""Reset per-user data in MongoDB for this project.

This deletes documents for a given user_id across the project's collections.

Typical usage:

  python scripts/reset_user_data.py --user-id <UUID> --yes

By default this keeps the user account record in the `users` collection.
Use --delete-user to remove that too.

Environment variables:
  MONGO_URI (default: mongodb://localhost:27017)
  MONGO_DB  (default: pricer)
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable

from pymongo import MongoClient


DEFAULT_COLLECTIONS: list[str] = [
    # Core data
    "pricing_groups",
    "bm_sell_listings",
    "bm_tradein_listings",
    # Trade-in pricing / safety / state
    "pricing_tradein_orphans",
    "pricing_unavailable_tradein_competitors",
    "pricing_bad_sell_skus",
    "pricing_bad_tradein_skus",
    # Sell-anchor & pricing settings
    "pricing_sell_anchor_settings",
    "user_settings",
    # Orders / rate-limits
    "bm_orders",
    "bm_orderlines",
    "bm_rate_state",
    # Depreciation data (if stored per-user)
    "depreciation_models",
    "depreciation_multipliers",
    # Optional extras
    "pricing_repair_costs",
]


def _iter_unique(seq: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in seq:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Delete all project data for a given user_id")
    p.add_argument("--user-id", required=True, help="The user_id (UUID string)")
    p.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        help="Mongo connection URI (default from MONGO_URI)",
    )
    p.add_argument(
        "--mongo-db",
        default=os.getenv("MONGO_DB", "pricer"),
        help="Mongo database name (default from MONGO_DB)",
    )
    p.add_argument(
        "--collections",
        nargs="*",
        default=None,
        help="Optional explicit list of collections to wipe. If omitted, uses a safe default list.",
    )
    p.add_argument(
        "--delete-user",
        action="store_true",
        help="Also delete the matching record from the `users` collection (id == user_id).",
    )
    p.add_argument("--yes", action="store_true", help="Skip confirmation prompt")

    args = p.parse_args()

    user_id = str(args.user_id).strip()
    if not user_id:
        print("--user-id must be non-empty", file=sys.stderr)
        return 2

    collections = _iter_unique(args.collections or DEFAULT_COLLECTIONS)

    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_db]

    # Preflight counts
    plan: list[tuple[str, int]] = []
    for name in collections:
        col = db[name]
        # Most collections use user_id; if they don't, this will just be 0.
        count = col.count_documents({"user_id": user_id})
        plan.append((name, int(count)))

    user_count = 0
    if args.delete_user:
        user_count = db["users"].count_documents({"id": user_id})

    total = sum(c for _, c in plan) + int(user_count)

    print(f"Mongo URI: {args.mongo_uri}")
    print(f"Mongo DB : {args.mongo_db}")
    print(f"User ID  : {user_id}")
    print("\nPlanned deletions:")
    for name, c in plan:
        print(f"  - {name}: {c}")
    if args.delete_user:
        print(f"  - users(id==user_id): {user_count}")
    print(f"\nTOTAL: {total}")

    if total == 0:
        print("Nothing to delete.")
        return 0

    if not args.yes:
        resp = input("\nType DELETE to confirm: ").strip()
        if resp != "DELETE":
            print("Aborted.")
            return 1

    print("\nDeleting...")
    deleted_total = 0
    for name, _ in plan:
        res = db[name].delete_many({"user_id": user_id})
        deleted_total += int(res.deleted_count)
        print(f"  - {name}: deleted {res.deleted_count}")

    if args.delete_user:
        res = db["users"].delete_many({"id": user_id})
        deleted_total += int(res.deleted_count)
        print(f"  - users: deleted {res.deleted_count}")

    print(f"\nDone. Deleted {deleted_total} documents.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
