from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorDatabase


ORPHAN_COL = "pricing_tradein_orphans"


class TradeinOrphansRepo:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.col = db[ORPHAN_COL]

    @staticmethod
    def _try_oid(orphan_id: str) -> Optional[ObjectId]:
        try:
            return ObjectId(str(orphan_id))
        except (InvalidId, TypeError):
            return None

    async def get_one(self, *, user_id: str, orphan_id: str) -> Optional[Dict[str, Any]]:
        oid = self._try_oid(orphan_id)
        if oid is None:
            return None
        doc = await self.col.find_one({"_id": oid, "user_id": user_id})
        return self._stringify_id(doc) if doc else None

    async def get_by_tradein_id(self, *, user_id: str, tradein_id: str) -> Optional[Dict[str, Any]]:
        doc = await self.col.find_one({"user_id": user_id, "tradein_id": tradein_id})
        return self._stringify_id(doc) if doc else None

    async def list_for_user(
        self,
        *,
        user_id: str,
        limit: int = 5000,
        include_disabled: bool = True,
        reason_codes: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {"user_id": user_id}
        if not include_disabled:
            q["enabled"] = True
        if reason_codes:
            q["reason_code"] = {"$in": list(reason_codes)}

        cur = self.col.find(q).sort("updated_at", -1).limit(int(limit))
        docs = await cur.to_list(length=None)
        return [self._stringify_id(d) for d in docs]

    @staticmethod
    def _stringify_id(doc: Dict[str, Any]) -> Dict[str, Any]:
        """Make Mongo docs JSON safe for FastAPI responses."""
        raw_id = doc.get("_id")
        if isinstance(raw_id, ObjectId):
            doc["_id"] = str(raw_id)
        doc["id"] = doc.get("_id")
        return doc

    async def set_enabled(self, *, user_id: str, orphan_id: str, enabled: bool, updated_at: datetime) -> bool:
        oid = self._try_oid(orphan_id)
        if oid is None:
            return False
        res = await self.col.update_one(
            {"_id": oid, "user_id": user_id},
            {"$set": {"enabled": bool(enabled), "enabled_updated_at": updated_at, "updated_at": updated_at}},
        )
        return bool(res.matched_count)

    async def set_manual_trade_sku(
        self,
        *,
        user_id: str,
        orphan_id: str,
        manual_trade_sku: Optional[str],
        updated_at: datetime,
        derived_fields: Dict[str, Any],
    ) -> bool:
        oid = self._try_oid(orphan_id)
        if oid is None:
            return False

        update: Dict[str, Any] = {"manual_trade_sku": manual_trade_sku, "updated_at": updated_at}
        update.update(derived_fields)

        res = await self.col.update_one({"_id": oid, "user_id": user_id}, {"$set": update})
        return bool(res.matched_count)

    async def set_manual_sell_anchor_gross(
        self,
        *,
        user_id: str,
        orphan_id: str,
        manual_sell_anchor_gross: Optional[float],
        updated_at: datetime,
    ) -> bool:
        oid = self._try_oid(orphan_id)
        if oid is None:
            return False
        res = await self.col.update_one(
            {"_id": oid, "user_id": user_id},
            {"$set": {"manual_sell_anchor_gross": manual_sell_anchor_gross, "updated_at": updated_at}},
        )
        return bool(res.matched_count)

    async def persist_sell_anchor(self, *, user_id: str, orphan_id: str, sell_anchor: Dict[str, Any], updated_at: datetime) -> bool:
        oid = self._try_oid(orphan_id)
        if oid is None:
            return False
        res = await self.col.update_one(
            {"_id": oid, "user_id": user_id},
            {"$set": {"sell_anchor": sell_anchor, "sell_anchor_updated_at": updated_at, "updated_at": updated_at}},
        )
        return bool(res.matched_count)

    async def persist_trade_pricing_computed(
        self,
        *,
        user_id: str,
        orphan_id: str,
        computed: Dict[str, Any],
        updated_at: datetime,
    ) -> bool:
        oid = self._try_oid(orphan_id)
        if oid is None:
            return False
        update: Dict[str, Any] = {
            "trade_pricing.computed": computed,
            "trade_pricing.best_scenario_key": computed.get("best_scenario_key"),
            "trade_pricing.max_trade_price_gross": computed.get("max_trade_price_gross"),
            "trade_pricing.max_trade_price_net": computed.get("max_trade_price_net"),
            "trade_pricing.max_trade_offer_gross": computed.get("max_trade_offer_gross"),
            "trade_pricing.final_update_price_gross": computed.get("final_update_price_gross"),
            "trade_pricing.final_update_price_net": computed.get("final_update_price_net"),
            "trade_pricing.final_update_reason": computed.get("final_update_reason"),
            "trade_pricing.profit_at_final_update": computed.get("profit_at_final_update"),
            "trade_pricing.margin_at_final_update": computed.get("margin_at_final_update"),
            "trade_pricing.vat_at_final_update": computed.get("vat_at_final_update"),
            "trade_pricing.not_ok_reasons": computed.get("not_ok_reasons") or [],
            "trade_pricing.ok_to_update": bool(computed.get("ok_to_update")),
            "trade_pricing.updated_at": updated_at,
            "updated_at": updated_at,
        }
        res = await self.col.update_one({"_id": oid, "user_id": user_id}, {"$set": update})
        return bool(res.matched_count)

    async def update_trade_pricing_settings(
        self,
        *,
        user_id: str,
        orphan_id: str,
        settings_patch: Dict[str, Any],
        updated_at: datetime,
    ) -> bool:
        oid = self._try_oid(orphan_id)
        if oid is None:
            return False
        update = {f"trade_pricing.settings.{k}": v for k, v in settings_patch.items()}
        update["trade_pricing.settings_updated_at"] = updated_at
        update["updated_at"] = updated_at
        res = await self.col.update_one({"_id": oid, "user_id": user_id}, {"$set": update})
        return bool(res.matched_count)

    async def persist_competitor_snapshot(
        self,
        *,
        user_id: str,
        orphan_id: str,
        latest_doc: Dict[str, Any],
        history_doc: Dict[str, Any],
        updated_at: datetime,
        history_max: int = 50,
    ) -> bool:
        oid = self._try_oid(orphan_id)
        if oid is None:
            return False
        res = await self.col.update_one(
            {"_id": oid, "user_id": user_id},
            {
                "$set": {
                    "tradein_listing.competitor": latest_doc,
                    "tradein_listing.competitor_updated_at": updated_at,
                    "updated_at": updated_at,
                },
                "$push": {
                    "tradein_listing.competitor_history": {
                        "$each": [history_doc],
                        "$slice": -int(history_max),
                    }
                },
            },
        )
        return bool(res.matched_count)

    async def persist_offer_update(
        self,
        *,
        user_id: str,
        orphan_id: str,
        offer_update_doc: Dict[str, Any],
        updated_at: datetime,
        history_max: int = 100,
    ) -> bool:
        oid = self._try_oid(orphan_id)
        if oid is None:
            return False
        res = await self.col.update_one(
            {"_id": oid, "user_id": user_id},
            {
                "$set": {
                    "tradein_listing.offer_update": offer_update_doc,
                    "tradein_listing.offer_update_updated_at": updated_at,
                    "updated_at": updated_at,
                },
                "$push": {
                    "tradein_listing.offer_update_history": {
                        "$each": [offer_update_doc],
                        "$slice": -int(history_max),
                    }
                },
            },
        )
        return bool(res.matched_count)

