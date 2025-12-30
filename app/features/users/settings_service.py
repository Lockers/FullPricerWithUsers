"""
Settings service:
- init user with BM credentials
- load BM creds for backmarket client
- update settings + tradein config
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.errors import BadRequestError, NotFoundError
from app.features.users.repo import UsersRepo
from app.features.users.schemas import UserInitRequest, UserInitResponse, UserRead
from app.features.users.settings_repo import UserSettingsRepo
from app.features.users.settings_schemas import TradeinConfig, TradeinConfigUpdate, UserSettingsRead, UserSettingsUpdate

logger = logging.getLogger(__name__)


def _normalise_bm_api_key(raw: str) -> str:
    """
    Store bm_api_key as RAW token (no 'Basic ', no quotes).
    """
    if not raw:
        return raw
    s = raw.strip()
    if s.lower().startswith("basic "):
        s = s[6:].strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _header_token(name: str, fallback: str) -> str:
    if not name:
        return fallback
    token = name.strip().replace(" ", "")
    token = re.sub(r"[^A-Za-z0-9\-]", "", token)
    return token or fallback


def _build_bm_user_agent(company_name: str, integration_name: str, contact_email: str) -> str:
    company_token = _header_token(company_name, "Company")
    integration_token = _header_token(integration_name, "App")
    email = (contact_email or "").strip() or "contact@example.com"
    return f"BM-{company_token}-{integration_token};{email}"


def _mask_key(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if len(raw) <= 6:
        return "***"
    return f"{raw[:3]}***{raw[-3:]}"


def _safe_tradein_config(user_id: str, doc: Dict[str, Any]) -> TradeinConfig:
    cfg_dict = doc.get("tradein_config") or {}
    try:
        return TradeinConfig(**cfg_dict)
    except Exception:
        # This should be rare; it can happen if historic data was written with invalid/null fields.
        logger.exception("tradein_config_invalid user_id=%s; falling back to defaults", user_id)
        return TradeinConfig()


def _to_user_settings_read(user_id: str, doc: Dict[str, Any]) -> UserSettingsRead:
    token = (doc.get("bm_api_key") or "").strip()
    tradein_cfg = _safe_tradein_config(user_id, doc)

    return UserSettingsRead(
        user_id=user_id,
        bm_api_key_set=bool(token),
        bm_api_key_masked=_mask_key(token),
        bm_user_id=doc.get("bm_user_id"),
        bm_seller_id=doc.get("bm_seller_id"),
        integration_name=doc.get("integration_name"),
        user_agent=doc.get("user_agent"),
        market_language=doc.get("market_language") or "en-gb",
        daily_trade_limit=doc.get("daily_trade_limit"),
        daily_spend_limit=doc.get("daily_spend_limit"),
        auto_pause_on_limit=bool(doc.get("auto_pause_on_limit", False)),
        tradein_config=tradein_cfg,
    )


async def init_user_with_bm(db: AsyncIOMotorDatabase, data: UserInitRequest) -> UserInitResponse:
    users_repo = UsersRepo(db)
    settings_repo = UserSettingsRepo(db)

    logger.info("init_user_with_bm:start email=%s company=%s", str(data.email), data.company_name)

    # Normalise + validate API key BEFORE creating the user to avoid dangling user docs on 400s.
    token = _normalise_bm_api_key(data.bm_api_key)
    if not token:
        raise BadRequestError(code="bm_api_key_missing", message="bm_api_key is required")

    # 1) create user
    user_doc = await users_repo.create(
        email=str(data.email),
        name=data.name,
        company_name=data.company_name,
    )
    user_id = user_doc["id"]

    # 2) derive integration name
    company_token = _header_token(data.company_name, "Company")
    integration_name = data.program_name or f"{company_token}BMpricer"

    # 3) build BM-compliant user agent
    user_agent = _build_bm_user_agent(data.company_name, integration_name, str(data.email))

    # 4) default trade-in config
    tradein_cfg = TradeinConfig()

    now = datetime.now(timezone.utc)

    # 5) upsert settings (rollback user on failure)
    try:
        await settings_repo.upsert(
            user_id,
            {
                "bm_api_key": token,
                "bm_user_id": data.bm_user_id,
                "bm_seller_id": data.bm_seller_id,
                "integration_name": integration_name,
                "user_agent": user_agent,
                "market_language": (data.market_language or "en-gb"),
                "daily_trade_limit": None,
                "daily_spend_limit": None,
                "auto_pause_on_limit": False,
                "tradein_config": tradein_cfg.model_dump(),
                "created_at": now,
                "updated_at": now,
            },
        )
    except Exception:
        logger.exception("init_user_with_bm:settings_upsert_failed user_id=%s; attempting rollback", user_id)
        # Best-effort cleanup
        try:
            await settings_repo.delete(user_id)
        except Exception:
            logger.exception("init_user_with_bm:rollback_settings_delete_failed user_id=%s", user_id)
        try:
            await users_repo.delete(user_id)
        except Exception:
            logger.exception("init_user_with_bm:rollback_user_delete_failed user_id=%s", user_id)
        raise

    logger.info("init_user_with_bm:done user_id=%s integration_name=%s", user_id, integration_name)

    return UserInitResponse(
        **UserRead(**user_doc).model_dump(),
        bm_credentials_set=True,
        market_language=data.market_language or "en-gb",
        integration_name=integration_name,
        user_agent=user_agent,
    )


async def get_user_settings(db: AsyncIOMotorDatabase, user_id: str) -> UserSettingsRead:
    settings_repo = UserSettingsRepo(db)
    doc = await settings_repo.get(user_id)
    if not doc:
        raise NotFoundError(code="user_settings_not_found", message="User settings not found", details={"user_id": user_id})

    return _to_user_settings_read(user_id, doc)


async def update_user_settings(db: AsyncIOMotorDatabase, user_id: str, patch: UserSettingsUpdate) -> UserSettingsRead:
    settings_repo = UserSettingsRepo(db)

    update: Dict[str, Any] = patch.model_dump(exclude_unset=True)

    if "bm_api_key" in update and update["bm_api_key"] is not None:
        update["bm_api_key"] = _normalise_bm_api_key(update["bm_api_key"])

    logger.info("update_user_settings user_id=%s fields=%s", user_id, sorted(update.keys()))

    doc = await settings_repo.update(user_id, update)
    return _to_user_settings_read(user_id, doc)


async def update_tradein_config(db: AsyncIOMotorDatabase, user_id: str, patch: TradeinConfigUpdate) -> UserSettingsRead:
    settings_repo = UserSettingsRepo(db)

    doc = await settings_repo.get(user_id)
    if not doc:
        raise NotFoundError(code="user_settings_not_found", message="User settings not found", details={"user_id": user_id})

    # IMPORTANT:
    # - exclude_none prevents writing explicit nulls into persisted tradein_config
    # - writing nulls breaks TradeinConfig validation on reads
    updates = patch.model_dump(exclude_unset=True, exclude_none=True)

    if not updates:
        # no-op patch
        return _to_user_settings_read(user_id, doc)

    current = _safe_tradein_config(user_id, doc)
    merged = {**current.model_dump(), **updates}
    new_cfg = TradeinConfig(**merged)

    logger.info("update_tradein_config user_id=%s fields=%s", user_id, sorted(updates.keys()))

    updated_doc = await settings_repo.update(user_id, {"tradein_config": new_cfg.model_dump()})
    return _to_user_settings_read(user_id, updated_doc)


# ---- BackMarket hook (used by backmarket/transport/headers.py) ----

async def get_user_bm_credentials(db: AsyncIOMotorDatabase, user_id: str) -> Dict[str, Any]:
    """
    Return the fields needed by BackMarket client.

    Required:
    - bm_api_key (RAW token; transport/headers.py will add 'Basic ')
    - user_agent
    """
    settings_repo = UserSettingsRepo(db)
    doc = await settings_repo.get(user_id)
    if not doc:
        raise NotFoundError(code="user_settings_not_found", message="User settings not found", details={"user_id": user_id})

    bm_api_key = (doc.get("bm_api_key") or "").strip()
    user_agent = (doc.get("user_agent") or "").strip()

    if not bm_api_key or not user_agent:
        logger.warning(
            "bm_settings_incomplete user_id=%s api_key_set=%s user_agent_set=%s",
            user_id,
            bool(bm_api_key),
            bool(user_agent),
        )
        raise BadRequestError(
            code="bm_settings_incomplete",
            message="bm_api_key and user_agent must be configured for this user",
            details={"user_id": user_id},
        )

    return {
        "bm_api_key": bm_api_key,
        "user_agent": user_agent,
        "market_language": (doc.get("market_language") or "en-gb"),
        "bm_user_id": doc.get("bm_user_id"),
        "bm_seller_id": doc.get("bm_seller_id"),
        "integration_name": doc.get("integration_name"),
    }

