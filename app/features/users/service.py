"""
User service: CRUD for user identity.
"""

from __future__ import annotations

import logging

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError

from app.core.errors import NotFoundError
from app.features.users.repo import UsersRepo
from app.features.users.schemas import UserCreate, UserRead, UserUpdate
from app.features.users.settings_repo import UserSettingsRepo
from app.features.users.settings_schemas import TradeinConfig
from app.features.users.settings_service import _build_bm_user_agent, _header_token

logger = logging.getLogger(__name__)


async def create_user(db: AsyncIOMotorDatabase, data: UserCreate) -> UserRead:
    """
    Create the user identity record AND ensure a default user_settings doc exists.

    Historically, /users created only the user identity doc. The UI expects user_settings
    to exist, so we create defaults here to make new users usable immediately.
    """
    repo = UsersRepo(db)
    settings_repo = UserSettingsRepo(db)

    logger.info("create_user:start email=%s company=%s", str(data.email), data.company_name)
    doc = await repo.create(
        email=str(data.email),
        name=data.name,
        company_name=data.company_name,
    )
    user_id = doc["id"]

    # Ensure user_settings exists with sensible defaults (no API key yet).
    company_token = _header_token(data.company_name, "Company")
    integration_name = f"{company_token}BMpricer"
    user_agent = _build_bm_user_agent(data.company_name, integration_name, str(data.email))
    tradein_cfg = TradeinConfig()

    try:
        await settings_repo.upsert(
            user_id,
            {
                "bm_api_key": None,
                "bm_user_id": None,
                "bm_seller_id": None,
                "integration_name": integration_name,
                "user_agent": user_agent,
                "market_language": "en-gb",
                "daily_trade_limit": None,
                "daily_spend_limit": None,
                "auto_pause_on_limit": False,
                "tradein_config": tradein_cfg.model_dump(),
            },
        )
    except Exception:
        logger.exception("create_user:settings_upsert_failed user_id=%s; attempting rollback", user_id)
        # Best-effort cleanup
        try:
            await settings_repo.delete(user_id)
        except Exception:
            logger.exception("create_user:rollback_settings_delete_failed user_id=%s", user_id)
        try:
            await repo.delete(user_id)
        except Exception:
            logger.exception("create_user:rollback_user_delete_failed user_id=%s", user_id)
        raise

    logger.info("create_user:done user_id=%s", user_id)
    return UserRead(**doc)


async def list_users(db: AsyncIOMotorDatabase, limit: int = 100) -> list[UserRead]:
    repo = UsersRepo(db)

    docs = await repo.list(limit=limit)
    logger.debug("list_users count=%s limit=%s", len(docs), int(limit))
    return [UserRead(**d) for d in docs]


async def get_user_by_id(db: AsyncIOMotorDatabase, user_id: str) -> UserRead:
    repo = UsersRepo(db)

    doc = await repo.get(user_id)
    if not doc:
        raise NotFoundError(code="user_not_found", message="User not found", details={"user_id": user_id})

    return UserRead(**doc)


async def update_user(db: AsyncIOMotorDatabase, user_id: str, patch: UserUpdate) -> UserRead:
    repo = UsersRepo(db)

    update = patch.model_dump(exclude_unset=True)
    logger.info("update_user user_id=%s fields=%s", user_id, sorted(update.keys()))

    doc = await repo.update(user_id, update)
    return UserRead(**doc)


async def delete_user(db: AsyncIOMotorDatabase, user_id: str) -> None:
    repo = UsersRepo(db)

    logger.info("delete_user:start user_id=%s", user_id)
    deleted = await repo.delete(user_id)
    if not deleted:
        raise NotFoundError(code="user_not_found", message="User not found", details={"user_id": user_id})

    # Optional cleanup (best-effort). Narrow exception handling to DB-layer failures only.
    user_settings_deleted = None
    bm_rate_state_deleted = None

    try:
        res1 = await db["user_settings"].delete_one({"user_id": user_id})
        user_settings_deleted = int(getattr(res1, "deleted_count", 0))
    except PyMongoError:
        logger.exception("delete_user:cleanup_user_settings_failed user_id=%s", user_id)

    try:
        res2 = await db["bm_rate_state"].delete_many({"user_id": user_id})
        bm_rate_state_deleted = int(getattr(res2, "deleted_count", 0))
    except PyMongoError:
        logger.exception("delete_user:cleanup_bm_rate_state_failed user_id=%s", user_id)

    logger.info(
        "delete_user:cleanup user_id=%s user_settings_deleted=%s bm_rate_state_deleted=%s",
        user_id,
        user_settings_deleted,
        bm_rate_state_deleted,
    )

    logger.info("delete_user:done user_id=%s", user_id)


