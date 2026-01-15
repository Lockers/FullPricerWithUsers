from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from app.core.config import config
from app.core.errors import register_exception_handlers
from app.core.logging import init_logging
from app.core.auto_price_all import start_auto_price_all, stop_auto_price_all
from app.db.mongo import mongo_lifespan
from app.features.backmarket.pricing_groups.router import router as pricing_groups_router
from app.features.backmarket.sell.router import router as bm_router
from app.features.backmarket.tradein.router import router as tradein_router
from app.features.backmarket.tradein_orphans.router import router as tradein_orphans_router
from app.features.backmarket.transport.cache import shutdown_bm_clients
from app.features.users.router import router as users_router
from app.features.repaircost.router import router as repaircost_router
from app.features.orders.router import router as orders_router
from app.features.depreciation_api import router as depreciation_router
from app.features.backmarket.pricing.routers import router as bm_pricing_router



# Configure logging ON IMPORT so all subsequent module logs behave correctly.
init_logging(
    root_level="INFO",
    # app_level="DEBUG" if config.debug else "INFO",
    third_party_level="WARNING",
)


@asynccontextmanager
async def lifespan(application: FastAPI):
    async with mongo_lifespan(application):
        # Optional background scheduler (disabled by default).
        # Enable with: AUTO_PRICE_ALL_ENABLED=true
        start_auto_price_all(application)
        try:
            yield
        finally:
            await stop_auto_price_all(application)
            await shutdown_bm_clients()


app = FastAPI(title=config.app_name, lifespan=lifespan)
register_exception_handlers(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For dev. Restrict in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(users_router)
app.include_router(bm_router)
app.include_router(tradein_router)
app.include_router(tradein_orphans_router)
app.include_router(pricing_groups_router)

app.include_router(repaircost_router)
app.include_router(orders_router)

app.include_router(depreciation_router)

app.include_router(bm_pricing_router)

@app.get("/health")
async def health():
    return {"ok": True}


