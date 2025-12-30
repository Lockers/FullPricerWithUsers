from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import config
from app.core.errors import register_exception_handlers
from app.core.logging import init_logging
from app.db.mongo import mongo_lifespan
from app.features.backmarket.pricing_groups.router import router as pricing_groups_router
from app.features.backmarket.sell.router import router as bm_router
from app.features.backmarket.tradein.router import router as tradein_router
from app.features.backmarket.transport.cache import shutdown_bm_clients
from app.features.users.router import router as users_router
from app.features.repaircost.router import router as repaircost_router
from app.features.orders.router import router as orders_router
from app.features.depreciation_api import router as depreciation_router



# Configure logging ON IMPORT so all subsequent module logs behave correctly.
init_logging(
    root_level="INFO",
    # app_level="DEBUG" if config.debug else "INFO",
    third_party_level="WARNING",
)


@asynccontextmanager
async def lifespan(application: FastAPI):
    async with mongo_lifespan(application):
        yield
    await shutdown_bm_clients()


app = FastAPI(title=config.app_name, lifespan=lifespan)
register_exception_handlers(app)

app.include_router(users_router)
app.include_router(bm_router)
app.include_router(tradein_router)
app.include_router(pricing_groups_router)

app.include_router(repaircost_router)
app.include_router(orders_router)

app.include_router(depreciation_router)

@app.get("/health")
async def health():
    return {"ok": True}


