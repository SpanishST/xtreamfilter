"""XtreamFilter — modular FastAPI application.

The entire codebase is split into models, services, and route modules.
This file only wires everything together via the FastAPI lifespan.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.services.config_service import ConfigService
from app.services.http_client import HttpClientService
from app.services.cache_service import CacheService
from app.services.epg_service import EpgService
from app.services.xtream_service import XtreamService
from app.services.notification_service import NotificationService
from app.services.category_service import CategoryService
from app.services.cart_service import CartService
from app.services.monitor_service import MonitorService
from app.services.m3u_service import M3uService

from app.routes import (
    ui,
    filter_api,
    source_api,
    playlist,
    browse_api,
    category_api,
    xtream_merged,
    stream_proxy,
    xtream_source,
    epg,
    config_api,
    cache_api,
    cart_api,
    monitor_api,
    health,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Background refresh loop
# ---------------------------------------------------------------------------

async def background_refresh_loop(
    cache: CacheService,
    epg_svc: EpgService,
    monitor: MonitorService,
    cat: CategoryService | None = None,
) -> None:
    """Periodically refresh cache, EPG, and run series monitoring."""
    logger.info("Background refresh task started")
    await asyncio.sleep(10)

    async def _on_cache_refreshed():
        if cat:
            await cat.refresh_pattern_categories_async()

    while True:
        try:
            if not cache.is_cache_valid():
                logger.info("Cache expired, triggering refresh…")
                await cache.refresh_cache(on_cache_refreshed=_on_cache_refreshed)
            else:
                logger.info(f"Cache still valid. Last refresh: {cache._api_cache.get('last_refresh', 'Never')}")

            try:
                await monitor.check_monitored_series()
            except Exception as exc:
                logger.error(f"Series monitoring error in background loop: {exc}")

            await asyncio.sleep(cache.config_service.get_cache_ttl())

        except asyncio.CancelledError:
            logger.info("Background refresh task cancelled")
            break
        except Exception as exc:
            logger.info(f"Background refresh error: {exc}")
            await asyncio.sleep(60)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise all services, attach them to app.state, start background work."""
    data_dir = os.environ.get("DATA_DIR", "/data" if os.path.exists("/data") else "./data")
    os.makedirs(data_dir, exist_ok=True)

    # --- instantiate services ---
    cfg = ConfigService(data_dir)
    cfg.load()

    http = HttpClientService()

    cache = CacheService(cfg, http)
    cache.load_cache_from_disk()

    epg_svc = EpgService(cfg, http, cache)
    epg_svc.load_epg_cache_from_disk()

    xtream = XtreamService(cfg, cache, http)

    notif = NotificationService(cfg, http)

    cat = CategoryService(cfg, cache, notif)

    cart = CartService(cfg, http, notif, xtream)
    cart.load_cart()
    # Recover stuck downloads
    recovered = 0
    for item in cart._download_cart:
        if item.get("status") == "downloading":
            item["status"] = "queued"
            item["progress"] = 0
            item["error"] = None
            recovered += 1
    if recovered:
        cart.save_cart()
        logger.info(f"Recovered {recovered} stuck download(s) back to queued")

    monitor = MonitorService(cfg, cache, xtream, notif, cart)
    monitor.load_monitored()

    m3u = M3uService(cfg, cache)

    # --- attach to app.state for DI ---
    app.state.config_service = cfg
    app.state.http_client = http
    app.state.cache_service = cache
    app.state.epg_service = epg_svc
    app.state.xtream_service = xtream
    app.state.notification_service = notif
    app.state.category_service = cat
    app.state.cart_service = cart
    app.state.monitor_service = monitor
    app.state.m3u_service = m3u

    # --- background tasks ---
    bg_task = asyncio.create_task(
        background_refresh_loop(cache, epg_svc, monitor, cat)
    )

    async def _initial_on_cache_refreshed():
        await cat.refresh_pattern_categories_async()

    if not cache.is_cache_valid():
        logger.info("Cache is empty or invalid, triggering initial refresh…")
        asyncio.create_task(cache.refresh_cache(on_cache_refreshed=_initial_on_cache_refreshed))

    if not epg_svc.is_epg_cache_valid():
        logger.info("EPG cache is empty or invalid, triggering initial EPG refresh…")
        asyncio.create_task(epg_svc.refresh_epg_cache())

    yield

    # --- shutdown ---
    bg_task.cancel()
    try:
        await bg_task
    except asyncio.CancelledError:
        pass
    await http.close()
    logger.info("Application shutdown complete")


# ---------------------------------------------------------------------------
# Create the FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(title="XtreamFilter", lifespan=lifespan)


# Middleware — ensure UTF-8 charset on JSON responses
@app.middleware("http")
async def add_utf8_charset(request: Request, call_next):
    response = await call_next(request)
    ct = response.headers.get("content-type", "")
    if "application/json" in ct and "charset" not in ct:
        response.headers["content-type"] = "application/json; charset=utf-8"
    return response


# ---------------------------------------------------------------------------
# Register routers  (order matters: specific routes before catch-all)
# ---------------------------------------------------------------------------

app.include_router(ui.router)
app.include_router(health.router)
app.include_router(filter_api.router)
app.include_router(source_api.router)
app.include_router(config_api.router)
app.include_router(cache_api.router)
app.include_router(cart_api.router)
app.include_router(monitor_api.router)
app.include_router(epg.router)
app.include_router(category_api.router)
app.include_router(browse_api.router)
app.include_router(playlist.router)
app.include_router(xtream_merged.router)
app.include_router(stream_proxy.router)
# xtream_source has catch-all /{source_route}/… patterns — must be last
app.include_router(xtream_source.router)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)
