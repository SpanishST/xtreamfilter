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

from app.database import DB_NAME, init_db
from app.migrate import run_migration_if_needed
from app.routes import (
    browse_api,
    cache_api,
    cart_api,
    category_api,
    config_api,
    epg,
    filter_api,
    health,
    log_api,
    monitor_api,
    player_api,
    playlist,
    source_api,
    stream_proxy,
    ui,
    xtream_merged,
    xtream_source,
)
from app.services.cache_service import CacheService
from app.services.cart_service import CartService
from app.services.category_service import CategoryService
from app.services.config_service import ConfigService
from app.services.epg_service import EpgService
from app.services.http_client import HttpClientService
from app.services.jellyfin_service import JellyfinService
from app.services.log_service import LogService
from app.services.m3u_service import M3uService
from app.services.monitor_service import MonitorService
from app.services.notification_service import NotificationService
from app.services.xtream_service import XtreamService

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

    while True:
        try:
            cache_was_refreshed = False
            if not cache.is_cache_valid():
                logger.info("Cache expired, triggering refresh…")
                await cache.refresh_cache()
                cache_was_refreshed = True
            else:
                logger.info(f"Cache still valid. Last refresh: {cache._api_cache.get('last_refresh', 'Never')}")

            if cache_was_refreshed:
                async def _run_post_refresh_tasks():
                    coros = [
                        monitor.check_monitored_series(),
                        monitor.check_monitored_movies(),
                    ]
                    if cat:
                        coros.insert(0, cat.refresh_pattern_categories_async())
                    results = await asyncio.gather(*coros, return_exceptions=True)
                    names = (
                        ["category refresh", "series monitoring", "movie monitoring"]
                        if cat
                        else ["series monitoring", "movie monitoring"]
                    )
                    for i, res in enumerate(results):
                        if isinstance(res, Exception):
                            logger.error(f"{names[i]} error in background loop: {res}")

                await _run_post_refresh_tasks()

            if not epg_svc.is_epg_cache_valid():
                logger.info("EPG cache expired, triggering refresh…")
                asyncio.create_task(epg_svc.refresh_epg_cache())

            # Sleep until the cache is about to expire, not a full TTL from now.
            # This prevents worst-case 2×TTL drift when the loop checks just
            # before the cache expires and then sleeps a full TTL.
            ttl = cache.config_service.get_cache_ttl()
            remaining = ttl - cache.get_cache_age()
            sleep_secs = max(30, remaining)
            logger.debug(f"Background loop sleeping {sleep_secs:.0f}s (remaining TTL: {remaining:.0f}s)")
            await asyncio.sleep(sleep_secs)

        except asyncio.CancelledError:
            logger.info("Background refresh task cancelled")
            break
        except Exception as exc:
            logger.error(f"Background refresh error: {exc}")
            await asyncio.sleep(60)


async def download_schedule_loop(cart: CartService) -> None:
    """Periodically check if we're inside a download window and auto-start the worker."""
    print("Download schedule loop started", flush=True)
    logger.info("Download schedule loop started")
    await asyncio.sleep(30)  # Initial delay to let other services start

    while True:
        try:
            schedule = cart.config_service.get_download_schedule()
            logger.info(f"Schedule check: enabled={schedule.get('enabled', False)}, in_window={cart.is_in_download_window()}, cart_size={len(cart.cart)}, queued={[i.get('name') for i in cart.cart if i.get('status') == 'queued']}")
            if schedule.get("enabled", False):
                has_queued = any(i.get("status") == "queued" for i in cart.cart)
                logger.info(f"has_queued={has_queued}")
                if has_queued and cart.is_in_download_window():
                    cart._force_started = False
                    started = cart._try_start_worker()
                    logger.info(f"Download schedule: has_queued={has_queued}, in_window={cart.is_in_download_window()}, started={started}")
                    if started:
                        queued_count = len([i for i in cart.cart if i.get("status") == "queued"])
                        logger.info(f"Download schedule: auto-started worker for {queued_count} queued items")
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            logger.info("Download schedule loop cancelled")
            break
        except Exception as exc:
            logger.error(f"Download schedule loop error: {exc}")
            await asyncio.sleep(60)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise all services, attach them to app.state, start background work."""
    data_dir = os.environ.get("DATA_DIR", "/data" if os.path.exists("/data") else "./data")
    os.makedirs(data_dir, exist_ok=True)

    # --- initialise SQLite DB (schema + JSON migration) ---
    db_path = os.path.join(data_dir, DB_NAME)
    init_db(db_path)
    run_migration_if_needed(db_path, data_dir)

    # --- instantiate services (minimal init before yield) ---
    cfg = ConfigService(data_dir)
    cfg.load()

    log_svc = LogService(db_path, cfg)
    await log_svc.prune_old_logs()

    http = HttpClientService()

    notif = NotificationService(cfg, http)

    cache = CacheService(cfg, http, notif)

    epg_svc = EpgService(cfg, http, cache)
    xtream = XtreamService(cfg, cache, http)
    jellyfin = JellyfinService(cfg, http)
    cat = CategoryService(cfg, cache, notif)
    cart = CartService(cfg, http, notif, xtream, jellyfin)
    monitor = MonitorService(cfg, cache, xtream, notif, cart)
    m3u = M3uService(cfg, cache)

    cache.log_service = log_svc
    cart.log_service = log_svc
    monitor.log_service = log_svc

    # --- attach to app.state for DI ---
    app.state.config_service = cfg
    app.state.http_client = http
    app.state.cache_service = cache
    app.state.epg_service = epg_svc
    app.state.xtream_service = xtream
    app.state.notification_service = notif
    app.state.jellyfin_service = jellyfin
    app.state.category_service = cat
    app.state.cart_service = cart
    app.state.monitor_service = monitor
    app.state.m3u_service = m3u
    app.state.log_service = log_svc

    # --- load cache BEFORE yield so is_cache_valid() sees correct state ---
    # This uses aiosqlite which runs on thread pool, doesn't block event loop
    await cache.load_cache_from_disk_async()

    # --- load monitored series/movies AND cart BEFORE yield so API returns data immediately ---
    monitor.load_monitored()
    monitor.load_monitored_movies()
    cart.load_cart()
    cache.cart_service = cart  # bind so refresh can defer to active downloads

    # --- load EPG cache BEFORE yield so is_epg_cache_valid() sees correct state ---
    epg_svc.load_epg_cache_from_disk()

    # --- background tasks (start before yield so they run during app lifetime) ---
    bg_task = asyncio.create_task(
        background_refresh_loop(cache, epg_svc, monitor, cat)
    )
    schedule_task = asyncio.create_task(
        download_schedule_loop(cart)
    )

    # --- yield here so server can start accepting requests ASAP ---
    yield

    cart.monitor_service = monitor  # late bind for canonical name lookup
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

    # --- shutdown ---
    bg_task.cancel()
    schedule_task.cancel()
    try:
        await bg_task
    except asyncio.CancelledError:
        pass
    try:
        await schedule_task
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
app.include_router(log_api.router)
app.include_router(epg.router)
app.include_router(category_api.router)
app.include_router(browse_api.router)
app.include_router(player_api.router)
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
