"""Cache management API routes."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends

from app.dependencies import get_cache_service
from app.services.cache_service import CacheService

router = APIRouter(tags=["cache"])


@router.get("/api/cache/status")
async def cache_status(cache: CacheService = Depends(get_cache_service)):
    refresh_progress = cache.load_refresh_progress()
    refresh_in_progress = refresh_progress.get("in_progress", False)

    async with cache._cache_lock:
        last_refresh = cache._api_cache.get("last_refresh")
        sources_cache = cache._api_cache.get("sources", {})

    live_streams = sum(len(s.get("live_streams", [])) for s in sources_cache.values())
    vod_streams = sum(len(s.get("vod_streams", [])) for s in sources_cache.values())
    series_count = sum(len(s.get("series", [])) for s in sources_cache.values())
    live_cats = sum(len(s.get("live_categories", [])) for s in sources_cache.values())
    vod_cats = sum(len(s.get("vod_categories", [])) for s in sources_cache.values())
    series_cats = sum(len(s.get("series_categories", [])) for s in sources_cache.values())

    cache_valid = cache.is_cache_valid()
    ttl = cache.config_service.get_cache_ttl()

    next_refresh = None
    if last_refresh:
        try:
            last_time = datetime.fromisoformat(last_refresh)
            next_refresh = (last_time + timedelta(seconds=ttl)).isoformat()
        except (ValueError, TypeError):
            pass

    sources_info = {}
    for source_id, source_cache in sources_cache.items():
        sources_info[source_id] = {
            "live_streams": len(source_cache.get("live_streams", [])),
            "vod_streams": len(source_cache.get("vod_streams", [])),
            "series": len(source_cache.get("series", [])),
            "last_refresh": source_cache.get("last_refresh"),
        }

    return {
        "last_refresh": last_refresh,
        "next_refresh": next_refresh,
        "cache_valid": cache_valid,
        "refresh_in_progress": refresh_in_progress,
        "refresh_progress": refresh_progress,
        "ttl_seconds": ttl,
        "sources_count": len(sources_cache),
        "counts": {
            "live_categories": live_cats,
            "vod_categories": vod_cats,
            "series_categories": series_cats,
            "live_streams": live_streams,
            "vod_streams": vod_streams,
            "series": series_count,
        },
        "sources": sources_info,
    }


@router.post("/api/cache/refresh")
async def trigger_cache_refresh(cache: CacheService = Depends(get_cache_service)):
    asyncio.create_task(cache.refresh_cache())
    return {"status": "refresh_started", "message": "Cache refresh has been triggered in the background"}


@router.post("/api/cache/cancel-refresh")
async def cancel_cache_refresh(cache: CacheService = Depends(get_cache_service)):
    cache.clear_refresh_progress()
    async with cache._cache_lock:
        cache._api_cache["refresh_in_progress"] = False
    return {"status": "ok", "message": "Refresh state cleared"}


@router.post("/api/cache/clear")
async def clear_cache(cache: CacheService = Depends(get_cache_service)):
    await cache.clear_cache()
    return {"status": "ok", "message": "Cache cleared"}
