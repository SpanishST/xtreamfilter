"""Series monitoring API routes."""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.dependencies import get_monitor_service, get_xtream_service
from app.services.filter_service import normalize_name
from app.services.monitor_service import MonitorService, _safe_episode_num
from app.services.xtream_service import XtreamService

router = APIRouter(tags=["monitor"])


@router.get("/api/monitor")
async def get_monitored(monitor: MonitorService = Depends(get_monitor_service)):
    return {"series": monitor._monitored_series}


@router.post("/api/monitor")
async def add_monitored(
    request: Request,
    monitor: MonitorService = Depends(get_monitor_service),
    xtream: XtreamService = Depends(get_xtream_service),
):
    data = await request.json()
    series_name = data.get("series_name", "").strip()
    series_id = str(data.get("series_id", ""))
    source_id = data.get("source_id") or None
    source_name = data.get("source_name") or None
    source_category = data.get("source_category") or None
    cover = data.get("cover", "")
    scope = data.get("scope", "new_only")
    season_filter = data.get("season_filter")
    action = data.get("action", "both")

    if not series_name:
        return JSONResponse(status_code=400, content={"error": "Series name is required"})
    if scope not in ("all", "season", "new_only"):
        return JSONResponse(status_code=400, content={"error": "Invalid scope"})
    if action not in ("notify", "download", "both"):
        return JSONResponse(status_code=400, content={"error": "Invalid action (must be notify, download or both)"})
    if scope == "season" and not season_filter:
        return JSONResponse(status_code=400, content={"error": "season_filter required when scope is 'season'"})

    # Duplicate check
    for m in monitor._monitored_series:
        if source_id:
            if m.get("source_id") == source_id and m.get("series_id") == series_id:
                return JSONResponse(status_code=409, content={"error": "This series is already being monitored from this source"})
        else:
            if not m.get("source_id") and normalize_name(m.get("series_name", "")) == normalize_name(series_name):
                return JSONResponse(status_code=409, content={"error": "This series is already being monitored (any source)"})

    # Build known episodes snapshot
    known_episodes: list[dict] = []
    if scope in ("new_only", "season"):
        if source_id:
            episodes = await xtream.fetch_series_episodes(source_id, series_id)
        else:
            matches = monitor.find_series_across_sources(series_name)
            episodes = []
            for m in matches:
                eps = await xtream.fetch_series_episodes(m["source_id"], m["series_id"])
                if eps:
                    episodes = eps
                    break

        for ep in episodes:
            if scope == "season" and str(ep.get("season")) != str(season_filter):
                continue
            known_episodes.append({
                "stream_id": str(ep.get("stream_id", "")),
                "source_id": source_id or "",
                "season": str(ep.get("season", "")),
                "episode_num": _safe_episode_num(ep.get("episode_num")),
            })

    entry = {
        "id": str(uuid.uuid4()),
        "series_name": series_name,
        "series_id": series_id,
        "source_id": source_id,
        "source_name": source_name,
        "source_category": source_category,
        "cover": cover,
        "scope": scope,
        "season_filter": str(season_filter) if season_filter else None,
        "action": action,
        "enabled": True,
        "known_episodes": known_episodes,
        "downloaded_episodes": [],
        "created_at": datetime.now().isoformat(),
        "last_checked": None,
        "last_new_count": 0,
    }

    monitor._monitored_series.append(entry)
    monitor.save_monitored()
    return {"status": "ok", "entry": entry}


@router.put("/api/monitor/{monitor_id}")
async def update_monitored(monitor_id: str, request: Request, monitor: MonitorService = Depends(get_monitor_service)):
    data = await request.json()
    for entry in monitor._monitored_series:
        if entry.get("id") == monitor_id:
            if "enabled" in data:
                entry["enabled"] = bool(data["enabled"])
            if "scope" in data and data["scope"] in ("all", "season", "new_only"):
                entry["scope"] = data["scope"]
            if "season_filter" in data:
                entry["season_filter"] = str(data["season_filter"]) if data["season_filter"] else None
            if "source_id" in data:
                entry["source_id"] = data["source_id"] or None
                entry["source_name"] = data.get("source_name") or None
            if "action" in data and data["action"] in ("notify", "download", "both"):
                entry["action"] = data["action"]
            monitor.save_monitored()
            return {"status": "ok", "entry": entry}
    return JSONResponse(status_code=404, content={"error": "Monitored entry not found"})


@router.delete("/api/monitor/{monitor_id}")
async def delete_monitored(monitor_id: str, monitor: MonitorService = Depends(get_monitor_service)):
    original_len = len(monitor._monitored_series)
    monitor._monitored_series[:] = [m for m in monitor._monitored_series if m.get("id") != monitor_id]
    if len(monitor._monitored_series) == original_len:
        return JSONResponse(status_code=404, content={"error": "Monitored entry not found"})
    monitor.save_monitored()
    return {"status": "ok"}


@router.post("/api/monitor/check")
async def trigger_monitor_check(monitor: MonitorService = Depends(get_monitor_service)):
    asyncio.create_task(monitor.check_monitored_series())
    return {"status": "ok", "message": "Monitoring check started"}


@router.get("/api/monitor/{monitor_id}/episodes")
async def preview_monitor_episodes(
    monitor_id: str,
    monitor: MonitorService = Depends(get_monitor_service),
    xtream: XtreamService = Depends(get_xtream_service),
):
    return await monitor.preview_episodes(monitor_id)
