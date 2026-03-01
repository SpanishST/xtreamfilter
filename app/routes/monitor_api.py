"""Series and movie monitoring API routes."""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse

from app.database import DB_NAME, db_connect
from app.dependencies import get_config_service, get_monitor_service, get_xtream_service
from app.services.config_service import ConfigService
from app.services.filter_service import normalize_name
from app.services.monitor_service import MonitorService, _normalize_imdb_id, _normalize_tmdb_id, _safe_episode_num  # noqa: F401 - _normalize_imdb_id used in series dup checks
from app.services.xtream_service import XtreamService

router = APIRouter(tags=["monitor"])


def _normalize_tmdb_id(value) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw.startswith("tmdb:"):
        raw = raw[5:].strip()
    if raw.isdigit():
        return raw
    return None


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
    # New: list of {source_id, series_ref, source_name, category} for multi-source monitoring
    monitor_sources: list[dict] = data.get("monitor_sources") or []
    # Optional list of custom category IDs to restrict monitoring scope
    raw_ccids = data.get("custom_category_ids") or []
    custom_category_ids: list[str] = [str(c) for c in raw_ccids if c] if isinstance(raw_ccids, list) else []
    cover = data.get("cover", "")
    tmdb_id = _normalize_tmdb_id(data.get("tmdb_id") or data.get("tmdb"))
    imdb_id = _normalize_imdb_id(data.get("imdb_id") or data.get("imdb"))
    canonical_name = (data.get("canonical_name") or "").strip() or None
    scope = data.get("scope", "new_only")
    season_filter = data.get("season_filter")
    action = data.get("action", "both")
    backfill = data.get("backfill", "none")          # "none" | "all" | "season"
    backfill_season = data.get("backfill_season")    # season number string

    if not series_name:
        return JSONResponse(status_code=400, content={"error": "Series name is required"})
    if scope not in ("all", "season", "new_only"):
        return JSONResponse(status_code=400, content={"error": "Invalid scope"})
    if action not in ("notify", "download", "both"):
        return JSONResponse(status_code=400, content={"error": "Invalid action (must be notify, download or both)"})
    if scope == "season" and not season_filter:
        return JSONResponse(status_code=400, content={"error": "season_filter required when scope is 'season'"})
    if backfill not in ("none", "all", "season"):
        return JSONResponse(status_code=400, content={"error": "Invalid backfill (must be none, all or season)"})
    if backfill == "season" and not backfill_season:
        return JSONResponse(status_code=400, content={"error": "backfill_season required when backfill is 'season'"})

    # Validate monitor_sources items
    validated_sources: list[dict] = []
    for ms in monitor_sources:
        ms_source_id = str(ms.get("source_id") or "").strip()
        ms_series_ref = str(ms.get("series_ref") or "").strip()
        if ms_source_id and ms_series_ref:
            validated_sources.append({
                "source_id": ms_source_id,
                "series_ref": ms_series_ref,
                "source_name": str(ms.get("source_name") or "").strip() or None,
                "category": str(ms.get("category") or "").strip() or None,
                "series_name": str(ms.get("series_name") or "").strip() or None,
            })

    # Duplicate check
    for m in monitor._monitored_series:
        if source_id:
            if m.get("source_id") == source_id and m.get("series_id") == series_id:
                return JSONResponse(status_code=409, content={"error": "This series is already being monitored from this source"})
        elif validated_sources:
            # Multi-source: check if the same set of sources is already monitored for the same series
            existing_tmdb_id = _normalize_tmdb_id(m.get("tmdb_id") or m.get("tmdb"))
            existing_imdb_id = _normalize_imdb_id(m.get("imdb_id") or m.get("imdb"))
            same_id = (
                (tmdb_id and existing_tmdb_id and tmdb_id == existing_tmdb_id)
                or (imdb_id and existing_imdb_id and imdb_id == existing_imdb_id)
            )
            same_name = normalize_name(m.get("series_name", "")) == normalize_name(series_name)
            if same_id or same_name:
                existing_sources = {
                    (ms.get("source_id"), ms.get("series_ref"))
                    for ms in m.get("monitor_sources", [])
                }
                overlap = any(
                    (vs["source_id"], vs["series_ref"]) in existing_sources
                    for vs in validated_sources
                )
                if overlap:
                    return JSONResponse(status_code=409, content={"error": "One or more selected sources are already monitored for this series"})
        else:
            existing_tmdb_id = _normalize_tmdb_id(m.get("tmdb_id") or m.get("tmdb"))
            existing_imdb_id = _normalize_imdb_id(m.get("imdb_id") or m.get("imdb"))
            same_id = (
                (tmdb_id and existing_tmdb_id and tmdb_id == existing_tmdb_id)
                or (imdb_id and existing_imdb_id and imdb_id == existing_imdb_id)
            )
            same_name = normalize_name(m.get("series_name", "")) == normalize_name(series_name)
            if not m.get("source_id") and (same_id or same_name):
                return JSONResponse(status_code=409, content={"error": "This series is already being monitored (any source)"})

    # Build known episodes snapshot across all selected sources
    known_episodes: list[dict] = []
    if scope in ("new_only", "season"):
        if validated_sources:
            seen_ep_keys: set[tuple] = set()
            for ms in validated_sources:
                eps = await xtream.fetch_series_episodes(ms["source_id"], ms["series_ref"])
                for ep in eps:
                    if scope == "season" and str(ep.get("season")) != str(season_filter):
                        continue
                    ep_key = (str(ep.get("season", "")), _safe_episode_num(ep.get("episode_num")))
                    if ep_key in seen_ep_keys:
                        continue
                    seen_ep_keys.add(ep_key)
                    known_episodes.append({
                        "stream_id": str(ep.get("stream_id", "")),
                        "source_id": ms["source_id"],
                        "season": str(ep.get("season", "")),
                        "episode_num": _safe_episode_num(ep.get("episode_num")),
                    })
        elif source_id:
            episodes = await xtream.fetch_series_episodes(source_id, series_id)
            for ep in episodes:
                if scope == "season" and str(ep.get("season")) != str(season_filter):
                    continue
                known_episodes.append({
                    "stream_id": str(ep.get("stream_id", "")),
                    "source_id": source_id or "",
                    "season": str(ep.get("season", "")),
                    "episode_num": _safe_episode_num(ep.get("episode_num")),
                })
        else:
            matches = monitor.find_series_across_sources(series_name, tmdb_id=tmdb_id, imdb_id=imdb_id)
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
        "canonical_name": canonical_name,
        "series_id": series_id,
        "source_id": source_id,
        "source_name": source_name,
        "source_category": source_category,
        "monitor_sources": validated_sources,
        "custom_category_ids": custom_category_ids,
        "cover": cover,
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
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

    # Immediately queue existing episodes if a backfill option was requested
    queued_count = 0
    if backfill != "none":
        queued_count = await monitor.backfill_episodes(
            entry,
            backfill=backfill,
            backfill_season=str(backfill_season) if backfill_season else None,
        )
        if queued_count:
            # Save again so downloaded_episodes list is persisted
            monitor.save_monitored()

    return {"status": "ok", "entry": entry, "backfill_queued": queued_count}


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
            if "tmdb_id" in data or "tmdb" in data:
                entry["tmdb_id"] = _normalize_tmdb_id(data.get("tmdb_id") or data.get("tmdb"))
            if "imdb_id" in data or "imdb" in data:
                entry["imdb_id"] = _normalize_imdb_id(data.get("imdb_id") or data.get("imdb"))
            if "action" in data and data["action"] in ("notify", "download", "both"):
                entry["action"] = data["action"]
            if "canonical_name" in data:
                entry["canonical_name"] = (data["canonical_name"] or "").strip() or None
            if "monitor_sources" in data:
                validated: list[dict] = []
                for ms in (data.get("monitor_sources") or []):
                    ms_sid = str(ms.get("source_id") or "").strip()
                    ms_ref = str(ms.get("series_ref") or "").strip()
                    if ms_sid and ms_ref:
                        validated.append({
                            "source_id": ms_sid,
                            "series_ref": ms_ref,
                            "source_name": str(ms.get("source_name") or "").strip() or None,
                            "category": str(ms.get("category") or "").strip() or None,
                            "series_name": str(ms.get("series_name") or "").strip() or None,
                        })
                entry["monitor_sources"] = validated
                # Clear legacy source fields when switching to explicit multi-source list
                if validated:
                    entry["source_id"] = None
                    entry["source_name"] = None
                    entry["source_category"] = None
            if "custom_category_ids" in data:
                raw = data["custom_category_ids"]
                entry["custom_category_ids"] = [str(c) for c in raw if c] if isinstance(raw, list) else []
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
    if monitor._check_in_progress:
        return JSONResponse(status_code=409, content={"status": "skipped", "message": "Monitoring check already in progress"})

    async def _run_both():
        await monitor.check_monitored_series()
        await monitor.check_monitored_movies()

    asyncio.create_task(_run_both())
    return {"status": "ok", "message": "Monitoring check started"}


@router.get("/api/monitor/series-meta/{source_id}/{series_id}")
async def get_series_meta(
    source_id: str,
    series_id: str,
    xtream: XtreamService = Depends(get_xtream_service),
):
    """Fetch TMDB metadata for a series to pre-fill the canonical name."""
    info = await xtream.fetch_series_info(source_id, series_id)
    if not info:
        return JSONResponse(status_code=404, content={"error": "Series info not found"})
    name = (info.get("name") or info.get("title") or "").strip()
    return {
        "name": name,
        "tmdb_id": _normalize_tmdb_id(info.get("tmdb_id") or info.get("tmdb")),
        "imdb_id": _normalize_imdb_id(info.get("imdb_id") or info.get("imdb")),
        "plot": (info.get("plot") or "").strip(),
    }


@router.get("/api/monitor/{monitor_id}/episodes")
async def preview_monitor_episodes(
    monitor_id: str,
    monitor: MonitorService = Depends(get_monitor_service),
    xtream: XtreamService = Depends(get_xtream_service),
):
    return await monitor.preview_episodes(monitor_id)


# ===========================================================================
# Movie monitoring endpoints
# ===========================================================================


@router.get("/api/monitor/movies")
async def list_monitored_movies(monitor: MonitorService = Depends(get_monitor_service)):
    return {"movies": monitor._monitored_movies}


@router.get("/api/monitor/movie-lookup")
async def movie_lookup(
    q: str = Query("", description="Movie name or tmdb:<id>"),
    monitor: MonitorService = Depends(get_monitor_service),
):
    """Search the VOD cache for movies matching a name or TMDB ID.
    The result powers the autocomplete in the 'Add movie monitor' form.
    """
    q = q.strip()
    if not q:
        return {"results": []}

    tmdb_id: str | None = None
    movie_name = q

    if q.lower().startswith("tmdb:"):
        raw = q[5:].strip()
        if raw.isdigit():
            tmdb_id = raw
            movie_name = ""
    elif q.isdigit() and len(q) >= 4:
        # Bare numeric string — treat as TMDB ID
        tmdb_id = q
        movie_name = ""

    matches = monitor.find_movie_across_sources(
        movie_name,
        tmdb_id=tmdb_id,
        limit=30,
    )

    # Deduplicate by tmdb_id (prefer id-match) then by normalised name
    seen_tmdb: set[str] = set()
    seen_names: set[str] = set()
    deduped: list[dict] = []
    for m in matches:
        tid = m.get("tmdb_id")
        nkey = normalize_name(m["name"])
        if tid:
            if tid in seen_tmdb:
                continue
            seen_tmdb.add(tid)
        else:
            if nkey in seen_names:
                continue
            seen_names.add(nkey)
        deduped.append({
            "name": m["name"],
            "tmdb_id": m.get("tmdb_id"),
            "cover": m.get("cover", ""),
            "source_id": m["source_id"],
            "stream_id": m["stream_id"],
            "container_extension": m.get("container_extension", "mkv"),
            "matched_by": m.get("matched_by"),
        })

    return {"results": deduped[:20]}


@router.get("/api/monitor/custom-categories")
async def get_monitor_custom_categories(
    config_svc: ConfigService = Depends(get_config_service),
):
    """Return user-defined custom categories for use as monitoring channels."""
    conn = db_connect(os.path.join(config_svc.data_dir, DB_NAME))
    try:
        rows = conn.execute(
            "SELECT id, name, icon, content_types FROM custom_categories ORDER BY sort_order, name"
        ).fetchall()
    finally:
        conn.close()

    categories = []
    for row in rows:
        ct_raw = row["content_types"]
        try:
            content_types = json.loads(ct_raw) if ct_raw else []
        except Exception:
            content_types = []
        categories.append({
            "id": row["id"],
            "name": row["name"],
            "icon": row["icon"] or "📁",
            "content_types": content_types,
        })

    return {"categories": categories}


@router.get("/api/monitor/vod-categories")
async def get_vod_categories(
    config_svc: ConfigService = Depends(get_config_service),
    monitor: MonitorService = Depends(get_monitor_service),
):
    """Return VOD categories per enabled source (used for per-source category filter UI)."""
    cfg = config_svc.config
    enabled_ids = [s["id"] for s in cfg.get("sources", []) if s.get("enabled", True)]
    if not enabled_ids:
        return {"by_source": {}}

    conn = db_connect(os.path.join(config_svc.data_dir, DB_NAME))
    try:
        placeholders = ",".join("?" * len(enabled_ids))
        rows = conn.execute(
            f"SELECT source_id, category_id, category_name FROM source_categories "
            f"WHERE content_type = 'vod' AND source_id IN ({placeholders}) "
            f"ORDER BY source_id, category_name",
            enabled_ids,
        ).fetchall()
    finally:
        conn.close()

    by_source: dict[str, list] = {}
    for row in rows:
        sid = row["source_id"]
        by_source.setdefault(sid, []).append({
            "category_id": row["category_id"],
            "category_name": row["category_name"] or row["category_id"],
        })

    # Build source info for the UI
    source_info = [
        {
            "id": s["id"],
            "name": s.get("name", s["id"]),
            "vod_group_filters": s.get("filters", {}).get("vod", {}).get("groups", []),
        }
        for s in cfg.get("sources", [])
        if s.get("enabled", True)
    ]

    return {"by_source": by_source, "sources": source_info}


@router.post("/api/monitor/movies")
async def add_monitored_movie(
    request: Request,
    monitor: MonitorService = Depends(get_monitor_service),
):
    data = await request.json()
    movie_name = (data.get("movie_name") or "").strip()
    tmdb_id = _normalize_tmdb_id(data.get("tmdb_id"))
    cover = (data.get("cover") or "").strip()
    canonical_name = (data.get("canonical_name") or "").strip() or None
    action = data.get("action", "notify")
    monitor_sources_raw: list[dict] = data.get("monitor_sources") or []

    if not movie_name and not tmdb_id:
        return JSONResponse(status_code=400, content={"error": "movie_name or tmdb_id is required"})
    if action not in ("notify", "download", "both"):
        return JSONResponse(status_code=400, content={"error": "Invalid action (must be notify, download or both)"})

    # Optional list of custom category IDs to restrict monitoring scope
    raw_mccids = data.get("custom_category_ids") or []
    movie_custom_category_ids: list[str] = [str(c) for c in raw_mccids if c] if isinstance(raw_mccids, list) else []

    # Validate sources
    validated_sources: list[dict] = []
    for ms in monitor_sources_raw:
        sid = str(ms.get("source_id") or "").strip()
        if sid:
            cf = ms.get("category_filter") or []
            if isinstance(cf, str):
                cf = [cf] if cf else []
            validated_sources.append({
                "source_id": sid,
                "source_name": str(ms.get("source_name") or "").strip() or None,
                "category_filter": cf,
            })

    # Duplicate check (same TMDB id or same normalised name)
    for m in monitor._monitored_movies:
        existing_tmdb = m.get("tmdb_id")
        existing_name_n = normalize_name(m.get("movie_name", ""))
        if tmdb_id and existing_tmdb and tmdb_id == existing_tmdb:
            return JSONResponse(status_code=409, content={"error": "A movie with this TMDB ID is already being monitored"})
        if movie_name and existing_name_n and normalize_name(movie_name) == existing_name_n:
            return JSONResponse(status_code=409, content={"error": "A movie with this title is already being monitored"})

    entry = {
        "id": str(uuid.uuid4()),
        "movie_name": movie_name or f"TMDB:{tmdb_id}",
        "canonical_name": canonical_name,
        "tmdb_id": tmdb_id,
        "cover": cover,
        "action": action,
        "enabled": True,
        "status": "watching",
        "monitor_sources": validated_sources,
        "custom_category_ids": movie_custom_category_ids,
        "created_at": datetime.now().isoformat(),
        "last_checked": None,
    }

    monitor._monitored_movies.append(entry)
    monitor.save_monitored_movies()
    return {"status": "ok", "entry": entry}


@router.put("/api/monitor/movies/{movie_id}")
async def update_monitored_movie(
    movie_id: str,
    request: Request,
    monitor: MonitorService = Depends(get_monitor_service),
):
    data = await request.json()
    for entry in monitor._monitored_movies:
        if entry.get("id") != movie_id:
            continue
        if "enabled" in data:
            entry["enabled"] = bool(data["enabled"])
        if "action" in data and data["action"] in ("notify", "download", "both"):
            entry["action"] = data["action"]
        if "status" in data and data["status"] in ("watching", "found", "downloaded"):
            entry["status"] = data["status"]
        if "canonical_name" in data:
            entry["canonical_name"] = (data["canonical_name"] or "").strip() or None
        if "movie_name" in data and data["movie_name"].strip():
            entry["movie_name"] = data["movie_name"].strip()
        if "tmdb_id" in data:
            entry["tmdb_id"] = _normalize_tmdb_id(data["tmdb_id"])
        if "cover" in data:
            entry["cover"] = (data["cover"] or "").strip()
        if "monitor_sources" in data:
            validated: list[dict] = []
            for ms in (data.get("monitor_sources") or []):
                sid = str(ms.get("source_id") or "").strip()
                if sid:
                    cf = ms.get("category_filter") or []
                    if isinstance(cf, str):
                        cf = [cf] if cf else []
                    validated.append({
                        "source_id": sid,
                        "source_name": str(ms.get("source_name") or "").strip() or None,
                        "category_filter": cf,
                    })
            entry["monitor_sources"] = validated
        if "custom_category_ids" in data:
            raw = data["custom_category_ids"]
            entry["custom_category_ids"] = [str(c) for c in raw if c] if isinstance(raw, list) else []
        monitor.save_monitored_movies()
        return {"status": "ok", "entry": entry}
    return JSONResponse(status_code=404, content={"error": "Movie monitor entry not found"})


@router.delete("/api/monitor/movies/{movie_id}")
async def delete_monitored_movie(
    movie_id: str,
    monitor: MonitorService = Depends(get_monitor_service),
):
    original_len = len(monitor._monitored_movies)
    monitor._monitored_movies[:] = [m for m in monitor._monitored_movies if m.get("id") != movie_id]
    if len(monitor._monitored_movies) == original_len:
        return JSONResponse(status_code=404, content={"error": "Movie monitor entry not found"})
    monitor.save_monitored_movies()
    return {"status": "ok"}
