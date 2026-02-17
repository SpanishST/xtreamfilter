"""Playlist routes — M3U file serving."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response

from app.dependencies import get_config_service, get_m3u_service
from app.services.config_service import ConfigService
from app.services.m3u_service import M3uService

router = APIRouter(tags=["playlist"])


@router.get("/playlist.m3u")
@router.get("/get.php")
async def playlist(
    request: Request,
    m3u: M3uService = Depends(get_m3u_service),
):
    server_url = str(request.base_url).rstrip("/")
    m3u_content = m3u.generate_m3u(server_url)
    return Response(
        content=m3u_content,
        media_type="audio/x-mpegurl",
        headers={"Content-Disposition": 'attachment; filename="playlist.m3u"', "Cache-Control": "no-cache"},
    )


@router.get("/playlist_full.m3u")
@router.get("/full.m3u")
async def playlist_full(
    request: Request,
    cfg: ConfigService = Depends(get_config_service),
    m3u: M3uService = Depends(get_m3u_service),
):
    config = cfg.config
    server_url = str(request.base_url).rstrip("/")
    # Build unfiltered config overlay
    unfiltered_sources = []
    for s in config.get("sources", []):
        s_copy = dict(s)
        s_copy["filters"] = {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        }
        unfiltered_sources.append(s_copy)
    # Temporarily swap sources
    original_sources = config.get("sources", [])
    config["sources"] = unfiltered_sources
    m3u_content = m3u.generate_m3u(server_url)
    config["sources"] = original_sources
    return Response(
        content=m3u_content,
        media_type="audio/x-mpegurl",
        headers={"Content-Disposition": 'attachment; filename="playlist_full.m3u"', "Cache-Control": "no-cache"},
    )
