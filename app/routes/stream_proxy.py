"""Stream proxy routes — merged / per-source / legacy stream endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse, Response

from app.dependencies import get_cache_service, get_config_service
from app.services.cache_service import CacheService
from app.services.config_service import ConfigService
from app.services.stream_service import proxy_stream
from app.services.xtream_service import decode_virtual_id

router = APIRouter(tags=["stream-proxy"])

_RESERVED_ROUTES = frozenset(("full", "live", "movie", "series", "api", "static", "merged"))


# ------------------------------------------------------------------
# Merged
# ------------------------------------------------------------------


@router.get("/merged/live/{username}/{password}/{stream_id}")
@router.get("/merged/live/{username}/{password}/{stream_id}.{ext}")
@router.get("/merged/{username}/{password}/{stream_id}")
@router.get("/merged/{username}/{password}/{stream_id}.{ext}")
async def proxy_live_stream_merged(
    request: Request, username: str, password: str, stream_id: str, ext: str = "ts",
    cfg: ConfigService = Depends(get_config_service),
):
    source_idx, original_id = decode_virtual_id(stream_id)
    source = cfg.get_source_by_index(source_idx)
    if not source:
        return Response(content="Source not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/{source['username']}/{source['password']}/{original_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="live")
    return RedirectResponse(url=upstream_url, status_code=302)


@router.get("/merged/movie/{username}/{password}/{stream_id}")
@router.get("/merged/movie/{username}/{password}/{stream_id}.{ext}")
async def proxy_movie_stream_merged(
    request: Request, username: str, password: str, stream_id: str, ext: str = "mp4",
    cfg: ConfigService = Depends(get_config_service),
):
    source_idx, original_id = decode_virtual_id(stream_id)
    source = cfg.get_source_by_index(source_idx)
    if not source:
        return Response(content="Source not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/movie/{source['username']}/{source['password']}/{original_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="vod")
    return RedirectResponse(url=upstream_url, status_code=302)


@router.get("/merged/series/{username}/{password}/{stream_id}")
@router.get("/merged/series/{username}/{password}/{stream_id}.{ext}")
async def proxy_series_stream_merged(
    request: Request, username: str, password: str, stream_id: str, ext: str = "mp4",
    cfg: ConfigService = Depends(get_config_service),
):
    source_idx, original_id = decode_virtual_id(stream_id)
    source = cfg.get_source_by_index(source_idx)
    if not source:
        return Response(content="Source not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/series/{source['username']}/{source['password']}/{original_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="series")
    return RedirectResponse(url=upstream_url, status_code=302)


# ------------------------------------------------------------------
# Legacy (no source prefix — auto-detect source by stream_id)
# ------------------------------------------------------------------


@router.get("/live/{username}/{password}/{stream_id}")
@router.get("/live/{username}/{password}/{stream_id}.{ext}")
async def proxy_live_stream(
    request: Request, username: str, password: str, stream_id: str, ext: str = "ts",
    cfg: ConfigService = Depends(get_config_service),
    cache: CacheService = Depends(get_cache_service),
):
    host, upstream_user, upstream_pass = cache.get_source_credentials_for_stream(stream_id, "live")
    if not host:
        return Response(content="Source not found", status_code=404)
    upstream_url = f"{host}/{upstream_user}/{upstream_pass}/{stream_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="live")
    return RedirectResponse(url=upstream_url, status_code=302)


@router.get("/movie/{username}/{password}/{stream_id}")
@router.get("/movie/{username}/{password}/{stream_id}.{ext}")
async def proxy_movie_stream(
    request: Request, username: str, password: str, stream_id: str, ext: str = "mp4",
    cfg: ConfigService = Depends(get_config_service),
    cache: CacheService = Depends(get_cache_service),
):
    host, upstream_user, upstream_pass = cache.get_source_credentials_for_stream(stream_id, "vod")
    if not host:
        return Response(content="Source not found", status_code=404)
    upstream_url = f"{host}/movie/{upstream_user}/{upstream_pass}/{stream_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="vod")
    return RedirectResponse(url=upstream_url, status_code=302)


@router.get("/series/{username}/{password}/{stream_id}")
@router.get("/series/{username}/{password}/{stream_id}.{ext}")
async def proxy_series_stream(
    request: Request, username: str, password: str, stream_id: str, ext: str = "mp4",
    cfg: ConfigService = Depends(get_config_service),
    cache: CacheService = Depends(get_cache_service),
):
    host, upstream_user, upstream_pass = cache.get_source_credentials_for_stream(stream_id, "series")
    if not host:
        return Response(content="Source not found", status_code=404)
    upstream_url = f"{host}/series/{upstream_user}/{upstream_pass}/{stream_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="series")
    return RedirectResponse(url=upstream_url, status_code=302)
