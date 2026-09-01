"""Per-source Xtream API, M3U, EPG, and stream proxy routes."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from lxml import etree

from app.dependencies import (
    get_cache_service,
    get_config_service,
    get_http_client,
    get_m3u_service,
)
from app.services.cache_service import CacheService
from app.services.config_service import ConfigService
from app.services.filter_service import (
    build_category_map,
    safe_copy_category,
    safe_get_category_name,
    should_include,
)
from app.services.http_client import HttpClientService
from app.services.m3u_service import M3uService
from app.services.stream_service import proxy_stream

logger = logging.getLogger(__name__)
router = APIRouter(tags=["xtream-source"])

_RESERVED = frozenset(("full", "live", "movie", "series", "api", "static", "merged"))


def _get_filtered_source_categories(action: str, source: dict, cache: CacheService) -> list[dict]:
    """Build a filtered source category response off the event loop."""
    ct = action.split("_")[1]
    group_filters = source.get("filters", {}).get(ct, {}).get("groups", [])
    prefix = source.get("prefix", "")
    result = []
    for category in cache.get_cached(f"{ct}_categories", source.get("id")):
        category_name = safe_get_category_name(category)
        if should_include(category_name, group_filters):
            category_copy = safe_copy_category(category)
            if prefix:
                category_copy["category_name"] = f"{prefix}{category_name}"
            result.append(category_copy)
    return result


def _get_filtered_source_streams(action: str, source: dict, cache: CacheService) -> list[dict]:
    """Filter a source catalog and hydrate matching rows off the event loop."""
    if action == "get_live_streams":
        ct = "live"
    elif action == "get_vod_streams":
        ct = "vod"
    else:
        ct = "series"
    source_id = source.get("id")
    content_filters = source.get("filters", {}).get(ct, {})
    group_filters = content_filters.get("groups", [])
    channel_filters = content_filters.get("channels", [])
    stream_key = "series" if ct == "series" else f"{ct}_streams"
    streams = cache.get_cached(stream_key, source_id)
    categories = cache.get_cached(f"{ct}_categories", source_id)
    category_map = build_category_map(categories)
    allowed_category_ids = {
        category_id for category_id, group_name in category_map.items()
        if should_include(group_name, group_filters)
    }
    include_unknown_group = should_include("", group_filters)

    id_field = "series_id" if ct == "series" else "stream_id"
    matching_ids = []
    for stream in streams:
        category_id = str(stream.get("category_id", ""))
        group_allowed = category_id in allowed_category_ids if category_id in category_map else include_unknown_group
        if not group_allowed or (channel_filters and not should_include(stream.get("name", ""), channel_filters)):
            continue
        stream_id = str(stream.get(id_field, ""))
        if stream_id:
            matching_ids.append(stream_id)

    full_data = cache.get_streams_full_data_batch(ct, source_id, matching_ids)
    result = []
    for stream_id in matching_ids:
        stream = full_data.get(stream_id)
        if not stream:
            continue
        if ct == "live":
            epg_id = stream.get("epg_channel_id", "")
            if epg_id:
                stream["epg_channel_id"] = f"{source_id}_{epg_id}".lower()
        result.append(stream)
    return result


def _get_full_source_categories(action: str, source: dict, cache: CacheService) -> list[dict]:
    """Build an unfiltered source category response off the event loop."""
    ct = action.split("_")[1]
    categories = cache.get_cached(f"{ct}_categories", source.get("id"))
    prefix = source.get("prefix", "")
    if not prefix:
        return categories
    result = []
    for category in categories:
        category_copy = safe_copy_category(category)
        category_copy["category_name"] = f"{prefix}{safe_get_category_name(category)}"
        result.append(category_copy)
    return result


def _get_full_source_streams(action: str, source: dict, cache: CacheService) -> list[dict]:
    """Hydrate an unfiltered source catalog off the event loop."""
    if action == "get_live_streams":
        ct, key, id_field = "live", "live_streams", "stream_id"
    elif action == "get_vod_streams":
        ct, key, id_field = "vod", "vod_streams", "stream_id"
    else:
        ct, key, id_field = "series", "series", "series_id"
    source_id = source.get("id")
    streams = cache.get_cached(key, source_id)
    stream_ids = [str(item.get(id_field, "")) for item in streams if item.get(id_field)]
    full_data = cache.get_streams_full_data_batch(ct, source_id, stream_ids)
    result = []
    for stream_id in stream_ids:
        stream = full_data.get(stream_id)
        if not stream:
            continue
        if ct == "live":
            epg_id = stream.get("epg_channel_id", "")
            if epg_id:
                stream["epg_channel_id"] = f"{source_id}_{epg_id}".lower()
        result.append(stream)
    return result


# ------------------------------------------------------------------
# Filtered Xtream API
# ------------------------------------------------------------------


@router.get("/{source_route}/player_api.php")
async def player_api_source(
    source_route: str,
    request: Request,
    cfg: ConfigService = Depends(get_config_service),
    http: HttpClientService = Depends(get_http_client),
    cache: CacheService = Depends(get_cache_service),
):
    if source_route in _RESERVED:
        return Response(content="Not found", status_code=404)
    source = cfg.get_source_by_route(source_route)
    if not source:
        return JSONResponse({"error": f"Source route '{source_route}' not found"}, status_code=404)

    action = request.query_params.get("action", "")
    client = await http.get_client()

    try:
        host = source["host"].rstrip("/")

        if not action:
            response = await client.get(
                f"{host}/player_api.php",
                params={"username": source["username"], "password": source["password"]},
                timeout=30.0,
            )
            if response.status_code == 200:
                data = response.json()
                if "server_info" in data:
                    data["server_info"]["url"] = str(request.base_url).rstrip("/") + f"/{source_route}"
                    data["server_info"]["port"] = "80"
                    data["server_info"]["https_port"] = "443"
                    data["server_info"]["rtmp_port"] = "80"
                return data
            return Response(content=response.content, status_code=response.status_code)

        elif action in ("get_live_categories", "get_vod_categories", "get_series_categories"):
            return await asyncio.to_thread(_get_filtered_source_categories, action, source, cache)

        elif action in ("get_live_streams", "get_vod_streams"):
            return await asyncio.to_thread(_get_filtered_source_streams, action, source, cache)

        elif action == "get_series":
            return await asyncio.to_thread(_get_filtered_source_streams, "get_series", source, cache)

        elif action in ("get_series_info", "get_vod_info"):
            param_key = "series_id" if action == "get_series_info" else "vod_id"
            param_val = request.query_params.get(param_key, "")
            params = {"username": source["username"], "password": source["password"], "action": action, param_key: param_val}
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

        else:
            params = dict(request.query_params)
            params["username"] = source["username"]
            params["password"] = source["password"]
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

    except httpx.TimeoutException:
        return JSONResponse({"error": "Upstream server timeout"}, status_code=504)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ------------------------------------------------------------------
# Full (unfiltered) Xtream API
# ------------------------------------------------------------------


@router.get("/{source_route}/full/player_api.php")
async def player_api_source_full(
    source_route: str,
    request: Request,
    cfg: ConfigService = Depends(get_config_service),
    http: HttpClientService = Depends(get_http_client),
    cache: CacheService = Depends(get_cache_service),
):
    if source_route in _RESERVED:
        return Response(content="Not found", status_code=404)
    source = cfg.get_source_by_route(source_route)
    if not source:
        return JSONResponse({"error": f"Source route '{source_route}' not found"}, status_code=404)

    action = request.query_params.get("action", "")
    client = await http.get_client()

    try:
        host = source["host"].rstrip("/")

        if not action:
            response = await client.get(
                f"{host}/player_api.php",
                params={"username": source["username"], "password": source["password"]},
                timeout=30.0,
            )
            if response.status_code == 200:
                data = response.json()
                if "server_info" in data:
                    data["server_info"]["url"] = str(request.base_url).rstrip("/") + f"/{source_route}/full"
                    data["server_info"]["port"] = "80"
                    data["server_info"]["https_port"] = "443"
                    data["server_info"]["rtmp_port"] = "80"
                return data
            return Response(content=response.content, status_code=response.status_code)

        elif action in ("get_live_categories", "get_vod_categories", "get_series_categories"):
            return await asyncio.to_thread(_get_full_source_categories, action, source, cache)

        elif action in ("get_live_streams", "get_vod_streams", "get_series"):
            return await asyncio.to_thread(_get_full_source_streams, action, source, cache)

        elif action in ("get_series_info", "get_vod_info"):
            param_key = "series_id" if action == "get_series_info" else "vod_id"
            param_val = request.query_params.get(param_key, "")
            params = {"username": source["username"], "password": source["password"], "action": action, param_key: param_val}
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

        else:
            params = dict(request.query_params)
            params["username"] = source["username"]
            params["password"] = source["password"]
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

    except httpx.TimeoutException:
        return JSONResponse({"error": "Upstream server timeout"}, status_code=504)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ------------------------------------------------------------------
# Per-source M3U
# ------------------------------------------------------------------


@router.get("/{source_route}/playlist.m3u")
@router.get("/{source_route}/get.php")
async def playlist_source(
    source_route: str,
    request: Request,
    cfg: ConfigService = Depends(get_config_service),
    m3u: M3uService = Depends(get_m3u_service),
):
    if source_route in _RESERVED:
        return Response(content="Not found", status_code=404)
    source = cfg.get_source_by_route(source_route)
    if not source:
        return JSONResponse({"error": f"Source route '{source_route}' not found"}, status_code=404)
    server_url = str(request.base_url).rstrip("/")
    m3u_content = m3u.generate_m3u_for_source(source, server_url, source_route)
    return Response(
        content=m3u_content,
        media_type="audio/x-mpegurl",
        headers={"Content-Disposition": f'attachment; filename="{source_route}_playlist.m3u"', "Cache-Control": "no-cache"},
    )


# ------------------------------------------------------------------
# Per-source XMLTV/EPG
# ------------------------------------------------------------------


@router.get("/{source_route}/xmltv.php")
@router.get("/{source_route}/epg.xml")
async def xmltv_source(
    source_route: str,
    cfg: ConfigService = Depends(get_config_service),
):
    if source_route == "merged":
        from app.routes.epg import get_merged_xmltv
        return await get_merged_xmltv()

    if source_route in _RESERVED:
        return Response(content="Not found", status_code=404)
    source = cfg.get_source_by_route(source_route)
    if not source:
        return JSONResponse({"error": f"Source route '{source_route}' not found"}, status_code=404)

    source_id = source.get("id")
    source_prefix = f"{source_id}_"
    cache_path = Path("/data/epg_cache.xml")

    if not cache_path.exists():
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="XtreamFilter"><!-- No EPG cache --></tv>',
            media_type="application/xml",
        )
    try:
        parser = etree.XMLParser(recover=True)
        tree = etree.parse(str(cache_path), parser)
        root = tree.getroot()
        new_root = etree.Element("tv", attrib={"generator-info-name": f"XtreamFilter - {source.get('name', source_id)}"})
        for channel in root.findall("channel"):
            if channel.get("id", "").startswith(source_prefix):
                new_channel = etree.Element("channel", id=channel.get("id", ""))
                for child in channel:
                    new_channel.append(child)
                new_root.append(new_channel)
        for programme in root.findall("programme"):
            if programme.get("channel", "").startswith(source_prefix):
                new_root.append(programme)
        result = etree.tostring(new_root, encoding="UTF-8", xml_declaration=True, pretty_print=False)
        return Response(content=result, media_type="application/xml", headers={"Content-Type": "application/xml; charset=utf-8"})
    except Exception as e:
        logger.error(f"Error processing EPG for source '{source_route}': {e}")
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="XtreamFilter"><!-- EPG error --></tv>',
            media_type="application/xml",
        )


# ------------------------------------------------------------------
# Per-source stream proxies (filtered)
# ------------------------------------------------------------------


@router.get("/{source_route}/live/{username}/{password}/{stream_id}")
@router.get("/{source_route}/live/{username}/{password}/{stream_id}.{ext}")
@router.get("/{source_route}/{username}/{password}/{stream_id}")
@router.get("/{source_route}/{username}/{password}/{stream_id}.{ext}")
async def proxy_live_stream_source(
    request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "ts",
    cfg: ConfigService = Depends(get_config_service),
):
    if source_route in _RESERVED:
        return Response(content="Not found", status_code=404)
    source = cfg.get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/{source['username']}/{source['password']}/{stream_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="live")
    return RedirectResponse(url=upstream_url, status_code=302)


@router.get("/{source_route}/movie/{username}/{password}/{stream_id}")
@router.get("/{source_route}/movie/{username}/{password}/{stream_id}.{ext}")
async def proxy_movie_stream_source(
    request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "mp4",
    cfg: ConfigService = Depends(get_config_service),
):
    if source_route in _RESERVED:
        return Response(content="Not found", status_code=404)
    source = cfg.get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/movie/{source['username']}/{source['password']}/{stream_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="vod")
    return RedirectResponse(url=upstream_url, status_code=302)


@router.get("/{source_route}/series/{username}/{password}/{stream_id}")
@router.get("/{source_route}/series/{username}/{password}/{stream_id}.{ext}")
async def proxy_series_stream_source(
    request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "mp4",
    cfg: ConfigService = Depends(get_config_service),
):
    if source_route in _RESERVED:
        return Response(content="Not found", status_code=404)
    source = cfg.get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/series/{source['username']}/{source['password']}/{stream_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="series")
    return RedirectResponse(url=upstream_url, status_code=302)


# ------------------------------------------------------------------
# Per-source FULL stream proxies
# ------------------------------------------------------------------


@router.get("/{source_route}/full/live/{username}/{password}/{stream_id}")
@router.get("/{source_route}/full/live/{username}/{password}/{stream_id}.{ext}")
@router.get("/{source_route}/full/{username}/{password}/{stream_id}")
@router.get("/{source_route}/full/{username}/{password}/{stream_id}.{ext}")
async def proxy_live_stream_source_full(
    request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "ts",
    cfg: ConfigService = Depends(get_config_service),
):
    if source_route in _RESERVED:
        return Response(content="Not found", status_code=404)
    source = cfg.get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/{source['username']}/{source['password']}/{stream_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="live")
    return RedirectResponse(url=upstream_url, status_code=302)


@router.get("/{source_route}/full/movie/{username}/{password}/{stream_id}")
@router.get("/{source_route}/full/movie/{username}/{password}/{stream_id}.{ext}")
async def proxy_movie_stream_source_full(
    request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "mp4",
    cfg: ConfigService = Depends(get_config_service),
):
    if source_route in _RESERVED:
        return Response(content="Not found", status_code=404)
    source = cfg.get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/movie/{source['username']}/{source['password']}/{stream_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="vod")
    return RedirectResponse(url=upstream_url, status_code=302)


@router.get("/{source_route}/full/series/{username}/{password}/{stream_id}")
@router.get("/{source_route}/full/series/{username}/{password}/{stream_id}.{ext}")
async def proxy_series_stream_source_full(
    request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "mp4",
    cfg: ConfigService = Depends(get_config_service),
):
    if source_route in _RESERVED:
        return Response(content="Not found", status_code=404)
    source = cfg.get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/series/{source['username']}/{source['password']}/{stream_id}.{ext}"
    if cfg.get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="series")
    return RedirectResponse(url=upstream_url, status_code=302)
