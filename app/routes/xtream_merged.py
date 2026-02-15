"""Xtream Codes merged API — combines all sources with virtual IDs."""
from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, Response

from app.dependencies import (
    get_cache_service,
    get_category_service,
    get_config_service,
    get_http_client,
)
from app.services.cache_service import CacheService
from app.services.category_service import CategoryService
from app.services.config_service import ConfigService
from app.services.filter_service import (
    build_category_map,
    safe_copy_category,
    safe_get_category_id,
    safe_get_category_name,
    should_include,
)
from app.services.http_client import HttpClientService
from app.services.xtream_service import decode_virtual_id, encode_virtual_id

logger = logging.getLogger(__name__)
router = APIRouter(tags=["xtream-merged"])


@router.get("/player_api.php")
async def player_api(request: Request):
    return await player_api_merged(request)


@router.get("/full/player_api.php")
async def player_api_full(
    request: Request,
    cfg: ConfigService = Depends(get_config_service),
):
    sources = cfg.config.get("sources", [])
    for source in sources:
        if source.get("enabled", True) and source.get("route"):
            from app.routes.xtream_source import player_api_source_full
            return await player_api_source_full(source.get("route"), request)
    available_routes = [s.get("route") for s in sources if s.get("enabled", True) and s.get("route")]
    if available_routes:
        return JSONResponse(
            {"error": "Please use a dedicated source route", "available_routes": [f"/{r}/full/player_api.php" for r in available_routes]},
            status_code=400,
        )
    return JSONResponse({"error": "No sources configured with dedicated routes."}, status_code=400)


@router.get("/merged/player_api.php")
async def player_api_merged(
    request: Request,
    cfg: ConfigService = Depends(get_config_service),
    http: HttpClientService = Depends(get_http_client),
    cache: CacheService = Depends(get_cache_service),
    cat_svc: CategoryService = Depends(get_category_service),
):
    config = cfg.config
    enabled_sources = [s for s in config.get("sources", []) if s.get("enabled", True)]
    action = request.query_params.get("action", "")

    if not enabled_sources:
        return JSONResponse({"error": "No sources configured"}, status_code=400)

    client = await http.get_client()

    try:
        if not action:
            source = enabled_sources[0]
            host = source["host"].rstrip("/")
            response = await client.get(
                f"{host}/player_api.php",
                params={"username": source["username"], "password": source["password"]},
                timeout=30.0,
            )
            if response.status_code == 200:
                data = response.json()
                if "server_info" in data:
                    data["server_info"]["url"] = str(request.base_url).rstrip("/") + "/merged"
                    data["server_info"]["port"] = "80"
                    data["server_info"]["https_port"] = "443"
                return data
            return Response(content=response.content, status_code=response.status_code)

        elif action in ("get_live_categories", "get_vod_categories", "get_series_categories"):
            return _handle_get_categories(action, enabled_sources, cache, cat_svc)

        elif action in ("get_live_streams", "get_vod_streams", "get_series"):
            return _handle_get_streams(action, request, enabled_sources, cache, cat_svc)

        elif action == "get_series_info":
            return await _handle_get_series_info(request, client, cfg)

        elif action == "get_vod_info":
            return await _handle_get_vod_info(request, client, cfg)

        else:
            return JSONResponse({"error": f"Unknown action: {action}"}, status_code=400)

    except httpx.TimeoutException:
        return JSONResponse({"error": "Upstream server timeout"}, status_code=504)
    except Exception as e:
        logger.error(f"Merged API error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

_CONTENT_TYPE_MAP = {
    "get_live_categories": ("live", "live_categories"),
    "get_vod_categories": ("vod", "vod_categories"),
    "get_series_categories": ("series", "series_categories"),
}

_STREAM_ACTION_MAP = {
    "get_live_streams": ("live", "live_streams", "live_categories", "stream_id"),
    "get_vod_streams": ("vod", "vod_streams", "vod_categories", "stream_id"),
    "get_series": ("series", "series", "series_categories", "series_id"),
}


def _handle_get_categories(action: str, enabled_sources, cache: CacheService, cat_svc: CategoryService):
    ct, cache_key = _CONTENT_TYPE_MAP[action]
    result = []
    # Custom categories first
    custom_cats = cat_svc.get_custom_categories_for_content_type(ct)
    for cc in custom_cats:
        result.append({
            "category_id": cat_svc.custom_cat_id_to_numeric(cc["id"]),
            "category_name": cc["name"],
            "parent_id": 0,
        })
    for idx, source in enumerate(enabled_sources):
        source_id = source.get("id")
        prefix = source.get("prefix", "")
        filters = source.get("filters", {}).get(ct, {}).get("groups", [])
        for cat in cache.get_cached(cache_key, source_id):
            cat_name = safe_get_category_name(cat)
            if should_include(cat_name, filters):
                display_name = f"{prefix}{cat_name}" if prefix else cat_name
                cat_copy = safe_copy_category(cat)
                cat_copy["category_name"] = display_name
                cat_copy["category_id"] = str(encode_virtual_id(idx, safe_get_category_id(cat)))
                result.append(cat_copy)
    return result


def _handle_get_streams(action: str, request: Request, enabled_sources, cache: CacheService, cat_svc: CategoryService):
    ct, stream_key, cat_key, id_field = _STREAM_ACTION_MAP[action]
    requested_cat_id = request.query_params.get("category_id")
    result = []

    custom_cats = cat_svc.get_custom_categories_for_content_type(ct)
    custom_item_cats: dict[tuple, list] = {}
    for cc in custom_cats:
        cat_id_str = cat_svc.custom_cat_id_to_numeric(cc["id"])
        for item in cc.get("items", []):
            key = (item.get("source_id"), str(item.get("id")))
            custom_item_cats.setdefault(key, []).append(cat_id_str)

    for idx, source in enumerate(enabled_sources):
        source_id = source.get("id")
        filters = source.get("filters", {})
        group_filters = filters.get(ct, {}).get("groups", [])
        channel_filters = filters.get(ct, {}).get("channels", [])
        categories = cache.get_cached(cat_key, source_id)
        cat_map = build_category_map(categories)

        for stream in cache.get_cached(stream_key, source_id):
            cat_id = str(stream.get("category_id", ""))
            group_name = cat_map.get(cat_id, "")
            channel_name = stream.get("name", "")
            stream_id_raw = str(stream.get(id_field, ""))

            if should_include(group_name, group_filters) and should_include(channel_name, channel_filters):
                virtual_cat_id = str(encode_virtual_id(idx, stream.get("category_id", 0)))
                if requested_cat_id is None or requested_cat_id == virtual_cat_id:
                    stream_copy = stream.copy()
                    stream_copy[id_field] = encode_virtual_id(idx, stream.get(id_field, 0))
                    stream_copy["category_id"] = virtual_cat_id
                    stream_copy["category_ids"] = [int(virtual_cat_id)]
                    if ct == "live":
                        epg_id = stream.get("epg_channel_id", "")
                        if epg_id:
                            stream_copy["epg_channel_id"] = f"{source_id}_{epg_id}".lower()
                    result.append(stream_copy)

            cids = custom_item_cats.get((source_id, stream_id_raw), [])
            for cid in cids:
                if requested_cat_id is None or requested_cat_id == cid:
                    stream_copy = stream.copy()
                    stream_copy[id_field] = encode_virtual_id(idx, stream.get(id_field, 0))
                    stream_copy["category_id"] = cid
                    stream_copy["category_ids"] = [int(cid)]
                    if ct == "live":
                        epg_id = stream.get("epg_channel_id", "")
                        if epg_id:
                            stream_copy["epg_channel_id"] = f"{source_id}_{epg_id}".lower()
                    result.append(stream_copy)
    return result


async def _handle_get_series_info(request: Request, client, cfg: ConfigService):
    virtual_series_id = request.query_params.get("series_id", "")
    source_idx, original_id = decode_virtual_id(virtual_series_id)
    source = cfg.get_source_by_index(source_idx)
    if not source:
        return JSONResponse({"error": "Source not found"}, status_code=404)
    host = source["host"].rstrip("/")
    params = {"username": source["username"], "password": source["password"], "action": "get_series_info", "series_id": original_id}
    response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
    if response.status_code == 200:
        data = response.json()
        if "episodes" in data:
            for season, episodes in data["episodes"].items():
                for ep in episodes:
                    if "id" in ep:
                        ep["id"] = encode_virtual_id(source_idx, ep["id"])
        return data
    return Response(content=response.content, status_code=response.status_code, media_type="application/json")


async def _handle_get_vod_info(request: Request, client, cfg: ConfigService):
    virtual_vod_id = request.query_params.get("vod_id", "")
    source_idx, original_id = decode_virtual_id(virtual_vod_id)
    source = cfg.get_source_by_index(source_idx)
    if not source:
        return JSONResponse({"error": "Source not found"}, status_code=404)
    host = source["host"].rstrip("/")
    params = {"username": source["username"], "password": source["password"], "action": "get_vod_info", "vod_id": original_id}
    response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
    return Response(content=response.content, status_code=response.status_code, media_type="application/json")
