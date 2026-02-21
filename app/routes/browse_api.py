"""Browse API routes — channel listing, groups, search, stats, preview."""
from __future__ import annotations

import asyncio
import functools
import time
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from app.database import db_connect

from app.dependencies import (
    get_cache_service,
    get_config_service,
    get_category_service,
    get_http_client,
    get_m3u_service,
)
from app.services.cache_service import CacheService
from app.services.category_service import CategoryService
from app.services.config_service import ConfigService
from app.services.filter_service import (
    build_category_map,
    group_similar_items,
    safe_get_category_name,
    should_include,
)
from app.services.http_client import HttpClientService
from app.services.m3u_service import M3uService

router = APIRouter(tags=["browse"])


@router.get("/groups")
async def groups(
    type: str = Query("live"),
    source_id: Optional[str] = Query(None),
    cache: CacheService = Depends(get_cache_service),
):
    if type == "live":
        categories = cache.get_cached("live_categories", source_id)
    elif type == "vod":
        categories = cache.get_cached("vod_categories", source_id)
    elif type == "series":
        categories = cache.get_cached("series_categories", source_id)
    else:
        categories = []
    groups_list = sorted(set(safe_get_category_name(cat) for cat in categories if safe_get_category_name(cat)))
    return {"groups": groups_list}


@router.get("/channels")
async def channels(
    type: str = Query("live"),
    source_id: Optional[str] = Query(None),
    search: str = Query(""),
    group: str = Query(""),
    page: int = Query(1),
    per_page: int = Query(100),
    cache: CacheService = Depends(get_cache_service),
):
    search_lower = search.lower()
    if type == "live":
        streams = cache.get_cached("live_streams", source_id)
        categories = cache.get_cached("live_categories", source_id)
    elif type == "vod":
        streams = cache.get_cached("vod_streams", source_id)
        categories = cache.get_cached("vod_categories", source_id)
    elif type == "series":
        streams = cache.get_cached("series", source_id)
        categories = cache.get_cached("series_categories", source_id)
    else:
        streams, categories = [], []

    cat_map = build_category_map(categories)
    items = []
    for s in streams:
        name = s.get("name", "")
        cat_id = str(s.get("category_id", ""))
        grp = cat_map.get(cat_id, "Unknown")
        if search_lower and search_lower not in name.lower():
            continue
        if group and grp != group:
            continue
        items.append({"name": name, "group": grp})

    total = len(items)
    start = (page - 1) * per_page
    paginated = items[start : start + per_page]
    return {
        "channels": [item["name"] for item in paginated],
        "items": paginated,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    }


@router.get("/api/browse")
async def api_browse(
    type: str = Query("live"),
    search: str = Query(""),
    group: str = Query(""),
    source: str = Query(""),
    news_days: int = Query(0),
    category_id: str = Query(""),
    use_source_filters: bool = Query(False),
    sort_by: str = Query(""),
    sort_order: str = Query("desc"),
    min_rating: float = Query(0),
    max_added_days: int = Query(0),
    page: int = Query(1),
    per_page: int = Query(50),
    cfg: ConfigService = Depends(get_config_service),
    cache: CacheService = Depends(get_cache_service),
    cat_svc: CategoryService = Depends(get_category_service),
):
    per_page = min(per_page, 200)
    search_lower = search.lower()

    # Handle tmdb:XXXX search prefix
    tmdb_search_id: Optional[str] = None
    if search_lower.startswith("tmdb:"):
        tmdb_search_id = search_lower[5:].strip()
        search_lower = ""  # disable name search

    sources_config = {}
    if use_source_filters:
        sources_config = {s.get("id"): s for s in cfg.config.get("sources", [])}

    current_time = int(time.time())
    news_cutoff = current_time - (news_days * 86400) if news_days > 0 else 0

    # Build category membership map via direct SQL (avoids loading all categories JSON)
    category_membership: dict[tuple, list] = {}
    _cm_conn = db_connect(cat_svc.db_path)
    try:
        for _row in _cm_conn.execute(
            "SELECT category_id, stream_id, source_id, content_type "
            "FROM category_manual_items"
        ).fetchall():
            _key = (_row["content_type"], _row["stream_id"], _row["source_id"])
            category_membership.setdefault(_key, []).append(_row["category_id"])
    finally:
        _cm_conn.close()

    # Determine which content types to query
    cat_data = None
    category_item_sets: dict[str, set[tuple]] = {}
    if category_id:
        cat_data = cat_svc.get_category_by_id(category_id)
        if cat_data:
            cat_items = cat_data.get("items", []) if cat_data.get("mode") == "manual" else cat_data.get("cached_items", [])
            for item in cat_items:
                ct = item.get("content_type", "")
                category_item_sets.setdefault(ct, set()).add((str(item.get("id")), item.get("source_id")))

    # When viewing a category, query all content types that have items; otherwise just the requested type
    content_types_to_query = list(category_item_sets.keys()) if category_id and category_item_sets else [type]

    TYPE_MAP = {
        "live": ("live_streams", "live_categories"),
        "vod": ("vod_streams", "vod_categories"),
        "series": ("series", "series_categories"),
    }

    source_set: dict[str, str] = {}
    group_counts: dict[str, int] = {}
    items = []

    for ct in content_types_to_query:
        if ct not in TYPE_MAP:
            continue
        streams_key, cats_key = TYPE_MAP[ct]
        streams, categories = cache.get_cached_with_source_info(streams_key, cats_key)
        cat_map = build_category_map(categories)

        cat_item_set = category_item_sets.get(ct, set())

        for s in streams:
            src_id = s.get("_source_id", "")
            src_name = s.get("_source_name", "Unknown")
            if src_id:
                source_set[src_id] = src_name
            cat_id_str = str(s.get("category_id", ""))
            grp = cat_map.get(cat_id_str, "Unknown")
            group_counts[grp] = group_counts.get(grp, 0) + 1

            name = s.get("name", "")
            icon = s.get("stream_icon", "") or s.get("cover", "")
            item_id = str(s.get("stream_id") or s.get("series_id") or "")
            added = s.get("added") or s.get("last_modified", 0)
            try:
                added_ts = int(added) if added else 0
            except (ValueError, TypeError):
                added_ts = 0

            if source and src_id != source:
                continue
            if use_source_filters and src_id in sources_config:
                source_cfg = sources_config[src_id]
                filters = source_cfg.get("filters", {})
                content_filters = filters.get(ct, {})
                if content_filters.get("groups") and not should_include(grp, content_filters["groups"]):
                    continue
                if content_filters.get("channels") and not should_include(name, content_filters["channels"]):
                    continue
            if news_days > 0 and added_ts < news_cutoff:
                continue
            if category_id and (item_id, src_id) not in cat_item_set:
                continue
            if tmdb_search_id is not None:
                raw_tmdb = s.get("tmdb_id") or s.get("tmdb")
                if not raw_tmdb or str(raw_tmdb).strip() != tmdb_search_id:
                    continue
            elif search_lower and search_lower not in name.lower() and search_lower not in grp.lower():
                continue
            if group and grp != group:
                continue

            # Parse rating
            raw_rating = s.get("rating", 0)
            try:
                rating_val = float(raw_rating) if raw_rating else 0.0
            except (ValueError, TypeError):
                rating_val = 0.0

            # Apply rating filter
            if min_rating > 0 and rating_val < min_rating:
                continue

            # Apply max_added_days filter
            if max_added_days > 0:
                added_cutoff = current_time - (max_added_days * 86400)
                if added_ts < added_cutoff:
                    continue

            item_data = {
                "name": name,
                "group": grp,
                "icon": icon,
                "id": item_id,
                "source_id": src_id,
                "source_name": src_name,
                "added": added_ts,
                "rating": rating_val,
                "content_type": ct,
            }
            if ct in ("vod", "series"):
                raw_tmdb = s.get("tmdb_id") or s.get("tmdb")
                item_data["tmdb_id"] = raw_tmdb if raw_tmdb else None
            if ct == "vod":
                item_data["container_extension"] = s.get("container_extension", "mp4")
            items.append(item_data)

    total = len(items)
    reverse = sort_order == "desc"
    if sort_by == "added":
        items.sort(key=lambda x: x["added"], reverse=reverse)
    elif sort_by == "rating":
        items.sort(key=lambda x: x["rating"], reverse=reverse)
    elif sort_by == "name":
        items.sort(key=lambda x: x["name"].lower(), reverse=reverse)
    elif news_days > 0:
        items.sort(key=lambda x: x["added"], reverse=True)
    else:
        items.sort(key=lambda x: (x["group"].lower(), x["name"].lower()))

    # Group vod/series results always (TMDB-ID first, fuzzy fallback);
    # also group when browsing a manual/smart category regardless of content type.
    should_group = bool(category_id) or all(
        ct in ("vod", "series") for ct in content_types_to_query
    )

    grouped = False
    if should_group:
        loop = asyncio.get_event_loop()
        grouped_items = await loop.run_in_executor(
            None, functools.partial(group_similar_items, items, 85)
        )
        # Re-sort grouped items by group-level rating/added if sort requested
        if sort_by == "added":
            grouped_items.sort(key=lambda x: x["added"], reverse=reverse)
        elif sort_by == "rating":
            grouped_items.sort(key=lambda x: x["rating"], reverse=reverse)
        elif sort_by == "name":
            grouped_items.sort(key=lambda x: x["name"].lower(), reverse=reverse)
        for gi in grouped_items:
            for sub in gi["items"]:
                key = (sub.get("content_type", type), sub["id"], sub["source_id"])
                sub["categories"] = category_membership.get(key, [])
        total = len(grouped_items)
        grouped = True
        start = (page - 1) * per_page
        paginated = grouped_items[start : start + per_page]
    else:
        start = (page - 1) * per_page
        paginated = items[start : start + per_page]
        for item in paginated:
            key = (type, item["id"], item["source_id"])
            item["categories"] = category_membership.get(key, [])

    groups_list = [{"name": g, "count": c} for g, c in sorted(group_counts.items())]
    sources_list = [{"id": sid, "name": sname} for sid, sname in sorted(source_set.items(), key=lambda x: x[1])]

    return {
        "items": paginated,
        "grouped": grouped,
        "groups": groups_list,
        "sources": sources_list,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
        "content_type": type,
    }


@router.get("/stats")
async def stats(
    cfg: ConfigService = Depends(get_config_service),
    http: HttpClientService = Depends(get_http_client),
):
    config = cfg.config
    xtream = config["xtream"]
    if not xtream["host"]:
        return {"error": "Not configured"}
    host = xtream["host"].rstrip("/")
    client = await http.get_client()
    try:
        params_base = {"username": xtream["username"], "password": xtream["password"]}
        live_cats = (await client.get(f"{host}/player_api.php", params={**params_base, "action": "get_live_categories"})).json()
        vod_cats = (await client.get(f"{host}/player_api.php", params={**params_base, "action": "get_vod_categories"})).json()
        series_cats = (await client.get(f"{host}/player_api.php", params={**params_base, "action": "get_series_categories"})).json()
        streams = (await client.get(f"{host}/player_api.php", params={**params_base, "action": "get_live_streams"})).json()
    except Exception:
        return {"error": "Failed to fetch stats"}

    filters = config["filters"]
    total_group_filters = sum(len(filters.get(c, {}).get("groups", [])) for c in ["live", "vod", "series"])
    total_channel_filters = sum(len(filters.get(c, {}).get("channels", [])) for c in ["live", "vod", "series"])

    return {
        "total_categories": len(live_cats) + len(vod_cats) + len(series_cats),
        "total_channels": len(streams),
        "live_categories": len(live_cats),
        "vod_categories": len(vod_cats),
        "series_categories": len(series_cats),
        "group_filters": total_group_filters,
        "channel_filters": total_channel_filters,
    }


@router.get("/preview")
async def preview(
    request: Request,
    m3u: M3uService = Depends(get_m3u_service),
):
    server_url = str(request.base_url).rstrip("/")
    m3u_content = m3u.generate_m3u(server_url)
    lines = m3u_content.split("\n")
    stats_line = lines[1] if len(lines) > 1 else ""
    sample = []
    for i in range(2, min(len(lines), 22), 2):
        if lines[i].startswith("#EXTINF"):
            name = lines[i].split(",", 1)[-1] if "," in lines[i] else lines[i]
            sample.append(name)
    return {"stats": stats_line, "sample_channels": sample}
