"""Browse API routes — channel listing, groups, search, stats, preview."""
from __future__ import annotations

import asyncio
import functools

from fastapi import APIRouter, Depends, Query, Request

from app.database import db_connect
from app.dependencies import (
    get_cache_service,
    get_cart_service,
    get_category_service,
    get_config_service,
    get_http_client,
    get_m3u_service,
)
from app.services.cache_service import CacheService
from app.services.cart_service import CartService
from app.services.category_service import CategoryService
from app.services.config_service import ConfigService
from app.services.filter_service import (
    group_similar_items,
    safe_get_category_name,
    should_include,
    source_rules_predicate,
)
from app.services.http_client import HttpClientService
from app.services.m3u_service import M3uService

router = APIRouter(tags=["browse"])


async def _annotate_download_history(
    items: list[dict], cart: CartService, default_type: str
) -> tuple[set[tuple[str, str, str]], dict[tuple[str, str, str], set[tuple[str, str]]]]:
    """Annotate catalog items and return lookup sets for grouped results."""
    keys: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in items:
        key = (
            str(item.get("source_id", "")),
            item.get("content_type", default_type),
            str(item.get("id", "")),
        )
        if key not in seen and key[1] in ("vod", "series"):
            seen.add(key)
            keys.append(key)

    rows = await asyncio.to_thread(cart.get_download_history_for_browse, keys)
    downloaded_movies: set[tuple[str, str, str]] = set()
    downloaded_episodes: dict[tuple[str, str, str], set[tuple[str, str]]] = {}
    for row in rows:
        key = (str(row["source_id"]), row["content_type"], str(row["stream_id"]))
        if row["content_type"] == "vod":
            downloaded_movies.add(key)
        elif row["content_type"] == "series":
            downloaded_episodes.setdefault(key, set()).add(
                (
                    str(row["season"]) if row["season"] is not None else "",
                    str(row["episode_num"]) if row["episode_num"] is not None else "",
                )
            )

    for item in items:
        key = (
            str(item.get("source_id", "")),
            item.get("content_type", default_type),
            str(item.get("id", "")),
        )
        if key[1] == "vod":
            item["downloaded"] = key in downloaded_movies
        elif key[1] == "series":
            item["downloaded_episode_count"] = len(downloaded_episodes.get(key, set()))
    return downloaded_movies, downloaded_episodes


def _annotate_download_history_groups(
    groups: list[dict],
    downloaded_movies: set[tuple[str, str, str]],
    downloaded_episodes: dict[tuple[str, str, str], set[tuple[str, str]]],
    default_type: str,
) -> None:
    """Add aggregate download state to grouped browse cards."""
    for group in groups:
        group_type = group["items"][0].get("content_type", default_type)
        if group_type == "vod":
            group["downloaded"] = any(
                (
                    str(item.get("source_id", "")),
                    item.get("content_type", group_type),
                    str(item.get("id", "")),
                )
                in downloaded_movies
                for item in group["items"]
            )
        elif group_type == "series":
            episodes: set[tuple[str, str]] = set()
            for item in group["items"]:
                episodes.update(
                    downloaded_episodes.get(
                        (
                            str(item.get("source_id", "")),
                            item.get("content_type", group_type),
                            str(item.get("id", "")),
                        ),
                        set(),
                    )
                )
            group["downloaded_episode_count"] = len(episodes)


def _group_matches_download_status(group: dict, download_status: str) -> bool:
    """Apply logical grouped-title semantics to a download status filter."""
    if download_status == "all":
        return True
    has_downloadable_content = any(
        item.get("content_type") in ("vod", "series") for item in group["items"]
    )
    if not has_downloadable_content:
        return False
    group_type = group["items"][0].get("content_type")
    is_downloaded = (
        bool(group.get("downloaded"))
        if group_type == "vod"
        else int(group.get("downloaded_episode_count", 0)) > 0
    )
    return is_downloaded if download_status == "downloaded" else not is_downloaded


@router.get("/groups")
async def groups(
    type: str = Query("live"),
    source_id: str | None = Query(None),
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
    groups_list = sorted({safe_get_category_name(cat) for cat in categories if safe_get_category_name(cat)})
    return {"groups": groups_list}


@router.get("/api/browse/groups")
async def api_browse_groups(
    type: str = Query("live"),
    source: str = Query(""),
    cache: CacheService = Depends(get_cache_service),
):
    """Return the list of groups with item counts for the given content type and optional source.

    This is a lightweight endpoint used to populate the group filter dropdown on page load,
    before the user performs any search.
    """
    TYPE_MAP = {
        "live": ("live_streams", "live_categories"),
        "vod": ("vod_streams", "vod_categories"),
        "series": ("series", "series_categories"),
    }
    if type not in TYPE_MAP:
        return {"groups": []}
    groups_list = await asyncio.to_thread(cache.browse_group_counts_db, type, source)
    return {"groups": groups_list}


@router.get("/channels")
async def channels(
    type: str = Query("live"),
    source_id: str | None = Query(None),
    search: str = Query(""),
    group: str = Query(""),
    page: int = Query(1),
    per_page: int = Query(100),
    cache: CacheService = Depends(get_cache_service),
):
    if type not in {"live", "vod", "series"}:
        return {"channels": [], "items": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
    page = max(1, page)
    per_page = min(max(1, per_page), 200)
    result = await asyncio.to_thread(
        cache.browse_channels_db,
        content_type=type,
        source=source_id or "",
        search=search,
        group=group,
        page=page,
        per_page=per_page,
    )
    paginated = result["items"]
    total = result["total"]
    return {
        "channels": [item["name"] for item in paginated],
        "items": paginated,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": result["total_pages"],
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
    download_status: str = Query("all", pattern="^(all|downloaded|not_downloaded)$"),
    page: int = Query(1),
    per_page: int = Query(50),
    cfg: ConfigService = Depends(get_config_service),
    cache: CacheService = Depends(get_cache_service),
    cat_svc: CategoryService = Depends(get_category_service),
    cart: CartService = Depends(get_cart_service),
):
    page = max(1, page)
    per_page = min(max(1, per_page), 200)
    search_lower = search.lower()

    # Handle tmdb:XXXX search prefix
    tmdb_search_id: str | None = None
    if search_lower.startswith("tmdb:"):
        tmdb_search_id = search_lower[5:].strip()
        search_lower = ""  # disable name search
        # TMDB IDs only appear on vod/series — force search over both
        content_types_to_query_override = ["vod", "series"]
    else:
        content_types_to_query_override = None

    sources_config = {}
    # Auto-enable source filters when the viewed category has use_source_filters set
    if use_source_filters:
        sources_config = {s.get("id"): s for s in cfg.config.get("sources", [])}

    # Determine which content types to query
    cat_data = None
    if category_id:
        cat_data = cat_svc.get_category_definition_by_id(category_id)
        if cat_data:
            # If the category itself has use_source_filters enabled, honour it even
            # if the browse request didn't explicitly pass use_source_filters=true.
            if not use_source_filters and cat_data.get("use_source_filters"):
                sources_config = {s.get("id"): s for s in cfg.config.get("sources", [])}

    # When viewing a category, query its configured content types; otherwise
    # query the requested type.
    content_types_to_query = (
        cat_data.get("content_types", ["live", "vod", "series"])
        if category_id and cat_data
        else [type]
    )
    # Override: TMDB search always covers vod+series regardless of current tab
    if content_types_to_query_override:
        content_types_to_query = content_types_to_query_override

    category_fast_path = bool(category_id and cat_data)
    should_group = bool(category_id) or all(
        ct in ("vod", "series") for ct in content_types_to_query
    )
    _GROUP_THRESHOLD = 500

    source_set: dict[str, str] = {}
    group_counts: dict[str, int] = {}
    items = []
    total_from_db: int | None = None
    items_are_paginated = False
    source_filters_applied = False
    group_download_filter_exact = False

    if not sources_config:
        query_content_type: str | list[str]
        if len(content_types_to_query) == 1:
            query_content_type = content_types_to_query[0]
        else:
            query_content_type = content_types_to_query

        browse_kwargs = {
            "content_type": query_content_type,
            "search": search_lower,
            "group": group,
            "source": source,
            "news_days": news_days,
            "min_rating": min_rating,
            "max_added_days": max_added_days,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "tmdb_search_id": tmdb_search_id,
            "download_status": download_status,
        }
        if category_fast_path:
            browse_kwargs["category_id"] = category_id
            browse_kwargs["category_mode"] = cat_data.get("mode", "manual")

        # Grouping needs the complete set only for small result sets. For large
        # sets the existing contract groups the current raw page, so fetch only
        # that page after a bounded count probe.
        if should_group:
            probe_kwargs = (
                {**browse_kwargs, "download_status": "all"}
                if download_status != "all"
                else browse_kwargs
            )
            probe = await asyncio.to_thread(
                cache.browse_streams_db,
                **probe_kwargs,
                page=1,
                per_page=1,
            )
            raw_total = probe["total"]
            if raw_total <= _GROUP_THRESHOLD:
                group_download_filter_exact = download_status != "all"
                result = await asyncio.to_thread(
                    cache.browse_streams_db,
                    **probe_kwargs,
                    page=1,
                    per_page=0,
                )
            else:
                result = await asyncio.to_thread(
                    cache.browse_streams_db,
                    **browse_kwargs,
                    page=page,
                    per_page=per_page,
                )
                items_are_paginated = True
        else:
            result = await asyncio.to_thread(
                cache.browse_streams_db,
                **browse_kwargs,
                page=page,
                per_page=per_page,
            )
            items_are_paginated = True

        items.extend(result["items"])
        total_from_db = result["total"]
        group_counts.update(result["group_counts"])
        source_set.update(result["source_set"])
    elif sources_config:
        query_content_type: str | list[str]
        if len(content_types_to_query) == 1:
            query_content_type = content_types_to_query[0]
        else:
            query_content_type = content_types_to_query

        browse_kwargs = {
            "content_type": query_content_type,
            "search": search_lower,
            "group": group,
            "source": source,
            "news_days": news_days,
            "min_rating": min_rating,
            "max_added_days": max_added_days,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "tmdb_search_id": tmdb_search_id,
            "download_status": download_status,
        }
        if category_fast_path:
            browse_kwargs["category_id"] = category_id
            browse_kwargs["category_mode"] = cat_data.get("mode", "manual")

        predicate = source_rules_predicate(sources_config, content_types_to_query)
        if predicate is not None:
            # Fast path: every active rule translated to SQL — paginate in the
            # database exactly like the unfiltered flow.
            rules_sql, rules_params = predicate
            # Fast path: every active rule translated to SQL — paginate in the
            # database exactly like the unfiltered flow.
            if should_group:
                probe_kwargs = (
                    {**browse_kwargs, "download_status": "all"}
                    if download_status != "all"
                    else browse_kwargs
                )
                probe = await asyncio.to_thread(
                    cache.browse_streams_db,
                    **probe_kwargs,
                    extra_where_sql=rules_sql,
                    extra_where_params=rules_params,
                    page=1,
                    per_page=1,
                )
                raw_total = probe["total"]
                if raw_total <= _GROUP_THRESHOLD:
                    group_download_filter_exact = download_status != "all"
                    result = await asyncio.to_thread(
                        cache.browse_streams_db,
                        **probe_kwargs,
                        extra_where_sql=rules_sql,
                        extra_where_params=rules_params,
                        page=1,
                        per_page=0,
                    )
                else:
                    result = await asyncio.to_thread(
                        cache.browse_streams_db,
                        **browse_kwargs,
                        extra_where_sql=rules_sql,
                        extra_where_params=rules_params,
                        page=page,
                        per_page=per_page,
                    )
                    items_are_paginated = True
            else:
                result = await asyncio.to_thread(
                    cache.browse_streams_db,
                    **browse_kwargs,
                    extra_where_sql=rules_sql,
                    extra_where_params=rules_params,
                    page=page,
                    per_page=per_page,
                )
                items_are_paginated = True
            items.extend(result["items"])
            total_from_db = result["total"]
            group_counts.update(result["group_counts"])
            source_set.update(result["source_set"])
            source_filters_applied = True
        else:
            # Fallback for untranslatable rules (regex / case-sensitive):
            # scan in bounded batches WITHOUT loading wide JSON blobs, keep
            # only matching keys, then hydrate just the needed rows.
            def passes_source_filters(item: dict) -> bool:
                src_id = item.get("source_id", "")
                if src_id not in sources_config:
                    return True
                source_cfg = sources_config[src_id]
                content_filters = source_cfg.get("filters", {}).get(item.get("content_type", ""), {})
                if content_filters.get("groups") and not should_include(
                    item.get("group", ""), content_filters["groups"]
                ):
                    return False
                if content_filters.get("channels") and not should_include(
                    item.get("name", ""), content_filters["channels"]
                ):
                    return False
                return True

            matched_keys: list[tuple[str, str, str]] = []
            window_keys: list[tuple[str, str, str]] = []
            smallset_keys: list[tuple[str, str, str]] = []
            filtered_total = 0
            raw_page = 1
            raw_total = None
            batch_size = 1000
            requested_start = (page - 1) * per_page

            while raw_total is None or (raw_page - 1) * batch_size < raw_total:
                result = await asyncio.to_thread(
                    cache.browse_streams_db,
                    **browse_kwargs,
                    page=raw_page,
                    per_page=batch_size,
                    _slim=True,
                )
                if raw_total is None:
                    raw_total = result["total"]
                    group_counts.update(result["group_counts"])
                    source_set.update(result["source_set"])
                if not result["items"]:
                    break
                for item in result["items"]:
                    if not passes_source_filters(item):
                        continue
                    key = (str(item.get("id", "")), item.get("source_id", ""), item.get("content_type", ""))
                    if filtered_total <= _GROUP_THRESHOLD:
                        smallset_keys.append(key)
                    if requested_start <= filtered_total < requested_start + per_page:
                        window_keys.append(key)
                    filtered_total += 1
                raw_page += 1

            matched_keys = smallset_keys if filtered_total <= _GROUP_THRESHOLD else window_keys
            items = await asyncio.to_thread(cache.hydrate_browse_items, matched_keys)
            items_are_paginated = filtered_total > _GROUP_THRESHOLD
            total_from_db = filtered_total
            source_filters_applied = True
    # Apply source filters in Python (can't express in SQL)
    if sources_config and not source_filters_applied:
        filtered = []
        for item in items:
            src_id = item.get("source_id", "")
            if src_id in sources_config:
                source_cfg = sources_config[src_id]
                filters = source_cfg.get("filters", {})
                content_filters = filters.get(item.get("content_type", ""), {})
                grp = item.get("group", "")
                name = item.get("name", "")
                if content_filters.get("groups") and not should_include(grp, content_filters["groups"]):
                    continue
                if content_filters.get("channels") and not should_include(name, content_filters["channels"]):
                    continue
            filtered.append(item)
        items = filtered

    # Annotate the complete in-memory result before grouping. Small grouped
    # results can then apply download status to the logical title rather than
    # to only one of its source variants.
    _downloaded_movies, _downloaded_episodes = await _annotate_download_history(
        items, cart, type
    )

    total = total_from_db if total_from_db is not None else len(items)
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

    grouped = False
    if should_group and total <= _GROUP_THRESHOLD:
        # Small result set: group the full set, then paginate (exact grouping)
        loop = asyncio.get_event_loop()
        grouped_items = await loop.run_in_executor(
            None, functools.partial(group_similar_items, items, 85)
        )
        _annotate_download_history_groups(
            grouped_items, _downloaded_movies, _downloaded_episodes, type
        )
        if group_download_filter_exact:
            grouped_items = [
                group_item
                for group_item in grouped_items
                if _group_matches_download_status(group_item, download_status)
            ]
        # Re-sort grouped items by group-level rating/added if sort requested
        if sort_by == "added":
            grouped_items.sort(key=lambda x: x["added"], reverse=reverse)
        elif sort_by == "rating":
            grouped_items.sort(key=lambda x: x["rating"], reverse=reverse)
        elif sort_by == "name":
            grouped_items.sort(key=lambda x: x["name"].lower(), reverse=reverse)
        total = len(grouped_items)
        grouped = True
        start = (page - 1) * per_page
        paginated = grouped_items[start : start + per_page]
    elif should_group and total > _GROUP_THRESHOLD:
        # Large result set: paginate first, then group only the current page
        # to avoid O(n²) fuzzy comparison on the full set.
        start = (page - 1) * per_page
        page_items = items if items_are_paginated else items[start : start + per_page]
        loop = asyncio.get_event_loop()
        grouped_page = await loop.run_in_executor(
            None, functools.partial(group_similar_items, page_items, 85)
        )
        _annotate_download_history_groups(
            grouped_page, _downloaded_movies, _downloaded_episodes, type
        )
        if sort_by == "added":
            grouped_page.sort(key=lambda x: x["added"], reverse=reverse)
        elif sort_by == "rating":
            grouped_page.sort(key=lambda x: x["rating"], reverse=reverse)
        elif sort_by == "name":
            grouped_page.sort(key=lambda x: x["name"].lower(), reverse=reverse)
        grouped = True
        paginated = grouped_page
    else:
        start = (page - 1) * per_page
        paginated = items if items_are_paginated else items[start : start + per_page]

    # Build category membership for paginated items only (not the full table).
    # Collect the unique (stream_id, source_id, content_type) triples from
    # the current page and query just those from the DB in a single batch.
    _page_keys: list[tuple[str, str, str]] = []
    _seen_keys: set[tuple[str, str, str]] = set()
    if grouped:
        for gi in paginated:
            for sub in gi["items"]:
                _pk = (sub["id"], sub["source_id"], sub.get("content_type", type))
                if _pk not in _seen_keys:
                    _seen_keys.add(_pk)
                    _page_keys.append(_pk)
    else:
        for item in paginated:
            _pk = (item["id"], item["source_id"], item.get("content_type", type))
            if _pk not in _seen_keys:
                _seen_keys.add(_pk)
                _page_keys.append(_pk)

    category_membership: dict[tuple, list] = {}
    if _page_keys:
        _cm_conn = db_connect(cat_svc.db_path)
        try:
            # Row-value IN over a VALUES list keeps one indexed lookup per key
            # without the OR-chain growing with page size.
            for _start in range(0, len(_page_keys), 300):
                _chunk = _page_keys[_start : _start + 300]
                values_sql = ",".join("(?, ?, ?)" for _ in _chunk)
                params: list[str] = []
                for sid, src, ct in _chunk:
                    params.extend([sid, src, ct])
                for _row in _cm_conn.execute(
                    f"SELECT m.stream_id, m.source_id, m.content_type, m.category_id "
                    f"FROM category_manual_items m "
                    f"WHERE (m.stream_id, m.source_id, m.content_type) "
                    f"IN (VALUES {values_sql})",
                    params,
                ).fetchall():
                    _key = (_row["stream_id"], _row["source_id"], _row["content_type"])
                    category_membership.setdefault(_key, []).append(_row["category_id"])
        finally:
            _cm_conn.close()

    # Annotate paginated items with their category memberships
    if grouped:
        for gi in paginated:
            for sub in gi["items"]:
                key = (sub["id"], sub["source_id"], sub.get("content_type", type))
                sub["categories"] = category_membership.get(key, [])
    else:
        for item in paginated:
            key = (item["id"], item["source_id"], item.get("content_type", type))
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
        "total_pages": (total + per_page - 1) // per_page if total > 0 else 0,
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
    return await asyncio.to_thread(m3u.generate_preview, server_url)
