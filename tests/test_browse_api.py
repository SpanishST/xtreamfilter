"""Tests for the /api/browse endpoint and related browse API routes."""

import json
import os

import pytest
from starlette.testclient import TestClient

from app.database import DB_NAME, db_connect, init_db
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


def _build_app(data_dir: str):
    """Build a fully-wired FastAPI app pointing at *data_dir*."""
    from fastapi import FastAPI, Request

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
        playlist,
        source_api,
        stream_proxy,
        ui,
        xtream_merged,
        xtream_source,
    )

    cfg = ConfigService(data_dir)
    cfg.load()
    init_db(os.path.join(data_dir, DB_NAME))
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
    log_svc = LogService(os.path.join(data_dir, DB_NAME), cfg)

    cache.log_service = log_svc
    cart.log_service = log_svc
    monitor.log_service = log_svc

    app = FastAPI()

    @app.middleware("http")
    async def add_utf8_charset(request: Request, call_next):
        response = await call_next(request)
        ct = response.headers.get("content-type", "")
        if "application/json" in ct and "charset" not in ct:
            response.headers["content-type"] = "application/json; charset=utf-8"
        return response

    for r in (
        ui, health, filter_api, source_api, config_api,
        cache_api, cart_api, monitor_api, log_api, epg, category_api,
        browse_api, playlist, xtream_merged, stream_proxy, xtream_source,
    ):
        app.include_router(r.router)

    # Attach state for DI
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

    return app


def _seed_streams(data_dir: str, streams: list[dict], content_type: str, source_id: str = "src1"):
    """Insert test streams directly into the SQLite database."""
    db_path = os.path.join(data_dir, DB_NAME)
    conn = db_connect(db_path)
    try:
        for s in streams:
            sid = str(s.get("stream_id") or s.get("series_id", ""))
            conn.execute(
                "INSERT OR REPLACE INTO streams "
                "(source_id, content_type, stream_id, name, category_id, added, data) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    source_id,
                    content_type,
                    sid,
                    s.get("name", ""),
                    str(s.get("category_id", "")),
                    s.get("added", 0),
                    json.dumps(s, ensure_ascii=False),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _seed_categories(data_dir: str, categories: list[dict], content_type: str, source_id: str = "src1"):
    """Insert test categories directly into the SQLite database."""
    db_path = os.path.join(data_dir, DB_NAME)
    conn = db_connect(db_path)
    try:
        for cat in categories:
            conn.execute(
                "INSERT OR REPLACE INTO source_categories "
                "(source_id, content_type, category_id, category_name, data) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    source_id,
                    content_type,
                    str(cat.get("category_id", "")),
                    cat.get("category_name", ""),
                    json.dumps(cat, ensure_ascii=False),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _seed_download_history(data_dir: str, rows: list[dict]):
    """Insert completed download records directly into the history ledger."""
    db_path = os.path.join(data_dir, DB_NAME)
    conn = db_connect(db_path)
    try:
        for index, row in enumerate(rows):
            conn.execute(
                """INSERT INTO download_history
                   (cart_item_id, stream_id, source_id, content_type, name,
                    series_name, series_id, season, episode_num, episode_title,
                    file_path, file_size, completed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    row.get("cart_item_id", f"history-{index}"),
                    str(row.get("stream_id", "")),
                    row.get("source_id", "src1"),
                    row.get("content_type", "vod"),
                    row.get("name", ""),
                    row.get("series_name"),
                    row.get("series_id"),
                    row.get("season"),
                    row.get("episode_num"),
                    row.get("episode_title"),
                    row.get("file_path", f"/downloads/{index}.mp4"),
                    row.get("file_size", 1),
                    row.get("completed_at", "2026-01-01T00:00:00+00:00"),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _seed_config(data_dir: str, sources: list[dict] | None = None):
    """Write a minimal config.json with the given sources."""
    config = {
        "sources": sources or [
            {
                "id": "src1",
                "name": "Source 1",
                "host": "http://example.com",
                "username": "user",
                "password": "pass",
                "enabled": True,
                "route": "src1",
                "filters": {},
            }
        ],
        "filters": {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
        "content_types": {"live": True, "vod": True, "series": True},
        "options": {},
    }
    (data_dir / "config.json").write_text(json.dumps(config))


@pytest.fixture()
def data_dir(tmp_path):
    _seed_config(tmp_path)
    return str(tmp_path)


@pytest.fixture()
def client(data_dir):
    app = _build_app(data_dir)
    with TestClient(app) as c:
        yield c


# -------------------------------------------------------------------
# Basic browse
# -------------------------------------------------------------------


def test_browse_empty(client):
    r = client.get("/api/browse?type=live")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 0
    assert data["items"] == []
    assert data["page"] == 1


def test_browse_live_streams(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
        {"category_id": "20", "category_name": "Sports"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "CNN", "category_id": "10", "added": 1000},
        {"stream_id": "2", "name": "BBC", "category_id": "10", "added": 2000},
        {"stream_id": "3", "name": "ESPN", "category_id": "20", "added": 3000},
    ], "live")

    # Reload cache from disk
    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get("/api/browse?type=live")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 3
    assert len(data["items"]) == 3
    assert data["grouped"] is False


def test_browse_search(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "CNN International", "category_id": "10", "added": 1000},
        {"stream_id": "2", "name": "BBC World", "category_id": "10", "added": 2000},
        {"stream_id": "3", "name": "Fox News", "category_id": "10", "added": 3000},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get("/api/browse?type=live&search=CNN")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["items"][0]["name"] == "CNN International"


def test_browse_search_case_insensitive(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "CNN International", "category_id": "10", "added": 1000},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get("/api/browse?type=live&search=cnn")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1


def test_browse_group_filter(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
        {"category_id": "20", "category_name": "Sports"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "CNN", "category_id": "10", "added": 1000},
        {"stream_id": "2", "name": "ESPN", "category_id": "20", "added": 2000},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get("/api/browse?type=live&group=Sports")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["items"][0]["name"] == "ESPN"


def test_browse_pagination(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
    ], "live")
    streams = [
        {"stream_id": str(i), "name": f"Channel {i:03d}", "category_id": "10", "added": i * 1000}
        for i in range(1, 26)
    ]
    _seed_streams(data_dir, streams, "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    # Page 1
    r = client.get("/api/browse?type=live&page=1&per_page=10")
    data = r.json()
    assert data["total"] == 25
    assert len(data["items"]) == 10
    assert data["page"] == 1
    assert data["total_pages"] == 3

    # Page 3
    r = client.get("/api/browse?type=live&page=3&per_page=10")
    data = r.json()
    assert len(data["items"]) == 5


def test_browse_uses_sql_pagination_for_large_live_catalog(client, data_dir, monkeypatch):
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": str(i), "name": f"Channel {i:04d}", "category_id": "10", "added": i}
        for i in range(1, 601)
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()
    original = cache.browse_streams_db
    calls = []

    def record_call(*args, **kwargs):
        calls.append(kwargs.copy())
        return original(*args, **kwargs)

    monkeypatch.setattr(cache, "browse_streams_db", record_call)
    r = client.get("/api/browse?type=live&page=4&per_page=25")

    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 600
    assert len(data["items"]) == 25
    assert data["items"][0]["name"] == "Channel 0076"
    assert any(call["page"] == 4 and call["per_page"] == 25 for call in calls)
    assert all(call["per_page"] != 0 for call in calls)


def test_browse_rating_filter_is_applied_before_sql_pagination(client, data_dir):
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "Movies"}], "vod")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "High", "category_id": "10", "rating": 9.0},
        {"stream_id": "2", "name": "Low", "category_id": "10", "rating": 1.0},
        {"stream_id": "3", "name": "Medium", "category_id": "10", "rating": 8.0},
    ], "vod")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get(
        "/api/browse?type=vod&sort_by=rating&sort_order=desc&min_rating=8&"
        "page=2&per_page=1"
    )
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 2
    assert len(data["items"]) == 1
    assert data["items"][0]["name"] == "Medium"


def test_channels_endpoint_paginates_in_sql(client, data_dir):
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": str(i), "name": f"Channel {i:04d}", "category_id": "10"}
        for i in range(1, 601)
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()
    result = client.get("/channels?type=live&page=3&per_page=20")

    assert result.status_code == 200
    data = result.json()
    assert data["total"] == 600
    assert len(data["items"]) == 20
    assert data["items"][0] == {"name": "Channel 0041", "group": "News"}


def test_browse_source_filters_keep_large_results_bounded(client, data_dir, monkeypatch):
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": str(i), "name": f"Keep {i:04d}", "category_id": "10"}
        for i in range(1, 601)
    ], "live")

    client.app.state.config_service.config["sources"][0]["filters"] = {
        "live": {
            "channels": [
                {"type": "include", "match": "starts_with", "value": "Keep", "case_sensitive": False}
            ]
        }
    }
    cache = client.app.state.cache_service
    cache.load_cache_from_disk()
    original = cache.browse_streams_db
    calls = []

    def record_call(*args, **kwargs):
        calls.append(kwargs.copy())
        return original(*args, **kwargs)

    monkeypatch.setattr(cache, "browse_streams_db", record_call)
    result = client.get("/api/browse?type=live&use_source_filters=true&page=4&per_page=25")

    assert result.status_code == 200
    data = result.json()
    assert data["total"] == 600
    assert len(data["items"]) == 25
    assert data["items"][0]["name"] == "Keep 0076"
    assert all(call["per_page"] <= 1000 for call in calls)
    assert all(call["per_page"] != 0 for call in calls)


def test_browse_custom_category_uses_sql_pagination(client, data_dir, monkeypatch):
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    streams = [
        {"stream_id": str(i), "name": f"Category Channel {i:04d}", "category_id": "10"}
        for i in range(1, 601)
    ]
    _seed_streams(data_dir, streams, "live")
    client.app.state.category_service.save_categories({
        "categories": [{
            "id": "large-category",
            "name": "Large category",
            "mode": "manual",
            "content_types": ["live"],
            "items": [
                {"id": s["stream_id"], "source_id": "src1", "content_type": "live"}
                for s in streams
            ],
        }]
    })

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    def fail_if_cache_index_is_used(*args, **kwargs):
        raise AssertionError("custom category browse should use SQLite pagination")

    monkeypatch.setattr(cache, "get_streams_by_ids", fail_if_cache_index_is_used)
    result = client.get("/api/browse?category_id=large-category&page=3&per_page=25")

    assert result.status_code == 200
    data = result.json()
    assert data["total"] == 600
    assert len(data["items"]) == 25
    assert data["items"][0]["name"] == "Category Channel 0051"


def test_browse_sort_by_name(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Zebra TV", "category_id": "10", "added": 1000},
        {"stream_id": "2", "name": "Alpha TV", "category_id": "10", "added": 2000},
        {"stream_id": "3", "name": "Middle TV", "category_id": "10", "added": 3000},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get("/api/browse?type=live&sort_by=name&sort_order=asc")
    data = r.json()
    names = [item["name"] for item in data["items"]]
    assert names == ["Alpha TV", "Middle TV", "Zebra TV"]


def test_browse_sort_by_added(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Old", "category_id": "10", "added": 1000},
        {"stream_id": "2", "name": "New", "category_id": "10", "added": 5000},
        {"stream_id": "3", "name": "Mid", "category_id": "10", "added": 3000},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get("/api/browse?type=live&sort_by=added&sort_order=desc")
    data = r.json()
    names = [item["name"] for item in data["items"]]
    assert names == ["New", "Mid", "Old"]


# -------------------------------------------------------------------
# VOD / Series grouping
# -------------------------------------------------------------------


def test_browse_vod_groups_by_tmdb(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "Movies"},
    ], "vod")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Inception (2010)", "category_id": "10", "added": 1000, "tmdb_id": "27205", "rating": 8.5},
        {"stream_id": "2", "name": "Inception", "category_id": "10", "added": 2000, "tmdb_id": "27205", "rating": 8.0},
        {"stream_id": "3", "name": "The Matrix", "category_id": "10", "added": 3000, "tmdb_id": "603", "rating": 9.0},
    ], "vod")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get("/api/browse?type=vod")
    data = r.json()
    assert data["grouped"] is True
    # Two groups: Inception (2 items) and The Matrix (1 item)
    assert data["total"] == 2
    group_names = {item["name"] for item in data["items"]}
    assert "Inception" in group_names or "Inception (2010)" in group_names


def test_browse_marks_downloaded_movie(client, data_dir):
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Downloaded Movie", "category_id": "10"},
        {"stream_id": "2", "name": "Pending Movie", "category_id": "10"},
    ], "vod")
    _seed_download_history(data_dir, [
        {"stream_id": "1", "content_type": "vod"},
    ])

    client.app.state.cache_service.load_cache_from_disk()
    response = client.get("/api/browse?type=vod")

    assert response.status_code == 200
    items = {item["name"]: item for item in response.json()["items"]}
    assert items["Downloaded Movie"]["downloaded"] is True
    assert items["Pending Movie"]["downloaded"] is False


def test_browse_download_status_filter(client, data_dir):
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Downloaded Movie", "category_id": "10"},
        {"stream_id": "2", "name": "Pending Movie", "category_id": "10"},
    ], "vod")
    _seed_download_history(data_dir, [{"stream_id": "1", "content_type": "vod"}])

    client.app.state.cache_service.load_cache_from_disk()
    downloaded = client.get("/api/browse?type=vod&download_status=downloaded").json()
    not_downloaded = client.get("/api/browse?type=vod&download_status=not_downloaded").json()
    all_items = client.get("/api/browse?type=vod&download_status=all").json()

    assert downloaded["total"] == 1
    assert downloaded["items"][0]["name"] == "Downloaded Movie"
    assert not_downloaded["total"] == 1
    assert not_downloaded["items"][0]["name"] == "Pending Movie"
    assert all_items["total"] == 2


def test_download_history_api_supports_filters_and_pagination(client, data_dir):
    _seed_download_history(data_dir, [
        {"cart_item_id": "movie-1", "stream_id": "1", "content_type": "vod", "name": "Movie One",
         "completed_at": "2026-01-01T00:00:00+00:00"},
        {"cart_item_id": "episode-1", "stream_id": "episode-1", "content_type": "series",
         "series_id": "series-1", "series_name": "Series One", "episode_title": "Pilot",
         "season": "1", "episode_num": 1, "completed_at": "2026-01-02T00:00:00+00:00"},
    ])

    page = client.get("/api/download-history?type=series&search=pilot&limit=1&offset=0")

    assert page.status_code == 200
    data = page.json()
    assert data["total"] == 1
    assert len(data["items"]) == 1
    assert data["items"][0]["source_name"] == "Source 1"
    assert data["items"][0]["series_name"] == "Series One"
    assert data["has_more"] is False


def test_download_history_item_api_returns_latest_three_matching_events(client, data_dir):
    _seed_download_history(data_dir, [
        {"cart_item_id": "movie-1", "stream_id": "1", "content_type": "vod", "name": "Movie One",
         "completed_at": "2026-01-01T00:00:00+00:00"},
        {"cart_item_id": "movie-2", "stream_id": "1", "content_type": "vod", "name": "Movie One",
         "completed_at": "2026-01-02T00:00:00+00:00"},
        {"cart_item_id": "movie-3", "stream_id": "1", "content_type": "vod", "name": "Movie One",
         "completed_at": "2026-01-03T00:00:00+00:00"},
        {"cart_item_id": "movie-4", "stream_id": "1", "content_type": "vod", "name": "Movie One",
         "completed_at": "2026-01-04T00:00:00+00:00"},
        {"cart_item_id": "other-movie", "stream_id": "2", "content_type": "vod", "name": "Other Movie",
         "completed_at": "2026-01-05T00:00:00+00:00"},
    ])

    response = client.get(
        "/api/download-history/item",
        params={"keys": json.dumps([{"source_id": "src1", "content_type": "vod", "stream_id": "1"}])},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 3
    assert [item["cart_item_id"] for item in data["items"]] == ["movie-4", "movie-3", "movie-2"]
    assert all(item["source_id"] == "src1" for item in data["items"])


def test_browse_counts_distinct_series_episodes(client, data_dir):
    _seed_streams(data_dir, [
        {"stream_id": "series-1", "name": "Example Series", "category_id": "10"},
    ], "series")
    _seed_download_history(data_dir, [
        {"cart_item_id": "episode-1", "stream_id": "episode-stream-1", "content_type": "series",
         "series_id": "series-1", "season": "1", "episode_num": 1},
        {"cart_item_id": "episode-1-redownload", "stream_id": "episode-stream-1", "content_type": "series",
         "series_id": "series-1", "season": "1", "episode_num": 1},
        {"cart_item_id": "episode-2", "stream_id": "episode-stream-2", "content_type": "series",
         "series_id": "series-1", "season": "1", "episode_num": 2},
    ])

    client.app.state.cache_service.load_cache_from_disk()
    response = client.get("/api/browse?type=series")

    assert response.status_code == 200
    assert response.json()["items"][0]["downloaded_episode_count"] == 2
    assert client.get(
        "/api/browse?type=series&download_status=not_downloaded"
    ).json()["total"] == 0


def test_browse_category_includes_download_history(client, data_dir):
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Category Movie", "category_id": "10"},
    ], "vod")
    client.app.state.category_service.save_categories({
        "categories": [{
            "id": "favorites",
            "name": "Favorites",
            "mode": "manual",
            "content_types": ["vod"],
            "items": [{"id": "1", "source_id": "src1", "content_type": "vod"}],
        }],
    })
    _seed_download_history(data_dir, [
        {"stream_id": "1", "content_type": "vod"},
    ])

    client.app.state.cache_service.load_cache_from_disk()
    response = client.get("/api/browse?category_id=favorites")

    assert response.status_code == 200
    assert response.json()["items"][0]["items"][0]["downloaded"] is True


def test_browse_grouped_series_deduplicates_episodes_across_sources(client, data_dir):
    _seed_streams(data_dir, [
        {"stream_id": "series-1", "name": "Example Series", "category_id": "10", "tmdb_id": "42"},
    ], "series", source_id="src1")
    _seed_streams(data_dir, [
        {"stream_id": "series-2", "name": "Example Series", "category_id": "10", "tmdb_id": "42"},
    ], "series", source_id="src2")
    _seed_download_history(data_dir, [
        {"cart_item_id": "src1-episode-1", "source_id": "src1", "stream_id": "episode-1",
         "content_type": "series", "series_id": "series-1", "season": "1", "episode_num": 1},
        {"cart_item_id": "src2-episode-1", "source_id": "src2", "stream_id": "episode-1-copy",
         "content_type": "series", "series_id": "series-2", "season": "1", "episode_num": 1},
        {"cart_item_id": "src2-episode-2", "source_id": "src2", "stream_id": "episode-2",
         "content_type": "series", "series_id": "series-2", "season": "1", "episode_num": 2},
    ])

    client.app.state.cache_service.load_cache_from_disk()
    response = client.get("/api/browse?type=series")

    assert response.status_code == 200
    assert response.json()["items"][0]["downloaded_episode_count"] == 2
    not_downloaded = client.get(
        "/api/browse?type=series&download_status=not_downloaded"
    ).json()
    assert not_downloaded["total"] == 0


def test_browse_news_days_filter(client, data_dir):
    import time
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
    ], "live")
    now = int(time.time())
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Recent", "category_id": "10", "added": now - 3600},
        {"stream_id": "2", "name": "Old", "category_id": "10", "added": now - 86400 * 30},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get("/api/browse?type=live&news_days=7")
    data = r.json()
    assert data["total"] == 1
    assert data["items"][0]["name"] == "Recent"


# -------------------------------------------------------------------
# Groups endpoint
# -------------------------------------------------------------------


def test_browse_groups(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
        {"category_id": "20", "category_name": "Sports"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "CNN", "category_id": "10", "added": 1000},
        {"stream_id": "2", "name": "BBC", "category_id": "10", "added": 2000},
        {"stream_id": "3", "name": "ESPN", "category_id": "20", "added": 3000},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get("/api/browse/groups?type=live")
    assert r.status_code == 200
    data = r.json()
    groups = {g["name"]: g["count"] for g in data["groups"]}
    assert groups["News"] == 2
    assert groups["Sports"] == 1


def test_browse_groups_are_cached_until_invalidated(client, data_dir):
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "CNN", "category_id": "10"},
        {"stream_id": "2", "name": "BBC", "category_id": "10"},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    first = {g["name"]: g["count"] for g in client.get("/api/browse/groups?type=live").json()["groups"]}
    assert first == {"News": 2}

    # Mutate the database directly, bypassing the service layer.
    conn = db_connect(os.path.join(data_dir, DB_NAME))
    try:
        conn.execute(
            "INSERT INTO streams "
            "(source_id, content_type, stream_id, name, category_id, added, data) "
            "VALUES (?,?,?,?,?,?,?)",
            ("src1", "live", "3", "Extra", "10", 0, json.dumps({"stream_id": "3", "name": "Extra"})),
        )
        conn.commit()
    finally:
        conn.close()

    cached = {g["name"]: g["count"] for g in client.get("/api/browse/groups?type=live").json()["groups"]}
    assert cached == {"News": 2}

    cache.invalidate_group_counts_cache()
    refreshed = {g["name"]: g["count"] for g in client.get("/api/browse/groups?type=live").json()["groups"]}
    assert refreshed == {"News": 3}


def test_browse_streams_db_uses_cache_only_for_baseline_scope(client, data_dir):
    import time as _time

    now = int(_time.time())
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "CNN", "category_id": "10", "added": now},
        {"stream_id": "2", "name": "BBC", "category_id": "10", "added": now},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    baseline = cache.browse_streams_db(content_type="live", per_page=1)
    assert baseline["group_counts"] == {"News": 2}

    conn = db_connect(os.path.join(data_dir, DB_NAME))
    try:
        conn.execute(
            "INSERT INTO streams "
            "(source_id, content_type, stream_id, name, category_id, added, data) "
            "VALUES (?,?,?,?,?,?,?)",
            ("src1", "live", "3", "Extra", "10", now, json.dumps({"stream_id": "3", "name": "Extra"})),
        )
        conn.commit()
    finally:
        conn.close()

    cached = cache.browse_streams_db(content_type="live", per_page=1)
    assert cached["group_counts"] == {"News": 2}

    filtered = cache.browse_streams_db(content_type="live", per_page=1, news_days=7)
    assert filtered["group_counts"] == {"News": 3}


def test_prune_sources_invalidates_group_counts(client, data_dir):
    import asyncio

    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live", source_id="src1")
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live", source_id="src2")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "A One", "category_id": "10"},
        {"stream_id": "2", "name": "A Two", "category_id": "10"},
    ], "live", source_id="src1")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "B One", "category_id": "10"},
        {"stream_id": "2", "name": "B Two", "category_id": "10"},
    ], "live", source_id="src2")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    warmed = {g["name"]: g["count"] for g in client.get("/api/browse/groups?type=live").json()["groups"]}
    assert warmed == {"News": 4}

    # Simulate an external catalog change (e.g. maintenance deleting rows)
    # that bypasses the service layer entirely.
    conn = db_connect(os.path.join(data_dir, DB_NAME))
    try:
        conn.execute("DELETE FROM streams WHERE source_id = ?", ("src2",))
        conn.execute("DELETE FROM source_categories WHERE source_id = ?", ("src2",))
        conn.commit()
    finally:
        conn.close()

    # Without invalidation the cached aggregate would still report 4.
    stale = {g["name"]: g["count"] for g in client.get("/api/browse/groups?type=live").json()["groups"]}
    assert stale == {"News": 4}

    asyncio.run(cache.prune_sources_to_ids({"src1"}))

    pruned = {g["name"]: g["count"] for g in client.get("/api/browse/groups?type=live").json()["groups"]}
    assert pruned == {"News": 2}


def test_browse_default_sort_uses_denormalized_index(client, data_dir):
    """The default group/name order must be an index walk, not a temp B-tree."""
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "vod")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Movie A", "category_id": "10"},
        {"stream_id": "2", "name": "Movie B", "category_id": "10"},
    ], "vod")
    cache = client.app.state.cache_service
    cache.load_cache_from_disk()
    assert client.get("/api/browse?type=vod").status_code == 200

    conn = db_connect(os.path.join(data_dir, DB_NAME))
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(streams)")}
        plan_rows = conn.execute(
            "EXPLAIN QUERY PLAN SELECT s.stream_id FROM streams s "
            "WHERE s.content_type = 'vod' "
            "ORDER BY lower(s.group_name), lower(s.name), s.source_id, s.stream_id "
            "LIMIT 50 OFFSET 0"
        ).fetchall()
        rating_plan = " ".join(
            row[3]
            for row in conn.execute(
                "EXPLAIN QUERY PLAN SELECT s.stream_id FROM streams s "
                "WHERE s.content_type = 'vod' AND s.rating >= 5 "
                "ORDER BY s.rating DESC, s.source_id DESC, s.stream_id DESC LIMIT 50"
            ).fetchall()
        )
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(streams)")}
    finally:
        conn.close()

    assert {"group_name", "rating"} <= cols
    assert {"idx_streams_ct_group_name", "idx_streams_ct_rating"} <= indexes
    default_plan = " ".join(row[3] for row in plan_rows)
    assert "TEMP B-TREE FOR ORDER BY" not in default_plan
    assert "idx_streams_ct_group_name" in default_plan
    assert "TEMP B-TREE FOR ORDER BY" not in rating_plan
    assert "idx_streams_ct_rating" in rating_plan


def test_browse_heals_legacy_rows_missing_group_name(client, data_dir):
    """Rows written before denormalization must self-heal on first browse."""
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "CNN", "category_id": "10", "rating": 7.5},
    ], "live")

    conn = db_connect(os.path.join(data_dir, DB_NAME))
    try:
        before = conn.execute(
            "SELECT group_name FROM streams WHERE stream_id = '1'"
        ).fetchone()["group_name"]
    finally:
        conn.close()
    assert before is None

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    groups = {g["name"]: g["count"] for g in client.get("/api/browse/groups?type=live").json()["groups"]}
    assert groups == {"News": 1}

    items = client.get("/api/browse?type=live").json()["items"]
    assert items[0]["group"] == "News"

    conn = db_connect(os.path.join(data_dir, DB_NAME))
    try:
        row = conn.execute(
            "SELECT group_name, rating FROM streams WHERE stream_id = '1'"
        ).fetchone()
        nulls = conn.execute("SELECT COUNT(*) FROM streams WHERE group_name IS NULL").fetchone()[0]
    finally:
        conn.close()
    assert row["group_name"] == "News"
    assert row["rating"] == 7.5
    assert nulls == 0


def _seed_manual_category(client, data_dir, category_id: str, members: list[dict]):
    """Create a manual custom category whose members exist in streams."""
    client.app.state.category_service.save_categories({
        "categories": [{
            "id": category_id,
            "name": f"Category {category_id}",
            "mode": "manual",
            "content_types": ["live"],
            "items": members,
        }]
    })


def test_category_count_plan_drives_from_membership_table(client, data_dir):
    """Category queries must be driven by the membership table, not a scan."""
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "CNN", "category_id": "10"},
        {"stream_id": "2", "name": "BBC", "category_id": "10"},
    ], "live")
    _seed_manual_category(client, data_dir, "plan-check", [
        {"id": "1", "source_id": "src1", "content_type": "live"},
        {"id": "2", "source_id": "src1", "content_type": "live"},
    ])
    cache = client.app.state.cache_service
    cache.load_cache_from_disk()
    result = client.get("/api/browse?category_id=plan-check")
    assert result.status_code == 200
    assert result.json()["total"] == 2

    conn = db_connect(os.path.join(data_dir, DB_NAME))
    try:
        plan_rows = conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT COUNT(*) AS cnt FROM category_manual_items ci "
            "JOIN streams s ON s.source_id = ci.source_id "
            "AND s.content_type = ci.content_type AND s.stream_id = ci.stream_id "
            "WHERE ci.category_id = ?",
            ("plan-check",),
        ).fetchall()
    finally:
        conn.close()
    plan = " ".join(row[3] for row in plan_rows)
    assert "SCAN s" not in plan
    assert "ci" in plan


def test_category_totals_cached_until_invalidated(client, data_dir):
    """Repeat category views reuse cached totals until the cache is dropped.

    Note: for small grouped sets the API's top-level ``total`` reports the
    grouped card count computed from freshly fetched items, so staleness is
    asserted against the metadata cache itself.
    """
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": str(i), "name": f"Channel {i}", "category_id": "10"}
        for i in range(1, 4)
    ], "live")
    _seed_manual_category(client, data_dir, "cached-cat", [
        {"id": "1", "source_id": "src1", "content_type": "live"},
        {"id": "2", "source_id": "src1", "content_type": "live"},
    ])

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    first = client.get("/api/browse?category_id=cached-cat").json()
    assert first["total"] == 2

    scope_key = next(
        key for key in cache._group_counts_cache if key[-1] == "manual:cached-cat"
    )
    assert cache._group_counts_cache[scope_key]["total"] == 2

    # External mutation bypassing the service layer.
    conn = db_connect(os.path.join(data_dir, DB_NAME))
    try:
        conn.execute(
            "INSERT INTO category_manual_items (category_id, stream_id, source_id, content_type) "
            "VALUES ('cached-cat', '3', 'src1', 'live')"
        )
        conn.commit()
    finally:
        conn.close()

    # A repeat view must be served from the cache: the stored entry survives
    # untouched because nothing invalidated it.
    client.get("/api/browse?category_id=cached-cat")
    generation_after_get = cache._group_counts_generation
    assert cache._group_counts_cache[scope_key]["total"] == 2
    assert cache._group_counts_cache[scope_key]["group_counts"] == {"News": 2}

    cache.invalidate_group_counts_cache()
    assert cache._group_counts_generation == generation_after_get + 1
    client.get("/api/browse?category_id=cached-cat")
    refreshed_entry = cache._group_counts_cache[scope_key]
    assert refreshed_entry["total"] == 3
    assert refreshed_entry["group_counts"] == {"News": 3}


def test_save_categories_invalidates_scope_metadata(client, data_dir):
    """Editing category membership must refresh cached totals automatically."""
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": str(i), "name": f"Channel {i}", "category_id": "10"}
        for i in range(1, 4)
    ], "live")
    _seed_manual_category(client, data_dir, "edit-cat", [
        {"id": "1", "source_id": "src1", "content_type": "live"},
        {"id": "2", "source_id": "src1", "content_type": "live"},
        {"id": "3", "source_id": "src1", "content_type": "live"},
    ])

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()
    warmed = client.get("/api/browse?category_id=edit-cat").json()
    assert warmed["total"] == 3

    # Removing a member via save_categories triggers the invalidation hook.
    _seed_manual_category(client, data_dir, "edit-cat", [
        {"id": "1", "source_id": "src1", "content_type": "live"},
    ])
    after = client.get("/api/browse?category_id=edit-cat").json()
    assert after["total"] == 1


def test_search_prefers_name_matches_over_group_matches(client, data_dir):
    """Name matches take priority; group-only matches need an empty name pass."""
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
        {"category_id": "20", "category_name": "Alpha Group"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Alpha One", "category_id": "10"},
        {"stream_id": "2", "name": "Zeta", "category_id": "20"},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    data = client.get("/api/browse?type=live&search=alpha").json()
    names = [item["name"] for item in data["items"]]
    assert names == ["Alpha One"]


def test_search_falls_back_to_group_matches_when_no_names_match(client, data_dir):
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "Documentary"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Whales", "category_id": "10"},
        {"stream_id": "2", "name": "Desert Life", "category_id": "10"},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    data = client.get("/api/browse?type=live&search=documenta").json()
    names = {item["name"] for item in data["items"]}
    assert names == {"Whales", "Desert Life"}
    assert data["total"] == 2


def test_source_filter_rules_pushdown_matches_python_semantics(client, data_dir):
    """Simple rules run as SQL and must match should_include() exactly."""
    from app.services.filter_service import should_include

    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "FR| News"},
        {"category_id": "20", "category_name": "AFR Sports"},
        {"category_id": "30", "category_name": "UK| Cinema"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": str(i), "name": f"Channel {i}", "category_id": cid}
        for i, cid in enumerate(["10", "20", "30"], start=1)
    ], "live")

    client.app.state.config_service.config["sources"][0]["filters"] = {
        "live": {
            "groups": [
                {"type": "include", "match": "starts_with", "value": "fr|", "case_sensitive": False},
                {"type": "exclude", "match": "starts_with", "value": "afr", "case_sensitive": False},
            ]
        }
    }
    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    rules = client.app.state.config_service.config["sources"][0]["filters"]["live"]["groups"]
    expected = {
        name
        for name, group in [("Channel 1", "FR| News"), ("Channel 2", "AFR Sports"), ("Channel 3", "UK| Cinema")]
        if should_include(group, rules)
    }

    data = client.get("/api/browse?type=live&use_source_filters=true&page=1&per_page=50").json()
    assert {item["name"] for item in data["items"]} == expected
    assert data["total"] == len(expected)


def test_source_filter_rules_paginate_large_results_in_sql(client, data_dir, monkeypatch):
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "Keep Group"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": str(i), "name": f"Keep {i:04d}", "category_id": "10"}
        for i in range(1, 601)
    ], "live")

    client.app.state.config_service.config["sources"][0]["filters"] = {
        "live": {
            "channels": [
                {"type": "include", "match": "starts_with", "value": "keep", "case_sensitive": False}
            ]
        }
    }
    cache = client.app.state.cache_service
    cache.load_cache_from_disk()
    calls = []
    original = cache.browse_streams_db

    def record_call(*args, **kwargs):
        calls.append(kwargs.copy())
        return original(*args, **kwargs)

    monkeypatch.setattr(cache, "browse_streams_db", record_call)
    result = client.get(
        "/api/browse?type=live&use_source_filters=true&page=4&per_page=25"
    ).json()

    assert result["total"] == 600
    assert len(result["items"]) == 25
    # Pushdown path: single bounded page fetch carrying the SQL predicate.
    assert any("extra_where_sql" in call for call in calls)
    assert all(call.get("_slim") is not True for call in calls)


def test_source_filter_regex_rules_use_bounded_slim_fallback(client, data_dir, monkeypatch):
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "News"}], "live")
    _seed_streams(data_dir, [
        {"stream_id": str(i), "name": f"Match-{i} x" if i % 2 else f"Other {i}", "category_id": "10"}
        for i in range(1, 121)
    ], "live")

    client.app.state.config_service.config["sources"][0]["filters"] = {
        "live": {
            "channels": [
                    {"type": "include", "match": "regex", "value": r"^Match-\d+", "case_sensitive": False}
            ]
        }
    }
    cache = client.app.state.cache_service
    cache.load_cache_from_disk()
    calls = []
    original = cache.browse_streams_db

    def record_call(*args, **kwargs):
        calls.append(kwargs.copy())
        return original(*args, **kwargs)

    monkeypatch.setattr(cache, "browse_streams_db", record_call)
    result = client.get(
        "/api/browse?type=live&use_source_filters=true&page=2&per_page=25"
    ).json()

    assert result["total"] == 60
    assert len(result["items"]) == 25
    slim_calls = [call for call in calls if call.get("_slim")]
    assert slim_calls, "fallback must scan with the slim projection"
    assert all(call["per_page"] <= 1000 for call in slim_calls)


# -------------------------------------------------------------------
# Response structure
# -------------------------------------------------------------------


def test_browse_response_structure(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "CNN", "category_id": "10", "added": 1000},
    ], "live")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    r = client.get("/api/browse?type=live")
    data = r.json()
    # Check all required fields are present
    assert "items" in data
    assert "grouped" in data
    assert "groups" in data
    assert "sources" in data
    assert "total" in data
    assert "page" in data
    assert "per_page" in data
    assert "total_pages" in data
    assert "content_type" in data

    # Check item structure
    item = data["items"][0]
    assert "name" in item
    assert "group" in item
    assert "icon" in item
    assert "id" in item
    assert "source_id" in item
    assert "source_name" in item
    assert "added" in item
    assert "rating" in item
    assert "content_type" in item
    assert "categories" in item


# -------------------------------------------------------------------
# Denormalized item columns (icon/tmdb_id/container_ext)
# -------------------------------------------------------------------


def test_browse_heals_legacy_item_columns(client, data_dir):
    """Legacy rows missing icon/tmdb_id/ext must heal and surface correctly."""
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "Movies"}], "vod")
    _seed_streams(data_dir, [{
        "stream_id": "50",
        "name": "Legacy Movie",
        "category_id": "10",
        "rating": 6.5,
        "stream_icon": "http://img/legacy.png",
        "tmdb_id": "999",
        "container_extension": "mkv",
    }], "vod")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    payload = client.get("/api/browse?type=vod&per_page=5").json()
    # Single vod row is wrapped into a grouped card; inspect its sub-item.
    sub = payload["items"][0]["items"][0]
    assert sub["icon"] == "http://img/legacy.png"
    assert sub["tmdb_id"] == "999"
    assert sub["container_extension"] == "mkv"

    conn = db_connect(os.path.join(data_dir, DB_NAME))
    try:
        row = conn.execute(
            "SELECT icon, tmdb_id, container_ext FROM streams WHERE stream_id = '50'"
        ).fetchone()
        nulls = conn.execute(
            "SELECT COUNT(*) FROM streams WHERE group_name IS NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert row["icon"] == "http://img/legacy.png"
    assert row["tmdb_id"] == "999"
    assert row["container_ext"] == "mkv"
    assert nulls == 0


def test_tmdb_search_uses_denormalized_column(client, data_dir):
    _seed_categories(data_dir, [{"category_id": "10", "category_name": "Movies"}], "vod")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "Inception", "category_id": "10", "tmdb_id": "27205"},
        {"stream_id": "2", "name": "Other Film", "category_id": "10"},
    ], "vod")

    cache = client.app.state.cache_service
    cache.load_cache_from_disk()

    data = client.get("/api/browse?type=vod&search=tmdb:27205").json()
    assert data["total"] == 1
    assert data["items"][0]["name"] == "Inception"


def test_membership_annotation_covers_multi_category_items(client, data_dir):
    _seed_categories(data_dir, [
        {"category_id": "10", "category_name": "News"},
        {"category_id": "20", "category_name": "Sports"},
    ], "live")
    _seed_streams(data_dir, [
        {"stream_id": "1", "name": "ESPN News", "category_id": "10"},
    ], "live")
    client.app.state.category_service.save_categories({
        "categories": [
            {"id": "cat-a", "name": "A", "mode": "manual", "content_types": ["live"],
             "items": [{"id": "1", "source_id": "src1", "content_type": "live"}]},
            {"id": "cat-b", "name": "B", "mode": "manual", "content_types": ["live"],
             "items": [{"id": "1", "source_id": "src1", "content_type": "live"}]},
        ]
    })
    client.app.state.cache_service.load_cache_from_disk()

    item = client.get("/api/browse?type=live").json()["items"][0]
    assert sorted(item["categories"]) == ["cat-a", "cat-b"]


def test_browse_responses_are_gzipped():
    """The production app must wire GZip compression for large payloads."""
    from app.main import app as prod_app

    middleware_names = [
        getattr(mw, "cls", mw).__name__ if not isinstance(mw, type) else mw.__name__
        for mw in prod_app.user_middleware
    ]
    assert "GZipMiddleware" in middleware_names
