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
