"""Integration tests — hit actual FastAPI routes via Starlette TestClient."""

import json
import os

import pytest
from starlette.testclient import TestClient

from app.services.config_service import ConfigService
from app.services.http_client import HttpClientService
from app.services.cache_service import CacheService
from app.services.epg_service import EpgService
from app.services.xtream_service import XtreamService
from app.services.notification_service import NotificationService
from app.services.category_service import CategoryService
from app.services.cart_service import CartService
from app.services.monitor_service import MonitorService
from app.services.m3u_service import M3uService
from app.database import DB_NAME, init_db


def _build_app(data_dir: str):
    """Build a fully-wired FastAPI app pointing at *data_dir*."""
    from fastapi import FastAPI, Request

    from app.routes import (
        ui, filter_api, source_api, playlist, browse_api,
        category_api, xtream_merged, stream_proxy, xtream_source,
        epg, config_api, cache_api, cart_api, monitor_api, health,
    )

    cfg = ConfigService(data_dir)
    cfg.load()
    init_db(os.path.join(data_dir, DB_NAME))
    http = HttpClientService()
    cache = CacheService(cfg, http)
    epg_svc = EpgService(cfg, http, cache)
    xtream = XtreamService(cfg, cache, http)
    notif = NotificationService(cfg, http)
    cat = CategoryService(cfg, cache, notif)
    cart = CartService(cfg, http, notif, xtream)
    monitor = MonitorService(cfg, cache, xtream, notif, cart)
    m3u = M3uService(cfg, cache)

    app = FastAPI()

    # Attach state for DI
    app.state.config_service = cfg
    app.state.http_client = http
    app.state.cache_service = cache
    app.state.epg_service = epg_svc
    app.state.xtream_service = xtream
    app.state.notification_service = notif
    app.state.category_service = cat
    app.state.cart_service = cart
    app.state.monitor_service = monitor
    app.state.m3u_service = m3u

    # UTF-8 charset middleware
    @app.middleware("http")
    async def add_utf8_charset(request: Request, call_next):
        response = await call_next(request)
        ct = response.headers.get("content-type", "")
        if "application/json" in ct and "charset" not in ct:
            response.headers["content-type"] = "application/json; charset=utf-8"
        return response

    for r in (
        ui, health, filter_api, source_api, config_api,
        cache_api, cart_api, monitor_api, epg, category_api,
        browse_api, playlist, xtream_merged, stream_proxy, xtream_source,
    ):
        app.include_router(r.router)

    return app


@pytest.fixture()
def data_dir(tmp_path):
    """Create a temporary data directory with a minimal config."""
    config = {
        "sources": [],
        "filters": {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
        "content_types": {"live": True, "vod": True, "series": True},
        "options": {},
    }
    (tmp_path / "config.json").write_text(json.dumps(config))
    return str(tmp_path)


@pytest.fixture()
def client(data_dir):
    app = _build_app(data_dir)
    with TestClient(app) as c:
        yield c


# -------------------------------------------------------------------
# Health / Version
# -------------------------------------------------------------------

def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_version(client):
    r = client.get("/api/version")
    assert r.status_code == 200
    data = r.json()
    assert "current" in data


# -------------------------------------------------------------------
# Options / Config API
# -------------------------------------------------------------------

def test_options_get(client):
    r = client.get("/api/options")
    assert r.status_code == 200


def test_options_proxy_toggle(client):
    r = client.post("/api/options/proxy", json={"enabled": False})
    assert r.status_code == 200
    assert r.json()["proxy_enabled"] is False

    r = client.get("/api/options/proxy")
    assert r.json()["proxy_enabled"] is False


def test_refresh_interval(client):
    r = client.post("/api/options/refresh_interval", json={"refresh_interval": 100})
    assert r.status_code == 200
    # Minimum is 300
    assert r.json()["refresh_interval"] >= 300


# -------------------------------------------------------------------
# Filters API
# -------------------------------------------------------------------

def test_get_filters(client):
    r = client.get("/api/filters")
    assert r.status_code == 200
    data = r.json()
    assert "live" in data


def test_add_filter(client):
    rule = {
        "content_type": "live",
        "filter_type": "groups",
        "rule": {"type": "include", "match": "contains", "value": "sports", "case_sensitive": False},
    }
    r = client.post("/api/filters/add", json=rule)
    assert r.status_code == 200

    r = client.get("/api/filters")
    assert len(r.json()["live"]["groups"]) == 1


# -------------------------------------------------------------------
# Sources API
# -------------------------------------------------------------------

def test_sources_crud(client):
    # GET — empty
    r = client.get("/api/sources")
    assert r.status_code == 200
    assert r.json()["sources"] == []

    # POST — create
    source = {
        "name": "Test source",
        "host": "http://example.com",
        "username": "user",
        "password": "pass",
    }
    r = client.post("/api/sources", json=source)
    assert r.status_code == 200
    src_id = r.json()["source"]["id"]

    # GET — now has 1
    r = client.get("/api/sources")
    assert len(r.json()["sources"]) == 1

    # PUT — update
    r = client.put(f"/api/sources/{src_id}", json={"name": "Updated"})
    assert r.status_code == 200
    assert r.json()["source"]["name"] == "Updated"

    # DELETE
    r = client.delete(f"/api/sources/{src_id}")
    assert r.status_code == 200
    r = client.get("/api/sources")
    assert r.json()["sources"] == []


# -------------------------------------------------------------------
# Cache API
# -------------------------------------------------------------------

def test_cache_status(client):
    r = client.get("/api/cache/status")
    assert r.status_code == 200
    data = r.json()
    assert "counts" in data


def test_cache_clear(client):
    r = client.post("/api/cache/clear")
    assert r.status_code == 200


# -------------------------------------------------------------------
# Cart API
# -------------------------------------------------------------------

def test_cart_empty(client):
    r = client.get("/api/cart")
    assert r.status_code == 200
    assert r.json()["items"] == []


def test_cart_status(client):
    r = client.get("/api/cart/status")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 0
    assert data["is_running"] is False


# -------------------------------------------------------------------
# Monitor API
# -------------------------------------------------------------------

def test_monitor_empty(client):
    r = client.get("/api/monitor")
    assert r.status_code == 200
    assert r.json()["series"] == []


def test_monitor_add_persists_external_ids(client):
    payload = {
        "series_name": "Test Show",
        "series_id": "123",
        "scope": "all",
        "action": "notify",
        "tmdb_id": "tmdb:100088",
        "imdb_id": "1234567",
    }
    r = client.post("/api/monitor", json=payload)
    assert r.status_code == 200
    entry = r.json()["entry"]
    assert entry["tmdb_id"] == "100088"
    assert entry["imdb_id"] == "tt1234567"


# -------------------------------------------------------------------
# EPG API
# -------------------------------------------------------------------

def test_epg_status(client):
    r = client.get("/api/epg/status")
    assert r.status_code == 200
    data = r.json()
    assert "cached" in data


# -------------------------------------------------------------------
# Playlist (empty cache → empty m3u)
# -------------------------------------------------------------------

def test_playlist_empty(client):
    r = client.get("/playlist.m3u")
    assert r.status_code == 200
    assert "#EXTM3U" in r.text


# -------------------------------------------------------------------
# Player profiles
# -------------------------------------------------------------------

def test_player_profiles(client):
    r = client.get("/api/options/player_profiles")
    assert r.status_code == 200
    data = r.json()
    assert "tivimate" in data


# -------------------------------------------------------------------
# UI pages serve HTML
# -------------------------------------------------------------------

def test_index_page(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]


def test_browse_page(client):
    r = client.get("/browse")
    assert r.status_code == 200


def test_cart_page(client):
    r = client.get("/cart")
    assert r.status_code == 200


def test_monitor_page(client):
    r = client.get("/monitor")
    assert r.status_code == 200
