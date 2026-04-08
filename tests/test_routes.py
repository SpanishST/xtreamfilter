"""Integration tests — hit actual FastAPI routes via Starlette TestClient."""

import asyncio
import json
import os

import pytest
from starlette.testclient import TestClient

from app.services.config_service import ConfigService
from app.services.http_client import HttpClientService
from app.services.jellyfin_service import JellyfinService
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
    jellyfin = JellyfinService(cfg, http)
    cat = CategoryService(cfg, cache, notif)
    cart = CartService(cfg, http, notif, xtream, jellyfin)
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
    app.state.jellyfin_service = jellyfin
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


def test_jellyfin_config_masks_api_key(client):
    r = client.post(
        "/api/config/jellyfin",
        json={
            "enabled": True,
            "base_url": "http://jellyfin.local:8096/",
            "api_key": "secret1234",
            "trigger_file": True,
            "trigger_queue": True,
        },
    )
    assert r.status_code == 200

    r = client.get("/api/config/jellyfin")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["base_url"] == "http://jellyfin.local:8096"
    assert data["api_key"] == ""
    assert data["api_key_masked"] == "***1234"


def test_jellyfin_config_preserves_existing_api_key(client):
    client.post(
        "/api/config/jellyfin",
        json={
            "enabled": True,
            "base_url": "http://jellyfin.local:8096",
            "api_key": "persist-me-1234",
            "trigger_file": True,
        },
    )

    r = client.post(
        "/api/config/jellyfin",
        json={
            "base_url": "http://other.local:8096/",
            "api_key": "",
            "trigger_queue": True,
        },
    )
    assert r.status_code == 200

    saved = client.app.state.config_service.get_jellyfin_config()
    assert saved["base_url"] == "http://other.local:8096"
    assert saved["api_key"] == "persist-me-1234"
    assert saved["trigger_file"] is True
    assert saved["trigger_queue"] is True


def test_jellyfin_test_route_success(client, monkeypatch):
    calls = {}

    async def fake_test_connection(base_url=None, api_key=None):
        calls["base_url"] = base_url
        calls["api_key"] = api_key
        return {
            "ok": True,
            "message": "Connected to Testfin (10.10.0)",
            "status_code": 200,
            "server_name": "Testfin",
            "version": "10.10.0",
        }

    monkeypatch.setattr(client.app.state.jellyfin_service, "test_connection", fake_test_connection)

    r = client.post(
        "/api/config/jellyfin/test",
        json={"base_url": "http://jellyfin.local:8096", "api_key": "secret"},
    )
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
    assert calls == {"base_url": "http://jellyfin.local:8096", "api_key": "secret"}


def test_jellyfin_test_route_propagates_errors(client, monkeypatch):
    async def fake_test_connection(base_url=None, api_key=None):
        return {
            "ok": False,
            "message": "Jellyfin rejected the API key",
            "status_code": 401,
        }

    monkeypatch.setattr(client.app.state.jellyfin_service, "test_connection", fake_test_connection)

    r = client.post("/api/config/jellyfin/test", json={"base_url": "http://jellyfin.local:8096"})
    assert r.status_code == 401
    assert r.json()["message"] == "Jellyfin rejected the API key"


def test_finalize_completed_download_triggers_jellyfin_file(client, monkeypatch):
    cart = client.app.state.cart_service
    calls = {"write": None, "embed": None, "notify": None, "triggers": []}

    async def fake_write_metadata(item, file_path):
        calls["write"] = file_path

    async def fake_embed_metadata(item, file_path):
        calls["embed"] = file_path

    async def fake_file_notification(item):
        calls["notify"] = item["id"]

    async def fake_refresh(trigger):
        calls["triggers"].append(trigger)
        return {"ok": True, "triggered": True, "skipped": False}

    monkeypatch.setattr(cart, "_write_metadata", fake_write_metadata)
    monkeypatch.setattr(cart, "_embed_container_metadata", fake_embed_metadata)
    monkeypatch.setattr(cart.notification_service, "send_download_file_notification", fake_file_notification)
    monkeypatch.setattr(cart.jellyfin_service, "trigger_library_refresh", fake_refresh)

    item = {
        "id": "item-1",
        "status": "completed",
        "file_path": "/tmp/test-file.mkv",
    }

    asyncio.run(cart._finalize_completed_download(item))

    assert calls["write"] == "/tmp/test-file.mkv"
    assert calls["embed"] == "/tmp/test-file.mkv"
    assert calls["notify"] == "item-1"
    assert calls["triggers"] == ["file"]


def test_move_route_reuses_finalize_completed_download(client, monkeypatch):
    cart = client.app.state.cart_service
    download_dir = os.path.join(client.app.state.config_service.data_dir, "downloads")
    temp_dir = os.path.join(client.app.state.config_service.data_dir, "tmp")
    os.makedirs(temp_dir, exist_ok=True)

    client.app.state.config_service.config.setdefault("options", {})
    client.app.state.config_service.config["options"]["download_path"] = download_dir
    client.app.state.config_service.save()

    temp_path = os.path.join(temp_dir, "movie.tmp")
    with open(temp_path, "wb") as f:
        f.write(b"123456")

    item = {
        "id": "move-1",
        "stream_id": "stream-1",
        "source_id": "",
        "content_type": "vod",
        "name": "Recovered Movie",
        "series_name": None,
        "series_id": None,
        "season": None,
        "episode_num": None,
        "episode_title": None,
        "icon": "",
        "group": "",
        "container_extension": "mp4",
        "added_at": "2026-01-01T00:00:00",
        "status": "move_failed",
        "progress": 0,
        "error": "move failed",
        "file_path": None,
        "file_size": None,
        "temp_path": temp_path,
    }
    cart._download_cart = [item]
    cart.save_cart()

    finalized = []

    async def fake_finalize(moved_item):
        finalized.append(moved_item["id"])

    monkeypatch.setattr(cart, "_finalize_completed_download", fake_finalize)

    r = client.post("/api/cart/move-1/move")
    assert r.status_code == 200
    assert finalized == ["move-1"]
    assert cart._download_cart[0]["status"] == "completed"
    assert os.path.exists(cart._download_cart[0]["file_path"])


def test_queue_complete_helper_triggers_jellyfin_queue(client, monkeypatch):
    cart = client.app.state.cart_service
    cart._download_cart = [{"id": "done-1", "status": "completed", "file_size": 42, "name": "Movie"}]
    calls = {"queue_items": None, "triggers": []}

    async def fake_queue_notification(items):
        calls["queue_items"] = [item["id"] for item in items]

    async def fake_refresh(trigger):
        calls["triggers"].append(trigger)
        return {"ok": True, "triggered": True, "skipped": False}

    monkeypatch.setattr(cart.notification_service, "send_download_queue_complete_notification", fake_queue_notification)
    monkeypatch.setattr(cart.jellyfin_service, "trigger_library_refresh", fake_refresh)

    asyncio.run(cart._handle_download_queue_complete())

    assert calls["queue_items"] == ["done-1"]
    assert calls["triggers"] == ["queue"]


def test_jellyfin_refresh_cooldown_skips_immediate_duplicate(client, monkeypatch):
    import httpx

    service = client.app.state.jellyfin_service
    client.app.state.config_service.config.setdefault("options", {})
    client.app.state.config_service.config["options"]["jellyfin"] = {
        "enabled": True,
        "base_url": "http://jellyfin.local:8096",
        "api_key": "secret",
        "trigger_file": True,
        "trigger_queue": True,
    }

    class FakeClient:
        def __init__(self):
            self.calls = []

        async def post(self, url, headers=None, timeout=None):
            self.calls.append({"url": url, "headers": headers, "timeout": timeout})
            return httpx.Response(204, request=httpx.Request("POST", url))

    fake_client = FakeClient()

    async def fake_get_client():
        return fake_client

    monkeypatch.setattr(service.http_client, "get_client", fake_get_client)

    first = asyncio.run(service.trigger_library_refresh("file"))
    second = asyncio.run(service.trigger_library_refresh("queue"))

    assert first["triggered"] is True
    assert second["skipped"] is True
    assert len(fake_client.calls) == 1


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
    assert "Jellyfin Refresh" in r.text


def test_browse_page(client):
    r = client.get("/browse")
    assert r.status_code == 200


def test_cart_page(client):
    r = client.get("/cart")
    assert r.status_code == 200
    assert "Jellyfin Refresh" not in r.text


def test_monitor_page(client):
    r = client.get("/monitor")
    assert r.status_code == 200
