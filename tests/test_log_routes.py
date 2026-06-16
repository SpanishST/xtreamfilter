"""Integration tests for the activity log API routes."""

from __future__ import annotations

import asyncio
import json
import os

import pytest
from starlette.testclient import TestClient

from app.database import DB_NAME, init_db
from app.services.config_service import ConfigService
from app.services.log_service import LogService


def _run(coro):
    return asyncio.run(coro)


def _build_log_app(tmp_dir: str):
    """Build a minimal FastAPI app with only the log routes wired."""
    from fastapi import FastAPI

    from app.routes import log_api, ui

    config = {
        "sources": [],
        "filters": {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
        "content_types": {"live": True, "vod": True, "series": True},
        "options": {"log_retention_days": 30},
    }
    with open(os.path.join(tmp_dir, "config.json"), "w") as f:
        json.dump(config, f)

    db_path = os.path.join(tmp_dir, DB_NAME)
    init_db(db_path)
    cfg = ConfigService(tmp_dir)
    cfg.load()
    log_svc = LogService(db_path, cfg)

    app = FastAPI()
    app.state.log_service = log_svc
    app.state.config_service = cfg
    app.include_router(log_api.router)
    app.include_router(ui.router)
    return app, log_svc


@pytest.fixture()
def log_app(tmp_path):
    app, svc = _build_log_app(str(tmp_path))
    return app, svc


@pytest.fixture()
def client(log_app):
    app, _ = log_app
    with TestClient(app) as c:
        yield c


# -------------------------------------------------------------------
# GET /api/logs
# -------------------------------------------------------------------

class TestGetLogs:

    def test_empty_logs(self, client):
        r = client.get("/api/logs")
        assert r.status_code == 200
        assert r.json() == []

    def test_returns_seeded_logs(self, client, log_app):
        _, svc = log_app
        _run(svc.log("cache", "info", "Refresh started"))
        _run(svc.log("cache", "error", "Refresh failed"))
        r = client.get("/api/logs")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 2
        # Newest first
        assert data[0]["message"] == "Refresh failed"
        assert data[1]["message"] == "Refresh started"

    def test_filter_by_category(self, client, log_app):
        _, svc = log_app
        _run(svc.log("cache", "info", "Cache event"))
        _run(svc.log("cart", "info", "Cart event"))
        _run(svc.log("monitor", "info", "Monitor event"))

        r = client.get("/api/logs", params={"category": "cart"})
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["category"] == "cart"

    def test_filter_by_level(self, client, log_app):
        _, svc = log_app
        _run(svc.log("cache", "info", "Info"))
        _run(svc.log("cache", "error", "Error"))
        _run(svc.log("cache", "warning", "Warning"))

        r = client.get("/api/logs", params={"level": "error"})
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["level"] == "error"

    def test_filter_by_category_and_level(self, client, log_app):
        _, svc = log_app
        _run(svc.log("cache", "info", "A"))
        _run(svc.log("cache", "error", "B"))
        _run(svc.log("cart", "error", "C"))

        r = client.get("/api/logs", params={"category": "cache", "level": "error"})
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["message"] == "B"

    def test_limit_param(self, client, log_app):
        _, svc = log_app
        for i in range(5):
            _run(svc.log("cache", "info", f"Event {i}"))

        r = client.get("/api/logs", params={"limit": 3})
        assert r.status_code == 200
        assert len(r.json()) == 3

    def test_offset_param(self, client, log_app):
        _, svc = log_app
        for i in range(5):
            _run(svc.log("cache", "info", f"Event {i}"))

        page1 = client.get("/api/logs", params={"limit": 2, "offset": 0}).json()
        page2 = client.get("/api/logs", params={"limit": 2, "offset": 2}).json()
        assert len(page1) == 2
        assert len(page2) == 2
        page1_ids = {e["id"] for e in page1}
        page2_ids = {e["id"] for e in page2}
        assert page1_ids.isdisjoint(page2_ids)

    def test_invalid_category_rejected(self, client):
        r = client.get("/api/logs", params={"category": "invalid"})
        assert r.status_code == 422

    def test_invalid_level_rejected(self, client):
        r = client.get("/api/logs", params={"level": "critical"})
        assert r.status_code == 422

    def test_details_parsed_as_json(self, client, log_app):
        _, svc = log_app
        _run(svc.log("cache", "info", "With details", {"key": "value", "count": 42}))
        r = client.get("/api/logs")
        data = r.json()
        assert data[0]["details"] == {"key": "value", "count": 42}

    def test_details_string_not_parsed(self, client, log_app):
        _, svc = log_app
        _run(svc.log("cache", "info", "Plain details", "just a string"))
        r = client.get("/api/logs")
        data = r.json()
        assert data[0]["details"] == "just a string"


# -------------------------------------------------------------------
# GET /api/logs/counts
# -------------------------------------------------------------------

class TestGetLogCounts:

    def test_empty_counts(self, client):
        r = client.get("/api/logs/counts")
        assert r.status_code == 200
        assert r.json() == {"cache": 0, "cart": 0, "monitor": 0}

    def test_populated_counts(self, client, log_app):
        _, svc = log_app
        _run(svc.log("cache", "info", "a"))
        _run(svc.log("cache", "info", "b"))
        _run(svc.log("cart", "info", "c"))
        _run(svc.log("monitor", "error", "d"))

        r = client.get("/api/logs/counts")
        assert r.status_code == 200
        assert r.json() == {"cache": 2, "cart": 1, "monitor": 1}


# -------------------------------------------------------------------
# DELETE /api/logs
# -------------------------------------------------------------------

class TestClearLogs:

    def test_clear_all(self, client, log_app):
        _, svc = log_app
        _run(svc.log("cache", "info", "a"))
        _run(svc.log("cart", "info", "b"))
        _run(svc.log("monitor", "info", "c"))

        r = client.delete("/api/logs")
        assert r.status_code == 200
        assert r.json()["deleted"] == 3

        # Verify empty
        r = client.get("/api/logs")
        assert r.json() == []

    def test_clear_by_category(self, client, log_app):
        _, svc = log_app
        _run(svc.log("cache", "info", "a"))
        _run(svc.log("cart", "info", "b"))

        r = client.delete("/api/logs", params={"category": "cache"})
        assert r.status_code == 200
        assert r.json()["deleted"] == 1

        # Cart still there
        r = client.get("/api/logs")
        assert len(r.json()) == 1
        assert r.json()[0]["category"] == "cart"

    def test_clear_empty(self, client):
        r = client.delete("/api/logs")
        assert r.status_code == 200
        assert r.json()["deleted"] == 0

    def test_clear_invalid_category_rejected(self, client):
        r = client.delete("/api/logs", params={"category": "nope"})
        assert r.status_code == 422


# -------------------------------------------------------------------
# GET /logs — UI page
# -------------------------------------------------------------------

class TestLogsPage:

    def test_page_renders(self, client):
        r = client.get("/logs")
        assert r.status_code == 200
        assert "Activity Logs" in r.text
        assert "Cache Refresh" in r.text
        assert "Cart" in r.text
        assert "Monitor" in r.text
