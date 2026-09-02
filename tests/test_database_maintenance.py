"""Tests for online database cleanup and optimization."""

from __future__ import annotations

import asyncio
import json
import os

from fastapi import FastAPI
from starlette.testclient import TestClient

from app.database import DB_NAME, db_connect, init_db
from app.dependencies import get_database_maintenance_service
from app.routes import database_api
from app.services.cache_service import CacheService
from app.services.config_service import ConfigService
from app.services.database_maintenance_service import DatabaseMaintenanceService
from app.services.http_client import HttpClientService


def _build_services(tmp_path):
    config = {
        "sources": [
            {
                "id": "src-1",
                "name": "Primary",
                "host": "http://provider.test",
                "username": "user",
                "password": "pass",
                "enabled": True,
            }
        ],
        "xtream": {"host": "", "username": "", "password": ""},
        "options": {"cache_ttl": 3600, "refresh_interval": 3600},
    }
    (tmp_path / "config.json").write_text(json.dumps(config))
    db_path = os.path.join(tmp_path, DB_NAME)
    init_db(db_path)
    cfg = ConfigService(str(tmp_path))
    cfg.load()
    cache = CacheService(cfg, HttpClientService())
    maintenance = DatabaseMaintenanceService(cfg, cache)
    return db_path, cache, maintenance


def _seed_database(db_path: str) -> None:
    conn = db_connect(db_path)
    try:
        conn.execute(
            "INSERT INTO streams "
            "(source_id, content_type, stream_id, name, category_id, added, data) "
            "VALUES (?,?,?,?,?,?,?)",
            ("src-1", "live", "current", "Current Channel", "10", 1, json.dumps({"stream_id": "current", "name": "Current Channel"})),
        )
        conn.execute(
            "INSERT INTO streams "
            "(source_id, content_type, stream_id, name, category_id, added, data) "
            "VALUES (?,?,?,?,?,?,?)",
            ("removed-source", "live", "removed", "Removed Channel", "20", 1, json.dumps({"stream_id": "removed", "name": "Removed Channel"})),
        )
        conn.execute(
            "INSERT INTO source_categories "
            "(source_id, content_type, category_id, category_name, data) VALUES (?,?,?,?,?)",
            ("src-1", "live", "10", "Current", json.dumps({"category_id": "10", "category_name": "Current"})),
        )
        conn.execute(
            "INSERT INTO source_categories "
            "(source_id, content_type, category_id, category_name, data) VALUES (?,?,?,?,?)",
            ("removed-source", "live", "20", "Removed", json.dumps({"category_id": "20", "category_name": "Removed"})),
        )
        conn.execute(
            "INSERT INTO custom_categories (id, name) VALUES (?, ?)",
            ("custom-1", "Custom"),
        )
        conn.execute(
            "INSERT INTO category_cached_items "
            "(category_id, stream_id, source_id, content_type) VALUES (?,?,?,?)",
            ("custom-1", "removed", "removed-source", "live"),
        )
        conn.execute(
            "INSERT INTO source_last_refresh (source_id, last_refresh) VALUES (?, ?)",
            ("removed-source", "2026-01-01T00:00:00+00:00"),
        )
        # Simulate the orphaned FTS documents created by the old persistence code.
        conn.execute(
            "INSERT INTO streams_fts(rowid, name) VALUES (?, ?)",
            (999999, "Orphaned Search Entry"),
        )
        conn.commit()
    finally:
        conn.close()


def _start_and_wait(maintenance: DatabaseMaintenanceService) -> tuple[dict, dict]:
    async def run():
        result = maintenance.start()
        status = await _wait_for_maintenance_async(maintenance)
        return result, status

    return asyncio.run(run())


async def _wait_for_maintenance_async(maintenance: DatabaseMaintenanceService) -> dict:
    while maintenance.is_active():
        await asyncio.sleep(0.01)
    return maintenance.get_status()


def test_database_maintenance_rebuilds_fts_and_removes_deconfigured_sources(tmp_path):
    db_path, cache, maintenance = _build_services(tmp_path)
    _seed_database(db_path)

    # Warm the group metadata cache so we can prove maintenance invalidates it.
    warmed = {g["name"]: g["count"] for g in cache.browse_group_counts_db("live")}
    assert warmed == {"Current": 1, "Removed": 1}

    result, status = _start_and_wait(maintenance)
    assert result["started"] is True

    assert status["status"] == "succeeded"
    assert status["phase"] == "complete"
    assert status["percent"] == 100
    assert status["sources_removed"] == 1
    assert status["rows_removed"] == 1
    assert status["categories_removed"] == 1
    assert status["category_items_removed"] == 1
    assert status["fts_rebuilt"] is True
    assert status["vacuumed"] is True
    assert status["fts_orphans_before"] == 1
    assert status["fts_orphans_after"] == 0

    conn = db_connect(db_path)
    try:
        streams = conn.execute(
            "SELECT source_id, content_type, stream_id FROM streams"
        ).fetchall()
        categories = conn.execute(
            "SELECT source_id, content_type, category_id FROM source_categories"
        ).fetchall()
        cached_items = conn.execute("SELECT source_id FROM category_cached_items").fetchall()
        source_refresh = conn.execute("SELECT source_id FROM source_last_refresh").fetchall()
        fts_documents = conn.execute("SELECT COUNT(*) FROM streams_fts_docsize").fetchone()[0]
        fts_orphans = conn.execute(
            "SELECT COUNT(*) FROM streams_fts_docsize d "
            "LEFT JOIN streams s ON s.rowid=d.id WHERE s.rowid IS NULL"
        ).fetchone()[0]
        current_matches = conn.execute(
            "SELECT COUNT(*) FROM streams_fts WHERE streams_fts MATCH ?", ("Current",)
        ).fetchone()[0]
        removed_matches = conn.execute(
            "SELECT COUNT(*) FROM streams_fts WHERE streams_fts MATCH ?", ("Removed",)
        ).fetchone()[0]
        orphan_matches = conn.execute(
            "SELECT COUNT(*) FROM streams_fts WHERE streams_fts MATCH ?", ("Orphaned",)
        ).fetchone()[0]
    finally:
        conn.close()

    assert [(row["source_id"], row["content_type"], row["stream_id"]) for row in streams] == [
        ("src-1", "live", "current")
    ]
    assert [(row["source_id"], row["content_type"], row["category_id"]) for row in categories] == [
        ("src-1", "live", "10")
    ]
    assert cached_items == []
    assert source_refresh == []
    assert fts_documents == 1
    assert fts_orphans == 0
    assert current_matches == 1
    assert removed_matches == 0
    assert orphan_matches == 0

    invalidated = {g["name"]: g["count"] for g in cache.browse_group_counts_db("live")}
    assert invalidated == {"Current": 1}


def test_database_maintenance_is_rejected_while_refresh_is_active(tmp_path):
    _db_path, cache, maintenance = _build_services(tmp_path)
    cache._api_cache["refresh_in_progress"] = True

    result = maintenance.start()

    assert result["started"] is False
    assert result["status"] == "busy"
    assert maintenance.is_active() is False
    assert cache.is_maintenance_active() is False


def test_database_cleanup_api_reports_status_and_conflicts(tmp_path):
    _db_path, _cache, maintenance = _build_services(tmp_path)
    app = FastAPI()
    app.state.database_maintenance_service = maintenance
    app.dependency_overrides[get_database_maintenance_service] = lambda: maintenance
    app.include_router(database_api.router)

    with TestClient(app) as client:
        status_response = client.get("/api/database/cleanup/status")
        assert status_response.status_code == 200
        assert status_response.json()["status"] == "idle"

        maintenance.start = lambda: {
            "started": False,
            "status": "busy",
            "message": "Cache refresh is currently running; try again when it completes",
        }
        start_response = client.post("/api/database/cleanup")

    assert start_response.status_code == 409
    assert start_response.json()["status"] == "busy"


def test_db_connect_enables_memory_mapped_io(tmp_path):
    """mmap keeps reads fast without hoarding process memory."""
    import os as _os

    from app.database import db_connect as _connect

    conn = _connect(str(tmp_path / DB_NAME))
    try:
        assert conn.execute("PRAGMA mmap_size").fetchone()[0] > 0
    finally:
        conn.close()
