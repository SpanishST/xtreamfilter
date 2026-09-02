"""Focused cache refresh behavior tests."""

from __future__ import annotations

import asyncio
import json
import os

from app.database import DB_NAME, db_connect, init_db
from app.services.cache_service import CacheService
from app.services.config_service import ConfigService
from app.services.http_client import HttpClientService


def _build_cache_service(tmp_path) -> CacheService:
    config = {
        "sources": [
            {
                "id": "src-1",
                "name": "Primary",
                "host": "http://provider.test",
                "username": "user",
                "password": "pass",
                "enabled": True,
                "prefix": "",
                "filters": {
                    "live": {"groups": [], "channels": []},
                    "vod": {"groups": [], "channels": []},
                    "series": {"groups": [], "channels": []},
                },
            }
        ],
        "filters": {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
        "content_types": {"live": True, "vod": True, "series": True},
        "options": {"cache_ttl": 3600, "refresh_interval": 3600},
    }
    (tmp_path / "config.json").write_text(json.dumps(config))
    init_db(os.path.join(tmp_path, DB_NAME))
    cfg = ConfigService(str(tmp_path))
    cfg.load()
    return CacheService(cfg, HttpClientService())


def _seed_existing_source(cache: CacheService):
    old_vod_streams = [{"stream_id": "vod-old", "name": "Old Movie", "category_id": "20"}]
    old_series = [{"series_id": "series-old", "name": "Legacy Series", "category_id": "30"}]
    cache._api_cache["sources"] = {
        "src-1": {
            "live_categories": [{"category_id": "10", "category_name": "Old Live"}],
            "vod_categories": [{"category_id": "20", "category_name": "Old VOD"}],
            "series_categories": [{"category_id": "30", "category_name": "Old Series"}],
            "live_streams": [{"stream_id": "live-old", "name": "Old Live", "category_id": "10"}],
            "vod_streams": old_vod_streams,
            "series": old_series,
            "last_refresh": "2026-04-09T12:00:00",
        }
    }
    cache._api_cache["last_refresh"] = "2026-04-09T12:00:00"
    return old_vod_streams, old_series


def _partial_refresh_responses() -> dict:
    return {
        "get_live_categories": {
            "ok": True,
            "action": "get_live_categories",
            "data": [{"category_id": "10", "category_name": "Fresh Live"}],
            "status_code": 200,
            "duration_ms": 10,
            "attempts": 1,
            "error": None,
        },
        "get_vod_categories": {
            "ok": True,
            "action": "get_vod_categories",
            "data": [{"category_id": "20", "category_name": "Fresh VOD"}],
            "status_code": 200,
            "duration_ms": 10,
            "attempts": 1,
            "error": None,
        },
        "get_series_categories": {
            "ok": True,
            "action": "get_series_categories",
            "data": [{"category_id": "30", "category_name": "Fresh Series"}],
            "status_code": 200,
            "duration_ms": 10,
            "attempts": 1,
            "error": None,
        },
        "get_live_streams": {
            "ok": True,
            "action": "get_live_streams",
            "data": [{"stream_id": "live-new", "name": "Fresh Live", "category_id": "10"}],
            "status_code": 200,
            "duration_ms": 12,
            "attempts": 1,
            "error": None,
        },
        "get_vod_streams": {
            "ok": False,
            "action": "get_vod_streams",
            "data": None,
            "status_code": 400,
            "duration_ms": 14,
            "attempts": 1,
            "error": {
                "type": "http_error",
                "message": "HTTP 400 while fetching get_vod_streams",
                "status_code": 400,
            },
        },
        "get_series": {
            "ok": True,
            "action": "get_series",
            "data": [{"series_id": "series-new", "name": "Fresh Series", "category_id": "30"}],
            "status_code": 200,
            "duration_ms": 11,
            "attempts": 1,
            "error": None,
        },
    }


def test_refresh_cache_preserves_stale_data_on_partial_failure(tmp_path):
    cache = _build_cache_service(tmp_path)
    old_vod_streams, _ = _seed_existing_source(cache)
    responses = _partial_refresh_responses()

    async def fake_fetch(_host, _username, _password, action, retries=2):
        return responses[action]

    cache.fetch_from_upstream = fake_fetch

    refreshed = asyncio.run(cache.refresh_cache())

    assert refreshed is True
    progress = cache.load_refresh_progress()
    assert progress["status"] == "partial"
    assert progress["summary"]["failed_steps"] == 1
    assert progress["summary"]["preserved_steps"] == 1
    assert progress["summary"]["partial_sources"] == 1

    source_result = progress["source_results"][0]
    assert source_result["status"] == "partial"
    assert source_result["errors"][0]["label"] == "VOD streams"
    assert source_result["errors"][0]["preserved_existing"] is True

    vod_step = next(step for step in source_result["steps"] if step["key"] == "vod_streams")
    assert vod_step["status"] == "failed"
    assert vod_step["preserved_existing"] is True
    assert vod_step["count"] == len(old_vod_streams)

    refreshed_source = cache._api_cache["sources"]["src-1"]
    assert refreshed_source["live_streams"][0]["stream_id"] == "live-new"
    assert refreshed_source["vod_streams"][0]["stream_id"] == old_vod_streams[0]["stream_id"]
    assert refreshed_source["vod_streams"][0]["name"] == old_vod_streams[0]["name"]
    assert refreshed_source["series"][0]["series_id"] == "series-new"
    assert cache._api_cache["last_refresh"] is not None


def test_refresh_cache_replaces_successful_steps_in_database(tmp_path):
    cache = _build_cache_service(tmp_path)
    _seed_existing_source(cache)
    cache.save_cache_to_disk()

    responses = _partial_refresh_responses()
    responses["get_vod_streams"] = {
        "ok": True,
        "action": "get_vod_streams",
        "data": [{"stream_id": "vod-new", "name": "Fresh Movie", "category_id": "20"}],
        "status_code": 200,
        "duration_ms": 10,
        "attempts": 1,
        "error": None,
    }

    async def fake_fetch(_host, _username, _password, action, retries=2):
        return responses[action]

    cache.fetch_from_upstream = fake_fetch
    assert asyncio.run(cache.refresh_cache()) is True

    conn = db_connect(os.path.join(tmp_path, DB_NAME))
    try:
        stream_rows = conn.execute(
            "SELECT content_type, stream_id FROM streams ORDER BY content_type, stream_id"
        ).fetchall()
        category_rows = conn.execute(
            "SELECT content_type, category_id FROM source_categories ORDER BY content_type, category_id"
        ).fetchall()
        fts_old = conn.execute(
            "SELECT COUNT(*) FROM streams_fts WHERE streams_fts MATCH ?", ("Old",)
        ).fetchone()[0]
        fts_new = conn.execute(
            "SELECT COUNT(*) FROM streams_fts WHERE streams_fts MATCH ?", ("Fresh",)
        ).fetchone()[0]
    finally:
        conn.close()

    assert [(row["content_type"], row["stream_id"]) for row in stream_rows] == [
        ("live", "live-new"),
        ("series", "series-new"),
        ("vod", "vod-new"),
    ]
    assert [(row["content_type"], row["category_id"]) for row in category_rows] == [
        ("live", "10"),
        ("series", "30"),
        ("vod", "20"),
    ]
    assert fts_old == 0
    assert fts_new == 3


def test_refresh_cache_dispatches_failure_notification(tmp_path):
    cache = _build_cache_service(tmp_path)
    _seed_existing_source(cache)
    responses = _partial_refresh_responses()
    sent_statuses = []

    class FakeNotificationService:
        async def send_cache_refresh_failure_notification(self, progress):
            sent_statuses.append(progress["status"])

    cache.notification_service = FakeNotificationService()

    async def fake_fetch(_host, _username, _password, action, retries=2):
        return responses[action]

    cache.fetch_from_upstream = fake_fetch

    refreshed = asyncio.run(cache.refresh_cache())

    assert refreshed is True
    assert sent_statuses == ["partial"]


def test_refresh_reports_browse_category_phase_before_completion(tmp_path):
    cache = _build_cache_service(tmp_path)
    responses = _partial_refresh_responses()
    category_started = asyncio.Event()
    release_categories = asyncio.Event()

    async def fake_fetch(_host, _username, _password, action, retries=2):
        return responses[action]

    async def refresh_categories():
        category_started.set()
        await release_categories.wait()

    cache.fetch_from_upstream = fake_fetch

    async def exercise():
        refresh_task = asyncio.create_task(cache.refresh_cache(on_cache_refreshed=refresh_categories))
        await category_started.wait()

        progress = cache.load_refresh_progress()
        assert progress["in_progress"] is True
        assert progress["phase"] == "categories"
        assert progress["current_step"] == "Refreshing configured browse categories..."
        assert progress["percent"] == 95
        persisted_progress = await cache.load_refresh_progress_async()
        assert persisted_progress["phase"] == "categories"
        assert persisted_progress["percent"] == 95

        release_categories.set()
        assert await refresh_task is True

    asyncio.run(exercise())

    progress = cache.load_refresh_progress()
    assert progress["in_progress"] is False
    assert progress["phase"] == "complete"
    assert progress["percent"] == 100


def test_refresh_cache_slims_memory_cache_when_persistence_fails(tmp_path):
    cache = _build_cache_service(tmp_path)
    responses = _partial_refresh_responses()
    responses["get_vod_streams"]["ok"] = True
    responses["get_vod_streams"]["data"] = [
        {
            "stream_id": "vod-new",
            "name": "New Movie",
            "category_id": "20",
            "plot": "Large metadata that must not remain in memory",
        }
    ]

    async def fake_fetch(_host, _username, _password, action, retries=2):
        return responses[action]

    async def failed_save():
        raise RuntimeError("database unavailable")

    cache.fetch_from_upstream = fake_fetch
    cache.save_cache_to_disk_async = failed_save

    refreshed = asyncio.run(cache.refresh_cache())

    assert refreshed is False
    assert cache.load_refresh_progress()["status"] == "failed"
    stream = cache._api_cache["sources"]["src-1"]["vod_streams"][0]
    assert stream["stream_id"] == "vod-new"
    assert "plot" not in stream


def test_refresh_cache_does_not_overlap(tmp_path):
    cache = _build_cache_service(tmp_path)
    active = 0
    maximum_active = 0

    async def fake_refresh(_callback=None):
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return True

    cache._refresh_cache_locked = fake_refresh

    async def exercise():
        return await asyncio.gather(cache.refresh_cache(), cache.refresh_cache())

    results = asyncio.run(exercise())

    assert sorted(results) == [False, True]
    assert maximum_active == 1


def test_start_refresh_owns_one_background_task(tmp_path):
    cache = _build_cache_service(tmp_path)
    started = 0

    async def fake_refresh(on_cache_refreshed=None):
        nonlocal started
        started += 1
        await asyncio.sleep(0)
        return True

    cache.refresh_cache = fake_refresh

    async def exercise():
        assert cache.start_refresh() is True
        assert cache.start_refresh() is False
        await cache._refresh_task
        assert cache.start_refresh() is True
        await cache._refresh_task

    asyncio.run(exercise())

    assert started == 2


def test_cancel_refresh_stops_task_and_persists_terminal_state(tmp_path):
    cache = _build_cache_service(tmp_path)
    first_fetch_started = asyncio.Event()
    calls = []

    async def blocked_fetch(_host, _username, _password, action, retries=2):
        calls.append(action)
        first_fetch_started.set()
        await asyncio.Event().wait()

    cache.fetch_from_upstream = blocked_fetch

    async def exercise():
        assert cache.start_refresh() is True
        await first_fetch_started.wait()

        assert await cache.cancel_refresh() is True
        await asyncio.sleep(0)

    asyncio.run(exercise())

    assert calls == ["get_live_categories"]
    assert cache._refresh_task is None
    assert cache._api_cache["refresh_in_progress"] is False
    assert cache.load_refresh_progress()["status"] == "cancelled"

    conn = db_connect(os.path.join(tmp_path, DB_NAME))
    try:
        row = conn.execute(
            "SELECT in_progress, status, current_step, last_error FROM refresh_progress WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()

    assert row["in_progress"] == 0
    assert row["status"] == "cancelled"
    assert row["current_step"] == "Cancelled"
    assert row["last_error"] == "Cancelled by user"


def test_cancel_refresh_does_not_publish_partial_cache_or_run_callback(tmp_path):
    cache = _build_cache_service(tmp_path)
    old_vod_streams, _ = _seed_existing_source(cache)
    second_fetch_started = asyncio.Event()
    calls = []
    callback_calls = []

    async def fetch_with_block(_host, _username, _password, action, retries=2):
        calls.append(action)
        if action == "get_live_categories":
            return {
                "ok": True,
                "action": action,
                "data": [{"category_id": "10", "category_name": "Fresh Live"}],
                "status_code": 200,
                "duration_ms": 1,
                "attempts": 1,
                "error": None,
            }
        second_fetch_started.set()
        await asyncio.Event().wait()

    async def post_refresh_callback():
        callback_calls.append(True)

    cache.fetch_from_upstream = fetch_with_block

    async def exercise():
        assert cache.start_refresh(on_cache_refreshed=post_refresh_callback) is True
        await second_fetch_started.wait()
        assert await cache.cancel_refresh() is True

    asyncio.run(exercise())

    assert calls == ["get_live_categories", "get_vod_categories"]
    assert callback_calls == []
    assert cache._api_cache["sources"]["src-1"]["vod_streams"] == old_vod_streams
    assert cache.load_refresh_progress()["status"] == "cancelled"
