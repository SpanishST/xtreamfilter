"""Focused cache refresh behavior tests."""

from __future__ import annotations

import asyncio
import json
import os

from app.database import DB_NAME, init_db
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