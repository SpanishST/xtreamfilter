"""Integration tests for the monitor edit / rebuild-known routes.

Covers the bug where PUT /api/monitor/{id} changed monitor_sources (or
scope/season_filter) but left the known_episodes snapshot stale, causing
spurious 'new episode' notifications for every episode on the newly-added
source. Also covers the POST /api/monitor/{id}/rebuild-known endpoint that
lets the user manually rebuild a stale snapshot.
"""

from __future__ import annotations

import asyncio
import json
import os

import pytest
from starlette.testclient import TestClient

from app.database import DB_NAME, init_db
from app.services.config_service import ConfigService
from app.services.monitor_service import MonitorService


def _run(coro):
    return asyncio.run(coro)


class _FakeXtream:
    """Minimal xtream_service stub returning canned series episodes per (source, series_id)."""

    def __init__(self, fetch_results: dict):
        self._fetch_results = fetch_results
        # Track fetch calls so tests can assert on them
        self.calls: list[tuple[str, str]] = []

    async def fetch_series_episodes(self, source_id, series_id):
        self.calls.append((source_id, str(series_id)))
        return self._fetch_results.get((source_id, str(series_id)), [])

    async def fetch_series_info(self, source_id, series_id):
        return None


def _build_monitor_app(tmp_dir: str, fetch_results: dict):
    """Build a minimal FastAPI app with only the monitor routes wired."""
    from fastapi import FastAPI

    from app.routes import monitor_api

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
    with open(os.path.join(tmp_dir, "config.json"), "w") as f:
        json.dump(config, f)

    db_path = os.path.join(tmp_dir, DB_NAME)
    init_db(db_path)
    cfg = ConfigService(tmp_dir)
    cfg.load()

    xtream = _FakeXtream(fetch_results)

    monitor = MonitorService.__new__(MonitorService)
    monitor._cfg = cfg
    monitor.cache_service = None
    monitor.cart_service = None
    monitor.notification_service = None
    monitor.xtream_service = xtream
    monitor.db_path = db_path
    monitor._monitored_series = []
    monitor._monitored_movies = []
    monitor._check_in_progress = False
    monitor.log_service = None

    app = FastAPI()
    app.state.config_service = cfg
    app.state.monitor_service = monitor
    app.state.xtream_service = xtream
    app.include_router(monitor_api.router)
    return app, monitor, xtream


@pytest.fixture()
def monitor_app(tmp_path):
    def _make(fetch_results: dict):
        return _build_monitor_app(str(tmp_path), fetch_results)
    return _make


# -------------------------------------------------------------------
# PUT /api/monitor/{id} — refresh known_episodes on Edit
# -------------------------------------------------------------------

class TestUpdateMonitorRefreshesKnownEpisodes:
    """When monitor_sources / scope / season_filter change on Edit the
    known_episodes snapshot must be rebuilt via build_known_episodes_snapshot
    so the new source's existing catalogue is not treated as 'new'."""

    def test_adding_a_source_rebuilds_known_episodes(self, monitor_app):
        fetch_results = {
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "E1"},
                {"stream_id": "a2", "season": "1", "episode_num": 2, "title": "E2"},
            ],
            ("srcB", "22"): [
                {"stream_id": "b1", "season": "1", "episode_num": 1, "title": "E1"},
                {"stream_id": "b2", "season": "1", "episode_num": 2, "title": "E2"},
                {"stream_id": "b3", "season": "1", "episode_num": 3, "title": "E3"},
            ],
        }
        app, monitor, xtream = monitor_app(fetch_results)
        # Seed a monitor with only srcA — known_episodes already contains
        # srcA's two episodes (as it would after a normal add_monitored).
        monitor._monitored_series = [
            {
                "id": "m1",
                "series_name": "Test Show",
                "canonical_name": None,
                "series_id": "11",
                "source_id": None,
                "source_name": None,
                "source_category": None,
                "monitor_sources": [{"source_id": "srcA", "series_ref": "11"}],
                "custom_category_ids": [],
                "cover": "",
                "scope": "new_only",
                "season_filter": None,
                "action": "notify",
                "enabled": True,
                "known_episodes": [
                    {"stream_id": "a1", "source_id": "srcA", "season": "1", "episode_num": 1},
                    {"stream_id": "a2", "source_id": "srcA", "season": "1", "episode_num": 2},
                ],
                "downloaded_episodes": [],
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }
        ]
        monitor.save_monitored()

        with TestClient(app) as c:
            r = c.put("/api/monitor/m1", json={
                "monitor_sources": [
                    {"source_id": "srcA", "series_ref": "11"},
                    {"source_id": "srcB", "series_ref": "22"},
                ],
            })

        assert r.status_code == 200
        entry = r.json()["entry"]
        # The snapshot must now contain 3 unique (season, ep_num) keys across
        # both sources — without the rebuild, srcB's entire catalogue would
        # be considered 'new' on the next check.
        keys = {(str(k["season"]), k["episode_num"]) for k in entry["known_episodes"]}
        assert keys == {("1", 1), ("1", 2), ("1", 3)}

    def test_changing_scope_to_all_clears_known_episodes(self, monitor_app):
        """scope='all' intentionally means 'notify about every episode',
        so the snapshot must be empty after the edit."""
        app, monitor, _ = monitor_app({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "E1"},
            ],
        })
        monitor._monitored_series = [
            {
                "id": "m2",
                "series_name": "Scope Show",
                "canonical_name": None,
                "series_id": "11",
                "source_id": None,
                "source_name": None,
                "source_category": None,
                "monitor_sources": [{"source_id": "srcA", "series_ref": "11"}],
                "custom_category_ids": [],
                "cover": "",
                "scope": "new_only",
                "season_filter": None,
                "action": "notify",
                "enabled": True,
                "known_episodes": [
                    {"stream_id": "a1", "source_id": "srcA", "season": "1", "episode_num": 1},
                ],
                "downloaded_episodes": [],
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }
        ]
        monitor.save_monitored()

        with TestClient(app) as c:
            r = c.put("/api/monitor/m2", json={"scope": "all"})

        assert r.status_code == 200
        entry = r.json()["entry"]
        assert entry["scope"] == "all"
        assert entry["known_episodes"] == []

    def test_changing_scope_from_all_to_new_only_builds_snapshot(self, monitor_app):
        app, monitor, _ = monitor_app({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "E1"},
                {"stream_id": "a2", "season": "1", "episode_num": 2, "title": "E2"},
            ],
        })
        monitor._monitored_series = [
            {
                "id": "m3",
                "series_name": "Scope Show",
                "canonical_name": None,
                "series_id": "11",
                "source_id": None,
                "source_name": None,
                "source_category": None,
                "monitor_sources": [{"source_id": "srcA", "series_ref": "11"}],
                "custom_category_ids": [],
                "cover": "",
                "scope": "all",
                "season_filter": None,
                "action": "notify",
                "enabled": True,
                "known_episodes": [],
                "downloaded_episodes": [],
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }
        ]
        monitor.save_monitored()

        with TestClient(app) as c:
            r = c.put("/api/monitor/m3", json={"scope": "new_only"})

        assert r.status_code == 200
        entry = r.json()["entry"]
        keys = {(str(k["season"]), k["episode_num"]) for k in entry["known_episodes"]}
        assert keys == {("1", 1), ("1", 2)}

    def test_unchanged_fields_do_not_trigger_rebuild(self, monitor_app):
        """When none of monitor_sources/scope/season_filter change, the
        snapshot must be preserved (no spurious upstream calls)."""
        app, monitor, xtream = monitor_app({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "E1"},
            ],
        })
        original_known = [
            {"stream_id": "a1", "source_id": "srcA", "season": "1", "episode_num": 1},
        ]
        monitor._monitored_series = [
            {
                "id": "m4",
                "series_name": "Untouched",
                "canonical_name": None,
                "series_id": "11",
                "source_id": None,
                "source_name": None,
                "source_category": None,
                "monitor_sources": [{"source_id": "srcA", "series_ref": "11"}],
                "custom_category_ids": [],
                "cover": "",
                "scope": "new_only",
                "season_filter": None,
                "action": "notify",
                "enabled": True,
                "known_episodes": list(original_known),
                "downloaded_episodes": [],
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }
        ]
        monitor.save_monitored()

        with TestClient(app) as c:
            # Only toggle enabled — must NOT trigger a snapshot rebuild
            r = c.put("/api/monitor/m4", json={"enabled": False})

        assert r.status_code == 200
        entry = r.json()["entry"]
        assert entry["known_episodes"] == original_known
        # fetch_series_episodes must not have been called for a snapshot rebuild
        assert xtream.calls == []

    def test_unchanged_scope_does_not_rebuild_or_prune(self, monitor_app):
        """Sanity check: when scope/season_filter/monitor_sources are unchanged
        on Edit, the snapshot and downloaded_episodes must be left alone even
        if a stale-looking (1, 52) key is present — pruning only happens
        alongside a snapshot rebuild."""
        app, monitor, _ = monitor_app({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 2, "title": "E2"},
                {"stream_id": "a2", "season": "1", "episode_num": 3, "title": "E3"},
            ],
        })
        stale_known = [
            {"stream_id": "x1", "source_id": "srcA", "season": "1", "episode_num": 52},
        ]
        stale_dl = [
            {"stream_id": "x1", "source_id": "srcA", "season": "1", "episode_num": 52},
        ]
        monitor._monitored_series = [
            {
                "id": "m5",
                "series_name": "Stale Show",
                "canonical_name": None,
                "series_id": "11",
                "source_id": None,
                "source_name": None,
                "source_category": None,
                "monitor_sources": [{"source_id": "srcA", "series_ref": "11"}],
                "custom_category_ids": [],
                "cover": "",
                "scope": "new_only",
                "season_filter": None,
                "action": "notify",
                "enabled": True,
                "known_episodes": list(stale_known),
                "downloaded_episodes": list(stale_dl),
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }
        ]
        monitor.save_monitored()

        with TestClient(app) as c:
            # PUT the same scope value — no diff → no rebuild, no prune.
            r = c.put("/api/monitor/m5", json={"scope": "new_only"})

        assert r.status_code == 200
        entry = r.json()["entry"]
        assert entry["known_episodes"] == stale_known
        assert entry["downloaded_episodes"] == stale_dl

    def test_prunes_dangling_downloaded_episodes_when_monitor_sources_change(self, monitor_app):
        """The actual prune scenario: monitor_sources change triggers a
        rebuild that returns a different key set, and any downloaded_episodes
        entry that no longer matches the new known set is pruned."""
        app, monitor, _ = monitor_app({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 2, "title": "E2"},
                {"stream_id": "a2", "season": "1", "episode_num": 3, "title": "E3"},
            ],
        })
        monitor._monitored_series = [
            {
                "id": "m5b",
                "series_name": "Prune Show B",
                "canonical_name": None,
                "series_id": "11",
                "source_id": "srcOld",  # legacy single-source — about to be replaced
                "source_name": None,
                "source_category": None,
                "monitor_sources": [],
                "custom_category_ids": [],
                "cover": "",
                "scope": "new_only",
                "season_filter": None,
                "action": "notify",
                "enabled": True,
                "known_episodes": [
                    {"stream_id": "old1", "source_id": "srcOld", "season": "1", "episode_num": 99},
                ],
                "downloaded_episodes": [
                    {"stream_id": "old1", "source_id": "srcOld", "season": "1", "episode_num": 99},
                ],
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }
        ]
        monitor.save_monitored()

        with TestClient(app) as c:
            r = c.put("/api/monitor/m5b", json={
                "monitor_sources": [{"source_id": "srcA", "series_ref": "11"}],
            })

        assert r.status_code == 200
        entry = r.json()["entry"]
        new_keys = {(str(k["season"]), k["episode_num"]) for k in entry["known_episodes"]}
        assert new_keys == {("1", 2), ("1", 3)}
        # The stale (1, 99) downloaded_episodes entry is pruned
        remaining_dl_keys = {
            (str(d["season"]), d["episode_num"])
            for d in entry["downloaded_episodes"]
        }
        assert remaining_dl_keys == set()  # (1, 99) was dangling → pruned


# -------------------------------------------------------------------
# POST /api/monitor/{id}/rebuild-known
# -------------------------------------------------------------------

class TestRebuildKnownEndpoint:
    """POST /api/monitor/{id}/rebuild-known manually rebuilds the known
    episodes snapshot — useful when upstream numbering has changed and the
    existing snapshot is stale."""

    def test_rebuilds_snapshot_and_prunes_dangling_downloaded(self, monitor_app):
        app, monitor, _ = monitor_app({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 2, "title": "E2"},
                {"stream_id": "a2", "season": "1", "episode_num": 3, "title": "E3"},
            ],
        })
        monitor._monitored_series = [
            {
                "id": "r1",
                "series_name": "Rebuild Show",
                "canonical_name": None,
                "series_id": "11",
                "source_id": None,
                "source_name": None,
                "source_category": None,
                "monitor_sources": [{"source_id": "srcA", "series_ref": "11"}],
                "custom_category_ids": [],
                "cover": "",
                "scope": "new_only",
                "season_filter": None,
                "action": "notify",
                "enabled": True,
                "known_episodes": [
                    {"stream_id": "old", "source_id": "srcA", "season": "1", "episode_num": 99},
                ],
                "downloaded_episodes": [
                    {"stream_id": "old", "source_id": "srcA", "season": "1", "episode_num": 99},
                ],
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }
        ]
        monitor.save_monitored()

        with TestClient(app) as c:
            r = c.post("/api/monitor/r1/rebuild-known")

        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["known_episodes_count"] == 2  # E2 + E3
        # Stale (1, 99) is pruned
        assert data["downloaded_episodes_count"] == 0

        # Verify persistence: reload from DB and check the snapshot is the new one
        monitor._monitored_series = []
        monitor.load_monitored()
        entry = monitor._monitored_series[0]
        keys = {(str(k["season"]), k["episode_num"]) for k in entry["known_episodes"]}
        assert keys == {("1", 2), ("1", 3)}
        assert entry["downloaded_episodes"] == []

    def test_rebuild_returns_404_for_unknown_monitor(self, monitor_app):
        app, _, _ = monitor_app({})
        with TestClient(app) as c:
            r = c.post("/api/monitor/nonexistent/rebuild-known")
        assert r.status_code == 404

    def test_rebuild_for_scope_all_returns_empty_snapshot(self, monitor_app):
        """scope='all' intentionally keeps the snapshot empty — rebuild must
        respect that and NOT prune downloaded_episodes (no reference set)."""
        app, monitor, _ = monitor_app({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "E1"},
            ],
        })
        downloaded = [
            {"stream_id": "a1", "source_id": "srcA", "season": "1", "episode_num": 1},
        ]
        monitor._monitored_series = [
            {
                "id": "r3",
                "series_name": "All Scope",
                "canonical_name": None,
                "series_id": "11",
                "source_id": None,
                "source_name": None,
                "source_category": None,
                "monitor_sources": [{"source_id": "srcA", "series_ref": "11"}],
                "custom_category_ids": [],
                "cover": "",
                "scope": "all",
                "season_filter": None,
                "action": "notify",
                "enabled": True,
                "known_episodes": [],
                "downloaded_episodes": list(downloaded),
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }
        ]
        monitor.save_monitored()

        with TestClient(app) as c:
            r = c.post("/api/monitor/r3/rebuild-known")

        assert r.status_code == 200
        data = r.json()
        assert data["known_episodes_count"] == 0
        # scope=all → no reference set → downloaded_episodes preserved
        assert data["downloaded_episodes_count"] == 1


# -------------------------------------------------------------------
# GET /api/monitor/series-lookup — discover additional sources
# -------------------------------------------------------------------

class _FakeCacheService:
    """Minimal cache_service stub returning canned series list."""

    def __init__(self, series_list: list):
        self._series_list = series_list

    def get_cached_with_source_info(self, key, category_key):
        return self._series_list, []


def _build_monitor_app_with_cache(tmp_dir: str, series_list: list, fetch_results: dict | None = None):
    """Build a FastAPI app with a fake cache_service for series-lookup tests."""
    from fastapi import FastAPI

    from app.routes import monitor_api

    config = {
        "sources": [
            {"id": "srcA", "name": "Source A", "host": "http://a", "username": "u", "password": "p", "enabled": True},
            {"id": "srcB", "name": "Source B", "host": "http://b", "username": "u", "password": "p", "enabled": True},
            {"id": "srcC", "name": "Source C", "host": "http://c", "username": "u", "password": "p", "enabled": True},
        ],
        "filters": {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
        "content_types": {"live": True, "vod": True, "series": True},
        "options": {},
    }
    with open(os.path.join(tmp_dir, "config.json"), "w") as f:
        json.dump(config, f)

    db_path = os.path.join(tmp_dir, DB_NAME)
    init_db(db_path)
    cfg = ConfigService(tmp_dir)
    cfg.load()

    xtream = _FakeXtream(fetch_results or {})

    monitor = MonitorService.__new__(MonitorService)
    monitor._cfg = cfg
    monitor.cache_service = _FakeCacheService(series_list)
    monitor.cart_service = None
    monitor.notification_service = None
    monitor.xtream_service = xtream
    monitor.db_path = db_path
    monitor._monitored_series = []
    monitor._monitored_movies = []
    monitor._check_in_progress = False
    monitor.log_service = None

    app = FastAPI()
    app.state.config_service = cfg
    app.state.monitor_service = monitor
    app.state.xtream_service = xtream
    app.include_router(monitor_api.router)
    return app, monitor, xtream


class TestSeriesLookupEndpoint:
    """GET /api/monitor/series-lookup returns sources that have a given series."""

    def test_returns_matching_sources_by_name(self, tmp_path):
        series_list = [
            {"_source_id": "srcA", "series_id": "11", "name": "Breaking Bad", "cover": "", "tmdb_id": None, "imdb_id": None},
            {"_source_id": "srcB", "series_id": "22", "name": "Breaking Bad", "cover": "", "tmdb_id": None, "imdb_id": None},
            {"_source_id": "srcC", "series_id": "33", "name": "Other Show", "cover": "", "tmdb_id": None, "imdb_id": None},
        ]
        app, _, _ = _build_monitor_app_with_cache(str(tmp_path), series_list)

        with TestClient(app) as c:
            r = c.get("/api/monitor/series-lookup", params={"name": "Breaking Bad"})

        assert r.status_code == 200
        results = r.json()["results"]
        assert len(results) == 2
        source_ids = {m["source_id"] for m in results}
        assert source_ids == {"srcA", "srcB"}
        # Verify source_name is resolved from config
        for m in results:
            assert "source_name" in m
            assert m["series_ref"] in ("11", "22")

    def test_returns_empty_for_empty_query(self, tmp_path):
        app, _, _ = _build_monitor_app_with_cache(str(tmp_path), [])

        with TestClient(app) as c:
            r = c.get("/api/monitor/series-lookup", params={"name": ""})

        assert r.status_code == 200
        assert r.json()["results"] == []

    def test_matches_by_tmdb_id(self, tmp_path):
        series_list = [
            {"_source_id": "srcA", "series_id": "11", "name": "Show A", "cover": "", "tmdb_id": "12345", "imdb_id": None},
            {"_source_id": "srcB", "series_id": "22", "name": "Show B", "cover": "", "tmdb_id": "12345", "imdb_id": None},
            {"_source_id": "srcC", "series_id": "33", "name": "Show C", "cover": "", "tmdb_id": "99999", "imdb_id": None},
        ]
        app, _, _ = _build_monitor_app_with_cache(str(tmp_path), series_list)

        with TestClient(app) as c:
            r = c.get("/api/monitor/series-lookup", params={"tmdb_id": "12345"})

        assert r.status_code == 200
        results = r.json()["results"]
        assert len(results) == 2
        source_ids = {m["source_id"] for m in results}
        assert source_ids == {"srcA", "srcB"}
        for m in results:
            assert m["matched_by"] == "id"

    def test_resolves_source_name_from_config(self, tmp_path):
        series_list = [
            {"_source_id": "srcA", "series_id": "11", "name": "My Show", "cover": "", "tmdb_id": None, "imdb_id": None},
        ]
        app, _, _ = _build_monitor_app_with_cache(str(tmp_path), series_list)

        with TestClient(app) as c:
            r = c.get("/api/monitor/series-lookup", params={"name": "My Show"})

        assert r.status_code == 200
        results = r.json()["results"]
        assert len(results) == 1
        assert results[0]["source_name"] == "Source A"
