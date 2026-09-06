"""Tests for category-driven automatic download reconciliation."""
from __future__ import annotations

import asyncio
import json
import os
from types import SimpleNamespace

from app.database import DB_NAME, db_connect, init_db
from app.services.autodownload_service import AutodownloadService


class _Config:
    def __init__(self, data_dir, sources):
        self.data_dir = str(data_dir)
        self._sources = sources

    def get_enabled_sources(self):
        return self._sources

    def get_download_destination(self, content_type):
        return "Series" if content_type == "series" else "Films"


class _Cart:
    def __init__(self):
        self.queued = []

    async def add_prebuilt_items(self, selections):
        items = []
        for index, selection in enumerate(selections):
            item = dict(selection)
            item["id"] = f"auto-{len(self.queued) + index}"
            items.append(item)
        self.queued.extend(items)
        return {"added": len(items), "items": items}

    def is_in_download_window(self):
        return False

    def _try_start_worker(self):
        return False


class _Xtream:
    def __init__(self, episodes):
        self.episodes = episodes

    async def fetch_series_episodes(self, source_id, series_id):
        return self.episodes.get((source_id, series_id), [])


def _insert_category(db_path, category_id="cat-1"):
    conn = db_connect(db_path)
    try:
        conn.execute(
            "INSERT INTO custom_categories "
            "(id, name, icon, mode, content_types, pattern_logic, sort_order) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (category_id, "Autodownload", "folder", "manual", '["vod","series"]', "or", 0),
        )
        conn.execute(
            "INSERT INTO category_autodownload "
            "(category_id, enabled, movies_enabled, series_enabled, source_priority, "
            "series_seasons, baseline_initialized) VALUES (?, 1, 1, 1, ?, '[]', ?)",
            (category_id, json.dumps(["src2", "src1"]), 1),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_stream(db_path, source_id, stream_id, content_type, name, tmdb_id=None):
    conn = db_connect(db_path)
    try:
        conn.execute(
            "INSERT INTO streams "
            "(source_id, content_type, stream_id, name, data, tmdb_id, title_key) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (source_id, content_type, stream_id, name, json.dumps({"name": name, "tmdb_id": tmdb_id}), tmdb_id, name.lower()),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_membership(db_path, category_id, source_id, stream_id, content_type):
    conn = db_connect(db_path)
    try:
        conn.execute(
            "INSERT INTO category_manual_items "
            "(category_id, stream_id, source_id, content_type) VALUES (?, ?, ?, ?)",
            (category_id, stream_id, source_id, content_type),
        )
        conn.commit()
    finally:
        conn.close()


def _service(tmp_path, *, episodes=None):
    db_path = os.path.join(tmp_path, DB_NAME)
    init_db(db_path)
    config = _Config(tmp_path, [
        {"id": "src1", "enabled": True},
        {"id": "src2", "enabled": True},
    ])
    cart = _Cart()
    service = AutodownloadService(
        config,
        SimpleNamespace(),
        SimpleNamespace(),
        _Xtream(episodes or {}),
        cart,
    )
    return db_path, service, cart


def test_category_autodownload_baselines_then_queues_new_movie(tmp_path):
    db_path, service, cart = _service(tmp_path)
    _insert_category(db_path)
    _insert_stream(db_path, "src1", "movie-1", "vod", "Movie One", "100")
    _insert_membership(db_path, "cat-1", "src1", "movie-1", "vod")
    category = {
        "id": "cat-1",
        "mode": "manual",
        "autodownload": {
            "enabled": True,
            "movies_enabled": True,
            "series_enabled": False,
            "source_priority": ["src1"],
            "series_seasons": [],
            "baseline_initialized": False,
        },
    }

    asyncio.run(service._reconcile_category(category))
    assert cart.queued == []

    _insert_stream(db_path, "src1", "movie-2", "vod", "Movie Two", "200")
    _insert_membership(db_path, "cat-1", "src1", "movie-2", "vod")
    category["autodownload"]["baseline_initialized"] = True
    asyncio.run(service._reconcile_category(category))

    assert [item["stream_id"] for item in cart.queued] == ["movie-2"]


def test_category_autodownload_backfill_queues_baseline_content(tmp_path):
    db_path, service, cart = _service(tmp_path)
    _insert_category(db_path)
    _insert_stream(db_path, "src1", "movie-1", "vod", "Movie One", "100")
    _insert_membership(db_path, "cat-1", "src1", "movie-1", "vod")
    category = {
        "id": "cat-1",
        "mode": "manual",
        "autodownload": {
            "enabled": True,
            "movies_enabled": True,
            "series_enabled": False,
            "source_priority": ["src1"],
            "series_seasons": [],
            "baseline_initialized": False,
        },
    }
    service.category_service.get_category_by_id = lambda category_id: category

    result = asyncio.run(service.backfill_category("cat-1"))

    assert result["queued"] == 1
    assert cart.queued[0]["stream_id"] == "movie-1"


def test_category_autodownload_uses_source_priority(tmp_path):
    db_path, service, cart = _service(tmp_path)
    _insert_category(db_path)
    _insert_stream(db_path, "src1", "movie-1", "vod", "Shared Movie", "100")
    _insert_stream(db_path, "src2", "movie-2", "vod", "Shared Movie", "100")
    _insert_membership(db_path, "cat-1", "src1", "movie-1", "vod")
    _insert_membership(db_path, "cat-1", "src2", "movie-2", "vod")
    category = {
        "id": "cat-1",
        "mode": "manual",
        "autodownload": {
            "enabled": True,
            "movies_enabled": True,
            "series_enabled": False,
            "source_priority": ["src2", "src1"],
            "series_seasons": [],
            "baseline_initialized": True,
        },
    }

    asyncio.run(service._reconcile_category(category))

    assert len(cart.queued) == 1
    assert cart.queued[0]["source_id"] == "src2"
    assert cart.queued[0]["stream_id"] == "movie-2"


def test_category_autodownload_queues_new_series_episode_from_preferred_source(tmp_path):
    episodes = {
        ("src1", "series-1"): [{"stream_id": "ep-1", "season": "1", "episode_num": 1, "title": "Pilot"}],
        ("src2", "series-2"): [{"stream_id": "ep-2", "season": "1", "episode_num": 1, "title": "Pilot"}],
    }
    db_path, service, cart = _service(tmp_path, episodes=episodes)
    _insert_category(db_path)
    _insert_stream(db_path, "src1", "series-1", "series", "Shared Series", "300")
    _insert_stream(db_path, "src2", "series-2", "series", "Shared Series", "300")
    _insert_membership(db_path, "cat-1", "src1", "series-1", "series")
    _insert_membership(db_path, "cat-1", "src2", "series-2", "series")
    category = {
        "id": "cat-1",
        "mode": "manual",
        "autodownload": {
            "enabled": True,
            "movies_enabled": False,
            "series_enabled": True,
            "source_priority": ["src2", "src1"],
            "series_seasons": ["1"],
            "baseline_initialized": True,
        },
    }

    asyncio.run(service._reconcile_category(category))

    assert len(cart.queued) == 1
    assert cart.queued[0]["source_id"] == "src2"
    assert cart.queued[0]["stream_id"] == "ep-2"
    assert cart.queued[0]["season"] == "1"
    assert cart.queued[0]["episode_num"] == 1
