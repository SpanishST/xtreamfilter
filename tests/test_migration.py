"""Tests for the JSON → SQLite one-shot migration."""

import json
import os
import tempfile

import pytest

from app.database import DB_NAME, db_connect, init_db
from app.migrate import run_migration_if_needed


def _write_json(path: str, obj) -> None:
    with open(path, "w") as f:
        json.dump(obj, f)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_db(d: str) -> str:
    db_path = os.path.join(d, DB_NAME)
    init_db(db_path)
    return db_path


# ---------------------------------------------------------------------------
# categories.json migration
# ---------------------------------------------------------------------------

class TestMigrateCategories:

    def test_categories_imported(self):
        """Categories (manual + automatic) are imported into custom_categories."""
        with tempfile.TemporaryDirectory() as d:
            db_path = _setup_db(d)
            _write_json(os.path.join(d, "categories.json"), {
                "categories": [
                    {
                        "id": "favorites",
                        "name": "Favorites",
                        "icon": "❤️",
                        "mode": "manual",
                        "content_types": ["live", "vod", "series"],
                        "items": [
                            {"id": "111", "source_id": "src1", "content_type": "live",
                             "added_at": "2026-01-01T00:00:00"},
                        ],
                        "patterns": [],
                        "pattern_logic": "or",
                        "use_source_filters": False,
                        "notify_telegram": False,
                        "recently_added_days": 0,
                        "cached_items": [],
                        "last_refresh": None,
                    },
                    {
                        "id": "action",
                        "name": "Action",
                        "icon": "🎬",
                        "mode": "automatic",
                        "content_types": ["vod"],
                        "items": [],
                        "patterns": [{"match": "contains", "value": "action", "case_sensitive": False}],
                        "pattern_logic": "or",
                        "use_source_filters": False,
                        "notify_telegram": False,
                        "recently_added_days": 0,
                        "cached_items": [],
                        "last_refresh": None,
                    },
                ]
            })

            run_migration_if_needed(db_path, d)

            conn = db_connect(db_path)
            try:
                cats = conn.execute("SELECT id, name, mode FROM custom_categories ORDER BY sort_order").fetchall()
                assert len(cats) == 2
                assert cats[0]["id"] == "favorites"
                assert cats[1]["id"] == "action"
                assert cats[1]["mode"] == "automatic"

                # Pattern imported
                pats = conn.execute(
                    "SELECT match_type, value FROM category_patterns WHERE category_id='action'"
                ).fetchall()
                assert len(pats) == 1
                assert pats[0]["value"] == "action"

                # Manual item imported
                items = conn.execute(
                    "SELECT stream_id FROM category_manual_items WHERE category_id='favorites'"
                ).fetchall()
                assert len(items) == 1
                assert items[0]["stream_id"] == "111"
            finally:
                conn.close()

    def test_categories_bak_created(self):
        """categories.json is renamed to .bak after migration."""
        with tempfile.TemporaryDirectory() as d:
            db_path = _setup_db(d)
            src = os.path.join(d, "categories.json")
            _write_json(src, {"categories": []})
            run_migration_if_needed(db_path, d)
            assert not os.path.exists(src)
            assert os.path.exists(src + ".bak")

    def test_migration_idempotent(self):
        """Running migration twice does not duplicate rows."""
        with tempfile.TemporaryDirectory() as d:
            db_path = _setup_db(d)
            _write_json(os.path.join(d, "categories.json"), {
                "categories": [
                    {"id": "fav", "name": "Fav", "icon": "❤️", "mode": "manual",
                     "content_types": ["live"], "items": [], "patterns": [],
                     "pattern_logic": "or", "use_source_filters": False,
                     "notify_telegram": False, "recently_added_days": 0,
                     "cached_items": [], "last_refresh": None},
                ]
            })
            run_migration_if_needed(db_path, d)
            # categories.json is now .bak — second call should be a no-op
            run_migration_if_needed(db_path, d)
            conn = db_connect(db_path)
            try:
                count = conn.execute("SELECT COUNT(*) FROM custom_categories").fetchone()[0]
                assert count == 1
            finally:
                conn.close()


# ---------------------------------------------------------------------------
# cart.json migration
# ---------------------------------------------------------------------------

class TestMigrateCart:

    def test_cart_items_imported(self):
        """Cart items are imported into cart_items table."""
        with tempfile.TemporaryDirectory() as d:
            db_path = _setup_db(d)
            _write_json(os.path.join(d, "cart.json"), {
                "items": [
                    {
                        "stream_id": "999",
                        "source_id": "src1",
                        "content_type": "vod",
                        "name": "Big Buck Bunny",
                        "status": "pending",
                        "added_at": "2026-01-01T12:00:00",
                    }
                ]
            })
            run_migration_if_needed(db_path, d)
            conn = db_connect(db_path)
            try:
                rows = conn.execute("SELECT * FROM cart_items").fetchall()
                assert len(rows) == 1
                assert rows[0]["stream_id"] == "999"
                assert rows[0]["name"] == "Big Buck Bunny"
            finally:
                conn.close()

    def test_cart_bak_created(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = _setup_db(d)
            src = os.path.join(d, "cart.json")
            _write_json(src, {"items": []})
            run_migration_if_needed(db_path, d)
            assert not os.path.exists(src)
            assert os.path.exists(src + ".bak")


# ---------------------------------------------------------------------------
# monitored_series.json migration
# ---------------------------------------------------------------------------

class TestMigrateMonitoredSeries:

    def test_series_and_episodes_imported(self):
        """Monitored series + known_episodes are imported."""
        with tempfile.TemporaryDirectory() as d:
            db_path = _setup_db(d)
            _write_json(os.path.join(d, "monitored_series.json"), {
                "series": [
                    {
                        "id": "uuid-1",
                        "series_name": "Severance",
                        "series_id": "77",
                        "source_id": "src1",
                        "source_name": "S1",
                        "cover": "",
                        "scope": "new_only",
                        "season_filter": None,
                        "enabled": True,
                        "known_episodes": [
                            {"stream_id": "501", "source_id": "src1", "season": "1", "episode_num": 1},
                        ],
                        "downloaded_episodes": [
                            {"stream_id": "501", "source_id": "src1", "season": "1", "episode_num": 1},
                        ],
                        "created_at": "2026-01-01T00:00:00",
                        "last_checked": None,
                        "last_new_count": 0,
                    }
                ]
            })
            run_migration_if_needed(db_path, d)
            conn = db_connect(db_path)
            try:
                series = conn.execute("SELECT * FROM monitored_series").fetchall()
                assert len(series) == 1
                assert series[0]["series_name"] == "Severance"

                eps = conn.execute("SELECT * FROM known_episodes").fetchall()
                assert len(eps) == 1
                assert eps[0]["stream_id"] == "501"

                dl_eps = conn.execute("SELECT * FROM downloaded_episodes").fetchall()
                assert len(dl_eps) == 1
            finally:
                conn.close()


# ---------------------------------------------------------------------------
# EPG meta migration
# ---------------------------------------------------------------------------

class TestMigrateEpgMeta:

    def test_epg_meta_imported(self):
        """EPG cache meta (last_refresh) is imported into epg_meta table."""
        with tempfile.TemporaryDirectory() as d:
            db_path = _setup_db(d)
            _write_json(os.path.join(d, "epg_cache.xml.meta"), {
                "last_refresh": "2026-01-15T10:30:00"
            })
            run_migration_if_needed(db_path, d)
            conn = db_connect(db_path)
            try:
                row = conn.execute("SELECT last_refresh FROM epg_meta WHERE id=1").fetchone()
                assert row is not None
                assert row["last_refresh"] == "2026-01-15T10:30:00"
            finally:
                conn.close()


# ---------------------------------------------------------------------------
# Sentinel check
# ---------------------------------------------------------------------------

class TestMigrationSentinel:

    def test_sentinel_prevents_rerun(self):
        """Second call with no JSON files and sentinel set is a clean no-op."""
        with tempfile.TemporaryDirectory() as d:
            db_path = _setup_db(d)
            # Run with no JSON files — migration marks done after finding nothing
            run_migration_if_needed(db_path, d)
            # Should not raise and should not create any unexpected rows
            run_migration_if_needed(db_path, d)
            conn = db_connect(db_path)
            try:
                # migration_meta table should exist and have done=1
                row = conn.execute(
                    "SELECT done FROM migration_meta WHERE id=1"
                ).fetchone()
                assert row is not None
                assert row["done"] == 1
            finally:
                conn.close()
