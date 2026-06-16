"""Unit tests for app.services.log_service."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

from app.database import DB_NAME, db_connect, init_db
from app.services.config_service import ConfigService
from app.services.log_service import LogService


def _run(coro):
    return asyncio.run(coro)


def _make_service(d: str, retention_days: int = 30) -> LogService:
    config = {
        "sources": [],
        "filters": {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
        "content_types": {"live": True, "vod": True, "series": True},
        "options": {"log_retention_days": retention_days},
    }
    (d + "/config.json").write(json.dumps(config)) if hasattr((d + "/config.json"), "write") else None
    config_path = os.path.join(d, "config.json")
    with open(config_path, "w") as f:
        json.dump(config, f)
    db_path = os.path.join(d, DB_NAME)
    init_db(db_path)
    cfg = ConfigService(d)
    cfg.load()
    return LogService(db_path, cfg)


def _count_rows(db_path: str, table: str = "activity_logs") -> int:
    conn = db_connect(db_path)
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return row[0]
    finally:
        conn.close()


# -------------------------------------------------------------------
# log() — write
# -------------------------------------------------------------------

class TestLogWrite:

    def test_log_inserts_entry(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            _run(svc.log("cache", "info", "Cache refreshed"))
            assert _count_rows(svc.db_path) == 1

    def test_log_with_dict_details(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            _run(svc.log("cart", "error", "Download failed", {"file": "movie.mkv", "code": 500}))
            assert _count_rows(svc.db_path) == 1
            conn = db_connect(svc.db_path)
            try:
                row = conn.execute("SELECT details FROM activity_logs").fetchone()
                details = json.loads(row[0])
                assert details["file"] == "movie.mkv"
                assert details["code"] == 500
            finally:
                conn.close()

    def test_log_with_list_details(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            _run(svc.log("monitor", "info", "New episodes", ["S01E01", "S01E02"]))
            conn = db_connect(svc.db_path)
            try:
                row = conn.execute("SELECT details FROM activity_logs").fetchone()
                details = json.loads(row[0])
                assert details == ["S01E01", "S01E02"]
            finally:
                conn.close()

    def test_log_with_string_details(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            _run(svc.log("cache", "warning", "Slow refresh", "took 45s"))
            conn = db_connect(svc.db_path)
            try:
                row = conn.execute("SELECT details FROM activity_logs").fetchone()
                assert row[0] == "took 45s"
            finally:
                conn.close()

    def test_log_with_no_details(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            _run(svc.log("cache", "info", "Simple message"))
            conn = db_connect(svc.db_path)
            try:
                row = conn.execute("SELECT details FROM activity_logs").fetchone()
                assert row[0] is None
            finally:
                conn.close()

    def test_log_multiple_entries(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            _run(svc.log("cache", "info", "Event 1"))
            _run(svc.log("cart", "info", "Event 2"))
            _run(svc.log("monitor", "error", "Event 3"))
            assert _count_rows(svc.db_path) == 3

    def test_log_sets_timestamp(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            before = datetime.now(timezone.utc).isoformat()
            _run(svc.log("cache", "info", "Test"))
            after = datetime.now(timezone.utc).isoformat()
            conn = db_connect(svc.db_path)
            try:
                row = conn.execute("SELECT timestamp FROM activity_logs").fetchone()
                assert before <= row[0] <= after
            finally:
                conn.close()


# -------------------------------------------------------------------
# get_logs() — read
# -------------------------------------------------------------------

class TestLogRead:

    def _seed(self, d: str) -> LogService:
        svc = _make_service(d)
        _run(svc.log("cache", "info", "Cache start"))
        _run(svc.log("cache", "error", "Cache fail"))
        _run(svc.log("cart", "info", "Download done"))
        _run(svc.log("cart", "warning", "Download slow"))
        _run(svc.log("monitor", "info", "New episode"))
        _run(svc.log("monitor", "error", "Check failed"))
        return svc

    def test_get_logs_all(self):
        with tempfile.TemporaryDirectory() as d:
            svc = self._seed(d)
            logs = _run(svc.get_logs())
            assert len(logs) == 6

    def test_get_logs_by_category(self):
        with tempfile.TemporaryDirectory() as d:
            svc = self._seed(d)
            cache_logs = _run(svc.get_logs(category="cache"))
            assert len(cache_logs) == 2
            assert all(e["category"] == "cache" for e in cache_logs)

            cart_logs = _run(svc.get_logs(category="cart"))
            assert len(cart_logs) == 2

            monitor_logs = _run(svc.get_logs(category="monitor"))
            assert len(monitor_logs) == 2

    def test_get_logs_by_level(self):
        with tempfile.TemporaryDirectory() as d:
            svc = self._seed(d)
            errors = _run(svc.get_logs(level="error"))
            assert len(errors) == 2
            assert all(e["level"] == "error" for e in errors)

            warnings = _run(svc.get_logs(level="warning"))
            assert len(warnings) == 1

    def test_get_logs_by_category_and_level(self):
        with tempfile.TemporaryDirectory() as d:
            svc = self._seed(d)
            logs = _run(svc.get_logs(category="cache", level="error"))
            assert len(logs) == 1
            assert logs[0]["message"] == "Cache fail"

    def test_get_logs_limit(self):
        with tempfile.TemporaryDirectory() as d:
            svc = self._seed(d)
            logs = _run(svc.get_logs(limit=3))
            assert len(logs) == 3

    def test_get_logs_offset(self):
        with tempfile.TemporaryDirectory() as d:
            svc = self._seed(d)
            page1 = _run(svc.get_logs(limit=3, offset=0))
            page2 = _run(svc.get_logs(limit=3, offset=3))
            assert len(page1) == 3
            assert len(page2) == 3
            # No overlap — newest first by id DESC
            page1_ids = {e["id"] for e in page1}
            page2_ids = {e["id"] for e in page2}
            assert page1_ids.isdisjoint(page2_ids)

    def test_get_logs_order_newest_first(self):
        with tempfile.TemporaryDirectory() as d:
            svc = self._seed(d)
            logs = _run(svc.get_logs())
            ids = [e["id"] for e in logs]
            assert ids == sorted(ids, reverse=True)

    def test_get_logs_returns_dict_keys(self):
        with tempfile.TemporaryDirectory() as d:
            svc = self._seed(d)
            logs = _run(svc.get_logs(limit=1))
            entry = logs[0]
            assert "id" in entry
            assert "timestamp" in entry
            assert "category" in entry
            assert "level" in entry
            assert "message" in entry
            assert "details" in entry

    def test_get_logs_empty(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            logs = _run(svc.get_logs())
            assert logs == []


# -------------------------------------------------------------------
# get_log_counts()
# -------------------------------------------------------------------

class TestLogCounts:

    def test_counts_empty(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            counts = _run(svc.get_log_counts())
            assert counts == {"cache": 0, "cart": 0, "monitor": 0}

    def test_counts_populated(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            _run(svc.log("cache", "info", "a"))
            _run(svc.log("cache", "info", "b"))
            _run(svc.log("cart", "info", "c"))
            _run(svc.log("monitor", "error", "d"))
            _run(svc.log("monitor", "info", "e"))
            _run(svc.log("monitor", "warning", "f"))
            counts = _run(svc.get_log_counts())
            assert counts == {"cache": 2, "cart": 1, "monitor": 3}

    def test_counts_after_clear(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            _run(svc.log("cache", "info", "a"))
            _run(svc.log("cart", "info", "b"))
            _run(svc.clear_logs("cache"))
            counts = _run(svc.get_log_counts())
            assert counts == {"cache": 0, "cart": 1, "monitor": 0}


# -------------------------------------------------------------------
# clear_logs()
# -------------------------------------------------------------------

class TestLogClear:

    def test_clear_all(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            _run(svc.log("cache", "info", "a"))
            _run(svc.log("cart", "info", "b"))
            _run(svc.log("monitor", "info", "c"))
            deleted = _run(svc.clear_logs())
            assert deleted == 3
            assert _count_rows(svc.db_path) == 0

    def test_clear_by_category(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            _run(svc.log("cache", "info", "a"))
            _run(svc.log("cart", "info", "b"))
            _run(svc.log("monitor", "info", "c"))
            deleted = _run(svc.clear_logs("cache"))
            assert deleted == 1
            assert _count_rows(svc.db_path) == 2
            remaining = _run(svc.get_logs())
            assert all(e["category"] != "cache" for e in remaining)

    def test_clear_empty_table(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            deleted = _run(svc.clear_logs())
            assert deleted == 0


# -------------------------------------------------------------------
# prune_old_logs()
# -------------------------------------------------------------------

class TestLogPrune:

    def test_prune_removes_old_entries(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d, retention_days=7)
            # Insert an old entry directly with a backdated timestamp
            old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
            conn = db_connect(svc.db_path)
            conn.execute(
                "INSERT INTO activity_logs (timestamp, category, level, message) VALUES (?, ?, ?, ?)",
                (old_ts, "cache", "info", "Old entry"),
            )
            conn.commit()
            conn.close()

            # Insert a recent entry via the service
            _run(svc.log("cache", "info", "Recent entry"))

            assert _count_rows(svc.db_path) == 2
            pruned = _run(svc.prune_old_logs())
            assert pruned == 1
            assert _count_rows(svc.db_path) == 1

            # Verify the recent one survived
            logs = _run(svc.get_logs())
            assert logs[0]["message"] == "Recent entry"

    def test_prune_keeps_recent_entries(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d, retention_days=30)
            _run(svc.log("cache", "info", "Recent"))
            pruned = _run(svc.prune_old_logs())
            assert pruned == 0
            assert _count_rows(svc.db_path) == 1

    def test_prune_respects_retention_config(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d, retention_days=3)
            # Insert entry from 5 days ago
            old_ts = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
            conn = db_connect(svc.db_path)
            conn.execute(
                "INSERT INTO activity_logs (timestamp, category, level, message) VALUES (?, ?, ?, ?)",
                (old_ts, "cart", "info", "Too old"),
            )
            # Insert entry from 2 days ago
            recent_ts = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
            conn.execute(
                "INSERT INTO activity_logs (timestamp, category, level, message) VALUES (?, ?, ?, ?)",
                (recent_ts, "cart", "info", "Within retention"),
            )
            conn.commit()
            conn.close()

            pruned = _run(svc.prune_old_logs())
            assert pruned == 1
            logs = _run(svc.get_logs())
            assert len(logs) == 1
            assert logs[0]["message"] == "Within retention"

    def test_prune_empty_table(self):
        with tempfile.TemporaryDirectory() as d:
            svc = _make_service(d)
            pruned = _run(svc.prune_old_logs())
            assert pruned == 0
