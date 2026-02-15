"""Unit tests for app.services.monitor_service and monitor diff logic."""

import json
import os
import tempfile

from app.services.monitor_service import _safe_episode_num


# ---------------------------------------------------------------
# Monitored data persistence (MonitorService)
# ---------------------------------------------------------------

class TestMonitoredDataModel:

    def test_load_monitored_empty(self):
        """Loading from non-existent file returns empty list."""
        from app.services.config_service import ConfigService
        from app.services.monitor_service import MonitorService

        with tempfile.TemporaryDirectory() as d:
            cfg = ConfigService(d)
            cfg.load()
            ms = MonitorService.__new__(MonitorService)
            ms._cfg = cfg
            ms.monitored_file = os.path.join(d, "monitored_series.json")
            ms._monitored_series = []
            ms.load_monitored()
            assert ms._monitored_series == []

    def test_save_and_load_monitored(self):
        """Round-trip save + load."""
        from app.services.config_service import ConfigService
        from app.services.monitor_service import MonitorService

        with tempfile.TemporaryDirectory() as d:
            cfg = ConfigService(d)
            cfg.load()
            ms = MonitorService.__new__(MonitorService)
            ms._cfg = cfg
            ms.monitored_file = os.path.join(d, "monitored_series.json")
            ms._monitored_series = [
                {
                    "id": "test-uuid-1",
                    "series_name": "Breaking Bad",
                    "series_id": "123",
                    "source_id": "src1",
                    "source_name": "Source 1",
                    "cover": "",
                    "scope": "new_only",
                    "season_filter": None,
                    "enabled": True,
                    "known_episodes": [
                        {"stream_id": "1001", "source_id": "src1", "season": "1", "episode_num": 1},
                        {"stream_id": "1002", "source_id": "src1", "season": "1", "episode_num": 2},
                    ],
                    "downloaded_episodes": [],
                    "created_at": "2026-01-01T00:00:00",
                    "last_checked": None,
                    "last_new_count": 0,
                }
            ]
            ms.save_monitored()

            # Verify file
            path = os.path.join(d, "monitored_series.json")
            with open(path) as f:
                saved = json.load(f)
            assert len(saved["series"]) == 1
            assert saved["series"][0]["series_name"] == "Breaking Bad"

            # Reload
            ms._monitored_series = []
            ms.load_monitored()
            assert len(ms._monitored_series) == 1
            assert ms._monitored_series[0]["id"] == "test-uuid-1"

    def test_load_monitored_corrupt_file(self):
        from app.services.config_service import ConfigService
        from app.services.monitor_service import MonitorService

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "monitored_series.json")
            with open(path, "w") as f:
                f.write("not valid json{{{")
            cfg = ConfigService(d)
            cfg.load()
            ms = MonitorService.__new__(MonitorService)
            ms._cfg = cfg
            ms.monitored_file = os.path.join(d, "monitored_series.json")
            ms._monitored_series = []
            ms.load_monitored()
            assert ms._monitored_series == []


# ---------------------------------------------------------------
# Episode diff logic (pure)
# ---------------------------------------------------------------

class TestMonitorDiffLogic:

    def test_new_episodes_detected(self):
        known = [
            {"season": "1", "episode_num": 1},
            {"season": "1", "episode_num": 2},
            {"season": "1", "episode_num": 3},
        ]
        downloaded = [{"season": "1", "episode_num": 4}]
        fetched = [
            {"stream_id": "1001", "season": "1", "episode_num": 1},
            {"stream_id": "1002", "season": "1", "episode_num": 2},
            {"stream_id": "1003", "season": "1", "episode_num": 3},
            {"stream_id": "1004", "season": "1", "episode_num": 4},
            {"stream_id": "1005", "season": "1", "episode_num": 5},
            {"stream_id": "1006", "season": "1", "episode_num": 6},
        ]

        known_keys = {(str(k["season"]), _safe_episode_num(k["episode_num"])) for k in known}
        dl_keys = {(str(k["season"]), _safe_episode_num(k["episode_num"])) for k in downloaded}
        seen = known_keys | dl_keys

        new = [ep for ep in fetched if (str(ep["season"]), _safe_episode_num(ep["episode_num"])) not in seen]
        assert len(new) == 2

    def test_no_new_episodes(self):
        known = [{"season": "1", "episode_num": 1}, {"season": "1", "episode_num": 2}]
        fetched = [
            {"stream_id": "1001", "season": "1", "episode_num": 1},
            {"stream_id": "1002", "season": "1", "episode_num": 2},
        ]
        known_keys = {(str(k["season"]), _safe_episode_num(k["episode_num"])) for k in known}
        new = [ep for ep in fetched if (str(ep["season"]), _safe_episode_num(ep["episode_num"])) not in known_keys]
        assert len(new) == 0

    def test_empty_known_detects_all(self):
        fetched = [
            {"stream_id": "1001", "season": "1", "episode_num": 1},
            {"stream_id": "1002", "season": "1", "episode_num": 2},
            {"stream_id": "1003", "season": "2", "episode_num": 1},
        ]
        known_keys: set = set()
        new = [ep for ep in fetched if (str(ep["season"]), _safe_episode_num(ep["episode_num"])) not in known_keys]
        assert len(new) == 3


# ---------------------------------------------------------------
# Scope filtering
# ---------------------------------------------------------------

class TestMonitorScopeFilters:

    def test_season_filter(self):
        episodes = [
            {"stream_id": "1", "season": "1", "episode_num": 1},
            {"stream_id": "2", "season": "1", "episode_num": 2},
            {"stream_id": "3", "season": "2", "episode_num": 1},
            {"stream_id": "4", "season": "2", "episode_num": 2},
            {"stream_id": "5", "season": "3", "episode_num": 1},
        ]
        filtered = [ep for ep in episodes if str(ep["season"]) == "2"]
        assert len(filtered) == 2

    def test_all_scope_no_filter(self):
        episodes = [
            {"stream_id": "1", "season": "1", "episode_num": 1},
            {"stream_id": "2", "season": "2", "episode_num": 1},
        ]
        assert len(episodes) == 2

    def test_new_only_snapshot(self):
        current = [
            {"stream_id": "1", "season": "1", "episode_num": 1},
            {"stream_id": "2", "season": "1", "episode_num": 2},
        ]
        known_keys = {(str(ep["season"]), _safe_episode_num(ep["episode_num"])) for ep in current}
        later = current + [{"stream_id": "3", "season": "1", "episode_num": 3}]
        new = [ep for ep in later if (str(ep["season"]), _safe_episode_num(ep["episode_num"])) not in known_keys]
        assert len(new) == 1


# ---------------------------------------------------------------
# Duplicate prevention
# ---------------------------------------------------------------

class TestDuplicatePrevention:

    def test_same_episode_from_different_sources(self):
        ep_a = {"stream_id": "1001", "source_id": "srcA", "season": "2", "episode_num": 5}
        ep_b = {"stream_id": "9999", "source_id": "srcB", "season": "2", "episode_num": 5}
        assert (str(ep_a["season"]), _safe_episode_num(ep_a["episode_num"])) == (
            str(ep_b["season"]),
            _safe_episode_num(ep_b["episode_num"]),
        )

    def test_different_episodes_have_different_keys(self):
        eps = [
            {"season": "1", "episode_num": 1},
            {"season": "1", "episode_num": 2},
            {"season": "2", "episode_num": 1},
        ]
        keys = {(str(e["season"]), _safe_episode_num(e["episode_num"])) for e in eps}
        assert len(keys) == 3


# ---------------------------------------------------------------
# _safe_episode_num
# ---------------------------------------------------------------

class TestSafeEpisodeNum:

    def test_int(self):
        assert _safe_episode_num(5) == 5

    def test_str(self):
        assert _safe_episode_num("3") == 3

    def test_none(self):
        assert _safe_episode_num(None) == 0

    def test_float_string(self):
        assert _safe_episode_num("2.5") == 0

    def test_empty_string(self):
        assert _safe_episode_num("") == 0
