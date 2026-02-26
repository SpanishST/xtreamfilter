"""Unit tests for app.services.monitor_service and monitor diff logic."""

import asyncio
import os
import tempfile

from app.services.monitor_service import _safe_episode_num
from app.database import DB_NAME, db_connect, init_db


def _run(coro):
    """Run a coroutine synchronously (helper for sync test methods)."""
    return asyncio.run(coro)


# ---------------------------------------------------------------
# Monitored data persistence (MonitorService)
# ---------------------------------------------------------------

class TestMonitoredDataModel:

    def _make_ms(self, d):
        """Build a minimal MonitorService pointing at temp dir's SQLite DB."""
        from app.services.config_service import ConfigService
        from app.services.monitor_service import MonitorService

        cfg = ConfigService(d)
        cfg.load()
        db_path = os.path.join(d, DB_NAME)
        init_db(db_path)
        ms = MonitorService.__new__(MonitorService)
        ms._cfg = cfg
        ms.db_path = db_path
        ms._monitored_series = []
        return ms

    def test_load_monitored_empty(self):
        """Loading from empty DB returns empty list."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
            ms.load_monitored()
            assert ms._monitored_series == []

    def test_save_and_load_monitored(self):
        """Round-trip save + load via SQLite."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
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

            # Verify row exists in DB
            conn = db_connect(ms.db_path)
            try:
                rows = conn.execute("SELECT * FROM monitored_series").fetchall()
                assert len(rows) == 1
                assert rows[0]["series_name"] == "Breaking Bad"
                ep_rows = conn.execute("SELECT * FROM known_episodes").fetchall()
                assert len(ep_rows) == 2
            finally:
                conn.close()

            # Reload
            ms._monitored_series = []
            ms.load_monitored()
            assert len(ms._monitored_series) == 1
            assert ms._monitored_series[0]["id"] == "test-uuid-1"

    def test_load_monitored_multiple_series(self):
        """Multiple monitored series round-trip correctly."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
            ms._monitored_series = [
                {
                    "id": "uuid-a", "series_name": "Show A", "series_id": "1",
                    "source_id": "src1", "source_name": "S1", "cover": "",
                    "scope": "new_only", "season_filter": None, "enabled": True,
                    "known_episodes": [], "downloaded_episodes": [],
                    "created_at": "2026-01-01T00:00:00", "last_checked": None, "last_new_count": 0,
                },
                {
                    "id": "uuid-b", "series_name": "Show B", "series_id": "2",
                    "source_id": "src2", "source_name": "S2", "cover": "",
                    "scope": "all", "season_filter": "1", "enabled": False,
                    "known_episodes": [], "downloaded_episodes": [],
                    "created_at": "2026-01-02T00:00:00", "last_checked": None, "last_new_count": 0,
                },
            ]
            ms.save_monitored()
            ms._monitored_series = []
            ms.load_monitored()
            assert len(ms._monitored_series) == 2
            names = {s["series_name"] for s in ms._monitored_series}
            assert names == {"Show A", "Show B"}


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


# ---------------------------------------------------------------
# ID-first cross-source matching
# ---------------------------------------------------------------

class TestMonitorIdFirstMatching:

    def _build_monitor_with_series(self, series_items: list[dict]):
        from app.services.monitor_service import MonitorService

        class _Cfg:
            config = {"sources": [{"id": "src1"}, {"id": "src2"}]}

        class _Cache:
            def get_cached_with_source_info(self, *_args, **_kwargs):
                return series_items, []

        ms = MonitorService.__new__(MonitorService)
        ms._cfg = _Cfg()
        ms.cache_service = _Cache()
        return ms

    def test_id_match_prioritized_over_fuzzy(self):
        ms = self._build_monitor_with_series([
            {
                "_source_id": "src1",
                "series_id": "100",
                "name": "NF - The Last Of Us",
                "tmdb": "100088",
            },
            {
                "_source_id": "src2",
                "series_id": "200",
                "name": "NF - The Last Of Us (FR)",
                "tmdb": "999999",
            },
        ])

        matches = ms.find_series_across_sources("The Last Of Us", tmdb_id="100088")
        assert matches
        assert matches[0]["source_id"] == "src1"
        assert matches[0]["matched_by"] == "id"

    def test_fallback_to_fuzzy_when_no_ids(self):
        ms = self._build_monitor_with_series([
            {
                "_source_id": "src1",
                "series_id": "100",
                "name": "FR - Breaking Bad",
            },
            {
                "_source_id": "src2",
                "series_id": "200",
                "name": "Breaking Bad",
            },
        ])

        matches = ms.find_series_across_sources("Breaking Bad")
        assert matches
        assert any(m["matched_by"] == "fuzzy" for m in matches)


# ---------------------------------------------------------------
# Multi-source monitor_sources persistence
# ---------------------------------------------------------------

class TestMonitorSourcesPersistence:
    """Verify that monitor_sources rows are written to / read from the DB."""

    def _make_ms(self, d):
        from app.services.config_service import ConfigService
        from app.services.monitor_service import MonitorService

        cfg = ConfigService(d)
        cfg.load()
        db_path = os.path.join(d, DB_NAME)
        init_db(db_path)
        ms = MonitorService.__new__(MonitorService)
        ms._cfg = cfg
        ms.db_path = db_path
        ms._monitored_series = []
        return ms

    def test_single_source_slot_roundtrip(self):
        """A single monitor_sources slot is persisted and reloaded."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
            ms._monitored_series = [{
                "id": "uuid-ms-1",
                "series_name": "Severance",
                "series_id": "999",
                "source_id": None,
                "source_name": None,
                "source_category": None,
                "monitor_sources": [
                    {"source_id": "srcA", "series_ref": "101", "source_name": "Provider A", "category": "EN Drama"},
                ],
                "cover": "",
                "scope": "new_only",
                "season_filter": None,
                "action": "both",
                "enabled": True,
                "known_episodes": [],
                "downloaded_episodes": [],
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }]
            ms.save_monitored()

            ms._monitored_series = []
            ms.load_monitored()

            assert len(ms._monitored_series) == 1
            srcs = ms._monitored_series[0]["monitor_sources"]
            assert len(srcs) == 1
            assert srcs[0]["source_id"] == "srcA"
            assert srcs[0]["series_ref"] == "101"
            assert srcs[0]["source_name"] == "Provider A"
            assert srcs[0]["category"] == "EN Drama"

    def test_multi_source_slots_roundtrip(self):
        """Two monitor_sources slots survive a save/load cycle."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
            ms._monitored_series = [{
                "id": "uuid-ms-2",
                "series_name": "Shogun",
                "series_id": "55",
                "source_id": None,
                "source_name": None,
                "source_category": None,
                "monitor_sources": [
                    {"source_id": "srcA", "series_ref": "201", "source_name": "Provider A", "category": "FR Séries"},
                    {"source_id": "srcB", "series_ref": "301", "source_name": "Provider B", "category": "EN Series"},
                ],
                "cover": "",
                "scope": "new_only",
                "season_filter": None,
                "action": "notify",
                "enabled": True,
                "known_episodes": [],
                "downloaded_episodes": [],
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }]
            ms.save_monitored()

            ms._monitored_series = []
            ms.load_monitored()

            srcs = ms._monitored_series[0]["monitor_sources"]
            assert len(srcs) == 2
            source_ids = {s["source_id"] for s in srcs}
            assert source_ids == {"srcA", "srcB"}

    def test_empty_monitor_sources_roundtrip(self):
        """Entries with no monitor_sources (legacy) still load cleanly."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
            ms._monitored_series = [{
                "id": "uuid-legacy",
                "series_name": "Old Show",
                "series_id": "77",
                "source_id": "srcLegacy",
                "source_name": "Legacy Provider",
                "source_category": "General",
                "monitor_sources": [],
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
            }]
            ms.save_monitored()

            ms._monitored_series = []
            ms.load_monitored()

            entry = ms._monitored_series[0]
            assert entry["source_id"] == "srcLegacy"
            assert entry["monitor_sources"] == []

    def test_deduplication_on_save(self):
        """Duplicate (source_id, series_ref) pairs are not stored twice."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
            ms._monitored_series = [{
                "id": "uuid-dup",
                "series_name": "Dupe Show",
                "series_id": "88",
                "source_id": None,
                "source_name": None,
                "source_category": None,
                "monitor_sources": [
                    {"source_id": "srcA", "series_ref": "401", "source_name": "P A", "category": ""},
                    {"source_id": "srcA", "series_ref": "401", "source_name": "P A", "category": ""},
                ],
                "cover": "",
                "scope": "new_only",
                "season_filter": None,
                "action": "notify",
                "enabled": True,
                "known_episodes": [],
                "downloaded_episodes": [],
                "created_at": "2026-01-01T00:00:00",
                "last_checked": None,
                "last_new_count": 0,
            }]
            ms.save_monitored()

            conn = db_connect(ms.db_path)
            try:
                rows = conn.execute(
                    "SELECT * FROM monitor_sources WHERE series_id = 'uuid-dup'"
                ).fetchall()
            finally:
                conn.close()

            assert len(rows) == 1


# ---------------------------------------------------------------
# Source resolution priority in _check_single_monitored
# ---------------------------------------------------------------

class TestMonitorSourceResolutionPriority:
    """Verify which sources are queried depending on how the entry is configured."""

    def _make_ms_with_fetcher(self, fetch_results: dict):
        """
        fetch_results: {(source_id, series_id): [episode_dicts]}
        Calls to fetch_series_episodes for unknown pairs return [].
        """
        from app.services.monitor_service import MonitorService

        class _Cfg:
            config = {"sources": [{"id": "srcA", "enabled": True}, {"id": "srcB", "enabled": True}]}

        class _Xtream:
            async def fetch_series_episodes(self, source_id, series_id):
                return fetch_results.get((source_id, str(series_id)), [])

        class _Cache:
            def get_cached_with_source_info(self, *_args, **_kwargs):
                return [], []

        class _Cart:
            cart = []

            def build_download_filepath(self, item):
                return "/nonexistent/" + item["stream_id"]

            def save_cart(self):
                pass

            def is_in_download_window(self):
                return False

        ms = MonitorService.__new__(MonitorService)
        ms._cfg = _Cfg()
        ms.cache_service = _Cache()
        ms.xtream_service = _Xtream()
        ms.cart_service = _Cart()
        ms._monitored_series = []
        return ms

    def test_monitor_sources_used_when_set(self):
        """When monitor_sources is set, only those sources are queried."""
        episodes_srcA = [
            {"stream_id": "e1", "season": "1", "episode_num": 1, "title": "Pilot"},
        ]
        ms = self._make_ms_with_fetcher({("srcA", "101"): episodes_srcA})
        entry = {
            "id": "u1",
            "series_name": "My Show",
            "series_id": "101",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "101", "source_name": "P A", "category": ""},
            ],
            "scope": "new_only",
            "season_filter": None,
            "action": "notify",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert len(new_eps) == 1
        assert new_eps[0]["stream_id"] == "e1"

    def test_legacy_source_id_used_when_no_monitor_sources(self):
        """Legacy entries with source_id but empty monitor_sources work correctly."""
        episodes = [
            {"stream_id": "e10", "season": "1", "episode_num": 1, "title": "Ep1"},
            {"stream_id": "e11", "season": "1", "episode_num": 2, "title": "Ep2"},
        ]
        ms = self._make_ms_with_fetcher({("srcLegacy", "999"): episodes})
        entry = {
            "id": "u2",
            "series_name": "Old Show",
            "series_id": "999",
            "source_id": "srcLegacy",
            "monitor_sources": [],
            "scope": "new_only",
            "season_filter": None,
            "action": "notify",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert len(new_eps) == 2

    def test_no_episodes_on_all_sources_returns_empty(self):
        """If no source returns episodes, result is empty and last_new_count = 0."""
        ms = self._make_ms_with_fetcher({})   # nothing available
        entry = {
            "id": "u3",
            "series_name": "Ghost Show",
            "series_id": "000",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "000", "source_name": "P A", "category": ""},
            ],
            "scope": "new_only",
            "season_filter": None,
            "action": "notify",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 5,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert new_eps == []
        assert entry["last_new_count"] == 0


# ---------------------------------------------------------------
# Multi-source episode detection & deduplication
# ---------------------------------------------------------------

class TestMultiSourceEpisodeDetection:
    """Check that new episodes are detected across sources and deduplicated."""

    def _make_ms(self, fetch_results: dict):
        from app.services.monitor_service import MonitorService

        class _Cfg:
            config = {"sources": []}

        class _Xtream:
            async def fetch_series_episodes(self, source_id, series_id):
                return fetch_results.get((source_id, str(series_id)), [])

        class _Cart:
            cart = []

            def build_download_filepath(self, item):
                return "/nonexistent/" + item["stream_id"]

            def save_cart(self):
                pass

        ms = MonitorService.__new__(MonitorService)
        ms._cfg = _Cfg()
        ms.xtream_service = _Xtream()
        ms.cart_service = _Cart()
        return ms

    def test_new_ep_detected_from_second_source(self):
        """An episode that only appears on the second source is still detected."""
        ms = self._make_ms({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "Ep1"},
            ],
            ("srcB", "22"): [
                {"stream_id": "b1", "season": "1", "episode_num": 1, "title": "Ep1"},
                {"stream_id": "b2", "season": "1", "episode_num": 2, "title": "Ep2"},   # new
            ],
        })
        entry = {
            "id": "u-ms",
            "series_name": "The Show",
            "series_id": "11",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "11"},
                {"source_id": "srcB", "series_ref": "22"},
            ],
            "scope": "new_only",
            "season_filter": None,
            "action": "notify",
            "known_episodes": [{"season": "1", "episode_num": 1}],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert len(new_eps) == 1
        assert new_eps[0]["episode_num"] == 2

    def test_same_episode_from_two_sources_not_duplicated(self):
        """The same (season, ep_num) appearing in both sources counts only once."""
        ms = self._make_ms({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "Ep1"},
                {"stream_id": "a2", "season": "1", "episode_num": 2, "title": "Ep2"},
            ],
            ("srcB", "22"): [
                {"stream_id": "b1", "season": "1", "episode_num": 1, "title": "Ep1"},
                {"stream_id": "b2", "season": "1", "episode_num": 2, "title": "Ep2"},
            ],
        })
        entry = {
            "id": "u-dup",
            "series_name": "Dup Show",
            "series_id": "11",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "11"},
                {"source_id": "srcB", "series_ref": "22"},
            ],
            "scope": "new_only",
            "season_filter": None,
            "action": "notify",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert len(new_eps) == 2  # only 2 unique episodes, not 4

    def test_known_episode_not_re_detected_across_sources(self):
        """An episode already in known_episodes is not returned as new from any source."""
        ms = self._make_ms({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "2", "episode_num": 3, "title": "Ep S2E3"},
            ],
            ("srcB", "22"): [
                {"stream_id": "b99", "season": "2", "episode_num": 3, "title": "Ep S2E3"},
                {"stream_id": "b100", "season": "2", "episode_num": 4, "title": "Ep S2E4"},
            ],
        })
        entry = {
            "id": "u-known",
            "series_name": "Known Show",
            "series_id": "11",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "11"},
                {"source_id": "srcB", "series_ref": "22"},
            ],
            "scope": "new_only",
            "season_filter": None,
            "action": "notify",
            "known_episodes": [{"season": "2", "episode_num": 3}],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert len(new_eps) == 1
        assert new_eps[0]["episode_num"] == 4

    def test_season_scope_filters_across_sources(self):
        """scope='season' is applied to episodes from every source."""
        ms = self._make_ms({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "S1E1"},
                {"stream_id": "a2", "season": "2", "episode_num": 1, "title": "S2E1"},
            ],
            ("srcB", "22"): [
                {"stream_id": "b1", "season": "1", "episode_num": 1, "title": "S1E1"},
                {"stream_id": "b2", "season": "2", "episode_num": 1, "title": "S2E1"},
                {"stream_id": "b3", "season": "2", "episode_num": 2, "title": "S2E2"},
            ],
        })
        entry = {
            "id": "u-season",
            "series_name": "Season Show",
            "series_id": "11",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "11"},
                {"source_id": "srcB", "series_ref": "22"},
            ],
            "scope": "season",
            "season_filter": "2",
            "action": "notify",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        # Season 1 episodes are excluded; S2E1 from both sources deduped → 2 unique S2 episodes
        assert all(str(ep["season"]) == "2" for ep in new_eps)
        assert len(new_eps) == 2

    def test_three_sources_all_contribute(self):
        """Three sources each have a unique episode — all three are detected."""
        ms = self._make_ms({
            ("srcA", "1"): [{"stream_id": "a1", "season": "1", "episode_num": 1, "title": "E1"}],
            ("srcB", "2"): [{"stream_id": "b1", "season": "1", "episode_num": 2, "title": "E2"}],
            ("srcC", "3"): [{"stream_id": "c1", "season": "1", "episode_num": 3, "title": "E3"}],
        })
        entry = {
            "id": "u-three",
            "series_name": "Triple Show",
            "series_id": "1",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "1"},
                {"source_id": "srcB", "series_ref": "2"},
                {"source_id": "srcC", "series_ref": "3"},
            ],
            "scope": "new_only",
            "season_filter": None,
            "action": "notify",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert len(new_eps) == 3
        ep_nums = {ep["episode_num"] for ep in new_eps}
        assert ep_nums == {1, 2, 3}

    def test_last_new_count_updated(self):
        """last_new_count on the entry reflects total unique new episodes found."""
        ms = self._make_ms({
            ("srcA", "10"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "E1"},
                {"stream_id": "a2", "season": "1", "episode_num": 2, "title": "E2"},
            ],
            ("srcB", "20"): [
                {"stream_id": "b1", "season": "1", "episode_num": 2, "title": "E2"},  # dup
                {"stream_id": "b2", "season": "1", "episode_num": 3, "title": "E3"},
            ],
        })
        entry = {
            "id": "u-count",
            "series_name": "Count Show",
            "series_id": "10",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "10"},
                {"source_id": "srcB", "series_ref": "20"},
            ],
            "scope": "new_only",
            "season_filter": None,
            "action": "notify",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        _run(ms._check_single_monitored(entry))
        assert entry["last_new_count"] == 3  # E1, E2, E3 — unique


# ---------------------------------------------------------------
# Backfill with multi-source entries
# ---------------------------------------------------------------

class TestMultiSourceBackfill:
    """Verify backfill collects episodes from all monitor_sources."""

    def _make_ms(self, fetch_results: dict, existing_paths=None):
        from app.services.monitor_service import MonitorService

        existing_paths = existing_paths or set()

        class _Cfg:
            config = {"sources": []}

        class _Xtream:
            async def fetch_series_episodes(self, source_id, series_id):
                return fetch_results.get((source_id, str(series_id)), [])

        class _Cart:
            cart = []

            def build_download_filepath(self, item):
                return "/dl/" + item["stream_id"] if item["stream_id"] in existing_paths else "/nonexistent/" + item["stream_id"]

            def save_cart(self):
                pass

            def is_in_download_window(self):
                return False

            def _try_start_worker(self):
                return False

        ms = MonitorService.__new__(MonitorService)
        ms._cfg = _Cfg()
        ms.xtream_service = _Xtream()
        ms.cart_service = _Cart()
        return ms

    def test_backfill_collects_from_both_sources(self):
        """backfill='all' queues unique episodes across two sources."""
        ms = self._make_ms({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "E1"},
                {"stream_id": "a2", "season": "1", "episode_num": 2, "title": "E2"},
            ],
            ("srcB", "22"): [
                {"stream_id": "b2", "season": "1", "episode_num": 2, "title": "E2"},  # dup
                {"stream_id": "b3", "season": "1", "episode_num": 3, "title": "E3"},
            ],
        })
        entry = {
            "id": "bf-1",
            "series_name": "Backfill Show",
            "series_id": "11",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "11"},
                {"source_id": "srcB", "series_ref": "22"},
            ],
            "scope": "all",
            "action": "both",
            "known_episodes": [],
            "downloaded_episodes": [],
            "cover": "",
        }
        count = _run(ms.backfill_episodes(entry, backfill="all"))
        assert count == 3  # E1, E2, E3 unique
        queued_nums = {i["episode_num"] for i in ms.cart_service.cart}
        assert queued_nums == {1, 2, 3}

    def test_backfill_season_filter_across_sources(self):
        """backfill='season' only collects episodes from the specified season."""
        ms = self._make_ms({
            ("srcA", "11"): [
                {"stream_id": "a1", "season": "1", "episode_num": 1, "title": "S1"},
                {"stream_id": "a2", "season": "2", "episode_num": 1, "title": "S2"},
            ],
            ("srcB", "22"): [
                {"stream_id": "b2", "season": "2", "episode_num": 1, "title": "S2"},  # dup
                {"stream_id": "b3", "season": "2", "episode_num": 2, "title": "S2E2"},
            ],
        })
        entry = {
            "id": "bf-2",
            "series_name": "Season Backfill",
            "series_id": "11",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "11"},
                {"source_id": "srcB", "series_ref": "22"},
            ],
            "scope": "season",
            "action": "both",
            "known_episodes": [],
            "downloaded_episodes": [],
            "cover": "",
        }
        count = _run(ms.backfill_episodes(entry, backfill="season", backfill_season="2"))
        assert count == 2  # S2E1 (deduped) + S2E2

    def test_backfill_skipped_when_action_notify(self):
        """backfill is a no-op when action='notify'."""
        ms = self._make_ms({
            ("srcA", "11"): [{"stream_id": "a1", "season": "1", "episode_num": 1, "title": "E1"}],
        })
        entry = {
            "id": "bf-3",
            "series_name": "Notify Only",
            "series_id": "11",
            "source_id": None,
            "monitor_sources": [{"source_id": "srcA", "series_ref": "11"}],
            "scope": "all",
            "action": "notify",
            "known_episodes": [],
            "downloaded_episodes": [],
            "cover": "",
        }
        count = _run(ms.backfill_episodes(entry, backfill="all"))
        assert count == 0
        assert ms.cart_service.cart == []


# ---------------------------------------------------------------
# monitor_sources vs. fuzzy-search fallback (integration)
# ---------------------------------------------------------------

class TestMonitorSourceFallbackToFuzzy:
    """When monitor_sources is empty AND source_id is None the service falls
    back to a fuzzy cross-source search — verify this still works."""

    def _make_ms_fuzzy(self, series_in_cache, fetch_results):
        from app.services.monitor_service import MonitorService

        class _Cfg:
            config = {"sources": [
                {"id": "srcA", "enabled": True},
                {"id": "srcB", "enabled": True},
            ]}

        class _Xtream:
            async def fetch_series_episodes(self, source_id, series_id):
                return fetch_results.get((source_id, str(series_id)), [])

        class _Cache:
            def get_cached_with_source_info(self, *_args, **_kwargs):
                return series_in_cache, []

        class _Cart:
            cart = []

            def build_download_filepath(self, item):
                return "/nonexistent/" + item["stream_id"]

            def save_cart(self):
                pass

        ms = MonitorService.__new__(MonitorService)
        ms._cfg = _Cfg()
        ms.cache_service = _Cache()
        ms.xtream_service = _Xtream()
        ms.cart_service = _Cart()
        return ms

    def test_fuzzy_fallback_finds_episodes(self):
        """With no monitor_sources and no source_id, fuzzy search finds the series."""
        series_in_cache = [
            {
                "_source_id": "srcA",
                "series_id": "500",
                "name": "Breaking Bad",
                "tmdb_id": None, "tmdb": None,
                "imdb_id": None, "imdb": None,
            }
        ]
        fetch_results = {
            ("srcA", "500"): [
                {"stream_id": "s1", "season": "1", "episode_num": 1, "title": "Ep1"},
                {"stream_id": "s2", "season": "1", "episode_num": 2, "title": "Ep2"},
            ]
        }
        ms = self._make_ms_fuzzy(series_in_cache, fetch_results)
        entry = {
            "id": "u-fz",
            "series_name": "Breaking Bad",
            "series_id": "500",
            "source_id": None,
            "monitor_sources": [],      # empty → fuzzy fallback
            "scope": "new_only",
            "season_filter": None,
            "action": "notify",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
            "tmdb_id": None,
            "imdb_id": None,
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert len(new_eps) == 2

    def test_fuzzy_fallback_returns_empty_when_series_not_found(self):
        """When fuzzy search finds nothing, result is empty with no error."""
        ms = self._make_ms_fuzzy([], {})
        entry = {
            "id": "u-fz2",
            "series_name": "Unknown Show",
            "series_id": "0",
            "source_id": None,
            "monitor_sources": [],
            "scope": "new_only",
            "season_filter": None,
            "action": "notify",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 99,
            "cover": "",
            "tmdb_id": None,
            "imdb_id": None,
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert new_eps == []
        assert entry["last_new_count"] == 0


# ---------------------------------------------------------------
# canonical_name persistence
# ---------------------------------------------------------------

class TestCanonicalNamePersistence:
    """Verify that canonical_name is saved to and loaded from the DB."""

    def _make_ms(self, d):
        from app.services.config_service import ConfigService
        from app.services.monitor_service import MonitorService

        cfg = ConfigService(d)
        cfg.load()
        db_path = os.path.join(d, DB_NAME)
        init_db(db_path)
        ms = MonitorService.__new__(MonitorService)
        ms._cfg = cfg
        ms.db_path = db_path
        ms._monitored_series = []
        return ms

    def _base_entry(self, **kwargs):
        e = {
            "id": "cn-uuid-1",
            "series_name": "The Last of Us",
            "series_id": "500",
            "source_id": "src1",
            "source_name": "Provider A",
            "source_category": None,
            "monitor_sources": [],
            "cover": "",
            "scope": "new_only",
            "season_filter": None,
            "action": "both",
            "enabled": True,
            "known_episodes": [],
            "downloaded_episodes": [],
            "created_at": "2026-01-01T00:00:00",
            "last_checked": None,
            "last_new_count": 0,
            "tmdb_id": "100088",
            "imdb_id": "tt3581920",
        }
        e.update(kwargs)
        return e

    def test_canonical_name_saved_and_loaded(self):
        """canonical_name round-trips correctly via SQLite."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
            ms._monitored_series = [self._base_entry(canonical_name="The Last of Us (2023)")]
            ms.save_monitored()

            ms._monitored_series = []
            ms.load_monitored()

            assert ms._monitored_series[0]["canonical_name"] == "The Last of Us (2023)"

    def test_canonical_name_none_saved_and_loaded(self):
        """When canonical_name is None it is stored as NULL and loaded as None."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
            ms._monitored_series = [self._base_entry(canonical_name=None)]
            ms.save_monitored()

            ms._monitored_series = []
            ms.load_monitored()

            assert ms._monitored_series[0]["canonical_name"] is None

    def test_canonical_name_updated_persists(self):
        """Updating canonical_name and saving again overwrites the previous value."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
            ms._monitored_series = [self._base_entry(canonical_name="Old Name")]
            ms.save_monitored()

            ms._monitored_series[0]["canonical_name"] = "New Canonical Name"
            ms.save_monitored()

            ms._monitored_series = []
            ms.load_monitored()

            assert ms._monitored_series[0]["canonical_name"] == "New Canonical Name"

    def test_canonical_name_independent_per_entry(self):
        """Two monitored entries each retain their own canonical_name."""
        with tempfile.TemporaryDirectory() as d:
            ms = self._make_ms(d)
            ms._monitored_series = [
                self._base_entry(id="cn-1", series_name="Show A", canonical_name="Show A Canon"),
                self._base_entry(id="cn-2", series_name="Show B", canonical_name=None),
            ]
            ms.save_monitored()

            ms._monitored_series = []
            ms.load_monitored()

            by_id = {e["id"]: e for e in ms._monitored_series}
            assert by_id["cn-1"]["canonical_name"] == "Show A Canon"
            assert by_id["cn-2"]["canonical_name"] is None


# ---------------------------------------------------------------
# resolve_canonical_name
# ---------------------------------------------------------------

class TestResolveCanonicalName:
    """Unit tests for MonitorService.resolve_canonical_name()."""

    def _make_ms(self, monitored: list[dict]):
        from app.services.monitor_service import MonitorService

        ms = MonitorService.__new__(MonitorService)
        ms._monitored_series = monitored
        return ms

    def _entry(self, canonical_name, tmdb_id=None, imdb_id=None):
        return {
            "id": "x",
            "series_name": "Some Show",
            "canonical_name": canonical_name,
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
        }

    def test_match_by_tmdb_id(self):
        ms = self._make_ms([self._entry("The Wire (2002)", tmdb_id="1438")])
        assert ms.resolve_canonical_name("1438", None) == "The Wire (2002)"

    def test_match_by_imdb_id(self):
        ms = self._make_ms([self._entry("Breaking Bad (2008)", imdb_id="tt0903747")])
        assert ms.resolve_canonical_name(None, "tt0903747") == "Breaking Bad (2008)"

    def test_tmdb_prefix_normalized(self):
        """Stored tmdb_id with 'tmdb:' prefix is still matched."""
        ms = self._make_ms([self._entry("Severance (2022)", tmdb_id="tmdb:114644")])
        assert ms.resolve_canonical_name("114644", None) == "Severance (2022)"

    def test_imdb_prefix_normalized(self):
        """Stored imdb_id with raw 'tt' prefix matches correctly."""
        ms = self._make_ms([self._entry("Shogun (2024)", imdb_id="tt2788316")])
        assert ms.resolve_canonical_name(None, "tt2788316") == "Shogun (2024)"

    def test_no_match_different_tmdb(self):
        ms = self._make_ms([self._entry("Show A", tmdb_id="111")])
        assert ms.resolve_canonical_name("999", None) is None

    def test_no_match_no_canonical_name_set(self):
        """Entry with matching TMDB but no canonical_name is skipped."""
        ms = self._make_ms([self._entry(None, tmdb_id="1438")])
        assert ms.resolve_canonical_name("1438", None) is None

    def test_no_match_both_ids_none(self):
        """When both tmdb_id and imdb_id are None, returns None immediately."""
        ms = self._make_ms([self._entry("The Wire (2002)", tmdb_id="1438")])
        assert ms.resolve_canonical_name(None, None) is None

    def test_empty_monitored_list(self):
        ms = self._make_ms([])
        assert ms.resolve_canonical_name("1438", "tt0306414") is None

    def test_tmdb_wins_before_imdb_checked(self):
        """When TMDB matches, the canonical_name from that entry is returned."""
        ms = self._make_ms([
            self._entry("Wire (TMDB match)", tmdb_id="1438", imdb_id="tt0306414"),
            self._entry("Wire (IMDB match)", tmdb_id="9999", imdb_id="tt0306414"),
        ])
        result = ms.resolve_canonical_name("1438", "tt0306414")
        assert result == "Wire (TMDB match)"

    def test_imdb_fallback_when_tmdb_mismatch(self):
        """When TMDB doesn't match, IMDB is tried on the same entry."""
        ms = self._make_ms([
            self._entry("Correct Show", tmdb_id="9999", imdb_id="tt0306414"),
        ])
        assert ms.resolve_canonical_name("0001", "tt0306414") == "Correct Show"

    def test_multiple_entries_correct_one_returned(self):
        """With several entries, only the matching one's canonical_name is returned."""
        ms = self._make_ms([
            self._entry("Show X", tmdb_id="111"),
            self._entry("Show Y", tmdb_id="222"),
            self._entry("Show Z", tmdb_id="333"),
        ])
        assert ms.resolve_canonical_name("222", None) == "Show Y"


# ---------------------------------------------------------------
# canonical_name used on cart items (check & backfill)
# ---------------------------------------------------------------

class TestCanonicalNameInCartItems:
    """Verify that the canonical_name of a monitored entry is used as the
    series_name on every cart item produced by _check_single_monitored and
    backfill_episodes."""

    def _make_ms(self, fetch_results: dict):
        from app.services.monitor_service import MonitorService

        class _Cfg:
            config = {"sources": []}

        class _Xtream:
            async def fetch_series_episodes(self, source_id, series_id):
                return fetch_results.get((source_id, str(series_id)), [])

        class _Cart:
            cart = []

            def build_download_filepath(self, item):
                return "/nonexistent/" + item["stream_id"]

            def save_cart(self):
                pass

            def is_in_download_window(self):
                return False

            def _try_start_worker(self):
                return False

        ms = MonitorService.__new__(MonitorService)
        ms._cfg = _Cfg()
        ms.xtream_service = _Xtream()
        ms.cart_service = _Cart()
        return ms

    def test_check_uses_canonical_name_on_cart_item(self):
        """_check_single_monitored sets series_name = canonical_name on each cart item."""
        ms = self._make_ms({
            ("srcA", "10"): [
                {"stream_id": "e1", "season": "1", "episode_num": 1, "title": "Pilot"},
            ],
        })
        entry = {
            "id": "u-cn",
            "series_name": "FR - The Wire",
            "canonical_name": "The Wire (2002)",
            "series_id": "10",
            "source_id": None,
            "monitor_sources": [{"source_id": "srcA", "series_ref": "10"}],
            "scope": "new_only",
            "season_filter": None,
            "action": "download",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert len(new_eps) == 1
        assert new_eps[0]["series_name"] == "The Wire (2002)"
        assert ms.cart_service.cart[0]["series_name"] == "The Wire (2002)"

    def test_check_fallback_to_series_name_when_no_canonical(self):
        """When canonical_name is absent/None, series_name is used as-is."""
        ms = self._make_ms({
            ("srcA", "10"): [
                {"stream_id": "e1", "season": "1", "episode_num": 1, "title": "Pilot"},
            ],
        })
        entry = {
            "id": "u-ns",
            "series_name": "FR - The Wire",
            "canonical_name": None,
            "series_id": "10",
            "source_id": None,
            "monitor_sources": [{"source_id": "srcA", "series_ref": "10"}],
            "scope": "new_only",
            "season_filter": None,
            "action": "download",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert len(new_eps) == 1
        assert new_eps[0]["series_name"] == "FR - The Wire"

    def test_check_canonical_name_used_across_multiple_sources(self):
        """Episodes from two sources both carry canonical_name as their series_name."""
        ms = self._make_ms({
            ("srcA", "10"): [{"stream_id": "a1", "season": "1", "episode_num": 1, "title": "E1"}],
            ("srcB", "20"): [{"stream_id": "b1", "season": "1", "episode_num": 2, "title": "E2"}],
        })
        entry = {
            "id": "u-multi-cn",
            "series_name": "NF - Breaking Bad",
            "canonical_name": "Breaking Bad (2008)",
            "series_id": "10",
            "source_id": None,
            "monitor_sources": [
                {"source_id": "srcA", "series_ref": "10"},
                {"source_id": "srcB", "series_ref": "20"},
            ],
            "scope": "new_only",
            "season_filter": None,
            "action": "download",
            "known_episodes": [],
            "downloaded_episodes": [],
            "last_new_count": 0,
            "cover": "",
        }
        new_eps = _run(ms._check_single_monitored(entry))
        assert len(new_eps) == 2
        assert all(ep["series_name"] == "Breaking Bad (2008)" for ep in new_eps)

    def test_backfill_uses_canonical_name(self):
        """backfill_episodes sets series_name = canonical_name on queued cart items."""
        ms = self._make_ms({
            ("srcA", "10"): [
                {"stream_id": "e1", "season": "1", "episode_num": 1, "title": "E1"},
                {"stream_id": "e2", "season": "1", "episode_num": 2, "title": "E2"},
            ],
        })
        entry = {
            "id": "bf-cn",
            "series_name": "NF - Shogun",
            "canonical_name": "Shogun (2024)",
            "series_id": "10",
            "source_id": None,
            "monitor_sources": [{"source_id": "srcA", "series_ref": "10"}],
            "scope": "all",
            "action": "both",
            "known_episodes": [],
            "downloaded_episodes": [],
            "cover": "",
        }
        count = _run(ms.backfill_episodes(entry, backfill="all"))
        assert count == 2
        assert all(i["series_name"] == "Shogun (2024)" for i in ms.cart_service.cart)

    def test_backfill_fallback_to_series_name_when_no_canonical(self):
        """backfill_episodes falls back to series_name when canonical_name is None."""
        ms = self._make_ms({
            ("srcA", "10"): [
                {"stream_id": "e1", "season": "1", "episode_num": 1, "title": "E1"},
            ],
        })
        entry = {
            "id": "bf-ns",
            "series_name": "NF - Shogun",
            "canonical_name": None,
            "series_id": "10",
            "source_id": None,
            "monitor_sources": [{"source_id": "srcA", "series_ref": "10"}],
            "scope": "all",
            "action": "both",
            "known_episodes": [],
            "downloaded_episodes": [],
            "cover": "",
        }
        count = _run(ms.backfill_episodes(entry, backfill="all"))
        assert count == 1
        assert ms.cart_service.cart[0]["series_name"] == "NF - Shogun"


# ---------------------------------------------------------------
# CartService._enrich_item_name_from_metadata canonical lookup
# ---------------------------------------------------------------

class TestCartServiceCanonicalNameResolution:
    """Verify _enrich_item_name_from_metadata uses monitor_service.resolve_canonical_name
    when the fetched series info contains a TMDB/IMDB id that matches a monitored entry."""

    def _make_cart(self, monitor_service=None):
        from app.services.cart_service import CartService

        class _Cfg:
            config = {}

        cart = CartService.__new__(CartService)
        cart._cfg = _Cfg()
        cart.monitor_service = monitor_service
        return cart

    def _make_monitor(self, canonical_name, tmdb_id=None, imdb_id=None):
        from app.services.monitor_service import MonitorService

        ms = MonitorService.__new__(MonitorService)
        ms._monitored_series = [{
            "id": "x",
            "series_name": "Raw Name",
            "canonical_name": canonical_name,
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
        }]
        return ms

    def test_canonical_name_used_when_tmdb_matches(self):
        """series_name is set to canonical_name when TMDB id matches a monitored entry."""
        from app.services.cart_service import CartService

        class _Xtream:
            async def fetch_series_info(self, source_id, series_id):
                return {"name": "FR - The Wire", "tmdb_id": "1438", "imdb_id": None}

        monitor = self._make_monitor("The Wire (2002)", tmdb_id="1438")
        cart = self._make_cart(monitor_service=monitor)
        cart.xtream_service = _Xtream()

        item = {
            "content_type": "series",
            "source_id": "src1",
            "stream_id": "9",
            "series_id": "42",
            "series_name": "FR - The Wire",
        }
        _run(CartService._enrich_item_name_from_metadata(cart, item))
        assert item["series_name"] == "The Wire (2002)"

    def test_canonical_name_used_when_imdb_matches(self):
        """series_name is set to canonical_name when IMDB id matches (no TMDB)."""
        from app.services.cart_service import CartService

        class _Xtream:
            async def fetch_series_info(self, source_id, series_id):
                return {"name": "NF - BB", "tmdb_id": None, "imdb_id": "tt0903747"}

        monitor = self._make_monitor("Breaking Bad (2008)", imdb_id="tt0903747")
        cart = self._make_cart(monitor_service=monitor)
        cart.xtream_service = _Xtream()

        item = {
            "content_type": "series",
            "source_id": "src1",
            "stream_id": "7",
            "series_id": "20",
            "series_name": "NF - BB",
        }
        _run(CartService._enrich_item_name_from_metadata(cart, item))
        assert item["series_name"] == "Breaking Bad (2008)"

    def test_falls_back_to_metadata_title_when_no_monitor_match(self):
        """When no monitored entry matches, series_name comes from metadata title."""
        from app.services.cart_service import CartService

        class _Xtream:
            async def fetch_series_info(self, source_id, series_id):
                return {"name": "Breaking Bad", "tmdb_id": "9999", "imdb_id": None}

        monitor = self._make_monitor("Other Show", tmdb_id="1111")
        cart = self._make_cart(monitor_service=monitor)
        cart.xtream_service = _Xtream()

        item = {
            "content_type": "series",
            "source_id": "src1",
            "stream_id": "7",
            "series_id": "20",
            "series_name": "NF - BB",
        }
        _run(CartService._enrich_item_name_from_metadata(cart, item))
        assert item["series_name"] == "Breaking Bad"

    def test_falls_back_to_metadata_title_when_monitor_service_none(self):
        """When monitor_service is None, series_name comes from metadata title."""
        from app.services.cart_service import CartService

        class _Xtream:
            async def fetch_series_info(self, source_id, series_id):
                return {"name": "Severance", "tmdb_id": "114644", "imdb_id": None}

        cart = self._make_cart(monitor_service=None)
        cart.xtream_service = _Xtream()

        item = {
            "content_type": "series",
            "source_id": "src1",
            "stream_id": "5",
            "series_id": "30",
            "series_name": "FR - Severance",
        }
        _run(CartService._enrich_item_name_from_metadata(cart, item))
        assert item["series_name"] == "Severance"

    def test_no_series_info_leaves_series_name_unchanged(self):
        """When xtream returns None for series info, series_name is untouched."""
        from app.services.cart_service import CartService

        class _Xtream:
            async def fetch_series_info(self, source_id, series_id):
                return None

        cart = self._make_cart(monitor_service=None)
        cart.xtream_service = _Xtream()

        item = {
            "content_type": "series",
            "source_id": "src1",
            "stream_id": "5",
            "series_id": "30",
            "series_name": "Original Name",
        }
        _run(CartService._enrich_item_name_from_metadata(cart, item))
        assert item["series_name"] == "Original Name"
