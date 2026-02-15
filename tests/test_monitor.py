"""Tests for the series monitoring feature."""

import os
import sys
import json
import tempfile

# Add app directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

# Mock the initialization to avoid side effects during import
os.environ["TESTING"] = "1"


class TestMonitoredDataModel:
    """Tests for monitored series data model and persistence."""

    def test_load_monitored_empty(self):
        """Test loading monitored series from non-existent file."""
        import main

        original_file = main.MONITORED_FILE
        main.MONITORED_FILE = "/tmp/nonexistent_monitored_test.json"
        try:
            result = main.load_monitored()
            assert result == []
            assert main._monitored_series == []
        finally:
            main.MONITORED_FILE = original_file

    def test_save_and_load_monitored(self):
        """Test round-trip save and load of monitored series."""
        import main

        original_file = main.MONITORED_FILE
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_file = f.name

        try:
            main.MONITORED_FILE = temp_file

            test_data = [
                {
                    "id": "test-uuid-1",
                    "series_name": "Breaking Bad",
                    "series_id": "123",
                    "source_id": "src1",
                    "source_name": "Source 1",
                    "cover": "http://example.com/cover.jpg",
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

            main.save_monitored(test_data)

            # Verify file content
            with open(temp_file) as f:
                saved = json.load(f)
            assert "series" in saved
            assert len(saved["series"]) == 1
            assert saved["series"][0]["series_name"] == "Breaking Bad"

            # Verify reload
            loaded = main.load_monitored()
            assert len(loaded) == 1
            assert loaded[0]["id"] == "test-uuid-1"
            assert loaded[0]["series_name"] == "Breaking Bad"
            assert len(loaded[0]["known_episodes"]) == 2
        finally:
            main.MONITORED_FILE = original_file
            os.unlink(temp_file)

    def test_load_monitored_corrupt_file(self):
        """Test loading from a corrupt JSON file."""
        import main

        original_file = main.MONITORED_FILE
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json{{{")
            temp_file = f.name

        try:
            main.MONITORED_FILE = temp_file
            result = main.load_monitored()
            assert result == []
        finally:
            main.MONITORED_FILE = original_file
            os.unlink(temp_file)


class TestMonitorDiffLogic:
    """Tests for the episode diff detection logic."""

    def test_new_episodes_detected(self):
        """Test that new episodes are correctly identified via diff."""
        # Simulate the diff logic used in _check_single_monitored
        known_episodes = [
            {"stream_id": "1001", "source_id": "src1", "season": "1", "episode_num": 1},
            {"stream_id": "1002", "source_id": "src1", "season": "1", "episode_num": 2},
            {"stream_id": "1003", "source_id": "src1", "season": "1", "episode_num": 3},
        ]
        downloaded_episodes = [
            {"stream_id": "1004", "source_id": "src1", "season": "1", "episode_num": 4},
        ]

        # Simulated fetched episodes (includes all known + 2 new)
        fetched_episodes = [
            {"stream_id": "1001", "season": "1", "episode_num": 1, "title": "Pilot"},
            {"stream_id": "1002", "season": "1", "episode_num": 2, "title": "Cat's in the Bag"},
            {"stream_id": "1003", "season": "1", "episode_num": 3, "title": "And the Bag's in the River"},
            {"stream_id": "1004", "season": "1", "episode_num": 4, "title": "Cancer Man"},
            {"stream_id": "1005", "season": "1", "episode_num": 5, "title": "Gray Matter"},
            {"stream_id": "1006", "season": "1", "episode_num": 6, "title": "Crazy Handful of Nothin'"},
        ]

        # Diff logic
        known_keys = {
            (str(k.get("season")), int(k.get("episode_num", 0))) for k in known_episodes
        }
        downloaded_keys = {
            (str(k.get("season")), int(k.get("episode_num", 0))) for k in downloaded_episodes
        }
        already_seen = known_keys | downloaded_keys

        new_episodes = []
        for ep in fetched_episodes:
            ep_key = (str(ep.get("season")), int(ep.get("episode_num", 0)))
            if ep_key not in already_seen:
                new_episodes.append(ep)

        assert len(new_episodes) == 2
        assert new_episodes[0]["title"] == "Gray Matter"
        assert new_episodes[1]["title"] == "Crazy Handful of Nothin'"

    def test_no_new_episodes(self):
        """Test when all episodes are already known."""
        known_episodes = [
            {"season": "1", "episode_num": 1},
            {"season": "1", "episode_num": 2},
        ]
        fetched_episodes = [
            {"stream_id": "1001", "season": "1", "episode_num": 1},
            {"stream_id": "1002", "season": "1", "episode_num": 2},
        ]

        known_keys = {(str(k["season"]), int(k["episode_num"])) for k in known_episodes}
        new_episodes = [
            ep for ep in fetched_episodes
            if (str(ep["season"]), int(ep["episode_num"])) not in known_keys
        ]

        assert len(new_episodes) == 0

    def test_empty_known_detects_all(self):
        """Test scope='all' behavior (known_episodes empty → everything is new)."""
        known_episodes = []
        fetched_episodes = [
            {"stream_id": "1001", "season": "1", "episode_num": 1},
            {"stream_id": "1002", "season": "1", "episode_num": 2},
            {"stream_id": "1003", "season": "2", "episode_num": 1},
        ]

        known_keys = {(str(k["season"]), int(k["episode_num"])) for k in known_episodes}
        new_episodes = [
            ep for ep in fetched_episodes
            if (str(ep["season"]), int(ep["episode_num"])) not in known_keys
        ]

        assert len(new_episodes) == 3


class TestMonitorScopeFilters:
    """Tests for monitoring scope filtering."""

    def test_scope_season_filter(self):
        """Test season scope only keeps episodes from the target season."""
        episodes = [
            {"stream_id": "1", "season": "1", "episode_num": 1},
            {"stream_id": "2", "season": "1", "episode_num": 2},
            {"stream_id": "3", "season": "2", "episode_num": 1},
            {"stream_id": "4", "season": "2", "episode_num": 2},
            {"stream_id": "5", "season": "3", "episode_num": 1},
        ]

        season_filter = "2"
        filtered = [ep for ep in episodes if str(ep.get("season")) == str(season_filter)]

        assert len(filtered) == 2
        assert all(ep["season"] == "2" for ep in filtered)

    def test_scope_all_no_filter(self):
        """Test 'all' scope doesn't filter any episodes."""
        episodes = [
            {"stream_id": "1", "season": "1", "episode_num": 1},
            {"stream_id": "2", "season": "2", "episode_num": 1},
        ]

        scope = "all"
        # No filter applied for scope=all
        filtered = episodes  # identity
        assert len(filtered) == 2

    def test_scope_new_only_snapshot(self):
        """Test 'new_only' scope: snapshot at activation means all current eps are known."""
        current_episodes = [
            {"stream_id": "1", "season": "1", "episode_num": 1},
            {"stream_id": "2", "season": "1", "episode_num": 2},
        ]

        # Snapshot all current as known
        known_episodes = [
            {"season": str(ep["season"]), "episode_num": int(ep["episode_num"])}
            for ep in current_episodes
        ]

        # Later check finds same + 1 new
        later_episodes = current_episodes + [
            {"stream_id": "3", "season": "1", "episode_num": 3},
        ]

        known_keys = {(str(k["season"]), int(k["episode_num"])) for k in known_episodes}
        new_eps = [ep for ep in later_episodes if (str(ep["season"]), int(ep["episode_num"])) not in known_keys]

        assert len(new_eps) == 1
        assert new_eps[0]["episode_num"] == 3


class TestCrossSourceMatching:
    """Tests for cross-source series name matching via normalize_name."""

    def test_normalize_strips_prefix(self):
        from main import normalize_name

        assert normalize_name("FR - Breaking Bad") == normalize_name("Breaking Bad")

    def test_normalize_strips_quality(self):
        from main import normalize_name

        assert normalize_name("Breaking Bad 4K") == normalize_name("Breaking Bad")
        assert normalize_name("Breaking Bad UHD HDR") == normalize_name("Breaking Bad")

    def test_normalize_strips_language_suffix(self):
        from main import normalize_name

        assert normalize_name("Breaking Bad_fr") == normalize_name("Breaking Bad")

    def test_normalize_strips_year(self):
        from main import normalize_name

        assert normalize_name("Industry - 2024") == normalize_name("Industry")

    def test_fuzzy_match_across_sources(self):
        """Test that similar names from different sources match via fuzzy."""
        from main import normalize_name
        from rapidfuzz import fuzz

        name_a = normalize_name("FR - The Bear (2022)")
        name_b = normalize_name("NF - The Bear")

        score = fuzz.token_sort_ratio(name_a, name_b)
        assert score >= 90, f"Expected score >= 90 but got {score}"

    def test_different_series_dont_match(self):
        """Test that genuinely different series don't match."""
        from main import normalize_name
        from rapidfuzz import fuzz

        name_a = normalize_name("Breaking Bad")
        name_b = normalize_name("Better Call Saul")

        score = fuzz.token_sort_ratio(name_a, name_b)
        assert score < 90, f"Expected score < 90 but got {score}"


class TestDuplicatePrevention:
    """Tests for preventing duplicate downloads in the monitored pipeline."""

    def test_episode_key_based_on_season_and_num(self):
        """Test that episode identity is based on (season, episode_num), not stream_id.
        This ensures the same logical episode from different sources isn't treated as different."""

        # Same episode from different sources
        ep_source_a = {"stream_id": "1001", "source_id": "srcA", "season": "2", "episode_num": 5}
        ep_source_b = {"stream_id": "9999", "source_id": "srcB", "season": "2", "episode_num": 5}

        key_a = (str(ep_source_a["season"]), int(ep_source_a["episode_num"]))
        key_b = (str(ep_source_b["season"]), int(ep_source_b["episode_num"]))

        assert key_a == key_b, "Same logical episode from different sources should have the same key"

    def test_different_episodes_have_different_keys(self):
        """Different episodes should have different keys."""
        ep_a = {"season": "1", "episode_num": 1}
        ep_b = {"season": "1", "episode_num": 2}
        ep_c = {"season": "2", "episode_num": 1}

        keys = {
            (str(ep["season"]), int(ep["episode_num"]))
            for ep in [ep_a, ep_b, ep_c]
        }

        assert len(keys) == 3
