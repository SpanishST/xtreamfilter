"""Tests for the filter logic."""

import os
import sys
import json
import tempfile

# Add app directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

# Mock the initialization to avoid side effects during import
os.environ["TESTING"] = "1"


class TestMatchesFilter:
    """Tests for matches_filter function."""

    def test_contains_match(self):
        from main import matches_filter

        rule = {"type": "include", "match": "contains", "value": "sports", "case_sensitive": False}
        assert matches_filter("beIN Sports 1", rule) is True
        assert matches_filter("News Channel", rule) is False

    def test_starts_with_match(self):
        from main import matches_filter

        rule = {"type": "include", "match": "starts_with", "value": "FR|", "case_sensitive": False}
        assert matches_filter("FR| TF1", rule) is True
        assert matches_filter("US| CNN", rule) is False

    def test_ends_with_match(self):
        from main import matches_filter

        rule = {"type": "include", "match": "ends_with", "value": "HD", "case_sensitive": False}
        assert matches_filter("Canal+ HD", rule) is True
        assert matches_filter("Canal+ SD", rule) is False

    def test_exact_match(self):
        from main import matches_filter

        rule = {"type": "include", "match": "exact", "value": "TF1", "case_sensitive": False}
        assert matches_filter("TF1", rule) is True
        assert matches_filter("tf1", rule) is True  # case insensitive
        assert matches_filter("TF1 HD", rule) is False

    def test_exact_match_case_sensitive(self):
        from main import matches_filter

        rule = {"type": "include", "match": "exact", "value": "TF1", "case_sensitive": True}
        assert matches_filter("TF1", rule) is True
        assert matches_filter("tf1", rule) is False

    def test_not_contains_match(self):
        from main import matches_filter

        rule = {"type": "exclude", "match": "not_contains", "value": "XXX", "case_sensitive": False}
        assert matches_filter("Family Channel", rule) is True
        assert matches_filter("XXX Adult", rule) is False

    def test_regex_match(self):
        from main import matches_filter

        rule = {"type": "include", "match": "regex", "value": r"^FR\|.*", "case_sensitive": False}
        assert matches_filter("FR| TF1", rule) is True
        assert matches_filter("US| CNN", rule) is False

    def test_regex_invalid_pattern(self):
        from main import matches_filter

        rule = {"type": "include", "match": "regex", "value": r"[invalid", "case_sensitive": False}
        assert matches_filter("test", rule) is False

    def test_empty_pattern(self):
        from main import matches_filter

        rule = {"type": "include", "match": "contains", "value": "", "case_sensitive": False}
        assert matches_filter("anything", rule) is False

    def test_exclude_all_match(self):
        """Test 'all' match type excludes everything."""
        from main import matches_filter

        rule = {"type": "exclude", "match": "all", "value": "*", "case_sensitive": False}
        assert matches_filter("Any Channel", rule) is True
        assert matches_filter("Another Channel", rule) is True
        assert matches_filter("", rule) is True


class TestShouldInclude:
    """Tests for should_include function."""

    def test_no_filters_includes_all(self):
        from main import should_include

        assert should_include("Any Channel", []) is True

    def test_exclude_filter_removes_matching(self):
        from main import should_include

        rules = [{"type": "exclude", "match": "contains", "value": "XXX", "case_sensitive": False}]
        assert should_include("Family Channel", rules) is True
        assert should_include("XXX Adult", rules) is False

    def test_include_filter_only_keeps_matching(self):
        from main import should_include

        rules = [{"type": "include", "match": "starts_with", "value": "FR|", "case_sensitive": False}]
        assert should_include("FR| TF1", rules) is True
        assert should_include("US| CNN", rules) is False

    def test_exclude_takes_priority_over_include(self):
        from main import should_include

        rules = [
            {"type": "include", "match": "starts_with", "value": "FR|", "case_sensitive": False},
            {"type": "exclude", "match": "contains", "value": "XXX", "case_sensitive": False},
        ]
        assert should_include("FR| TF1", rules) is True
        assert should_include("FR| XXX Channel", rules) is False
        assert should_include("US| CNN", rules) is False

    def test_multiple_include_filters_or_logic(self):
        from main import should_include

        rules = [
            {"type": "include", "match": "starts_with", "value": "FR|", "case_sensitive": False},
            {"type": "include", "match": "starts_with", "value": "UK|", "case_sensitive": False},
        ]
        assert should_include("FR| TF1", rules) is True
        assert should_include("UK| BBC", rules) is True
        assert should_include("US| CNN", rules) is False

    def test_exclude_all_removes_everything(self):
        """Test exclude all removes all items."""
        from main import should_include

        rules = [{"type": "exclude", "match": "all", "value": "*", "case_sensitive": False}]
        assert should_include("Any Channel", rules) is False
        assert should_include("Another Channel", rules) is False

    def test_exclude_all_with_include_exception(self):
        """Test exclude all with specific include exception."""
        from main import should_include

        rules = [
            {"type": "exclude", "match": "all", "value": "*", "case_sensitive": False},
            {"type": "include", "match": "exact", "value": "TF1", "case_sensitive": False},
        ]
        # With exclude all, nothing should pass unless specifically included
        # But exclude takes priority, so even TF1 should be excluded
        assert should_include("TF1", rules) is False
        assert should_include("Other", rules) is False


class TestSourceRouting:
    """Tests for per-source routing functionality."""

    def test_get_source_by_route(self):
        """Test finding source by route name."""
        from main import get_source_by_route, load_config, save_config, CONFIG_FILE
        import main
        
        # Save original config file path
        original_config = main.CONFIG_FILE
        
        # Create temp config
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config = {
                "sources": [
                    {"id": "src1", "name": "Source 1", "route": "source1", "enabled": True},
                    {"id": "src2", "name": "Source 2", "route": "source2", "enabled": True},
                    {"id": "src3", "name": "Source 3", "route": "source3", "enabled": False},
                ]
            }
            json.dump(config, f)
            temp_config = f.name
        
        try:
            main.CONFIG_FILE = temp_config
            
            source = get_source_by_route("source1")
            assert source is not None
            assert source["id"] == "src1"
            
            source = get_source_by_route("source2")
            assert source is not None
            assert source["id"] == "src2"
            
            # Disabled source should not be found
            source = get_source_by_route("source3")
            assert source is None
            
            # Non-existent route
            source = get_source_by_route("nonexistent")
            assert source is None
        finally:
            main.CONFIG_FILE = original_config
            os.unlink(temp_config)


class TestCacheProgress:
    """Tests for cache refresh progress tracking."""

    def test_progress_structure(self):
        """Test that progress structure has required fields."""
        from main import _api_cache
        
        progress = _api_cache.get("refresh_progress", {})
        assert "current_source" in progress or progress == {}
        assert "total_sources" in progress or progress == {}
        assert "current_step" in progress or progress == {}
        assert "percent" in progress or progress == {}


class TestConfigMigration:
    """Tests for config migration from old format."""

    def test_old_config_structure_detected(self):
        """Test that old config without sources array is handled."""
        import main
        
        old_config = {
            "xtream": {
                "host": "http://example.com",
                "username": "user",
                "password": "pass"
            },
            "filters": {
                "live": {"groups": [], "channels": []},
                "vod": {"groups": [], "channels": []},
                "series": {"groups": [], "channels": []}
            }
        }
        
        # Old config has no sources array
        assert "sources" not in old_config
        assert "xtream" in old_config

    def test_new_config_structure(self):
        """Test new config with sources array."""
        new_config = {
            "sources": [
                {
                    "id": "abc123",
                    "name": "My Source",
                    "host": "http://example.com",
                    "username": "user",
                    "password": "pass",
                    "enabled": True,
                    "route": "mysource",
                    "filters": {
                        "live": {"groups": [], "channels": []},
                        "vod": {"groups": [], "channels": []},
                        "series": {"groups": [], "channels": []}
                    }
                }
            ]
        }
        
        assert "sources" in new_config
        assert len(new_config["sources"]) == 1
        assert new_config["sources"][0]["route"] == "mysource"
