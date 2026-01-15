"""Tests for the filter logic."""

import os
import sys

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
