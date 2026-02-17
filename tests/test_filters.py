"""Unit tests for app.services.filter_service."""

from app.services.filter_service import matches_filter, should_include, normalize_name


# ---------------------------------------------------------------
# matches_filter
# ---------------------------------------------------------------

class TestMatchesFilter:

    def test_contains_match(self):
        rule = {"type": "include", "match": "contains", "value": "sports", "case_sensitive": False}
        assert matches_filter("beIN Sports 1", rule) is True
        assert matches_filter("News Channel", rule) is False

    def test_starts_with_match(self):
        rule = {"type": "include", "match": "starts_with", "value": "FR|", "case_sensitive": False}
        assert matches_filter("FR| TF1", rule) is True
        assert matches_filter("US| CNN", rule) is False

    def test_ends_with_match(self):
        rule = {"type": "include", "match": "ends_with", "value": "HD", "case_sensitive": False}
        assert matches_filter("Canal+ HD", rule) is True
        assert matches_filter("Canal+ SD", rule) is False

    def test_exact_match(self):
        rule = {"type": "include", "match": "exact", "value": "TF1", "case_sensitive": False}
        assert matches_filter("TF1", rule) is True
        assert matches_filter("tf1", rule) is True
        assert matches_filter("TF1 HD", rule) is False

    def test_exact_match_case_sensitive(self):
        rule = {"type": "include", "match": "exact", "value": "TF1", "case_sensitive": True}
        assert matches_filter("TF1", rule) is True
        assert matches_filter("tf1", rule) is False

    def test_not_contains_match(self):
        rule = {"type": "exclude", "match": "not_contains", "value": "XXX", "case_sensitive": False}
        assert matches_filter("Family Channel", rule) is True
        assert matches_filter("XXX Adult", rule) is False

    def test_regex_match(self):
        rule = {"type": "include", "match": "regex", "value": r"^FR\|.*", "case_sensitive": False}
        assert matches_filter("FR| TF1", rule) is True
        assert matches_filter("US| CNN", rule) is False

    def test_regex_invalid_pattern(self):
        rule = {"type": "include", "match": "regex", "value": r"[invalid", "case_sensitive": False}
        assert matches_filter("test", rule) is False

    def test_empty_pattern(self):
        rule = {"type": "include", "match": "contains", "value": "", "case_sensitive": False}
        assert matches_filter("anything", rule) is False

    def test_exclude_all_match(self):
        rule = {"type": "exclude", "match": "all", "value": "*", "case_sensitive": False}
        assert matches_filter("Any Channel", rule) is True
        assert matches_filter("", rule) is True


# ---------------------------------------------------------------
# should_include
# ---------------------------------------------------------------

class TestShouldInclude:

    def test_no_filters_includes_all(self):
        assert should_include("Any Channel", []) is True

    def test_exclude_filter_removes_matching(self):
        rules = [{"type": "exclude", "match": "contains", "value": "XXX", "case_sensitive": False}]
        assert should_include("Family Channel", rules) is True
        assert should_include("XXX Adult", rules) is False

    def test_include_filter_only_keeps_matching(self):
        rules = [{"type": "include", "match": "starts_with", "value": "FR|", "case_sensitive": False}]
        assert should_include("FR| TF1", rules) is True
        assert should_include("US| CNN", rules) is False

    def test_exclude_takes_priority_over_include(self):
        rules = [
            {"type": "include", "match": "starts_with", "value": "FR|", "case_sensitive": False},
            {"type": "exclude", "match": "contains", "value": "XXX", "case_sensitive": False},
        ]
        assert should_include("FR| TF1", rules) is True
        assert should_include("FR| XXX Channel", rules) is False
        assert should_include("US| CNN", rules) is False

    def test_multiple_include_filters_or_logic(self):
        rules = [
            {"type": "include", "match": "starts_with", "value": "FR|", "case_sensitive": False},
            {"type": "include", "match": "starts_with", "value": "UK|", "case_sensitive": False},
        ]
        assert should_include("FR| TF1", rules) is True
        assert should_include("UK| BBC", rules) is True
        assert should_include("US| CNN", rules) is False

    def test_exclude_all_removes_everything(self):
        rules = [{"type": "exclude", "match": "all", "value": "*", "case_sensitive": False}]
        assert should_include("Any Channel", rules) is False

    def test_exclude_all_with_include_exception(self):
        rules = [
            {"type": "exclude", "match": "all", "value": "*", "case_sensitive": False},
            {"type": "include", "match": "exact", "value": "TF1", "case_sensitive": False},
        ]
        # include first, then exclude: TF1 passes include but is still excluded by "all"
        assert should_include("TF1", rules) is False
        assert should_include("Other", rules) is False


# ---------------------------------------------------------------
# normalize_name  (used for cross-source matching)
# ---------------------------------------------------------------

class TestNormalizeName:

    def test_strips_prefix(self):
        assert normalize_name("FR - Breaking Bad") == normalize_name("Breaking Bad")

    def test_strips_quality(self):
        assert normalize_name("Breaking Bad 4K") == normalize_name("Breaking Bad")
        assert normalize_name("Breaking Bad UHD HDR") == normalize_name("Breaking Bad")

    def test_strips_language_suffix(self):
        assert normalize_name("Breaking Bad_fr") == normalize_name("Breaking Bad")

    def test_strips_year(self):
        assert normalize_name("Industry - 2024") == normalize_name("Industry")

    def test_fuzzy_match_across_sources(self):
        from rapidfuzz import fuzz

        name_a = normalize_name("FR - The Bear (2022)")
        name_b = normalize_name("NF - The Bear")
        score = fuzz.token_sort_ratio(name_a, name_b)
        assert score >= 90

    def test_different_series_dont_match(self):
        from rapidfuzz import fuzz

        name_a = normalize_name("Breaking Bad")
        name_b = normalize_name("Better Call Saul")
        score = fuzz.token_sort_ratio(name_a, name_b)
        assert score < 90
