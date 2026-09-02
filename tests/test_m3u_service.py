"""Tests for M3U generation and the bounded preview path."""

from app.services.m3u_service import M3uService


class _Config:
    def __init__(self, config):
        self.config = config


class _Cache:
    def __init__(self, sources):
        self._sources = sources

    def get_cached(self, key, source_id=None):
        if source_id is not None:
            return self._sources.get(source_id, {}).get(key, [])
        return [item for source in self._sources.values() for item in source.get(key, [])]


def _source(filters=None):
    return {
        "id": "src1",
        "name": "Test source",
        "host": "http://provider.test",
        "username": "user",
        "password": "pass",
        "enabled": True,
        "filters": filters or {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
    }


def test_preview_returns_exact_stats_and_bounded_sample():
    filters = {
        "live": {
            "groups": [{"type": "include", "match": "exact", "value": "Sports"}],
            "channels": [{"type": "exclude", "match": "contains", "value": "bad"}],
        },
        "vod": {"groups": [], "channels": []},
        "series": {"groups": [], "channels": []},
    }
    config = _Config({
        "sources": [_source(filters)],
        "content_types": {"live": True, "vod": True, "series": False},
    })
    cache = _Cache({
        "src1": {
            "live_categories": [
                {"category_id": "1", "category_name": "Sports"},
                {"category_id": "2", "category_name": "News"},
            ],
            "live_streams": [
                {"stream_id": "1", "name": "Good match", "category_id": "1"},
                {"stream_id": "2", "name": "Bad match", "category_id": "1"},
                {"stream_id": "3", "name": "News item", "category_id": "2"},
            ],
            "vod_categories": [],
            "vod_streams": [{"stream_id": "4", "name": "Movie", "category_id": ""}],
            "series_categories": [],
            "series": [],
        }
    })

    result = M3uService(config, cache).generate_preview("http://server", sample_limit=2)

    assert result == {
        "stats": "# Content: 1 live, 1 movies, 0 series | 2 excluded | 1 source(s)",
        "sample_channels": ["Good match", "Movie"],
    }


def test_preview_does_not_require_a_configured_source():
    config = _Config({
        "sources": [],
        "content_types": {"live": True, "vod": True, "series": True},
    })

    result = M3uService(config, _Cache({})).generate_preview("http://server")

    assert result == {"stats": "# Error: No sources configured", "sample_channels": []}
