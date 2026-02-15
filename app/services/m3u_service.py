"""M3U generation service — builds filtered playlists from cached data."""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.services.filter_service import build_category_map, should_include
from app.services.xtream_service import encode_virtual_id

if TYPE_CHECKING:
    from app.services.cache_service import CacheService
    from app.services.config_service import ConfigService


class M3uService:
    """Generates M3U playlists (merged or per-source)."""

    def __init__(self, config_service: "ConfigService", cache_service: "CacheService"):
        self.config_service = config_service
        self.cache_service = cache_service

    # ------------------------------------------------------------------
    # Merged M3U
    # ------------------------------------------------------------------

    def generate_m3u(self, server_url: str, use_virtual_ids: bool = True) -> str:
        """Generate filtered M3U playlist merging all enabled sources."""
        config = self.config_service.config
        sources = config.get("sources", [])
        content_types = config.get("content_types", {"live": True, "vod": True, "series": True})

        if not sources and config.get("xtream", {}).get("host"):
            sources = [{
                "id": "default",
                "name": "Default",
                "host": config["xtream"]["host"],
                "username": config["xtream"]["username"],
                "password": config["xtream"]["password"],
                "enabled": True,
                "prefix": "",
                "filters": config.get("filters", {}),
            }]

        if not sources:
            return "#EXTM3U\n# Error: No sources configured\n"

        server_url = server_url.rstrip("/")
        stream_base = "/merged" if use_virtual_ids else ""

        lines = ["#EXTM3U"]
        stats = {"live": 0, "vod": 0, "series": 0, "excluded": 0, "sources": 0}
        enabled_sources = [s for s in sources if s.get("enabled", True)]

        for source_index, source in enumerate(enabled_sources):
            source_id = source.get("id", "default")
            prefix = source.get("prefix", "")
            filters = source.get("filters", {})

            if not source.get("host") or not source.get("username") or not source.get("password"):
                continue

            stats["sources"] += 1

            # Live TV
            if content_types.get("live", True):
                self._append_live(
                    lines, stats, source_id, prefix, filters,
                    server_url, stream_base, use_virtual_ids, source_index,
                )

            # VOD
            if content_types.get("vod", True):
                self._append_vod(
                    lines, stats, source_id, prefix, filters,
                    server_url, stream_base, use_virtual_ids, source_index,
                )

            # Series
            if content_types.get("series", True):
                self._append_series(lines, stats, source_id, prefix, filters)

        stats_line = (
            f"# Content: {stats['live']} live, {stats['vod']} movies, {stats['series']} series "
            f"| {stats['excluded']} excluded | {stats['sources']} source(s)"
        )
        lines.insert(1, stats_line)
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Per-source M3U
    # ------------------------------------------------------------------

    def generate_m3u_for_source(self, source: dict, server_url: str, source_route: str) -> str:
        """Generate filtered M3U playlist for a single source."""
        source_id = source.get("id", "default")
        prefix = source.get("prefix", "")
        filters = source.get("filters", {})

        server_url = server_url.rstrip("/")
        stream_base = f"/{source_route}"

        lines = ["#EXTM3U"]
        stats = {"live": 0, "vod": 0, "series": 0, "excluded": 0}

        self._append_live(
            lines, stats, source_id, prefix, filters,
            server_url, stream_base, False, 0,
        )
        self._append_vod(
            lines, stats, source_id, prefix, filters,
            server_url, stream_base, False, 0,
        )
        self._append_series(lines, stats, source_id, prefix, filters)

        stats_line = (
            f"# Content: {stats['live']} live, {stats['vod']} movies, {stats['series']} series "
            f"| {stats['excluded']} excluded | Source: {source.get('name', source_id)}"
        )
        lines.insert(1, stats_line)
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _append_live(
        self, lines: list, stats: dict,
        source_id: str, prefix: str, filters: dict,
        server_url: str, stream_base: str,
        use_virtual_ids: bool, source_index: int,
    ) -> None:
        live_filters = filters.get("live", {"groups": [], "channels": []})
        live_group_filters = live_filters.get("groups", [])
        live_channel_filters = live_filters.get("channels", [])

        categories = self.cache_service.get_cached("live_categories", source_id)
        cat_map = build_category_map(categories)
        streams = self.cache_service.get_cached("live_streams", source_id)

        for stream in streams:
            stream_id = stream.get("stream_id")
            name = stream.get("name", "Unknown")
            icon = stream.get("stream_icon", "")
            cat_id = str(stream.get("category_id", ""))
            group = cat_map.get(cat_id, "Unknown")
            epg_id = stream.get("epg_channel_id", "")
            if epg_id:
                epg_id = f"{source_id}_{epg_id}".lower()

            if not should_include(group, live_group_filters):
                stats["excluded"] += 1
                continue
            if not should_include(name, live_channel_filters):
                stats["excluded"] += 1
                continue

            display_group = f"{prefix}{group}" if prefix else group
            url_id = encode_virtual_id(source_index, stream_id) if use_virtual_ids else stream_id
            stream_url = f"{server_url}{stream_base}/user/pass/{url_id}.ts"
            lines.append(f'#EXTINF:-1 tvg-id="{epg_id}" tvg-logo="{icon}" group-title="{display_group}",{name}')
            lines.append(stream_url)
            stats["live"] += 1

    def _append_vod(
        self, lines: list, stats: dict,
        source_id: str, prefix: str, filters: dict,
        server_url: str, stream_base: str,
        use_virtual_ids: bool, source_index: int,
    ) -> None:
        vod_filters = filters.get("vod", {"groups": [], "channels": []})
        vod_group_filters = vod_filters.get("groups", [])
        vod_channel_filters = vod_filters.get("channels", [])

        vod_categories = self.cache_service.get_cached("vod_categories", source_id)
        vod_cat_map = build_category_map(vod_categories)
        vod_streams = self.cache_service.get_cached("vod_streams", source_id)

        for vod in vod_streams:
            stream_id = vod.get("stream_id")
            name = vod.get("name", "Unknown")
            icon = vod.get("stream_icon", "")
            cat_id = str(vod.get("category_id", ""))
            group = vod_cat_map.get(cat_id, "Movies")
            extension = vod.get("container_extension", "mp4")

            if not should_include(group, vod_group_filters):
                stats["excluded"] += 1
                continue
            if not should_include(name, vod_channel_filters):
                stats["excluded"] += 1
                continue

            display_group = f"{prefix}{group}" if prefix else group
            url_id = encode_virtual_id(source_index, stream_id) if use_virtual_ids else stream_id
            stream_url = f"{server_url}{stream_base}/movie/user/pass/{url_id}.{extension}"
            lines.append(f'#EXTINF:-1 tvg-logo="{icon}" group-title="{display_group}",{name}')
            lines.append(stream_url)
            stats["vod"] += 1

    def _append_series(
        self, lines: list, stats: dict,
        source_id: str, prefix: str, filters: dict,
    ) -> None:
        series_filters = filters.get("series", {"groups": [], "channels": []})
        series_group_filters = series_filters.get("groups", [])
        series_channel_filters = series_filters.get("channels", [])

        series_categories = self.cache_service.get_cached("series_categories", source_id)
        series_cat_map = build_category_map(series_categories)
        all_series = self.cache_service.get_cached("series", source_id)

        for series in all_series:
            series_name = series.get("name", "Unknown")
            series_icon = series.get("cover", "")
            cat_id = str(series.get("category_id", ""))
            group = series_cat_map.get(cat_id, "Series")

            if not should_include(group, series_group_filters):
                stats["excluded"] += 1
                continue
            if not should_include(series_name, series_channel_filters):
                stats["excluded"] += 1
                continue

            display_group = f"{prefix}{group}" if prefix else group
            lines.append(f'#EXTINF:-1 tvg-logo="{series_icon}" group-title="{display_group}",{series_name}')
            lines.append("# Series - use Xtream API for episodes")
            stats["series"] += 1
