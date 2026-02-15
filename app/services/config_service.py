"""Configuration service — loads, saves, migrates, and provides access to AppConfig."""
from __future__ import annotations

import json
import logging
import os
import uuid

logger = logging.getLogger(__name__)


class ConfigService:
    """Manages application configuration with file persistence.

    The config is kept in-memory after first load and re-read on explicit
    ``load()`` or ``reload()`` calls.  Every route that needs the config
    should depend on this service rather than reading the JSON directly.
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.config_file = os.path.join(data_dir, "config.json")
        self._config: dict = self._default_config()

    # ------------------------------------------------------------------
    # Defaults
    # ------------------------------------------------------------------

    @staticmethod
    def _default_filters() -> dict:
        return {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        }

    @classmethod
    def _default_config(cls) -> dict:
        return {
            "sources": [],
            "xtream": {"host": "", "username": "", "password": ""},
            "filters": cls._default_filters(),
            "content_types": {"live": True, "vod": True, "series": True},
            "options": {
                "cache_enabled": True,
                "cache_ttl": 3600,
                "refresh_interval": 3600,
                "proxy_streams": True,
                "telegram": {"enabled": False, "bot_token": "", "chat_id": ""},
            },
        }

    # ------------------------------------------------------------------
    # Load / Save
    # ------------------------------------------------------------------

    def load(self) -> dict:
        """Load configuration from disk, applying defaults and migrations."""
        default = self._default_config()

        if os.path.exists(self.config_file):
            try:
                with open(self.config_file) as f:
                    config = json.load(f)

                # Ensure top-level keys exist
                for key in default:
                    if key not in config:
                        config[key] = default[key]

                # Migrate old single-source → multi-source
                if not config.get("sources") and config.get("xtream", {}).get("host"):
                    filters = config.get("filters", {})
                    if "groups" in filters and isinstance(filters["groups"], list):
                        old_groups = filters.get("groups", [])
                        old_channels = filters.get("channels", [])
                        filters = {
                            "live": {"groups": list(old_groups), "channels": list(old_channels)},
                            "vod": {"groups": list(old_groups), "channels": list(old_channels)},
                            "series": {"groups": list(old_groups), "channels": list(old_channels)},
                        }

                    config["sources"] = [
                        {
                            "id": str(uuid.uuid4())[:8],
                            "name": "Default",
                            "host": config["xtream"]["host"],
                            "username": config["xtream"]["username"],
                            "password": config["xtream"]["password"],
                            "enabled": True,
                            "prefix": "",
                            "filters": filters,
                        }
                    ]
                    logger.info("Migrated legacy single-source config to multi-source format")

                # Ensure every source has required fields
                default_filters = self._default_filters()
                for source in config.get("sources", []):
                    source.setdefault("filters", dict(default_filters))
                    source.setdefault("enabled", True)
                    source.setdefault("prefix", "")
                    for cat in ("live", "vod", "series"):
                        source["filters"].setdefault(cat, {"groups": [], "channels": []})
                        source["filters"][cat].setdefault("groups", [])
                        source["filters"][cat].setdefault("channels", [])

                config.setdefault("content_types", {"live": True, "vod": True, "series": True})

                self._config = config
                return self._config

            except (OSError, json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error loading config: {e}")

        self._config = default
        return self._config

    def reload(self) -> dict:
        """Alias for ``load()``."""
        return self.load()

    def save(self, config: dict | None = None) -> None:
        """Persist the config to disk."""
        if config is not None:
            self._config = config
        os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
        with open(self.config_file, "w") as f:
            json.dump(self._config, f, indent=2)

    @property
    def config(self) -> dict:
        return self._config

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def get_sources(self) -> list[dict]:
        return self._config.get("sources", [])

    def get_enabled_sources(self) -> list[dict]:
        return [s for s in self.get_sources() if s.get("enabled", True)]

    def get_source_by_id(self, source_id: str) -> dict | None:
        for source in self.get_sources():
            if source.get("id") == source_id:
                return source
        # Legacy fallback
        xtream = self._config.get("xtream", {})
        if xtream.get("host"):
            return {
                "id": "default",
                "host": xtream["host"],
                "username": xtream["username"],
                "password": xtream["password"],
            }
        return None

    def get_source_by_index(self, index: int) -> dict | None:
        enabled = self.get_enabled_sources()
        if 0 <= index < len(enabled):
            return enabled[index]
        return None

    def get_source_index(self, source_id: str) -> int:
        for i, source in enumerate(self.get_enabled_sources()):
            if source.get("id") == source_id:
                return i
        return 0

    def get_source_by_route(self, route_name: str) -> dict | None:
        for source in self.get_sources():
            if source.get("route") == route_name and source.get("enabled", True):
                return source
        return None

    def get_proxy_enabled(self) -> bool:
        return self._config.get("options", {}).get("proxy_streams", True)

    proxy_enabled = property(get_proxy_enabled)

    def get_cache_ttl(self) -> int:
        return self._config.get("options", {}).get("cache_ttl", 3600)

    def get_epg_cache_ttl(self) -> int:
        return self._config.get("options", {}).get("epg_cache_ttl", 21600)

    def get_refresh_interval(self) -> int:
        interval = self._config.get("options", {}).get("refresh_interval", 3600)
        return max(interval, 300)

    refresh_interval = property(get_refresh_interval)

    def get_telegram_config(self) -> dict:
        return self._config.get("options", {}).get(
            "telegram", {"enabled": False, "bot_token": "", "chat_id": ""}
        )

    def get_download_path(self) -> str:
        return self._config.get("options", {}).get("download_path", "/data/downloads")

    download_path = property(get_download_path)

    def get_download_temp_path(self) -> str:
        return self._config.get("options", {}).get("download_temp_path", "/data/downloads/.tmp")

    download_temp_path = property(get_download_temp_path)

    def get_download_throttle_settings(self) -> dict:
        opts = self._config.get("options", {})
        return {
            "bandwidth_limit": opts.get("download_bandwidth_limit", 0),
            "pause_interval": opts.get("download_pause_interval", 0),
            "pause_duration": opts.get("download_pause_duration", 0),
            "player_profile": opts.get("download_player_profile", "tivimate"),
            "burst_reconnect": opts.get("download_burst_reconnect", 0),
        }

    download_throttle = property(get_download_throttle_settings)
