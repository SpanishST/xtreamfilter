"""Category service — CRUD and pattern-refresh for custom categories."""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import TYPE_CHECKING

from app.models.xtream import CUSTOM_CAT_ID_BASE, ICON_EMOJI_MAP
from app.services.filter_service import (
    build_category_map,
    matches_filter,
    should_include,
)

if TYPE_CHECKING:
    from app.services.cache_service import CacheService
    from app.services.config_service import ConfigService
    from app.services.notification_service import NotificationService

logger = logging.getLogger(__name__)


class CategoryService:
    """Manages custom categories (manual + automatic/pattern-based)."""

    def __init__(
        self,
        config_service: "ConfigService",
        cache_service: "CacheService",
        notification_service: "NotificationService",
    ):
        self.config_service = config_service
        self.cache_service = cache_service
        self.notification_service = notification_service
        self.data_dir = config_service.data_dir
        self.categories_file = os.path.join(self.data_dir, "categories.json")

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load_categories(self) -> dict:
        default_categories = {
            "categories": [
                {
                    "id": "favorites",
                    "name": "Favorite streams",
                    "icon": "❤️",
                    "mode": "manual",
                    "content_types": ["live", "vod", "series"],
                    "items": [],
                    "patterns": [],
                    "pattern_logic": "or",
                    "use_source_filters": False,
                    "cached_items": [],
                    "last_refresh": None,
                }
            ]
        }

        if os.path.exists(self.categories_file):
            try:
                with open(self.categories_file) as f:
                    data = json.load(f)
                if "categories" in data:
                    return data
            except (OSError, json.JSONDecodeError) as e:
                logger.error(f"Error loading categories: {e}")

        # Check for old favorites.json to migrate
        old_favorites_file = os.path.join(self.data_dir, "favorites.json")
        if os.path.exists(old_favorites_file):
            try:
                with open(old_favorites_file) as f:
                    old_favorites = json.load(f)
                migrated_items: list[dict] = []
                for content_type in ("live", "vod", "series"):
                    for fav in old_favorites.get(content_type, []):
                        migrated_items.append(
                            {
                                "id": fav.get("id"),
                                "source_id": fav.get("source_id"),
                                "content_type": content_type,
                                "added_at": fav.get("added_at", datetime.now().isoformat()),
                            }
                        )
                if migrated_items:
                    default_categories["categories"][0]["items"] = migrated_items
                    logger.info(f"Migrated {len(migrated_items)} items from favorites.json")
                self.save_categories(default_categories)
                os.rename(old_favorites_file, old_favorites_file + ".bak")
                logger.info("Renamed old favorites.json to favorites.json.bak")
            except (OSError, json.JSONDecodeError) as e:
                logger.error(f"Error migrating favorites: {e}")

        return default_categories

    def save_categories(self, data: dict) -> None:
        os.makedirs(os.path.dirname(self.categories_file), exist_ok=True)
        with open(self.categories_file, "w") as f:
            json.dump(data, f, indent=2)

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    def get_category_by_id(self, category_id: str) -> dict | None:
        data = self.load_categories()
        for cat in data.get("categories", []):
            if cat.get("id") == category_id:
                return cat
        return None

    def is_in_category(self, category_id: str, content_type: str, item_id: str, source_id: str) -> bool:
        cat = self.get_category_by_id(category_id)
        if not cat:
            return False
        if cat.get("mode") == "manual":
            for item in cat.get("items", []):
                if (
                    item.get("id") == item_id
                    and item.get("source_id") == source_id
                    and item.get("content_type") == content_type
                ):
                    return True
        return False

    def get_item_categories(self, content_type: str, item_id: str, source_id: str) -> list[str]:
        data = self.load_categories()
        result: list[str] = []
        for cat in data.get("categories", []):
            if cat.get("mode") == "manual":
                for item in cat.get("items", []):
                    if (
                        item.get("id") == item_id
                        and item.get("source_id") == source_id
                        and item.get("content_type") == content_type
                    ):
                        result.append(cat.get("id"))
                        break
        return result

    def get_item_details_from_cache(self, item_id: str, source_id: str, content_type: str) -> dict:
        if content_type == "live":
            streams, _ = self.cache_service.get_cached_with_source_info("live_streams", "live_categories")
            id_field = "stream_id"
        elif content_type == "vod":
            streams, _ = self.cache_service.get_cached_with_source_info("vod_streams", "vod_categories")
            id_field = "stream_id"
        elif content_type == "series":
            streams, _ = self.cache_service.get_cached_with_source_info("series", "series_categories")
            id_field = "series_id"
        else:
            return {}
        for stream in streams:
            if str(stream.get(id_field, "")) == item_id and stream.get("_source_id", "") == source_id:
                return {"name": stream.get("name", "Unknown"), "cover": stream.get("stream_icon", "") or stream.get("cover", "")}
        return {}

    # ------------------------------------------------------------------
    # Xtream virtual category helpers
    # ------------------------------------------------------------------

    def get_custom_cat_id_map(self) -> dict[str, int]:
        data = self.load_categories()
        return {cat.get("id"): CUSTOM_CAT_ID_BASE + idx for idx, cat in enumerate(data.get("categories", []))}

    def custom_cat_id_to_numeric(self, cat_id: str) -> str:
        cat_id_map = self.get_custom_cat_id_map()
        return str(cat_id_map.get(cat_id, CUSTOM_CAT_ID_BASE))

    def get_custom_categories_for_content_type(self, content_type: str) -> list[dict]:
        data = self.load_categories()
        result: list[dict] = []
        for cat in data.get("categories", []):
            if content_type not in cat.get("content_types", []):
                continue
            items = cat.get("items", []) if cat.get("mode") == "manual" else cat.get("cached_items", [])
            filtered_items = [item for item in items if item.get("content_type") == content_type]
            if filtered_items:
                result.append(
                    {"id": cat.get("id"), "name": cat.get("name"), "icon": cat.get("icon", "📁"), "items": filtered_items}
                )
        return result

    def get_custom_category_streams(self, content_type: str, custom_cat_id: str) -> list[tuple[str, str]]:
        data = self.load_categories()
        for cat in data.get("categories", []):
            if cat.get("id") != custom_cat_id:
                continue
            items = cat.get("items", []) if cat.get("mode") == "manual" else cat.get("cached_items", [])
            return [(item.get("source_id"), str(item.get("id"))) for item in items if item.get("content_type") == content_type]
        return []

    @staticmethod
    def get_icon_emoji(icon_name: str) -> str:
        return ICON_EMOJI_MAP.get(icon_name, "")

    # ------------------------------------------------------------------
    # Pattern refresh
    # ------------------------------------------------------------------

    def _refresh_pattern_categories_internal(self) -> list[tuple[str, list]]:
        """Refresh automatic categories and return notifications to send."""
        data = self.load_categories()
        config = self.config_service.config
        sources_config = {s.get("id"): s for s in config.get("sources", [])}
        current_time = int(time.time())
        updated = False
        notifications_to_send: list[tuple[str, list]] = []

        for cat in data.get("categories", []):
            if cat.get("mode") != "automatic":
                continue
            patterns = cat.get("patterns", [])
            recently_added_days = cat.get("recently_added_days", 0)
            if not patterns and recently_added_days <= 0:
                continue
            content_types = cat.get("content_types", ["live", "vod", "series"])
            use_source_filters = cat.get("use_source_filters", False)
            pattern_logic = cat.get("pattern_logic", "or")
            recently_added_cutoff = current_time - (recently_added_days * 86400) if recently_added_days > 0 else 0
            matched_items: list[dict] = []

            for ct in content_types:
                if ct == "live":
                    streams, categories = self.cache_service.get_cached_with_source_info("live_streams", "live_categories")
                elif ct == "vod":
                    streams, categories = self.cache_service.get_cached_with_source_info("vod_streams", "vod_categories")
                elif ct == "series":
                    streams, categories = self.cache_service.get_cached_with_source_info("series", "series_categories")
                else:
                    continue
                cat_map = build_category_map(categories)
                for stream in streams:
                    name = stream.get("name", "")
                    item_id = str(stream.get("stream_id") or stream.get("series_id") or "")
                    src_id = stream.get("_source_id", "")
                    if recently_added_days > 0:
                        added = stream.get("added") or stream.get("last_modified", 0)
                        try:
                            added_ts = int(added) if added else 0
                        except (ValueError, TypeError):
                            added_ts = 0
                        if added_ts < recently_added_cutoff:
                            continue
                    if use_source_filters and src_id in sources_config:
                        source_cfg = sources_config[src_id]
                        content_filters = source_cfg.get("filters", {}).get(ct, {})
                        group_filters = content_filters.get("groups", [])
                        channel_filters = content_filters.get("channels", [])
                        cat_id = str(stream.get("category_id", ""))
                        group_name = cat_map.get(cat_id, "")
                        if group_filters and not should_include(group_name, group_filters):
                            continue
                        if channel_filters and not should_include(name, channel_filters):
                            continue
                    if patterns:
                        if pattern_logic == "and":
                            if not all(matches_filter(name, p) for p in patterns):
                                continue
                        else:
                            if not any(matches_filter(name, p) for p in patterns):
                                continue
                    matched_items.append(
                        {
                            "id": item_id,
                            "source_id": src_id,
                            "content_type": ct,
                            "name": name,
                            "cover": stream.get("stream_icon", "") or stream.get("cover", ""),
                            "tmdb_id": stream.get("tmdb_id") or stream.get("tmdb"),
                            "imdb_id": stream.get("imdb_id") or stream.get("imdb"),
                        }
                    )

            old_items = cat.get("cached_items", [])
            old_keys = {(i.get("id"), i.get("source_id"), i.get("content_type")) for i in old_items}
            new_items = [
                {
                    "name": i["name"],
                    "cover": i["cover"],
                    "source_id": i["source_id"],
                    "tmdb_id": i.get("tmdb_id"),
                    "imdb_id": i.get("imdb_id"),
                }
                for i in matched_items
                if (i["id"], i["source_id"], i["content_type"]) not in old_keys
            ]
            if cat.get("notify_telegram", False) and new_items:
                notifications_to_send.append((cat.get("name", "Unknown Category"), new_items))
            cat["cached_items"] = [{"id": i["id"], "source_id": i["source_id"], "content_type": i["content_type"]} for i in matched_items]
            cat["last_refresh"] = datetime.now().isoformat()
            updated = True
            logger.info(f"Category '{cat.get('name')}' refreshed: {len(matched_items)} items matched, {len(new_items)} new")

        if updated:
            self.save_categories(data)
        return notifications_to_send

    async def refresh_pattern_categories_async(self) -> None:
        notifications = self._refresh_pattern_categories_internal()
        for category_name, new_items in notifications:
            await self.notification_service.send_category_notification(category_name, new_items)

    def refresh_pattern_categories(self) -> None:
        self._refresh_pattern_categories_internal()
