"""Monitor service — series monitoring and new-episode detection."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from rapidfuzz import fuzz

from app.services.filter_service import normalize_name

if TYPE_CHECKING:
    from app.services.cache_service import CacheService
    from app.services.cart_service import CartService
    from app.services.config_service import ConfigService
    from app.services.notification_service import NotificationService
    from app.services.xtream_service import XtreamService

logger = logging.getLogger(__name__)


def _safe_episode_num(val) -> int:
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


class MonitorService:
    """Manages monitored series and checks for new episodes."""

    def __init__(
        self,
        config_service: "ConfigService",
        cache_service: "CacheService",
        xtream_service: "XtreamService",
        notification_service: "NotificationService",
        cart_service: "CartService",
    ):
        self._cfg = config_service
        self.cache_service = cache_service
        self.cart_service = cart_service
        self.notification_service = notification_service
        self.xtream_service = xtream_service
        self.monitored_file = os.path.join(config_service.data_dir, "monitored_series.json")
        self._monitored_series: list[dict] = []

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def series(self) -> list[dict]:
        return self._monitored_series

    @series.setter
    def series(self, value: list[dict]):
        self._monitored_series = value

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load_monitored(self) -> list:
        if os.path.exists(self.monitored_file):
            try:
                with open(self.monitored_file) as f:
                    data = json.load(f)
                self._monitored_series = data.get("series", [])
                return self._monitored_series
            except (OSError, json.JSONDecodeError) as e:
                logger.error(f"Error loading monitored series: {e}")
        self._monitored_series = []
        return self._monitored_series

    def save_monitored(self, items: list | None = None) -> None:
        if items is not None:
            self._monitored_series = items
        os.makedirs(os.path.dirname(self.monitored_file), exist_ok=True)
        with open(self.monitored_file, "w") as f:
            json.dump({"series": self._monitored_series}, f, indent=2)

    # ------------------------------------------------------------------
    # Cross-source lookup
    # ------------------------------------------------------------------

    def find_series_across_sources(self, series_name: str, exclude_source_id: str | None = None) -> list[dict]:
        """Find a series across all enabled sources by fuzzy name matching."""
        config = self._cfg.config
        enabled_sources = {s["id"]: s for s in config.get("sources", []) if s.get("enabled", True)}
        target_normalized = normalize_name(series_name)
        if not target_normalized:
            return []

        matches: list[dict] = []
        series_list, _ = self.cache_service.get_cached_with_source_info("series", "series_categories")

        for s in series_list:
            src_id = s.get("_source_id", "")
            if src_id not in enabled_sources:
                continue
            if exclude_source_id and src_id == exclude_source_id:
                continue
            s_name = s.get("name", "")
            s_normalized = normalize_name(s_name)
            if not s_normalized:
                continue
            score = fuzz.token_sort_ratio(target_normalized, s_normalized)
            if score >= 90:
                matches.append({
                    "source_id": src_id,
                    "series_id": str(s.get("series_id", "")),
                    "name": s_name,
                    "cover": s.get("cover", "") or s.get("stream_icon", ""),
                    "score": score,
                })

        source_order = {s["id"]: i for i, s in enumerate(config.get("sources", []))}
        matches.sort(key=lambda m: (-m["score"], source_order.get(m["source_id"], 999)))
        return matches

    # ------------------------------------------------------------------
    # Check logic
    # ------------------------------------------------------------------

    async def check_monitored_series(self) -> None:
        """Check all enabled monitored series for new episodes (called after cache refresh)."""
        if not self._monitored_series:
            return

        enabled = [m for m in self._monitored_series if m.get("enabled", True)]
        if not enabled:
            return

        logger.info(f"Series monitoring: checking {len(enabled)} monitored series...")
        all_notifications: list[tuple] = []
        any_queued = False

        for entry in enabled:
            try:
                new_eps = await self._check_single_monitored(entry)
                if new_eps:
                    action = entry.get("action", "both")
                    all_notifications.append((
                        entry.get("series_name", "Unknown"),
                        new_eps,
                        entry.get("cover", ""),
                        action,
                    ))
                    if action in ("download", "both"):
                        any_queued = True
            except Exception as e:
                logger.error(f"Error checking monitored series '{entry.get('series_name', '?')}': {e}")
            await asyncio.sleep(1)

        self.save_monitored()

        for series_name, new_eps, cover, action in all_notifications:
            if action in ("notify", "both"):
                await self.notification_service.send_monitor_notification(series_name, new_eps, cover, action)

        if any_queued:
            queued = [i for i in self.cart_service.cart if i.get("status") == "queued"]
            if queued and (self.cart_service.download_task is None or self.cart_service.download_task.done()):
                download_path = self._cfg.download_path
                os.makedirs(download_path, exist_ok=True)
                self.cart_service.download_task = asyncio.create_task(self.cart_service.download_worker())
                logger.info(f"Series monitoring: auto-started download worker for {len(queued)} queued items")

        total_new = sum(len(eps) for _, eps, _, _ in all_notifications)
        logger.info(f"Series monitoring: check complete. {total_new} new episodes found.")

    async def _check_single_monitored(self, entry: dict) -> list[dict]:
        """Check one monitored series entry. Returns new episode dicts."""
        series_name = entry.get("series_name", "")
        source_id = entry.get("source_id")
        series_id = entry.get("series_id", "")
        scope = entry.get("scope", "new_only")
        season_filter = entry.get("season_filter")

        sources_to_try: list[dict] = []
        if source_id:
            sources_to_try = [{"source_id": source_id, "series_id": series_id}]
        else:
            matches = self.find_series_across_sources(series_name)
            if matches:
                sources_to_try = [{"source_id": m["source_id"], "series_id": m["series_id"]} for m in matches]
            else:
                logger.warning(f"Series monitoring: '{series_name}' not found in any source")
                entry["last_checked"] = datetime.now().isoformat()
                entry["last_new_count"] = 0
                return []

        episodes: list = []
        used_source_id: Optional[str] = None
        for src in sources_to_try:
            eps = await self.xtream_service.fetch_series_episodes(src["source_id"], src["series_id"])
            if eps:
                episodes = eps
                used_source_id = src["source_id"]
                break
            await asyncio.sleep(0.5)

        if not episodes:
            entry["last_checked"] = datetime.now().isoformat()
            entry["last_new_count"] = 0
            return []

        if scope == "season" and season_filter:
            episodes = [ep for ep in episodes if str(ep.get("season")) == str(season_filter)]

        known_keys = {
            (str(k.get("season", "")), _safe_episode_num(k.get("episode_num")))
            for k in entry.get("known_episodes", [])
        }
        downloaded_keys = {
            (str(k.get("season", "")), _safe_episode_num(k.get("episode_num")))
            for k in entry.get("downloaded_episodes", [])
        }
        already_seen = known_keys | downloaded_keys
        action = entry.get("action", "both")
        should_download = action in ("download", "both")

        new_episodes: list[dict] = []
        for ep in episodes:
            ep_key = (str(ep.get("season", "")), _safe_episode_num(ep.get("episode_num")))
            if ep_key in already_seen:
                continue

            stream_id = str(ep.get("stream_id", ""))
            if should_download and any(
                i.get("source_id") == used_source_id
                and i.get("stream_id") == stream_id
                and i.get("status") in ("queued", "downloading")
                for i in self.cart_service.cart
            ):
                continue

            cart_item = {
                "id": str(uuid.uuid4()),
                "stream_id": stream_id,
                "source_id": used_source_id,
                "content_type": "series",
                "name": ep.get("title", "") or f"Episode {ep.get('episode_num', '')}",
                "series_name": ep.get("series_name", series_name),
                "season": ep.get("season"),
                "episode_num": ep.get("episode_num", 0),
                "episode_title": ep.get("title", ""),
                "icon": entry.get("cover", ""),
                "group": "",
                "container_extension": ep.get("container_extension", "mp4"),
                "added_at": datetime.now().isoformat(),
                "status": "queued",
                "progress": 0,
                "error": None,
                "file_path": None,
                "file_size": None,
            }

            if should_download:
                filepath = self.cart_service.build_download_filepath(cart_item)
                if os.path.exists(filepath):
                    entry.setdefault("downloaded_episodes", []).append({
                        "stream_id": stream_id,
                        "source_id": used_source_id,
                        "season": ep.get("season"),
                        "episode_num": ep.get("episode_num", 0),
                    })
                    continue
                self.cart_service.cart.append(cart_item)

            new_episodes.append(cart_item)
            entry.setdefault("downloaded_episodes", []).append({
                "stream_id": stream_id,
                "source_id": used_source_id,
                "season": ep.get("season"),
                "episode_num": ep.get("episode_num", 0),
            })

        if new_episodes and should_download:
            self.cart_service.save_cart()
            logger.info(f"Series monitoring: '{series_name}' — {len(new_episodes)} new episodes queued")
        elif new_episodes:
            logger.info(f"Series monitoring: '{series_name}' — {len(new_episodes)} new episodes detected (notify only)")

        entry["last_checked"] = datetime.now().isoformat()
        entry["last_new_count"] = len(new_episodes)
        return new_episodes

    # ------------------------------------------------------------------
    # Preview
    # ------------------------------------------------------------------

    async def preview_episodes(self, monitor_id: str) -> dict | None:
        """Get current vs known episode comparison for a monitored entry."""
        entry = None
        for m in self._monitored_series:
            if m.get("id") == monitor_id:
                entry = m
                break
        if not entry:
            return None

        source_id = entry.get("source_id")
        series_id = entry.get("series_id", "")

        if source_id:
            episodes = await self.xtream_service.fetch_series_episodes(source_id, series_id)
        else:
            matches = self.find_series_across_sources(entry.get("series_name", ""))
            episodes = []
            for m in matches:
                eps = await self.xtream_service.fetch_series_episodes(m["source_id"], m["series_id"])
                if eps:
                    episodes = eps
                    break

        known_keys = {
            (str(k.get("season", "")), _safe_episode_num(k.get("episode_num")))
            for k in entry.get("known_episodes", [])
        }
        downloaded_keys = {
            (str(k.get("season", "")), _safe_episode_num(k.get("episode_num")))
            for k in entry.get("downloaded_episodes", [])
        }

        for ep in episodes:
            key = (str(ep.get("season", "")), _safe_episode_num(ep.get("episode_num")))
            ep["is_known"] = key in known_keys
            ep["is_downloaded"] = key in downloaded_keys
            ep["is_new"] = key not in (known_keys | downloaded_keys)

        return {
            "entry": entry,
            "episodes": episodes,
            "known_count": len(known_keys),
            "downloaded_count": len(downloaded_keys),
            "new_count": sum(1 for ep in episodes if ep.get("is_new")),
        }
