"""Cache service — in-memory API cache with disk persistence and stream-source mapping."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

import httpx

if TYPE_CHECKING:
    from app.services.config_service import ConfigService

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


class CacheService:
    """Manages the in-memory API cache, disk persistence, and stream-source mapping."""

    def __init__(self, config_service: "ConfigService", http_client: "HttpClientService | None" = None):
        self.config_service = config_service
        self.http_client = http_client
        self.data_dir = config_service.data_dir
        self.api_cache_file = os.path.join(self.data_dir, "api_cache.json")
        self.progress_file = os.path.join(self.data_dir, "refresh_progress.json")

        self._api_cache: dict[str, Any] = {
            "sources": {},
            "last_refresh": None,
            "refresh_in_progress": False,
            "refresh_progress": {
                "current_source": 0,
                "total_sources": 0,
                "current_source_name": "",
                "current_step": "",
                "percent": 0,
            },
        }
        self._cache_lock = asyncio.Lock()

        self._stream_source_map: dict[str, dict[str, str]] = {
            "live": {},
            "vod": {},
            "series": {},
        }
        self._stream_map_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Progress file helpers
    # ------------------------------------------------------------------

    def save_refresh_progress(self, progress_data: dict) -> None:
        try:
            os.makedirs(os.path.dirname(self.progress_file), exist_ok=True)
            with open(self.progress_file, "w") as f:
                json.dump(progress_data, f)
        except Exception as e:
            logger.warning(f"Failed to save progress: {e}")

    def load_refresh_progress(self) -> dict:
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file) as f:
                    return json.load(f)
        except Exception:
            pass
        return {
            "in_progress": False,
            "current_source": 0,
            "total_sources": 0,
            "current_source_name": "",
            "current_step": "",
            "percent": 0,
            "started_at": None,
        }

    def clear_refresh_progress(self) -> None:
        try:
            if os.path.exists(self.progress_file):
                os.remove(self.progress_file)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Disk persistence
    # ------------------------------------------------------------------

    def load_cache_from_disk(self) -> None:
        if os.path.exists(self.api_cache_file):
            try:
                with open(self.api_cache_file) as f:
                    data = json.load(f)
                if "sources" not in data and "live_streams" in data:
                    logger.info("Migrating old cache format to multi-source format")
                    self._api_cache = {
                        "sources": {},
                        "last_refresh": data.get("last_refresh"),
                        "refresh_in_progress": False,
                    }
                else:
                    self._api_cache.update(data)
                    self._api_cache["refresh_in_progress"] = False
                self._rebuild_stream_source_map_sync()
                logger.info(f"Loaded cache from disk. Last refresh: {self._api_cache.get('last_refresh', 'Never')}")
            except Exception as e:
                logger.error(f"Failed to load cache from disk: {e}")

    def save_cache_to_disk(self) -> None:
        try:
            data = {k: v for k, v in self._api_cache.items() if k != "refresh_in_progress"}
            os.makedirs(os.path.dirname(self.api_cache_file), exist_ok=True)
            with open(self.api_cache_file, "w") as f:
                json.dump(data, f)
            logger.info(f"Cache saved to disk at {datetime.now().isoformat()}")
        except Exception as e:
            logger.error(f"Failed to save cache to disk: {e}")

    # ------------------------------------------------------------------
    # Stream source map
    # ------------------------------------------------------------------

    def _rebuild_stream_source_map_sync(self) -> None:
        sources = self._api_cache.get("sources", {})
        new_map: dict[str, dict[str, str]] = {"live": {}, "vod": {}, "series": {}}
        for source_id, source_cache in sources.items():
            for stream in source_cache.get("live_streams", []):
                sid = str(stream.get("stream_id", ""))
                if sid:
                    new_map["live"][sid] = source_id
            for stream in source_cache.get("vod_streams", []):
                sid = str(stream.get("stream_id", ""))
                if sid:
                    new_map["vod"][sid] = source_id
            for series in source_cache.get("series", []):
                sid = str(series.get("series_id", ""))
                if sid:
                    new_map["series"][sid] = source_id
        self._stream_source_map = new_map
        logger.info(
            f"Rebuilt stream-source map: {len(new_map['live'])} live, "
            f"{len(new_map['vod'])} vod, {len(new_map['series'])} series"
        )

    async def rebuild_stream_source_map(self) -> None:
        async with self._cache_lock:
            sources = self._api_cache.get("sources", {})
        new_map: dict[str, dict[str, str]] = {"live": {}, "vod": {}, "series": {}}
        for source_id, source_cache in sources.items():
            for stream in source_cache.get("live_streams", []):
                sid = str(stream.get("stream_id", ""))
                if sid:
                    new_map["live"][sid] = source_id
            for stream in source_cache.get("vod_streams", []):
                sid = str(stream.get("stream_id", ""))
                if sid:
                    new_map["vod"][sid] = source_id
            for series in source_cache.get("series", []):
                sid = str(series.get("series_id", ""))
                if sid:
                    new_map["series"][sid] = source_id
        async with self._stream_map_lock:
            self._stream_source_map = new_map
        logger.info(
            f"Rebuilt stream-source map: {len(new_map['live'])} live, "
            f"{len(new_map['vod'])} vod, {len(new_map['series'])} series"
        )

    # ------------------------------------------------------------------
    # Cache validity
    # ------------------------------------------------------------------

    def is_cache_valid(self) -> bool:
        last_refresh = self._api_cache.get("last_refresh")
        if not last_refresh:
            return False
        try:
            last_time = datetime.fromisoformat(last_refresh)
            age = (datetime.now() - last_time).total_seconds()
            return age < self.config_service.get_cache_ttl()
        except (ValueError, TypeError):
            return False

    # ------------------------------------------------------------------
    # Data accessors
    # ------------------------------------------------------------------

    def get_cached(self, key: str, source_id: str | None = None) -> list:
        sources = self._api_cache.get("sources", {})
        if source_id is not None:
            return sources.get(source_id, {}).get(key, [])
        result: list = []
        for src_cache in sources.values():
            result.extend(src_cache.get(key, []))
        return result

    def get_cached_with_source_info(self, key: str, category_key: str) -> tuple[list, list]:
        sources = self._api_cache.get("sources", {})
        config = self.config_service.config
        source_names: dict[str, str] = {}
        for src in config.get("sources", []):
            source_names[src.get("id")] = src.get("name", "Unknown")

        result: list = []
        all_categories: list = []
        for src_id, src_cache in sources.items():
            src_name = source_names.get(src_id, "Unknown")
            for item in src_cache.get(key, []):
                item_copy = dict(item)
                item_copy["_source_id"] = src_id
                item_copy["_source_name"] = src_name
                result.append(item_copy)
            all_categories.extend(src_cache.get(category_key, []))
        return result, all_categories

    def get_source_for_stream(self, stream_id: str, stream_type: str = "live") -> str | None:
        return self._stream_source_map.get(stream_type, {}).get(str(stream_id))

    def get_source_credentials_for_stream(
        self, stream_id: str, stream_type: str = "live"
    ) -> tuple[str, str, str]:
        source_id = self.get_source_for_stream(stream_id, stream_type)
        if source_id:
            source = self.config_service.get_source_by_id(source_id)
            if source:
                return (
                    source.get("host", "").rstrip("/"),
                    source.get("username", ""),
                    source.get("password", ""),
                )
        config = self.config_service.config
        sources = config.get("sources", [])
        if sources:
            source = sources[0]
            return (
                source.get("host", "").rstrip("/"),
                source.get("username", ""),
                source.get("password", ""),
            )
        xtream = config.get("xtream", {})
        return (
            xtream.get("host", "").rstrip("/"),
            xtream.get("username", ""),
            xtream.get("password", ""),
        )

    # ------------------------------------------------------------------
    # Upstream fetching
    # ------------------------------------------------------------------

    async def fetch_from_upstream(
        self,
        host: str,
        username: str,
        password: str,
        action: str,
        retries: int = 2,
    ) -> Any:
        url = f"{host}/player_api.php"
        params = {"username": username, "password": password, "action": action}
        for attempt in range(retries + 1):
            try:
                async with httpx.AsyncClient(
                    headers=HEADERS,
                    timeout=httpx.Timeout(connect=30.0, read=600.0, write=30.0, pool=30.0),
                    follow_redirects=True,
                ) as client:
                    start_time = time.time()
                    response = await client.get(url, params=params)
                    elapsed = time.time() - start_time
                    if response.status_code == 200:
                        data = response.json()
                        logger.debug(
                            f"Fetched {action}: {len(data) if isinstance(data, list) else 'ok'} items in {elapsed:.1f}s"
                        )
                        return data
                    else:
                        logger.warning(f"Fetch {action} failed with status {response.status_code} in {elapsed:.1f}s")
            except httpx.TimeoutException:
                logger.error(f"Timeout fetching {action} (attempt {attempt + 1}/{retries + 1})")
            except httpx.RemoteProtocolError as e:
                logger.error(f"Protocol error fetching {action}: {e} (attempt {attempt + 1}/{retries + 1})")
            except httpx.ReadError as e:
                logger.error(f"Read error fetching {action}: {e} (attempt {attempt + 1}/{retries + 1})")
            except httpx.ConnectError as e:
                logger.error(f"Connection error fetching {action}: {e}")
                break
            except Exception as e:
                logger.error(f"Error fetching {action}: {e}")
                break
            if attempt < retries:
                await asyncio.sleep(2**attempt)
        return None

    # ------------------------------------------------------------------
    # Full cache refresh
    # ------------------------------------------------------------------

    async def refresh_cache(self, on_cache_refreshed=None) -> bool:
        """Refresh all cached data from all configured sources.

        *on_cache_refreshed* is an optional async callback invoked after a
        successful data fetch (used to refresh pattern categories, etc.).
        """
        existing_progress = self.load_refresh_progress()
        if existing_progress.get("in_progress"):
            started_at = existing_progress.get("started_at")
            if started_at:
                try:
                    started_time = datetime.fromisoformat(started_at)
                    if (datetime.now() - started_time).total_seconds() < 600:
                        logger.info("Refresh already in progress, skipping")
                        return False
                except (ValueError, TypeError):
                    pass

        progress: dict[str, Any] = {
            "in_progress": True,
            "current_source": 0,
            "total_sources": 0,
            "current_source_name": "",
            "current_step": "Initializing...",
            "percent": 0,
            "started_at": datetime.now().isoformat(),
        }
        self.save_refresh_progress(progress)

        async with self._cache_lock:
            self._api_cache["refresh_in_progress"] = True

        config = self.config_service.config
        sources = config.get("sources", [])

        # Backward compat
        if not sources and config.get("xtream", {}).get("host"):
            sources = [
                {
                    "id": "default",
                    "name": "Default",
                    "host": config["xtream"]["host"],
                    "username": config["xtream"]["username"],
                    "password": config["xtream"]["password"],
                    "enabled": True,
                    "prefix": "",
                    "filters": config.get("filters", {}),
                }
            ]

        enabled_sources = [
            s
            for s in sources
            if s.get("enabled", True) and s.get("host") and s.get("username") and s.get("password")
        ]

        if not enabled_sources:
            logger.info("Cannot refresh - no valid sources configured")
            async with self._cache_lock:
                self._api_cache["refresh_in_progress"] = False
            self.clear_refresh_progress()
            return False

        total_sources = len(enabled_sources)
        progress["total_sources"] = total_sources
        self.save_refresh_progress(progress)

        logger.info(f"Starting full refresh at {datetime.now().isoformat()} for {total_sources} source(s)")

        new_sources_cache: dict[str, dict] = {}
        total_stats = {"live_cats": 0, "live_streams": 0, "vod_cats": 0, "vod_streams": 0, "series_cats": 0, "series": 0}

        try:
            for source_idx, source in enumerate(enabled_sources):
                source_id = source.get("id", "default")
                source_name = source.get("name", source_id)
                host = source.get("host", "").rstrip("/")
                username = source.get("username", "")
                password = source.get("password", "")

                progress = self.load_refresh_progress()
                progress["current_source"] = source_idx + 1
                progress["current_source_name"] = source_name
                self.save_refresh_progress(progress)

                logger.info(f"Refreshing source: {source_name}")

                try:

                    def update_step(step_name: str, step_num: int) -> None:
                        p = self.load_refresh_progress()
                        p["current_step"] = f"{source_name}: {step_name}"
                        base_percent = (source_idx / total_sources) * 100
                        step_percent = (step_num / 6) * (100 / total_sources)
                        p["percent"] = int(base_percent + step_percent)
                        self.save_refresh_progress(p)
                        logger.info(f"[{source_name}] Step {step_num}/6: {step_name} (progress: {p['percent']}%)")

                    update_step("Live categories", 0)
                    live_cats = await self.fetch_from_upstream(host, username, password, "get_live_categories") or []

                    update_step("VOD categories", 1)
                    vod_cats = await self.fetch_from_upstream(host, username, password, "get_vod_categories") or []

                    update_step("Series categories", 2)
                    series_cats = await self.fetch_from_upstream(host, username, password, "get_series_categories") or []

                    update_step("Live streams", 3)
                    live_streams = await self.fetch_from_upstream(host, username, password, "get_live_streams") or []

                    update_step("VOD streams", 4)
                    vod_streams = await self.fetch_from_upstream(host, username, password, "get_vod_streams") or []

                    update_step("Series", 5)
                    series = await self.fetch_from_upstream(host, username, password, "get_series") or []

                    new_sources_cache[source_id] = {
                        "live_categories": live_cats,
                        "vod_categories": vod_cats,
                        "series_categories": series_cats,
                        "live_streams": live_streams,
                        "vod_streams": vod_streams,
                        "series": series,
                        "last_refresh": datetime.now().isoformat(),
                    }
                    total_stats["live_cats"] += len(live_cats)
                    total_stats["live_streams"] += len(live_streams)
                    total_stats["vod_cats"] += len(vod_cats)
                    total_stats["vod_streams"] += len(vod_streams)
                    total_stats["series_cats"] += len(series_cats)
                    total_stats["series"] += len(series)

                except Exception as e:
                    logger.error(f"Failed to refresh source {source_id}: {e}")

            if new_sources_cache:
                async with self._cache_lock:
                    self._api_cache["sources"] = new_sources_cache
                    self._api_cache["last_refresh"] = datetime.now().isoformat()
                    self._api_cache["refresh_in_progress"] = False
                await self.rebuild_stream_source_map()
                self.save_cache_to_disk()

                self.save_refresh_progress(
                    {
                        "in_progress": True,
                        "current_source": total_sources,
                        "total_sources": total_sources,
                        "current_source_name": "Categories",
                        "current_step": "Refreshing automatic categories...",
                        "percent": 95,
                    }
                )
                if on_cache_refreshed:
                    await on_cache_refreshed()

                logger.info(
                    f"Refresh complete. Total: {total_stats['live_streams']} live, "
                    f"{total_stats['vod_streams']} vod, {total_stats['series']} series"
                )
            else:
                logger.warning("Refresh completed but no data was fetched from any source")
                async with self._cache_lock:
                    self._api_cache["refresh_in_progress"] = False
        finally:
            self.save_refresh_progress(
                {
                    "in_progress": False,
                    "current_source": total_sources,
                    "total_sources": total_sources,
                    "current_source_name": "",
                    "current_step": "Complete",
                    "percent": 100,
                }
            )
        return True

    # ------------------------------------------------------------------
    # Clear
    # ------------------------------------------------------------------

    async def clear_cache(self) -> None:
        async with self._cache_lock:
            self._api_cache = {"sources": {}, "last_refresh": None, "refresh_in_progress": False}
        async with self._stream_map_lock:
            self._stream_source_map = {"live": {}, "vod": {}, "series": {}}
        if os.path.exists(self.api_cache_file):
            os.remove(self.api_cache_file)
