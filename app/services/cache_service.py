"""Cache service — in-memory API cache with disk persistence and stream-source mapping."""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

import httpx

from app.database import DB_NAME, db_connect

if TYPE_CHECKING:
    from app.services.config_service import ConfigService
    from app.services.notification_service import NotificationService

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

REFRESH_STEP_DEFINITIONS: list[tuple[str, str, str]] = [
    ("live_categories", "Live categories", "get_live_categories"),
    ("vod_categories", "VOD categories", "get_vod_categories"),
    ("series_categories", "Series categories", "get_series_categories"),
    ("live_streams", "Live streams", "get_live_streams"),
    ("vod_streams", "VOD streams", "get_vod_streams"),
    ("series", "Series", "get_series"),
]
REFRESH_SOURCE_KEYS = tuple(step[0] for step in REFRESH_STEP_DEFINITIONS)


class CacheService:
    """Manages the in-memory API cache, disk persistence, and stream-source mapping."""

    def __init__(
        self,
        config_service: "ConfigService",
        http_client: "HttpClientService | None" = None,
        notification_service: "NotificationService | None" = None,
    ):
        self.config_service = config_service
        self.http_client = http_client
        self.notification_service = notification_service
        self.data_dir = config_service.data_dir
        self.db_path = os.path.join(self.data_dir, DB_NAME)

        self._api_cache: dict[str, Any] = {
            "sources": {},
            "last_refresh": None,
            "refresh_in_progress": False,
            "refresh_progress": self._default_refresh_progress(),
        }
        self._cache_lock = asyncio.Lock()

        self._stream_source_map: dict[str, dict[str, str]] = {
            "live": {},
            "vod": {},
            "series": {},
        }
        self._stream_map_lock = asyncio.Lock()

        # Fast lookup index: (content_type, source_id, stream_id) → stream dict
        self._stream_index: dict[tuple[str, str, str], dict] = {}
        # Pre-built source names
        self._source_names: dict[str, str] = {}

    @staticmethod
    def _empty_source_cache() -> dict[str, Any]:
        return {
            "live_categories": [],
            "vod_categories": [],
            "series_categories": [],
            "live_streams": [],
            "vod_streams": [],
            "series": [],
            "last_refresh": None,
        }

    @staticmethod
    def _default_refresh_summary(total_sources: int = 0) -> dict[str, Any]:
        return {
            "total_sources": total_sources,
            "successful_sources": 0,
            "partial_sources": 0,
            "failed_sources": 0,
            "pending_sources": 0,
            "running_sources": 0,
            "processed_steps": 0,
            "successful_steps": 0,
            "failed_steps": 0,
            "preserved_steps": 0,
            "total_steps": total_sources * len(REFRESH_STEP_DEFINITIONS),
            "live_categories": 0,
            "vod_categories": 0,
            "series_categories": 0,
            "live_streams": 0,
            "vod_streams": 0,
            "series": 0,
        }

    def _default_refresh_progress(self) -> dict[str, Any]:
        return {
            "in_progress": False,
            "status": "idle",
            "current_source": 0,
            "total_sources": 0,
            "current_source_name": "",
            "current_step": "",
            "percent": 0,
            "started_at": None,
            "finished_at": None,
            "last_error": "",
            "source_results": [],
            "summary": self._default_refresh_summary(),
        }

    def _normalise_refresh_progress(self, progress_data: dict | None = None) -> dict[str, Any]:
        progress = self._default_refresh_progress()
        if not progress_data:
            return progress

        progress["in_progress"] = bool(progress_data.get("in_progress", progress["in_progress"]))
        progress["current_source"] = int(progress_data.get("current_source", progress["current_source"]) or 0)
        progress["total_sources"] = int(progress_data.get("total_sources", progress["total_sources"]) or 0)
        progress["current_source_name"] = str(progress_data.get("current_source_name", progress["current_source_name"]) or "")
        progress["current_step"] = str(progress_data.get("current_step", progress["current_step"]) or "")
        progress["percent"] = int(progress_data.get("percent", progress["percent"]) or 0)
        progress["started_at"] = progress_data.get("started_at")
        progress["finished_at"] = progress_data.get("finished_at")
        progress["last_error"] = str(progress_data.get("last_error", progress["last_error"]) or "")

        status = progress_data.get("status")
        if not status:
            status = "running" if progress["in_progress"] else "idle"
        progress["status"] = str(status)

        source_results = progress_data.get("source_results", progress["source_results"])
        if isinstance(source_results, str):
            try:
                source_results = json.loads(source_results)
            except (TypeError, json.JSONDecodeError):
                source_results = []
        if not isinstance(source_results, list):
            source_results = []
        progress["source_results"] = source_results

        if progress["total_sources"] == 0 and source_results:
            progress["total_sources"] = len(source_results)

        summary = progress_data.get("summary", {})
        if isinstance(summary, str):
            try:
                summary = json.loads(summary)
            except (TypeError, json.JSONDecodeError):
                summary = {}
        if not isinstance(summary, dict):
            summary = {}
        default_summary = self._default_refresh_summary(progress["total_sources"])
        for key in default_summary:
            if key in summary:
                default_summary[key] = summary[key]
        progress["summary"] = default_summary
        return progress

    def _build_source_result(self, source_id: str, source_name: str, last_refresh: str | None) -> dict[str, Any]:
        return {
            "source_id": source_id,
            "source_name": source_name,
            "status": "pending",
            "last_refresh": last_refresh,
            "counts": {key: 0 for key in REFRESH_SOURCE_KEYS},
            "errors": [],
            "steps": [
                {
                    "key": key,
                    "label": label,
                    "status": "pending",
                    "count": 0,
                    "duration_ms": None,
                    "preserved_existing": False,
                    "error": None,
                }
                for key, label, _ in REFRESH_STEP_DEFINITIONS
            ],
        }

    @staticmethod
    def _build_source_counts(source_cache: dict[str, Any]) -> dict[str, int]:
        return {
            key: len(source_cache.get(key, []))
            for key in REFRESH_SOURCE_KEYS
        }

    def _build_refresh_summary(self, source_results: list[dict], total_sources: int) -> dict[str, Any]:
        summary = self._default_refresh_summary(total_sources)
        for source_result in source_results:
            source_status = source_result.get("status", "pending")
            if source_status == "success":
                summary["successful_sources"] += 1
            elif source_status == "partial":
                summary["partial_sources"] += 1
            elif source_status == "failed":
                summary["failed_sources"] += 1
            elif source_status == "running":
                summary["running_sources"] += 1
            else:
                summary["pending_sources"] += 1

            for key, value in source_result.get("counts", {}).items():
                if key in summary:
                    summary[key] += int(value or 0)

            for step in source_result.get("steps", []):
                step_status = step.get("status", "pending")
                if step_status in {"success", "failed"}:
                    summary["processed_steps"] += 1
                if step_status == "success":
                    summary["successful_steps"] += 1
                elif step_status == "failed":
                    summary["failed_steps"] += 1
                    if step.get("preserved_existing"):
                        summary["preserved_steps"] += 1
        return summary

    @staticmethod
    def _derive_source_status(source_result: dict[str, Any]) -> str:
        statuses = {step.get("status", "pending") for step in source_result.get("steps", [])}
        if "running" in statuses:
            return "running"
        has_success = "success" in statuses
        has_failure = "failed" in statuses
        if has_success and has_failure:
            return "partial"
        if has_success:
            return "success"
        if has_failure:
            return "failed"
        return "pending"

    @staticmethod
    def _format_step_error(source_name: str, step_label: str, error: dict[str, Any]) -> str:
        status_code = error.get("status_code")
        if status_code:
            return f"{source_name}: {step_label} failed with HTTP {status_code}"
        message = error.get("message") or error.get("type") or "Unknown error"
        return f"{source_name}: {step_label} failed - {message}"

    # ------------------------------------------------------------------
    # Progress helpers  (SQLite replaces refresh_progress.json)
    # ------------------------------------------------------------------

    def save_refresh_progress(self, progress_data: dict) -> None:
        progress_data = self._normalise_refresh_progress(progress_data)
        conn = db_connect(self.db_path)
        try:
            conn.execute(
                """INSERT OR REPLACE INTO refresh_progress
                   (id, in_progress, current_source, total_sources,
                    current_source_name, current_step, percent, started_at,
                    status, source_results, summary, finished_at, last_error)
                   VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    int(progress_data.get("in_progress", False)),
                    progress_data.get("current_source", 0),
                    progress_data.get("total_sources", 0),
                    progress_data.get("current_source_name", ""),
                    progress_data.get("current_step", ""),
                    progress_data.get("percent", 0),
                    progress_data.get("started_at"),
                    progress_data.get("status", "idle"),
                    json.dumps(progress_data.get("source_results", []), ensure_ascii=False),
                    json.dumps(progress_data.get("summary", {}), ensure_ascii=False),
                    progress_data.get("finished_at"),
                    progress_data.get("last_error", ""),
                ),
            )
            conn.commit()
            self._api_cache["refresh_progress"] = progress_data
        except Exception as e:
            logger.warning(f"Failed to save progress: {e}")
        finally:
            conn.close()

    def load_refresh_progress(self) -> dict:
        conn = db_connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT in_progress, current_source, total_sources, "
                "current_source_name, current_step, percent, started_at, "
                "status, source_results, summary, finished_at, last_error "
                "FROM refresh_progress WHERE id = 1"
            ).fetchone()
            if row:
                return self._normalise_refresh_progress({
                    "in_progress": bool(row["in_progress"]),
                    "current_source": row["current_source"],
                    "total_sources": row["total_sources"],
                    "current_source_name": row["current_source_name"],
                    "current_step": row["current_step"],
                    "percent": row["percent"],
                    "started_at": row["started_at"],
                    "status": row["status"],
                    "source_results": row["source_results"],
                    "summary": row["summary"],
                    "finished_at": row["finished_at"],
                    "last_error": row["last_error"],
                })
        except Exception:
            pass
        finally:
            conn.close()
        return self._default_refresh_progress()

    def clear_refresh_progress(self, status: str = "cancelled", last_error: str = "") -> None:
        progress = self.load_refresh_progress()
        progress["in_progress"] = False
        progress["status"] = status
        progress["current_step"] = "Cancelled" if status == "cancelled" else ""
        progress["current_source_name"] = ""
        progress["percent"] = 0 if status == "cancelled" else progress.get("percent", 0)
        progress["finished_at"] = datetime.now().isoformat()
        progress["last_error"] = last_error or progress.get("last_error", "")
        progress["summary"] = self._build_refresh_summary(
            progress.get("source_results", []),
            progress.get("total_sources", 0),
        )
        self.save_refresh_progress(progress)

    # ------------------------------------------------------------------
    # Disk persistence  (SQLite replaces api_cache.json)
    # ------------------------------------------------------------------

    def load_cache_from_disk(self) -> None:
        conn = db_connect(self.db_path)
        try:
            # Global last_refresh
            meta_row = conn.execute(
                "SELECT last_refresh FROM cache_meta WHERE id = 1"
            ).fetchone()
            if meta_row:
                self._api_cache["last_refresh"] = meta_row["last_refresh"]

            # Per-source categories
            cat_rows = conn.execute(
                "SELECT source_id, content_type, category_id, category_name, data "
                "FROM source_categories ORDER BY source_id, content_type"
            ).fetchall()

            # Per-source streams
            stream_rows = conn.execute(
                "SELECT source_id, content_type, stream_id, data "
                "FROM streams ORDER BY source_id, content_type"
            ).fetchall()

            sources: dict[str, dict] = {}

            CAT_KEY = {
                "live": "live_categories",
                "vod": "vod_categories",
                "series": "series_categories",
            }
            STREAM_KEY = {
                "live": "live_streams",
                "vod": "vod_streams",
                "series": "series",
            }

            for row in cat_rows:
                src = row["source_id"]
                ct = row["content_type"]
                if src not in sources:
                    sources[src] = {
                        "live_categories": [], "vod_categories": [],
                        "series_categories": [], "live_streams": [],
                        "vod_streams": [], "series": [], "last_refresh": None,
                    }
                key = CAT_KEY.get(ct)
                if key:
                    try:
                        sources[src][key].append(json.loads(row["data"]))
                    except (json.JSONDecodeError, TypeError):
                        pass

            for row in stream_rows:
                src = row["source_id"]
                ct = row["content_type"]
                if src not in sources:
                    sources[src] = {
                        "live_categories": [], "vod_categories": [],
                        "series_categories": [], "live_streams": [],
                        "vod_streams": [], "series": [], "last_refresh": None,
                    }
                key = STREAM_KEY.get(ct)
                if key:
                    try:
                        sources[src][key].append(json.loads(row["data"]))
                    except (json.JSONDecodeError, TypeError):
                        pass

            # Per-source last_refresh from separate table
            src_refresh_rows = conn.execute(
                "SELECT source_id, last_refresh FROM source_last_refresh"
            ).fetchall()
            for rr in src_refresh_rows:
                if rr["source_id"] in sources:
                    sources[rr["source_id"]]["last_refresh"] = rr["last_refresh"]

            if sources:
                self._api_cache["sources"] = sources
                self._api_cache["refresh_in_progress"] = False
                self._inject_source_info()
                self._rebuild_stream_source_map_sync()
                self._rebuild_stream_index()
                logger.info(
                    f"Loaded cache from DB. Last refresh: {self._api_cache.get('last_refresh', 'Never')}"
                )
            else:
                logger.info("DB cache is empty — will refresh on first request")
        except Exception as e:
            logger.error(f"Failed to load cache from DB: {e}")
        finally:
            conn.close()

    def save_cache_to_disk(self) -> None:
        conn = db_connect(self.db_path)
        try:
            sources = self._api_cache.get("sources", {})

            # Collect all current source IDs so we can prune stale entries
            active_source_ids = list(sources.keys())

            # Clear stale source data
            if active_source_ids:
                placeholders = ",".join("?" * len(active_source_ids))
                conn.execute(
                    f"DELETE FROM streams WHERE source_id NOT IN ({placeholders})",
                    active_source_ids,
                )
                conn.execute(
                    f"DELETE FROM source_categories WHERE source_id NOT IN ({placeholders})",
                    active_source_ids,
                )
                conn.execute(
                    f"DELETE FROM source_last_refresh WHERE source_id NOT IN ({placeholders})",
                    active_source_ids,
                )
            else:
                conn.execute("DELETE FROM streams")
                conn.execute("DELETE FROM source_categories")
                conn.execute("DELETE FROM source_last_refresh")

            TYPE_MAP: list[tuple[str, str, str]] = [
                ("live_streams", "live", "stream_id"),
                ("vod_streams", "vod", "stream_id"),
                ("series", "series", "series_id"),
            ]
            CAT_MAP: list[tuple[str, str]] = [
                ("live_categories", "live"),
                ("vod_categories", "vod"),
                ("series_categories", "series"),
            ]

            for source_id, src_cache in sources.items():
                # Categories
                cat_rows = []
                for cat_key, ct in CAT_MAP:
                    for cat in src_cache.get(cat_key, []):
                        cat_rows.append((
                            source_id, ct,
                            str(cat.get("category_id", "")),
                            cat.get("category_name", ""),
                            json.dumps(cat),
                        ))
                if cat_rows:
                    conn.executemany(
                        "INSERT OR REPLACE INTO source_categories "
                        "(source_id, content_type, category_id, category_name, data) "
                        "VALUES (?,?,?,?,?)",
                        cat_rows,
                    )

                # Streams
                stream_rows = []
                for list_key, ct, id_field in TYPE_MAP:
                    for stream in src_cache.get(list_key, []):
                        sid = str(stream.get(id_field, ""))
                        if not sid:
                            continue
                        added_raw = stream.get("added") or stream.get("last_modified", 0)
                        try:
                            added = int(added_raw) if added_raw else 0
                        except (ValueError, TypeError):
                            added = 0
                        stream_rows.append((
                            source_id, ct, sid,
                            stream.get("name", ""),
                            str(stream.get("category_id", "")),
                            added,
                            json.dumps(stream),
                        ))
                if stream_rows:
                    conn.executemany(
                        "INSERT OR REPLACE INTO streams "
                        "(source_id, content_type, stream_id, name, category_id, added, data) "
                        "VALUES (?,?,?,?,?,?,?)",
                        stream_rows,
                    )

                # Per-source last_refresh
                src_last = src_cache.get("last_refresh")
                if src_last:
                    conn.execute(
                        "INSERT OR REPLACE INTO source_last_refresh (source_id, last_refresh) VALUES (?,?)",
                        (source_id, src_last),
                    )

            # Global last_refresh
            global_refresh = self._api_cache.get("last_refresh")
            if global_refresh:
                conn.execute(
                    "INSERT OR REPLACE INTO cache_meta (id, last_refresh) VALUES (1, ?)",
                    (global_refresh,),
                )

            conn.commit()
            logger.info(f"Cache saved to DB at {datetime.now().isoformat()}")
        except Exception as e:
            logger.error(f"Failed to save cache to DB: {e}")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Source-info injection & stream index
    # ------------------------------------------------------------------

    def _inject_source_info(self) -> None:
        """Stamp _source_id and _source_name onto every stream dict in‑place.

        This runs once at load/refresh time so that read‑time accessors
        never need to copy dicts.
        """
        source_names: dict[str, str] = {}
        for src in self.config_service.config.get("sources", []):
            source_names[src.get("id")] = src.get("name", "Unknown")
        self._source_names = source_names

        STREAM_KEYS = ("live_streams", "vod_streams", "series")
        sources = self._api_cache.get("sources", {})
        for src_id, src_cache in sources.items():
            src_name = source_names.get(src_id, "Unknown")
            for key in STREAM_KEYS:
                for item in src_cache.get(key, []):
                    item["_source_id"] = src_id
                    item["_source_name"] = src_name

    def _rebuild_stream_index(self) -> None:
        """Build a (content_type, source_id, stream_id) → dict lookup index."""
        idx: dict[tuple[str, str, str], dict] = {}
        TYPE_MAP = {
            "live_streams": ("live", "stream_id"),
            "vod_streams": ("vod", "stream_id"),
            "series": ("series", "series_id"),
        }
        sources = self._api_cache.get("sources", {})
        for src_id, src_cache in sources.items():
            for list_key, (ct, id_field) in TYPE_MAP.items():
                for item in src_cache.get(list_key, []):
                    sid = str(item.get(id_field, ""))
                    if sid:
                        idx[(ct, src_id, sid)] = item
        self._stream_index = idx
        logger.debug(f"Built stream index with {len(idx)} entries")

    def get_streams_by_ids(
        self, content_type: str, id_set: set[tuple[str, str]]
    ) -> list[dict]:
        """Return stream dicts for a set of (stream_id, source_id) pairs.

        Uses the pre-built ``_stream_index`` for O(1) lookups per item,
        avoiding full iteration of all streams.
        """
        result: list[dict] = []
        for stream_id, source_id in id_set:
            item = self._stream_index.get((content_type, source_id, stream_id))
            if item is not None:
                result.append(item)
        return result

    def get_categories_raw(self, category_key: str) -> list:
        """Return the raw (un-copied) category list for all sources."""
        result: list = []
        for src_cache in self._api_cache.get("sources", {}).values():
            result.extend(src_cache.get(category_key, []))
        return result

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

    def get_cache_age(self) -> float:
        """Return the age of the cache in seconds, or infinity if unknown."""
        last_refresh = self._api_cache.get("last_refresh")
        if not last_refresh:
            return float("inf")
        try:
            last_time = datetime.fromisoformat(last_refresh)
            return (datetime.now() - last_time).total_seconds()
        except (ValueError, TypeError):
            return float("inf")

    def is_cache_valid(self) -> bool:
        age = self.get_cache_age()
        if age == float("inf"):
            return False
        return age < self.config_service.get_cache_ttl()

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
        """Return (streams, categories) with _source_id/_source_name on each stream.

        Since source info is now injected at load/refresh time, this simply
        concatenates the original lists — no per-item dict copy.
        """
        sources = self._api_cache.get("sources", {})
        result: list = []
        all_categories: list = []
        for src_cache in sources.values():
            result.extend(src_cache.get(key, []))
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
    ) -> dict[str, Any]:
        url = f"{host}/player_api.php"
        params = {"username": username, "password": password, "action": action}
        last_error: dict[str, Any] | None = None
        for attempt in range(retries + 1):
            start_time = time.time()
            try:
                async with httpx.AsyncClient(
                    headers=HEADERS,
                    timeout=httpx.Timeout(connect=30.0, read=600.0, write=30.0, pool=30.0),
                    follow_redirects=True,
                ) as client:
                    response = await client.get(url, params=params)
                    elapsed_ms = int((time.time() - start_time) * 1000)
                    if response.status_code == 200:
                        try:
                            data = response.json()
                        except ValueError as exc:
                            last_error = {
                                "type": "parse_error",
                                "message": str(exc),
                                "status_code": 200,
                                "attempt": attempt + 1,
                                "duration_ms": elapsed_ms,
                            }
                            logger.error(f"Invalid JSON for {action}: {exc} (attempt {attempt + 1}/{retries + 1})")
                            if attempt < retries:
                                await asyncio.sleep(2**attempt)
                            continue

                        if not isinstance(data, list):
                            last_error = {
                                "type": "invalid_payload",
                                "message": f"Expected a list response for {action}",
                                "status_code": 200,
                                "attempt": attempt + 1,
                                "duration_ms": elapsed_ms,
                            }
                            logger.error(
                                f"Unexpected payload for {action}: {type(data).__name__} "
                                f"(attempt {attempt + 1}/{retries + 1})"
                            )
                            if attempt < retries:
                                await asyncio.sleep(2**attempt)
                            continue

                        logger.debug(
                            f"Fetched {action}: {len(data)} items in {elapsed_ms / 1000:.1f}s"
                        )
                        return {
                            "ok": True,
                            "action": action,
                            "data": data,
                            "status_code": 200,
                            "duration_ms": elapsed_ms,
                            "attempts": attempt + 1,
                            "error": None,
                        }
                    else:
                        last_error = {
                            "type": "http_error",
                            "message": f"HTTP {response.status_code} while fetching {action}",
                            "status_code": response.status_code,
                            "attempt": attempt + 1,
                            "duration_ms": elapsed_ms,
                        }
                        logger.warning(
                            f"Fetch {action} failed with status {response.status_code} "
                            f"in {elapsed_ms / 1000:.1f}s"
                        )
            except httpx.TimeoutException:
                last_error = {
                    "type": "timeout",
                    "message": "Request timed out",
                    "attempt": attempt + 1,
                    "duration_ms": int((time.time() - start_time) * 1000),
                }
                logger.error(f"Timeout fetching {action} (attempt {attempt + 1}/{retries + 1})")
            except httpx.RemoteProtocolError as e:
                last_error = {
                    "type": "protocol_error",
                    "message": str(e),
                    "attempt": attempt + 1,
                    "duration_ms": int((time.time() - start_time) * 1000),
                }
                logger.error(f"Protocol error fetching {action}: {e} (attempt {attempt + 1}/{retries + 1})")
            except httpx.ReadError as e:
                last_error = {
                    "type": "read_error",
                    "message": str(e),
                    "attempt": attempt + 1,
                    "duration_ms": int((time.time() - start_time) * 1000),
                }
                logger.error(f"Read error fetching {action}: {e} (attempt {attempt + 1}/{retries + 1})")
            except httpx.ConnectError as e:
                last_error = {
                    "type": "connection_error",
                    "message": str(e),
                    "attempt": attempt + 1,
                    "duration_ms": int((time.time() - start_time) * 1000),
                }
                logger.error(f"Connection error fetching {action}: {e}")
                break
            except Exception as e:
                last_error = {
                    "type": "unexpected_error",
                    "message": str(e),
                    "attempt": attempt + 1,
                    "duration_ms": int((time.time() - start_time) * 1000),
                }
                logger.error(f"Error fetching {action}: {e}")
                break
            if attempt < retries:
                await asyncio.sleep(2**attempt)
        return {
            "ok": False,
            "action": action,
            "data": None,
            "status_code": (last_error or {}).get("status_code"),
            "duration_ms": (last_error or {}).get("duration_ms"),
            "attempts": retries + 1,
            "error": last_error or {
                "type": "unknown_error",
                "message": f"Unknown error while fetching {action}",
            },
        }

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
            self.save_refresh_progress(
                {
                    "in_progress": False,
                    "status": "failed",
                    "current_source": 0,
                    "total_sources": 0,
                    "current_source_name": "",
                    "current_step": "No valid sources configured",
                    "percent": 0,
                    "started_at": datetime.now().isoformat(),
                    "finished_at": datetime.now().isoformat(),
                    "last_error": "No valid sources configured",
                    "source_results": [],
                    "summary": self._default_refresh_summary(),
                }
            )
            return False

        total_sources = len(enabled_sources)

        async with self._cache_lock:
            existing_sources_snapshot = copy.deepcopy(self._api_cache.get("sources", {}))
            previous_last_refresh = self._api_cache.get("last_refresh")
            self._api_cache["refresh_in_progress"] = True

        source_results = [
            self._build_source_result(
                source.get("id", "default"),
                source.get("name", source.get("id", "default")),
                existing_sources_snapshot.get(source.get("id", "default"), {}).get("last_refresh"),
            )
            for source in enabled_sources
        ]
        progress = self._normalise_refresh_progress(
            {
                "in_progress": True,
                "status": "running",
                "current_source": 0,
                "total_sources": total_sources,
                "current_source_name": "",
                "current_step": "Initializing...",
                "percent": 0,
                "started_at": datetime.now().isoformat(),
                "finished_at": None,
                "last_error": "",
                "source_results": source_results,
                "summary": self._build_refresh_summary(source_results, total_sources),
            }
        )
        self.save_refresh_progress(progress)

        logger.info(f"Starting full refresh at {datetime.now().isoformat()} for {total_sources} source(s)")

        new_sources_cache: dict[str, dict] = {}
        any_source_updated = False
        finished_at = datetime.now().isoformat()
        final_status = "failed"

        try:
            for source_idx, source in enumerate(enabled_sources):
                source_id = source.get("id", "default")
                source_name = source.get("name", source_id)
                host = source.get("host", "").rstrip("/")
                username = source.get("username", "")
                password = source.get("password", "")
                source_result = progress["source_results"][source_idx]
                source_result["status"] = "running"

                progress["current_source"] = source_idx + 1
                progress["current_source_name"] = source_name
                progress["current_step"] = f"{source_name}: Initializing"
                progress["summary"] = self._build_refresh_summary(progress["source_results"], total_sources)
                self.save_refresh_progress(progress)

                logger.info(f"Refreshing source: {source_name}")

                existing_source_cache = copy.deepcopy(
                    existing_sources_snapshot.get(source_id, self._empty_source_cache())
                )
                source_cache = copy.deepcopy(existing_source_cache)
                source_updated = False

                for step_idx, (cache_key, label, action) in enumerate(REFRESH_STEP_DEFINITIONS):
                    step_result = source_result["steps"][step_idx]
                    step_result["status"] = "running"
                    step_result["error"] = None
                    step_result["duration_ms"] = None
                    step_result["preserved_existing"] = False
                    progress["current_step"] = f"{source_name}: {label}"
                    progress["summary"] = self._build_refresh_summary(progress["source_results"], total_sources)
                    progress["percent"] = int(
                        (progress["summary"].get("processed_steps", 0) / max(progress["summary"].get("total_steps", 1), 1))
                        * 100
                    )
                    self.save_refresh_progress(progress)
                    logger.info(
                        f"[{source_name}] Step {step_idx + 1}/{len(REFRESH_STEP_DEFINITIONS)}: {label} "
                        f"(progress: {progress['percent']}%)"
                    )

                    fetch_result = await self.fetch_from_upstream(host, username, password, action)
                    step_result["duration_ms"] = fetch_result.get("duration_ms")
                    if fetch_result.get("ok"):
                        data = fetch_result.get("data") or []
                        source_cache[cache_key] = data
                        step_result["status"] = "success"
                        step_result["count"] = len(data)
                        source_updated = True
                    else:
                        error = dict(fetch_result.get("error") or {})
                        step_result["status"] = "failed"
                        step_result["error"] = error
                        step_result["preserved_existing"] = source_id in existing_sources_snapshot
                        step_result["count"] = len(source_cache.get(cache_key, []))
                        source_result["errors"].append(
                            {
                                "key": cache_key,
                                "label": label,
                                "preserved_existing": step_result["preserved_existing"],
                                **error,
                            }
                        )
                        progress["last_error"] = self._format_step_error(source_name, label, error)

                    source_result["counts"] = self._build_source_counts(source_cache)
                    source_result["status"] = self._derive_source_status(source_result)
                    progress["summary"] = self._build_refresh_summary(progress["source_results"], total_sources)
                    progress["percent"] = int(
                        (progress["summary"].get("processed_steps", 0) / max(progress["summary"].get("total_steps", 1), 1))
                        * 100
                    )
                    self.save_refresh_progress(progress)

                if source_updated:
                    any_source_updated = True
                    source_cache["last_refresh"] = datetime.now().isoformat()

                source_result["counts"] = self._build_source_counts(source_cache)
                source_result["last_refresh"] = source_cache.get("last_refresh")
                source_result["status"] = self._derive_source_status(source_result)

                if source_updated or source_id in existing_sources_snapshot:
                    new_sources_cache[source_id] = source_cache

            summary = self._build_refresh_summary(progress["source_results"], total_sources)
            failed_sources = summary.get("failed_sources", 0)
            partial_sources = summary.get("partial_sources", 0)

            if new_sources_cache or existing_sources_snapshot:
                async with self._cache_lock:
                    self._api_cache["sources"] = new_sources_cache
                    self._api_cache["last_refresh"] = datetime.now().isoformat() if any_source_updated else previous_last_refresh
                    self._api_cache["refresh_in_progress"] = False
                self._inject_source_info()
                await self.rebuild_stream_source_map()
                self._rebuild_stream_index()
                self.save_cache_to_disk()

                if on_cache_refreshed and any_source_updated:
                    progress["in_progress"] = True
                    progress["current_source"] = total_sources
                    progress["current_source_name"] = "Categories"
                    progress["current_step"] = "Refreshing automatic categories..."
                    progress["percent"] = min(progress.get("percent", 0), 95)
                    progress["summary"] = summary
                    self.save_refresh_progress(progress)
                    await on_cache_refreshed()

                if failed_sources or partial_sources:
                    final_status = "partial" if any_source_updated or partial_sources else "failed"
                else:
                    final_status = "success" if any_source_updated else "failed"

                logger.info(
                    f"Refresh complete with status={final_status}. Total: {summary['live_streams']} live, "
                    f"{summary['vod_streams']} vod, {summary['series']} series"
                )
            else:
                logger.warning("Refresh completed but no data was fetched from any source")
                async with self._cache_lock:
                    self._api_cache["refresh_in_progress"] = False
                final_status = "failed"
        except Exception as exc:
            logger.error(f"Cache refresh failed unexpectedly: {exc}")
            progress["last_error"] = progress.get("last_error") or str(exc)
            final_status = "partial" if any_source_updated else "failed"
            async with self._cache_lock:
                self._api_cache["refresh_in_progress"] = False
        finally:
            finished_at = datetime.now().isoformat()
            progress["in_progress"] = False
            progress["status"] = final_status
            progress["current_source"] = total_sources
            progress["current_source_name"] = ""
            progress["current_step"] = {
                "success": "Complete",
                "partial": "Complete with warnings",
                "failed": "Refresh failed",
            }.get(final_status, "Complete")
            progress["percent"] = 100 if total_sources else 0
            progress["finished_at"] = finished_at
            progress["summary"] = self._build_refresh_summary(progress["source_results"], total_sources)
            if final_status == "failed" and not progress.get("last_error"):
                progress["last_error"] = "Refresh failed for every source"
            self.save_refresh_progress(
                progress
            )
            if final_status in {"partial", "failed"} and self.notification_service:
                try:
                    await self.notification_service.send_cache_refresh_failure_notification(progress)
                except Exception as exc:
                    logger.error(f"Failed to dispatch cache refresh notification: {exc}")
        return final_status in {"success", "partial"}

    # ------------------------------------------------------------------
    # Clear
    # ------------------------------------------------------------------

    async def clear_cache(self) -> None:
        async with self._cache_lock:
            self._api_cache = {"sources": {}, "last_refresh": None, "refresh_in_progress": False}
        async with self._stream_map_lock:
            self._stream_source_map = {"live": {}, "vod": {}, "series": {}}
        self._stream_index = {}
        self._source_names = {}
        conn = db_connect(self.db_path)
        try:
            conn.execute("DELETE FROM streams")
            conn.execute("DELETE FROM source_categories")
            conn.execute("DELETE FROM source_last_refresh")
            conn.execute("DELETE FROM cache_meta")
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to clear cache in DB: {e}")
        finally:
            conn.close()
