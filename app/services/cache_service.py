"""Cache service — in-memory API cache with disk persistence and stream-source mapping."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import httpx

from app.database import DB_NAME, db_connect, adb_transaction, _row_to_dict

if TYPE_CHECKING:
    from app.services.config_service import ConfigService
    from app.services.notification_service import NotificationService
    from app.services.cart_service import CartService

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
        self.cart_service: "CartService | None" = None
        self.log_service = None  # set after init via attribute binding
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

        # Async progress save throttling
        self._progress_save_lock = asyncio.Lock()
        self._last_progress_save = 0.0
        self._progress_save_interval = 2.0  # seconds
        self._pending_progress_saves: list[asyncio.Task] = []

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
            "heartbeat_at": None,
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
        progress["heartbeat_at"] = progress_data.get("heartbeat_at")
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
        use_async = self.config_service.config.get("database", {}).get("use_async", True)
        progress_data = self._normalise_refresh_progress(progress_data)
        self._api_cache["refresh_progress"] = progress_data
        if use_async:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                self._save_refresh_progress_sync(progress_data)
                return
            task = loop.create_task(self.save_refresh_progress_async(progress_data))
            self._pending_progress_saves.append(task)
            task.add_done_callback(lambda t: self._pending_progress_saves.remove(t) if t in self._pending_progress_saves else None)
        else:
            self._save_refresh_progress_sync(progress_data)

    async def _flush_pending_progress_saves(self) -> None:
        if self._pending_progress_saves:
            await asyncio.gather(*self._pending_progress_saves, return_exceptions=True)
        self._pending_progress_saves.clear()

    def _save_refresh_progress_sync(self, progress_data: dict) -> None:
        progress_data = self._normalise_refresh_progress(progress_data)
        conn = db_connect(self.db_path)
        try:
            conn.execute(
                """INSERT OR REPLACE INTO refresh_progress
                   (id, in_progress, current_source, total_sources,
                    current_source_name, current_step, percent, started_at,
                    heartbeat_at, status, source_results, summary, finished_at, last_error)
                   VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    int(progress_data.get("in_progress", False)),
                    progress_data.get("current_source", 0),
                    progress_data.get("total_sources", 0),
                    progress_data.get("current_source_name", ""),
                    progress_data.get("current_step", ""),
                    progress_data.get("percent", 0),
                    progress_data.get("started_at"),
                    progress_data.get("heartbeat_at"),
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
        cached = self._api_cache.get("refresh_progress")
        if cached:
            return cached
        conn = db_connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT in_progress, current_source, total_sources, "
                "current_source_name, current_step, percent, started_at, "
                "heartbeat_at, status, source_results, summary, finished_at, last_error "
                "FROM refresh_progress WHERE id = 1"
            ).fetchone()
            if row:
                progress = self._normalise_refresh_progress({
                    "in_progress": bool(row["in_progress"]),
                    "current_source": row["current_source"],
                    "total_sources": row["total_sources"],
                    "current_source_name": row["current_source_name"],
                    "current_step": row["current_step"],
                    "percent": row["percent"],
                    "started_at": row["started_at"],
                    "heartbeat_at": row["heartbeat_at"],
                    "status": row["status"],
                    "source_results": row["source_results"],
                    "summary": row["summary"],
                    "finished_at": row["finished_at"],
                    "last_error": row["last_error"],
                })
                self._api_cache["refresh_progress"] = progress
                return progress
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
        progress["finished_at"] = datetime.now(timezone.utc).isoformat()
        progress["last_error"] = last_error or progress.get("last_error", "")
        progress["summary"] = self._build_refresh_summary(
            progress.get("source_results", []),
            progress.get("total_sources", 0),
        )
        self.save_refresh_progress(progress)

    async def save_refresh_progress_async(self, progress_data: dict) -> None:
        progress_data = self._normalise_refresh_progress(progress_data)
        # Always update in-memory state so the UI sees the latest progress
        self._api_cache["refresh_progress"] = progress_data
        async with self._progress_save_lock:
            now = time.time()
            if now - self._last_progress_save < self._progress_save_interval:
                return
            self._last_progress_save = now
            try:
                async with adb_transaction(self.db_path) as conn:
                    await conn.execute(
                        """INSERT OR REPLACE INTO refresh_progress
                           (id, in_progress, current_source, total_sources,
                            current_source_name, current_step, percent, started_at,
                            heartbeat_at, status, source_results, summary, finished_at, last_error)
                           VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            int(progress_data.get("in_progress", False)),
                            progress_data.get("current_source", 0),
                            progress_data.get("total_sources", 0),
                            progress_data.get("current_source_name", ""),
                            progress_data.get("current_step", ""),
                            progress_data.get("percent", 0),
                            progress_data.get("started_at"),
                            progress_data.get("heartbeat_at"),
                            progress_data.get("status", "idle"),
                            json.dumps(progress_data.get("source_results", []), ensure_ascii=False),
                            json.dumps(progress_data.get("summary", {}), ensure_ascii=False),
                            progress_data.get("finished_at"),
                            progress_data.get("last_error", ""),
                        ),
                    )
            except Exception as e:
                logger.warning(f"Failed to save progress async: {e}")

    async def load_refresh_progress_async(self) -> dict:
        try:
            async with adb_transaction(self.db_path) as conn:
                async with conn.execute(
                    "SELECT in_progress, current_source, total_sources, "
                    "current_source_name, current_step, percent, started_at, "
                    "heartbeat_at, status, source_results, summary, finished_at, last_error "
                    "FROM refresh_progress WHERE id = 1"
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        d = _row_to_dict(row)
                        return self._normalise_refresh_progress({
                            "in_progress": bool(d["in_progress"]),
                            "current_source": d["current_source"],
                            "total_sources": d["total_sources"],
                            "current_source_name": d["current_source_name"],
                            "current_step": d["current_step"],
                            "percent": d["percent"],
                            "started_at": d["started_at"],
                            "heartbeat_at": d.get("heartbeat_at"),
                            "status": d["status"],
                            "source_results": d["source_results"],
                            "summary": d["summary"],
                            "finished_at": d["finished_at"],
                            "last_error": d["last_error"],
                        })
        except Exception:
            pass
        return self._default_refresh_progress()

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
                        full_data = json.loads(row["data"])
                        sources[src][key].append(self._slim_stream(full_data, ct))
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

    async def load_cache_from_disk_async(self) -> None:
        async with adb_transaction(self.db_path) as conn:
            async with conn.execute(
                "SELECT last_refresh FROM cache_meta WHERE id = 1"
            ) as cursor:
                meta_row = await cursor.fetchone()
            if meta_row:
                self._api_cache["last_refresh"] = meta_row["last_refresh"]

            async with conn.execute(
                "SELECT source_id, content_type, category_id, category_name, data "
                "FROM source_categories ORDER BY source_id, content_type"
            ) as cursor:
                cat_rows = await cursor.fetchall()

            async with conn.execute(
                "SELECT source_id, content_type, stream_id, data "
                "FROM streams ORDER BY source_id, content_type"
            ) as cursor:
                stream_rows = await cursor.fetchall()

            async with conn.execute(
                "SELECT source_id, last_refresh FROM source_last_refresh"
            ) as cursor:
                src_refresh_rows = await cursor.fetchall()

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
                    full_data = json.loads(row["data"])
                    sources[src][key].append(self._slim_stream(full_data, ct))
                except (json.JSONDecodeError, TypeError):
                    pass

        for rr in src_refresh_rows:
            if rr["source_id"] in sources:
                sources[rr["source_id"]]["last_refresh"] = rr["last_refresh"]

        refresh_progress = await self.load_refresh_progress_async()
        if refresh_progress.get("in_progress"):
            if not refresh_progress.get("finished_at"):
                refresh_progress["finished_at"] = datetime.now(timezone.utc).isoformat()
            refresh_progress["in_progress"] = False
            refresh_progress["status"] = "failed"
            self.save_refresh_progress(refresh_progress)
        self._api_cache["refresh_progress"] = refresh_progress

        if sources:
            self._api_cache["sources"] = sources
            self._api_cache["refresh_in_progress"] = False
            self._inject_source_info()
            await asyncio.to_thread(self._rebuild_stream_source_map_sync)
            await asyncio.to_thread(self._rebuild_stream_index)
            logger.info(
                f"Loaded cache from DB. Last refresh: {self._api_cache.get('last_refresh', 'Never')}"
            )
        else:
            logger.info("DB cache is empty — will refresh on first request")

    def _save_cache_to_disk_sync(self) -> None:
        conn = db_connect(self.db_path)
        try:
            sources = self._api_cache.get("sources", {})

            active_source_ids = list(sources.keys())

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
                cat_rows = []
                for cat_key, ct in CAT_MAP:
                    for cat in src_cache.get(cat_key, []):
                        cat_rows.append((
                            source_id, ct,
                            str(cat.get("category_id", "")),
                            cat.get("category_name", ""),
                            json.dumps(cat, ensure_ascii=False),
                        ))
                if cat_rows:
                    conn.executemany(
                        "INSERT OR REPLACE INTO source_categories "
                        "(source_id, content_type, category_id, category_name, data) "
                        "VALUES (?,?,?,?,?)",
                        cat_rows,
                    )

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
                            json.dumps(stream, ensure_ascii=False),
                        ))
                if stream_rows:
                    conn.executemany(
                        "INSERT OR REPLACE INTO streams "
                        "(source_id, content_type, stream_id, name, category_id, added, data) "
                        "VALUES (?,?,?,?,?,?,?)",
                        stream_rows,
                    )

                src_last = src_cache.get("last_refresh")
                if src_last:
                    conn.execute(
                        "INSERT OR REPLACE INTO source_last_refresh (source_id, last_refresh) VALUES (?,?)",
                        (source_id, src_last),
                    )

            global_refresh = self._api_cache.get("last_refresh")
            if global_refresh:
                conn.execute(
                    "INSERT OR REPLACE INTO cache_meta (id, last_refresh) VALUES (1, ?)",
                    (global_refresh,),
                )

            conn.commit()
            logger.info(f"Cache saved to DB at {datetime.now(timezone.utc).isoformat()}")
        except Exception as e:
            logger.error(f"Failed to save cache to DB: {e}")
        finally:
            conn.close()

    async def save_cache_to_disk_async(self) -> None:
        async with adb_transaction(self.db_path) as conn:
            sources = self._api_cache.get("sources", {})

            active_source_ids = list(sources.keys())

            if active_source_ids:
                placeholders = ",".join("?" * len(active_source_ids))
                await conn.execute(
                    f"DELETE FROM streams WHERE source_id NOT IN ({placeholders})",
                    active_source_ids,
                )
                await conn.execute(
                    f"DELETE FROM source_categories WHERE source_id NOT IN ({placeholders})",
                    active_source_ids,
                )
                await conn.execute(
                    f"DELETE FROM source_last_refresh WHERE source_id NOT IN ({placeholders})",
                    active_source_ids,
                )
            else:
                await conn.execute("DELETE FROM streams")
                await conn.execute("DELETE FROM source_categories")
                await conn.execute("DELETE FROM source_last_refresh")

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
                cat_rows = []
                for cat_key, ct in CAT_MAP:
                    for cat in src_cache.get(cat_key, []):
                        cat_rows.append((
                            source_id, ct,
                            str(cat.get("category_id", "")),
                            cat.get("category_name", ""),
                            json.dumps(cat, ensure_ascii=False),
                        ))
                if cat_rows:
                    await conn.executemany(
                        "INSERT OR REPLACE INTO source_categories "
                        "(source_id, content_type, category_id, category_name, data) "
                        "VALUES (?,?,?,?,?)",
                        cat_rows,
                    )

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
                            json.dumps(stream, ensure_ascii=False),
                        ))
                if stream_rows:
                    await conn.executemany(
                        "INSERT OR REPLACE INTO streams "
                        "(source_id, content_type, stream_id, name, category_id, added, data) "
                        "VALUES (?,?,?,?,?,?,?)",
                        stream_rows,
                    )

                src_last = src_cache.get("last_refresh")
                if src_last:
                    await conn.execute(
                        "INSERT OR REPLACE INTO source_last_refresh (source_id, last_refresh) VALUES (?,?)",
                        (source_id, src_last),
                    )

            global_refresh = self._api_cache.get("last_refresh")
            if global_refresh:
                await conn.execute(
                    "INSERT OR REPLACE INTO cache_meta (id, last_refresh) VALUES (1, ?)",
                    (global_refresh,),
                )

        logger.info(f"Cache saved to DB at {datetime.now(timezone.utc).isoformat()}")

    def save_cache_to_disk(self) -> None:
        use_async = self.config_service.config.get("database", {}).get("use_async", True)
        if use_async:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                self._save_cache_to_disk_sync()
                return
            task = loop.create_task(self.save_cache_to_disk_async())
            task.add_done_callback(
                lambda t: logger.error(f"Async cache save failed: {t.exception()}") if t.exception() else None
            )
        else:
            self._save_cache_to_disk_sync()

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

    @staticmethod
    def _slim_stream(stream: dict, content_type: str) -> dict:
        """Extract only the fields needed for routing, M3U, monitoring, and display."""
        slim: dict = {}
        # ID fields
        if content_type == "series":
            slim["series_id"] = stream.get("series_id", "")
        else:
            slim["stream_id"] = stream.get("stream_id", "")
        # Common fields
        slim["name"] = stream.get("name", "")
        slim["category_id"] = stream.get("category_id", "")
        slim["added"] = stream.get("added") or stream.get("last_modified", 0)
        # Icon/cover
        slim["stream_icon"] = stream.get("stream_icon", "")
        slim["cover"] = stream.get("cover", "")
        # Type-specific fields
        if content_type == "live":
            slim["epg_channel_id"] = stream.get("epg_channel_id", "")
        elif content_type == "vod":
            slim["container_extension"] = stream.get("container_extension", "mp4")
            slim["tmdb_id"] = stream.get("tmdb_id", "")
            slim["tmdb"] = stream.get("tmdb", "")
            slim["rating"] = stream.get("rating", 0)
        elif content_type == "series":
            slim["tmdb_id"] = stream.get("tmdb_id", "")
            slim["tmdb"] = stream.get("tmdb", "")
            slim["imdb_id"] = stream.get("imdb_id", "")
            slim["imdb"] = stream.get("imdb", "")
        return slim

    def _slim_cache_in_memory(self) -> None:
        """Replace full stream dicts in the in-memory cache with slim versions.

        Call this AFTER the full data has been persisted to SQLite. The slim
        dicts contain only the fields needed for routing, M3U, monitoring,
        and display — the full upstream JSON blob stays in the DB only.
        """
        STREAM_KEYS = {
            "live_streams": "live",
            "vod_streams": "vod",
            "series": "series",
        }
        sources = self._api_cache.get("sources", {})
        for _src_id, src_cache in sources.items():
            for list_key, ct in STREAM_KEYS.items():
                slim_list = [
                    self._slim_stream(item, ct)
                    for item in src_cache.get(list_key, [])
                ]
                src_cache[list_key] = slim_list

    def get_stream_full_data(
        self, content_type: str, source_id: str, stream_id: str
    ) -> dict | None:
        """Fetch the full stream JSON from SQLite on demand.

        Use this when a consumer (e.g. Xtream API emulation) needs the
        complete upstream data blob that is no longer kept in memory.
        """
        conn = db_connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT data FROM streams "
                "WHERE source_id=? AND content_type=? AND stream_id=?",
                (source_id, content_type, stream_id),
            ).fetchone()
            if row:
                try:
                    return json.loads(row["data"])
                except (json.JSONDecodeError, TypeError):
                    pass
        finally:
            conn.close()
        return None

    def get_streams_full_data_batch(
        self, content_type: str, source_id: str, stream_ids: list[str]
    ) -> dict[str, dict]:
        """Fetch multiple full stream JSON blobs from SQLite in one query.

        Returns a dict mapping stream_id -> full stream dict.
        """
        if not stream_ids:
            return {}
        conn = db_connect(self.db_path)
        try:
            placeholders = ",".join("?" * len(stream_ids))
            rows = conn.execute(
                f"SELECT stream_id, data FROM streams "
                f"WHERE source_id=? AND content_type=? AND stream_id IN ({placeholders})",
                [source_id, content_type] + stream_ids,
            ).fetchall()
            result: dict[str, dict] = {}
            for row in rows:
                try:
                    result[row["stream_id"]] = json.loads(row["data"])
                except (json.JSONDecodeError, TypeError):
                    pass
            return result
        finally:
            conn.close()

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
    # SQLite-backed browse
    # ------------------------------------------------------------------

    def browse_streams_db(
        self,
        content_type: str,
        search: str = "",
        group: str = "",
        source: str = "",
        news_days: int = 0,
        min_rating: float = 0,
        max_added_days: int = 0,
        sort_by: str = "",
        sort_order: str = "desc",
        page: int = 1,
        per_page: int = 50,
        tmdb_search_id: str | None = None,
    ) -> dict:
        """Query streams directly from SQLite with filters, sort and pagination.

        Returns a dict with: {items, total, page, per_page, total_pages,
        group_counts, source_counts} where items are lightweight dicts
        containing only the fields needed by the browse UI.
        """
        current_time = int(time.time())
        news_cutoff = current_time - (news_days * 86400) if news_days > 0 else 0
        added_cutoff = current_time - (max_added_days * 86400) if max_added_days > 0 else 0

        conn = db_connect(self.db_path)
        try:
            # Build WHERE clauses
            conditions: list[str] = ["s.content_type = ?"]
            params: list = [content_type]

            if source:
                conditions.append("s.source_id = ?")
                params.append(source)

            if news_days > 0:
                conditions.append("s.added >= ?")
                params.append(news_cutoff)

            if max_added_days > 0:
                conditions.append("s.added >= ?")
                params.append(added_cutoff)

            # Search: LIKE on name and group name
            if tmdb_search_id is not None:
                # TMDB search: match against json_extract on data column
                conditions.append(
                    "(json_extract(s.data, '$.tmdb_id') = ? OR "
                    "json_extract(s.data, '$.tmdb') = ? OR "
                    "json_extract(s.data, '$.tmdb_id') = ? OR "
                    "json_extract(s.data, '$.tmdb') = ?)"
                )
                tmdb_prefixed = f"tmdb:{tmdb_search_id}"
                params.extend([tmdb_search_id, tmdb_search_id, tmdb_prefixed, tmdb_prefixed])
            elif search:
                # Use FTS5 for fast name search, fall back to LIKE for group name
                fts_query = search.replace('"', '""')
                conditions.append(
                    "(s.rowid IN (SELECT rowid FROM streams_fts WHERE streams_fts MATCH ?) "
                    "OR lower(sc.category_name) LIKE lower(?))"
                )
                params.extend([fts_query, f"%{search}%"])

            if group:
                conditions.append("sc.category_name = ?")
                params.append(group)

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            # Sorting
            order_map = {
                "added": "s.added",
                "name": "lower(s.name)",
                "rating": "CAST(json_extract(s.data, '$.rating') AS REAL)",
            }
            order_col = order_map.get(sort_by, "")
            if order_col:
                direction = "DESC" if sort_order == "desc" else "ASC"
                order_clause = f"{order_col} {direction}"
            elif news_days > 0:
                order_clause = "s.added DESC"
            else:
                order_clause = "lower(sc.category_name), lower(s.name)"

            # Count total matching rows
            count_sql = f"""
                SELECT COUNT(*) as cnt
                FROM streams s
                LEFT JOIN source_categories sc
                    ON s.source_id = sc.source_id
                    AND s.content_type = sc.content_type
                    AND s.category_id = sc.category_id
                WHERE {where_clause}
            """
            total = conn.execute(count_sql, params).fetchone()["cnt"]

            # Fetch the page of results
            if per_page > 0:
                offset = (page - 1) * per_page
                limit_clause = "LIMIT ? OFFSET ?"
                limit_params: list = [per_page, offset]
            else:
                limit_clause = ""
                limit_params = []
            data_sql = f"""
                SELECT s.source_id, s.content_type, s.stream_id, s.name,
                       s.category_id, s.added, s.data,
                       COALESCE(sc.category_name, 'Unknown') as group_name
                FROM streams s
                LEFT JOIN source_categories sc
                    ON s.source_id = sc.source_id
                    AND s.content_type = sc.content_type
                    AND s.category_id = sc.category_id
                WHERE {where_clause}
                ORDER BY {order_clause}
                {limit_clause}
            """
            rows = conn.execute(data_sql, params + limit_params).fetchall()

            # Resolve source names from in-memory config
            source_names = self._source_names

            items = []
            for row in rows:
                src_id = row["source_id"]
                src_name = source_names.get(src_id, "Unknown")
                grp = row["group_name"]
                name = row["name"]

                try:
                    data = json.loads(row["data"])
                except (json.JSONDecodeError, TypeError):
                    data = {}

                icon = data.get("stream_icon", "") or data.get("cover", "")
                item_id = str(data.get("stream_id") or data.get("series_id") or row["stream_id"])
                added_ts = row["added"] or 0

                raw_rating = data.get("rating", 0)
                try:
                    rating_val = float(raw_rating) if raw_rating else 0.0
                except (ValueError, TypeError):
                    rating_val = 0.0

                # Apply rating filter in Python (json_extract in WHERE is slower)
                if min_rating > 0 and rating_val < min_rating:
                    continue

                item_data = {
                    "name": name,
                    "group": grp,
                    "icon": icon,
                    "id": item_id,
                    "source_id": src_id,
                    "source_name": src_name,
                    "added": added_ts,
                    "rating": rating_val,
                    "content_type": row["content_type"],
                }
                if content_type in ("vod", "series"):
                    raw_tmdb = data.get("tmdb_id") or data.get("tmdb")
                    item_data["tmdb_id"] = raw_tmdb if raw_tmdb else None
                if content_type == "vod":
                    item_data["container_extension"] = data.get("container_extension", "mp4")
                items.append(item_data)

            # Collect group counts and source set for the response metadata.
            # We need these for the full (unfiltered) result set, so run a
            # separate lightweight query.
            gc_conditions: list[str] = ["s.content_type = ?"]
            gc_params: list = [content_type]
            if source:
                gc_conditions.append("s.source_id = ?")
                gc_params.append(source)
            if news_days > 0:
                gc_conditions.append("s.added >= ?")
                gc_params.append(news_cutoff)
            if max_added_days > 0:
                gc_conditions.append("s.added >= ?")
                gc_params.append(added_cutoff)
            # For tmdb search, include the tmdb filter in group counts too
            if tmdb_search_id is not None:
                gc_conditions.append(
                    "(json_extract(s.data, '$.tmdb_id') = ? OR "
                    "json_extract(s.data, '$.tmdb') = ? OR "
                    "json_extract(s.data, '$.tmdb_id') = ? OR "
                    "json_extract(s.data, '$.tmdb') = ?)"
                )
                tmdb_prefixed = f"tmdb:{tmdb_search_id}"
                gc_params.extend([tmdb_search_id, tmdb_search_id, tmdb_prefixed, tmdb_prefixed])

            gc_where = " AND ".join(gc_conditions)
            gc_sql = f"""
                SELECT COALESCE(sc.category_name, 'Unknown') as grp, COUNT(*) as cnt,
                       s.source_id
                FROM streams s
                LEFT JOIN source_categories sc
                    ON s.source_id = sc.source_id
                    AND s.content_type = sc.content_type
                    AND s.category_id = sc.category_id
                WHERE {gc_where}
                GROUP BY grp, s.source_id
            """
            group_counts: dict[str, int] = {}
            source_set: dict[str, str] = {}
            for row in conn.execute(gc_sql, gc_params).fetchall():
                grp = row["grp"]
                group_counts[grp] = group_counts.get(grp, 0) + row["cnt"]
                src_id = row["source_id"]
                if src_id:
                    source_set[src_id] = source_names.get(src_id, "Unknown")

            total_pages = (total + per_page - 1) // per_page if total > 0 and per_page > 0 else 0

            return {
                "items": items,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": total_pages,
                "group_counts": group_counts,
                "source_set": source_set,
            }
        finally:
            conn.close()

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

        def _build() -> dict[str, dict[str, str]]:
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
            return new_map

        new_map = await asyncio.to_thread(_build)
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
            return (datetime.now(timezone.utc) - last_time).total_seconds()
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

    async def _refresh_source_async(
        self,
        source: dict,
        source_idx: int,
        source_result: dict,
        existing_sources_snapshot: dict[str, dict],
        total_sources: int,
        progress: dict,
    ) -> tuple[str, dict, bool]:
        source_id = source.get("id", "default")
        source_name = source.get("name", source_id)
        host = source.get("host", "").rstrip("/")
        username = source.get("username", "")
        password = source.get("password", "")

        source_result["status"] = "running"

        progress["current_source"] = source_idx + 1
        progress["current_source_name"] = source_name
        progress["current_step"] = f"{source_name}: Initializing"
        progress["summary"] = self._build_refresh_summary(progress["source_results"], total_sources)
        self.save_refresh_progress(progress)

        logger.info(f"Refreshing source: {source_name}")

        existing_source_cache = json.loads(json.dumps(
            existing_sources_snapshot.get(source_id, self._empty_source_cache()),
            ensure_ascii=False,
        ))
        source_cache = json.loads(json.dumps(existing_source_cache, ensure_ascii=False))
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
            source_cache["last_refresh"] = datetime.now(timezone.utc).isoformat()

        source_result["counts"] = self._build_source_counts(source_cache)
        source_result["last_refresh"] = source_cache.get("last_refresh")
        source_result["status"] = self._derive_source_status(source_result)

        return source_id, source_cache, source_updated

    async def refresh_cache(self, on_cache_refreshed=None) -> bool:
        """Refresh all cached data from all configured sources.

        *on_cache_refreshed* is an optional async callback invoked after a
        successful data fetch (used to refresh pattern categories, etc.).
        """
        existing_progress = await asyncio.to_thread(self.load_refresh_progress)
        if existing_progress.get("in_progress"):
            # Use heartbeat_at (updated periodically) for the staleness guard,
            # not started_at (immutable, used for UI elapsed-time display).
            last_activity = existing_progress.get("heartbeat_at") or existing_progress.get("started_at")
            if last_activity:
                try:
                    last_activity_time = datetime.fromisoformat(last_activity)
                    if (datetime.now(timezone.utc) - last_activity_time).total_seconds() < 600:
                        logger.info("Refresh already in progress, skipping")
                        return False
                except (ValueError, TypeError):
                    pass

        # If a download is currently active in the cart, delay the refresh
        # until it completes so we don't compete with it for upstream
        # bandwidth / rate limits. We keep `in_progress=True` so concurrent
        # refresh triggers coalesce into a single waiter.
        if self.cart_service is not None and self.cart_service.is_download_active():
            logger.info("Cache refresh delayed: download in progress in cart")
            if getattr(self, 'log_service', None):
                await getattr(self, 'log_service', None).log("cache", "warning", "Cache refresh delayed — download in progress")
            wait_started_at = datetime.now(timezone.utc).isoformat()
            async with self._cache_lock:
                self._api_cache["refresh_in_progress"] = True
            self.save_refresh_progress(
                {
                    "in_progress": True,
                    "status": "waiting",
                    "current_source": 0,
                    "total_sources": 0,
                    "current_source_name": "",
                    "current_step": "Waiting for active download to finish",
                    "percent": 0,
                    "started_at": wait_started_at,
                    "heartbeat_at": wait_started_at,
                    "finished_at": None,
                    "last_error": "",
                    "source_results": [],
                    "summary": self._default_refresh_summary(),
                }
            )

            max_wait_seconds = 24 * 3600  # safety cap
            poll_interval = 30
            heartbeat_interval = 300  # refresh started_at every 5min so the
                                      # 600s staleness guard at the top keeps
                                      # coalescing concurrent refresh triggers
            waited = 0
            last_heartbeat = 0
            try:
                while self.cart_service.is_download_active():
                    if waited >= max_wait_seconds:
                        logger.warning(
                            "Cache refresh waited %ss for downloads to finish; aborting",
                            waited,
                        )
                        if getattr(self, 'log_service', None):
                            await getattr(self, 'log_service', None).log("cache", "error", f"Cache refresh aborted — waited {waited}s for downloads to finish")
                        async with self._cache_lock:
                            self._api_cache["refresh_in_progress"] = False
                        self.save_refresh_progress(
                            {
                                "in_progress": False,
                                "status": "failed",
                                "current_source": 0,
                                "total_sources": 0,
                                "current_source_name": "",
                                "current_step": "Aborted: download still active after wait",
                                "percent": 0,
                                "started_at": wait_started_at,
                                "finished_at": datetime.now(timezone.utc).isoformat(),
                                "last_error": "Cache refresh aborted: download still in progress",
                                "source_results": [],
                                "summary": self._default_refresh_summary(),
                            }
                        )
                        return False
                    await asyncio.sleep(poll_interval)
                    waited += poll_interval
                    if waited - last_heartbeat >= heartbeat_interval:
                        last_heartbeat = waited
                        self.save_refresh_progress(
                            {
                                "in_progress": True,
                                "status": "waiting",
                                "current_source": 0,
                                "total_sources": 0,
                                "current_source_name": "",
                                "current_step": (
                                    f"Waiting for active download to finish "
                                    f"({waited // 60} min)"
                                ),
                                "percent": 0,
                                "started_at": wait_started_at,
                                "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                                "finished_at": None,
                                "last_error": "",
                                "source_results": [],
                                "summary": self._default_refresh_summary(),
                            }
                        )
            except asyncio.CancelledError:
                async with self._cache_lock:
                    self._api_cache["refresh_in_progress"] = False
                self.clear_refresh_progress(
                    status="cancelled",
                    last_error="Cancelled while waiting for downloads",
                )
                raise

            logger.info(
                "Download finished after %ss, resuming cache refresh", waited
            )

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
            if getattr(self, 'log_service', None):
                await getattr(self, 'log_service', None).log("cache", "warning", "Cache refresh skipped — no valid sources configured")
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
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "last_error": "No valid sources configured",
                    "source_results": [],
                    "summary": self._default_refresh_summary(),
                }
            )
            return False

        total_sources = len(enabled_sources)

        async with self._cache_lock:
            raw_sources = self._api_cache.get("sources", {})
            previous_last_refresh = self._api_cache.get("last_refresh")
            self._api_cache["refresh_in_progress"] = True
        existing_sources_snapshot = await asyncio.to_thread(
            lambda: json.loads(json.dumps(raw_sources, ensure_ascii=False))
        )

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
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": None,
                "last_error": "",
                "source_results": source_results,
                "summary": self._build_refresh_summary(source_results, total_sources),
            }
        )
        self.save_refresh_progress(progress)

        logger.info(f"Starting full refresh at {datetime.now(timezone.utc).isoformat()} for {total_sources} source(s)")
        if getattr(self, 'log_service', None):
            await getattr(self, 'log_service', None).log("cache", "info", f"Cache refresh started ({total_sources} source(s))")

        new_sources_cache: dict[str, dict] = {}
        any_source_updated = False
        final_status = "failed"

        try:
            tasks = [
                self._refresh_source_async(
                    source,
                    source_idx,
                    progress["source_results"][source_idx],
                    existing_sources_snapshot,
                    total_sources,
                    progress,
                )
                for source_idx, source in enumerate(enabled_sources)
            ]
            results = await asyncio.gather(*tasks)

            for source_id, source_cache, source_updated in results:
                if source_updated:
                    any_source_updated = True
                if source_updated or source_id in existing_sources_snapshot:
                    new_sources_cache[source_id] = source_cache

            summary = self._build_refresh_summary(progress["source_results"], total_sources)
            failed_sources = summary.get("failed_sources", 0)
            partial_sources = summary.get("partial_sources", 0)

            if new_sources_cache or existing_sources_snapshot:
                async with self._cache_lock:
                    self._api_cache["sources"] = new_sources_cache
                    # Always update last_refresh so the UI shows the most recent
                    # attempt time. The last_refresh_outcome field distinguishes
                    # success from failure.
                    self._api_cache["last_refresh"] = datetime.now(timezone.utc).isoformat()
                    self._api_cache["refresh_in_progress"] = False
                await asyncio.to_thread(self._inject_source_info)
                await self.rebuild_stream_source_map()
                await asyncio.to_thread(self._rebuild_stream_index)
                await self.save_cache_to_disk_async()
                # Slim down in-memory cache now that full data is in DB
                await asyncio.to_thread(self._slim_cache_in_memory)
                # Re-inject source info on the new slim dicts
                await asyncio.to_thread(self._inject_source_info)
                await asyncio.to_thread(self._rebuild_stream_index)

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
                if getattr(self, 'log_service', None):
                    level = "info" if final_status == "success" else "warning"
                    msg = f"Cache refresh completed ({final_status}): {summary['live_streams']} live, {summary['vod_streams']} vod, {summary['series']} series"
                    details = {"status": final_status, **summary}
                    # Add per-source error details if any
                    failed_steps = []
                    for sr in progress.get("source_results", []):
                        for err in sr.get("errors", []):
                            failed_steps.append(f"{sr.get('source_name', sr.get('source_id', '?'))}: {err.get('label', '?')}")
                    if failed_steps:
                        details["failed_steps"] = failed_steps
                    await getattr(self, 'log_service', None).log("cache", level, msg, details)
            else:
                logger.warning("Refresh completed but no data was fetched from any source")
                if getattr(self, 'log_service', None):
                    await getattr(self, 'log_service', None).log("cache", "error", "Cache refresh failed — no data fetched from any source")
                async with self._cache_lock:
                    self._api_cache["refresh_in_progress"] = False
                    self._api_cache["last_refresh"] = datetime.now(timezone.utc).isoformat()
                final_status = "failed"
        except Exception as exc:
            logger.error(f"Cache refresh failed unexpectedly: {exc}")
            if getattr(self, 'log_service', None):
                await getattr(self, 'log_service', None).log("cache", "error", f"Cache refresh failed unexpectedly: {exc}")
            progress["last_error"] = progress.get("last_error") or str(exc)
            final_status = "partial" if any_source_updated else "failed"
            async with self._cache_lock:
                self._api_cache["refresh_in_progress"] = False
        finally:
            await self._flush_pending_progress_saves()
            finished_at = datetime.now(timezone.utc).isoformat()
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
            self._save_refresh_progress_sync(
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
