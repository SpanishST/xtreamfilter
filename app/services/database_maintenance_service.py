"""Online database cleanup and optimization service."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import shutil
import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from app.database import DB_NAME, db_connect

if TYPE_CHECKING:
    from app.services.cache_service import CacheService
    from app.services.config_service import ConfigService

logger = logging.getLogger(__name__)

MAINTENANCE_STATUSES = {"running", "stop_requested"}
UNINTERRUPTIBLE_PHASES = {"vacuum", "fts_rebuild"}


class DatabaseMaintenanceService:
    """Run safe, serialized cleanup work against the application database."""

    def __init__(self, config_service: ConfigService, cache_service: CacheService):
        self.config_service = config_service
        self.cache_service = cache_service
        self.db_path = os.path.join(config_service.data_dir, DB_NAME)

        self._task: asyncio.Task | None = None
        self._stop_event: threading.Event | None = None
        self._status_lock = threading.Lock()
        self._status = self._load_status_sync()

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _default_status() -> dict[str, Any]:
        return {
            "status": "idle",
            "phase": "idle",
            "percent": 0,
            "indeterminate": False,
            "started_at": None,
            "heartbeat_at": None,
            "finished_at": None,
            "database_size_current": 0,
            "database_size_before": 0,
            "database_size_after": 0,
            "sources_removed": 0,
            "rows_removed": 0,
            "categories_removed": 0,
            "category_items_removed": 0,
            "fts_documents_before": 0,
            "fts_documents_after": 0,
            "fts_orphans_before": 0,
            "fts_orphans_after": 0,
            "vacuumed": False,
            "fts_rebuilt": False,
            "last_error": "",
            "details": {},
        }

    def _load_status_sync(self) -> dict[str, Any]:
        status = self._default_status()
        try:
            conn = db_connect(self.db_path)
            try:
                row = conn.execute(
                    "SELECT status, phase, percent, started_at, heartbeat_at, finished_at, "
                    "database_size_before, database_size_after, sources_removed, rows_removed, "
                    "categories_removed, category_items_removed, fts_documents_before, "
                    "fts_documents_after, fts_orphans_before, fts_orphans_after, vacuumed, "
                    "fts_rebuilt, last_error, details "
                    "FROM database_maintenance_progress WHERE id=1"
                ).fetchone()
                if row:
                    for key in (
                        "status",
                        "phase",
                        "percent",
                        "started_at",
                        "heartbeat_at",
                        "finished_at",
                        "database_size_before",
                        "database_size_after",
                        "sources_removed",
                        "rows_removed",
                        "categories_removed",
                        "category_items_removed",
                        "fts_documents_before",
                        "fts_documents_after",
                        "fts_orphans_before",
                        "fts_orphans_after",
                        "vacuumed",
                        "fts_rebuilt",
                        "last_error",
                    ):
                        if key in row.keys():
                            status[key] = row[key]
                    try:
                        details = json.loads(row["details"] or "{}")
                        status["details"] = details if isinstance(details, dict) else {}
                    except (TypeError, json.JSONDecodeError):
                        pass
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("Could not load database maintenance status: %s", exc)

        status["vacuumed"] = bool(status["vacuumed"])
        status["fts_rebuilt"] = bool(status["fts_rebuilt"])
        status["indeterminate"] = status["phase"] in UNINTERRUPTIBLE_PHASES and status["status"] in MAINTENANCE_STATUSES
        return status

    def recover_interrupted(self) -> None:
        """Mark maintenance interrupted by a previous process shutdown."""
        with self._status_lock:
            if self._status.get("status") not in MAINTENANCE_STATUSES:
                return
            status = copy.deepcopy(self._status)
            status.update(
                {
                    "status": "interrupted",
                    "phase": "interrupted",
                    "percent": 0,
                    "finished_at": self._now(),
                    "last_error": "Database maintenance was interrupted by application shutdown",
                }
            )
            self._status = status
        self._persist_status_sync(status)

    def _persist_status_sync(self, status: dict[str, Any]) -> None:
        details = status.get("details", {})
        try:
            conn = db_connect(self.db_path)
            try:
                conn.execute(
                    """INSERT OR REPLACE INTO database_maintenance_progress
                       (id, status, phase, percent, started_at, heartbeat_at,
                        finished_at, database_size_before, database_size_after,
                        sources_removed, rows_removed, categories_removed,
                        category_items_removed, fts_documents_before,
                        fts_documents_after, fts_orphans_before, fts_orphans_after,
                        vacuumed, fts_rebuilt, last_error, details)
                       VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        status.get("status", "idle"),
                        status.get("phase", "idle"),
                        int(status.get("percent", 0) or 0),
                        status.get("started_at"),
                        status.get("heartbeat_at"),
                        status.get("finished_at"),
                        int(status.get("database_size_before", 0) or 0),
                        int(status.get("database_size_after", 0) or 0),
                        int(status.get("sources_removed", 0) or 0),
                        int(status.get("rows_removed", 0) or 0),
                        int(status.get("categories_removed", 0) or 0),
                        int(status.get("category_items_removed", 0) or 0),
                        int(status.get("fts_documents_before", 0) or 0),
                        int(status.get("fts_documents_after", 0) or 0),
                        int(status.get("fts_orphans_before", 0) or 0),
                        int(status.get("fts_orphans_after", 0) or 0),
                        int(bool(status.get("vacuumed", False))),
                        int(bool(status.get("fts_rebuilt", False))),
                        status.get("last_error", ""),
                        json.dumps(details, ensure_ascii=False, default=str),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("Could not persist database maintenance status: %s", exc)

    def _set_status(
        self,
        *,
        status: str | None = None,
        phase: str | None = None,
        percent: int | None = None,
        **updates: Any,
    ) -> None:
        with self._status_lock:
            current = copy.deepcopy(self._status)
            if status is not None:
                current["status"] = status
            if phase is not None:
                current["phase"] = phase
            if percent is not None:
                current["percent"] = max(0, min(100, percent))
            current.update(updates)
            if current.get("status") in MAINTENANCE_STATUSES:
                current["heartbeat_at"] = self._now()
            current["indeterminate"] = (
                current.get("phase") in UNINTERRUPTIBLE_PHASES
                and current.get("status") in MAINTENANCE_STATUSES
            )
            self._status = current
        self._persist_status_sync(current)

    def get_status(self) -> dict[str, Any]:
        with self._status_lock:
            status = copy.deepcopy(self._status)
        try:
            status["database_size_current"] = os.path.getsize(self.db_path)
        except OSError:
            status["database_size_current"] = status.get("database_size_after", 0)
        status["indeterminate"] = (
            status.get("phase") in UNINTERRUPTIBLE_PHASES
            and status.get("status") in MAINTENANCE_STATUSES
        )
        return status

    def is_active(self) -> bool:
        task = self._task
        return task is not None and not task.done()

    def _configured_source_ids(self) -> set[str]:
        config = self.config_service.config
        source_ids = {
            str(source.get("id"))
            for source in config.get("sources", [])
            if source.get("id")
        }
        if not source_ids and config.get("xtream", {}).get("host"):
            source_ids.add("default")
        return source_ids

    def start(self) -> dict[str, Any]:
        """Start maintenance without yielding between ownership checks."""
        if self.is_active():
            return {
                "started": False,
                "status": "already_running",
                "message": "Database maintenance is already running",
            }

        if not self.cache_service.try_begin_maintenance():
            return {
                "started": False,
                "status": "busy",
                "message": "Cache refresh is currently running; try again when it completes",
            }

        started_at = self._now()
        status = self._default_status()
        status.update(
            {
                "status": "running",
                "phase": "preflight",
                "started_at": started_at,
                "heartbeat_at": started_at,
            }
        )
        with self._status_lock:
            self._status = status
        self._persist_status_sync(status)

        stop_event = threading.Event()
        self._stop_event = stop_event
        try:
            self._task = asyncio.create_task(self._run(stop_event, self._configured_source_ids()))
            self._task.add_done_callback(self._task_done)
        except Exception:
            self._stop_event = None
            self.cache_service.end_maintenance()
            self._set_status(
                status="failed",
                phase="preflight",
                percent=0,
                finished_at=self._now(),
                last_error="Could not start database maintenance task",
            )
            raise

        return {
            "started": True,
            "status": "started",
            "message": "Database cleanup and optimization started",
        }

    def _task_done(self, task: asyncio.Task) -> None:
        if self._task is task:
            self._task = None
        try:
            task.result()
        except asyncio.CancelledError:
            logger.info("Database maintenance task cancelled")
        except Exception:
            logger.exception("Unhandled database maintenance task error")

    async def request_stop(self) -> dict[str, Any]:
        if not self.is_active() or self._stop_event is None:
            return {
                "stopped": False,
                "status": "idle",
                "message": "No database maintenance is running",
            }

        self._stop_event.set()
        phase = self.get_status().get("phase", "unknown")
        self._set_status(
            status="stop_requested",
            phase=phase,
            details={**self.get_status().get("details", {}), "stop_requested": True},
        )
        if phase in UNINTERRUPTIBLE_PHASES:
            message = "Stop requested; the current database phase must finish safely"
        else:
            message = "Database maintenance stop requested"
        return {"stopped": True, "status": "stop_requested", "message": message}

    async def shutdown(self) -> None:
        """Wait for the worker to finish without abandoning SQLite work."""
        task = self._task
        if task is not None:
            await task

    async def _run(self, stop_event: threading.Event, source_ids: set[str]) -> None:
        try:
            await asyncio.to_thread(self._run_sync, stop_event, source_ids)
        except asyncio.CancelledError:
            self._set_status(
                status="interrupted",
                phase="interrupted",
                percent=0,
                finished_at=self._now(),
                last_error="Database maintenance task was cancelled",
            )
            raise
        except Exception as exc:
            logger.exception("Database maintenance failed")
            self._set_status(
                status="failed",
                phase=self.get_status().get("phase", "unknown"),
                finished_at=self._now(),
                last_error=str(exc),
            )
        finally:
            self._stop_event = None
            self.cache_service.end_maintenance()

    def _run_sync(self, stop_event: threading.Event, source_ids: set[str]) -> None:
        current_phase = "preflight"
        vacuum_started = False
        try:
            size_before = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            usage = shutil.disk_usage(os.path.dirname(self.db_path) or ".")
            required_free = (size_before * 2) + (64 * 1024 * 1024)
            before = self._database_metrics()
            self._set_status(
                phase=current_phase,
                percent=5,
                database_size_before=size_before,
                database_size_after=size_before,
                fts_documents_before=before["fts_documents"],
                fts_orphans_before=before["fts_orphans"],
                details={
                    "free_space": usage.free,
                    "required_free_space": required_free,
                    "quick_check": before["quick_check"],
                },
            )
            if before["quick_check"] != "ok":
                raise RuntimeError(f"SQLite quick_check failed: {before['quick_check']}")
            if usage.free < required_free:
                raise RuntimeError(
                    f"Not enough free disk space for VACUUM: {usage.free} bytes available, "
                    f"approximately {required_free} required"
                )
            if stop_event.is_set():
                self._finish_cancelled("Stopped before cleanup")
                return

            current_phase = "cleanup"
            self._set_status(phase=current_phase, percent=20)
            cleanup = self._remove_deconfigured_sources(source_ids)
            self._set_status(
                phase=current_phase,
                percent=30,
                sources_removed=cleanup["sources_removed"],
                rows_removed=cleanup["rows_removed"],
                categories_removed=cleanup["categories_removed"],
                category_items_removed=cleanup["category_items_removed"],
                details={
                    **self.get_status().get("details", {}),
                    "configured_source_count": len(source_ids),
                },
            )
            if stop_event.is_set():
                self._finish_cancelled("Stopped after cache cleanup")
                return

            current_phase = "vacuum"
            vacuum_started = True
            self._set_status(phase=current_phase, percent=50)
            self._vacuum()

            current_phase = "fts_rebuild"
            self._set_status(phase=current_phase, percent=75, vacuumed=True)
            self._rebuild_fts()

            current_phase = "optimize"
            self._set_status(phase=current_phase, percent=90, fts_rebuilt=True)
            self._optimize()

            after = self._database_metrics()
            size_after = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            details = {
                **self.get_status().get("details", {}),
                "quick_check_after": after["quick_check"],
                "stop_requested_during_uninterruptible_phase": stop_event.is_set(),
            }
            if after["quick_check"] != "ok":
                raise RuntimeError(f"SQLite quick_check failed after maintenance: {after['quick_check']}")
            if after["fts_orphans"]:
                raise RuntimeError(f"FTS rebuild left {after['fts_orphans']} orphaned documents")
            self._set_status(
                status="succeeded",
                phase="complete",
                percent=100,
                finished_at=self._now(),
                database_size_after=size_after,
                fts_documents_after=after["fts_documents"],
                fts_orphans_after=after["fts_orphans"],
                details=details,
            )
        except Exception as exc:
            self._set_status(
                status="failed",
                phase=current_phase,
                finished_at=self._now(),
                vacuumed=vacuum_started and self.get_status().get("vacuumed", False),
                last_error=str(exc),
            )

    def _finish_cancelled(self, message: str) -> None:
        self._set_status(
            status="cancelled",
            phase="cancelled",
            percent=0,
            finished_at=self._now(),
            last_error=message,
        )

    def _database_metrics(self) -> dict[str, Any]:
        conn = db_connect(self.db_path)
        try:
            quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
            streams = conn.execute("SELECT COUNT(*) FROM streams").fetchone()[0]
            fts_documents = conn.execute("SELECT COUNT(*) FROM streams_fts_docsize").fetchone()[0]
            fts_orphans = conn.execute(
                "SELECT COUNT(*) FROM streams_fts_docsize d "
                "LEFT JOIN streams s ON s.rowid=d.id WHERE s.rowid IS NULL"
            ).fetchone()[0]
            return {
                "quick_check": quick_check,
                "streams": streams,
                "fts_documents": fts_documents,
                "fts_orphans": fts_orphans,
            }
        finally:
            conn.close()

    def _remove_deconfigured_sources(self, source_ids: set[str]) -> dict[str, int]:
        conn = db_connect(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            params = sorted(source_ids)
            if source_ids:
                placeholders = ",".join("?" * len(source_ids))
                source_count = conn.execute(
                    f"""SELECT COUNT(*) FROM (
                            SELECT source_id FROM streams WHERE source_id NOT IN ({placeholders})
                            UNION
                            SELECT source_id FROM source_categories WHERE source_id NOT IN ({placeholders})
                            UNION
                            SELECT source_id FROM source_last_refresh WHERE source_id NOT IN ({placeholders})
                        )""",
                    params * 3,
                ).fetchone()[0]
                streams_cursor = conn.execute(
                    f"DELETE FROM streams WHERE source_id NOT IN ({placeholders})", params
                )
                categories_cursor = conn.execute(
                    f"DELETE FROM source_categories WHERE source_id NOT IN ({placeholders})", params
                )
                source_refresh_cursor = conn.execute(
                    f"DELETE FROM source_last_refresh WHERE source_id NOT IN ({placeholders})", params
                )
                cached_items_cursor = conn.execute(
                    f"DELETE FROM category_cached_items WHERE source_id NOT IN ({placeholders})", params
                )
            else:
                streams_cursor = conn.execute("DELETE FROM streams")
                categories_cursor = conn.execute("DELETE FROM source_categories")
                source_refresh_cursor = conn.execute("DELETE FROM source_last_refresh")
                cached_items_cursor = conn.execute("DELETE FROM category_cached_items")
                source_count = 0
            conn.commit()
            return {
                "sources_removed": source_count,
                "rows_removed": max(0, streams_cursor.rowcount),
                "categories_removed": max(0, categories_cursor.rowcount),
                "category_items_removed": max(0, cached_items_cursor.rowcount),
                "source_refresh_rows_removed": max(0, source_refresh_cursor.rowcount),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _vacuum(self) -> None:
        conn = db_connect(self.db_path)
        try:
            conn.execute("VACUUM")
        finally:
            conn.close()

    def _rebuild_fts(self) -> None:
        conn = db_connect(self.db_path)
        try:
            conn.execute("INSERT INTO streams_fts(streams_fts) VALUES ('rebuild')")
            conn.commit()
        finally:
            conn.close()

    def _optimize(self) -> None:
        conn = db_connect(self.db_path)
        try:
            conn.execute("PRAGMA optimize")
            conn.commit()
        finally:
            conn.close()
