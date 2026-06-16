"""Activity log service — records significant app events to SQLite."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from app.database import adb_connect

if TYPE_CHECKING:
    from app.services.config_service import ConfigService

logger = logging.getLogger(__name__)


class LogService:
    """Persists and queries activity logs for the UI."""

    def __init__(self, db_path: str, config_service: ConfigService):
        self.db_path = db_path
        self.config_service = config_service

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def log(
        self,
        category: str,
        level: str,
        message: str,
        details: dict | list | str | None = None,
    ) -> None:
        """Insert a single activity log entry."""
        ts = datetime.now(timezone.utc).isoformat()
        details_json: str | None = None
        if details is not None:
            if isinstance(details, (dict, list)):
                details_json = json.dumps(details, ensure_ascii=False, default=str)
            else:
                details_json = str(details)
        db = await adb_connect(self.db_path)
        try:
            await db.execute(
                "INSERT INTO activity_logs (timestamp, category, level, message, details) VALUES (?, ?, ?, ?, ?)",
                (ts, category, level, message, details_json),
            )
            await db.commit()
        except Exception as exc:
            logger.error(f"Failed to write activity log: {exc}")
        finally:
            await db.close()

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get_logs(
        self,
        category: str | None = None,
        level: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Return log entries, newest first, with optional filters."""
        clauses: list[str] = []
        params: list[object] = []
        if category:
            clauses.append("category = ?")
            params.append(category)
        if level:
            clauses.append("level = ?")
            params.append(level)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])

        db = await adb_connect(self.db_path)
        try:
            db.row_factory = _dict_row_factory
            rows = await db.execute_fetchall(
                f"SELECT id, timestamp, category, level, message, details "
                f"FROM activity_logs {where} ORDER BY id DESC LIMIT ? OFFSET ?",
                params,
            )
        finally:
            await db.close()
        return rows

    async def get_log_counts(self) -> dict[str, int]:
        """Return log counts per category."""
        db = await adb_connect(self.db_path)
        try:
            rows = await db.execute_fetchall(
                "SELECT category, COUNT(*) as cnt FROM activity_logs GROUP BY category"
            )
        finally:
            await db.close()
        counts: dict[str, int] = {"cache": 0, "cart": 0, "monitor": 0}
        for row in rows:
            counts[row[0]] = row[1]
        return counts

    # ------------------------------------------------------------------
    # Delete / Prune
    # ------------------------------------------------------------------

    async def clear_logs(self, category: str | None = None) -> int:
        """Delete logs. Returns number of rows deleted."""
        db = await adb_connect(self.db_path)
        try:
            if category:
                cursor = await db.execute("DELETE FROM activity_logs WHERE category = ?", (category,))
            else:
                cursor = await db.execute("DELETE FROM activity_logs")
            await db.commit()
            return cursor.rowcount
        finally:
            await db.close()

    async def prune_old_logs(self) -> int:
        """Delete entries older than the configured retention period."""
        days = self.config_service.log_retention_days
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        db = await adb_connect(self.db_path)
        try:
            cursor = await db.execute(
                "DELETE FROM activity_logs WHERE timestamp < ?", (cutoff,)
            )
            await db.commit()
            if cursor.rowcount:
                logger.info(f"Pruned {cursor.rowcount} activity log(s) older than {days} days")
            return cursor.rowcount
        except Exception as exc:
            logger.error(f"Failed to prune activity logs: {exc}")
            return 0
        finally:
            await db.close()


def _dict_row_factory(cursor, row):
    """Row factory that returns dicts keyed by column name."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
