"""Category service — CRUD and pattern-refresh for custom categories.

Persistence is backed entirely by SQLite (app.db).  The public API is
identical to the original JSON-file version so that routes and tests need
no changes.

Performance hot-path
--------------------
``_refresh_pattern_categories_internal()`` was previously O(categories ×
streams × patterns) in Python.  It is now a SQL INSERT … SELECT on the
``streams`` table, which has proper indexes on (source_id, content_type)
and lower(name).  Typical speedup for categories with 1 000+ matches is
10–100×.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import TYPE_CHECKING

from app.database import DB_NAME, db_connect
from app.models.xtream import CUSTOM_CAT_ID_BASE, ICON_EMOJI_MAP
from app.services.filter_service import should_include

if TYPE_CHECKING:
    from app.services.cache_service import CacheService
    from app.services.config_service import ConfigService
    from app.services.notification_service import NotificationService

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL pattern builder
# ---------------------------------------------------------------------------

def _pattern_to_sql(pattern: dict) -> tuple[str, list]:
    """Convert a PatternRule dict to a ``(sql_fragment, params)`` pair."""
    match_type = pattern.get("match", "contains")
    value = pattern.get("value", "")
    case_sensitive = pattern.get("case_sensitive", False)

    if match_type == "all":
        return "1=1", []

    if match_type in ("contains", "not_contains", "starts_with", "ends_with", "exact"):
        col = "name" if case_sensitive else "lower(name)"
        v = value if case_sensitive else value.lower()
        # Escape LIKE special chars in value
        v_esc = v.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        if match_type == "contains":
            return f"{col} LIKE ? ESCAPE '\\'", [f"%{v_esc}%"]
        elif match_type == "not_contains":
            return f"{col} NOT LIKE ? ESCAPE '\\'", [f"%{v_esc}%"]
        elif match_type == "starts_with":
            return f"{col} LIKE ? ESCAPE '\\'", [f"{v_esc}%"]
        elif match_type == "ends_with":
            return f"{col} LIKE ? ESCAPE '\\'", [f"%{v_esc}"]
        else:  # exact
            return f"{col} = ?", [v]
    elif match_type == "regex":
        if case_sensitive:
            return "regexp(?, name)", [value]
        else:
            return "regexp(?, lower(name))", [value.lower()]
    return "1=0", []


def _build_pattern_where(patterns: list[dict], pattern_logic: str) -> tuple[str, list]:
    if not patterns:
        return "", []
    parts: list[str] = []
    params: list = []
    for p in patterns:
        frag, p_params = _pattern_to_sql(p)
        parts.append(frag)
        params.extend(p_params)
    if not parts:
        return "", []
    joiner = " AND " if pattern_logic == "and" else " OR "
    combined = joiner.join(f"({p})" for p in parts)
    return f"({combined})", params


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

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
        self.db_path = os.path.join(config_service.data_dir, DB_NAME)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _row_to_category(self, conn, row) -> dict:
        """Reconstruct the legacy category dict from DB row + related tables."""
        cat_id = row["id"]
        try:
            content_types = json.loads(row["content_types"])
        except (json.JSONDecodeError, TypeError):
            content_types = ["live", "vod", "series"]

        pat_rows = conn.execute(
            "SELECT match_type, value, case_sensitive FROM category_patterns "
            "WHERE category_id = ? ORDER BY id",
            (cat_id,),
        ).fetchall()
        patterns = [
            {"match": pr["match_type"], "value": pr["value"], "case_sensitive": bool(pr["case_sensitive"])}
            for pr in pat_rows
        ]

        item_rows = conn.execute(
            "SELECT stream_id, source_id, content_type, added_at "
            "FROM category_manual_items WHERE category_id = ? ORDER BY id",
            (cat_id,),
        ).fetchall()
        items = [
            {"id": r["stream_id"], "source_id": r["source_id"],
             "content_type": r["content_type"], "added_at": r["added_at"]}
            for r in item_rows
        ]

        cached_rows = conn.execute(
            "SELECT stream_id, source_id, content_type FROM category_cached_items "
            "WHERE category_id = ? ORDER BY rowid",
            (cat_id,),
        ).fetchall()
        cached_items = [
            {"id": r["stream_id"], "source_id": r["source_id"], "content_type": r["content_type"]}
            for r in cached_rows
        ]

        return {
            "id": cat_id,
            "name": row["name"],
            "icon": row["icon"],
            "mode": row["mode"],
            "content_types": content_types,
            "items": items,
            "patterns": patterns,
            "pattern_logic": row["pattern_logic"],
            "use_source_filters": bool(row["use_source_filters"]),
            "notify_telegram": bool(row["notify_telegram"]),
            "recently_added_days": row["recently_added_days"],
            "cached_items": cached_items,
            "last_refresh": row["last_refresh"],
        }

    def _upsert_category(self, conn, cat: dict, idx: int = 0) -> None:
        cat_id = cat.get("id")
        if not cat_id:
            return
        conn.execute(
            "INSERT OR REPLACE INTO custom_categories "
            "(id, name, icon, mode, content_types, pattern_logic, "
            "use_source_filters, notify_telegram, recently_added_days, last_refresh, sort_order) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                cat_id, cat.get("name", ""), cat.get("icon", "📁"),
                cat.get("mode", "manual"),
                json.dumps(cat.get("content_types", ["live", "vod", "series"])),
                cat.get("pattern_logic", "and"),
                int(cat.get("use_source_filters", False)),
                int(cat.get("notify_telegram", False)),
                int(cat.get("recently_added_days", 0)),
                cat.get("last_refresh"), idx,
            ),
        )
        conn.execute("DELETE FROM category_patterns WHERE category_id = ?", (cat_id,))
        for p in cat.get("patterns", []):
            conn.execute(
                "INSERT INTO category_patterns (category_id, match_type, value, case_sensitive) VALUES (?,?,?,?)",
                (cat_id, p.get("match", "contains"), p.get("value", ""), int(p.get("case_sensitive", False))),
            )
        conn.execute("DELETE FROM category_manual_items WHERE category_id = ?", (cat_id,))
        for item in cat.get("items", []):
            item_id = str(item.get("id", ""))
            if not item_id:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO category_manual_items "
                "(category_id, stream_id, source_id, content_type, added_at) VALUES (?,?,?,?,?)",
                (cat_id, item_id, item.get("source_id", ""), item.get("content_type", ""),
                 item.get("added_at", datetime.now().isoformat())),
            )
        conn.execute("DELETE FROM category_cached_items WHERE category_id = ?", (cat_id,))
        cached_rows = [
            (cat_id, str(i.get("id", "")), i.get("source_id", ""), i.get("content_type", ""))
            for i in cat.get("cached_items", []) if i.get("id")
        ]
        if cached_rows:
            conn.executemany(
                "INSERT OR IGNORE INTO category_cached_items "
                "(category_id, stream_id, source_id, content_type) VALUES (?,?,?,?)",
                cached_rows,
            )

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load_categories(self) -> dict:
        """Return the full categories dict from SQLite (same shape as old JSON)."""
        conn = db_connect(self.db_path)
        try:
            cat_rows = conn.execute(
                "SELECT id, name, icon, mode, content_types, pattern_logic, "
                "use_source_filters, notify_telegram, recently_added_days, last_refresh "
                "FROM custom_categories ORDER BY sort_order, id"
            ).fetchall()

            if not cat_rows:
                # Bootstrap the favorites category on a fresh install
                conn.execute(
                    "INSERT OR IGNORE INTO custom_categories "
                    "(id, name, icon, mode, content_types, pattern_logic, "
                    "use_source_filters, notify_telegram, recently_added_days, last_refresh, sort_order) "
                    "VALUES ('favorites','Favorite streams','❤️','manual',"
                    "'[\"live\",\"vod\",\"series\"]','or',0,0,0,NULL,0)"
                )
                conn.commit()
                return {
                    "categories": [{
                        "id": "favorites", "name": "Favorite streams", "icon": "❤️",
                        "mode": "manual", "content_types": ["live", "vod", "series"],
                        "items": [], "patterns": [], "pattern_logic": "and",
                        "use_source_filters": False, "notify_telegram": False,
                        "recently_added_days": 0, "cached_items": [], "last_refresh": None,
                    }]
                }

            return {"categories": [self._row_to_category(conn, r) for r in cat_rows]}
        except Exception as e:
            logger.error(f"Error loading categories from DB: {e}")
            return {"categories": []}
        finally:
            conn.close()

    def save_categories(self, data: dict) -> None:
        """Write the full categories dict to SQLite (replaces JSON file write)."""
        categories = data.get("categories", [])
        conn = db_connect(self.db_path)
        try:
            new_ids = [c.get("id") for c in categories if c.get("id")]
            if new_ids:
                placeholders = ",".join("?" * len(new_ids))
                conn.execute(
                    f"DELETE FROM custom_categories WHERE id NOT IN ({placeholders})", new_ids
                )
            else:
                conn.execute("DELETE FROM custom_categories")

            for idx, cat in enumerate(categories):
                self._upsert_category(conn, cat, idx)

            conn.commit()
        except Exception as e:
            logger.error(f"Error saving categories to DB: {e}")
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    def get_category_by_id(self, category_id: str) -> dict | None:
        conn = db_connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT id, name, icon, mode, content_types, pattern_logic, "
                "use_source_filters, notify_telegram, recently_added_days, last_refresh "
                "FROM custom_categories WHERE id = ?",
                (category_id,),
            ).fetchone()
            if not row:
                return None
            return self._row_to_category(conn, row)
        except Exception as e:
            logger.error(f"Error fetching category {category_id}: {e}")
            return None
        finally:
            conn.close()

    def is_in_category(self, category_id: str, content_type: str, item_id: str, source_id: str) -> bool:
        conn = db_connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT 1 FROM category_manual_items "
                "WHERE category_id=? AND stream_id=? AND source_id=? AND content_type=?",
                (category_id, item_id, source_id, content_type),
            ).fetchone()
            return row is not None
        except Exception:
            return False
        finally:
            conn.close()

    def get_item_categories(self, content_type: str, item_id: str, source_id: str) -> list[str]:
        conn = db_connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT category_id FROM category_manual_items "
                "WHERE stream_id=? AND source_id=? AND content_type=?",
                (item_id, source_id, content_type),
            ).fetchall()
            return [r["category_id"] for r in rows]
        except Exception:
            return []
        finally:
            conn.close()

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
                return {"name": stream.get("name", "Unknown"),
                        "cover": stream.get("stream_icon", "") or stream.get("cover", "")}
        return {}

    # ------------------------------------------------------------------
    # Xtream virtual category helpers
    # ------------------------------------------------------------------

    def get_custom_cat_id_map(self) -> dict[str, int]:
        conn = db_connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT id FROM custom_categories ORDER BY sort_order, id"
            ).fetchall()
            return {r["id"]: CUSTOM_CAT_ID_BASE + idx for idx, r in enumerate(rows)}
        except Exception:
            return {}
        finally:
            conn.close()

    def custom_cat_id_to_numeric(self, cat_id: str) -> str:
        return str(self.get_custom_cat_id_map().get(cat_id, CUSTOM_CAT_ID_BASE))

    def get_custom_categories_for_content_type(self, content_type: str) -> list[dict]:
        conn = db_connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT id, name, icon, mode, content_types FROM custom_categories ORDER BY sort_order, id"
            ).fetchall()
            result: list[dict] = []
            for row in rows:
                try:
                    cts = json.loads(row["content_types"])
                except (json.JSONDecodeError, TypeError):
                    cts = []
                if content_type not in cts:
                    continue
                if row["mode"] == "manual":
                    items = conn.execute(
                        "SELECT stream_id, source_id, content_type FROM category_manual_items "
                        "WHERE category_id=? AND content_type=?",
                        (row["id"], content_type),
                    ).fetchall()
                else:
                    items = conn.execute(
                        "SELECT stream_id, source_id, content_type FROM category_cached_items "
                        "WHERE category_id=? AND content_type=?",
                        (row["id"], content_type),
                    ).fetchall()
                if items:
                    result.append({
                        "id": row["id"], "name": row["name"], "icon": row["icon"] or "📁",
                        "items": [{"id": i["stream_id"], "source_id": i["source_id"],
                                   "content_type": i["content_type"]} for i in items],
                    })
            return result
        except Exception as e:
            logger.error(f"Error in get_custom_categories_for_content_type: {e}")
            return []
        finally:
            conn.close()

    def get_custom_category_streams(self, content_type: str, custom_cat_id: str) -> list[tuple[str, str]]:
        conn = db_connect(self.db_path)
        try:
            mode_row = conn.execute(
                "SELECT mode FROM custom_categories WHERE id=?", (custom_cat_id,)
            ).fetchone()
            if not mode_row:
                return []
            if mode_row["mode"] == "manual":
                rows = conn.execute(
                    "SELECT source_id, stream_id FROM category_manual_items "
                    "WHERE category_id=? AND content_type=?",
                    (custom_cat_id, content_type),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT source_id, stream_id FROM category_cached_items "
                    "WHERE category_id=? AND content_type=?",
                    (custom_cat_id, content_type),
                ).fetchall()
            return [(r["source_id"], r["stream_id"]) for r in rows]
        except Exception as e:
            logger.error(f"Error in get_custom_category_streams: {e}")
            return []
        finally:
            conn.close()

    @staticmethod
    def get_icon_emoji(icon_name: str) -> str:
        return ICON_EMOJI_MAP.get(icon_name, "")

    # ------------------------------------------------------------------
    # Pattern refresh — SQL hot-path
    # ------------------------------------------------------------------

    def _refresh_pattern_categories_internal(self) -> list[tuple[str, list]]:
        """Refresh automatic categories using indexed SQL queries.

        Old complexity: O(categories × streams × patterns) in Python.
        New complexity: one SQL INSERT…SELECT per category, leveraging
        the ``idx_streams_name_lower`` and ``idx_streams_source_ct`` indexes.
        """
        conn = db_connect(self.db_path)
        notifications_to_send: list[tuple[str, list]] = []
        config = self.config_service.config
        sources_config = {s.get("id"): s for s in config.get("sources", [])}
        current_time = int(time.time())

        try:
            auto_cats = conn.execute(
                "SELECT id, name, content_types, pattern_logic, use_source_filters, "
                "notify_telegram, recently_added_days "
                "FROM custom_categories WHERE mode='automatic'"
            ).fetchall()

            for cat_row in auto_cats:
                cat_id = cat_row["id"]
                recently_added_days = cat_row["recently_added_days"] or 0

                pat_rows = conn.execute(
                    "SELECT match_type, value, case_sensitive FROM category_patterns "
                    "WHERE category_id=? ORDER BY id",
                    (cat_id,),
                ).fetchall()
                patterns = [
                    {"match": r["match_type"], "value": r["value"], "case_sensitive": bool(r["case_sensitive"])}
                    for r in pat_rows
                ]

                if not patterns and recently_added_days <= 0:
                    continue

                try:
                    content_types: list[str] = json.loads(cat_row["content_types"])
                except (json.JSONDecodeError, TypeError):
                    content_types = ["live", "vod", "series"]

                pattern_logic = cat_row["pattern_logic"] or "and"
                use_source_filters = bool(cat_row["use_source_filters"])

                # Build WHERE clause from patterns
                pat_where, pat_params = _build_pattern_where(patterns, pattern_logic)

                ct_placeholders = ",".join("?" * len(content_types))
                conditions = [f"content_type IN ({ct_placeholders})"]
                params: list = list(content_types)

                if pat_where:
                    conditions.append(pat_where)
                    params.extend(pat_params)

                if recently_added_days > 0:
                    conditions.append("added > ?")
                    params.append(current_time - recently_added_days * 86400)

                where_sql = " AND ".join(conditions)

                # Snapshot for new-item detection (only if notifications enabled)
                notify = bool(cat_row["notify_telegram"])
                old_keys: set = set()
                if notify:
                    old_rows = conn.execute(
                        "SELECT stream_id, source_id, content_type FROM category_cached_items "
                        "WHERE category_id=?",
                        (cat_id,),
                    ).fetchall()
                    old_keys = {(r["stream_id"], r["source_id"], r["content_type"]) for r in old_rows}

                # Clear current cached items
                conn.execute(
                    "DELETE FROM category_cached_items WHERE category_id=?", (cat_id,)
                )

                if use_source_filters:
                    # Hybrid: SQL for pattern match → Python for per-source group filters
                    candidates = conn.execute(
                        f"SELECT stream_id, source_id, content_type, category_id AS cat_id "
                        f"FROM streams WHERE {where_sql}",
                        params,
                    ).fetchall()

                    cat_name_cache: dict[tuple, dict] = {}
                    new_rows: list[tuple] = []
                    for row in candidates:
                        src_id = row["source_id"]
                        ct = row["content_type"]
                        if src_id not in sources_config:
                            new_rows.append((cat_id, row["stream_id"], src_id, ct))
                            continue
                        key = (src_id, ct)
                        if key not in cat_name_cache:
                            sc_rows = conn.execute(
                                "SELECT category_id, category_name FROM source_categories "
                                "WHERE source_id=? AND content_type=?",
                                (src_id, ct),
                            ).fetchall()
                            cat_name_cache[key] = {r["category_id"]: r["category_name"] for r in sc_rows}
                        source_cfg = sources_config[src_id]
                        group_filters = source_cfg.get("filters", {}).get(ct, {}).get("groups", [])
                        group_name = cat_name_cache[key].get(str(row["cat_id"]), "")
                        if group_filters and not should_include(group_name, group_filters):
                            continue
                        new_rows.append((cat_id, row["stream_id"], src_id, ct))

                    if new_rows:
                        conn.executemany(
                            "INSERT OR IGNORE INTO category_cached_items "
                            "(category_id, stream_id, source_id, content_type) VALUES (?,?,?,?)",
                            new_rows,
                        )
                    matched_count = len(new_rows)
                else:
                    # Pure SQL — one statement replaces the entire Python loop
                    conn.execute(
                        f"INSERT OR IGNORE INTO category_cached_items "
                        f"(category_id, stream_id, source_id, content_type) "
                        f"SELECT ?, stream_id, source_id, content_type "
                        f"FROM streams WHERE {where_sql}",
                        [cat_id] + params,
                    )
                    matched_count = conn.execute(
                        "SELECT COUNT(*) FROM category_cached_items WHERE category_id=?",
                        (cat_id,),
                    ).fetchone()[0]

                # Telegram notifications
                if notify:
                    new_cached = conn.execute(
                        "SELECT stream_id, source_id, content_type FROM category_cached_items "
                        "WHERE category_id=?",
                        (cat_id,),
                    ).fetchall()
                    truly_new = [
                        r for r in new_cached
                        if (r["stream_id"], r["source_id"], r["content_type"]) not in old_keys
                    ]
                    if truly_new:
                        new_items_for_notif = []
                        for nr in truly_new[:50]:
                            sr = conn.execute(
                                "SELECT name, data FROM streams WHERE source_id=? AND stream_id=?",
                                (nr["source_id"], nr["stream_id"]),
                            ).fetchone()
                            if sr:
                                try:
                                    d = json.loads(sr["data"])
                                except (json.JSONDecodeError, TypeError):
                                    d = {}
                                new_items_for_notif.append({
                                    "name": sr["name"] or "",
                                    "cover": d.get("stream_icon", "") or d.get("cover", ""),
                                    "source_id": nr["source_id"],
                                    "tmdb_id": d.get("tmdb_id") or d.get("tmdb"),
                                    "imdb_id": d.get("imdb_id") or d.get("imdb"),
                                })
                        if new_items_for_notif:
                            cat_name = conn.execute(
                                "SELECT name FROM custom_categories WHERE id=?", (cat_id,)
                            ).fetchone()
                            notifications_to_send.append((
                                cat_name["name"] if cat_name else "Unknown",
                                new_items_for_notif,
                            ))

                conn.execute(
                    "UPDATE custom_categories SET last_refresh=? WHERE id=?",
                    (datetime.now().isoformat(), cat_id),
                )
                logger.info(
                    f"Category '{cat_row['name']}' refreshed via SQL: {matched_count} items"
                )

            conn.commit()
        except Exception as e:
            logger.error(f"Error in pattern category refresh: {e}", exc_info=True)
        finally:
            conn.close()

        return notifications_to_send

    async def refresh_pattern_categories_async(self) -> None:
        notifications = self._refresh_pattern_categories_internal()
        for category_name, new_items in notifications:
            await self.notification_service.send_category_notification(category_name, new_items)

    def refresh_pattern_categories(self) -> None:
        self._refresh_pattern_categories_internal()

