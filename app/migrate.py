"""One-shot JSON → SQLite migration.

Called automatically at startup when existing JSON data files are present
but the DB does not yet contain the migrated data.  Subsequent startups
are no-ops (idempotent sentinel check).

After a successful migration each source JSON file is renamed to ``.bak``.
``config.json`` is intentionally excluded — it stays as a plain JSON file.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime

from app.database import db_connect

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sentinel helpers
# ---------------------------------------------------------------------------

def _is_migrated(conn: sqlite3.Connection) -> bool:
    """Return True if the DB already contains migrated data.

    We use the presence of any row in ``custom_categories`` *or* the
    ``_migration_done`` key in ``cache_meta`` as the sentinel so that a
    freshly-set-up instance (no JSON files at all) also skips migration.
    """
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM custom_categories"
        )
        if (cur.fetchone() or (0,))[0] > 0:
            return True
        # Check sentinel flag stored in cache_meta id=2
        cur = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='migration_meta'"
        )
        if (cur.fetchone() or (0,))[0] > 0:
            cur = conn.execute("SELECT done FROM migration_meta WHERE id=1")
            row = cur.fetchone()
            if row and row[0]:
                return True
    except Exception:
        pass
    return False


def _mark_migrated(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS migration_meta (id INTEGER PRIMARY KEY, done INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute(
        "INSERT OR REPLACE INTO migration_meta (id, done) VALUES (1, 1)"
    )
    conn.commit()


def _bak(path: str) -> None:
    """Rename path → path.bak safely (skips if already .bak or missing)."""
    bak = path + ".bak"
    if os.path.exists(path) and not os.path.exists(bak):
        os.rename(path, bak)
        logger.info(f"Renamed {path} → {bak}")


# ---------------------------------------------------------------------------
# Per-file migration helpers
# ---------------------------------------------------------------------------

def _migrate_api_cache(conn: sqlite3.Connection, data_dir: str) -> int:
    """Migrate api_cache.json → streams / source_categories / cache_meta."""
    path = os.path.join(data_dir, "api_cache.json")
    if not os.path.exists(path):
        return 0

    logger.info("Migrating api_cache.json …")
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read api_cache.json: {e}")
        return 0

    # Handle legacy flat format (before multi-source)
    if "sources" not in data and "live_streams" in data:
        data = {"sources": {"default": data}, "last_refresh": data.get("last_refresh")}

    last_refresh = data.get("last_refresh")
    if last_refresh:
        conn.execute(
            "INSERT OR REPLACE INTO cache_meta (id, last_refresh) VALUES (1, ?)",
            (last_refresh,),
        )

    stream_rows: list[tuple] = []
    cat_rows: list[tuple] = []

    TYPE_MAP = {
        "live_streams": ("live", "stream_id"),
        "vod_streams": ("vod", "stream_id"),
        "series": ("series", "series_id"),
    }
    CAT_MAP = {
        "live": "live_categories",
        "vod": "vod_categories",
        "series": "series_categories",
    }

    for source_id, src_cache in data.get("sources", {}).items():
        src_refresh = src_cache.get("last_refresh")
        if src_refresh:
            conn.execute(
                "INSERT OR REPLACE INTO source_last_refresh (source_id, last_refresh) VALUES (?, ?)",
                (source_id, src_refresh),
            )

        # Categories
        for content_type, cat_key in CAT_MAP.items():
            for cat in src_cache.get(cat_key, []):
                cat_id = str(cat.get("category_id", ""))
                cat_name = cat.get("category_name", "")
                cat_rows.append((
                    source_id, content_type, cat_id, cat_name, json.dumps(cat),
                ))

        # Streams
        for list_key, (content_type, id_field) in TYPE_MAP.items():
            for stream in src_cache.get(list_key, []):
                stream_id = str(stream.get(id_field, ""))
                if not stream_id:
                    continue
                name = stream.get("name", "")
                category_id = str(stream.get("category_id", ""))
                added_raw = stream.get("added") or stream.get("last_modified", 0)
                try:
                    added = int(added_raw) if added_raw else 0
                except (ValueError, TypeError):
                    added = 0
                stream_rows.append((
                    source_id, content_type, stream_id, name, category_id, added, json.dumps(stream),
                ))

    conn.executemany(
        "INSERT OR IGNORE INTO source_categories "
        "(source_id, content_type, category_id, category_name, data) VALUES (?,?,?,?,?)",
        cat_rows,
    )
    conn.executemany(
        "INSERT OR IGNORE INTO streams "
        "(source_id, content_type, stream_id, name, category_id, added, data) VALUES (?,?,?,?,?,?,?)",
        stream_rows,
    )
    conn.commit()

    total = len(stream_rows)
    logger.info(f"Migrated {len(cat_rows)} categories and {total} streams from api_cache.json")
    _bak(path)
    return total


def _migrate_categories(conn: sqlite3.Connection, data_dir: str) -> int:
    """Migrate categories.json → custom_categories / category_patterns /
       category_manual_items / category_cached_items.
    """
    path = os.path.join(data_dir, "categories.json")
    if not os.path.exists(path):
        # Insert the built-in favorites category so it always exists
        _ensure_favorites(conn)
        return 0

    logger.info("Migrating categories.json …")
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read categories.json: {e}")
        return 0

    categories = data.get("categories", [])
    count = 0
    for idx, cat in enumerate(categories):
        cat_id = cat.get("id")
        if not cat_id:
            continue

        conn.execute(
            """INSERT OR REPLACE INTO custom_categories
               (id, name, icon, mode, content_types, pattern_logic,
                use_source_filters, notify_telegram, recently_added_days,
                last_refresh, sort_order)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                cat_id,
                cat.get("name", ""),
                cat.get("icon", "📁"),
                cat.get("mode", "manual"),
                json.dumps(cat.get("content_types", ["live", "vod", "series"])),
                cat.get("pattern_logic", "and"),
                int(cat.get("use_source_filters", False)),
                int(cat.get("notify_telegram", False)),
                int(cat.get("recently_added_days", 0)),
                cat.get("last_refresh"),
                idx,
            ),
        )

        # Patterns
        for pattern in cat.get("patterns", []):
            conn.execute(
                """INSERT OR IGNORE INTO category_patterns
                   (category_id, match_type, value, case_sensitive)
                   VALUES (?,?,?,?)""",
                (
                    cat_id,
                    pattern.get("match", "contains"),
                    pattern.get("value", ""),
                    int(pattern.get("case_sensitive", False)),
                ),
            )

        # Manual items
        for item in cat.get("items", []):
            item_id = str(item.get("id", ""))
            if not item_id:
                continue
            conn.execute(
                """INSERT OR IGNORE INTO category_manual_items
                   (category_id, stream_id, source_id, content_type, added_at)
                   VALUES (?,?,?,?,?)""",
                (
                    cat_id,
                    item_id,
                    item.get("source_id", ""),
                    item.get("content_type", ""),
                    item.get("added_at", datetime.now().isoformat()),
                ),
            )

        # Cached items (automatic categories)
        cached_rows = [
            (cat_id, str(i.get("id", "")), i.get("source_id", ""), i.get("content_type", ""))
            for i in cat.get("cached_items", [])
            if i.get("id")
        ]
        if cached_rows:
            conn.executemany(
                """INSERT OR IGNORE INTO category_cached_items
                   (category_id, stream_id, source_id, content_type)
                   VALUES (?,?,?,?)""",
                cached_rows,
            )

        count += 1

    conn.commit()
    logger.info(f"Migrated {count} categories from categories.json")
    _bak(path)
    return count


def _ensure_favorites(conn: sqlite3.Connection) -> None:
    """Insert the built-in favorites category if it does not already exist."""
    conn.execute(
        """INSERT OR IGNORE INTO custom_categories
           (id, name, icon, mode, content_types, pattern_logic,
            use_source_filters, notify_telegram, recently_added_days,
            last_refresh, sort_order)
           VALUES ('favorites','Favorite streams','❤️','manual',
                   '["live","vod","series"]','or',0,0,0,NULL,0)"""
    )
    conn.commit()


def _migrate_cart(conn: sqlite3.Connection, data_dir: str) -> int:
    """Migrate cart.json → cart_items."""
    path = os.path.join(data_dir, "cart.json")
    if not os.path.exists(path):
        return 0

    logger.info("Migrating cart.json …")
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read cart.json: {e}")
        return 0

    rows: list[tuple] = []
    for item in data.get("items", []):
        item_id = item.get("id") or str(uuid.uuid4())
        rows.append((
            item_id,
            item.get("stream_id", ""),
            item.get("source_id", ""),
            item.get("content_type", ""),
            item.get("name"),
            item.get("series_name"),
            item.get("season"),
            item.get("episode_num"),
            item.get("episode_title"),
            item.get("icon"),
            item.get("group"),
            item.get("container_extension"),
            item.get("added_at"),
            item.get("status", "queued"),
            float(item.get("progress", 0)),
            item.get("error"),
            item.get("file_path"),
            item.get("file_size"),
            item.get("temp_path"),
        ))

    if rows:
        conn.executemany(
            """INSERT OR REPLACE INTO cart_items
               (id, stream_id, source_id, content_type, name, series_name,
                season, episode_num, episode_title, icon, grp,
                container_extension, added_at, status, progress,
                error, file_path, file_size, temp_path)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        conn.commit()

    logger.info(f"Migrated {len(rows)} cart items from cart.json")
    _bak(path)
    return len(rows)


def _migrate_monitored(conn: sqlite3.Connection, data_dir: str) -> int:
    """Migrate monitored_series.json → monitored_series / known_episodes /
       downloaded_episodes.
    """
    path = os.path.join(data_dir, "monitored_series.json")
    if not os.path.exists(path):
        return 0

    logger.info("Migrating monitored_series.json …")
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read monitored_series.json: {e}")
        return 0

    count = 0
    for entry in data.get("series", []):
        entry_id = entry.get("id")
        if not entry_id:
            continue

        conn.execute(
            """INSERT OR REPLACE INTO monitored_series
               (id, series_name, series_id, source_id, source_name,
                source_category, cover, scope, season_filter, action,
                enabled, created_at, last_checked, last_new_count,
                tmdb_id, imdb_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                entry_id,
                entry.get("series_name", ""),
                entry.get("series_id", ""),
                entry.get("source_id"),
                entry.get("source_name"),
                entry.get("source_category"),
                entry.get("cover"),
                entry.get("scope", "all"),
                entry.get("season_filter"),
                entry.get("action", "notify"),
                int(entry.get("enabled", True)),
                entry.get("created_at"),
                entry.get("last_checked"),
                int(entry.get("last_new_count", 0)),
                entry.get("tmdb_id"),
                entry.get("imdb_id"),
            ),
        )

        for ep in entry.get("known_episodes", []):
            conn.execute(
                """INSERT OR IGNORE INTO known_episodes
                   (series_id, stream_id, source_id, season, episode_num)
                   VALUES (?,?,?,?,?)""",
                (entry_id, ep.get("stream_id", ""), ep.get("source_id", ""),
                 ep.get("season"), ep.get("episode_num")),
            )

        for ep in entry.get("downloaded_episodes", []):
            conn.execute(
                """INSERT OR IGNORE INTO downloaded_episodes
                   (series_id, stream_id, source_id, season, episode_num)
                   VALUES (?,?,?,?,?)""",
                (entry_id, ep.get("stream_id", ""), ep.get("source_id", ""),
                 ep.get("season"), ep.get("episode_num")),
            )

        count += 1

    conn.commit()
    logger.info(f"Migrated {count} monitored series from monitored_series.json")
    _bak(path)
    return count


def _migrate_refresh_progress(conn: sqlite3.Connection, data_dir: str) -> None:
    """Migrate refresh_progress.json → refresh_progress table (best-effort)."""
    path = os.path.join(data_dir, "refresh_progress.json")
    if not os.path.exists(path):
        return

    try:
        with open(path) as f:
            p = json.load(f)
        # Always reset in_progress to False on startup
        conn.execute(
            """INSERT OR REPLACE INTO refresh_progress
               (id, in_progress, current_source, total_sources,
                current_source_name, current_step, percent, started_at)
               VALUES (1, 0, ?, ?, ?, ?, ?, ?)""",
            (
                p.get("current_source", 0),
                p.get("total_sources", 0),
                p.get("current_source_name", ""),
                p.get("current_step", ""),
                p.get("percent", 0),
                p.get("started_at"),
            ),
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"Could not migrate refresh_progress.json: {e}")
    finally:
        _bak(path)


def _migrate_epg_meta(conn: sqlite3.Connection, data_dir: str) -> None:
    """Migrate epg_cache.xml.meta → epg_meta table."""
    path = os.path.join(data_dir, "epg_cache.xml.meta")
    if not os.path.exists(path):
        return

    try:
        with open(path) as f:
            meta = json.load(f)
        conn.execute(
            "INSERT OR REPLACE INTO epg_meta (id, last_refresh) VALUES (1, ?)",
            (meta.get("last_refresh"),),
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"Could not migrate epg_cache.xml.meta: {e}")
    finally:
        _bak(path)


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------

def run_migration_if_needed(db_path: str, data_dir: str) -> None:
    """Run the full JSON → SQLite migration when needed.

    Safe to call unconditionally on every startup:
    - If migration has already been done (sentinel in DB), returns immediately.
    - If no JSON files exist, marks the DB as migrated and returns.
    - Otherwise migrates each JSON file, renames it to .bak, then sets sentinel.
    """
    conn = db_connect(db_path)
    try:
        if _is_migrated(conn):
            logger.debug("SQLite migration already done — skipping")
            return

        logger.info("Starting JSON → SQLite migration …")

        _migrate_api_cache(conn, data_dir)
        cat_count = _migrate_categories(conn, data_dir)
        _migrate_cart(conn, data_dir)
        _migrate_monitored(conn, data_dir)
        _migrate_refresh_progress(conn, data_dir)
        _migrate_epg_meta(conn, data_dir)

        # Ensure the favorites category exists only when no categories were migrated
        if cat_count == 0:
            _ensure_favorites(conn)

        _mark_migrated(conn)
        logger.info("JSON → SQLite migration complete")
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        raise
    finally:
        conn.close()
