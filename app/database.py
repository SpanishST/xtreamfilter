"""SQLite database — schema, connection helpers and migration sentinels.

Usage
-----
Synchronous (all services except async pattern-refresh):
    conn = db_connect(db_path)
    try:
        conn.execute(...)
        conn.commit()
    finally:
        conn.close()

Async (category pattern-refresh hot-path):
    async with aiosqlite.connect(db_path) as conn:
        await _pragma_setup(conn)
        await conn.execute(...)
        await conn.commit()
"""
from __future__ import annotations

import re
import sqlite3
import logging

logger = logging.getLogger(__name__)

DB_NAME = "app.db"


# ---------------------------------------------------------------------------
# Low-level connection helpers
# ---------------------------------------------------------------------------

def _regexp(pattern: str, value: str | None) -> bool:
    """SQLite user function: REGEXP(pattern, string)."""
    if value is None:
        return False
    try:
        return bool(re.search(pattern, value))
    except re.error:
        return False


def db_connect(db_path: str) -> sqlite3.Connection:
    """Return a synchronous :class:`sqlite3.Connection` tuned for performance.

    *Always* called inside a ``try/finally`` or ``with`` block by callers.
    """
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32768")   # 32 MB page cache
    conn.create_function("regexp", 2, _regexp)
    return conn


async def _pragma_setup_async(conn) -> None:
    """Apply the same PRAGMAs for async aiosqlite connections."""
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA cache_size=-32768")
    await conn.create_function("regexp", 2, _regexp)


# ---------------------------------------------------------------------------
# Schema – CREATE TABLE IF NOT EXISTS
# ---------------------------------------------------------------------------

_SCHEMA = """
-- ── API cache ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cache_meta (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    last_refresh TEXT
);

CREATE TABLE IF NOT EXISTS source_last_refresh (
    source_id    TEXT PRIMARY KEY,
    last_refresh TEXT
);

-- One row per stream/series from the upstream API.
-- 'name' and 'category_id' are denormalised columns for fast SQL filtering.
-- 'data' holds the full upstream JSON blob so no API signature changes.
CREATE TABLE IF NOT EXISTS streams (
    source_id    TEXT NOT NULL,
    content_type TEXT NOT NULL,
    stream_id    TEXT NOT NULL,
    name         TEXT,
    category_id  TEXT,
    added        INTEGER,
    data         TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (source_id, content_type, stream_id)
);

CREATE INDEX IF NOT EXISTS idx_streams_source_ct
    ON streams (source_id, content_type);
CREATE INDEX IF NOT EXISTS idx_streams_source_ct_cat
    ON streams (source_id, content_type, category_id);
CREATE INDEX IF NOT EXISTS idx_streams_name_lower
    ON streams (lower(name));

CREATE TABLE IF NOT EXISTS source_categories (
    source_id     TEXT NOT NULL,
    content_type  TEXT NOT NULL,
    category_id   TEXT NOT NULL,
    category_name TEXT,
    data          TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (source_id, content_type, category_id)
);

CREATE INDEX IF NOT EXISTS idx_source_cats_source_ct
    ON source_categories (source_id, content_type);

-- ── Refresh progress ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS refresh_progress (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    in_progress         INTEGER NOT NULL DEFAULT 0,
    current_source      INTEGER NOT NULL DEFAULT 0,
    total_sources       INTEGER NOT NULL DEFAULT 0,
    current_source_name TEXT NOT NULL DEFAULT '',
    current_step        TEXT NOT NULL DEFAULT '',
    percent             INTEGER NOT NULL DEFAULT 0,
    started_at          TEXT
);

INSERT OR IGNORE INTO refresh_progress
    (id, in_progress, current_source, total_sources,
     current_source_name, current_step, percent, started_at)
VALUES (1, 0, 0, 0, '', '', 0, NULL);

-- ── Custom categories ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS custom_categories (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    icon                TEXT NOT NULL DEFAULT '📁',
    mode                TEXT NOT NULL DEFAULT 'manual',
    content_types       TEXT NOT NULL DEFAULT '["live","vod","series"]',
    pattern_logic       TEXT NOT NULL DEFAULT 'and',
    use_source_filters  INTEGER NOT NULL DEFAULT 0,
    notify_telegram     INTEGER NOT NULL DEFAULT 0,
    recently_added_days INTEGER NOT NULL DEFAULT 0,
    last_refresh        TEXT,
    sort_order          INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS category_patterns (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id    TEXT NOT NULL
                   REFERENCES custom_categories(id) ON DELETE CASCADE,
    match_type     TEXT NOT NULL,
    value          TEXT NOT NULL,
    case_sensitive INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_patterns_category
    ON category_patterns (category_id);

CREATE TABLE IF NOT EXISTS category_manual_items (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id  TEXT NOT NULL
                 REFERENCES custom_categories(id) ON DELETE CASCADE,
    stream_id    TEXT NOT NULL,
    source_id    TEXT NOT NULL,
    content_type TEXT NOT NULL,
    added_at     TEXT,
    UNIQUE (category_id, stream_id, source_id, content_type)
);

CREATE INDEX IF NOT EXISTS idx_manual_items_category
    ON category_manual_items (category_id);

CREATE TABLE IF NOT EXISTS category_cached_items (
    category_id  TEXT NOT NULL
                 REFERENCES custom_categories(id) ON DELETE CASCADE,
    stream_id    TEXT NOT NULL,
    source_id    TEXT NOT NULL,
    content_type TEXT NOT NULL,
    PRIMARY KEY (category_id, stream_id, source_id, content_type)
);

CREATE INDEX IF NOT EXISTS idx_cached_items_category
    ON category_cached_items (category_id);

-- ── Cart ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cart_items (
    id                  TEXT PRIMARY KEY,
    stream_id           TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    content_type        TEXT NOT NULL,
    name                TEXT,
    series_name         TEXT,
    season              TEXT,
    episode_num         INTEGER,
    episode_title       TEXT,
    icon                TEXT,
    grp                 TEXT,
    container_extension TEXT,
    added_at            TEXT,
    status              TEXT NOT NULL DEFAULT 'queued',
    progress            REAL NOT NULL DEFAULT 0,
    error               TEXT,
    file_path           TEXT,
    file_size           INTEGER,
    temp_path           TEXT
);

-- ── Monitored series ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS monitored_series (
    id              TEXT PRIMARY KEY,
    series_name     TEXT NOT NULL,
    series_id       TEXT NOT NULL,
    source_id       TEXT,
    source_name     TEXT,
    source_category TEXT,
    cover           TEXT,
    scope           TEXT NOT NULL DEFAULT 'all',
    season_filter   TEXT,
    action          TEXT NOT NULL DEFAULT 'notify',
    enabled         INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT,
    last_checked    TEXT,
    last_new_count  INTEGER NOT NULL DEFAULT 0,
    tmdb_id         TEXT,
    imdb_id         TEXT
);

CREATE TABLE IF NOT EXISTS known_episodes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id   TEXT NOT NULL
                REFERENCES monitored_series(id) ON DELETE CASCADE,
    stream_id   TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    season      TEXT,
    episode_num INTEGER,
    UNIQUE (series_id, stream_id, source_id)
);

CREATE INDEX IF NOT EXISTS idx_known_eps_series
    ON known_episodes (series_id);

CREATE TABLE IF NOT EXISTS downloaded_episodes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id   TEXT NOT NULL
                REFERENCES monitored_series(id) ON DELETE CASCADE,
    stream_id   TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    season      TEXT,
    episode_num INTEGER,
    UNIQUE (series_id, stream_id, source_id)
);

CREATE INDEX IF NOT EXISTS idx_dl_eps_series
    ON downloaded_episodes (series_id);

-- ── EPG meta ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS epg_meta (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    last_refresh TEXT
);

INSERT OR IGNORE INTO epg_meta (id, last_refresh) VALUES (1, NULL);
"""


def init_db(db_path: str) -> None:
    """Create all tables and indexes. Safe to call on every startup (idempotent)."""
    conn = db_connect(db_path)
    try:
        conn.executescript(_SCHEMA)
        conn.commit()
        logger.info(f"Database initialised at {db_path}")
    finally:
        conn.close()
