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
    # Memory-mapped I/O: pages are served from evictable OS file cache,
    # avoiding read() syscalls and copies. The cap bounds address space only;
    # RSS grows solely with pages actually touched.
    conn.execute("PRAGMA mmap_size=1073741824")
    conn.execute("PRAGMA recursive_triggers=ON")
    conn.create_function("regexp", 2, _regexp)
    return conn


async def _pragma_setup_async(conn) -> None:
    """Apply the same PRAGMAs for async aiosqlite connections."""
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA cache_size=-32768")
    await conn.execute("PRAGMA mmap_size=1073741824")
    await conn.execute("PRAGMA recursive_triggers=ON")
    await conn.create_function("regexp", 2, _regexp)


async def adb_connect(db_path: str) -> aiosqlite.Connection:
    """Return an async :class:`aiosqlite.Connection` tuned for performance.

    Usage::

        async with adb_connect(db_path) as conn:
            await conn.execute(...)
            await conn.commit()
    """
    import aiosqlite
    conn = await aiosqlite.connect(db_path, timeout=30)
    conn.row_factory = aiosqlite.Row
    await _pragma_setup_async(conn)
    return conn


class adb_transaction:
    """Async context manager for transactional SQLite operations.

    Usage::

        async with adb_transaction(conn) as tx:
            await tx.execute(...)
        # auto-commits on success, rolls back on exception

    Or with a db_path directly::

        async with adb_transaction(db_path) as tx:
            await tx.execute(...)
    """

    def __init__(self, conn_or_path):
        if isinstance(conn_or_path, str):
            self._db_path = conn_or_path
            self.conn: aiosqlite.Connection | None = None
        else:
            self._db_path = None
            self.conn = conn_or_path
        self._committed = False

    async def __aenter__(self) -> aiosqlite.Connection:
        if self.conn is None:
            self.conn = await adb_connect(self._db_path)
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool | None:
        if self.conn is None:
            return False
        try:
            if exc_type is not None:
                await self.conn.rollback()
                return False
            await self.conn.commit()
            self._committed = True
            return False
        finally:
            await self.conn.close()


def _row_to_dict(row: aiosqlite.Row) -> dict:
    """Convert an aiosqlite.Row to a dict using column names."""
    return {key: row[key] for key in row.keys()}


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
-- 'name', 'category_id', 'added' are denormalised columns for fast SQL filtering.
-- 'group_name'/'rating' are denormalised from source_categories/data JSON so that
-- browse sorting/filtering can use plain indexes instead of joins and json_extract.
-- 'data' holds the full upstream JSON blob so no API signature changes.
CREATE TABLE IF NOT EXISTS streams (
    source_id    TEXT NOT NULL,
    content_type TEXT NOT NULL,
    stream_id    TEXT NOT NULL,
    name         TEXT,
    category_id  TEXT,
    added        INTEGER,
    data         TEXT NOT NULL DEFAULT '{}',
    group_name   TEXT,
    rating       REAL,
    icon         TEXT,
    tmdb_id      TEXT,
    container_ext TEXT,
    PRIMARY KEY (source_id, content_type, stream_id)
);

CREATE INDEX IF NOT EXISTS idx_streams_source_ct
    ON streams (source_id, content_type);
CREATE INDEX IF NOT EXISTS idx_streams_source_ct_cat
    ON streams (source_id, content_type, category_id);
CREATE INDEX IF NOT EXISTS idx_streams_name_lower
    ON streams (lower(name));
CREATE INDEX IF NOT EXISTS idx_streams_ct
    ON streams (content_type);
CREATE INDEX IF NOT EXISTS idx_streams_ct_added
    ON streams (content_type, added);
CREATE INDEX IF NOT EXISTS idx_streams_ct_name
    ON streams (content_type, lower(name));
-- Created after column upgrades in init_db() because older databases gain
-- group_name/rating through _apply_column_upgrades(), which runs post-schema.

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

-- ── Full-text search for stream names ────────────────────────────────────

CREATE VIRTUAL TABLE IF NOT EXISTS streams_fts USING fts5(
    name, content='streams', content_rowid='rowid',
    tokenize='unicode61 remove_diacritics 2'
);

-- Triggers to keep FTS index in sync with the streams table
CREATE TRIGGER IF NOT EXISTS streams_ai AFTER INSERT ON streams BEGIN
    INSERT INTO streams_fts(rowid, name) VALUES (new.rowid, new.name);
END;
CREATE TRIGGER IF NOT EXISTS streams_ad AFTER DELETE ON streams BEGIN
    INSERT INTO streams_fts(streams_fts, rowid, name) VALUES('delete', old.rowid, old.name);
END;
CREATE TRIGGER IF NOT EXISTS streams_au AFTER UPDATE ON streams BEGIN
    INSERT INTO streams_fts(streams_fts, rowid, name) VALUES('delete', old.rowid, old.name);
    INSERT INTO streams_fts(rowid, name) VALUES (new.rowid, new.name);
END;

-- ── Refresh progress ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS refresh_progress (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    in_progress         INTEGER NOT NULL DEFAULT 0,
    current_source      INTEGER NOT NULL DEFAULT 0,
    total_sources       INTEGER NOT NULL DEFAULT 0,
    current_source_name TEXT NOT NULL DEFAULT '',
    current_step        TEXT NOT NULL DEFAULT '',
    percent             INTEGER NOT NULL DEFAULT 0,
    started_at          TEXT,
    status              TEXT NOT NULL DEFAULT 'idle',
    phase               TEXT NOT NULL DEFAULT 'sources',
    source_results      TEXT NOT NULL DEFAULT '[]',
    summary             TEXT NOT NULL DEFAULT '{}',
    finished_at         TEXT,
    last_error          TEXT NOT NULL DEFAULT ''
);

INSERT OR IGNORE INTO refresh_progress
    (id, in_progress, current_source, total_sources,
    current_source_name, current_step, percent, started_at)
VALUES (1, 0, 0, 0, '', '', 0, NULL);

-- ── Database maintenance progress ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS database_maintenance_progress (
    id                      INTEGER PRIMARY KEY CHECK (id = 1),
    status                  TEXT NOT NULL DEFAULT 'idle',
    phase                   TEXT NOT NULL DEFAULT 'idle',
    percent                 INTEGER NOT NULL DEFAULT 0,
    started_at              TEXT,
    heartbeat_at            TEXT,
    finished_at             TEXT,
    database_size_before    INTEGER NOT NULL DEFAULT 0,
    database_size_after     INTEGER NOT NULL DEFAULT 0,
    sources_removed         INTEGER NOT NULL DEFAULT 0,
    rows_removed            INTEGER NOT NULL DEFAULT 0,
    categories_removed      INTEGER NOT NULL DEFAULT 0,
    category_items_removed  INTEGER NOT NULL DEFAULT 0,
    fts_documents_before    INTEGER NOT NULL DEFAULT 0,
    fts_documents_after     INTEGER NOT NULL DEFAULT 0,
    fts_orphans_before      INTEGER NOT NULL DEFAULT 0,
    fts_orphans_after       INTEGER NOT NULL DEFAULT 0,
    vacuumed                INTEGER NOT NULL DEFAULT 0,
    fts_rebuilt             INTEGER NOT NULL DEFAULT 0,
    last_error              TEXT NOT NULL DEFAULT '',
    details                 TEXT NOT NULL DEFAULT '{}'
);

INSERT OR IGNORE INTO database_maintenance_progress
    (id, status, phase, percent)
VALUES (1, 'idle', 'idle', 0);

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

CREATE INDEX IF NOT EXISTS idx_manual_items_stream
    ON category_manual_items (stream_id, source_id, content_type);

CREATE TABLE IF NOT EXISTS category_cached_items (
    category_id  TEXT NOT NULL
                 REFERENCES custom_categories(id) ON DELETE CASCADE,
    stream_id    TEXT NOT NULL,
    source_id    TEXT NOT NULL,
    content_type TEXT NOT NULL,
    PRIMARY KEY (category_id, stream_id, source_id, content_type)
);

-- Note: no separate idx_cached_items_category needed; the PK already
-- covers lookups by category_id prefix.

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
    temp_path           TEXT,
    series_id           TEXT
);

-- Durable record of files that reached their final destination. This is
-- intentionally independent from cart_items and monitored-series state.
CREATE TABLE IF NOT EXISTS download_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    cart_item_id        TEXT NOT NULL UNIQUE,
    stream_id           TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    content_type        TEXT NOT NULL,
    name                TEXT,
    series_name         TEXT,
    series_id           TEXT,
    season              TEXT,
    episode_num         INTEGER,
    episode_title       TEXT,
    icon                TEXT,
    grp                 TEXT,
    container_extension TEXT,
    file_path           TEXT NOT NULL,
    file_size           INTEGER NOT NULL DEFAULT 0,
    completed_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_download_history_movie
    ON download_history (source_id, content_type, stream_id);
CREATE INDEX IF NOT EXISTS idx_download_history_series
    ON download_history (source_id, content_type, series_id, season, episode_num);
CREATE INDEX IF NOT EXISTS idx_download_history_completed_at
    ON download_history (completed_at DESC);

-- ── Monitored series ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS monitored_series (
    id              TEXT PRIMARY KEY,
    series_name     TEXT NOT NULL,
    canonical_name  TEXT,
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

-- Multiple source+category slots per monitored series entry.
-- When this table has rows for a given series_id, the monitoring check
-- iterates over these sources instead of performing a fuzzy cross-source search.
CREATE TABLE IF NOT EXISTS monitor_sources (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id   TEXT NOT NULL
                REFERENCES monitored_series(id) ON DELETE CASCADE,
    source_id   TEXT NOT NULL,
    series_ref  TEXT NOT NULL,
    source_name TEXT,
    category    TEXT,
    series_name TEXT,
    UNIQUE (series_id, source_id, series_ref)
);

CREATE INDEX IF NOT EXISTS idx_monitor_sources_series
    ON monitor_sources (series_id);

-- ── Monitored movies ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS monitored_movies (
    id              TEXT PRIMARY KEY,
    movie_name      TEXT NOT NULL,
    canonical_name  TEXT,
    tmdb_id         TEXT,
    imdb_id         TEXT,
    cover           TEXT,
    action          TEXT NOT NULL DEFAULT 'notify',
    enabled         INTEGER NOT NULL DEFAULT 1,
    status          TEXT NOT NULL DEFAULT 'watching',
    created_at      TEXT,
    last_checked    TEXT
);

-- One source slot per monitored-movie rule.
-- category_filter is the Xtream category_id to restrict VOD search on that source.
CREATE TABLE IF NOT EXISTS movie_monitor_sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id        TEXT NOT NULL
                    REFERENCES monitored_movies(id) ON DELETE CASCADE,
    source_id       TEXT NOT NULL,
    source_name     TEXT,
    category_filter TEXT,
    UNIQUE (movie_id, source_id)
);

CREATE INDEX IF NOT EXISTS idx_movie_monitor_sources_movie
    ON movie_monitor_sources (movie_id);

-- ── EPG meta ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS epg_meta (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    last_refresh TEXT
);

INSERT OR IGNORE INTO epg_meta (id, last_refresh) VALUES (1, NULL);

-- ── Activity logs ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS activity_logs (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    category  TEXT NOT NULL,
    level     TEXT NOT NULL,
    message   TEXT NOT NULL,
    details   TEXT
);

CREATE INDEX IF NOT EXISTS idx_activity_logs_cat_ts
    ON activity_logs (category, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_activity_logs_ts
    ON activity_logs (timestamp DESC);
"""


def init_db(db_path: str) -> None:
    """Create all tables and indexes. Safe to call on every startup (idempotent)."""
    conn = db_connect(db_path)
    try:
        conn.executescript(_SCHEMA)
        # Idempotent column additions for databases that pre-date the column.
        _apply_column_upgrades(conn)
        _backfill_download_history(conn)
        _create_denormalized_browse_indexes(conn)
        _backfill_streams_denormalized(conn)
        conn.commit()
        logger.info(f"Database initialised at {db_path}")
    finally:
        conn.close()


_BROWSE_DENORMALIZED_INDEXES: list[tuple[str, str]] = [
    (
        "idx_streams_ct_group_name",
        "CREATE INDEX IF NOT EXISTS idx_streams_ct_group_name "
        "ON streams (content_type, lower(group_name), lower(name))",
    ),
    (
        "idx_streams_ct_rating",
        "CREATE INDEX IF NOT EXISTS idx_streams_ct_rating "
        "ON streams (content_type, rating, source_id, stream_id)",
    ),
]


def _create_denormalized_browse_indexes(conn: sqlite3.Connection) -> None:
    """Create browse-sort indexes that depend on upgraded columns."""
    for name, statement in _BROWSE_DENORMALIZED_INDEXES:
        existing = {row[1] for row in conn.execute("PRAGMA index_list(streams)")}
        if name not in existing:
            conn.execute(statement)
            logger.info(f"Schema upgrade: created index {name}")


def backfill_streams_denormalized_sql() -> str:
    """SQL that fills group_name/rating from the authoritative sources.

    group_name resolves through source_categories exactly like the old
    LEFT JOIN ... COALESCE(category_name, 'Unknown') behaviour; rating is
    parsed once from the stored JSON blob. Only rows written before
    denormalization (group_name IS NULL) are touched, so the probe-and-update
    is a no-op on healthy databases.
    """
    return """
        UPDATE streams SET
            group_name = COALESCE((
                SELECT sc.category_name FROM source_categories sc
                WHERE sc.source_id = streams.source_id
                  AND sc.content_type = streams.content_type
                  AND sc.category_id = streams.category_id
            ), 'Unknown'),
            rating = CASE WHEN json_valid(streams.data)
                THEN COALESCE(CAST(json_extract(streams.data, '$.rating') AS REAL), 0)
                ELSE 0 END,
            icon = COALESCE(
                NULLIF(json_extract(streams.data, '$.stream_icon'), ''),
                json_extract(streams.data, '$.cover'), ''),
            tmdb_id = COALESCE(
                NULLIF(json_extract(streams.data, '$.tmdb_id'), ''),
                json_extract(streams.data, '$.tmdb')),
            container_ext = CASE WHEN streams.content_type = 'vod'
                THEN COALESCE(NULLIF(json_extract(streams.data, '$.container_extension'), ''), 'mp4')
                ELSE '' END
        WHERE group_name IS NULL OR icon IS NULL OR container_ext IS NULL
    """


def _backfill_streams_denormalized(conn: sqlite3.Connection) -> None:
    """Populate denormalized columns for rows predating the migrations.

    The probe treats NULL as the not-yet-backfilled marker. tmdb_id and
    rating are excluded because they are legitimately NULL/absent upstream;
    icon/container_ext/group_name are always written as non-NULL strings by
    both the refresh path and this backfill.
    """
    missing = conn.execute(
        "SELECT EXISTS(SELECT 1 FROM streams "
        "WHERE group_name IS NULL OR icon IS NULL OR container_ext IS NULL)"
    ).fetchone()[0]
    if not missing:
        return
    conn.execute(backfill_streams_denormalized_sql())
    logger.info("Schema upgrade: backfilled streams denormalized columns")


_COLUMN_UPGRADES: list[tuple[str, str, str]] = [
    # (table, column, definition)
    ("cart_items", "series_id", "TEXT"),
    ("monitor_sources", "series_name", "TEXT"),
    ("monitored_series", "canonical_name", "TEXT"),
    ("monitored_series", "custom_category_ids", "TEXT"),
    ("monitored_movies", "custom_category_ids", "TEXT"),
    ("refresh_progress", "status", "TEXT NOT NULL DEFAULT 'idle'"),
    ("refresh_progress", "phase", "TEXT NOT NULL DEFAULT 'sources'"),
    ("refresh_progress", "source_results", "TEXT NOT NULL DEFAULT '[]'"),
    ("refresh_progress", "summary", "TEXT NOT NULL DEFAULT '{}'"),
    ("refresh_progress", "finished_at", "TEXT"),
    ("refresh_progress", "last_error", "TEXT NOT NULL DEFAULT ''"),
    ("refresh_progress", "heartbeat_at", "TEXT"),
    ("cart_items", "monitor_canonical", "TEXT"),
    ("cart_items", "expected_size", "INTEGER"),
    ("cart_items", "retried_once", "INTEGER NOT NULL DEFAULT 0"),
    # Browse denormalization: lets sorting/filtering use plain indexes
    # instead of joining source_categories and parsing JSON per row.
    ("streams", "group_name", "TEXT"),
    ("streams", "rating", "REAL"),
    # Item-card fields so browse pages never touch the wide JSON blob.
    ("streams", "icon", "TEXT"),
    ("streams", "tmdb_id", "TEXT"),
    ("streams", "container_ext", "TEXT"),
]


def _apply_column_upgrades(conn: sqlite3.Connection) -> None:
    """Add columns that were introduced after initial schema creation."""
    for table, column, definition in _COLUMN_UPGRADES:
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            logger.info(f"Schema upgrade: added {table}.{column}")


def _backfill_download_history(conn: sqlite3.Connection) -> None:
    """Import completed cart rows into the durable history ledger once."""
    conn.execute(
        """INSERT OR IGNORE INTO download_history
           (cart_item_id, stream_id, source_id, content_type, name, series_name,
            series_id, season, episode_num, episode_title, icon, grp,
            container_extension, file_path, file_size, completed_at)
           SELECT id, stream_id, source_id, content_type, name, series_name,
                  series_id, season, episode_num, episode_title, icon, grp,
                  container_extension, file_path, COALESCE(file_size, 0),
                  COALESCE(added_at, datetime('now'))
           FROM cart_items
           WHERE status = 'completed' AND file_path IS NOT NULL"""
    )
