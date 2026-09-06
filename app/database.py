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

import json
import logging
import re
import sqlite3
import time

from app.services.media_identity import build_media_identity

logger = logging.getLogger(__name__)

DB_NAME = "app.db"
MEDIA_IDENTITY_MIGRATION_VERSION = 1


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
    imdb_id      TEXT,
    title_key    TEXT,
    release_year TEXT,
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

-- ── Category autodownload ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS category_autodownload (
    category_id            TEXT PRIMARY KEY
                           REFERENCES custom_categories(id) ON DELETE CASCADE,
    enabled                INTEGER NOT NULL DEFAULT 0,
    movies_enabled         INTEGER NOT NULL DEFAULT 1,
    series_enabled         INTEGER NOT NULL DEFAULT 1,
    source_priority        TEXT NOT NULL DEFAULT '[]',
    series_seasons         TEXT NOT NULL DEFAULT '[]',
    baseline_initialized   INTEGER NOT NULL DEFAULT 0,
    last_run               TEXT,
    last_queued            INTEGER NOT NULL DEFAULT 0,
    last_error             TEXT
);

CREATE TABLE IF NOT EXISTS category_autodownload_targets (
    category_id       TEXT NOT NULL
                      REFERENCES custom_categories(id) ON DELETE CASCADE,
    target_key        TEXT NOT NULL,
    content_type      TEXT NOT NULL,
    title             TEXT,
    media_tmdb_id     TEXT,
    media_imdb_id     TEXT,
    media_title_key   TEXT,
    media_year        TEXT,
    series_id         TEXT,
    season            TEXT,
    episode_num       INTEGER,
    source_id         TEXT,
    stream_id         TEXT,
    status            TEXT NOT NULL DEFAULT 'seen',
    cart_item_id      TEXT,
    first_seen        TEXT,
    last_seen         TEXT,
    completed_at      TEXT,
    last_error        TEXT,
    PRIMARY KEY (category_id, target_key)
);

CREATE INDEX IF NOT EXISTS idx_category_autodownload_targets_status
    ON category_autodownload_targets (category_id, status);

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
    queue_order         INTEGER,
    status              TEXT NOT NULL DEFAULT 'queued',
    progress            REAL NOT NULL DEFAULT 0,
    error               TEXT,
    file_path           TEXT,
    file_size           INTEGER,
    temp_path           TEXT,
    series_id           TEXT,
    destination         TEXT,
    media_tmdb_id       TEXT,
    media_imdb_id       TEXT,
    media_title_key     TEXT,
    media_year          TEXT
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
    completed_at        TEXT NOT NULL,
    media_tmdb_id       TEXT,
    media_imdb_id       TEXT,
    media_title_key     TEXT,
    media_year          TEXT
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
        _backfill_cart_order(conn)
        _backfill_download_history(conn)
        if _get_user_version(conn) < MEDIA_IDENTITY_MIGRATION_VERSION:
            migration_started = time.monotonic()
            logger.warning(
                "Starting one-time media identity migration for the existing catalog. "
                "This can take several minutes; browse will become available when startup completes."
            )
            _backfill_media_identity(conn)
            conn.execute(f"PRAGMA user_version = {MEDIA_IDENTITY_MIGRATION_VERSION}")
            logger.info(
                "Media identity migration completed in %.1f seconds",
                time.monotonic() - migration_started,
            )
        _create_denormalized_browse_indexes(conn)
        _create_identity_indexes(conn)
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


def _create_identity_indexes(conn: sqlite3.Connection) -> None:
    """Create indexes that depend on provider-independent identity columns."""
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_history_media_identity "
        "ON download_history (content_type, media_tmdb_id, media_imdb_id, media_title_key, media_year)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_streams_media_identity "
        "ON streams (content_type, tmdb_id, imdb_id, title_key, release_year)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_history_media_tmdb "
        "ON download_history (content_type, media_tmdb_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_history_media_imdb "
        "ON download_history (content_type, media_imdb_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_download_history_media_title_year "
        "ON download_history (content_type, media_title_key, media_year)"
    )


def _get_user_version(conn: sqlite3.Connection) -> int:
    """Return SQLite's application schema version."""
    return int(conn.execute("PRAGMA user_version").fetchone()[0] or 0)


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
    ("cart_items", "destination", "TEXT"),
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
    ("cart_items", "queue_order", "INTEGER"),
    # Browse denormalization: lets sorting/filtering use plain indexes
    # instead of joining source_categories and parsing JSON per row.
    ("streams", "group_name", "TEXT"),
    ("streams", "rating", "REAL"),
    # Item-card fields so browse pages never touch the wide JSON blob.
    ("streams", "icon", "TEXT"),
    ("streams", "tmdb_id", "TEXT"),
    ("streams", "imdb_id", "TEXT"),
    ("streams", "title_key", "TEXT"),
    ("streams", "release_year", "TEXT"),
    ("streams", "container_ext", "TEXT"),
    ("cart_items", "media_tmdb_id", "TEXT"),
    ("cart_items", "media_imdb_id", "TEXT"),
    ("cart_items", "media_title_key", "TEXT"),
    ("cart_items", "media_year", "TEXT"),
    ("download_history", "media_tmdb_id", "TEXT"),
    ("download_history", "media_imdb_id", "TEXT"),
    ("download_history", "media_title_key", "TEXT"),
    ("download_history", "media_year", "TEXT"),
]


def _apply_column_upgrades(conn: sqlite3.Connection) -> None:
    """Add columns that were introduced after initial schema creation."""
    for table, column, definition in _COLUMN_UPGRADES:
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            logger.info(f"Schema upgrade: added {table}.{column}")


def _backfill_cart_order(conn: sqlite3.Connection) -> None:
    """Give existing cart rows a stable order matching their old FIFO order."""
    rows = conn.execute(
        "SELECT id FROM cart_items WHERE queue_order IS NULL ORDER BY added_at, id"
    ).fetchall()
    if not rows:
        return
    conn.executemany(
        "UPDATE cart_items SET queue_order = ? WHERE id = ?",
        [(index, row[0]) for index, row in enumerate(rows)],
    )
    logger.info(f"Schema upgrade: backfilled queue order for {len(rows)} cart item(s)")


def _backfill_download_history(conn: sqlite3.Connection) -> None:
    """Import completed cart rows into the durable history ledger once."""
    conn.execute(
        """INSERT OR IGNORE INTO download_history
           (cart_item_id, stream_id, source_id, content_type, name, series_name,
            series_id, season, episode_num, episode_title, icon, grp,
            container_extension, file_path, file_size, completed_at,
            media_tmdb_id, media_imdb_id, media_title_key, media_year)
           SELECT id, stream_id, source_id, content_type, name, series_name,
                  series_id, season, episode_num, episode_title, icon, grp,
                  container_extension, file_path, COALESCE(file_size, 0),
                  COALESCE(added_at, datetime('now')), media_tmdb_id,
                  media_imdb_id, media_title_key, media_year
           FROM cart_items
           WHERE status = 'completed' AND file_path IS NOT NULL"""
    )


def _backfill_media_identity(conn: sqlite3.Connection) -> None:
    """Populate provider-independent identity fields for existing rows once.

    The migration deliberately keeps only one bounded batch in Python. It is
    gated by ``PRAGMA user_version`` in ``init_db()``, so the expensive scan is
    not repeated on later startups.
    """
    batch_size = 1000
    progress_interval = 50_000
    processed_streams = 0
    processed_related = 0
    stream_pending_where = """
        content_type IN ('vod', 'series')
        AND (
            (COALESCE(name, '') <> '' AND title_key IS NULL)
            OR (
                json_valid(data)
                AND tmdb_id IS NULL
                AND COALESCE(NULLIF(json_extract(data, '$.tmdb_id'), ''),
                             NULLIF(json_extract(data, '$.tmdb'), '')) IS NOT NULL
            )
            OR (
                json_valid(data)
                AND imdb_id IS NULL
                AND COALESCE(NULLIF(json_extract(data, '$.imdb_id'), ''),
                             NULLIF(json_extract(data, '$.imdb'), '')) IS NOT NULL
            )
            OR (
                json_valid(data)
                AND release_year IS NULL
                AND COALESCE(
                    NULLIF(json_extract(data, '$.releasedate'), ''),
                    NULLIF(json_extract(data, '$.releaseDate'), ''),
                    NULLIF(json_extract(data, '$.release_date'), ''),
                    NULLIF(json_extract(data, '$.year'), ''),
                    ''
                ) <> ''
            )
        )
    """

    last_stream_key: tuple[str, str, str] | None = None
    if conn.execute(f"SELECT EXISTS(SELECT 1 FROM streams WHERE {stream_pending_where})").fetchone()[0]:
        while True:
            keyset = "" if last_stream_key is None else "AND (source_id, content_type, stream_id) > (?, ?, ?)"
            params = () if last_stream_key is None else last_stream_key
            stream_rows = conn.execute(
                "SELECT source_id, content_type, stream_id, name, data, tmdb_id, imdb_id, "
                "title_key, release_year FROM streams "
                f"WHERE {stream_pending_where} {keyset} "
                "ORDER BY source_id, content_type, stream_id LIMIT ?",
                (*params, batch_size),
            ).fetchall()
            if not stream_rows:
                break

            processed_streams += len(stream_rows)
            for row in stream_rows:
                try:
                    data = json.loads(row["data"] or "{}")
                except (TypeError, ValueError):
                    data = {}
                identity = build_media_identity(row["content_type"], data, name=row["name"] or "")
                values = (
                    identity["media_tmdb_id"] or row["tmdb_id"],
                    identity["media_imdb_id"] or row["imdb_id"],
                    identity["media_title_key"] or row["title_key"],
                    identity["media_year"] or row["release_year"],
                )
                current_values = (row["tmdb_id"], row["imdb_id"], row["title_key"], row["release_year"])
                if values != current_values:
                    conn.execute(
                        "UPDATE streams SET tmdb_id=?, imdb_id=?, title_key=?, release_year=? "
                        "WHERE source_id=? AND content_type=? AND stream_id=?",
                        (*values, row["source_id"], row["content_type"], row["stream_id"]),
                    )
            last = stream_rows[-1]
            last_stream_key = (last["source_id"], last["content_type"], last["stream_id"])
            if processed_streams % progress_interval < batch_size:
                logger.info("Media identity migration: processed %d catalog rows", processed_streams)

    for table, id_column in (("cart_items", "id"), ("download_history", "id")):
        last_id = "" if table == "cart_items" else 0
        while True:
            rows = conn.execute(
                f"SELECT t.{id_column} AS identity_row_id, t.source_id, t.content_type, "
                f"t.stream_id, t.series_id, t.name, t.series_name, t.media_tmdb_id, "
                f"t.media_imdb_id, t.media_title_key, t.media_year, s.name AS stream_name, "
                f"s.data AS stream_data, s.tmdb_id AS stream_tmdb_id, "
                f"s.imdb_id AS stream_imdb_id, s.title_key AS stream_title_key, "
                f"s.release_year AS stream_release_year "
                f"FROM {table} t LEFT JOIN streams s ON s.source_id=t.source_id "
                f"AND s.content_type=t.content_type AND s.stream_id="
                f"CASE WHEN t.content_type='vod' THEN t.stream_id ELSE t.series_id END "
                f"WHERE t.content_type IN ('vod', 'series') AND t.{id_column} > ? "
                f"AND ("
                f"(t.media_title_key IS NULL AND COALESCE(t.name, t.series_name, '') <> '') "
                f"OR (t.media_tmdb_id IS NULL AND s.tmdb_id IS NOT NULL) "
                f"OR (t.media_imdb_id IS NULL AND s.imdb_id IS NOT NULL) "
                f"OR (t.media_year IS NULL AND s.release_year IS NOT NULL)"
                f") "
                f"ORDER BY t.{id_column} LIMIT ?",
                (last_id, batch_size),
            ).fetchall()
            if not rows:
                break

            processed_related += len(rows)
            for row in rows:
                try:
                    stream_data = json.loads(row["stream_data"] or "{}")
                except (TypeError, ValueError):
                    stream_data = {}
                stream_identity = {
                    "media_tmdb_id": row["stream_tmdb_id"],
                    "media_imdb_id": row["stream_imdb_id"],
                    "media_title_key": row["stream_title_key"],
                    "media_year": row["stream_release_year"],
                }
                fallback = build_media_identity(
                    row["content_type"],
                    stream_data,
                    name=row["name"] or row["stream_name"] or "",
                    series_name=row["series_name"] or "",
                )
                values = (
                    row["media_tmdb_id"] or stream_identity["media_tmdb_id"] or fallback["media_tmdb_id"],
                    row["media_imdb_id"] or stream_identity["media_imdb_id"] or fallback["media_imdb_id"],
                    row["media_title_key"] or stream_identity["media_title_key"] or fallback["media_title_key"],
                    row["media_year"] or stream_identity["media_year"] or fallback["media_year"],
                )
                current_values = (
                    row["media_tmdb_id"],
                    row["media_imdb_id"],
                    row["media_title_key"],
                    row["media_year"],
                )
                if values != current_values:
                    conn.execute(
                        f"UPDATE {table} SET media_tmdb_id=?, media_imdb_id=?, "
                        f"media_title_key=?, media_year=? WHERE {id_column}=?",
                        (*values, row["identity_row_id"]),
                    )
            last_id = rows[-1]["identity_row_id"]
            if processed_related % progress_interval < batch_size:
                logger.info(
                    "Media identity migration: processed %d cart/history rows",
                    processed_related,
                )
