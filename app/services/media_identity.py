"""Provider-independent media identity helpers."""
from __future__ import annotations

import re

from app.services.filter_service import normalize_name


def normalize_tmdb_id(value) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw.startswith("tmdb:"):
        raw = raw[5:].strip()
    return raw if raw.isdigit() and raw != "0" else None


def normalize_imdb_id(value) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw.startswith("imdb:"):
        raw = raw[5:].strip()
    if raw.startswith("tt") and raw[2:].isdigit():
        return raw
    if raw.isdigit():
        return f"tt{raw}"
    return None


def extract_year(data: dict | None, fallback_name: str = "") -> str | None:
    data = data or {}
    for key in ("releasedate", "releaseDate", "release_date", "year"):
        value = str(data.get(key, "")).strip()
        match = re.search(r"(\d{4})", value)
        if match:
            return match.group(1)
    match = re.search(r"(?:^|[\s(\[])((?:19|20)\d{2})(?:$|[\s)\]])", fallback_name)
    return match.group(1) if match else None


def build_media_identity(
    content_type: str,
    data: dict | None = None,
    *,
    name: str = "",
    series_name: str = "",
) -> dict[str, str | None]:
    """Return identity fields for a movie or its parent series."""
    data = data or {}
    title = series_name if content_type == "series" else name
    title = title or data.get("series_name") or data.get("name") or data.get("title") or ""
    return {
        "media_tmdb_id": normalize_tmdb_id(data.get("tmdb_id") or data.get("tmdb")),
        "media_imdb_id": normalize_imdb_id(data.get("imdb_id") or data.get("imdb")),
        "media_title_key": normalize_name(str(title)) if title else None,
        "media_year": extract_year(data, str(title)),
    }


def history_match_sql(stream_alias: str = "s", history_alias: str = "dh") -> str:
    """Build the SQL predicate matching history to a catalog parent item.

    The final source-local branch keeps legacy rows usable when identity
    metadata was unavailable at the time of the download.
    """
    logical_match = f"""
        (
            ({history_alias}.content_type = {stream_alias}.content_type
             AND COALESCE({stream_alias}.tmdb_id, '') <> ''
             AND {history_alias}.media_tmdb_id = {stream_alias}.tmdb_id)
            OR ({history_alias}.content_type = {stream_alias}.content_type
                AND COALESCE({stream_alias}.imdb_id, '') <> ''
                AND {history_alias}.media_imdb_id = {stream_alias}.imdb_id)
            OR (
                {history_alias}.content_type = {stream_alias}.content_type
                AND
                COALESCE({stream_alias}.title_key, '') <> ''
                AND {history_alias}.media_title_key = {stream_alias}.title_key
                AND NOT (
                    COALESCE({stream_alias}.tmdb_id, '') <> ''
                    AND COALESCE({history_alias}.media_tmdb_id, '') <> ''
                    AND {stream_alias}.tmdb_id <> {history_alias}.media_tmdb_id
                )
                AND NOT (
                    COALESCE({stream_alias}.imdb_id, '') <> ''
                    AND COALESCE({history_alias}.media_imdb_id, '') <> ''
                    AND {stream_alias}.imdb_id <> {history_alias}.media_imdb_id
                )
                AND (
                    (
                        COALESCE({stream_alias}.release_year, '') <> ''
                        AND {history_alias}.media_year = {stream_alias}.release_year
                    )
                    OR (
                        COALESCE({stream_alias}.release_year, '') = ''
                        AND COALESCE({history_alias}.media_year, '') = ''
                    )
                )
            )
        )
    """
    source_local = f"""
        (
            ({stream_alias}.content_type = 'vod'
             AND {history_alias}.source_id = {stream_alias}.source_id
             AND {history_alias}.content_type = {stream_alias}.content_type
             AND {history_alias}.stream_id = {stream_alias}.stream_id)
            OR ({stream_alias}.content_type = 'series'
                AND {history_alias}.source_id = {stream_alias}.source_id
                AND {history_alias}.content_type = {stream_alias}.content_type
                AND {history_alias}.series_id = {stream_alias}.stream_id)
        )
    """
    return f"(({logical_match}) OR ({source_local}))"
