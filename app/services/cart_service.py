"""Cart service — download queue management and background download worker."""
from __future__ import annotations

import asyncio
import errno
import json
import logging
import os
import re
import shutil
import subprocess
import time
import unicodedata
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Optional
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

import httpx

from app.database import DB_NAME, db_connect

from app.models.xtream import PLAYER_PROFILES

if TYPE_CHECKING:
    from app.services.config_service import ConfigService
    from app.services.http_client import HttpClientService
    from app.services.notification_service import NotificationService
    from app.services.xtream_service import XtreamService

logger = logging.getLogger(__name__)

DAY_NAMES = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def sanitize_filename(name: str) -> str:
    name = unicodedata.normalize("NFKD", name)
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.strip(".")
    return name or "untitled"


def get_player_headers(profile_key: str = "tivimate") -> dict:
    profile = PLAYER_PROFILES.get(profile_key, PLAYER_PROFILES["tivimate"])
    return dict(profile["headers"])


# ------------------------------------------------------------------
# NFO generation helpers (Jellyfin / Kodi compatible)
# ------------------------------------------------------------------

def _xml_prettify(elem: Element) -> str:
    """Return a pretty-printed XML string with Jellyfin-compatible declaration."""
    rough = tostring(elem, encoding="unicode")
    parsed = minidom.parseString(rough)
    pretty = parsed.toprettyxml(indent="  ", encoding=None)
    # Replace default declaration with Jellyfin/Kodi-compatible one
    pretty = pretty.replace(
        '<?xml version="1.0" ?>',
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
    )
    return pretty


def _str_val(value) -> str:
    """Safely coerce an API value to a string.

    Xtream APIs sometimes return lists where a string is expected.
    """
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(str(v) for v in value if v)
    return str(value)


def _add_text(parent: Element, tag: str, text) -> None:
    """Add a text sub-element if the text is non-empty."""
    t = _str_val(text)
    if t:
        el = SubElement(parent, tag)
        el.text = t.strip()


def _add_uniqueid(parent: Element, provider: str, value: str, default: bool = False) -> None:
    """Add a <uniqueid type="provider"> tag (Kodi/Jellyfin standard)."""
    el = SubElement(parent, "uniqueid")
    el.set("type", provider)
    if default:
        el.set("default", "true")
    el.text = str(value).strip()


def _add_ratings(parent: Element, value: float, name: str = "default", max_val: int = 10) -> None:
    """Add a <ratings> block with proper Jellyfin/Kodi structure."""
    ratings_el = SubElement(parent, "ratings")
    rating_el = SubElement(ratings_el, "rating")
    rating_el.set("name", name)
    rating_el.set("max", str(max_val))
    rating_el.set("default", "true")
    val_el = SubElement(rating_el, "value")
    val_el.text = f"{value:.1f}"


def _add_thumb(parent: Element, url: str, aspect: str = "poster") -> None:
    """Add a <thumb aspect="..."> tag for Jellyfin/Kodi image references."""
    el = SubElement(parent, "thumb")
    el.set("aspect", aspect)
    el.text = url


def _extract_year(info: dict) -> str:
    """Try to extract a 4-digit year from various Xtream info fields."""
    for key in ("releasedate", "releaseDate", "release_date", "year"):
        val = str(info.get(key, "")).strip()
        if val:
            m = re.search(r"(\d{4})", val)
            if m:
                return m.group(1)
    return ""


def generate_movie_nfo(info: dict, name: str = "") -> str:
    """Generate a Jellyfin-compatible movie NFO from Xtream VOD info.

    See https://jellyfin.org/docs/general/server/metadata/nfo/
    Matches the format Jellyfin's own "Nfo saver" produces.
    """
    root = Element("movie")

    # Plot first (matches Jellyfin's own output order)
    _add_text(root, "plot", info.get("plot") or info.get("description"))
    _add_text(root, "lockdata", "false")
    _add_text(root, "dateadded", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    title = info.get("name") or info.get("title") or info.get("o_name") or name
    _add_text(root, "title", title)
    _add_text(root, "originaltitle", info.get("o_name"))

    # Director
    director_str = _str_val(info.get("director"))
    for director in re.split(r"[,/]", director_str):
        director = director.strip()
        if director:
            _add_text(root, "director", director)

    # Rating — Jellyfin's own saver uses bare <rating> tag
    rating_val = info.get("rating") or info.get("rating_5based")
    if rating_val:
        try:
            r = float(rating_val)
            if r > 0:
                if info.get("rating_5based") and not info.get("rating") and r <= 5:
                    r = r * 2
                _add_text(root, "rating", f"{r:.3f}")
        except (ValueError, TypeError):
            pass

    year = _extract_year(info)
    _add_text(root, "year", year)
    _add_text(root, "mpaa", info.get("mpaa") or info.get("certification"))

    # Provider IDs — both flat tags and <uniqueid> for max compatibility
    tmdb_id = info.get("tmdb_id") or info.get("tmdb")
    imdb_id = info.get("imdb_id") or info.get("imdb")
    if imdb_id:
        _add_text(root, "imdbid", str(imdb_id))
    if tmdb_id:
        _add_text(root, "tmdbid", str(tmdb_id))

    # Premiered + releasedate (Jellyfin uses both)
    release_date = ""
    for key in ("releasedate", "releaseDate", "release_date"):
        rd = str(info.get(key, "")).strip()
        if rd:
            release_date = rd
            break
    if release_date:
        _add_text(root, "premiered", release_date)
        _add_text(root, "releasedate", release_date)

    _add_text(root, "runtime", info.get("duration") or info.get("runtime"))
    _add_text(root, "tagline", info.get("tagline"))
    _add_text(root, "country", info.get("country"))

    # Genres
    genre_str = _str_val(info.get("genre") or info.get("category_name"))
    for genre in re.split(r"[,/]", genre_str):
        genre = genre.strip()
        if genre:
            _add_text(root, "genre", genre)

    _add_text(root, "studio", info.get("studio"))

    # Cast
    cast_str = _str_val(info.get("cast") or info.get("actors"))
    for idx, actor_name in enumerate(re.split(r",", cast_str)):
        actor_name = actor_name.strip()
        if actor_name:
            actor_el = SubElement(root, "actor")
            name_el = SubElement(actor_el, "name")
            name_el.text = actor_name
            type_el = SubElement(actor_el, "type")
            type_el.text = "Actor"
            sort_el = SubElement(actor_el, "sortorder")
            sort_el.text = str(idx)

    # IMDb ID as <id> tag (Jellyfin's own saver includes this)
    if imdb_id:
        _add_text(root, "id", str(imdb_id))

    # uniqueid tags for Kodi compatibility
    if imdb_id:
        _add_uniqueid(root, "imdb", str(imdb_id), default=True)
    if tmdb_id:
        _add_uniqueid(root, "tmdb", str(tmdb_id), default=not imdb_id)

    # Poster / fanart — use <thumb> tags (Jellyfin/Kodi URL format)
    cover = _str_val(info.get("cover_big") or info.get("cover") or info.get("movie_image") or info.get("stream_icon"))
    if cover:
        _add_thumb(root, cover, aspect="poster")
    backdrop = _str_val(info.get("backdrop_path") or info.get("backdrop"))
    if backdrop:
        fanart_el = SubElement(root, "fanart")
        thumb_el = SubElement(fanart_el, "thumb")
        thumb_el.text = backdrop

    return _xml_prettify(root)


def generate_tvshow_nfo(info: dict, name: str = "") -> str:
    """Generate a Jellyfin-compatible tvshow.nfo from Xtream series info."""
    root = Element("tvshow")

    _add_text(root, "plot", info.get("plot") or info.get("description"))
    _add_text(root, "lockdata", "false")
    _add_text(root, "dateadded", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    title = info.get("name") or info.get("title") or name
    _add_text(root, "title", title)
    _add_text(root, "originaltitle", info.get("o_name"))

    # Rating
    rating_val = info.get("rating") or info.get("rating_5based")
    if rating_val:
        try:
            r = float(rating_val)
            if r > 0:
                if info.get("rating_5based") and not info.get("rating") and r <= 5:
                    r = r * 2
                _add_text(root, "rating", f"{r:.3f}")
        except (ValueError, TypeError):
            pass

    year = _extract_year(info)
    _add_text(root, "year", year)
    _add_text(root, "mpaa", info.get("mpaa") or info.get("certification"))

    # Provider IDs
    tmdb_id = info.get("tmdb_id") or info.get("tmdb")
    imdb_id = info.get("imdb_id") or info.get("imdb")
    if imdb_id:
        _add_text(root, "imdbid", str(imdb_id))
    if tmdb_id:
        _add_text(root, "tmdbid", str(tmdb_id))

    # Premiered + releasedate
    release_date = ""
    for key in ("releasedate", "releaseDate", "release_date"):
        rd = str(info.get(key, "")).strip()
        if rd:
            release_date = rd
            break
    if release_date:
        _add_text(root, "premiered", release_date)
        _add_text(root, "releasedate", release_date)

    _add_text(root, "runtime", info.get("duration") or info.get("runtime"))

    genre_str = _str_val(info.get("genre") or info.get("category_name"))
    for genre in re.split(r"[,/]", genre_str):
        genre = genre.strip()
        if genre:
            _add_text(root, "genre", genre)

    _add_text(root, "studio", info.get("studio"))

    cast_str = _str_val(info.get("cast") or info.get("actors"))
    for idx, actor_name in enumerate(re.split(r",", cast_str)):
        actor_name = actor_name.strip()
        if actor_name:
            actor_el = SubElement(root, "actor")
            name_el = SubElement(actor_el, "name")
            name_el.text = actor_name
            type_el = SubElement(actor_el, "type")
            type_el.text = "Actor"
            sort_el = SubElement(actor_el, "sortorder")
            sort_el.text = str(idx)

    # <id> tag
    if imdb_id:
        _add_text(root, "id", str(imdb_id))

    # uniqueid tags
    if imdb_id:
        _add_uniqueid(root, "imdb", str(imdb_id), default=True)
    if tmdb_id:
        _add_uniqueid(root, "tmdb", str(tmdb_id), default=not imdb_id)

    # Poster / fanart
    cover = _str_val(info.get("cover") or info.get("cover_big") or info.get("stream_icon"))
    if cover:
        _add_thumb(root, cover, aspect="poster")
    backdrop = _str_val(info.get("backdrop_path") or info.get("backdrop"))
    if backdrop:
        fanart_el = SubElement(root, "fanart")
        thumb_el = SubElement(fanart_el, "thumb")
        thumb_el.text = backdrop

    return _xml_prettify(root)


def generate_episode_nfo(item: dict, ep_info: dict | None = None) -> str:
    """Generate a Jellyfin-compatible episodedetails NFO."""
    root = Element("episodedetails")

    _add_text(root, "lockdata", "false")
    _add_text(root, "dateadded", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    _add_text(root, "title", item.get("episode_title") or item.get("name", ""))
    _add_text(root, "season", str(item.get("season", 1)))
    _add_text(root, "episode", str(item.get("episode_num", 1)))
    _add_text(root, "showtitle", item.get("series_name", ""))

    if ep_info:
        _add_text(root, "plot", ep_info.get("plot") or ep_info.get("description"))
        aired = ep_info.get("air_date") or ep_info.get("releasedate")
        _add_text(root, "aired", aired)
        rating_val = ep_info.get("rating")
        if rating_val:
            try:
                r = float(rating_val)
                if r > 0:
                    _add_text(root, "rating", f"{r:.3f}")
            except (ValueError, TypeError):
                pass
        _add_text(root, "runtime", ep_info.get("duration") or ep_info.get("runtime"))

        # Episode thumbnail
        thumb_url = ep_info.get("movie_image") or ep_info.get("cover_big") or ep_info.get("cover")
        if thumb_url:
            _add_thumb(root, thumb_url, aspect="thumb")

    # Provider IDs at the episode level
    tmdb_id = (ep_info or {}).get("tmdb_id") or item.get("tmdb_id")
    if tmdb_id:
        _add_uniqueid(root, "tmdb", str(tmdb_id), default=True)

    return _xml_prettify(root)


def safe_makedirs(path: str) -> None:
    """Like os.makedirs(path, exist_ok=True) but tolerates ELOOP from CIFS mounts.

    On CIFS/SMB shares the kernel driver can return ELOOP (errno 40) for
    perfectly valid directories — in particular for directories that already
    exist or whose names confuse the CIFS DFS resolver.  The standard
    os.makedirs call fails in that case even though the target is reachable.

    Work-around: when ELOOP is raised, create each path component one at a
    time with os.mkdir, silently ignoring FileExistsError for components that
    are already present.  Nothing is ever removed.
    """
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as exc:
        if exc.errno != errno.ELOOP:
            raise
        logger.debug(
            f"[safe_makedirs] ELOOP on os.makedirs (CIFS quirk), "
            f"falling back to component-wise mkdir for: {path}"
        )
        # Build the path component by component, tolerating already-existing dirs.
        parts = os.path.normpath(path).split(os.sep)
        current = os.sep
        for part in parts:
            if not part:
                continue
            current = os.path.join(current, part)
            try:
                os.mkdir(current)
            except FileExistsError:
                pass  # directory already exists — that's fine
            except OSError as mkdir_exc:
                if mkdir_exc.errno == errno.EEXIST:
                    pass  # race-condition variant of FileExistsError
                else:
                    raise


async def download_poster(url: str, dest_path: str) -> bool:
    """Download a poster image to the given path. Returns True on success."""
    if not url or not url.startswith("http"):
        return False
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200 and len(resp.content) > 100:
                safe_makedirs(os.path.dirname(dest_path))
                with open(dest_path, "wb") as f:
                    f.write(resp.content)
                return True
    except Exception as e:
        logger.debug(f"Failed to download poster {url}: {e}")
    return False


class CartService:
    """Download cart with FIFO queue and background worker."""

    def __init__(
        self,
        config_service: "ConfigService",
        http_client: "HttpClientService",
        notification_service: "NotificationService",
        xtream_service: "XtreamService",
    ):
        self.config_service = config_service
        self.http_client = http_client
        self.notification_service = notification_service
        self.xtream_service = xtream_service
        self.db_path = os.path.join(config_service.data_dir, DB_NAME)

        self._download_cart: list[dict] = []
        self._download_task: Optional[asyncio.Task] = None
        self._download_cancel_event: Optional[asyncio.Event] = None
        self._download_current_item: Optional[dict] = None
        self._download_progress: dict = {
            "bytes_downloaded": 0,
            "total_bytes": 0,
            "speed": 0,
            "eta_speed": 0,
            "paused": False,
            "pause_remaining": 0,
        }
        self._force_started: bool = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def cart(self) -> list[dict]:
        return self._download_cart

    @property
    def download_task(self) -> Optional[asyncio.Task]:
        return self._download_task

    @download_task.setter
    def download_task(self, value):
        self._download_task = value

    @property
    def current_item(self) -> Optional[dict]:
        return self._download_current_item

    @property
    def progress(self) -> dict:
        return self._download_progress

    # ------------------------------------------------------------------
    # Download schedule
    # ------------------------------------------------------------------

    def is_in_download_window(self) -> bool:
        """Check if the current time falls within the configured download window.

        Returns True if schedule is disabled (always allowed) or if we're
        inside the current day's time slot.  Handles overnight windows
        (e.g. 23:00 → 06:00) correctly.
        """
        schedule = self.config_service.get_download_schedule()
        if not schedule.get("enabled", False):
            return True  # No schedule — always allowed

        now = datetime.now()
        day_name = DAY_NAMES[now.weekday()]
        day_cfg = schedule.get(day_name, {})
        if not day_cfg.get("enabled", False):
            return False  # This day is not enabled

        try:
            start_h, start_m = map(int, day_cfg.get("start", "00:00").split(":"))
            end_h, end_m = map(int, day_cfg.get("end", "00:00").split(":"))
        except (ValueError, AttributeError):
            return False

        start_minutes = start_h * 60 + start_m
        end_minutes = end_h * 60 + end_m
        now_minutes = now.hour * 60 + now.minute

        if start_minutes <= end_minutes:
            # Same-day window (e.g. 01:00 → 07:00)
            return start_minutes <= now_minutes < end_minutes
        else:
            # Overnight window (e.g. 23:00 → 06:00)
            return now_minutes >= start_minutes or now_minutes < end_minutes

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load_cart(self) -> list:
        conn = db_connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT id, stream_id, source_id, content_type, name, series_name, "
                "series_id, season, episode_num, episode_title, icon, grp, container_extension, "
                "added_at, status, progress, error, file_path, file_size, temp_path "
                "FROM cart_items ORDER BY added_at"
            ).fetchall()
            self._download_cart = [
                {
                    "id": r["id"],
                    "stream_id": r["stream_id"],
                    "source_id": r["source_id"],
                    "content_type": r["content_type"],
                    "name": r["name"],
                    "series_name": r["series_name"],
                    "series_id": r["series_id"],
                    "season": r["season"],
                    "episode_num": r["episode_num"],
                    "episode_title": r["episode_title"],
                    "icon": r["icon"],
                    "group": r["grp"],
                    "container_extension": r["container_extension"],
                    "added_at": r["added_at"],
                    "status": r["status"],
                    "progress": r["progress"],
                    "error": r["error"],
                    "file_path": r["file_path"],
                    "file_size": r["file_size"],
                    "temp_path": r["temp_path"],
                }
                for r in rows
            ]
            return self._download_cart
        except Exception as e:
            logger.error(f"Error loading cart from DB: {e}")
            self._download_cart = []
            return self._download_cart
        finally:
            conn.close()

    def save_cart(self, items: list | None = None) -> None:
        if items is not None:
            self._download_cart = items
        conn = db_connect(self.db_path)
        try:
            # Full sync: delete removed items, upsert the rest
            current_ids = [i["id"] for i in self._download_cart if i.get("id")]
            if current_ids:
                placeholders = ",".join("?" * len(current_ids))
                conn.execute(
                    f"DELETE FROM cart_items WHERE id NOT IN ({placeholders})",
                    current_ids,
                )
            else:
                conn.execute("DELETE FROM cart_items")

            rows = [
                (
                    i["id"],
                    i.get("stream_id", ""),
                    i.get("source_id", ""),
                    i.get("content_type", ""),
                    i.get("name"),
                    i.get("series_name"),
                    i.get("series_id"),
                    i.get("season"),
                    i.get("episode_num"),
                    i.get("episode_title"),
                    i.get("icon"),
                    i.get("group"),
                    i.get("container_extension"),
                    i.get("added_at"),
                    i.get("status", "queued"),
                    float(i.get("progress", 0)),
                    i.get("error"),
                    i.get("file_path"),
                    i.get("file_size"),
                    i.get("temp_path"),
                )
                for i in self._download_cart
                if i.get("id")
            ]
            if rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO cart_items "
                    "(id, stream_id, source_id, content_type, name, series_name, "
                    "series_id, season, episode_num, episode_title, icon, grp, "
                    "container_extension, added_at, status, progress, "
                    "error, file_path, file_size, temp_path) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    rows,
                )
            conn.commit()
        except Exception as e:
            logger.error(f"Error saving cart to DB: {e}")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    async def _enrich_item_name_from_metadata(self, item: dict) -> None:
        """Fetch metadata from the Xtream API and update the item's name/series_name
        with the proper title (and year) when available.

        This is called *before* building the download path so that filenames and
        directories use the canonical metadata name instead of the raw catalogue
        entry which often contains prefixes, tags or provider-specific labels.
        """
        source_id = item.get("source_id", "")
        content_type = item.get("content_type", "vod")

        try:
            if content_type == "vod":
                stream_id = item.get("stream_id", "")
                info = await self.xtream_service.fetch_vod_info(source_id, stream_id)
                if info:
                    meta_title = _str_val(
                        info.get("name") or info.get("title") or info.get("o_name")
                    ).strip()
                    if meta_title:
                        year = _extract_year(info)
                        if year and year not in meta_title:
                            meta_title = f"{meta_title} ({year})"
                        item["name"] = meta_title
                        # Store info for later NFO generation to avoid a second API call
                        item["_vod_info"] = info
                        logger.info(f"[META] Enriched movie name: '{meta_title}'")

            elif content_type == "series":
                series_id = item.get("series_id", "")
                if series_id:
                    info = await self.xtream_service.fetch_series_info(source_id, series_id)
                    if info:
                        # Always store series info for NFO generation.
                        item["_series_info"] = info
                        # Only override series_name when it is not already set.
                        # The name stored in the cart item comes from the user
                        # (browse page selection or monitor entry) and is the
                        # canonical display name — the stream's info.name may
                        # contain provider-specific tags or formatting.
                        if not item.get("series_name"):
                            meta_title = _str_val(
                                info.get("name") or info.get("title")
                            ).strip()
                            if meta_title:
                                year = _extract_year(info)
                                if year and year not in meta_title:
                                    meta_title = f"{meta_title} ({year})"
                                item["series_name"] = meta_title
                                logger.info(f"[META] Enriched series name: '{meta_title}'")
        except Exception as e:
            logger.debug(f"Could not enrich item name from metadata: {e}")

    def build_download_filepath(self, item: dict, ext_override: str | None = None) -> str:
        base_path = self.config_service.get_download_path()
        ext = ext_override or item.get("container_extension", "mp4")
        name = sanitize_filename(item.get("name", "untitled"))
        if item.get("content_type") == "vod":
            return os.path.join(base_path, "Films", name, f"{name}.{ext}")
        elif item.get("content_type") == "series":
            series_name = sanitize_filename(item.get("series_name", name))
            season = item.get("season", "1")
            episode = item.get("episode_num", 1)
            ep_title = item.get("episode_title", "")
            season_str = f"S{int(season):02d}"
            episode_str = f"E{int(episode):02d}"
            if ep_title and ep_title != name:
                ep_title_clean = sanitize_filename(ep_title)
                filename = f"{series_name} {season_str}{episode_str} - {ep_title_clean}.{ext}"
            else:
                filename = f"{series_name} {season_str}{episode_str}.{ext}"
            return os.path.join(base_path, "Series", series_name, season_str, filename)
        else:
            return os.path.join(base_path, f"{name}.{ext}")

    def build_upstream_url(self, item: dict) -> str | None:
        source = self.config_service.get_source_by_id(item.get("source_id", ""))
        if not source:
            return None
        host = source["host"].rstrip("/")
        username = source["username"]
        password = source["password"]
        stream_id = item.get("stream_id", "")
        ext = item.get("container_extension", "mp4")
        ct = item.get("content_type", "vod")
        if ct == "vod":
            return f"{host}/movie/{username}/{password}/{stream_id}.{ext}"
        elif ct == "series":
            return f"{host}/series/{username}/{password}/{stream_id}.{ext}"
        return None

    # ------------------------------------------------------------------
    # Cart CRUD (used by routes)
    # ------------------------------------------------------------------

    async def add_to_cart(self, data: dict) -> dict:
        """Add item(s) to the cart. Returns {"added": n, "items": [...]} or {"error": ...}."""
        content_type = data.get("content_type", "vod")
        add_mode = data.get("add_mode", "episode")
        added_items: list[dict] = []

        if content_type == "series" and add_mode in ("series", "season"):
            series_id = data.get("series_id", data.get("stream_id", ""))
            source_id = data.get("source_id", "")
            series_name = data.get("series_name", data.get("name", ""))
            season_filter = data.get("season_num") if add_mode == "season" else None
            episodes = await self.xtream_service.fetch_series_episodes(source_id, series_id)
            if not episodes:
                return {"error": "Could not fetch series episodes"}
            for ep in episodes:
                if season_filter and str(ep["season"]) != str(season_filter):
                    continue
                if any(
                    i.get("source_id") == source_id
                    and i.get("stream_id") == ep["stream_id"]
                    and i.get("status") in ("queued", "downloading")
                    for i in self._download_cart
                ):
                    continue
                item = self._build_cart_item(
                    source_id=source_id,
                    stream_id=ep["stream_id"],
                    content_type="series",
                    name=ep.get("title", "") or f"Episode {ep['episode_num']}",
                    series_name=ep.get("series_name", series_name),
                    series_id=series_id,
                    season=ep["season"],
                    episode_num=ep.get("episode_num", 0),
                    episode_title=ep.get("title", ""),
                    episode_info=ep.get("info"),
                    icon=data.get("icon", ""),
                    group=data.get("group", ""),
                    container_extension=ep.get("container_extension", "mp4"),
                )
                self._download_cart.append(item)
                added_items.append(item)
        else:
            source_id = data.get("source_id", "")
            stream_id = data.get("stream_id", "")
            if any(
                i.get("source_id") == source_id
                and i.get("stream_id") == stream_id
                and i.get("status") in ("queued", "downloading")
                for i in self._download_cart
            ):
                return {"error": "Item already in cart"}
            item = self._build_cart_item(
                source_id=source_id,
                stream_id=stream_id,
                content_type=content_type,
                name=data.get("name", ""),
                series_name=data.get("series_name"),
                series_id=data.get("series_id"),
                season=data.get("season"),
                episode_num=data.get("episode_num"),
                episode_title=data.get("episode_title"),
                episode_info=data.get("episode_info"),
                icon=data.get("icon", ""),
                group=data.get("group", ""),
                container_extension=data.get("container_extension", "mp4"),
            )
            self._download_cart.append(item)
            added_items.append(item)

        self.save_cart()
        return {"added": len(added_items), "items": added_items}

    @staticmethod
    def _build_cart_item(**kwargs) -> dict:
        return {
            "id": str(uuid.uuid4()),
            "stream_id": kwargs.get("stream_id", ""),
            "source_id": kwargs.get("source_id", ""),
            "content_type": kwargs.get("content_type", "vod"),
            "name": kwargs.get("name", ""),
            "series_name": kwargs.get("series_name"),
            "series_id": kwargs.get("series_id"),
            "season": kwargs.get("season"),
            "episode_num": kwargs.get("episode_num"),
            "episode_title": kwargs.get("episode_title"),
            "episode_info": kwargs.get("episode_info"),
            "icon": kwargs.get("icon", ""),
            "group": kwargs.get("group", ""),
            "container_extension": kwargs.get("container_extension", "mp4"),
            "added_at": datetime.now().isoformat(),
            "status": "queued",
            "progress": 0,
            "error": None,
            "file_path": None,
            "file_size": None,
        }

    def cancel_download(self) -> bool:
        if self._download_cancel_event:
            self._download_cancel_event.set()
            return True
        return False

    def get_status(self) -> dict:
        queued = len([i for i in self._download_cart if i.get("status") == "queued"])
        downloading = len([i for i in self._download_cart if i.get("status") == "downloading"])
        completed = len([i for i in self._download_cart if i.get("status") == "completed"])
        failed = len([i for i in self._download_cart if i.get("status") in ("failed", "cancelled", "move_failed")])
        current = None
        if self._download_current_item:
            current = {
                "name": self._download_current_item.get("name", ""),
                "series_name": self._download_current_item.get("series_name"),
                "season": self._download_current_item.get("season"),
                "episode_num": self._download_current_item.get("episode_num"),
                "progress": self._download_current_item.get("progress", 0),
                "bytes_downloaded": self._download_progress.get("bytes_downloaded", 0),
                "total_bytes": self._download_progress.get("total_bytes", 0),
                "speed": self._download_progress.get("speed", 0),
                "paused": self._download_progress.get("paused", False),
                "pause_remaining": self._download_progress.get("pause_remaining", 0),
            }
        is_running = self._download_task is not None and not self._download_task.done()
        return {
            "is_running": is_running,
            "queued": queued,
            "downloading": downloading,
            "completed": completed,
            "failed": failed,
            "total": len(self._download_cart),
            "current": current,
        }

    # ------------------------------------------------------------------
    # Download worker
    # ------------------------------------------------------------------

    async def download_worker(self) -> None:
        """Background worker that processes the download queue sequentially."""
        self._download_cancel_event = asyncio.Event()
        await self.http_client.close()
        logger.info("Closed global HTTP client to free provider connection slots")

        while True:
            queued = [item for item in self._download_cart if item.get("status") == "queued"]
            if not queued:
                logger.info("Download queue empty, worker stopping")
                break

            # Check download schedule (skip if force-started by user)
            if not self._force_started and not self.is_in_download_window():
                logger.info("Outside download window, worker pausing until next slot")
                break

            item = queued[0]
            self._download_current_item = item
            self._download_progress = {
                "bytes_downloaded": 0,
                "total_bytes": 0,
                "speed": 0,
                "eta_speed": 0,
                "paused": False,
                "pause_remaining": 0,
            }
            item["status"] = "downloading"
            item["progress"] = 0
            self.save_cart()

            upstream_url = self.build_upstream_url(item)
            if not upstream_url:
                item["status"] = "failed"
                item["error"] = "Could not build upstream URL (source not found)"
                self.save_cart()
                continue

            # Enrich item name from upstream metadata (title, year) before
            # building the file path so directories/filenames are canonical.
            await self._enrich_item_name_from_metadata(item)

            file_path = self.build_download_filepath(item)
            item["file_path"] = file_path
            temp_dir = self.config_service.get_download_temp_path()
            temp_filename = f"{item['id']}_{os.path.basename(file_path)}"
            temp_path = os.path.join(temp_dir, temp_filename)

            try:
                safe_makedirs(os.path.dirname(file_path))
                safe_makedirs(temp_dir)
                throttle = self.config_service.get_download_throttle_settings()
                bw_limit = throttle["bandwidth_limit"] * 1024
                pause_interval = throttle["pause_interval"]
                pause_duration = throttle["pause_duration"]
                player_profile = throttle["player_profile"]
                burst_reconnect = throttle["burst_reconnect"]
                chunk_size = min(64 * 1024, bw_limit) if bw_limit > 0 else 512 * 1024
                player_headers = get_player_headers(player_profile)
                player_headers["Connection"] = "close"

                timeout = httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=30.0)
                max_retries = 5
                retry_base_delay = 30
                RETRYABLE_CODES = {429, 458, 503, 551}

                downloaded = 0
                total = 0
                start_time = time.time()
                last_save = time.time()
                speed_window_duration = 3.0
                speed_samples: list[tuple[float, int]] = []
                eta_speed_window_duration = 20.0
                eta_speed_samples: list[tuple[float, int]] = []
                window_start = time.time()
                window_bytes = 0
                last_pause_time = time.time()
                segment_start_time = time.time()
                reconnect_count = 0
                download_failed = False
                download_complete = False

                for attempt in range(1, max_retries + 1):
                    if download_complete or download_failed:
                        break
                    if attempt > 1:
                        delay = retry_base_delay * attempt
                        self._download_progress["paused"] = True
                        self._download_progress["pause_remaining"] = delay
                        item["error"] = f"Retrying in {delay}s ({attempt}/{max_retries})"
                        self.save_cart()
                        remaining = delay
                        while remaining > 0:
                            if self._download_cancel_event.is_set():
                                item["status"] = "cancelled"
                                item["error"] = "Cancelled by user"
                                self.save_cart()
                                self._download_current_item = None
                                self._download_cancel_event.clear()
                                return
                            await asyncio.sleep(min(1.0, remaining))
                            remaining -= 1
                            self._download_progress["pause_remaining"] = round(remaining, 1)
                        self._download_progress["paused"] = False
                        self._download_progress["pause_remaining"] = 0
                        item["error"] = None

                    while not download_complete and not download_failed:
                        request_headers = dict(player_headers)
                        if downloaded > 0:
                            request_headers["Range"] = f"bytes={downloaded}-"
                        async with httpx.AsyncClient(headers=request_headers, timeout=timeout, follow_redirects=True) as client:
                            async with client.stream("GET", upstream_url) as response:
                                if response.status_code in RETRYABLE_CODES and attempt < max_retries:
                                    break
                                if response.status_code >= 400:
                                    item["status"] = "failed"
                                    item["error"] = f"HTTP {response.status_code}"
                                    self.save_cart()
                                    await self.notification_service.send_download_file_notification(item)
                                    try:
                                        if os.path.exists(temp_path):
                                            os.remove(temp_path)
                                    except OSError:
                                        pass
                                    download_failed = True
                                    break
                                item["error"] = None
                                if total == 0:
                                    content_range = response.headers.get("content-range", "")
                                    if content_range and "/" in content_range:
                                        try:
                                            total = int(content_range.split("/")[-1])
                                        except (ValueError, IndexError):
                                            pass
                                    if not total:
                                        total = int(response.headers.get("content-length", 0))
                                    self._download_progress["total_bytes"] = total

                                segment_start_time = time.time()
                                should_reconnect = False
                                with open(temp_path, "ab") as f:
                                    async for chunk in response.aiter_bytes(chunk_size=chunk_size):
                                        if self._download_cancel_event.is_set():
                                            item["status"] = "cancelled"
                                            item["error"] = "Cancelled by user"
                                            self.save_cart()
                                            try:
                                                os.remove(temp_path)
                                            except OSError:
                                                pass
                                            self._download_current_item = None
                                            self._download_cancel_event.clear()
                                            return
                                        f.write(chunk)
                                        downloaded += len(chunk)
                                        window_bytes += len(chunk)
                                        now_speed = time.time()
                                        speed_samples.append((now_speed, downloaded))
                                        cutoff = now_speed - speed_window_duration
                                        while speed_samples and speed_samples[0][0] < cutoff:
                                            speed_samples.pop(0)
                                        if len(speed_samples) >= 2:
                                            dt = speed_samples[-1][0] - speed_samples[0][0]
                                            db = speed_samples[-1][1] - speed_samples[0][1]
                                            speed = db / dt if dt > 0 else 0
                                        else:
                                            elapsed = now_speed - start_time
                                            speed = downloaded / elapsed if elapsed > 0 else 0
                                        # ETA speed — longer window for stable remaining-time estimate
                                        eta_speed_samples.append((now_speed, downloaded))
                                        eta_cutoff = now_speed - eta_speed_window_duration
                                        while eta_speed_samples and eta_speed_samples[0][0] < eta_cutoff:
                                            eta_speed_samples.pop(0)
                                        if len(eta_speed_samples) >= 2:
                                            eta_dt = eta_speed_samples[-1][0] - eta_speed_samples[0][0]
                                            eta_db = eta_speed_samples[-1][1] - eta_speed_samples[0][1]
                                            eta_speed = eta_db / eta_dt if eta_dt > 0 else speed
                                        else:
                                            eta_speed = speed
                                        self._download_progress["bytes_downloaded"] = downloaded
                                        self._download_progress["speed"] = speed
                                        self._download_progress["eta_speed"] = eta_speed
                                        item["progress"] = round((downloaded / total) * 100, 1) if total > 0 else 0
                                        if time.time() - last_save > 5:
                                            self.save_cart()
                                            last_save = time.time()
                                        if bw_limit > 0:
                                            now = time.time()
                                            window_elapsed = now - window_start
                                            if window_bytes >= bw_limit:
                                                sleep_time = max(0, 1.0 - window_elapsed)
                                                if sleep_time > 0:
                                                    await asyncio.sleep(sleep_time)
                                                window_start = time.time()
                                                window_bytes = 0
                                            elif window_elapsed >= 1.0:
                                                window_start = now
                                                window_bytes = 0
                                        if pause_interval > 0 and pause_duration > 0:
                                            now = time.time()
                                            if now - last_pause_time >= pause_interval:
                                                self._download_progress["paused"] = True
                                                self._download_progress["pause_remaining"] = pause_duration
                                                remaining = pause_duration
                                                while remaining > 0:
                                                    if self._download_cancel_event.is_set():
                                                        break
                                                    await asyncio.sleep(min(1.0, remaining))
                                                    remaining -= 1
                                                    self._download_progress["pause_remaining"] = round(remaining, 1)
                                                self._download_progress["paused"] = False
                                                self._download_progress["pause_remaining"] = 0
                                                last_pause_time = time.time()
                                                window_start = time.time()
                                                window_bytes = 0
                                        if burst_reconnect > 0 and (time.time() - segment_start_time) >= burst_reconnect:
                                            if total == 0 or downloaded < total:
                                                should_reconnect = True
                                                break
                                if should_reconnect:
                                    reconnect_count += 1
                                    speed_samples.clear()
                                    continue
                                download_complete = True
                    if download_complete:
                        break

                if item.get("status") == "downloading" and not download_failed:
                    move_ok = await self._move_temp_to_destination(item, temp_path, file_path)
                    if not move_ok:
                        continue
                    # Write Jellyfin-compatible NFO metadata + poster
                    await self._write_metadata(item, file_path)
                    # Embed metadata directly into the video container (MKV/MP4)
                    await self._embed_container_metadata(item, file_path)
                    await self.notification_service.send_download_file_notification(item)

                if pause_interval > 0 and pause_duration > 0:
                    self._download_progress["paused"] = True
                    self._download_progress["pause_remaining"] = pause_duration
                    remaining = pause_duration
                    while remaining > 0:
                        if self._download_cancel_event.is_set():
                            break
                        await asyncio.sleep(min(1.0, remaining))
                        remaining -= 1
                        self._download_progress["pause_remaining"] = round(remaining, 1)
                    self._download_progress["paused"] = False
                    self._download_progress["pause_remaining"] = 0

            except Exception as e:
                logger.error(f"Download error for {item.get('name')}: {e}")
                item["status"] = "failed"
                item["error"] = str(e)
                self.save_cart()
                await self.notification_service.send_download_file_notification(item)
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except OSError:
                    pass

        self._download_current_item = None
        self._download_progress = {"bytes_downloaded": 0, "total_bytes": 0, "speed": 0, "paused": False, "pause_remaining": 0}
        self._force_started = False
        # Only send queue-complete notification if queue is actually empty
        remaining_queued = [i for i in self._download_cart if i.get("status") == "queued"]
        if not remaining_queued:
            await self.notification_service.send_download_queue_complete_notification(self._download_cart)

    async def _move_temp_to_destination(self, item: dict, temp_path: str, file_path: str) -> bool:
        item_name = item.get("name", "unknown")
        if not os.path.exists(temp_path):
            item["status"] = "move_failed"
            item["error"] = f"Temp file disappeared: {temp_path}"
            item["temp_path"] = temp_path
            self.save_cart()
            return False
        temp_size = os.path.getsize(temp_path)
        if temp_size == 0:
            item["status"] = "move_failed"
            item["error"] = f"Temp file is empty (0 bytes): {temp_path}"
            item["temp_path"] = temp_path
            self.save_cart()
            return False
        dest_dir = os.path.dirname(file_path)
        try:
            safe_makedirs(dest_dir)
        except Exception as e:
            item["status"] = "move_failed"
            item["error"] = f"Cannot create destination directory: {e}"
            item["temp_path"] = temp_path
            self.save_cart()
            return False
        moved = False
        try:
            shutil.move(temp_path, file_path)
            moved = True
        except Exception:
            try:
                shutil.copy2(temp_path, file_path)
                moved = True
            except Exception:
                pass
        if not moved:
            item["status"] = "move_failed"
            item["error"] = "Download OK but failed to move file to destination. Temp file preserved."
            item["temp_path"] = temp_path
            self.save_cart()
            return False
        if not os.path.exists(file_path):
            item["status"] = "move_failed"
            item["error"] = "Destination file missing after move operation"
            item["temp_path"] = temp_path
            self.save_cart()
            return False
        dest_size = os.path.getsize(file_path)
        if dest_size != temp_size:
            try:
                os.remove(file_path)
            except OSError:
                pass
            item["status"] = "move_failed"
            item["error"] = f"Size mismatch after move (temp={temp_size}, dest={dest_size}). Temp file preserved."
            item["temp_path"] = temp_path
            self.save_cart()
            return False
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        item["status"] = "completed"
        item["progress"] = 100
        item["file_size"] = dest_size
        item["file_path"] = file_path
        item.pop("temp_path", None)
        self.save_cart()
        logger.info(f"[MOVE] ✅ Completed: '{item_name}' -> {file_path} ({dest_size / 1024 / 1024:.1f} MB)")
        return True

    # ------------------------------------------------------------------
    # Metadata / NFO writing (Jellyfin-compatible)
    # ------------------------------------------------------------------

    async def _write_metadata(self, item: dict, file_path: str) -> None:
        """Fetch metadata from the Xtream API and write NFO + poster next to the video file.

        For **movies**: writes ``movie.nfo`` and ``poster.jpg`` in the movie folder.
        For **series episodes**: writes ``<Episode>.nfo`` next to the episode, plus
        ``tvshow.nfo`` and ``poster.jpg`` in the series root folder (only if they don't exist yet).
        """
        content_type = item.get("content_type", "vod")
        source_id = item.get("source_id", "")

        nfo_path = os.path.splitext(file_path)[0] + ".nfo"

        try:
            if content_type == "vod":
                await self._write_movie_metadata(item, file_path, nfo_path, source_id)
            elif content_type == "series":
                await self._write_episode_metadata(item, file_path, nfo_path, source_id)
        except Exception as e:
            logger.warning(f"Failed to write metadata for '{item.get('name')}': {e}")

    async def _write_movie_metadata(
        self, item: dict, file_path: str, nfo_path: str, source_id: str
    ) -> None:
        """Fetch VOD info and write movie NFO + poster."""
        stream_id = item.get("stream_id", "")
        # Reuse info cached during name enrichment if available
        info = item.pop("_vod_info", None)
        if not info:
            info = await self.xtream_service.fetch_vod_info(source_id, stream_id)
        if not info:
            logger.debug(f"No VOD info available for stream {stream_id}, skipping NFO")
            return

        # Write movie NFO
        nfo_content = generate_movie_nfo(info, name=item.get("name", ""))
        with open(nfo_path, "w", encoding="utf-8") as f:
            f.write(nfo_content)
        logger.info(f"[META] Wrote movie NFO: {nfo_path}")

        # Download poster into the movie folder.
        # Jellyfin expects: <MovieFolder>/poster.jpg  (or <moviefilename>-poster.jpg)
        cover_url = (
            info.get("cover_big")
            or info.get("cover")
            or info.get("movie_image")
            or info.get("stream_icon")
        )
        if cover_url:
            poster_ext = ".jpg"
            if ".png" in cover_url.lower():
                poster_ext = ".png"
            movie_dir = os.path.dirname(file_path)
            poster_path = os.path.join(movie_dir, "poster" + poster_ext)
            if not os.path.exists(poster_path):
                ok = await download_poster(cover_url, poster_path)
                if ok:
                    logger.info(f"[META] Downloaded poster: {poster_path}")

    async def _write_episode_metadata(
        self, item: dict, file_path: str, nfo_path: str, source_id: str
    ) -> None:
        """Write episode NFO + series-level tvshow.nfo and poster."""
        # Episode-level NFO (uses info already in the cart item)
        ep_info = item.get("episode_info") or {}
        nfo_content = generate_episode_nfo(item, ep_info=ep_info)
        with open(nfo_path, "w", encoding="utf-8") as f:
            f.write(nfo_content)
        logger.info(f"[META] Wrote episode NFO: {nfo_path}")

        # Series-level metadata (tvshow.nfo + poster in the series root folder)
        # Series root = <download_path>/Series/<SeriesName>/
        series_name = sanitize_filename(item.get("series_name", item.get("name", "")))
        base_path = self.config_service.get_download_path()
        series_root = os.path.join(base_path, "Series", series_name)
        tvshow_nfo_path = os.path.join(series_root, "tvshow.nfo")

        if not os.path.exists(tvshow_nfo_path):
            # Reuse info cached during name enrichment if available
            series_id = item.get("series_id", "")
            series_info = item.pop("_series_info", None)
            if not series_info and series_id:
                series_info = await self.xtream_service.fetch_series_info(source_id, series_id)
            if series_info:
                safe_makedirs(series_root)
                tvshow_content = generate_tvshow_nfo(series_info, name=series_name)
                with open(tvshow_nfo_path, "w", encoding="utf-8") as f:
                    f.write(tvshow_content)
                logger.info(f"[META] Wrote tvshow NFO: {tvshow_nfo_path}")

                # Download series poster
                cover_url = (
                    series_info.get("cover")
                    or series_info.get("cover_big")
                    or series_info.get("stream_icon")
                )
                if cover_url:
                    poster_ext = ".jpg"
                    if ".png" in cover_url.lower():
                        poster_ext = ".png"
                    poster_path = os.path.join(series_root, "poster" + poster_ext)
                    if not os.path.exists(poster_path):
                        ok = await download_poster(cover_url, poster_path)
                        if ok:
                            logger.info(f"[META] Downloaded series poster: {poster_path}")

    # ------------------------------------------------------------------
    # Container metadata embedding (MKV / MP4)
    # ------------------------------------------------------------------

    async def _embed_container_metadata(self, item: dict, file_path: str) -> None:
        """Embed metadata tags directly into the video container file.

        - **MKV**: uses ``mkvpropedit`` (in-place, no remux needed).
        - **MP4/other**: uses ``ffmpeg -c copy`` (fast remux, no re-encoding).
        """
        content_type = item.get("content_type", "vod")
        source_id = item.get("source_id", "")

        try:
            # Gather metadata from the Xtream API (reuse cached info when possible)
            meta = await self._gather_embed_metadata(item, content_type, source_id)
            if not meta.get("title"):
                logger.debug(f"No title for container embed, skipping: {file_path}")
                return

            ext = os.path.splitext(file_path)[1].lower()
            if ext == ".mkv":
                await self._embed_mkv_metadata(file_path, meta)
            else:
                await self._embed_ffmpeg_metadata(file_path, meta)
        except Exception as e:
            logger.warning(f"Failed to embed container metadata for '{item.get('name')}': {e}")

    async def _gather_embed_metadata(
        self, item: dict, content_type: str, source_id: str
    ) -> dict:
        """Build a flat metadata dict from Xtream info for container embedding."""
        meta: dict[str, str] = {}

        if content_type == "vod":
            stream_id = item.get("stream_id", "")
            info = await self.xtream_service.fetch_vod_info(source_id, stream_id)
            if info:
                meta["title"] = info.get("name") or info.get("title") or item.get("name", "")
                meta["date"] = str(info.get("releasedate") or info.get("releaseDate") or info.get("release_date") or "")
                year = _extract_year(info)
                if year:
                    meta["year"] = year
                meta["description"] = info.get("plot") or info.get("description") or ""
                meta["genre"] = info.get("genre") or info.get("category_name") or ""
                meta["artist"] = info.get("director") or ""
            else:
                meta["title"] = item.get("name", "")

        elif content_type == "series":
            series_name = item.get("series_name", item.get("name", ""))
            ep_title = item.get("episode_title", "")
            season = item.get("season", 1)
            episode = item.get("episode_num", 1)
            meta["title"] = ep_title or f"S{int(season):02d}E{int(episode):02d}"
            meta["show"] = series_name
            meta["season_number"] = str(season)
            meta["episode_sort"] = str(episode)
            ep_info = item.get("episode_info") or {}
            meta["description"] = ep_info.get("plot") or ep_info.get("description") or ""
            meta["date"] = ep_info.get("air_date") or ep_info.get("releasedate") or ""
        else:
            meta["title"] = item.get("name", "")

        # Strip empty values
        return {k: v for k, v in meta.items() if v}

    async def _embed_mkv_metadata(self, file_path: str, meta: dict) -> None:
        """Embed metadata into an MKV file using mkvpropedit (in-place, no remux)."""
        cmd = ["mkvpropedit", file_path]

        # Segment-level title
        if meta.get("title"):
            cmd += ["--edit", "info", "--set", f"title={meta['title']}"]

        # Date (MKV uses "date" tag at segment level)
        if meta.get("date"):
            cmd += ["--edit", "info", "--set", f"date={meta['date']}"]

        # Tags via XML for richer metadata (genre, description, etc.)
        tags_xml = self._build_mkv_tags_xml(meta)
        if tags_xml:
            import tempfile
            tags_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".xml", delete=False, encoding="utf-8"
            )
            try:
                tags_file.write(tags_xml)
                tags_file.close()
                cmd += ["--tags", f"global:{tags_file.name}"]

                result = await asyncio.to_thread(
                    subprocess.run, cmd,
                    capture_output=True, text=True, timeout=60,
                )
                if result.returncode == 0:
                    logger.info(f"[META] Embedded MKV metadata: {file_path}")
                else:
                    logger.warning(f"mkvpropedit failed ({result.returncode}): {result.stderr.strip()}")
            finally:
                try:
                    os.unlink(tags_file.name)
                except OSError:
                    pass
        else:
            # No tags XML needed, just set the title/date
            result = await asyncio.to_thread(
                subprocess.run, cmd,
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode == 0:
                logger.info(f"[META] Embedded MKV metadata: {file_path}")
            else:
                logger.warning(f"mkvpropedit failed ({result.returncode}): {result.stderr.strip()}")

    @staticmethod
    def _build_mkv_tags_xml(meta: dict) -> str:
        """Build an MKV tags XML string for mkvpropedit --tags."""
        tag_map = {
            "TITLE": meta.get("title", ""),
            "DATE_RELEASED": meta.get("date", ""),
            "GENRE": meta.get("genre", ""),
            "DESCRIPTION": meta.get("description", ""),
            "DIRECTOR": meta.get("artist", ""),
            "SHOW": meta.get("show", ""),
            "SEASON_NUMBER": meta.get("season_number", ""),
            "EPISODE_SORT": meta.get("episode_sort", ""),
        }
        # Filter out empty tags
        tag_map = {k: v for k, v in tag_map.items() if v}
        if not tag_map:
            return ""

        lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<!DOCTYPE Tags SYSTEM \"matroskatags.dtd\">", "<Tags>", "  <Tag>", "    <Targets/>"]
        for name, value in tag_map.items():
            # Escape XML special characters
            value = value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            lines.append(f"    <Simple>")
            lines.append(f"      <Name>{name}</Name>")
            lines.append(f"      <String>{value}</String>")
            lines.append(f"    </Simple>")
        lines += ["  </Tag>", "</Tags>"]
        return "\n".join(lines)

    async def _embed_ffmpeg_metadata(self, file_path: str, meta: dict) -> None:
        """Embed metadata into MP4/other containers using ffmpeg (fast copy-remux)."""
        tmp_path = file_path + ".meta_tmp" + os.path.splitext(file_path)[1]
        cmd = ["ffmpeg", "-y", "-i", file_path, "-c", "copy"]

        # Map metadata keys to ffmpeg metadata tags
        ffmpeg_map = {
            "title": meta.get("title", ""),
            "date": meta.get("date", ""),
            "year": meta.get("year", ""),
            "description": meta.get("description", ""),
            "synopsis": meta.get("description", ""),
            "genre": meta.get("genre", ""),
            "artist": meta.get("artist", ""),
            "show": meta.get("show", ""),
            "season_number": meta.get("season_number", ""),
            "episode_sort": meta.get("episode_sort", ""),
        }
        has_meta = False
        for key, value in ffmpeg_map.items():
            if value:
                cmd += ["-metadata", f"{key}={value}"]
                has_meta = True

        if not has_meta:
            return

        cmd.append(tmp_path)

        result = await asyncio.to_thread(
            subprocess.run, cmd,
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode == 0 and os.path.exists(tmp_path):
            tmp_size = os.path.getsize(tmp_path)
            orig_size = os.path.getsize(file_path)
            # Sanity check: remuxed file should be roughly the same size
            if tmp_size > orig_size * 0.9:
                os.replace(tmp_path, file_path)
                logger.info(f"[META] Embedded container metadata (ffmpeg): {file_path}")
            else:
                os.unlink(tmp_path)
                logger.warning(f"[META] Remuxed file too small ({tmp_size} vs {orig_size}), skipped")
        else:
            logger.warning(f"ffmpeg metadata embed failed ({result.returncode}): {result.stderr.strip()[:200]}")
            if os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
