"""Cart service — download queue management and background download worker."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import time
import unicodedata
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Optional
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

import httpx

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
    """Return a pretty-printed XML string with declaration."""
    rough = tostring(elem, encoding="unicode")
    parsed = minidom.parseString(rough)
    return parsed.toprettyxml(indent="  ", encoding=None)


def _add_text(parent: Element, tag: str, text: str | None) -> None:
    """Add a text sub-element if the text is non-empty."""
    if text:
        el = SubElement(parent, tag)
        el.text = str(text).strip()


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
    """
    root = Element("movie")
    title = info.get("name") or info.get("title") or info.get("o_name") or name
    _add_text(root, "title", title)
    _add_text(root, "originaltitle", info.get("o_name"))
    year = _extract_year(info)
    _add_text(root, "year", year)
    _add_text(root, "plot", info.get("plot") or info.get("description"))
    _add_text(root, "tagline", info.get("tagline"))
    _add_text(root, "runtime", info.get("duration") or info.get("runtime"))

    # Rating
    rating_val = info.get("rating") or info.get("rating_5based")
    if rating_val:
        try:
            r = float(rating_val)
            if r > 0:
                _add_text(root, "rating", str(r))
        except (ValueError, TypeError):
            pass

    # TMDb / IMDB IDs — Jellyfin uses these for matching
    tmdb_id = info.get("tmdb_id") or info.get("tmdb")
    if tmdb_id:
        _add_text(root, "tmdbid", str(tmdb_id))
    imdb_id = info.get("imdb_id") or info.get("imdb")
    if imdb_id:
        _add_text(root, "imdbid", str(imdb_id))

    # Genres
    genre_str = info.get("genre") or info.get("category_name") or ""
    for genre in re.split(r"[,/]", genre_str):
        genre = genre.strip()
        if genre:
            _add_text(root, "genre", genre)

    # Director
    director_str = info.get("director") or ""
    for director in re.split(r"[,/]", director_str):
        director = director.strip()
        if director:
            _add_text(root, "director", director)

    # Cast
    cast_str = info.get("cast") or info.get("actors") or ""
    for actor_name in re.split(r",", cast_str):
        actor_name = actor_name.strip()
        if actor_name:
            actor_el = SubElement(root, "actor")
            name_el = SubElement(actor_el, "name")
            name_el.text = actor_name

    # Country / Studio
    _add_text(root, "country", info.get("country"))
    _add_text(root, "studio", info.get("studio"))

    # Poster / fanart (URL references for Jellyfin)
    cover = info.get("cover_big") or info.get("cover") or info.get("movie_image") or info.get("stream_icon")
    if cover:
        art = SubElement(root, "art")
        _add_text(art, "poster", cover)

    return _xml_prettify(root)


def generate_tvshow_nfo(info: dict, name: str = "") -> str:
    """Generate a Jellyfin-compatible tvshow.nfo from Xtream series info."""
    root = Element("tvshow")
    title = info.get("name") or info.get("title") or name
    _add_text(root, "title", title)
    _add_text(root, "originaltitle", info.get("o_name"))
    year = _extract_year(info)
    _add_text(root, "year", year)
    _add_text(root, "plot", info.get("plot") or info.get("description"))

    rating_val = info.get("rating") or info.get("rating_5based")
    if rating_val:
        try:
            r = float(rating_val)
            if r > 0:
                _add_text(root, "rating", str(r))
        except (ValueError, TypeError):
            pass

    tmdb_id = info.get("tmdb_id") or info.get("tmdb")
    if tmdb_id:
        _add_text(root, "tmdbid", str(tmdb_id))
    imdb_id = info.get("imdb_id") or info.get("imdb")
    if imdb_id:
        _add_text(root, "imdbid", str(imdb_id))

    genre_str = info.get("genre") or info.get("category_name") or ""
    for genre in re.split(r"[,/]", genre_str):
        genre = genre.strip()
        if genre:
            _add_text(root, "genre", genre)

    cast_str = info.get("cast") or info.get("actors") or ""
    for actor_name in re.split(r",", cast_str):
        actor_name = actor_name.strip()
        if actor_name:
            actor_el = SubElement(root, "actor")
            name_el = SubElement(actor_el, "name")
            name_el.text = actor_name

    cover = info.get("cover") or info.get("cover_big") or info.get("stream_icon")
    if cover:
        art = SubElement(root, "art")
        _add_text(art, "poster", cover)

    return _xml_prettify(root)


def generate_episode_nfo(item: dict, ep_info: dict | None = None) -> str:
    """Generate a Jellyfin-compatible episodedetails NFO."""
    root = Element("episodedetails")
    _add_text(root, "title", item.get("episode_title") or item.get("name", ""))
    _add_text(root, "season", str(item.get("season", 1)))
    _add_text(root, "episode", str(item.get("episode_num", 1)))
    _add_text(root, "showtitle", item.get("series_name", ""))

    if ep_info:
        _add_text(root, "plot", ep_info.get("plot") or ep_info.get("description"))
        _add_text(root, "aired", ep_info.get("air_date") or ep_info.get("releasedate"))
        rating_val = ep_info.get("rating")
        if rating_val:
            try:
                r = float(rating_val)
                if r > 0:
                    _add_text(root, "rating", str(r))
            except (ValueError, TypeError):
                pass
        _add_text(root, "runtime", ep_info.get("duration") or ep_info.get("runtime"))

    return _xml_prettify(root)


async def download_poster(url: str, dest_path: str) -> bool:
    """Download a poster image to the given path. Returns True on success."""
    if not url or not url.startswith("http"):
        return False
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200 and len(resp.content) > 100:
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
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
        self.cart_file = os.path.join(config_service.data_dir, "cart.json")

        self._download_cart: list[dict] = []
        self._download_task: Optional[asyncio.Task] = None
        self._download_cancel_event: Optional[asyncio.Event] = None
        self._download_current_item: Optional[dict] = None
        self._download_progress: dict = {
            "bytes_downloaded": 0,
            "total_bytes": 0,
            "speed": 0,
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
        if os.path.exists(self.cart_file):
            try:
                with open(self.cart_file) as f:
                    data = json.load(f)
                self._download_cart = data.get("items", [])
                return self._download_cart
            except (OSError, json.JSONDecodeError) as e:
                logger.error(f"Error loading cart: {e}")
        self._download_cart = []
        return self._download_cart

    def save_cart(self, items: list | None = None) -> None:
        if items is not None:
            self._download_cart = items
        os.makedirs(os.path.dirname(self.cart_file), exist_ok=True)
        with open(self.cart_file, "w") as f:
            json.dump({"items": self._download_cart}, f, indent=2)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

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

            file_path = self.build_download_filepath(item)
            item["file_path"] = file_path
            temp_dir = self.config_service.get_download_temp_path()
            temp_filename = f"{item['id']}_{os.path.basename(file_path)}"
            temp_path = os.path.join(temp_dir, temp_filename)

            try:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                os.makedirs(temp_dir, exist_ok=True)
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
                                        self._download_progress["bytes_downloaded"] = downloaded
                                        self._download_progress["speed"] = speed
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
            os.makedirs(dest_dir, exist_ok=True)
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

        For **movies**: writes ``<MovieName>.nfo`` and ``poster.jpg`` in the same directory.
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
        info = await self.xtream_service.fetch_vod_info(source_id, stream_id)
        if not info:
            logger.debug(f"No VOD info available for stream {stream_id}, skipping NFO")
            return

        # Write movie NFO
        nfo_content = generate_movie_nfo(info, name=item.get("name", ""))
        with open(nfo_path, "w", encoding="utf-8") as f:
            f.write(nfo_content)
        logger.info(f"[META] Wrote movie NFO: {nfo_path}")

        # Download poster next to the video file (poster.jpg or <name>-poster.jpg)
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
            poster_path = os.path.splitext(file_path)[0] + "-poster" + poster_ext
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
            # Fetch series-level info from the Xtream API
            series_id = item.get("series_id", "")
            if series_id:
                series_info = await self.xtream_service.fetch_series_info(source_id, series_id)
                if series_info:
                    os.makedirs(series_root, exist_ok=True)
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
