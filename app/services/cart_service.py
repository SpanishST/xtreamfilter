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

import httpx

from app.models.xtream import PLAYER_PROFILES

if TYPE_CHECKING:
    from app.services.config_service import ConfigService
    from app.services.http_client import HttpClientService
    from app.services.notification_service import NotificationService
    from app.services.xtream_service import XtreamService

logger = logging.getLogger(__name__)


def sanitize_filename(name: str) -> str:
    name = unicodedata.normalize("NFKD", name)
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.strip(".")
    return name or "untitled"


def get_player_headers(profile_key: str = "tivimate") -> dict:
    profile = PLAYER_PROFILES.get(profile_key, PLAYER_PROFILES["tivimate"])
    return dict(profile["headers"])


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
            return os.path.join(base_path, "Films", f"{name}.{ext}")
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
                    season=ep["season"],
                    episode_num=ep.get("episode_num", 0),
                    episode_title=ep.get("title", ""),
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
                season=data.get("season"),
                episode_num=data.get("episode_num"),
                episode_title=data.get("episode_title"),
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
            "season": kwargs.get("season"),
            "episode_num": kwargs.get("episode_num"),
            "episode_title": kwargs.get("episode_title"),
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
