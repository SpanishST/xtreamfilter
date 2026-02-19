"""EPG service — XMLTV cache with per-source filtering and disk persistence."""
from __future__ import annotations

import asyncio
import io
import json
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import httpx
from lxml import etree

from app.services.filter_service import matches_filter

if TYPE_CHECKING:
    from app.services.cache_service import CacheService
    from app.services.config_service import ConfigService

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


class EpgService:
    """Manages the merged EPG / XMLTV cache."""

    def __init__(
        self,
        config_service: "ConfigService",
        http_client: "HttpClientService | None" = None,
        cache_service: "CacheService | None" = None,
    ):
        self.config_service = config_service
        self.http_client = http_client
        self.cache_service = cache_service
        self.epg_cache_file = os.path.join(config_service.data_dir, "epg_cache.xml")

        self._epg_cache: dict = {
            "data": None,
            "last_refresh": None,
            "refresh_in_progress": False,
        }
        self._epg_cache_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Validity
    # ------------------------------------------------------------------

    def is_epg_cache_valid(self) -> bool:
        last_refresh = self._epg_cache.get("last_refresh")
        if not last_refresh or self._epg_cache.get("data") is None:
            return False
        try:
            last_time = datetime.fromisoformat(last_refresh)
            age = (datetime.now() - last_time).total_seconds()
            return age < self.config_service.get_epg_cache_ttl()
        except (ValueError, TypeError):
            return False

    # ------------------------------------------------------------------
    # Disk persistence
    # ------------------------------------------------------------------

    def load_epg_cache_from_disk(self) -> None:
        if os.path.exists(self.epg_cache_file):
            try:
                with open(self.epg_cache_file, "rb") as f:
                    self._epg_cache["data"] = f.read()
                meta_file = self.epg_cache_file + ".meta"
                if os.path.exists(meta_file):
                    with open(meta_file) as f:
                        meta = json.load(f)
                        self._epg_cache["last_refresh"] = meta.get("last_refresh")
                logger.info(
                    f"Loaded EPG cache from disk. Last refresh: {self._epg_cache.get('last_refresh', 'Unknown')}"
                )
            except Exception as e:
                logger.error(f"Failed to load EPG cache from disk: {e}")

    def save_epg_cache_to_disk(self) -> None:
        try:
            if self._epg_cache.get("data"):
                os.makedirs(os.path.dirname(self.epg_cache_file), exist_ok=True)
                with open(self.epg_cache_file, "wb") as f:
                    f.write(self._epg_cache["data"])
                meta_file = self.epg_cache_file + ".meta"
                with open(meta_file, "w") as f:
                    json.dump({"last_refresh": self._epg_cache.get("last_refresh")}, f)
                logger.info(f"EPG cache saved to disk at {datetime.now().isoformat()}")
        except Exception as e:
            logger.error(f"Failed to save EPG cache to disk: {e}")

    # ------------------------------------------------------------------
    # Fetch helpers
    # ------------------------------------------------------------------

    async def _fetch_epg_from_source(self, source: dict) -> bytes | None:
        host = source["host"].rstrip("/")
        url = f"{host}/xmltv.php"
        params = {"username": source["username"], "password": source["password"]}
        try:
            async with httpx.AsyncClient(
                headers=HEADERS,
                timeout=httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=30.0),
                follow_redirects=True,
            ) as client:
                response = await client.get(url, params=params)
                if response.status_code == 200:
                    logger.info(
                        f"Fetched EPG from source '{source.get('name', source['id'])}': {len(response.content)} bytes"
                    )
                    return response.content
                else:
                    logger.warning(
                        f"Failed to fetch EPG from source '{source.get('name', source['id'])}': HTTP {response.status_code}"
                    )
        except Exception as e:
            logger.error(f"Error fetching EPG from source '{source.get('name', source['id'])}': {e}")
        return None

    def _get_filtered_epg_ids_for_source(self, source: dict) -> set | None:
        source_id = source.get("id", "unknown")
        filters = source.get("filters", {})
        live_filters = filters.get("live", {"groups": [], "channels": []})
        group_filters = live_filters.get("groups", [])
        channel_filters = live_filters.get("channels", [])

        categories = self.cache_service.get_cached("live_categories", source_id)
        streams = self.cache_service.get_cached("live_streams", source_id)

        if not streams:
            return None

        cat_map: dict[str, str] = {}
        for cat in categories:
            if isinstance(cat, dict):
                cat_map[str(cat.get("category_id", ""))] = cat.get("category_name", "")

        valid_epg_ids: set[str] = set()
        for stream in streams:
            name = stream.get("name", "Unknown")
            cat_id = str(stream.get("category_id", ""))
            group = cat_map.get(cat_id, "Unknown")
            epg_id = stream.get("epg_channel_id", "")
            if not epg_id:
                continue
            # Apply group filters
            if group_filters:
                include_rules = [r for r in group_filters if r.get("type") == "include"]
                exclude_rules = [r for r in group_filters if r.get("type") == "exclude"]
                excluded = any(matches_filter(group, r) for r in exclude_rules)
                if excluded:
                    continue
                if include_rules and not any(matches_filter(group, r) for r in include_rules):
                    continue
            # Apply channel filters
            if channel_filters:
                include_rules = [r for r in channel_filters if r.get("type") == "include"]
                exclude_rules = [r for r in channel_filters if r.get("type") == "exclude"]
                excluded = any(matches_filter(name, r) for r in exclude_rules)
                if excluded:
                    continue
                if include_rules and not any(matches_filter(name, r) for r in include_rules):
                    continue
            valid_epg_ids.add(f"{source_id}_{epg_id}".lower())
        return valid_epg_ids

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    async def refresh_epg_cache(self) -> None:
        async with self._epg_cache_lock:
            if self._epg_cache.get("refresh_in_progress"):
                logger.info("EPG refresh already in progress, skipping")
                return
            self._epg_cache["refresh_in_progress"] = True

        try:
            config = self.config_service.config
            enabled_sources = [s for s in config.get("sources", []) if s.get("enabled", True)]
            if not enabled_sources:
                logger.warning("No enabled sources for EPG refresh")
                return

            logger.info(f"Starting EPG refresh from {len(enabled_sources)} source(s)")

            source_epg_filters: dict[str, set | None] = {}
            for source in enabled_sources:
                sid = source.get("id", "unknown")
                valid_ids = self._get_filtered_epg_ids_for_source(source)
                source_epg_filters[sid] = valid_ids

            tasks = [self._fetch_epg_from_source(s) for s in enabled_sources]
            results = await asyncio.gather(*tasks)

            merged_root = etree.Element("tv", attrib={"generator-info-name": "XtreamFilter Merged EPG"})
            total_stats = {
                "channels_included": 0,
                "channels_excluded": 0,
                "programmes_included": 0,
                "programmes_excluded": 0,
            }

            for source, epg_data in zip(enabled_sources, results):
                if epg_data is None:
                    continue
                source_id = source.get("id", "unknown")
                valid_epg_ids = source_epg_filters.get(source_id)
                try:
                    parser = etree.XMLParser(recover=True)
                    tree = etree.parse(io.BytesIO(epg_data), parser)
                    root = tree.getroot()
                    src_stats = {"channels": 0, "channels_excluded": 0, "programmes": 0, "programmes_excluded": 0}

                    for channel in root.findall("channel"):
                        original_id = channel.get("id", "")
                        if original_id:
                            new_id = f"{source_id}_{original_id}".lower()
                            if valid_epg_ids is not None and new_id not in valid_epg_ids:
                                src_stats["channels_excluded"] += 1
                                continue
                            channel.set("id", new_id)
                        merged_root.append(channel)
                        src_stats["channels"] += 1

                    for programme in root.findall("programme"):
                        original_channel = programme.get("channel", "")
                        if original_channel:
                            new_channel = f"{source_id}_{original_channel}".lower()
                            if valid_epg_ids is not None and new_channel not in valid_epg_ids:
                                src_stats["programmes_excluded"] += 1
                                continue
                            programme.set("channel", new_channel)
                        merged_root.append(programme)
                        src_stats["programmes"] += 1

                    total_stats["channels_included"] += src_stats["channels"]
                    total_stats["channels_excluded"] += src_stats["channels_excluded"]
                    total_stats["programmes_included"] += src_stats["programmes"]
                    total_stats["programmes_excluded"] += src_stats["programmes_excluded"]
                except Exception as e:
                    logger.error(f"Error parsing EPG from source '{source.get('name', source_id)}': {e}")

            merged_data = etree.tostring(merged_root, encoding="UTF-8", xml_declaration=True, pretty_print=False)

            async with self._epg_cache_lock:
                self._epg_cache["data"] = merged_data
                self._epg_cache["last_refresh"] = datetime.now().isoformat()

            self.save_epg_cache_to_disk()
            logger.info(
                f"EPG refresh complete. Total: {total_stats['channels_included']} channels, "
                f"{total_stats['programmes_included']} programmes. Size: {len(merged_data)} bytes"
            )
        except Exception as e:
            logger.error(f"EPG refresh failed: {e}")
        finally:
            async with self._epg_cache_lock:
                self._epg_cache["refresh_in_progress"] = False

    # ------------------------------------------------------------------
    # Access
    # ------------------------------------------------------------------

    def get_epg_data(self) -> bytes | None:
        return self._epg_cache.get("data")

    def get_epg_status(self) -> dict:
        return {
            "cached": self._epg_cache.get("data") is not None,
            "last_refresh": self._epg_cache.get("last_refresh"),
            "refresh_in_progress": self._epg_cache.get("refresh_in_progress", False),
            "cache_valid": self.is_epg_cache_valid(),
            "cache_ttl_seconds": self.config_service.get_epg_cache_ttl(),
            "cache_size_bytes": len(self._epg_cache.get("data", b"")) if self._epg_cache.get("data") else 0,
        }

    # ------------------------------------------------------------------
    # EPG now / next programme lookup
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_xmltv_time(time_str: str) -> datetime | None:
        """Parse XMLTV datetime string like '20260217120000 +0100' into a timezone-aware datetime."""
        if not time_str:
            return None
        try:
            # Strip whitespace and try common XMLTV formats
            time_str = time_str.strip()
            # Format: YYYYMMDDHHmmss +HHMM
            if " " in time_str:
                dt_part, tz_part = time_str.rsplit(" ", 1)
            else:
                dt_part = time_str
                tz_part = "+0000"
            dt = datetime.strptime(dt_part, "%Y%m%d%H%M%S")
            # Parse timezone offset
            tz_sign = 1 if tz_part[0] == "+" else -1
            tz_hours = int(tz_part[1:3])
            tz_minutes = int(tz_part[3:5])
            from datetime import timedelta, timezone as tz
            offset = timedelta(hours=tz_hours, minutes=tz_minutes) * tz_sign
            dt = dt.replace(tzinfo=tz(offset))
            return dt
        except Exception:
            return None

    def get_now_next(self, channel_id: str) -> dict:
        """Get current and next programme for a channel from cached EPG XML.

        Args:
            channel_id: The EPG channel ID (e.g. 'source1_channel.epg.id')

        Returns:
            Dict with 'current' and 'next' programme info, or empty values if unavailable.
        """
        result = {"current": None, "next": None}
        data = self._epg_cache.get("data")
        if not data:
            return result

        try:
            parser = etree.XMLParser(recover=True)
            tree = etree.parse(io.BytesIO(data), parser)
            root = tree.getroot()

            now = datetime.now(timezone.utc)
            channel_id_lower = channel_id.lower()

            # Collect all programmes for this channel
            programmes = []
            for prog in root.findall("programme"):
                prog_channel = (prog.get("channel") or "").lower()
                if prog_channel != channel_id_lower:
                    continue

                start = self._parse_xmltv_time(prog.get("start", ""))
                stop = self._parse_xmltv_time(prog.get("stop", ""))
                if not start:
                    continue

                title_el = prog.find("title")
                title = title_el.text if title_el is not None and title_el.text else ""
                desc_el = prog.find("desc")
                desc = desc_el.text if desc_el is not None and desc_el.text else ""

                programmes.append({
                    "title": title,
                    "description": desc,
                    "start": start,
                    "stop": stop,
                    "start_ts": int(start.timestamp()),
                    "stop_ts": int(stop.timestamp()) if stop else None,
                })

            if not programmes:
                return result

            # Sort by start time
            programmes.sort(key=lambda p: p["start"])

            # Find current programme (start <= now < stop)
            current = None
            current_idx = -1
            for i, prog in enumerate(programmes):
                start = prog["start"]
                stop = prog["stop"]
                if start <= now and (stop is None or now < stop):
                    current = prog
                    current_idx = i
                    break

            if current:
                duration = (current["stop"] - current["start"]).total_seconds() if current["stop"] else 0
                elapsed = (now - current["start"]).total_seconds()
                progress_pct = round((elapsed / duration) * 100, 1) if duration > 0 else 0
                result["current"] = {
                    "title": current["title"],
                    "description": current["description"],
                    "start": current["start_ts"],
                    "stop": current["stop_ts"],
                    "progress_pct": min(progress_pct, 100.0),
                }
                # Next programme
                if current_idx + 1 < len(programmes):
                    nxt = programmes[current_idx + 1]
                    result["next"] = {
                        "title": nxt["title"],
                        "start": nxt["start_ts"],
                    }
            else:
                # No current programme — find the next upcoming one
                for prog in programmes:
                    if prog["start"] > now:
                        result["next"] = {
                            "title": prog["title"],
                            "start": prog["start_ts"],
                        }
                        break

        except Exception as e:
            logger.error(f"Error parsing EPG for now/next (channel={channel_id}): {e}")

        return result
