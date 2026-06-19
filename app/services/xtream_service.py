"""Xtream service — virtual ID encoding, source resolution, provider info."""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Optional

import httpx

if TYPE_CHECKING:
    from app.services.cache_service import CacheService
    from app.services.config_service import ConfigService
    from app.services.http_client import HttpClientService

logger = logging.getLogger(__name__)

VIRTUAL_ID_OFFSET = 10_000_000

# Episode-number normalization — some upstream sources return bogus episode_num
# values (e.g. 52 for S01E02). When the upstream value exceeds this threshold
# we attempt to parse a real SxxExx pattern out of the episode title.
EPISODE_NUM_SANITY_THRESHOLD = 15

_EPISODE_TITLE_RE = re.compile(r"S(\d{1,2})\s*E(\d{1,3})", re.IGNORECASE)


def _normalize_episode_num(
    raw_num,
    title: str,
    season_size: int,
    threshold: int = EPISODE_NUM_SANITY_THRESHOLD,
) -> tuple[int, str | None]:
    """Return ``(episode_num, parsed_season_or_None)`` for a fetched episode.

    The episode number is overridden only when **all** of the following hold:

    * ``raw_num`` exceeds ``threshold`` (some sources return implausibly large
      numbers like 52 for S01E02 — very few series have >15 episodes per
      season, so anything above the threshold is suspicious).
    * ``title`` contains a ``SxxExx`` pattern.
    * The parsed episode number is plausible: ``1 <= parsed_ep <= season_size``.

    The parsed season is returned alongside (as ``str | None``) so callers can
    also override the upstream ``season`` field when it disagrees.
    """
    try:
        num = int(raw_num)
    except (ValueError, TypeError):
        return 0, None

    if num <= threshold or not title:
        return num, None

    m = _EPISODE_TITLE_RE.search(title)
    if not m:
        return num, None

    try:
        parsed_season = str(int(m.group(1)))
        parsed_ep = int(m.group(2))
    except (ValueError, TypeError):
        return num, None

    if parsed_ep < 1:
        return num, None
    if season_size and parsed_ep > season_size:
        return num, None

    return parsed_ep, parsed_season


def encode_virtual_id(source_index: int, original_id) -> int:
    """Encode a source index and original stream ID into a virtual ID."""
    try:
        return source_index * VIRTUAL_ID_OFFSET + int(original_id)
    except (ValueError, TypeError):
        return original_id


def decode_virtual_id(virtual_id) -> tuple[int, int]:
    """Decode a virtual ID back to (source_index, original_id)."""
    try:
        vid = int(virtual_id)
        return vid // VIRTUAL_ID_OFFSET, vid % VIRTUAL_ID_OFFSET
    except (ValueError, TypeError):
        return 0, virtual_id


class XtreamService:
    """Xtream Codes API helper — provider info, episode fetching, etc."""

    def __init__(
        self,
        config_service: "ConfigService",
        cache_service: "CacheService",
        http_client: "HttpClientService",
    ):
        self.config_service = config_service
        self.cache_service = cache_service
        self.http_client = http_client

    async def get_provider_info(self, source: dict) -> dict:
        """Fetch provider info including connection slots and allowed output formats."""
        result: dict = {"has_free_slot": True, "allowed_output_formats": []}
        try:
            host = source["host"].rstrip("/")
            params = {"username": source["username"], "password": source["password"]}
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(10.0),
                follow_redirects=True,
                headers={"Connection": "close"},
            ) as client:
                resp = await client.get(f"{host}/player_api.php", params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    ui = data.get("user_info", {})
                    active = int(ui.get("active_cons", 0))
                    max_cons = int(ui.get("max_connections", 1))
                    allowed = ui.get("allowed_output_formats", [])
                    if isinstance(allowed, str):
                        allowed = [allowed]
                    result["has_free_slot"] = active < max_cons
                    result["allowed_output_formats"] = allowed
        except Exception as e:
            logger.warning(f"Could not check provider info: {e}")
        return result

    async def check_provider_connections(self, source: dict) -> bool:
        info = await self.get_provider_info(source)
        return info["has_free_slot"]

    async def fetch_vod_info(self, source_id: str, vod_id: str) -> dict | None:
        """Fetch VOD (movie) metadata from upstream Xtream API.

        Returns the raw ``info`` dict with fields like name, plot, cast,
        director, genre, rating, tmdb_id, releasedate, cover_big, etc.
        Returns *None* on failure.
        """
        source = self.config_service.get_source_by_id(source_id)
        if not source:
            return None
        host = source["host"].rstrip("/")
        params = {
            "username": source["username"],
            "password": source["password"],
            "action": "get_vod_info",
            "vod_id": vod_id,
        }
        try:
            client = await self.http_client.get_client()
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            if response.status_code != 200:
                return None
            data = response.json()
            return data.get("info") or data.get("movie_data") or {}
        except Exception as e:
            logger.error(f"Error fetching VOD info for {vod_id}: {e}")
            return None

    async def fetch_series_info(self, source_id: str, series_id: str) -> dict | None:
        """Fetch series-level metadata from upstream Xtream API.

        Returns the raw ``info`` dict with fields like name, plot, cast,
        director, genre, rating, tmdb_id, cover, releaseDate, etc.
        Returns *None* on failure.
        """
        source = self.config_service.get_source_by_id(source_id)
        if not source:
            return None
        host = source["host"].rstrip("/")
        params = {
            "username": source["username"],
            "password": source["password"],
            "action": "get_series_info",
            "series_id": series_id,
        }
        try:
            client = await self.http_client.get_client()
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            if response.status_code != 200:
                return None
            data = response.json()
            return data.get("info", {})
        except Exception as e:
            logger.error(f"Error fetching series info for {series_id}: {e}")
            return None

    async def fetch_series_episodes(self, source_id: str, series_id: str) -> list:
        """Fetch all episodes for a series from upstream."""
        source = self.config_service.get_source_by_id(source_id)
        if not source:
            return []
        host = source["host"].rstrip("/")
        params = {
            "username": source["username"],
            "password": source["password"],
            "action": "get_series_info",
            "series_id": series_id,
        }
        try:
            client = await self.http_client.get_client()
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            if response.status_code != 200:
                return []
            data = response.json()
            episodes: list[dict] = []
            series_info = data.get("info", {})
            series_name = series_info.get("name", "") or series_info.get("title", "")
            for season_num, season_episodes in data.get("episodes", {}).items():
                season_size = len(season_episodes) if isinstance(season_episodes, list) else 0
                for ep in season_episodes:
                    raw_ep_num = ep.get("episode_num", 0)
                    raw_title = ep.get("title", "") or ""
                    norm_ep_num, parsed_season = _normalize_episode_num(
                        raw_ep_num, raw_title, season_size,
                    )
                    episodes.append(
                        {
                            "stream_id": str(ep.get("id", "")),
                            "season": parsed_season or str(season_num),
                            "episode_num": norm_ep_num,
                            "title": raw_title,
                            "container_extension": ep.get("container_extension", "mp4"),
                            "info": ep.get("info", {}),
                            "series_name": series_name,
                        }
                    )
            return episodes
        except Exception as e:
            logger.error(f"Error fetching series episodes: {e}")
            return []
