"""Xtream service — virtual ID encoding, source resolution, provider info."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

import httpx

if TYPE_CHECKING:
    from app.services.cache_service import CacheService
    from app.services.config_service import ConfigService
    from app.services.http_client import HttpClientService

logger = logging.getLogger(__name__)

VIRTUAL_ID_OFFSET = 10_000_000


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
                for ep in season_episodes:
                    episodes.append(
                        {
                            "stream_id": str(ep.get("id", "")),
                            "season": str(season_num),
                            "episode_num": ep.get("episode_num", 0),
                            "title": ep.get("title", ""),
                            "container_extension": ep.get("container_extension", "mp4"),
                            "info": ep.get("info", {}),
                            "series_name": series_name,
                        }
                    )
            return episodes
        except Exception as e:
            logger.error(f"Error fetching series episodes: {e}")
            return []
