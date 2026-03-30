"""Jellyfin integration service for connectivity tests and library refreshes."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from app.services.config_service import ConfigService
    from app.services.http_client import HttpClientService

logger = logging.getLogger(__name__)


class JellyfinService:
    """Encapsulates Jellyfin API calls used by the application."""

    REFRESH_COOLDOWN_SECONDS = 15.0

    def __init__(self, config_service: "ConfigService", http_client: "HttpClientService"):
        self.config_service = config_service
        self.http_client = http_client
        self._refresh_lock = asyncio.Lock()
        self._last_refresh_monotonic = 0.0

    @staticmethod
    def normalize_base_url(base_url: str | None) -> str:
        return str(base_url or "").strip().rstrip("/")

    @staticmethod
    def _mask_secret(secret: str) -> str:
        if not secret:
            return ""
        return f"***{secret[-4:]}" if len(secret) > 4 else "***"

    @staticmethod
    def _auth_headers(api_key: str) -> dict[str, str]:
        return {
            "X-Emby-Token": api_key,
            "Accept": "application/json",
        }

    @staticmethod
    def _response_message(response: httpx.Response) -> str:
        retry_after = response.headers.get("Retry-After")
        header_message = response.headers.get("Message")
        if header_message:
            if retry_after:
                return f"{header_message} Retry after {retry_after}s."
            return header_message
        try:
            payload = response.json()
        except Exception:
            payload = None
        if isinstance(payload, dict):
            for key in ("detail", "title", "message", "Message"):
                value = payload.get(key)
                if value:
                    return str(value)
        if retry_after:
            return f"Jellyfin is temporarily unavailable. Retry after {retry_after}s."
        return f"Jellyfin returned HTTP {response.status_code}"

    def get_config(self) -> dict:
        return self.config_service.get_jellyfin_config()

    def get_masked_config(self) -> dict:
        config = self.get_config().copy()
        api_key = str(config.get("api_key", "")).strip()
        config["api_key"] = ""
        if api_key:
            config["api_key_masked"] = self._mask_secret(api_key)
        return config

    async def test_connection(self, base_url: str | None = None, api_key: str | None = None) -> dict:
        config = self.get_config()
        resolved_base_url = self.normalize_base_url(base_url if base_url is not None else config.get("base_url"))
        resolved_api_key = str(api_key if api_key is not None else config.get("api_key", "")).strip()
        if not resolved_base_url or not resolved_api_key:
            return {"ok": False, "message": "Base URL and API key are required", "status_code": 400}

        client = await self.http_client.get_client()
        try:
            response = await client.get(
                f"{resolved_base_url}/System/Info",
                headers=self._auth_headers(resolved_api_key),
                timeout=20.0,
            )
        except httpx.RequestError as exc:
            return {"ok": False, "message": f"Failed to reach Jellyfin: {exc}", "status_code": 502}

        if response.status_code == 200:
            payload = response.json()
            server_name = payload.get("ServerName") or payload.get("serverName") or "Jellyfin"
            version = payload.get("Version") or payload.get("version")
            message = f"Connected to {server_name}"
            if version:
                message += f" ({version})"
            return {
                "ok": True,
                "message": message,
                "status_code": 200,
                "server_name": server_name,
                "version": version,
            }

        if response.status_code in (401, 403):
            return {
                "ok": False,
                "message": "Jellyfin rejected the API key",
                "status_code": response.status_code,
            }

        return {
            "ok": False,
            "message": self._response_message(response),
            "status_code": response.status_code if response.status_code >= 400 else 502,
        }

    async def trigger_library_refresh(self, trigger: str) -> dict:
        config = self.get_config()
        if not config.get("enabled", False):
            return {
                "ok": True,
                "triggered": False,
                "skipped": True,
                "message": "Jellyfin integration is disabled",
            }

        trigger_key = "trigger_queue" if trigger == "queue" else "trigger_file"
        if not config.get(trigger_key, False):
            return {
                "ok": True,
                "triggered": False,
                "skipped": True,
                "message": f"Jellyfin {trigger} refresh is disabled",
            }

        base_url = self.normalize_base_url(config.get("base_url"))
        api_key = str(config.get("api_key", "")).strip()
        if not base_url or not api_key:
            return {
                "ok": False,
                "message": "Jellyfin is enabled but the base URL or API key is missing",
                "status_code": 400,
            }

        async with self._refresh_lock:
            now = time.monotonic()
            if now - self._last_refresh_monotonic < self.REFRESH_COOLDOWN_SECONDS:
                logger.info("Skipped Jellyfin library refresh for '%s' due to cooldown", trigger)
                return {
                    "ok": True,
                    "triggered": False,
                    "skipped": True,
                    "message": "A Jellyfin library refresh was already requested recently",
                }

            client = await self.http_client.get_client()
            try:
                response = await client.post(
                    f"{base_url}/Library/Refresh",
                    headers=self._auth_headers(api_key),
                    timeout=30.0,
                )
            except httpx.RequestError as exc:
                return {"ok": False, "message": f"Failed to reach Jellyfin: {exc}", "status_code": 502}

            if response.status_code in (200, 202, 204):
                self._last_refresh_monotonic = now
                logger.info("Triggered Jellyfin library refresh (%s)", trigger)
                return {
                    "ok": True,
                    "triggered": True,
                    "skipped": False,
                    "message": "Jellyfin library scan started",
                }

            if response.status_code in (401, 403):
                return {
                    "ok": False,
                    "message": "Jellyfin rejected the API key",
                    "status_code": response.status_code,
                }

            return {
                "ok": False,
                "message": self._response_message(response),
                "status_code": response.status_code if response.status_code >= 400 else 502,
            }