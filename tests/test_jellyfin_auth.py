"""Regression tests for Jellyfin 12 API-key authentication."""

import httpx
import pytest

from app.services.jellyfin_service import JellyfinService


class StubConfigService:
    def get_jellyfin_config(self):
        return {
            "enabled": True,
            "trigger_queue": True,
            "trigger_file": True,
            "base_url": "http://jellyfin.test:8096",
            "api_key": "test-api-key",
        }


class StubHttpClientService:
    def __init__(self, client):
        self.client = client

    async def get_client(self):
        return self.client


@pytest.mark.asyncio
async def test_connection_uses_mediabrowser_authorization_header():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/System/Info"
        assert request.headers["Authorization"] == 'MediaBrowser Token="test-api-key"'
        assert "X-Emby-Token" not in request.headers
        return httpx.Response(200, json={"ServerName": "Jellyfin", "Version": "12.4.0"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = JellyfinService(StubConfigService(), StubHttpClientService(client))
        result = await service.test_connection()

    assert result["ok"] is True
    assert result["status_code"] == 200


@pytest.mark.asyncio
async def test_library_refresh_uses_mediabrowser_authorization_header():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/Library/Refresh"
        assert request.headers["Authorization"] == 'MediaBrowser Token="test-api-key"'
        assert "X-Emby-Token" not in request.headers
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = JellyfinService(StubConfigService(), StubHttpClientService(client))
        result = await service.trigger_library_refresh("queue")

    assert result["ok"] is True
    assert result["triggered"] is True
