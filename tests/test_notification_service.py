"""Notification service tests."""

from __future__ import annotations

import asyncio
import json

from app.services.config_service import ConfigService
from app.services.http_client import HttpClientService
from app.services.notification_service import NotificationService


class _FakeTelegramResponse:
    status_code = 200

    def json(self):
        return {"ok": True}


class _FakeTelegramClient:
    def __init__(self):
        self.posts = []

    async def post(self, url, json):
        self.posts.append((url, json))
        return _FakeTelegramResponse()


def test_send_cache_refresh_failure_notification_posts_when_configured(tmp_path, monkeypatch):
    config = {
        "sources": [],
        "filters": {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
        "content_types": {"live": True, "vod": True, "series": True},
        "options": {
            "telegram": {
                "enabled": True,
                "bot_token": "test-bot-token",
                "chat_id": "123456",
            }
        },
    }
    (tmp_path / "config.json").write_text(json.dumps(config))
    cfg = ConfigService(str(tmp_path))
    cfg.load()

    http = HttpClientService()
    fake_client = _FakeTelegramClient()

    async def fake_get_client():
        return fake_client

    monkeypatch.setattr(http, "get_client", fake_get_client)

    service = NotificationService(cfg, http)
    progress = {
        "status": "partial",
        "total_sources": 2,
        "last_error": "Primary: VOD streams failed with HTTP 400",
        "summary": {
            "total_sources": 2,
            "successful_sources": 1,
            "partial_sources": 1,
            "failed_sources": 0,
            "preserved_steps": 1,
        },
        "source_results": [
            {
                "source_id": "src-1",
                "source_name": "Primary",
                "status": "partial",
                "errors": [
                    {
                        "key": "vod_streams",
                        "label": "VOD streams",
                        "status_code": 400,
                        "message": "HTTP 400 while fetching get_vod_streams",
                        "type": "http_error",
                        "preserved_existing": True,
                    }
                ],
            }
        ],
    }

    asyncio.run(service.send_cache_refresh_failure_notification(progress))
    asyncio.run(service.send_cache_refresh_failure_notification(progress))

    assert len(fake_client.posts) == 1
    url, payload = fake_client.posts[0]
    assert url == "https://api.telegram.org/bottest-bot-token/sendMessage"
    assert payload["chat_id"] == "123456"
    assert payload["parse_mode"] == "HTML"
    assert "Cache refresh completed with warnings" in payload["text"]
    assert "VOD streams: HTTP 400" in payload["text"]
    assert "kept previous data" in payload["text"]