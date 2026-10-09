"""Persistent webhook configuration and delivery tests."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json

from app.database import DB_NAME, db_connect, init_db
from app.services.config_service import ConfigService
from app.services.webhook_service import WebhookService


class _Response:
    def __init__(self, status_code: int):
        self.status_code = status_code


class _FakeClient:
    def __init__(self, status_code: int = 204):
        self.status_code = status_code
        self.requests = []

    async def post(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return _Response(self.status_code)


class _FakeHttp:
    def __init__(self, client):
        self.client = client

    async def get_client(self):
        return self.client


def _service(tmp_path):
    db_path = str(tmp_path / DB_NAME)
    init_db(db_path)
    cfg = ConfigService(str(tmp_path))
    cfg.load()
    return WebhookService(cfg, _FakeHttp(_FakeClient()), db_path)


def test_endpoint_secrets_are_masked_and_inputs_validated(tmp_path):
    service = _service(tmp_path)
    endpoint = service.save_endpoint(
        {
            "name": "Automation",
            "url": "https://example.test/hooks/app",
            "secret": "very-secret",
            "events": ["cache.refresh.started"],
        }
    )

    assert endpoint["secret_configured"] is True
    assert "secret" not in endpoint
    assert service.list_endpoints()[0]["url"] == "https://example.test/hooks/app"

    try:
        service.save_endpoint({"name": "Bad", "url": "file:///etc/passwd", "events": []})
    except ValueError as exc:
        assert "http://" in str(exc)
    else:
        raise AssertionError("Non-HTTP webhook URL should be rejected")

    try:
        service.save_endpoint({"name": "Bad", "url": "http://example.test", "events": ["unknown"]})
    except ValueError as exc:
        assert "Unknown webhook event" in str(exc)
    else:
        raise AssertionError("Unknown webhook events should be rejected")


def test_subscribed_event_is_signed_and_persisted_as_delivered(tmp_path):
    service = _service(tmp_path)
    service.save_endpoint(
        {
            "name": "Automation",
            "url": "https://example.test/hooks/app",
            "secret": "shared-secret",
            "events": ["download.item.completed"],
        }
    )
    service.save_endpoint(
        {
            "name": "Second receiver",
            "url": "https://second.example.test/events",
            "events": ["download.item.completed"],
        }
    )
    client = _FakeClient()
    service.http_client = _FakeHttp(client)

    async def run():
        event_id = await service.publish_event("download.item.completed", {"name": "Example"})
        assert event_id
        assert await service.deliver_due() == 2

    asyncio.run(run())

    url, request = client.requests[0]
    assert url == "https://example.test/hooks/app"
    payload = request["content"]
    decoded = json.loads(payload)
    assert decoded["event"] == "download.item.completed"
    assert decoded["id"] == request["headers"]["Idempotency-Key"]
    timestamp = request["headers"]["X-Webhook-Timestamp"]
    expected = hmac.new(
        b"shared-secret",
        timestamp.encode("ascii") + b"." + payload,
        hashlib.sha256,
    ).hexdigest()
    assert request["headers"]["X-Webhook-Signature"] == f"sha256={expected}"
    assert {call[0] for call in client.requests} == {
        "https://example.test/hooks/app",
        "https://second.example.test/events",
    }

    conn = db_connect(service.db_path)
    try:
        rows = conn.execute("SELECT status, attempts, last_status_code FROM webhook_deliveries").fetchall()
    finally:
        conn.close()
    assert [tuple(row) for row in rows] == [("delivered", 1, 204), ("delivered", 1, 204)]


def test_unsubscribed_event_is_not_enqueued_and_failed_response_retries(tmp_path):
    service = _service(tmp_path)
    service.save_endpoint(
        {
            "name": "Automation",
            "url": "http://example.test/hook",
            "events": ["cache.refresh.started"],
        }
    )
    client = _FakeClient(status_code=503)
    service.http_client = _FakeHttp(client)

    async def run():
        await service.publish_event("download.item.failed", {"name": "ignored"})
        conn = db_connect(service.db_path)
        try:
            assert conn.execute("SELECT COUNT(*) FROM webhook_deliveries").fetchone()[0] == 0
        finally:
            conn.close()
        await service.publish_event("cache.refresh.started", {"total_sources": 1})
        await service.deliver_due()

    asyncio.run(run())

    conn = db_connect(service.db_path)
    try:
        row = conn.execute("SELECT status, attempts, last_status_code, last_error FROM webhook_deliveries").fetchone()
    finally:
        conn.close()
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["last_status_code"] == 503
    assert row["last_error"] == "Receiver returned HTTP 503"
