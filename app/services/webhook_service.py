"""Signed, persistent webhook event delivery."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import httpx

from app.database import adb_connect, adb_transaction, db_connect

if TYPE_CHECKING:
    from app.services.config_service import ConfigService
    from app.services.http_client import HttpClientService

logger = logging.getLogger(__name__)

WEBHOOK_EVENTS = (
    "cache.refresh.started",
    "cache.refresh.completed",
    "cache.refresh.failed",
    "cache.refresh.cancelled",
    "cart.item.added",
    "download.queue.started",
    "download.queue.completed",
    "download.item.started",
    "download.item.completed",
    "download.item.failed",
    "download.item.cancelled",
)
MAX_ATTEMPTS = 8
REQUEST_TIMEOUT_SECONDS = 10
DELIVERY_RETENTION_DAYS = 30


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: datetime | None = None) -> str:
    return (value or _utc_now()).isoformat()


def _validate_url(value: object) -> str:
    url = str(value or "").strip()
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Webhook URL is invalid") from exc
    if port == 0:
        raise ValueError("Webhook URL port must be greater than zero")
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Webhook URL must be an absolute http:// or https:// URL")
    if parsed.username or parsed.password:
        raise ValueError("Webhook URLs cannot contain embedded credentials")
    if any(char in url for char in "\r\n\x00"):
        raise ValueError("Webhook URL contains invalid characters")
    return url


class WebhookService:
    """Stores event deliveries in SQLite and retries them in the background."""

    def __init__(self, config_service: ConfigService, http_client: HttpClientService, db_path: str):
        self.config_service = config_service
        self.http_client = http_client
        self.db_path = db_path
        self._wake = asyncio.Event()

    # ------------------------------------------------------------------
    # Endpoint configuration
    # ------------------------------------------------------------------

    @staticmethod
    def _public_endpoint(endpoint: dict) -> dict:
        return {
            "id": endpoint.get("id", ""),
            "name": endpoint.get("name", ""),
            "url": endpoint.get("url", ""),
            "enabled": bool(endpoint.get("enabled", True)),
            "events": list(endpoint.get("events", [])),
            "secret_configured": bool(endpoint.get("secret")),
        }

    def list_endpoints(self) -> list[dict]:
        endpoints = self.config_service.config.get("options", {}).get("webhooks", [])
        if not isinstance(endpoints, list):
            return []
        return [self._public_endpoint(endpoint) for endpoint in endpoints if isinstance(endpoint, dict)]

    def _endpoints(self) -> list[dict]:
        endpoints = self.config_service.config.get("options", {}).get("webhooks", [])
        return [dict(endpoint) for endpoint in endpoints if isinstance(endpoint, dict)] if isinstance(endpoints, list) else []

    def save_endpoint(self, data: dict, endpoint_id: str | None = None) -> dict:
        if not isinstance(data, dict):
            raise ValueError("Webhook configuration must be an object")
        name = str(data.get("name", "")).strip()[:100]
        if not name:
            raise ValueError("Webhook name is required")
        url = _validate_url(data.get("url"))
        events = data.get("events", [])
        if not isinstance(events, list) or any(not isinstance(event, str) for event in events):
            raise ValueError("Events must be a list of event names")
        unknown_events = sorted(set(events) - set(WEBHOOK_EVENTS))
        if unknown_events:
            raise ValueError(f"Unknown webhook event(s): {', '.join(unknown_events)}")
        events = list(dict.fromkeys(events))
        secret_value = data.get("secret")
        if secret_value is not None and not isinstance(secret_value, str):
            raise ValueError("Webhook secret must be a string")
        if secret_value and len(secret_value) > 512:
            raise ValueError("Webhook secret must be 512 characters or fewer")
        enabled_value = data.get("enabled")
        if enabled_value is not None and not isinstance(enabled_value, bool):
            raise ValueError("Enabled must be a boolean")

        endpoints = self._endpoints()
        existing = next((endpoint for endpoint in endpoints if endpoint.get("id") == endpoint_id), None)
        if endpoint_id and existing is None:
            raise KeyError("Webhook endpoint not found")
        endpoint = {
            "id": endpoint_id or str(uuid.uuid4()),
            "name": name,
            "url": url,
            "enabled": bool(data.get("enabled", existing.get("enabled", True) if existing else True)),
            "events": events,
            "secret": secret_value if secret_value else (existing or {}).get("secret", ""),
        }
        if existing:
            endpoints = [endpoint if item.get("id") == endpoint_id else item for item in endpoints]
        else:
            endpoints.append(endpoint)
        self.config_service.config.setdefault("options", {})["webhooks"] = endpoints
        self.config_service.save()
        self._wake.set()
        return self._public_endpoint(endpoint)

    def delete_endpoint(self, endpoint_id: str) -> bool:
        endpoints = self._endpoints()
        updated = [endpoint for endpoint in endpoints if endpoint.get("id") != endpoint_id]
        if len(updated) == len(endpoints):
            return False
        self.config_service.config.setdefault("options", {})["webhooks"] = updated
        self.config_service.save()
        conn = db_connect(self.db_path)
        try:
            conn.execute(
                "UPDATE webhook_deliveries SET status = 'cancelled', last_error = 'Endpoint was removed' "
                "WHERE endpoint_id = ? AND status = 'pending'",
                (endpoint_id,),
            )
            conn.commit()
        finally:
            conn.close()
        return True

    # ------------------------------------------------------------------
    # Event publishing and outbox
    # ------------------------------------------------------------------

    async def publish_event(self, event_name: str, data: dict | None = None) -> str | None:
        """Persist one event for each enabled, subscribed endpoint."""
        if event_name not in WEBHOOK_EVENTS:
            logger.warning("Ignoring unknown webhook event %s", event_name)
            return None
        event_id = str(uuid.uuid4())
        created_at = _timestamp()
        payload = {
            "id": event_id,
            "event": event_name,
            "version": 1,
            "occurred_at": created_at,
            "data": data or {},
        }
        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
        endpoint_ids = [
            str(endpoint.get("id"))
            for endpoint in self._endpoints()
            if endpoint.get("id") and endpoint.get("enabled", True) and event_name in endpoint.get("events", [])
        ]
        if not endpoint_ids:
            return event_id
        try:
            async with adb_transaction(self.db_path) as conn:
                await conn.executemany(
                    """INSERT INTO webhook_deliveries
                    (id, event_id, endpoint_id, event_name, payload, status, attempts,
                     next_attempt_at, created_at)
                    VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?)""",
                    [
                        (str(uuid.uuid4()), event_id, endpoint_id, event_name, serialized, created_at, created_at)
                        for endpoint_id in endpoint_ids
                    ],
                )
            self._wake.set()
        except Exception:
            logger.exception("Could not persist webhook event %s", event_name)
            return None
        return event_id

    @staticmethod
    def _signature_headers(payload: bytes, secret: str, event_name: str, event_id: str, delivery_id: str) -> dict:
        timestamp = str(int(_utc_now().timestamp()))
        signature_input = timestamp.encode("ascii") + b"." + payload
        signature = hmac.new(secret.encode("utf-8"), signature_input, hashlib.sha256).hexdigest()
        return {
            "Content-Type": "application/json",
            "X-Webhook-Event": event_name,
            "X-Webhook-Delivery": delivery_id,
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": f"sha256={signature}",
            "Idempotency-Key": event_id,
        }

    async def send_test(self, endpoint_id: str) -> dict:
        endpoint = next((item for item in self._endpoints() if item.get("id") == endpoint_id), None)
        if endpoint is None:
            raise KeyError("Webhook endpoint not found")
        now = _timestamp()
        event_id = str(uuid.uuid4())
        payload = json.dumps(
            {"id": event_id, "event": "webhook.test", "version": 1, "occurred_at": now, "data": {"message": "Webhook test"}},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        headers = self._signature_headers(payload, endpoint.get("secret", ""), "webhook.test", event_id, event_id)
        try:
            client = await self.http_client.get_client()
            response = await client.post(
                endpoint["url"],
                content=payload,
                headers=headers,
                timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS),
            )
        except httpx.HTTPError as exc:
            return {"ok": False, "message": f"Webhook request failed ({type(exc).__name__})"}
        if 200 <= response.status_code < 300:
            return {"ok": True, "status_code": response.status_code, "message": "Test event delivered"}
        return {"ok": False, "status_code": response.status_code, "message": f"Receiver returned HTTP {response.status_code}"}

    async def _due_deliveries(self, limit: int = 20) -> list[dict]:
        conn = await adb_connect(self.db_path)
        try:
            cursor = await conn.execute(
                """SELECT * FROM webhook_deliveries
                   WHERE status = 'pending' AND next_attempt_at <= ?
                   ORDER BY created_at LIMIT ?""",
                (_timestamp(), limit),
            )
            rows = await cursor.fetchall()
        finally:
            await conn.close()
        return [dict(row) for row in rows]

    async def _update_delivery(self, delivery_id: str, **values) -> None:
        columns = ", ".join(f"{column} = ?" for column in values)
        conn = await adb_connect(self.db_path)
        try:
            await conn.execute(
                f"UPDATE webhook_deliveries SET {columns} WHERE id = ?",
                (*values.values(), delivery_id),
            )
            await conn.commit()
        finally:
            await conn.close()

    async def _deliver(self, delivery: dict) -> None:
        endpoint = next(
            (item for item in self._endpoints() if item.get("id") == delivery["endpoint_id"]),
            None,
        )
        if endpoint is None:
            await self._update_delivery(delivery["id"], status="cancelled", last_error="Endpoint was removed")
            return
        if not endpoint.get("enabled", True):
            await self._update_delivery(delivery["id"], next_attempt_at=_timestamp(_utc_now() + timedelta(minutes=1)))
            return
        if delivery["event_name"] not in endpoint.get("events", []):
            await self._update_delivery(delivery["id"], status="cancelled", last_error="Event subscription was removed")
            return

        attempts = int(delivery.get("attempts", 0)) + 1
        payload = delivery["payload"].encode("utf-8")
        headers = self._signature_headers(
            payload,
            endpoint.get("secret", ""),
            delivery["event_name"],
            delivery["event_id"],
            delivery["id"],
        )
        status_code = None
        error = ""
        try:
            client = await self.http_client.get_client()
            response = await client.post(
                endpoint["url"],
                content=payload,
                headers=headers,
                timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS),
            )
            status_code = response.status_code
            if 200 <= status_code < 300:
                await self._update_delivery(
                    delivery["id"],
                    status="delivered",
                    attempts=attempts,
                    delivered_at=_timestamp(),
                    last_status_code=status_code,
                    last_error="",
                )
                return
            error = f"Receiver returned HTTP {status_code}"
        except httpx.HTTPError as exc:
            error = f"{type(exc).__name__}: webhook request failed"
        except Exception as exc:
            logger.exception("Unexpected webhook delivery error")
            error = f"{type(exc).__name__}: webhook request failed"

        terminal = attempts >= MAX_ATTEMPTS
        delay = min(5 * (2 ** (attempts - 1)), 3600)
        await self._update_delivery(
            delivery["id"],
            status="failed" if terminal else "pending",
            attempts=attempts,
            next_attempt_at=_timestamp(_utc_now() + timedelta(seconds=delay)),
            last_status_code=status_code,
            last_error=error,
        )
        logger.warning("Webhook delivery %s failed: %s", delivery["id"], error)

    async def deliver_due(self, limit: int = 20) -> int:
        deliveries = await self._due_deliveries(limit)
        for delivery in deliveries:
            await self._deliver(delivery)
        return len(deliveries)

    async def prune_old_deliveries(self) -> None:
        cutoff = _timestamp(_utc_now() - timedelta(days=DELIVERY_RETENTION_DAYS))
        conn = await adb_connect(self.db_path)
        try:
            await conn.execute(
                "DELETE FROM webhook_deliveries WHERE status IN ('delivered', 'failed', 'cancelled') AND created_at < ?",
                (cutoff,),
            )
            await conn.commit()
        finally:
            await conn.close()

    async def run(self) -> None:
        """Background outbox worker. Pending deliveries survive app restarts."""
        last_prune = 0.0
        while True:
            try:
                loop = asyncio.get_running_loop()
                if loop.time() - last_prune >= 3600:
                    await self.prune_old_deliveries()
                    last_prune = loop.time()
                processed = await self.deliver_due()
                if processed:
                    continue
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=5)
                except TimeoutError:
                    pass
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Webhook dispatcher loop failed")
                await asyncio.sleep(5)

    def list_deliveries(self, limit: int = 50, endpoint_id: str | None = None) -> list[dict]:
        safe_limit = min(max(int(limit), 1), 200)
        conn = db_connect(self.db_path)
        try:
            if endpoint_id:
                rows = conn.execute(
                    """SELECT id, event_id, endpoint_id, event_name, status, attempts,
                              created_at, delivered_at, next_attempt_at, last_status_code, last_error
                       FROM webhook_deliveries WHERE endpoint_id = ?
                       ORDER BY created_at DESC LIMIT ?""",
                    (endpoint_id, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT id, event_id, endpoint_id, event_name, status, attempts,
                              created_at, delivered_at, next_attempt_at, last_status_code, last_error
                       FROM webhook_deliveries ORDER BY created_at DESC LIMIT ?""",
                    (safe_limit,),
                ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()
