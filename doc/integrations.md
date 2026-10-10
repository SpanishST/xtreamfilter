# Notifications and Integrations

## Telegram and Jellyfin

Telegram can send automatic category notifications, monitoring notifications, and optional download notifications. Jellyfin can trigger a full library scan after successful downloads.

### Telegram Setup

1. Create a bot with [@BotFather](https://t.me/BotFather).
2. Get your chat ID.
3. Configure the token and chat ID in the UI or API.
4. Send a test notification.

Telegram and Jellyfin configuration endpoints are listed in the [API reference](api-reference.md#integration-apis).

## Webhooks

Configure one or more HTTP webhook receivers from **Settings → Webhooks**. Each endpoint has an independent URL, enabled flag, optional signing secret, and event subscriptions. Requests are queued in SQLite and delivered in the background, so a slow receiver does not block cache refresh or downloads.

Failed requests retry with exponential backoff (up to 8 attempts). Terminal delivery records are retained for 30 days, and delivery history is visible in Webhooks settings.

Available events:

- `cache.refresh.started`, `cache.refresh.completed`, `cache.refresh.failed`, `cache.refresh.cancelled`
- `cart.item.added`
- `download.queue.started`, `download.queue.completed`
- `download.item.started`, `download.item.completed`, `download.item.failed`, `download.item.cancelled`

Every request contains a versioned JSON event with an `id`, `event`, `occurred_at`, and `data`. The `Idempotency-Key` header contains the event ID; receivers should use it to recognize retries. When an endpoint has a signing secret, requests include `X-Webhook-Timestamp` and `X-Webhook-Signature`. The signature is `sha256=` followed by the HMAC-SHA256 hex digest of `<timestamp>.<raw request body>`. Other headers include `X-Webhook-Event` and `X-Webhook-Delivery`.

The Test button sends a `webhook.test` event directly to that endpoint. Webhook management endpoints are listed in the [API reference](api-reference.md#webhook-api).
