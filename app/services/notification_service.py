"""Notification service — all Telegram messaging logic."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from rapidfuzz import fuzz

from app.services.filter_service import normalize_name

if TYPE_CHECKING:
    from app.services.config_service import ConfigService
    from app.services.http_client import HttpClientService

logger = logging.getLogger(__name__)


class NotificationService:
    """Sends Telegram notifications for categories and downloads."""

    def __init__(self, config_service: "ConfigService", http_client: "HttpClientService"):
        self.config_service = config_service
        self.http_client = http_client

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_telegram_credentials(self) -> tuple[str, str] | None:
        cfg = self.config_service.get_telegram_config()
        if not cfg.get("enabled"):
            return None
        bot_token = cfg.get("bot_token", "")
        chat_id = cfg.get("chat_id", "")
        if not bot_token or not chat_id:
            return None
        return bot_token, chat_id

    @staticmethod
    def _format_bytes(b: int) -> str:
        if not b or b == 0:
            return "0 B"
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(b) < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} PB"

    @staticmethod
    def _get_download_item_display_name(item: dict) -> str:
        if item.get("content_type") == "series" and item.get("series_name"):
            name = item["series_name"]
            name += f" S{str(item.get('season', 1)).zfill(2)}E{str(item.get('episode_num', 0)).zfill(2)}"
            if item.get("episode_title"):
                name += f" - {item['episode_title']}"
            return name
        return item.get("name", "Unknown")

    def _group_new_items_for_notification(self, new_items: list) -> list:
        if not new_items:
            return []
        groups: list[dict] = []
        config = self.config_service.config
        sources_by_id = {s.get("id"): s.get("name", s.get("id", "?")) for s in config.get("sources", [])}
        for item in new_items:
            item_name = item.get("name", "")
            item_normalized = normalize_name(item_name)
            source_name = sources_by_id.get(item.get("source_id", ""), "")
            best_group = None
            best_score = 0
            for group in groups:
                score = fuzz.token_sort_ratio(item_normalized, group["normalized"])
                if score >= 85 and score > best_score:
                    best_score = score
                    best_group = group
            if best_group is not None:
                if source_name and source_name not in best_group["sources"]:
                    best_group["sources"].append(source_name)
                if not best_group["cover"] and item.get("cover"):
                    best_group["cover"] = item["cover"]
                if len(item_name) < len(best_group["name"]):
                    best_group["name"] = item_name
            else:
                groups.append(
                    {
                        "normalized": item_normalized,
                        "name": item_name,
                        "cover": item.get("cover", ""),
                        "sources": [source_name] if source_name else [],
                    }
                )
        return [
            {"name": g["name"], "cover": g["cover"], "sources": g["sources"], "count": len(g["sources"])}
            for g in groups
        ]

    @staticmethod
    def _format_grouped_item_line(grouped_item: dict) -> str:
        name = grouped_item.get("name", "")
        sources = grouped_item.get("sources", [])
        if len(sources) > 1:
            return f"• {name} ({', '.join(sources)})\n"
        elif len(sources) == 1:
            return f"• {name} ({sources[0]})\n"
        return f"• {name}\n"

    # ------------------------------------------------------------------
    # Category notification
    # ------------------------------------------------------------------

    async def send_category_notification(self, category_name: str, new_items: list) -> None:
        creds = self._get_telegram_credentials()
        if not creds:
            return
        bot_token, chat_id = creds
        if not new_items:
            return
        grouped = self._group_new_items_for_notification(new_items)
        unique_count = len(grouped)
        try:
            client = await self.http_client.get_client()
            groups_with_covers = [g for g in grouped if g.get("cover")]

            if len(groups_with_covers) >= 2:
                media_items = groups_with_covers[:10]
                media = []
                for i, g in enumerate(media_items):
                    media_obj: dict = {"type": "photo", "media": g["cover"]}
                    if i == 0:
                        caption = f"🆕 <b>{category_name}</b> - {unique_count} new unique title(s)\n\n"
                        for gr in grouped:
                            line = self._format_grouped_item_line(gr)
                            if len(caption) + len(line) < 1000:
                                caption += line
                            else:
                                break
                        caption = caption.rstrip()
                        media_obj["caption"] = caption
                        media_obj["parse_mode"] = "HTML"
                    media.append(media_obj)
                url = f"https://api.telegram.org/bot{bot_token}/sendMediaGroup"
                response = await client.post(url, json={"chat_id": chat_id, "media": media})
                if response.status_code != 200:
                    await self._send_text_message(client, bot_token, chat_id, category_name, grouped)
                else:
                    items_in_caption = caption.count("• ")
                    if items_in_caption < unique_count:
                        await self._send_text_message(client, bot_token, chat_id, category_name, grouped)
            elif len(groups_with_covers) == 1:
                g = groups_with_covers[0]
                caption = f"🆕 <b>{category_name}</b>\n\n" + self._format_grouped_item_line(g).rstrip()
                url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
                response = await client.post(
                    url, json={"chat_id": chat_id, "photo": g["cover"], "caption": caption, "parse_mode": "HTML"}
                )
                if response.status_code != 200:
                    await self._send_text_message(client, bot_token, chat_id, category_name, grouped)
            else:
                await self._send_text_message(client, bot_token, chat_id, category_name, grouped)
            logger.info(f"Telegram notification sent for category '{category_name}': {len(new_items)} new items")
        except Exception as e:
            logger.error(f"Failed to send Telegram notification: {e}")

    async def _send_text_message(self, client, bot_token: str, chat_id: str, category_name: str, grouped_items: list):
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        MAX_MESSAGE_LENGTH = 4096
        unique_count = len(grouped_items)
        header = f"🆕 <b>{category_name}</b> - {unique_count} new unique title(s)\n\n"
        all_lines = [self._format_grouped_item_line(g) for g in grouped_items]
        messages: list[str] = []
        current_message = header
        for item_line in all_lines:
            if len(current_message) + len(item_line) > MAX_MESSAGE_LENGTH - 50:
                messages.append(current_message.rstrip())
                part_num = len(messages) + 1
                current_message = f"📋 <b>{category_name}</b> (continued - part {part_num})\n\n{item_line}"
            else:
                current_message += item_line
        if current_message.strip():
            messages.append(current_message.rstrip())
        for message in messages:
            await client.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"})

    # ------------------------------------------------------------------
    # Download notifications
    # ------------------------------------------------------------------

    async def send_download_file_notification(self, item: dict) -> None:
        config = self.config_service.config
        opts = config.get("options", {})
        if not opts.get("download_notify_file", False):
            return
        creds = self._get_telegram_credentials()
        if not creds:
            return
        bot_token, chat_id = creds
        name = self._get_download_item_display_name(item)
        status = item.get("status", "unknown")
        icon = "✅" if status == "completed" else "❌"
        file_size = self._format_bytes(item.get("file_size", 0)) if status == "completed" else ""
        error_msg = f"\n⚠️ Error: {item.get('error', '')}" if item.get("error") else ""
        size_line = f"\n💾 Size: {file_size}" if file_size else ""
        text = f"{icon} <b>Download {status}</b>\n\n🎬 {name}{size_line}{error_msg}"
        try:
            client = await self.http_client.get_client()
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
        except Exception as e:
            logger.error(f"Failed to send Telegram download notification: {e}")

    async def send_download_queue_complete_notification(self, cart_items: list) -> None:
        config = self.config_service.config
        opts = config.get("options", {})
        if not opts.get("download_notify_queue", False):
            return
        creds = self._get_telegram_credentials()
        if not creds:
            return
        bot_token, chat_id = creds
        completed = [i for i in cart_items if i.get("status") == "completed"]
        failed = [i for i in cart_items if i.get("status") in ("failed", "cancelled")]
        total = len(completed) + len(failed)
        if total == 0:
            return
        text = "🏁 <b>Download queue finished</b>\n\n"
        text += f"✅ Completed: {len(completed)}\n"
        if failed:
            text += f"❌ Failed: {len(failed)}\n"
        text += "\n"
        if completed:
            text += "<b>Completed:</b>\n"
            for item in completed[:20]:
                name = self._get_download_item_display_name(item)
                size = self._format_bytes(item.get("file_size", 0))
                text += f"  • {name} ({size})\n"
            if len(completed) > 20:
                text += f"  ... and {len(completed) - 20} more\n"
        if failed:
            text += "\n<b>Failed:</b>\n"
            for item in failed[:10]:
                name = self._get_download_item_display_name(item)
                err = item.get("error", "Unknown error")
                text += f"  • {name} — {err}\n"
            if len(failed) > 10:
                text += f"  ... and {len(failed) - 10} more\n"
        if len(text) > 4000:
            text = text[:3990] + "\n..."
        try:
            client = await self.http_client.get_client()
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
        except Exception as e:
            logger.error(f"Failed to send Telegram queue-complete notification: {e}")

    # ------------------------------------------------------------------
    # Monitor notification
    # ------------------------------------------------------------------

    async def send_monitor_notification(self, series_name: str, new_episodes: list, cover: str) -> None:
        creds = self._get_telegram_credentials()
        if not creds:
            return
        bot_token, chat_id = creds
        count = len(new_episodes)
        ep_lines: list[str] = []
        for ep in new_episodes[:15]:
            season = ep.get("season", "?")
            ep_num = ep.get("episode_num", "?")
            title = ep.get("episode_title") or ep.get("name", "")
            ep_lines.append(f"  • S{int(season):02d}E{int(ep_num):02d} — {title}")
        text = f"📡 <b>Series Monitor</b> — <b>{series_name}</b>\n"
        text += f"{count} new episode(s) detected and queued:\n\n"
        text += "\n".join(ep_lines)
        if count > 15:
            text += f"\n  ... and {count - 15} more"
        try:
            client = await self.http_client.get_client()
            if cover:
                url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
                payload = {"chat_id": chat_id, "photo": cover, "caption": text, "parse_mode": "HTML"}
                response = await client.post(url, json=payload)
                if response.status_code != 200:
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
            else:
                url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
            logger.info(f"Telegram monitoring notification sent for '{series_name}'")
        except Exception as e:
            logger.error(f"Failed to send monitoring notification: {e}")

    # ------------------------------------------------------------------
    # Test notification
    # ------------------------------------------------------------------

    async def send_test_notification(self) -> dict:
        config = self.config_service.config
        telegram_config = config.get("options", {}).get("telegram", {})
        bot_token = telegram_config.get("bot_token", "")
        chat_id = telegram_config.get("chat_id", "")
        if not bot_token or not chat_id:
            return {"status": "error", "message": "Bot token and chat ID are required"}
        try:
            client = await self.http_client.get_client()
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": "🔔 <b>Test Notification</b>\n\nYour Telegram integration is working correctly!",
                "parse_mode": "HTML",
            }
            response = await client.post(url, json=payload)
            result = response.json()
            if response.status_code == 200 and result.get("ok"):
                return {"status": "ok", "message": "Test notification sent successfully"}
            return {"status": "error", "message": f"Telegram API error: {result.get('description', 'Unknown error')}"}
        except Exception as e:
            return {"status": "error", "message": f"Failed to send test: {str(e)}"}
