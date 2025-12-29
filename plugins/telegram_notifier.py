# plugins/telegram_notifier.py
import html
import re
import requests
import logging
import asyncio
from urllib.parse import urlparse, parse_qs, urlunparse
from plugin_base import ToolPlugin
from plugin_settings import get_plugin_settings

logger = logging.getLogger("telegram_notifier")

class TelegramNotifierPlugin(ToolPlugin):
    name = "telegram_notifier"
    plugin_name = "Telegram Notifier"
    description = "Provides Telegram bot token and chat ID settings for RSS announcements."
    plugin_dec = "Send RSS summaries to a Telegram chat via bot token and chat ID."
    usage = ""
    platforms = []
    settings_category = "Telegram Notifier"
    notifier = True  # Used by RSS manager to detect notifiers

    required_settings = {
        "telegram_bot_token": {
            "label": "Telegram Bot Token",
            "type": "string",
            "default": "",
            "description": "Bot token from @BotFather"
        },
        "telegram_chat_id": {
            "label": "Telegram Channel ID",
            "type": "string",
            "default": "",
            "description": "Channel or group ID (usually starts with -100...)"
        }
    }

    def strip_utm(self, url: str) -> str:
        try:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            clean_query = {k: v for k, v in query.items() if not k.lower().startswith("utm_")}
            parsed = parsed._replace(query="&".join(f"{k}={v[0]}" for k, v in clean_query.items()))
            return urlunparse(parsed)
        except Exception:
            return url

    def post_to_telegram(self, message: str):
        settings = get_plugin_settings(self.settings_category)
        bot_token = settings.get("telegram_bot_token")
        chat_id = settings.get("telegram_chat_id")

        if not bot_token or not chat_id:
            logger.debug("Telegram bot token or chat ID not set.")
            return

        try:
            lines = message.strip().splitlines()
            cleaned_lines = []

            for line in lines:
                line = line.strip()

                # Convert Discord-style **bold**
                line = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", line)

                # Convert ## Header to bold
                if line.startswith("##"):
                    line = f"<b>{html.escape(line.lstrip('#').strip())}</b>"
                    cleaned_lines.append(line)
                    continue

                # Handle bullets: *, -, • → •
                if line.startswith(("* ", "- ", "• ")):
                    line = f"• {line[2:].strip()}"

                # Convert bare URLs to links with stripped UTM
                if re.fullmatch(r"https?://\S+", line):
                    url = self.strip_utm(line)
                    escaped_url = html.escape(url)
                    line = f'<a href="{escaped_url}">{escaped_url}</a>'
                else:
                    line = html.escape(line, quote=False)
                    line = line.replace("&lt;b&gt;", "<b>").replace("&lt;/b&gt;", "</b>")

                cleaned_lines.append(line)

            formatted_message = "\n".join(cleaned_lines)

            payload = {
                "chat_id": chat_id,
                "text": formatted_message,
                "parse_mode": "HTML",
                "disable_web_page_preview": False
            }

            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            requests.post(url, json=payload, timeout=10)

        except Exception as e:
            logger.warning(f"Failed to send Telegram message: {e}")

    async def notify(self, title: str, content: str):
        await asyncio.to_thread(self.post_to_telegram, content)

plugin = TelegramNotifierPlugin()
