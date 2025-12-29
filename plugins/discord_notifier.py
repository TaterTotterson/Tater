# plugins/discord_notifier.py
import logging
import re
import requests
import asyncio
from urllib.parse import urlparse, parse_qs, urlunparse
from plugin_base import ToolPlugin
from plugin_settings import get_plugin_settings

logger = logging.getLogger("discord_notifier")

class DiscordNotifierPlugin(ToolPlugin):
    name = "discord_notifier"
    plugin_name = "Discord Notifier"
    description = "Posts RSS summaries to a Discord channel via webhook."
    plugin_dec = "Post RSS summaries to a Discord channel via webhook."
    usage = ""
    platforms = []
    settings_category = "Discord Notifier"
    notifier = True

    required_settings = {
        "discord_webhook_url": {
            "label": "Discord Webhook URL",
            "type": "string",
            "default": "",
            "description": "Paste the full webhook URL created in your Discord channel's Integrations tab."
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

    def format_summary_for_discord(self, text: str) -> str:
        # Replace [url](url) with just url (Discord doesn't auto-link this form)
        text = re.sub(r'\[(https?://[^\]]+)\]\(\1\)', r'\1', text)
        # Strip UTM params from all URLs
        text = re.sub(r'(https?://\S+)', lambda m: self.strip_utm(m.group(1)), text)
        # Convert ### headings to #
        text = text.replace("### ", "# ")
        return text

    async def notify(self, title: str, content: str):
        settings = get_plugin_settings(self.settings_category)
        webhook_url = settings.get("discord_webhook_url")

        if not webhook_url:
            logger.warning("Discord webhook URL is not set.")
            return

        formatted_message = self.format_summary_for_discord(content)

        def post_to_discord():
            try:
                return requests.post(webhook_url, json={"content": formatted_message}, timeout=10)
            except Exception as e:
                return e

        result = await asyncio.to_thread(post_to_discord)

        if isinstance(result, Exception):
            logger.warning(f"Exception while posting to Discord webhook: {result}")
        elif result.status_code >= 400:
            logger.warning(f"Failed to post to Discord webhook: {result.status_code} {result.text}")

plugin = DiscordNotifierPlugin()
