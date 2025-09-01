# plugins/ntfy_notifier.py
import re
import logging
import requests
import asyncio
import html
from urllib.parse import urlparse, parse_qs, urlunparse
from plugin_base import ToolPlugin
from plugin_settings import get_plugin_settings

logger = logging.getLogger("ntfy_notifier")

class NtfyNotifierPlugin(ToolPlugin):
    name = "ntfy_notifier"
    description = "Sends RSS announcements to an ntfy topic (self-hosted or ntfy.sh)."
    usage = ""
    platforms = []
    settings_category = "NTFY Notifier"
    notifier = True

    required_settings = {
        "ntfy_server": {
            "label": "ntfy Server URL",
            "type": "string",
            "default": "https://ntfy.sh",
            "description": "Base server URL (e.g., https://ntfy.sh or your self-hosted https://ntfy.example.com)"
        },
        "ntfy_topic": {
            "label": "ntfy Topic",
            "type": "string",
            "default": "",
            "description": "Topic name/channel to publish to (e.g., tater_updates)"
        },
        "ntfy_priority": {
            "label": "Priority (1-5)",
            "type": "string",
            "default": "3",
            "description": "1=min, 3=default, 5=max"
        },
        "ntfy_tags": {
            "label": "Tags (comma-separated)",
            "type": "string",
            "default": "",
            "description": "Optional tags/emojis for ntfy (e.g., rss,news,mega)"
        },
        "ntfy_click_from_first_url": {
            "label": "Use first URL in message as Click action",
            "type": "bool",
            "default": True,
            "description": "If found, set ntfy Click header so notification opens the article"
        },
        "ntfy_token": {
            "label": "Bearer Token (optional)",
            "type": "string",
            "default": "",
            "description": "If your server requires auth via token, set it here"
        },
        "ntfy_username": {
            "label": "Username (optional)",
            "type": "string",
            "default": "",
            "description": "For Basic Auth; ignored if token is set"
        },
        "ntfy_password": {
            "label": "Password (optional)",
            "type": "string",
            "default": "",
            "description": "For Basic Auth; ignored if token is set"
        },
    }

    URL_PATTERN = re.compile(r"https?://\S+")

    def format_plaintext(self, title: str, message: str) -> str:
        lines = message.strip().splitlines()
        cleaned = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Remove Markdown bold/headers
            line = re.sub(r"\*\*(.+?)\*\*", r"\1", line)
            if line.startswith("##"):
                line = line.lstrip("#").strip()

            # Normalize bullets
            if line.startswith(("* ", "- ", "• ")):
                line = f"• {line[2:].strip()}"

            # Strip UTM params from bare URLs
            if re.fullmatch(r"https?://\S+", line):
                line = self.strip_utm(line)

            # Decode HTML entities
            line = html.unescape(line)

            cleaned.append(line)

        # Put the article title at the very top, then a blank line, then content
        output = []
        if title:
            output.append(title.strip())
            output.append("")  # blank line
        output.extend(cleaned)

        return "\n".join(output)

    def strip_utm(self, url: str) -> str:
        try:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            clean_query = {k: v for k, v in query.items() if not k.lower().startswith("utm_")}
            parsed = parsed._replace(query="&".join(f"{k}={v[0]}" for k, v in clean_query.items()))
            return urlunparse(parsed)
        except Exception:
            return url

    def _first_url(self, text: str) -> str | None:
        m = self.URL_PATTERN.search(text or "")
        if not m:
            return None
        return self.strip_utm(m.group(0))

    def post_to_ntfy(self, title: str, message: str):
        settings = get_plugin_settings(self.settings_category)
        server = (settings.get("ntfy_server") or "https://ntfy.sh").rstrip("/")
        topic = (settings.get("ntfy_topic") or "").strip()
        priority = str(settings.get("ntfy_priority") or "3").strip()
        tags = (settings.get("ntfy_tags") or "").strip()
        use_click = bool(settings.get("ntfy_click_from_first_url")) if settings.get("ntfy_click_from_first_url") is not None else True

        token = (settings.get("ntfy_token") or "").strip()
        username = (settings.get("ntfy_username") or "").strip()
        password = (settings.get("ntfy_password") or "").strip()

        if not topic:
            logger.debug("ntfy topic not set; skipping.")
            return

        url = f"{server}/{topic}"
        headers = {
            "Title": title or "",
            "Priority": priority if priority in {"1", "2", "3", "4", "5"} else "3",
        }

        if tags:
            # Accept comma or space separated; ntfy prefers comma-separated
            norm = ",".join([t.strip() for t in re.split(r"[,\s]+", tags) if t.strip()])
            if norm:
                headers["Tags"] = norm

        # Optional Click action from first URL in message
        if use_click:
            click_url = self._first_url(message)
            if click_url:
                headers["Click"] = click_url

        # Auth header preference: Bearer token > Basic auth
        auth = None
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif username and password:
            auth = (username, password)

        try:
            # ntfy body is plain text. Keep content as-is, but strip UTM on bare-URL-only lines
            # (We won't do HTML/Markdown conversion here; clients render plain text well.)
            body = self.format_plaintext(title, message)

            resp = requests.post(url, data=body.encode("utf-8"), headers=headers, auth=auth, timeout=10)
            if resp.status_code >= 300:
                logger.warning(f"ntfy publish failed ({resp.status_code}): {resp.text[:300]}")
        except Exception as e:
            logger.warning(f"Failed to send ntfy message: {e}")

    async def notify(self, title: str, content: str):
        # Mirror Telegram plugin pattern
        await asyncio.to_thread(self.post_to_ntfy, title, content)

plugin = NtfyNotifierPlugin()
