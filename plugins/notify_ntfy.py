# plugins/notify_ntfy.py
import re
import logging
import requests
import asyncio
import html
from urllib.parse import urlparse, parse_qs, urlunparse
from plugin_base import ToolPlugin
from plugin_settings import get_plugin_settings

logger = logging.getLogger("notify_ntfy")

class NtfyNotifierPlugin(ToolPlugin):
    name = "notify_ntfy"
    plugin_name = "Ntfy Notifier"
    pretty_name = "Ntfy Notifier"
    version = "1.0.0"
    min_tater_version = "50"
    description = "Sends RSS announcements to an ntfy topic (self-hosted or ntfy.sh)."
    plugin_dec = "Send RSS announcements to an ntfy topic."
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
    MARKDOWN_LINK_PATTERN = re.compile(r"\[([^\]]+)\]\((https?://[^\s)]+)\)")
    BARE_URL_PATTERN = re.compile(r"(?<!\()(?<!\])\bhttps?://\S+\b")

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

    def _short_url_text(self, url: str) -> str:
        try:
            p = urlparse(url)
            return f"{p.netloc}{p.path}".rstrip("/")
        except Exception:
            return url

    def _linkify_bare_urls(self, text: str) -> str:
        # Convert bare URLs to [domain/path](url), skipping ones already in markdown links
        def repl(m: re.Match):
            raw = m.group(0)
            clean = self.strip_utm(raw)
            label = self._short_url_text(clean)
            return f"[{label}]({clean})"
        return self.BARE_URL_PATTERN.sub(repl, text)

    def format_markdown(self, title: str, message: str) -> str:
        # Preserve Discord-style markdown, just enhance:
        msg = html.unescape(message or "").strip()

        # Normalize simple bullets (Discord uses *, -, • … all fine in Markdown)
        lines = []
        for line in msg.splitlines():
            s = line.rstrip()
            # Keep existing markdown; just ensure bullets have a space after symbol
            if s.startswith(("*", "-", "•")) and not s.startswith(("* ", "- ", "• ")):
                s = s[0] + " " + s[1:].lstrip()
            lines.append(s)
        msg = "\n".join(lines).strip()

        # Convert bare URLs to Markdown links & strip utm_ params
        msg = self._linkify_bare_urls(msg)

        # Prepend title as H2
        head = f"## {title.strip()}\n\n" if title and title.strip() else ""
        return f"{head}{msg}"

    def post_to_ntfy(self, title: str, message: str) -> bool:
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
            return False

        url = f"{server}/{topic}"
        headers = {
            # "Title": title or "",
            "Priority": priority if priority in {"1", "2", "3", "4", "5"} else "3",
            "Markdown": "yes",  # render Markdown in ntfy
        }

        if tags:
            norm = ",".join([t.strip() for t in re.split(r"[,\s]+", tags) if t.strip()])
            if norm:
                headers["Tags"] = norm

        if use_click:
            click_url = self._first_url(message)
            if click_url:
                headers["Click"] = click_url

        auth = None
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif username and password:
            auth = (username, password)

        try:
            body = self.format_markdown(title, message)
            resp = requests.post(url, data=body.encode("utf-8"), headers=headers, auth=auth, timeout=10)
            if resp.status_code >= 300:
                logger.warning(f"ntfy publish failed ({resp.status_code}): {resp.text[:300]}")
                return False
            return True
        except Exception as e:
            logger.warning(f"Failed to send ntfy message: {e}")
            return False

    async def notify(self, title: str, content: str, targets=None, origin=None, meta=None):
        ok = await asyncio.to_thread(self.post_to_ntfy, title, content)
        if ok:
            return "Queued notification for ntfy"
        return "Cannot queue: missing ntfy topic or send failed"

plugin = NtfyNotifierPlugin()
