import logging
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

from helpers import redis_client
from plugin_base import ToolPlugin
from rss_store import delete_feed, get_all_feeds

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _extract_feed_url(args: dict) -> str:
    if not isinstance(args, dict):
        return ""
    for key in ("feed_url", "url", "rss_url", "feed"):
        value = args.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _normalize_feed_url(raw_url: str) -> str:
    raw = (raw_url or "").strip().strip("<>").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
    except Exception:
        return ""

    scheme = (parsed.scheme or "").lower()
    netloc = (parsed.netloc or "").lower().strip()
    if scheme not in {"http", "https"} or not netloc:
        return ""

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")

    return urlunsplit((scheme, netloc, path, parsed.query or "", ""))


class UnwatchFeedPlugin(ToolPlugin):
    name = "rss_unwatch"
    plugin_name = "RSS Unwatch"
    version = "1.1.0"
    min_tater_version = "50"
    when_to_use = "Use when the user asks to stop watching or unsubscribe from an RSS/Atom feed."
    optional_args = ["url", "rss_url", "feed"]
    usage = '{"function":"unwatch_feed","arguments":{"feed_url":"<RSS feed URL>"}}'
    description = "Removes an RSS feed provided by the user from the RSS watch list."
    plugin_dec = "Remove an RSS feed from the watch list."
    pretty_name = "Unwatch RSS Feed"
    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are removing the feed from the watch list now. "
        "Only output that message."
    )
    platforms = ["discord", "webui", "irc", "matrix", "telegram"]

    async def _unwatch_feed(self, feed_url: Optional[str], username: Optional[str] = None) -> str:
        prefix = f"{username}: " if username else ""
        if not feed_url:
            return f"{prefix}No feed URL provided for unwatching."

        raw_input = str(feed_url).strip()
        normalized = _normalize_feed_url(raw_input)
        feeds = get_all_feeds(redis_client) or {}

        target_feed = ""
        if raw_input in feeds:
            target_feed = raw_input
        elif normalized and normalized in feeds:
            target_feed = normalized
        elif normalized:
            for existing_url in feeds.keys():
                if _normalize_feed_url(existing_url) == normalized:
                    target_feed = existing_url
                    break

        if not target_feed:
            shown = normalized or raw_input
            return f"{prefix}Feed {shown} was not found in the watch list."

        removed = delete_feed(redis_client, target_feed)
        if removed:
            return f"{prefix}Stopped watching feed: {target_feed}"
        else:
            return f"{prefix}Feed {target_feed} was not found in the watch list."

    # ---------- Discord ----------
    async def handle_discord(self, message, args, llm_client):
        feed_url = _extract_feed_url(args or {})
        return await self._unwatch_feed(feed_url)

    # ---------- WebUI ----------
    async def handle_webui(self, args, llm_client):
        feed_url = _extract_feed_url(args or {})
        return await self._unwatch_feed(feed_url)

    # ---------- Telegram ----------
    async def handle_telegram(self, update, args, llm_client):
        feed_url = _extract_feed_url(args or {})
        return await self._unwatch_feed(feed_url)

    # ---------- IRC ----------
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        feed_url = _extract_feed_url(args or {})
        return await self._unwatch_feed(feed_url, username=user)

    # ---------- Matrix ----------
    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        feed_url = _extract_feed_url(args or {})
        return await self._unwatch_feed(feed_url)


plugin = UnwatchFeedPlugin()
