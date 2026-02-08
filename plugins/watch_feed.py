import logging
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

import feedparser

from helpers import redis_client
from plugin_base import ToolPlugin
from rss_store import ensure_feed, get_feed

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


class WatchFeedPlugin(ToolPlugin):
    name = "rss_watch"
    plugin_name = "RSS Watch"
    version = "1.1.0"
    min_tater_version = "50"
    when_to_use = "Use when the user asks to subscribe to or watch an RSS/Atom feed URL."
    optional_args = ["url", "rss_url", "feed"]
    usage = (
        "{\n"
        '  "function": "rss_watch",\n'
        '  "arguments": {"feed_url": "<RSS feed URL>"}\n'
        "}\n"
    )
    description = "Adds an RSS/Atom feed to the watch list; the poller posts only the newest item once, then tracks new entries."
    plugin_dec = "Add an RSS/Atom feed to the watch list and post only the newest item once."
    pretty_name = "Adding Your Feed"
    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are adding the feed to the watch list now. "
        "Only output that message."
    )
    platforms = ["discord", "webui", "irc", "matrix", "telegram"]

    # -------- internals --------
    async def _watch_feed(self, feed_url: Optional[str], username: Optional[str] = None) -> str:
        prefix = f"{username}: " if username else ""
        if not feed_url:
            return f"{prefix}No feed URL provided for watching."

        normalized_url = _normalize_feed_url(feed_url)
        if not normalized_url:
            return f"{prefix}Please provide a valid http/https RSS feed URL."

        if get_feed(redis_client, normalized_url):
            return f"{prefix}Already watching feed: {normalized_url}"

        try:
            parsed = feedparser.parse(normalized_url)
        except Exception as e:
            logger.debug(f"[watch_feed] feedparser threw: {e}")
            return f"{prefix}Failed to parse feed: {normalized_url}"

        entries = list(getattr(parsed, "entries", None) or [])
        feed_meta = getattr(parsed, "feed", None) or {}
        title = ""
        if isinstance(feed_meta, dict):
            title = str(feed_meta.get("title") or "").strip()
        else:
            title = str(getattr(feed_meta, "title", "") or "").strip()
        bozo = bool(getattr(parsed, "bozo", 0))
        if bozo and not entries and not title:
            return f"{prefix}Failed to parse feed: {normalized_url}"
        if not entries and not title:
            return f"{prefix}That URL does not look like a valid RSS/Atom feed: {normalized_url}"

        # Set last_ts=0 so the poller posts only the newest item once.
        ensure_feed(redis_client, normalized_url, 0.0)
        if title:
            return f"{prefix}Now watching feed: {normalized_url} ({title})"
        return f"{prefix}Now watching feed: {normalized_url}"

    # -------- platform handlers --------
    async def handle_discord(self, message, args, llm_client):
        feed_url = _extract_feed_url(args or {})
        return await self._watch_feed(feed_url)

    async def handle_webui(self, args, llm_client):
        feed_url = _extract_feed_url(args or {})
        return await self._watch_feed(feed_url)

    async def handle_telegram(self, update, args, llm_client):
        feed_url = _extract_feed_url(args or {})
        return await self._watch_feed(feed_url)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        feed_url = _extract_feed_url(args or {})
        return await self._watch_feed(feed_url, username=user)

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        feed_url = _extract_feed_url(args or {})
        return await self._watch_feed(feed_url)


plugin = WatchFeedPlugin()
