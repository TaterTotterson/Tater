# plugins/watch_feed.py
import os
import feedparser
import logging
from dotenv import load_dotenv
from plugin_base import ToolPlugin
import redis
from typing import Optional
from rss_store import ensure_feed

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)


class WatchFeedPlugin(ToolPlugin):
    name = "watch_feed"
    plugin_name = "Watch Feed"
    version = "1.0.0"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "watch_feed",\n'
        '  "arguments": {"feed_url": "<RSS feed URL>"}\n'
        "}\n"
    )
    description = "Adds an RSS/Atom feed to the watch list; the poller posts only the newest item once, then tracks new entries."
    plugin_dec = "Add an RSS/Atom feed to the watch list and post only the newest item once."
    pretty_name = "Adding Your Feed"
    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re adding the feed to the watch list now! "
        "Only output that message."
    )
    platforms = ["discord", "webui", "irc", "matrix"]

    # -------- internals --------
    async def _watch_feed(self, feed_url: Optional[str], username: Optional[str] = None) -> str:
        prefix = f"{username}: " if username else ""
        if not feed_url:
            return f"{prefix}No feed URL provided for watching."

        try:
            parsed = feedparser.parse(feed_url)
        except Exception as e:
            logger.debug(f"[watch_feed] feedparser threw: {e}")
            return f"{prefix}Failed to parse feed: {feed_url}"

        # feedparser sets bozo on *any* parse issue, but the feed can still be usable.
        # If it’s bozo *and* has no entries/title, treat as failed.
        if getattr(parsed, "bozo", 0) and not getattr(parsed, "entries", None):
            return f"{prefix}Failed to parse feed: {feed_url}"

        # Set last_ts=0 so the poller posts only the newest item once.
        ensure_feed(redis_client, feed_url, 0.0)
        return f"{prefix}Now watching feed: {feed_url}"

    # -------- platform handlers --------
    async def handle_discord(self, message, args, llm_client):
        feed_url = args.get("feed_url")
        return await self._watch_feed(feed_url)

    async def handle_webui(self, args, llm_client):
        feed_url = args.get("feed_url")
        return await self._watch_feed(feed_url)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        feed_url = args.get("feed_url")
        return await self._watch_feed(feed_url, username=user)

    async def handle_matrix(self, client, room, sender, body, args, ll_client=None, **kwargs):
        feed_url = args.get("feed_url")
        return await self._watch_feed(feed_url, username=sender)


plugin = WatchFeedPlugin()
