# plugins/watch_feed.py
import os
import time
import feedparser
import logging
from dotenv import load_dotenv
from plugin_base import ToolPlugin
import redis
from typing import Optional

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)


class WatchFeedPlugin(ToolPlugin):
    name = "watch_feed"
    plugin_name = "Watch Feed"
    usage = (
        "{\n"
        '  "function": "watch_feed",\n'
        '  "arguments": {"feed_url": "<RSS feed URL>"}\n'
        "}\n"
    )
    description = "Adds an RSS/Atom feed to the watch list and records the latest seen item so future checks only post new entries."
    plugin_dec = "Add an RSS/Atom feed to the watch list and track new items."
    pretty_name = "Adding Your Feed"
    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re adding the feed to the watch list now! "
        "Only output that message."
    )
    platforms = ["discord", "webui", "irc", "matrix"]

    # -------- internals --------
    def _latest_entry_ts(self, parsed_feed) -> float:
        """
        Return the most recent timestamp available from entries.
        Falls back to current time if none are timestamped.
        """
        last_ts = 0.0
        try:
            entries = getattr(parsed_feed, "entries", []) or []
            for entry in entries:
                # Prefer published_parsed, then updated_parsed
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    ts = time.mktime(entry.published_parsed)
                elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                    ts = time.mktime(entry.updated_parsed)
                else:
                    continue
                if ts > last_ts:
                    last_ts = ts
        except Exception as e:
            logger.debug(f"[watch_feed] timestamp scan failed: {e}")

        if last_ts <= 0.0:
            last_ts = time.time()
        return float(last_ts)

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

        last_ts = self._latest_entry_ts(parsed)
        # store as integer epoch seconds for simplicity
        redis_client.hset("rss:feeds", feed_url, int(last_ts))
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
