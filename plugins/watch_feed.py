# plugins/watch_feed.py
import os
import time
import feedparser
import logging
from dotenv import load_dotenv
from plugin_base import ToolPlugin
from helpers import format_irc
import redis

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

class WatchFeedPlugin(ToolPlugin):
    name = "watch_feed"
    usage = (
        "{\n"
        '  "function": "watch_feed",\n'
        '  "arguments": {"feed_url": "<RSS feed URL>"}\n'
        "}\n"
    )
    description = "Adds an RSS feed provided by the user to the rss watch list."
    pretty_name = "Adding Your Feed"
    waiting_prompt_template = "Write a friendly message telling {mention} you’re adding the feed to the watch list now! Only output that message."
    platforms = ["discord", "webui", "irc"]

    async def _watch_feed(self, feed_url: str, username: str = None):
        prefix = f"{username}: " if username else ""

        if not feed_url:
            return f"{prefix}No feed URL provided for watching."

        parsed_feed = feedparser.parse(feed_url)
        if parsed_feed.bozo:
            return f"{prefix}Failed to parse feed: {feed_url}"

        last_ts = 0.0
        if parsed_feed.entries:
            for entry in parsed_feed.entries:
                if 'published_parsed' in entry:
                    entry_ts = time.mktime(entry.published_parsed)
                    if entry_ts > last_ts:
                        last_ts = entry_ts
        else:
            last_ts = time.time()

        redis_client.hset("rss:feeds", feed_url, last_ts)
        return f"{prefix}Now watching feed: {feed_url}"

    async def handle_discord(self, message, args, llm_client):
        feed_url = args.get("feed_url")
        return await self._watch_feed(feed_url)

    async def handle_webui(self, args, llm_client):
        feed_url = args.get("feed_url")
        return await self._watch_feed(feed_url)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        feed_url = args.get("feed_url")
        return await self._watch_feed(feed_url, username=user)

plugin = WatchFeedPlugin()