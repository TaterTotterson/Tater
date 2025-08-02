# plugins/unwatch_feed.py
import os
import logging
import redis
from dotenv import load_dotenv
from plugin_base import ToolPlugin
from helpers import format_irc  # optional, for consistent IRC output

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

class UnwatchFeedPlugin(ToolPlugin):
    name = "unwatch_feed"
    usage = (
        "{\n"
        '  "function": "unwatch_feed",\n'
        '  "arguments": {"feed_url": "<RSS feed URL>"}\n'
        "}\n"
    )
    description = "Removes an RSS feed provided by the user from the rss watch list."
    waiting_prompt_template = "Write a friendly message telling {mention} you’re removing the feed from the watch list now! Only output that message."
    platforms = ["discord", "webui", "irc"]

    async def _unwatch_feed(self, feed_url: str, username: str = None):
        if not feed_url:
            return f"{username + ': ' if username else ''}No feed URL provided for unwatching."

        removed = redis_client.hdel("rss:feeds", feed_url)
        if removed:
            return f"{username + ': ' if username else ''}Stopped watching feed: {feed_url}"
        else:
            return f"{username + ': ' if username else ''}Feed {feed_url} was not found in the watch list."

    async def handle_discord(self, message, args, ollama_client):
        feed_url = args.get("feed_url")
        return await self._unwatch_feed(feed_url)

    async def handle_webui(self, args, ollama_client):
        feed_url = args.get("feed_url")
        return await self._unwatch_feed(feed_url)

    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        feed_url = args.get("feed_url")
        return await self._unwatch_feed(feed_url, username=user)

plugin = UnwatchFeedPlugin()