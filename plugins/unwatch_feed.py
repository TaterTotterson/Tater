# plugins/unwatch_feed.py
import os
import logging
import redis
from dotenv import load_dotenv
from plugin_base import ToolPlugin

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)


class UnwatchFeedPlugin(ToolPlugin):
    name = "unwatch_feed"
    plugin_name = "Unwatch Feed"
    usage = (
        "{\n"
        '  "function": "unwatch_feed",\n'
        '  "arguments": {"feed_url": "<RSS feed URL>"}\n'
        "}\n"
    )
    description = "Removes an RSS feed provided by the user from the RSS watch list."
    plugin_dec = "Remove an RSS feed from the watch list."
    pretty_name = "Unwatch RSS Feed"
    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re removing the feed from the watch list now! "
        "Only output that message."
    )
    platforms = ["discord", "webui", "irc", "matrix"]

    async def _unwatch_feed(self, feed_url: str, username: str | None = None) -> str:
        prefix = f"{username}: " if username else ""
        if not feed_url:
            return f"{prefix}No feed URL provided for unwatching."

        removed = redis_client.hdel("rss:feeds", feed_url)
        if removed:
            return f"{prefix}Stopped watching feed: {feed_url}"
        else:
            return f"{prefix}Feed {feed_url} was not found in the watch list."

    # ---------- Discord ----------
    async def handle_discord(self, message, args, llm_client):
        feed_url = args.get("feed_url")
        return await self._unwatch_feed(feed_url)

    # ---------- WebUI ----------
    async def handle_webui(self, args, llm_client):
        feed_url = args.get("feed_url")
        return await self._unwatch_feed(feed_url)

    # ---------- IRC ----------
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        feed_url = args.get("feed_url")
        return await self._unwatch_feed(feed_url, username=user)

    # ---------- Matrix ----------
    async def handle_matrix(self, client, room, sender, body, args, ll_client=None, **kwargs):
        feed_url = args.get("feed_url")
        return await self._unwatch_feed(feed_url, username=sender)


plugin = UnwatchFeedPlugin()
