# plugins/list_feeds.py
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


class ListFeedsPlugin(ToolPlugin):
    name = "list_feeds"
    plugin_name = "List Feeds"
    usage = (
        "{\n"
        '  "function": "list_feeds",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = "Lists the RSS feeds currently being watched."
    plugin_dec = "Show the RSS feeds currently being watched."
    pretty_name = "Getting RSS Feeds"
    waiting_prompt_template = (
        "Write a friendly, casual message telling {mention} you’re grabbing the current watched feeds now! Only output that message."
    )
    platforms = ["discord", "webui", "irc", "matrix"]

    async def _list_feeds(self, username: str | None = None) -> str:
        feeds = redis_client.hgetall("rss:feeds") or {}
        # sort for stable output
        items = sorted(feeds.items(), key=lambda kv: kv[0].lower())

        prefix = f"{username}: " if username else ""
        if not items:
            return f"{prefix}No RSS feeds are currently being watched."

        if username:
            lines = [f"{username}: Currently watched feeds:"]
            lines += [f"{name} (last update: {last})" for name, last in items]
            return "\n".join(lines)
        else:
            feed_list = "\n".join(f"{name} (last update: {last})" for name, last in items)
            return f"Currently watched feeds:\n{feed_list}"

    # ---------- Discord ----------
    async def handle_discord(self, message, args, llm_client):
        return await self._list_feeds()

    # ---------- WebUI ----------
    async def handle_webui(self, args, llm_client):
        return await self._list_feeds()

    # ---------- IRC ----------
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._list_feeds(username=user)

    # ---------- Matrix ----------
    async def handle_matrix(self, client, room, sender, body, args, ll_client=None, **kwargs):
        # Keep it simple: same content style as Discord/WebUI; no username prefix needed.
        # (If you prefer sender-prefixed lines, pass username=sender instead.)
        return await self._list_feeds()


plugin = ListFeedsPlugin()
