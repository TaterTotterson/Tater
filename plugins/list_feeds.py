# plugins/list_feeds.py
import os
import logging
import redis
from dotenv import load_dotenv
from plugin_base import ToolPlugin

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

class ListFeedsPlugin(ToolPlugin):
    name = "list_feeds"
    usage = (
        "{\n"
        '  "function": "list_feeds",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = "Lists the RSS feeds currently being watched."
    pretty_name = "Getting RSS Feeds"
    waiting_prompt_template = "Write a friendly, casual message telling {mention} you’re grabbing the current watched feeds now! Only output that message."
    platforms = ["discord", "webui", "irc"]

    async def _list_feeds(self, username: str = None):
        feeds = redis_client.hgetall("rss:feeds")
        prefix = f"{username}: " if username else ""
        if not feeds:
            return f"{prefix}No RSS feeds are currently being watched."

        if username:
            lines = [f"{username}: Currently watched feeds:"]
            lines += [f"{feed} (last update: {feeds[feed]})" for feed in feeds]
            return "\n".join(lines)
        else:
            feed_list = "\n".join(f"{feed} (last update: {feeds[feed]})" for feed in feeds)
            return f"Currently watched feeds:\n{feed_list}"

    async def handle_discord(self, message, args, llm_client):
        return await self._list_feeds()

    async def handle_webui(self, args, llm_client):
        return await self._list_feeds()

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._list_feeds(username=user)

plugin = ListFeedsPlugin()