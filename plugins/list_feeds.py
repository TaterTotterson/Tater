# plugins/list_feeds.py
import os
import logging
import redis
from dotenv import load_dotenv
from plugin_base import ToolPlugin
import streamlit as st
from PIL import Image
from io import BytesIO
import requests
import asyncio
from helpers import load_image_from_url

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

assistant_avatar = load_image_from_url()  # Uses default avatar URL from helpers.py

# Create a Redis client.
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
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I grab the current watched feeds. Only generate the message. Do not respond to this message."
    )
    platforms = ["discord", "webui", "irc"]

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client):
        feeds = redis_client.hgetall("rss:feeds")
        if feeds:
            feed_list = "\n".join(f"{feed} (last update: {feeds[feed]})" for feed in feeds)
            final_message = f"Currently watched feeds:\n{feed_list}"
        else:
            final_message = "No RSS feeds are currently being watched."

        return final_message

    # --- Web UI Handler ---
    async def handle_webui(self, args, ollama_client):
        feeds = redis_client.hgetall("rss:feeds")
        if feeds:
            feed_list = "\n".join(f"{feed} (last update: {feeds[feed]})" for feed in feeds)
            final_message = f"Currently watched feeds:\n{feed_list}"
        else:
            final_message = "No RSS feeds are currently being watched."

        return final_message

    # --- IRC Handler ---
    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        feeds = redis_client.hgetall("rss:feeds")
        if feeds:
            lines = [f"{user}: Currently watched feeds:"]
            lines += [f"{feed} (last update: {feeds[feed]})" for feed in feeds]
        else:
            lines = [f"{user}: No RSS feeds are currently being watched."]
        return "\n".join(lines)

# Export the plugin instance.
plugin = ListFeedsPlugin()