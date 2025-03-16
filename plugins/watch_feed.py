# plugins/watch_feed.py
import os
import time
import feedparser
import logging
from dotenv import load_dotenv
from plugin_base import ToolPlugin
import streamlit as st
from PIL import Image
from io import BytesIO
import requests
import asyncio
import redis

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Import helper functions from helpers.py.
from helpers import load_image_from_url, send_waiting_message
assistant_avatar = load_image_from_url()  # Uses default avatar URL from helpers.py

# Create a Redis client (adjust DB if needed)
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
    description = "Adds an RSS feed provided by the user to the watch list."
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I add the feed to the watch list. Only generate the message. Do not respond to this message."
    )
    platforms = ["discord", "webui"]

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        # Format waiting prompt with the user's mention.
        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: message.channel.send(text)
        )
        feed_url = args.get("feed_url")
        if feed_url:
            parsed_feed = feedparser.parse(feed_url)
            if parsed_feed.bozo:
                final_message = f"Failed to parse feed: {feed_url}"
            else:
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
                final_message = f"Now watching feed: {feed_url}"
        else:
            final_message = "No feed URL provided for watching."
        await message.channel.send(final_message)
        return ""

    # --- Web UI Handler ---
    async def handle_webui(self, args, ollama_client, context_length):
        waiting_prompt = self.waiting_prompt_template.format(mention="User")
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: st.chat_message("assistant", avatar=assistant_avatar).write(text)
        )
        feed_url = args.get("feed_url")
        if not feed_url:
            return "No feed URL provided for watching."
        parsed_feed = feedparser.parse(feed_url)
        if parsed_feed.bozo:
            return f"Failed to parse feed: {feed_url}"
        else:
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
            return f"Now watching feed: {feed_url}"

# Export the plugin instance.
plugin = WatchFeedPlugin()