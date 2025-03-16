# plugins/unwatch_feed.py
import os
import logging
import redis
import asyncio
from dotenv import load_dotenv
from plugin_base import ToolPlugin
import streamlit as st
from PIL import Image
from io import BytesIO
import requests

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Import helper functions from helpers.py.
from helpers import load_image_from_url, send_waiting_message
assistant_avatar = load_image_from_url()  # Uses default URL from helpers.py

# Create a Redis client (adjust DB if needed)
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
import redis
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

class UnwatchFeedPlugin(ToolPlugin):
    name = "unwatch_feed"
    usage = (
        "{\n"
        '  "function": "unwatch_feed",\n'
        '  "arguments": {"feed_url": "<RSS feed URL>"}\n'
        "}\n"
    )
    description = "Removes an RSS feed provided by the user from the watch list."
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I remove the feed from the watch list. Only generate the message. Do not respond to this message."
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
            removed = redis_client.hdel("rss:feeds", feed_url)
            if removed:
                final_message = f"Stopped watching feed: {feed_url}"
            else:
                final_message = f"Feed {feed_url} was not found in the watch list."
        else:
            final_message = "No feed URL provided for unwatching."
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
            return "No feed URL provided for unwatching."
        removed = redis_client.hdel("rss:feeds", feed_url)
        if removed:
            return f"Stopped watching feed: {feed_url}"
        else:
            return f"Feed {feed_url} was not found in the watch list."

    async def generate_error_message(self, prompt, fallback, message):
        return fallback

# Export the plugin instance.
plugin = UnwatchFeedPlugin()