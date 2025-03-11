# plugins/unwatch_feed.py
import os
import logging
from dotenv import load_dotenv
import redis
from plugin_base import ToolPlugin
from PIL import Image
from io import BytesIO
import requests
import streamlit as st

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2").strip()
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

# Create a Redis client (adjust DB if needed)
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

def load_image_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content))

assistant_avatar = load_image_from_url("https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater.png")

class UnwatchFeedPlugin(ToolPlugin):
    name = "unwatch_feed"
    usage = (
        "{\n"
        '  "function": "unwatch_feed",\n'
        '  "arguments": {"feed_url": "<RSS feed URL>"}\n'
        "}\n"
    )
    description = "Removes a rss feed provided buy the user from the watch list."
    platforms = ["discord", "webui"]

    async def handle_webui(self, args, ollama_client, context_length):
        # Send a waiting message to the user in the web UI.
        waiting_prompt = (
            "Generate a brief message to User telling them to wait a moment while you remove the feed from the watchlist for them. Only generate the message. Do not respond to this message."
        )
        waiting_response = await ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "system", "content": waiting_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        waiting_text = waiting_response['message'].get('content', '').strip()
        if waiting_text:
            st.chat_message("assistant", avatar=assistant_avatar).write(waiting_text)
        else:
            st.chat_message("assistant", avatar=assistant_avatar).write("Please wait a moment while I remove the feed from the watchlist...")

        feed_url = args.get("feed_url")
        if not feed_url:
            return "No feed URL provided for unwatching."
        removed = redis_client.hdel("rss:feeds", feed_url)
        if removed:
            return f"Stopped watching feed: {feed_url}"
        else:
            return f"Feed {feed_url} was not found in the watch list."

    async def handle_discord(self, message, args, ollama, context_length, max_response_length):
        waiting_prompt = (
            f"Generate a brief message to {message.author.mention} telling them to wait a moment while I remove the RSS feed from the watch list. Only generate the message. Do not respond to this message."
        )
        waiting_response = await ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "system", "content": waiting_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        waiting_text = waiting_response['message'].get('content', '').strip()
        if waiting_text:
            await message.channel.send(waiting_text)
        else:
            await message.channel.send("Please wait a moment while I remove the RSS feed...")
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

# Export the plugin instance.
plugin = UnwatchFeedPlugin()
