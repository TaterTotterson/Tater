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

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2").strip()
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

# Create a Redis client
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

def load_image_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content))

assistant_avatar = load_image_from_url("https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater.png")

class ListFeedsPlugin(ToolPlugin):
    name = "list_feeds"
    usage = (
        "{\n"
        '  "function": "list_feeds",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = "Lists all rss feeds currently on the watch list."
    platforms = ["discord", "webui"]

    async def handle_webui(self, args, ollama_client, context_length):
        # Send a waiting message to the user in the web UI.
        waiting_prompt = (
            "Generate a brief message to User telling them to wait a moment while you grab the current watched feeds for them. Only generate the message. Do not respond to this message."
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
            st.chat_message("assistant", avatar=assistant_avatar).write("Please wait a moment while I grab the current watched feeds for them...")
        feeds = redis_client.hgetall("rss:feeds")
        if feeds:
            feed_list = "\n".join(f"{feed} (last update: {feeds[feed]})" for feed in feeds)
            return f"Currently watched feeds:\n{feed_list}"
        else:
            return "No RSS feeds are currently being watched."

    async def handle_discord(self, message, args, ollama, context_length, max_response_length):
        waiting_prompt = (
            f"Generate a brief message to {message.author.mention} telling them to wait a moment while you grab the current watched feeds for them. Only generate the message. Do not respond to this message."
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
            await message.channel.send("Please wait a moment while I list the RSS feeds...")
        feeds = redis_client.hgetall("rss:feeds")
        if feeds:
            feed_list = "\n".join(f"{feed} (last update: {feeds[feed]})" for feed in feeds)
            final_message = f"Currently watched feeds:\n{feed_list}"
        else:
            final_message = "No RSS feeds are currently being watched."
        await message.channel.send(final_message)
        return ""

# Export the plugin instance.
plugin = ListFeedsPlugin()