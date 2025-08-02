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
from helpers import format_irc

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

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
    description = "Adds an RSS feed provided by the user to the rss watch list."
    pretty_name = "Adding Your Feed"
    waiting_prompt_template = "Write a friendly message telling {mention} you’re adding the feed to the watch list now! Only output that message."
    platforms = ["discord", "webui", "irc"]

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client):
        feed_url = args.get("feed_url")
        if not feed_url:
            return "No feed URL provided for watching."

        parsed_feed = feedparser.parse(feed_url)
        if parsed_feed.bozo:
            return f"Failed to parse feed: {feed_url}"

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


    # --- Web UI Handler ---
    async def handle_webui(self, args, ollama_client):
        def inner():
            feed_url = args.get("feed_url")
            if not feed_url:
                return ["No feed URL provided for watching."]

            parsed_feed = feedparser.parse(feed_url)
            if parsed_feed.bozo:
                return [f"Failed to parse feed: {feed_url}"]

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
            return [f"Now watching feed: {feed_url}"]

        try:
            loop = asyncio.get_running_loop()
            return await asyncio.to_thread(inner)
        except RuntimeError:
            return asyncio.run(asyncio.to_thread(inner))


    # --- IRC Handler ---
    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        feed_url = args.get("feed_url")
        if not feed_url:
            return f"{user}: No feed URL provided for watching."

        parsed_feed = feedparser.parse(feed_url)
        if parsed_feed.bozo:
            return f"{user}: Failed to parse feed: {feed_url}"

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
        return f"{user}: Now watching feed: {feed_url}"

# Export the plugin instance.
plugin = WatchFeedPlugin()