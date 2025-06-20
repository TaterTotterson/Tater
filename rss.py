# rss.py
import asyncio
import time
import os
import feedparser
import logging
import redis
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import ollama
from plugin_registry import plugin_registry
from plugin_settings import get_plugin_enabled, get_plugin_settings
from helpers import run_async

logger = logging.getLogger("rss")
logger.setLevel(logging.DEBUG)

# Load settings from environment variables
load_dotenv()
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
max_response_length = int(os.getenv("MAX_RESPONSE_LENGTH", 1500))
POLL_INTERVAL = int(os.getenv("RSS_POLL_INTERVAL", 60))  # seconds between polls

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3:27b").strip()
context_length = int(os.getenv("CONTEXT_LENGTH", 5000))

# Create a Redis client
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

#############################
# Helper Functions
#############################

def fetch_web_summary(webpage_url, model=OLLAMA_MODEL, retries=3, backoff=2):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(webpage_url, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Non-200 status code on attempt {attempt}: {response.status_code}")
                continue
            html = response.text
            soup = BeautifulSoup(html, "html.parser")
            for element in soup(["script", "style", "header", "footer", "nav", "aside"]):
                element.decompose()
            text = soup.get_text(separator="\n")
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            article_text = "\n".join(lines)
            return article_text
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for {webpage_url}: {e}")
            if attempt < retries:
                time.sleep(backoff ** attempt)
    logger.error(f"Error in fetch_web_summary after {retries} attempts: {webpage_url}")
    return None

#############################
# RSS Manager Class
#############################

class RSSManager:
    def __init__(self, ollama_client):
        self.ollama_client = ollama_client
        self.redis = redis_client
        self.feeds_key = "rss:feeds"

    def add_feed(self, feed_url: str) -> bool:
        parsed_feed = feedparser.parse(feed_url)
        if parsed_feed.bozo:
            logger.error(f"Failed to parse feed: {feed_url}")
            return False

        last_ts = 0.0
        if parsed_feed.entries:
            for entry in parsed_feed.entries:
                if 'published_parsed' in entry:
                    entry_ts = time.mktime(entry.published_parsed)
                    if entry_ts > last_ts:
                        last_ts = entry_ts
        else:
            last_ts = time.time()

        try:
            self.redis.hset(self.feeds_key, feed_url, last_ts)
            logger.info(f"Added feed: {feed_url} with last_ts: {last_ts}")
            return True
        except Exception as e:
            logger.error(f"Error adding feed {feed_url}: {e}")
            return False

    def remove_feed(self, feed_url: str) -> bool:
        try:
            removed = self.redis.hdel(self.feeds_key, feed_url)
            if removed:
                logger.info(f"Removed feed: {feed_url}")
                return True
            else:
                logger.info(f"Feed not found: {feed_url}")
                return False
        except Exception as e:
            logger.error(f"Error removing feed {feed_url}: {e}")
            return False

    def get_feeds(self) -> dict:
        try:
            return self.redis.hgetall(self.feeds_key)
        except Exception as e:
            logger.error(f"Error fetching feeds: {e}")
            return {}

    async def process_entry(self, feed_title: str, entry: dict):
        entry_title = entry.get("title", "No Title")
        link = entry.get("link", "")
        logger.info(f"Processing entry: {entry_title} from {feed_title}")
        
        loop = asyncio.get_running_loop()
        article_text = await loop.run_in_executor(None, fetch_web_summary, link, OLLAMA_MODEL)
        
        if not article_text:
            summary_text = "Could not retrieve a summary for this article."
        else:
            summarization_prompt = (
                "Please summarize the following article in a clear and engaging format for Discord. "
                "Include a title and use bullet points for the main takeaways:\n\n"
                f"{article_text}\n\nSummary:"
            )
            try:
                summarization_response = await self.ollama_client.chat(
                    model=OLLAMA_MODEL,
                    messages=[{"role": "system", "content": summarization_prompt}],
                    stream=False,
                    keep_alive=-1,
                    options={"num_ctx": context_length}
                )
                summary_text = summarization_response['message'].get('content', '').strip()
                if not summary_text:
                    summary_text = "Failed to generate a summary from the article."
            except Exception as e:
                logger.error(f"Error summarizing article {link}: {e}")
                summary_text = f"Error summarizing article: {e}"

        announcement = (
            f"📰 **New article from {feed_title}**\n"
            f"**{entry_title}**\n"
            f"{link}\n\n"
            f"{summary_text}"
        )

        for name, plugin in plugin_registry.items():
            if getattr(plugin, "notifier", False) and get_plugin_enabled(name):
                try:
                    await plugin.notify(title=entry_title, content=announcement)
                except Exception as e:
                    logger.warning(f"{name} plugin failed: {e}")

    def any_notifier_enabled(self) -> bool:
        for plugin in plugin_registry.values():
            if getattr(plugin, "notifier", False) and get_plugin_enabled(plugin.name):
                return True
        return False

    async def poll_feeds(self):
        logger.info("Starting RSS feed polling...")
        while True:
            if not self.any_notifier_enabled():
                logger.debug("No notifier plugins are enabled. Skipping RSS check.")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            feeds = self.get_feeds()
            for feed_url, last_ts_str in feeds.items():
                try:
                    last_ts = float(last_ts_str) if last_ts_str else 0.0
                    parsed_feed = await asyncio.to_thread(feedparser.parse, feed_url)
                    if parsed_feed.bozo:
                        logger.error(f"Error parsing feed {feed_url}: {parsed_feed.bozo_exception}")
                        continue
                    feed_title = parsed_feed.feed.get("title", feed_url)
                    new_last_ts = last_ts
                    sorted_entries = sorted(
                        parsed_feed.entries,
                        key=lambda e: time.mktime(e.published_parsed) if 'published_parsed' in e else 0
                    )
                    for entry in sorted_entries:
                        if 'published_parsed' not in entry:
                            continue
                        entry_ts = time.mktime(entry.published_parsed)
                        if entry_ts > last_ts:
                            await self.process_entry(feed_title, entry)
                            if entry_ts > new_last_ts:
                                new_last_ts = entry_ts
                    if new_last_ts > last_ts:
                        self.redis.hset(self.feeds_key, feed_url, new_last_ts)
                except Exception as e:
                    logger.error(f"Error processing feed {feed_url}: {e}")
            await asyncio.sleep(POLL_INTERVAL)
