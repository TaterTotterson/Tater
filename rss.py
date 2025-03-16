# rss.py
import asyncio
import time
import os
import feedparser
import logging
import redis
import discord
import requests
from dotenv import load_dotenv
import ollama

logger = logging.getLogger("discord.rss")
logger.setLevel(logging.DEBUG)

load_dotenv()
# Redis configuration
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
max_response_length = int(os.getenv("MAX_RESPONSE_LENGTH", 1500))
POLL_INTERVAL = int(os.getenv("RSS_POLL_INTERVAL", 60))  # seconds between polls

redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

#############################
# Crawl4AI-based Extraction
#############################
# Import Crawl4AI components and pydantic for schema
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode, LLMConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from pydantic import BaseModel, Field

# Define a simple Pydantic schema for an article.
class ArticleSchema(BaseModel):
    title: str = Field(..., description="The article title")
    content: str = Field(..., description="The main article content")

#############################
# RSS Manager Class
#############################
class RSSManager:
    def __init__(self, bot: discord.Client, rss_channel_id: int, ollama_client):
        self.bot = bot
        self.rss_channel_id = rss_channel_id
        self.ollama_client = ollama_client  # The passed asynchronous Ollama client
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
            feeds = self.redis.hgetall(self.feeds_key)
            return feeds
        except Exception as e:
            logger.error(f"Error fetching feeds: {e}")
            return {}

    @staticmethod
    async def fetch_web_summary_with_crawl4ai(webpage_url, ollama_client):
        """
        Uses Crawl4AI with an LLM extraction strategy to extract structured article content
        from the given webpage URL. It builds the LLM configuration using the model from
        ollama_client and sets base_url to ollama_client.host.
        Returns a dictionary following ArticleSchema, or None on failure.
        """
        llm_config = LLMConfig(
            provider=f"ollama/{ollama_client.model}",
            api_token="no-token",
            base_url=ollama_client.host
        )
        browser_config = BrowserConfig(verbose=False)
        run_config = CrawlerRunConfig(
            word_count_threshold=100,
            extraction_strategy=LLMExtractionStrategy(
                llm_config=llm_config,
                schema=ArticleSchema.schema(),
                extraction_type="schema",
                instruction="Extract the article title and main content."
            ),
            cache_mode=CacheMode.BYPASS,
        )
        async with AsyncWebCrawler(config=browser_config) as crawler:
            result = await crawler.arun(url=webpage_url, config=run_config)
            return result.extracted_content

    async def process_entry(self, feed_title: str, entry: dict):
        entry_title = entry.get("title", "No Title")
        link = entry.get("link", "")
        logger.info(f"Processing entry: {entry_title} from {feed_title}")
        
        # Use Crawl4AI extraction to get a structured summary.
        structured = await self.fetch_web_summary_with_crawl4ai(link, self.ollama_client)
        if structured and isinstance(structured, dict):
            summary_text = f"Title: {structured.get('title', '')}\nContent: {structured.get('content', '')}"
        else:
            summary_text = "Failed to extract information from the article."
        
        formatted_summary = summary_text.replace("### ", "# ")
        announcement = (
            f"📰 **New article from {feed_title}**\n"
            f"**{entry_title}**\n"
            f"{link}\n\n"
            f"{formatted_summary}"
        )
        chunks = split_message(announcement, chunk_size=max_response_length)
        try:
            channel = self.bot.get_channel(self.rss_channel_id)
            if channel is None:
                logger.error("RSS channel not found.")
                return
            for chunk in chunks:
                await channel.send(chunk)
        except Exception as e:
            logger.error(f"Error sending announcement for article {link}: {e}")

    async def poll_feeds(self):
        logger.info("Starting RSS feed polling...")
        while True:
            feeds = self.get_feeds()  # {feed_url: last_processed_timestamp (as string)}
            for feed_url, last_ts_str in feeds.items():
                try:
                    last_ts = float(last_ts_str) if last_ts_str else 0.0
                    parsed_feed = feedparser.parse(feed_url)
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

def setup_rss_manager(bot: discord.Client, rss_channel_id: int, ollama_client) -> RSSManager:
    rss_manager = RSSManager(bot, rss_channel_id, ollama_client)
    asyncio.create_task(rss_manager.poll_feeds())
    return rss_manager