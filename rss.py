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
import plugin_registry as pr
from plugin_settings import get_plugin_enabled

logger = logging.getLogger("rss")
logger.setLevel(logging.DEBUG)

# Load settings from environment variables
load_dotenv()

redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
POLL_INTERVAL = int(os.getenv("RSS_POLL_INTERVAL", 60))  # seconds between polls

# Create a Redis client
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

#############################
# Helper Functions
#############################

MAX_ARTICLE_CHARS = 12000  # keep well under model context; tune per model

MAX_ARTICLE_CHARS = 12000  # keep well under model context; tune per model

def _build_summary_messages(title: str, source_name: str, content: str):
    """
    Build a punchy, newsletter-style brief:
    - Short headline (keep original if good; otherwise improve)
    - One-sentence hook
    - 3–6 crisp bullets (facts, changes, dates, versions)
    - A 'Why it matters' or 'TL;DR' wrap-up
    - Keep links at the end
    """
    safe_content = (content or "")[:MAX_ARTICLE_CHARS]

    system = (
        "You are a witty, conversational news writer who crafts short, engaging summaries for a newsletter audience.\n"
        "Write in a natural, human tone — think punchy intros, short paragraphs, light humor, and clear takeaways.\n\n"
        "Guidelines:\n"
        "- Be concise (about 150–200 words) but write in full sentences and short paragraphs.\n"
        "- Start with a lively hook or observation to draw the reader in.\n"
        "- Explain what happened and why it matters, with 2–4 short paragraphs.\n"
        "- You can use bullet points or short lists *only if they make sense* for clarity or emphasis.\n"
        "- Avoid repeating the title or link — the header and URL are already provided elsewhere.\n"
        "- Keep it conversational, confident, and easy to read — like a quick newsletter blurb, not a report.\n\n"
    )

    user = (
        f"Source: {source_name.strip() if source_name else '(unknown)'}\n"
        f"Original Title: {title.strip() if title else '(untitled)'}\n\n"
        f"Article Content:\n{safe_content}"
    )

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]

def fetch_web_summary(webpage_url, retries=3, backoff=2):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "DNT": "1",
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(webpage_url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning(f"[fetch_web_summary] Non-200 status code on attempt {attempt}: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            for element in soup(["script", "style", "header", "footer", "nav", "aside"]):
                element.decompose()

            container = soup.find("article") or soup.find("main") or soup.body
            if not container:
                return None

            text = container.get_text(separator="\n")
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            article_text = "\n".join(lines)

            if len(article_text.split()) > 3000:
                article_text = " ".join(article_text.split()[:3000])

            logger.info(f"[fetch_web_summary] Extracted {len(article_text)} characters from {webpage_url}")
            return article_text

        except Exception as e:
            logger.warning(f"[fetch_web_summary] Attempt {attempt} failed for {webpage_url}: {e}")
            if attempt < retries:
                time.sleep(backoff ** attempt)

    logger.error(f"[fetch_web_summary] All {retries} attempts failed: {webpage_url}")
    return None

#############################
# RSS Manager Class
#############################

class RSSManager:
    def __init__(self, llm_client):
        self.llm_client = llm_client
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
        link = entry.get("link", "").strip()
        if not link:
            logger.info(f"[RSS] Entry has no link: '{entry_title}' from {feed_title}")
            summary_text = "No article link was provided in this feed item."
        else:
            logger.info(f"Processing entry: {entry_title} from {feed_title}")
            loop = asyncio.get_running_loop()
            # stop passing LLM_MODEL; fetch_web_summary no longer takes it
            article_text = await loop.run_in_executor(None, fetch_web_summary, link)

            if not article_text:
                summary_text = "Could not retrieve a summary for this article."
            else:
                try:
                    messages = _build_summary_messages(entry_title, feed_title, article_text)
                    summarization_response = await self.llm_client.chat(
                        messages=messages,
                        stream=False,
                        # optional: timeout=30, max_tokens=400  # keep it tidy
                    )
                    summary_text = summarization_response['message'].get('content', '').strip() or \
                                   "Failed to generate a summary from the article."
                except Exception as e:
                    logger.error(f"Error summarizing article {link}: {e}")
                    summary_text = f"Error summarizing article."

        announcement = (
            f"📰 **New article from {feed_title}**\n"
            f"**{entry_title}**\n"
            f"{link}\n\n"
            f"{summary_text}"
        )

        plugins = pr.get_registry_snapshot()
        for name, plugin in plugins.items():
            if getattr(plugin, "notifier", False) and get_plugin_enabled(name):
                try:
                    await plugin.notify(title=entry_title, content=announcement)
                except Exception as e:
                    logger.warning(f"{name} plugin failed: {e}")

    def any_notifier_enabled(self) -> bool:
        plugins = pr.get_registry_snapshot()
        visible_plugins = list(plugins.values())
        enabled_notifiers = [
            plugin for plugin in visible_plugins
            if getattr(plugin, "notifier", False) and get_plugin_enabled(plugin.name)
        ]
        logger.debug(
            "[RSS] Number of plugins visible: %s | Number of enabled notifier tools: %s",
            len(plugins),
            len(enabled_notifiers),
        )
        return bool(enabled_notifiers)

    async def poll_feeds(self, stop_event=None):
        logger.info("Starting RSS feed polling...")
        try:
            while not (stop_event and stop_event.is_set()):
                if not self.any_notifier_enabled():
                    logger.debug("No notifier plugins are enabled. Skipping RSS check.")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                feeds = self.get_feeds()
                for feed_url, last_ts_str in feeds.items():
                    if stop_event and stop_event.is_set():
                        break
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
                            if stop_event and stop_event.is_set():
                                break
                            if 'published_parsed' not in entry:
                                continue
                            entry_ts = time.mktime(entry.published_parsed)
                            if entry_ts > last_ts:
                                await self.process_entry(feed_title, entry)
                                if entry_ts > new_last_ts:
                                    new_last_ts = entry_ts
                                    self.redis.hset(self.feeds_key, feed_url, new_last_ts)
                    except Exception as e:
                        logger.error(f"Error processing feed {feed_url}: {e}")

                if stop_event and stop_event.is_set():
                    break
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logger.info("RSS polling task cancelled; exiting cleanly.")
            return
