# plugins/web_summary.py
import os
import requests
import asyncio
import logging
import streamlit as st
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from plugin_base import ToolPlugin

load_dotenv()
logger = logging.getLogger("web_summary")
logger.setLevel(logging.INFO)

class WebSummaryPlugin(ToolPlugin):
    name = "web_summary"
    plugin_name = "Web Summary"
    usage = (
        "{\n"
        '  "function": "web_summary",\n'
        '  "arguments": {"url": "<Webpage URL>"}\n'
        "}\n"
    )
    description = "Summarizes an article from a URL provided by the user."
    plugin_dec = "Summarize the main points of a webpage from its URL."
    pretty_name = "Summarizing Your Article"
    waiting_prompt_template = "Write a casual, friendly message telling {mention} you’re reading the article and preparing a summary now! Only output that message."
    platforms = ["discord", "webui", "irc", "matrix"]

    @staticmethod
    def fetch_web_summary(webpage_url, model=None):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }

        try:
            resp = requests.get(webpage_url, headers=headers, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Request failed: {resp.status_code} - {webpage_url}")
                return None

            soup = BeautifulSoup(resp.text, "html.parser")
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
            logger.error(f"[fetch_web_summary error] {e}")
            return None

    @staticmethod
    def split_message(text, chunk_size=1500):
        parts = []
        while len(text) > chunk_size:
            split = text.rfind("\n", 0, chunk_size)
            if split == -1:
                split = text.rfind(" ", 0, chunk_size)
            if split == -1:
                split = chunk_size
            parts.append(text[:split])
            text = text[split:].strip()
        parts.append(text)
        return parts

    async def safe_send(self, channel, content):
        if len(content) <= 2000:
            await channel.send(content)
        else:
            for chunk in self.split_message(content, 1900):
                await channel.send(chunk)

    async def _web_summary(self, url, llm_client):
        article_text = self.fetch_web_summary(url, getattr(llm_client, "model", None))
        if not article_text:
            return None

        prompt = (
            "Summarize the following article.\n\n"
            "Return the result as:\n"
            "- A short title\n"
            "- 4–8 bullet points covering key takeaways\n\n"
            f"Article:\n{article_text}"
        )

        response = await llm_client.chat(
            messages=[{"role": "user", "content": prompt}]
        )
        return response["message"].get("content", "")

    # --- Discord ---
    async def handle_discord(self, message, args, llm_client):
        url = args.get("url")
        if not url:
            return "No webpage URL provided."

        try:
            return await self._web_summary(url, llm_client)
        except Exception as e:
            return f"Failed to summarize the article: {e}"

    # --- WebUI ---
    async def handle_webui(self, args, llm_client):
        url = args.get("url")
        if not url:
            return ["No webpage URL provided."]

        try:
            loop = asyncio.get_running_loop()
            return await self._web_summary(url, llm_client)
        except RuntimeError:
            return asyncio.run(self._web_summary(url, llm_client))
        except Exception as e:
            return [f"Failed to summarize the article: {e}"]

    # --- IRC ---
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        url = args.get("url")
        if not url:
            return f"{user}: No URL provided."

        summary = await self._web_summary(url, llm_client)
        if not summary:
            return f"{user}: Failed to summarize article."

        return f"{user}: {summary}"

    # --- Matrix ---
    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        llm = llm_client or kwargs.get("llm")
        url = (args or {}).get("url")
        if not url:
            return "No webpage URL provided."

        try:
            result = await self._web_summary(url, llm)
            return result or "Failed to summarize the article."
        except Exception as e:
            return f"Failed to summarize the article: {e}"

plugin = WebSummaryPlugin()
