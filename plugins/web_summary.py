# plugins/web_summary.py
import os
import requests
import asyncio
import logging
import streamlit as st
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from plugin_base import ToolPlugin
from helpers import format_irc

load_dotenv()
logger = logging.getLogger("web_summary")
logger.setLevel(logging.INFO)

class WebSummaryPlugin(ToolPlugin):
    name = "web_summary"
    usage = (
        "{\n"
        '  "function": "web_summary",\n'
        '  "arguments": {"url": "<Webpage URL>"}\n'
        "}\n"
    )
    description = "Summarizes an article from a URL provided by the user."
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I read this boring article and summarize it. Only generate the message. Do not respond to this message."
    )
    platforms = ["discord", "webui", "irc"]

    @staticmethod
    def fetch_web_summary(webpage_url, model):
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

    async def web_summary(self, url, ollama_client):
        article_text = self.fetch_web_summary(url, ollama_client.model)
        if not article_text:
            return None

        prompt = (
            "Summarize the following article.\n\n"
            "Return the result as:\n"
            "- A short title\n"
            "- 4–8 bullet points covering key takeaways\n\n"
            f"Article:\n{article_text}"
        )

        response = await ollama_client.chat(
            messages=[{"role": "user", "content": prompt}]
        )
        return response["message"].get("content", "")

    async def handle_discord(self, message, args, ollama_client):
        url = args.get("url")
        if not url:
            return "No webpage URL provided."

        summary = await self.web_summary(url, ollama_client)
        if not summary:
            return "Failed to summarize the article."

        return "\n".join(self.split_message(summary, max_response_length))

    async def handle_webui(self, args, ollama_client):
        url = args.get("url")
        if not url:
            return "No webpage URL provided."

        summary = await self.web_summary(url, ollama_client)
        if not summary:
            msg = "Failed to summarize the article."
            return msg

        return "\n".join(self.split_message(summary))

    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        url = args.get("url")
        if not url:
            return f"{user}: No URL provided."

        summary = await self.web_summary(url, ollama_client)
        if not summary:
            return f"{user}: Failed to summarize article."

        formatted = format_irc(summary)
        return "\n".join(self.split_message(formatted, 400))

plugin = WebSummaryPlugin()