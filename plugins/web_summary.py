# plugins/web_summary.py
import os
import requests
import re
import asyncio
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse
import streamlit as st
from plugin_base import ToolPlugin
from helpers import load_image_from_url, send_waiting_message

load_dotenv()
assistant_avatar = load_image_from_url()  # Uses default avatar URL from helpers.py

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
    platforms = ["discord", "webui"]

    @staticmethod
    def extract_article_text(webpage_url):
        """
        Extract the main textual content from a webpage URL using requests and BeautifulSoup.
        """
        headers = {'User-Agent': 'Mozilla/5.0 (TaterBot Article Summarizer)'}
        try:
            response = requests.get(webpage_url, headers=headers, timeout=10)
            if response.status_code != 200:
                return None
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
                article_text = " ".join(article_text.split()[:3000])  # Limit to ~3000 words
            return article_text

        except Exception as e:
            print(f"Error extracting article: {e}")
            return None

    @staticmethod
    def format_summary_for_discord(summary):
        return summary.replace("### ", "# ")

    @staticmethod
    def split_message(message_content, chunk_size=1500):
        message_parts = []
        while len(message_content) > chunk_size:
            split_point = message_content.rfind('\n', 0, chunk_size)
            if split_point == -1:
                split_point = message_content.rfind(' ', 0, chunk_size)
            if split_point == -1:
                split_point = chunk_size
            message_parts.append(message_content[:split_point])
            message_content = message_content[split_point:].strip()
        message_parts.append(message_content)
        return message_parts

    async def async_fetch_web_summary(self, webpage_url, ollama_client):
        article_text = self.extract_article_text(webpage_url)
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
            model=ollama_client.model,
            messages=[{"role": "user", "content": prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": ollama_client.context_length}
        )
        return response['message'].get('content', '')

    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        webpage_url = args.get("url")
        if not webpage_url:
            return "No webpage URL provided."

        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: asyncio.create_task(self.safe_send(message.channel, text))
        )

        summary = await self.async_fetch_web_summary(webpage_url, ollama_client)
        if not summary:
            return "Failed to retrieve summary from the webpage."

        formatted_summary = self.format_summary_for_discord(summary)
        for chunk in self.split_message(formatted_summary, max_response_length):
            await self.safe_send(message.channel, chunk)
        return ""

    async def handle_webui(self, args, ollama_client, context_length):
        webpage_url = args.get("url")
        if not webpage_url:
            return "No webpage URL provided."

        waiting_prompt = self.waiting_prompt_template.format(mention="User")
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: st.chat_message("assistant", avatar=assistant_avatar).write(text)
        )

        summary = await self.async_fetch_web_summary(webpage_url, ollama_client)
        if not summary:
            return "Failed to retrieve summary from the webpage."

        formatted_summary = self.format_summary_for_discord(summary)
        return "\n".join(self.split_message(formatted_summary))

    async def safe_send(self, channel, content):
        if len(content) <= 2000:
            await channel.send(content)
        else:
            for chunk in self.split_message(content, 1900):
                await channel.send(chunk)

plugin = WebSummaryPlugin()