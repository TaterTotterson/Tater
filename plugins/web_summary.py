# plugins/web_summary.py
import os
import requests
import re
import asyncio
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse
import ollama
from plugin_base import ToolPlugin
import streamlit as st
from PIL import Image
from io import BytesIO

load_dotenv()
# We now load the assistant avatar using our helper (or define it here).
from helpers import load_image_from_url, send_waiting_message
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

    # --- Helper Functions as Static Methods ---
    @staticmethod
    def extract_article_text(webpage_url):
        """
        Extract the main textual content from a webpage URL using requests and BeautifulSoup.
        """
        try:
            response = requests.get(webpage_url, timeout=10)
            if response.status_code != 200:
                return None
            html = response.text
            soup = BeautifulSoup(html, "html.parser")
            # Remove unwanted elements.
            for element in soup(["script", "style", "header", "footer", "nav", "aside"]):
                element.decompose()
            text = soup.get_text(separator="\n")
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            article_text = "\n".join(lines)
            return article_text
        except Exception as e:
            print(f"Error extracting article: {e}")
            return None

    @staticmethod
    def format_summary_for_discord(summary):
        """
        Optionally format the summary text for Discord.
        """
        return summary.replace("### ", "# ")

    @staticmethod
    def split_message(message_content, chunk_size=1500):
        """
        Split a long message into chunks so it can be sent as multiple messages.
        """
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
        """
        Asynchronously extract article text from a webpage and summarize it using the asynchronous Ollama client.
        """
        article_text = self.extract_article_text(webpage_url)
        if not article_text:
            return None
        prompt = (
            f"Please summarize the following article. Give it a title and use bullet points when necessary:\n\n{article_text}"
        )
        response = await ollama_client.chat(
            model=ollama_client.model,
            messages=[{"role": "user", "content": prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": ollama_client.context_length}
        )
        summary = response['message'].get('content', '')
        return summary

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        webpage_url = args.get("url")
        if not webpage_url:
            return "No webpage URL provided."
        
        # Format waiting prompt with user's mention.
        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: message.channel.send(text)
        )
        
        # Fetch the summary asynchronously.
        summary = await self.async_fetch_web_summary(webpage_url, ollama_client)
        if not summary:
            return "Failed to retrieve summary from the webpage."
        formatted_summary = self.format_summary_for_discord(summary)
        message_chunks = self.split_message(formatted_summary, chunk_size=max_response_length)
        for chunk in message_chunks:
            await message.channel.send(chunk)
        return ""

    # --- Web UI Handler ---
    async def handle_webui(self, args, ollama_client, context_length):
        webpage_url = args.get("url")
        if not webpage_url:
            return "No webpage URL provided."
        
        # Format waiting prompt with a generic mention.
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
        chunks = self.split_message(formatted_summary)
        final_response = "\n".join(chunks)
        return final_response

# Export an instance of the plugin.
plugin = WebSummaryPlugin()