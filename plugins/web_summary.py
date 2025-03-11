# plugins/web_summary.py
import os
import requests
import re
import asyncio
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse
import ollama  # Using the synchronous Ollama client
from plugin_base import ToolPlugin
import streamlit as st
from PIL import Image
from io import BytesIO

# Load environment variables
load_dotenv()
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2").strip()
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

# Load the assistant avatar from URL using requests and Pillow.
def load_image_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content))

assistant_avatar = load_image_from_url("https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater.png")

class WebSummaryPlugin(ToolPlugin):
    name = "web_summary"
    usage = (
        "{\n"
        '  "function": "web_summary",\n'
        '  "arguments": {"url": "<Webpage URL>"}\n'
        "}\n"
    )
    description = "Summarizes a artichle from a URL provided by the user."
    platforms = ["discord", "webui"]

    async def handle_discord(self, message, args, ollama, context_length, max_response_length):
        webpage_url = args.get("url")
        if not webpage_url:
            return "No webpage URL provided."

        waiting_prompt = (
            f"Generate a brief message to {message.author.mention} telling them to wait a moment while you read this boring article for them and summarize it. Only generate the message. Do not respond to this message."
        )
        waiting_response = await ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "system", "content": waiting_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        waiting_text = waiting_response['message'].get('content', '')
        if waiting_text:
            await message.channel.send(waiting_text)
        else:
            await message.channel.send("Please wait a moment while I summarize the article...")

        # Use asyncio.to_thread to run the synchronous fetch_web_summary function.
        summary = await asyncio.to_thread(fetch_web_summary, webpage_url, OLLAMA_MODEL)
        if not summary:
            return "Failed to retrieve summary from the webpage."
        formatted_summary = format_summary_for_discord(summary)
        message_chunks = split_message(formatted_summary, chunk_size=max_response_length)
        for chunk in message_chunks:
            await message.channel.send(chunk)
        return ""  # No additional text is returned after sending the summary

    async def handle_webui(self, args, ollama_client, context_length):
        webpage_url = args.get("url")
        if not webpage_url:
            return "No webpage URL provided."

        # Send a waiting message to the user in the web UI.
        waiting_prompt = (
            "Generate a brief message to User telling them to wait a moment while you read this boring article for them and summarize it. Only generate the message. Do not respond to this message."
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
            st.chat_message("assistant", avatar=assistant_avatar).write("Please wait a moment while I summarize the article...")

        summary = await asyncio.to_thread(fetch_web_summary, webpage_url, OLLAMA_MODEL)
        if not summary:
            return "Failed to retrieve summary from the webpage."
        formatted_summary = format_summary_for_discord(summary)
        chunks = split_message(formatted_summary)
        final_response = "\n".join(chunks)
        return final_response

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
        #for element in soup(["script", "style", "header", "footer", "nav", "aside"]):
        #    element.decompose()
        text = soup.get_text(separator="\n")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        article_text = "\n".join(lines)
        return article_text
    except Exception as e:
        print(f"Error extracting article: {e}")
        return None

def fetch_web_summary(webpage_url, model=OLLAMA_MODEL):
    """
    Extract article text from a webpage and summarize it using the Ollama model.
    This function is synchronous.
    """
    article_text = extract_article_text(webpage_url)
    if not article_text:
        return None
    prompt = f"Please summarize the following article. Give it a title and use bullet points when necessary:\n\n{article_text}"
    try:
        client = ollama.Client(host=OLLAMA_URL)
        response = client.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        summary = response['message'].get('content', '')
        return summary
    except Exception as e:
        print(f"Error fetching web summary: {e}")
        return None

def format_summary_for_discord(summary):
    """
    Optionally format the summary text for Discord.
    """
    return summary.replace("### ", "# ")

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

# Export an instance of the plugin.
plugin = WebSummaryPlugin()