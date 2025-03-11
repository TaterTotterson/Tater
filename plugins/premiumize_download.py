# plugins/premiumize_download.py
import os
import aiohttp
import logging
from urllib.parse import quote
import asyncio
from dotenv import load_dotenv
from plugin_base import ToolPlugin
from discord import ui, ButtonStyle
from io import BytesIO
import requests
import streamlit as st
from PIL import Image

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2").strip()
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

PREMIUMIZE_API_KEY = os.getenv("PREMIUMIZE_API_KEY")

def load_image_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content))

assistant_avatar = load_image_from_url("https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater.png")

async def get_premiumize_download_links(item: str):
    """
    Fetch download links for an item (URL or magnet link) from Premiumize.me.
    Returns a list of file dictionaries if successful; otherwise, returns None.
    """
    api_url = "https://www.premiumize.me/api/transfer/directdl"
    payload = {
        "apikey": PREMIUMIZE_API_KEY,
        "src": item
    }
    logger.debug(f"Fetching download links for item: {item} with payload: {payload}")
    async with aiohttp.ClientSession() as session:
        async with session.post(api_url, data=payload) as response:
            logger.debug(f"Download links response status: {response.status}")
            if response.status == 200:
                data = await response.json()
                logger.debug(f"Download links response: {data}")
                if data.get("status") == "success":
                    links = data.get("content", [])
                    return links
                else:
                    logger.error(f"Download links error: {data.get('message')}")
                    return None
            else:
                logger.error(f"Failed to connect to Premiumize.me: {response.status}")
                return None

def encode_filename(filename: str) -> str:
    return quote(filename)

async def process_download_web(url: str, max_response_length=2000) -> str:
    """
    Process a Premiumize download request for the web UI.
    Returns a text message with download links.
    """
    logger.debug(f"Processing web download for URL: {url}")
    download_links = await get_premiumize_download_links(url)
    if download_links:
        links_message = f"**Download Links for `{url}`:**\n"
        for file in download_links:
            encoded_filename = encode_filename(file['path'])
            encoded_link = file['link'].replace(file['path'], encoded_filename)
            new_line = f"- [{file['path']}]({encoded_link})\n"
            if len(links_message) + len(new_line) > max_response_length:
                break
            links_message += new_line
        return links_message
    else:
        return f"The URL `{url}` is not cached on Premiumize.me."

async def process_download_discord(channel, url: str, max_response_length=2000):
    """
    Process a Premiumize download request for Discord.
    Sends download links to the provided channel.
    """
    logger.debug(f"Processing download for URL: {url}")
    download_links = await get_premiumize_download_links(url)
    if download_links:
        if len(download_links) > 10:
            from discord import ui, ButtonStyle
            view = PaginatedLinks(download_links, f"Download Links for `{url}`")
            await channel.send(content=view.get_page_content(), view=view)
        else:
            links_message = f"**Download Links for `{url}`:**\n"
            for file in download_links:
                encoded_filename = encode_filename(file['path'])
                encoded_link = file['link'].replace(file['path'], encoded_filename)
                new_line = f"- [{file['path']}]({encoded_link})\n"
                if len(links_message) + len(new_line) > max_response_length:
                    break
                links_message += new_line
            await channel.send(content=links_message)
    else:
        await channel.send(content=f"The URL `{url}` is not cached on Premiumize.me.")

# Simple paginated view for Discord (if many links)
class PaginatedLinks(ui.View):
    def __init__(self, links, title, page_size=10):
        super().__init__()
        self.links = links
        self.title = title
        self.page_size = page_size
        self.current_page = 0
        self.update_buttons()
    def get_page_content(self):
        start = self.current_page * self.page_size
        end = start + self.page_size
        page_links = self.links[start:end]
        links_message = f"**{self.title} (Page {self.current_page + 1}):**\n"
        for file in page_links:
            encoded_filename = encode_filename(file['path'])
            encoded_link = file['link'].replace(file['path'], encoded_filename)
            new_line = f"- [{file['path']}]({encoded_link})\n"
            if len(links_message) + len(new_line) > 2000:
                break
            links_message += new_line
        return links_message
    def update_buttons(self):
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = (self.current_page + 1) * self.page_size >= len(self.links)
    @ui.button(label="Previous", style=ButtonStyle.grey)
    async def previous_button(self, interaction, button):
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            await interaction.response.edit_message(content=self.get_page_content(), view=self)
    @ui.button(label="Next", style=ButtonStyle.grey)
    async def next_button(self, interaction, button):
        if (self.current_page + 1) * self.page_size < len(self.links):
            self.current_page += 1
            self.update_buttons()
            await interaction.response.edit_message(content=self.get_page_content(), view=self)

class PremiumizeDownloadPlugin(ToolPlugin):
    name = "premiumize_download"
    usage = (
        "For Premiumize URL download check:\n"
        "{\n"
        '  "function": "premiumize_download",\n'
        '  "arguments": {"url": "<URL to check>"}\n'
        "}\n"
    )
    description = "Checks if a file link provided by the user is cached on premiumize.me."
    platforms = ["discord", "webui"]

    async def handle_webui(self, args, ollama_client, context_length):
        # Send a waiting message to the user in the web UI.
        waiting_prompt = (
            "Generate a brief message to User telling them to wait a moment while you remove the feed from the watchlist for them. Only generate the message. Do not respond to this message."
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
            st.chat_message("assistant", avatar=assistant_avatar).write("Please wait a moment while I remove the feed from the watchlist...")

        url = args.get("url")
        if not url:
            return "No URL provided for Premiumize download check."
        result = await process_download_web(url)
        return result

    async def handle_discord(self, message, args, ollama, context_length, max_response_length):
        url = args.get("url")
        if url:
            waiting_prompt = (
                f"Generate a brief message to {message.author.mention} telling them to wait a moment while I check Premiumize for that URL and retrieve download links for them. Only generate the message. Do not respond to this message."
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
                await message.channel.send("Hold on while I check Premiumize for that URL...")
            async with message.channel.typing():
                try:
                    await process_download_discord(message.channel, url, max_response_length)
                    return ""
                except Exception as e:
                    prompt = f"Generate an error message to {message.author.mention} explaining that I was unable to retrieve the Premiumize download links for the URL. Only generate the message. Do not respond to this message."
                    error_msg = await self.generate_error_message(prompt, f"Failed to retrieve Premiumize download links: {e}", message)
                    return error_msg
        else:
            prompt = f"Generate an error message to {message.author.mention} explaining that no URL was provided for Premiumize download check. Only generate the message. Do not respond to this message."
            error_msg = await self.generate_error_message(prompt, "No URL provided for Premiumize download check.", message)
            return error_msg

    async def generate_error_message(self, prompt, fallback, message):
        return fallback

# Export an instance of the plugin.
plugin = PremiumizeDownloadPlugin()