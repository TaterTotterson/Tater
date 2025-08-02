# plugins/premiumize_download.py
import os
import aiohttp
import logging
import asyncio
from urllib.parse import quote
from plugin_base import ToolPlugin
from discord import ui, ButtonStyle
from io import BytesIO
import requests
import streamlit as st
from PIL import Image
from helpers import redis_client, format_irc


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class PremiumizeDownloadPlugin(ToolPlugin):
    name = "premiumize_download"
    usage = (
        "{\n"
        '  "function": "premiumize_download",\n'
        '  "arguments": {"url": "<URL to check>"}\n'
        "}\n"
    )
    description = "Checks if a file link provided by the user is cached on Premiumize.me."
    pretty_name = "Getting Links"
    settings_category = "Premiumize"
    required_settings = {
        "PREMIUMIZE_API_KEY": {
            "label": "Premiumize API Key",
            "type": "password",
            "default": "",
            "description": "Your Premiumize.me API key."
        }
    }
    waiting_prompt_template = "Write a friendly message telling {mention} you’re checking Premiumize and retrieving download links now! Only output that message."
    platforms = ["discord", "webui", "irc"]

    @staticmethod
    async def get_premiumize_download_links(item: str, api_key: str):
        """
        Fetch download links for an item (URL or magnet link) from Premiumize.me.
        Returns a list of file dictionaries if successful; otherwise, returns None.
        """
        api_url = "https://www.premiumize.me/api/transfer/directdl"
        payload = {
            "apikey": api_key,
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
                        return data.get("content", [])
                    else:
                        logger.error(f"Download links error: {data.get('message')}")
                        return None
                else:
                    logger.error(f"Failed to connect to Premiumize.me: {response.status}")
                    return None

    @staticmethod
    def encode_filename(filename: str) -> str:
        return quote(filename)

    @classmethod
    async def _process_download_web(cls, url: str, max_response_length=2000):
        """
        Process a Premiumize download request for the Web UI.
        Returns a text message with download links.
        """
        # Retrieve API key from plugin settings in Redis.
        key = "plugin_settings:Premiumize"
        settings = redis_client.hgetall(key)
        api_key = settings.get("PREMIUMIZE_API_KEY", "")
        if not api_key:
            return "Premiumize API key not configured."
        logger.debug(f"Processing web download for URL: {url}")
        download_links = await cls.get_premiumize_download_links(url, api_key)
        if download_links:
            links_message = f"**Download Links for `{url}`:**\n"
            for file in download_links:
                encoded_filename = cls.encode_filename(file['path'])
                encoded_link = file['link'].replace(file['path'], encoded_filename)
                new_line = f"- [{file['path']}]({encoded_link})\n"
                if len(links_message) + len(new_line) > max_response_length:
                    break
                links_message += new_line
            return links_message
        else:
            return f"The URL `{url}` is not cached on Premiumize.me."

    @classmethod
    async def process_download_discord(cls, channel, url: str, max_response_length=2000):
        """
        Process a Premiumize download request for Discord.
        Sends download links to the provided channel.
        """
        key = "plugin_settings:Premiumize"
        settings = redis_client.hgetall(key)
        api_key = settings.get("PREMIUMIZE_API_KEY", "")
        if not api_key:
            await channel.send("Premiumize API key not configured.")
            return
        logger.debug(f"Processing download for URL: {url}")
        download_links = await cls.get_premiumize_download_links(url, api_key)
        if download_links:
            if len(download_links) > 10:
                view = cls.PaginatedLinks(download_links, f"Download Links for `{url}`")
                await channel.send(content=view.get_page_content(), view=view)
            else:
                links_message = f"**Download Links for `{url}`:**\n"
                for file in download_links:
                    encoded_filename = cls.encode_filename(file['path'])
                    encoded_link = file['link'].replace(file['path'], encoded_filename)
                    new_line = f"- [{file['path']}]({encoded_link})\n"
                    if len(links_message) + len(new_line) > max_response_length:
                        break
                    links_message += new_line
                await channel.send(content=links_message)
        else:
            await channel.send(content=f"The URL `{url}` is not cached on Premiumize.me.")

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
                encoded_filename = PremiumizeDownloadPlugin.encode_filename(file['path'])
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

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client):
        url = args.get("url")
        if not url:
            return f"{message.author.mention}: No URL provided for Premiumize download check."

        try:
            result = await PremiumizeDownloadPlugin.process_download_web(url)
            return result
        except Exception as e:
            return f"{message.author.mention}: Failed to retrieve Premiumize download links: {e}"

    # --- WebUI Handler ---
    async def handle_webui(self, args, ollama_client):
        url = args.get("url")
        if not url:
            return ["No URL provided for Premiumize download check."]

        try:
            asyncio.get_running_loop()
            result = await self._process_download_web(url)
        except RuntimeError:
            result = asyncio.run(self._process_download_web(url))
        except Exception as e:
            return [f"❌ Failed to check download: {e}"]

        return result if isinstance(result, list) else [result]

    # --- IRC Handler ---
    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        url = args.get("url")
        if not url:
            return f"{user}: No URL provided for Premiumize download check."

        try:
            asyncio.get_running_loop()
            result = await self._process_download_web(url)
        except RuntimeError:
            result = asyncio.run(self._process_download_web(url))
        except Exception as e:
            return f"{user}: Error checking download: {e}"

        return format_irc(result)

plugin = PremiumizeDownloadPlugin()