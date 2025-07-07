# plugins/premiumize_torrent.py
import os
import aiohttp
import hashlib
import bencodepy
from urllib.parse import quote
import discord
from discord import ui, ButtonStyle
import logging
import asyncio
from plugin_base import ToolPlugin
import requests
import streamlit as st
from io import BytesIO
from helpers import send_waiting_message

# No need to call load_dotenv here since settings are handled via the WebUI.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Import helper functions and shared redis_client from helpers.py.
from helpers import redis_client

class PremiumizeTorrentPlugin(ToolPlugin):
    name = "premiumize_torrent"
    usage = (
        "{\n"
        '  "function": "premiumize_torrent",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = "Checks if a torrent file provided by the user is cached on Premiumize.me."
    settings_category = "Premiumize"
    required_settings = {
        "PREMIUMIZE_API_KEY": {
            "label": "Premiumize API Key",
            "type": "password",
            "default": "",
            "description": "Your Premiumize.me API key."
        }
    }
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I check Premiumize for that torrent and retrieve download links. Only generate the message. Do not respond to this message."
    )
    platforms = ["discord"]

    @staticmethod
    async def check_premiumize_cache(item: str):
        """
        Check if an item (a URL or torrent hash) is cached on Premiumize.me.
        Returns a tuple: (True, filename) if cached; otherwise, (False, None).
        Retrieves the API key from Redis under "plugin_settings:Premiumize".
        """
        key = "plugin_settings:Premiumize"
        settings = redis_client.hgetall(key)
        api_key = settings.get("PREMIUMIZE_API_KEY", "")
        if not api_key:
            logger.error("Premiumize API key not configured.")
            return False, None

        api_url = "https://www.premiumize.me/api/cache/check"
        params = {"apikey": api_key, "items[]": item}
        logger.debug(f"Checking cache for item: {item} with params: {params}")
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, params=params) as response:
                logger.debug(f"Cache check response status: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Cache check response: {data}")
                    if data.get("status") == "success" and data.get("response") and data["response"][0]:
                        filename = data.get("filename", [None])[0] if data.get("filename") else None
                        return True, filename
                    return False, None
                else:
                    logger.error(f"Cache check failed: {response.status}")
                    return False, None

    @staticmethod
    def extract_torrent_hash(file_path: str) -> str:
        """
        Extract the torrent hash (SHA-1 in uppercase hex) from a torrent file.
        """
        try:
            with open(file_path, "rb") as f:
                torrent_data = f.read()
            decoded_data = bencodepy.decode(torrent_data)
            info_dict = decoded_data[b'info']
            encoded_info = bencodepy.encode(info_dict)
            return hashlib.sha1(encoded_info).hexdigest().upper()
        except Exception as e:
            logger.error(f"Failed to extract torrent hash: {e}")
            return None

    @staticmethod
    def create_magnet_link(torrent_hash: str) -> str:
        return f"magnet:?xt=urn:btih:{torrent_hash}"

    @staticmethod
    def encode_filename(filename: str) -> str:
        return quote(filename)

    @staticmethod
    async def get_premiumize_download_links(item: str):
        """
        Fetch download links for an item (URL or magnet link) from Premiumize.me.
        Retrieves the API key from Redis under "plugin_settings:Premiumize".
        """
        key = "plugin_settings:Premiumize"
        settings = redis_client.hgetall(key)
        api_key = settings.get("PREMIUMIZE_API_KEY", "")
        if not api_key:
            logger.error("Premiumize API key not configured.")
            return None

        api_url = "https://www.premiumize.me/api/transfer/directdl"
        payload = {"apikey": api_key, "src": item}
        async with aiohttp.ClientSession() as session:
            async with session.post(api_url, data=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("status") == "success":
                        return data.get("content", [])
                return None

    async def process_torrent(self, channel: discord.TextChannel, torrent_attachment: discord.Attachment, max_response_length=2000):
        file_path = f"./{torrent_attachment.filename}"
        await torrent_attachment.save(file_path)
        try:
            torrent_hash = self.extract_torrent_hash(file_path)
            if not torrent_hash:
                await channel.send(content="Failed to extract torrent hash.")
                return ""
            cached, filename = await self.check_premiumize_cache(torrent_hash)
            if cached:
                magnet_link = self.create_magnet_link(torrent_hash)
                download_links = await self.get_premiumize_download_links(magnet_link)
                if download_links:
                    if len(download_links) > 10:
                        view = self.PaginatedLinks(download_links, f"Download Links for `{filename}`")
                        await channel.send(content=view.get_page_content(), view=view)
                        return f"[Premiumize] Download links sent for `{filename}`."
                    else:
                        links_message = f"**Download Links for `{filename}`:**\n"
                        for file in download_links:
                            encoded_filename = self.encode_filename(file['path'])
                            encoded_link = file['link'].replace(file['path'], encoded_filename)
                            new_line = f"- [{file['path']}]({encoded_link})\n"
                            if len(links_message) + len(new_line) > max_response_length:
                                break
                            links_message += new_line
                        await channel.send(content=links_message)
                else:
                    await channel.send(content="Failed to fetch download links.")
            else:
                await channel.send(content=f"The torrent `{torrent_attachment.filename}` is not cached on Premiumize.me.")
        except Exception as e:
            logger.error(f"Error processing torrent: {e}")
            await channel.send(content="An error occurred while processing the torrent file.")
        finally:
            try:
                import os
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception:
                pass
        return ""

    async def process_torrent_web(self, file_content: bytes, filename: str, max_response_length=2000):
        file_path = f"./{filename}"
        try:
            with open(file_path, "wb") as f:
                f.write(file_content)
            torrent_hash = self.extract_torrent_hash(file_path)
            if not torrent_hash:
                return "Failed to extract torrent hash."
            cached, cached_filename = await self.check_premiumize_cache(torrent_hash)
            if cached:
                magnet_link = self.create_magnet_link(torrent_hash)
                download_links = await self.get_premiumize_download_links(magnet_link)
                if download_links:
                    links_message = f"**Download Links for `{cached_filename}`:**\n"
                    for file in download_links:
                        encoded_filename = self.encode_filename(file['path'])
                        encoded_link = file['link'].replace(file['path'], encoded_filename)
                        new_line = f"- [{file['path']}]({encoded_link})\n"
                        if len(links_message) + len(new_line) > max_response_length:
                            break
                        links_message += new_line
                    return links_message
                return "Failed to fetch download links."
            return f"The torrent `{filename}` is not cached on Premiumize.me."
        except Exception as e:
            logger.error(f"Error processing torrent: {e}")
            return "An error occurred while processing the torrent file."
        finally:
            try:
                import os
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception:
                pass

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
                encoded_filename = PremiumizeTorrentPlugin.encode_filename(file['path'])
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
        async def previous_button(self, interaction: discord.Interaction, button: ui.Button):
            if self.current_page > 0:
                self.current_page -= 1
                self.update_buttons()
                await interaction.response.edit_message(content=self.get_page_content(), view=self)

        @ui.button(label="Next", style=ButtonStyle.grey)
        async def next_button(self, interaction: discord.Interaction, button: ui.Button):
            if (self.current_page + 1) * self.page_size < len(self.links):
                self.current_page += 1
                self.update_buttons()
                await interaction.response.edit_message(content=self.get_page_content(), view=self)

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        if not message.attachments:
            return f"{message.author.mention}: No torrent file attached for Premiumize torrent check."

        try:
            result = await self.process_torrent(message.channel, message.attachments[0], max_response_length)
            return result
        except Exception as e:
            return f"{message.author.mention}: Failed to retrieve Premiumize torrent info: {e}"

    # --- WebUI Handler ---
    async def handle_webui(self, args, ollama_client, context_length):
        return "❌ This plugin is only supported on Discord."

    # --- IRC Handler ---
    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        return f"{user}: ❌ This plugin is only supported on Discord."

    async def generate_error_message(self, prompt, fallback, message):
        return fallback

plugin = PremiumizeTorrentPlugin()