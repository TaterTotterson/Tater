# plugins/premiumize_torrent.py
import os
import aiohttp
import hashlib
import bencodepy
from urllib.parse import quote
import logging
import asyncio
import base64

# Discord bits are optional; keep imports but scope usage to the Discord handler
import discord
from discord import ui, ButtonStyle

from plugin_base import ToolPlugin
from helpers import get_latest_file_from_history, redis_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PremiumizeTorrentPlugin(ToolPlugin):
    name = "premiumize_torrent"
    plugin_name = "Premiumize Torrent"
    usage = (
        "{\n"
        '  "function": "premiumize_torrent",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = "Checks if a .torrent file in recent chat history is cached on Premiumize.me and returns direct links if available."
    plugin_dec = "Check the latest torrent or magnet against Premiumize cache and provide direct links."
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
    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re checking Premiumize for that torrent and retrieving download links now! "
        "Only output that message."
    )
    platforms = ["discord", "matrix"]

    # ------------------- API helpers -------------------
    @staticmethod
    def _get_api_key() -> str:
        settings = redis_client.hgetall("plugin_settings:Premiumize")
        return (settings.get("PREMIUMIZE_API_KEY") or "").strip()

    @classmethod
    async def check_premiumize_cache(cls, item: str):
        """
        Check if an item (torrent hash or URL) is cached on Premiumize.me.
        Returns (True, display_name) if cached; else (False, None).
        """
        api_key = cls._get_api_key()
        if not api_key:
            logger.error("Premiumize API key not configured.")
            return False, None

        api_url = "https://www.premiumize.me/api/cache/check"
        params = {"apikey": api_key, "items[]": item}
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.get(api_url, params=params) as resp:
                    if resp.status != 200:
                        logger.error(f"[Premiumize cache/check] HTTP {resp.status}")
                        return False, None
                    data = await resp.json()
            except Exception as e:
                logger.error(f"[Premiumize cache/check] request error: {e}")
                return False, None

        if data.get("status") == "success" and data.get("response") and data["response"][0]:
            # Premiumize may provide filename array
            filename = (data.get("filename") or [None])[0] if isinstance(data.get("filename"), list) else None
            return True, filename
        return False, None

    @classmethod
    async def get_premiumize_download_links(cls, src: str):
        """
        Fetch direct download links for a given source (magnet or URL).
        Returns list of file dicts or None.
        """
        api_key = cls._get_api_key()
        if not api_key:
            logger.error("Premiumize API key not configured.")
            return None

        api_url = "https://www.premiumize.me/api/transfer/directdl"
        payload = {"apikey": api_key, "src": src}
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(api_url, data=payload) as resp:
                    if resp.status != 200:
                        logger.error(f"[Premiumize directdl] HTTP {resp.status}")
                        return None
                    data = await resp.json()
            except Exception as e:
                logger.error(f"[Premiumize directdl] request error: {e}")
                return None

        if data.get("status") == "success":
            return data.get("content", [])
        logger.error(f"[Premiumize directdl] API error: {data}")
        return None

    # ------------------- Torrent helpers -------------------
    @staticmethod
    def extract_torrent_hash_from_bytes(torrent_bytes: bytes) -> str | None:
        """
        Extract the torrent SHA-1 hash (uppercase hex) from raw .torrent bytes.
        """
        try:
            decoded = bencodepy.decode(torrent_bytes)
            info_dict = decoded[b"info"]
            encoded_info = bencodepy.encode(info_dict)
            return hashlib.sha1(encoded_info).hexdigest().upper()
        except Exception as e:
            logger.error(f"Failed to extract torrent hash: {e}")
            return None

    @staticmethod
    def magnet_from_hash(torrent_hash: str) -> str:
        return f"magnet:?xt=urn:btih:{torrent_hash}"

    @staticmethod
    def encode_filename(filename: str) -> str:
        return quote(filename)

    @classmethod
    def _links_to_message(cls, title: str, links: list, max_len: int = 2000) -> str:
        header = f"**Download Links for `{title}`:**\n"
        body = ""
        for f in links or []:
            path = f.get("path", "")
            link = f.get("link", "")
            if not path or not link:
                continue
            encoded_path = cls.encode_filename(path)
            safe_link = link.replace(path, encoded_path)
            line = f"- [{path}]({safe_link})\n"
            if len(header) + len(body) + len(line) > max_len:
                break
            body += line
        return header + (body or "_No files returned by Premiumize._")

    # ------------------- Core worker (bytes-based) -------------------
    @classmethod
    async def process_torrent_bytes(cls, filename: str, torrent_bytes: bytes, max_response_length=2000) -> str:
        """
        Given a torrent filename and bytes, check Premiumize cache and return a message with links or status.
        """
        if not torrent_bytes:
            return "No data found in the provided .torrent."

        thash = cls.extract_torrent_hash_from_bytes(torrent_bytes)
        if not thash:
            return "Failed to extract torrent hash."

        cached, display_name = await cls.check_premiumize_cache(thash)
        title = display_name or filename or thash

        if not cached:
            return f"The torrent `{title}` is not cached on Premiumize.me."

        magnet = cls.magnet_from_hash(thash)
        links = await cls.get_premiumize_download_links(magnet)
        if not links:
            return "Failed to fetch download links."

        return cls._links_to_message(title, links, max_len=max_response_length)

    # ------------------- Discord flow -------------------
    async def process_torrent_discord(self, channel: discord.TextChannel, max_response_length=2000):
        """
        Pull the latest .torrent from history (Discord channel), post links with pagination if needed.
        """
        file_data = get_latest_file_from_history(channel.id, filetype="file", extensions=[".torrent"])
        if not file_data:
            await channel.send(content="Please upload a `.torrent` file before using this tool.")
            return ""

        filename = file_data.get("name") or "file.torrent"
        try:
            torrent_bytes = base64.b64decode(file_data.get("data") or "")
        except Exception:
            await channel.send(content="Could not read the torrent data.")
            return ""

        # Cache check + links
        thash = self.extract_torrent_hash_from_bytes(torrent_bytes)
        if not thash:
            await channel.send(content="Failed to extract torrent hash.")
            return ""

        cached, display_name = await self.check_premiumize_cache(thash)
        title = display_name or filename or thash

        if not cached:
            await channel.send(content=f"The torrent `{title}` is not cached on Premiumize.me.")
            return ""

        links = await self.get_premiumize_download_links(self.magnet_from_hash(thash))
        if not links:
            await channel.send(content="Failed to fetch download links.")
            return ""

        if len(links) > 10:
            view = self.PaginatedLinks(links, f"Download Links for `{title}`")
            await channel.send(content=view.get_page_content(), view=view)
        else:
            await channel.send(content=self._links_to_message(title, links, max_len=max_response_length))
        return ""

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
            header = f"**{self.title} (Page {self.current_page + 1}):**\n"
            body = ""
            for f in page_links:
                encoded_filename = PremiumizeTorrentPlugin.encode_filename(f.get("path", ""))
                encoded_link = (f.get("link", "") or "").replace(f.get("path", "") or "", encoded_filename)
                line = f"- [{f.get('path','')}]({encoded_link})\n"
                if len(header) + len(body) + len(line) > 2000:
                    break
                body += line
            return header + body

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

    # ------------------- Platform handlers -------------------
    async def handle_discord(self, message, args, llm_client):
        try:
            return await self.process_torrent_discord(message.channel)
        except Exception as e:
            logger.exception("[PremiumizeTorrent handle_discord] %s", e)
            return f"{getattr(message.author, 'mention', '')}: Failed to retrieve Premiumize torrent info: {e}"

    async def handle_matrix(self, client, room, sender, body, args, ll_client=None, **kwargs):
        """
        Matrix: read the latest .torrent from this room’s history and return a plain text message.
        The Matrix platform will handle chunking/sending.
        """
        prefix = f"{sender}: " if sender else ""
        try:
            file_data = get_latest_file_from_history(room.room_id, filetype="file", extensions=[".torrent"])
            if not file_data:
                return f"{prefix}Please upload a `.torrent` file before using this tool."

            filename = file_data.get("name") or "file.torrent"
            try:
                torrent_bytes = base64.b64decode(file_data.get("data") or "")
            except Exception:
                return f"{prefix}Could not read the torrent data."

            result = await self.process_torrent_bytes(filename, torrent_bytes, max_response_length=3800)
            return f"{prefix}{result}"
        except Exception as e:
            logger.exception("[PremiumizeTorrent handle_matrix] %s", e)
            return f"{prefix}An error occurred while processing the torrent."

    async def handle_webui(self, args, llm_client):
        return "❌ This plugin is supported on Discord and Matrix only."

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return f"{user}: ❌ This plugin is supported on Discord and Matrix only."

    async def generate_error_message(self, prompt, fallback, message):
        return fallback


plugin = PremiumizeTorrentPlugin()
