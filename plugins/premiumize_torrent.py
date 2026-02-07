# plugins/premiumize_torrent.py
import aiohttp
import hashlib
import bencodepy
from urllib.parse import quote
import logging
import base64

# Discord bits are optional; keep imports but scope usage to the Discord handler
import discord
from discord import ui, ButtonStyle

from plugin_base import ToolPlugin
from helpers import get_latest_file_from_history, redis_client
from plugin_diagnostics import combine_diagnosis, diagnose_hash_fields, diagnose_redis_keys, needs_from_diagnosis
from plugin_result import action_failure, action_success

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PremiumizeTorrentPlugin(ToolPlugin):
    name = "premiumize_torrent"
    plugin_name = "Premiumize Torrent"
    version = "1.0.1"
    min_tater_version = "50"
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

    def _diagnosis(self) -> dict:
        hash_diag = diagnose_hash_fields(
            "plugin_settings:Premiumize",
            fields={"premiumize_api_key": "PREMIUMIZE_API_KEY"},
            validators={"premiumize_api_key": lambda v: len(v.strip()) >= 10},
        )
        key_diag = diagnose_redis_keys(
            keys={"premiumize_api_key": "tater:premiumize:api_key"},
            validators={"premiumize_api_key": lambda v: len(v.strip()) >= 10},
        )
        return combine_diagnosis(hash_diag, key_diag)

    def _to_contract(self, message: str, filename: str = "") -> dict:
        msg = (message or "").strip()
        low = msg.lower()
        if not msg:
            return action_failure(
                code="empty_result",
                message="Premiumize torrent lookup returned no output.",
                diagnosis=self._diagnosis(),
                needs=["Please upload a .torrent file and try again."],
                say_hint="Explain that no result was returned and ask for a torrent file.",
            )

        if "please upload" in low or "no data" in low:
            return action_failure(
                code="missing_torrent",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=["Please upload a .torrent file first."],
                say_hint="Ask the user to upload a torrent file.",
            )

        if "api key not configured" in low:
            diagnosis = self._diagnosis()
            needs = needs_from_diagnosis(
                diagnosis,
                {"premiumize_api_key": "Please set your Premiumize API key in plugin settings."},
            )
            return action_failure(
                code="premiumize_config_missing",
                message="Premiumize API key is not configured.",
                diagnosis=diagnosis,
                needs=needs,
                say_hint="Explain missing Premiumize configuration and ask for the API key.",
            )

        if "not cached" in low:
            return action_failure(
                code="not_cached",
                message=msg,
                diagnosis=self._diagnosis(),
                say_hint="Explain the torrent is not cached on Premiumize.",
            )

        if low.startswith("failed") or "error" in low:
            return action_failure(
                code="premiumize_request_failed",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=["Please try the torrent again in a moment."],
                say_hint="Explain that torrent lookup failed and ask whether to retry.",
            )

        return action_success(
            facts={"filename": filename or "torrent", "result": msg},
            say_hint="Return the Premiumize torrent result exactly as provided.",
            suggested_followups=["Want me to check another torrent file?"],
        )

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
    @staticmethod
    def _file_bytes_from_payload(file_data: dict) -> bytes:
        data = file_data.get("data")
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        if data is None:
            data = file_data.get("bytes")
            if isinstance(data, (bytes, bytearray)):
                return bytes(data)
        if isinstance(data, str) and data:
            try:
                return base64.b64decode(data)
            except Exception:
                return b""
        return b""

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
        Pull the latest .torrent from history (Discord channel) and return a plain message.
        """
        file_data = get_latest_file_from_history(channel.id, filetype="file", extensions=[".torrent"])
        if not file_data:
            return "Please upload a `.torrent` file before using this tool."

        filename = file_data.get("name") or "file.torrent"
        torrent_bytes = self._file_bytes_from_payload(file_data)
        if not torrent_bytes:
            return "Could not read the torrent data."

        # Cache check + links
        thash = self.extract_torrent_hash_from_bytes(torrent_bytes)
        if not thash:
            return "Failed to extract torrent hash."

        cached, display_name = await self.check_premiumize_cache(thash)
        title = display_name or filename or thash

        if not cached:
            return f"The torrent `{title}` is not cached on Premiumize.me."

        links = await self.get_premiumize_download_links(self.magnet_from_hash(thash))
        if not links:
            return "Failed to fetch download links."

        return self._links_to_message(title, links, max_len=max_response_length)

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
            text = await self.process_torrent_discord(message.channel)
            return self._to_contract(text)
        except Exception as e:
            logger.exception("[PremiumizeTorrent handle_discord] %s", e)
            return action_failure(
                code="premiumize_exception",
                message=f"Failed to retrieve Premiumize torrent info: {e}",
                diagnosis=self._diagnosis(),
                say_hint="Explain that torrent processing failed and ask whether to retry.",
            )

    async def handle_matrix(self, client, room, sender, body, args, ll_client=None, **kwargs):
        """
        Matrix: read the latest .torrent from this room’s history and return a plain text message.
        The Matrix platform will handle chunking/sending.
        """
        try:
            file_data = get_latest_file_from_history(room.room_id, filetype="file", extensions=[".torrent"])
            if not file_data:
                return self._to_contract("Please upload a `.torrent` file before using this tool.")

            filename = file_data.get("name") or "file.torrent"
            torrent_bytes = self._file_bytes_from_payload(file_data)
            if not torrent_bytes:
                return self._to_contract("Could not read the torrent data.", filename)

            result = await self.process_torrent_bytes(filename, torrent_bytes, max_response_length=3800)
            return self._to_contract(result, filename)
        except Exception as e:
            logger.exception("[PremiumizeTorrent handle_matrix] %s", e)
            return action_failure(
                code="premiumize_exception",
                message="An error occurred while processing the torrent.",
                diagnosis=self._diagnosis(),
                say_hint="Explain that torrent processing failed and ask whether to retry.",
            )

    async def handle_webui(self, args, llm_client):
        return action_failure(
            code="unsupported_platform",
            message="This plugin is supported on Discord and Matrix only.",
            available_on=["discord", "matrix"],
            say_hint="Explain this tool is unavailable on the current platform and list supported platforms.",
        )

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return action_failure(
            code="unsupported_platform",
            message="This plugin is supported on Discord and Matrix only.",
            available_on=["discord", "matrix"],
            say_hint="Explain this tool is unavailable on the current platform and list supported platforms.",
        )

    async def generate_error_message(self, prompt, fallback, message):
        return fallback


plugin = PremiumizeTorrentPlugin()
