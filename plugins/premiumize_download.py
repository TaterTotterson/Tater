# plugins/premiumize_download.py
import os
import aiohttp
import logging
import asyncio
from urllib.parse import quote
from plugin_base import ToolPlugin
from helpers import redis_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PremiumizeDownloadPlugin(ToolPlugin):
    name = "premiumize_download"
    plugin_name = "Premiumize Download"
    usage = (
        "{\n"
        '  "function": "premiumize_download",\n'
        '  "arguments": {"url": "<URL to check>"}\n'
        "}\n"
    )
    description = (
        "Checks if a link (HTTP/HTTPS or magnet) is cached on Premiumize.me and, if so, returns direct download links."
    )
    plugin_dec = "Check if a link is cached on Premiumize and return direct download links."
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
        "Write a friendly message telling {mention} you’re checking Premiumize and retrieving download links now! "
        "Only output that message."
    )
    platforms = ["discord", "webui", "irc", "matrix"]

    # ------------------- API -------------------
    @staticmethod
    async def get_premiumize_download_links(item: str, api_key: str):
        """
        Fetch download links for an item (URL or magnet) from Premiumize.me.
        Returns a list of file dicts on success, else None.
        """
        api_url = "https://www.premiumize.me/api/transfer/directdl"
        payload = {"apikey": api_key, "src": item}
        logger.debug(f"[Premiumize] directdl payload: {payload}")

        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(api_url, data=payload) as resp:
                    logger.debug(f"[Premiumize] status={resp.status}")
                    if resp.status != 200:
                        logger.error(f"[Premiumize] HTTP {resp.status}")
                        return None
                    data = await resp.json()
            except Exception as e:
                logger.error(f"[Premiumize] request error: {e}")
                return None

        if data.get("status") == "success":
            return data.get("content", [])
        logger.error(f"[Premiumize] API error: {data.get('message')}")
        return None

    @staticmethod
    def encode_filename(filename: str) -> str:
        # Encode only the path portion to avoid breaking query/fragment
        return quote(filename)

    # ------------------- Renderers -------------------
    @classmethod
    def _links_to_message(cls, url: str, links: list, max_len: int = 2000) -> str:
        header = f"**Download Links for `{url}`:**\n"
        body = ""
        for f in links:
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

    # ------------------- Core worker -------------------
    @classmethod
    async def _process_download_web(cls, url: str, max_response_length=2000) -> str:
        """
        Process a Premiumize download request and return a markdown string with links.
        """
        settings = redis_client.hgetall("plugin_settings:Premiumize")
        api_key = (settings.get("PREMIUMIZE_API_KEY") or "").strip()
        if not api_key:
            return "Premiumize API key not configured."

        logger.debug(f"[Premiumize] checking: {url}")
        links = await cls.get_premiumize_download_links(url, api_key)
        if links:
            return cls._links_to_message(url, links, max_len=max_response_length)
        return f"The URL `{url}` is not cached on Premiumize.me."

    # ------------------- Platform handlers -------------------
    async def handle_discord(self, message, args, llm_client):
        url = (args or {}).get("url")
        if not url:
            return f"{getattr(message.author, 'mention', '')}: No URL provided for Premiumize download check."
        try:
            result = await self._process_download_web(url)
            return result
        except Exception as e:
            logger.exception("[Premiumize handle_discord] %s", e)
            return f"{getattr(message.author, 'mention', '')}: Failed to retrieve Premiumize download links: {e}"

    async def handle_webui(self, args, llm_client):
        url = (args or {}).get("url")
        if not url:
            return ["No URL provided for Premiumize download check."]
        try:
            asyncio.get_running_loop()
            result = await self._process_download_web(url)
        except RuntimeError:
            result = asyncio.run(self._process_download_web(url))
        except Exception as e:
            logger.exception("[Premiumize handle_webui] %s", e)
            return [f"❌ Failed to check download: {e}"]
        # WebUI expects a list of strings to render
        return [result]

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        url = (args or {}).get("url")
        if not url:
            return f"{user}: No URL provided for Premiumize download check."
        try:
            asyncio.get_running_loop()
            result = await self._process_download_web(url)
        except RuntimeError:
            result = asyncio.run(self._process_download_web(url))
        except Exception as e:
            logger.exception("[Premiumize handle_irc] %s", e)
            return f"{user}: Error checking download: {e}"
        return f"{user}: {result}"

    async def handle_matrix(self, client, room, sender, body, args, ll_client=None, **kwargs):
        """
        Matrix returns a plain string; the platform will handle chunking and sending.
        """
        url = (args or {}).get("url")
        prefix = f"{sender}: " if sender else ""
        if not url:
            return f"{prefix}No URL provided for Premiumize download check."
        try:
            result = await self._process_download_web(url)
            return f"{prefix}{result}"
        except Exception as e:
            logger.exception("[Premiumize handle_matrix] %s", e)
            return f"{prefix}Failed to retrieve Premiumize download links: {e}"


plugin = PremiumizeDownloadPlugin()
