# plugins/premiumize_download.py
import aiohttp
import logging
import asyncio
from urllib.parse import quote
from plugin_base import ToolPlugin
from helpers import redis_client
from plugin_diagnostics import combine_diagnosis, diagnose_hash_fields, diagnose_redis_keys, needs_from_diagnosis
from plugin_result import action_failure, action_success

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PremiumizeDownloadPlugin(ToolPlugin):
    name = "premiumize_download"
    plugin_name = "Premiumize Download"
    version = "1.0.1"
    min_tater_version = "50"
    usage = '{"function":"premiumize_download","arguments":{"url":"<URL to check>"}}'
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
    platforms = ["discord", "webui", "irc", "matrix", "telegram"]

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

    def _to_contract(self, result_text: str, url: str) -> dict:
        msg = (result_text or "").strip()
        low = msg.lower()
        if not msg:
            return action_failure(
                code="empty_result",
                message="Premiumize did not return a response.",
                diagnosis=self._diagnosis(),
                needs=["Which URL should I check in Premiumize?"],
                say_hint="Explain that no output was returned and ask for the URL.",
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
                say_hint="Explain that Premiumize API settings are missing and ask for the API key.",
            )

        if "not cached" in low:
            return action_failure(
                code="not_cached",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=[],
                say_hint="Explain the URL is not currently cached on Premiumize.",
            )

        if low.startswith("failed") or "error" in low:
            return action_failure(
                code="premiumize_request_failed",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=["Please verify the URL and try again."],
                say_hint="Explain that Premiumize lookup failed and ask whether to retry.",
            )

        return action_success(
            facts={"url": url, "result": msg},
            say_hint="Return the download result message exactly as provided without inventing details.",
            suggested_followups=["Want me to check another URL?"],
        )

    # ------------------- Platform handlers -------------------
    async def handle_discord(self, message, args, llm_client):
        url = (args or {}).get("url")
        if not url:
            return action_failure(
                code="missing_url",
                message="No URL provided for Premiumize download check.",
                needs=["What URL should I check in Premiumize?"],
                say_hint="Ask the user for the URL to check.",
            )
        try:
            result = await self._process_download_web(url)
            return self._to_contract(result, url)
        except Exception as e:
            logger.exception("[Premiumize handle_discord] %s", e)
            return action_failure(
                code="premiumize_exception",
                message=f"Failed to retrieve Premiumize download links: {e}",
                diagnosis=self._diagnosis(),
                say_hint="Explain that Premiumize lookup failed and ask whether to retry.",
            )

    async def handle_webui(self, args, llm_client):
        url = (args or {}).get("url")
        if not url:
            return action_failure(
                code="missing_url",
                message="No URL provided for Premiumize download check.",
                needs=["What URL should I check in Premiumize?"],
                say_hint="Ask for the URL.",
            )
        try:
            asyncio.get_running_loop()
            result = await self._process_download_web(url)
        except RuntimeError:
            result = asyncio.run(self._process_download_web(url))
        except Exception as e:
            logger.exception("[Premiumize handle_webui] %s", e)
            return action_failure(
                code="premiumize_exception",
                message=f"Failed to check download: {e}",
                diagnosis=self._diagnosis(),
                say_hint="Explain that the Premiumize check failed and ask if the user wants to retry.",
            )
        return self._to_contract(result, url)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        url = (args or {}).get("url")
        if not url:
            return action_failure(
                code="missing_url",
                message="No URL provided for Premiumize download check.",
                needs=["What URL should I check in Premiumize?"],
                say_hint="Ask for the URL to check.",
            )
        try:
            asyncio.get_running_loop()
            result = await self._process_download_web(url)
        except RuntimeError:
            result = asyncio.run(self._process_download_web(url))
        except Exception as e:
            logger.exception("[Premiumize handle_irc] %s", e)
            return action_failure(
                code="premiumize_exception",
                message=f"Error checking download: {e}",
                diagnosis=self._diagnosis(),
                say_hint="Explain that Premiumize lookup failed and ask whether to retry.",
            )
        return self._to_contract(result, url)

    async def handle_matrix(self, client, room, sender, body, args, ll_client=None, **kwargs):
        """
        Matrix returns a plain string; the platform will handle chunking and sending.
        """
        url = (args or {}).get("url")
        if not url:
            return action_failure(
                code="missing_url",
                message="No URL provided for Premiumize download check.",
                needs=["What URL should I check in Premiumize?"],
                say_hint="Ask for the URL.",
            )
        try:
            result = await self._process_download_web(url)
            return self._to_contract(result, url)
        except Exception as e:
            logger.exception("[Premiumize handle_matrix] %s", e)
            return action_failure(
                code="premiumize_exception",
                message=f"Failed to retrieve Premiumize download links: {e}",
                diagnosis=self._diagnosis(),
                say_hint="Explain that Premiumize lookup failed and ask whether to retry.",
            )

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args, llm_client)


plugin = PremiumizeDownloadPlugin()
