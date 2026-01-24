# plugins/overseerr_trending.py
import asyncio
import logging
import requests
import re
from dotenv import load_dotenv
from plugin_base import ToolPlugin
from helpers import redis_client, get_tater_name

load_dotenv()
logger = logging.getLogger("overseerr_trending")
logger.setLevel(logging.INFO)


class OverseerrTrendingPlugin(ToolPlugin):
    name = "overseerr_trending"
    plugin_name = "Overseerr Trending"
    pretty_name = "Overseerr: Trending & Upcoming"
    settings_category = "Overseerr"

    usage = (
        "{\n"
        '  "function": "overseerr_trending",\n'
        '  "arguments": {\n'
        '    "kind": "movies|tv",\n'
        '    "when": "trending|upcoming"\n'
        "  }\n"
        "}\n"
    )

    description = "Lists trending or upcoming movies/TV from Overseerr. Use this ONLY to list titles."
    plugin_dec = "List trending or upcoming movies/TV from Overseerr."
    waiting_prompt_template = (
        "Give {mention} a short, cheerful note that you’re fetching the latest lists from Overseerr now. "
        "Only output that message."
    )
    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit"]

    required_settings = {
        "OVERSEERR_BASE_URL": {
            "label": "Overseerr Base URL (e.g., http://overseerr.local:5055)",
            "type": "string",
            "default": "http://localhost:5055",
        },
        "OVERSEERR_API_KEY": {
            "label": "Overseerr API Key",
            "type": "string",
            "default": "",
        },
    }

    # ---------- Internals ----------
    @staticmethod
    def _get_settings():
        s = redis_client.hgetall("plugin_settings:Overseerr") or {}

        def _val(k, default=""):
            v = s.get(k, default)
            return v.decode() if isinstance(v, (bytes, bytearray)) else v

        base = (_val("OVERSEERR_BASE_URL", "http://localhost:5055") or "http://localhost:5055").rstrip("/")
        api = _val("OVERSEERR_API_KEY", "")
        return base, api

    @staticmethod
    def _is_tts_platform(platform: str) -> bool:
        return (platform or "").lower() in {"homeassistant", "homekit"}

    def _fetch_list(self, kind: str, when: str):
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}

        kind = (kind or "movies").lower().strip()
        when = (when or "trending").lower().strip()
        norm_kind = "movie" if kind.startswith("mov") else "tv"

        if when == "trending":
            url = f"{base}/api/v1/discover/trending"
        else:
            url = f"{base}/api/v1/discover/{'movies' if norm_kind == 'movie' else 'tv'}/upcoming"

        headers = {"X-Api-Key": api_key, "Accept": "application/json"}

        try:
            resp = requests.get(url, params={"page": 1}, headers=headers, timeout=12)
            if resp.status_code != 200:
                logger.error(f"[Overseerr {when}] HTTP {resp.status_code} :: {resp.text}")
                return {"error": f"Overseerr returned HTTP {resp.status_code} for {when}."}
            return resp.json() or {}
        except Exception as e:
            logger.exception("[Overseerr list fetch error] %s", e)
            return {"error": f"Failed to reach Overseerr: {e}"}

    def _extract_titles(self, data: dict, kind: str) -> list[str]:
        """
        Extract up to 10 titles, preferring requested kind if trending mixes.
        """
        results = (data.get("results") or data.get("items") or [])[:10]
        if not results:
            return []

        want = "movie" if (kind or "").lower().startswith("mov") else "tv"
        filtered = [r for r in results if (r.get("mediaType") or r.get("media_type") or "").lower() == want]
        use = filtered[:10] if filtered else results

        titles = []
        for r in use[:10]:
            title = r.get("title") or r.get("name") or "Unknown"
            title = re.sub(r"\s+", " ", str(title)).strip()
            if title:
                titles.append(title)
        return titles

    @staticmethod
    def _tts_title_list(titles: list[str], max_titles: int = 8) -> str:
        """
        Format for TTS: Title. Title. Title. and Title.
        Forces pauses in Home Assistant / Siri.
        """
        clean = [re.sub(r"\s+", " ", (t or "").strip()) for t in titles if (t or "").strip()]
        clean = clean[:max_titles]
        if not clean:
            return ""
        if len(clean) == 1:
            return f"{clean[0]}."

        head = clean[:-1]
        last = clean[-1]
        parts = [f"{t}." for t in head]
        parts.append(f"and {last}.")
        return " ".join(parts)

    async def _ask_llm_intro_prefix(self, kind: str, when: str, titles: list[str], llm_client) -> str:
        """
        Generate ONLY the intro prefix ending with ':'.
        Prefer "this week’s hottest ..." when trending.
        DO NOT include titles (we append them ourselves).
        """
        first, last = get_tater_name()
        tater = f"{first} {last}"

        kind_label = "movies" if (kind or "").lower().startswith("mov") else "TV shows"
        when_norm = (when or "").lower().strip()
        when_label = "trending" if when_norm == "trending" else "upcoming"
        sample = ", ".join(titles[:5])

        sys = (
            f"You are {tater}. Write ONE short friendly intro prefix for a list of {when_label} {kind_label}.\n"
            "Rules:\n"
            "- Output ONLY the prefix text.\n"
            "- Do NOT include any movie/show titles.\n"
            "- End with a colon.\n"
            "- No emojis.\n"
            "- Keep it under 80 characters.\n"
            "- If when is 'trending', prefer wording like \"Here are this week’s hottest ...:\".\n"
        )
        user = (
            f"Context: user asked for {when_label} {kind_label}.\n"
            f"Examples (do NOT include in output): {sample}\n"
            "Write the intro prefix now."
        )

        try:
            resp = await llm_client.chat([{"role": "system", "content": sys}, {"role": "user", "content": user}])
            content = ((resp or {}).get("message", {}) or {}).get("content", "") or ""
            out = re.sub(r"\s+", " ", content).strip()
            if out and not out.endswith(":"):
                out = out.rstrip(".") + ":"
            out = out[:80].strip()
            if out:
                return out
            if when_norm == "trending":
                return f"Here are this week’s hottest {kind_label}:"
            return f"Here are some upcoming {kind_label}:"
        except Exception as e:
            logger.exception("[Overseerr intro prefix LLM error] %s", e)
            if (when or "").lower().strip() == "trending":
                kind_label = "movies" if (kind or "").lower().startswith("mov") else "TV shows"
                return f"Here are this week’s hottest {kind_label}:"
            kind_label = "movies" if (kind or "").lower().startswith("mov") else "TV shows"
            return f"Here are some upcoming {kind_label}:"

    async def _ask_llm_followup(self, kind: str, when: str, llm_client) -> str:
        """
        Short friendly follow-up without instructions.
        """
        first, last = get_tater_name()
        tater = f"{first} {last}"

        kind_label = "movies" if (kind or "").lower().startswith("mov") else "TV shows"
        when_label = "trending" if (when or "").lower().strip() == "trending" else "upcoming"

        sys = (
            f"You are {tater}. Write ONE short friendly follow-up question.\n"
            f"Context: you just listed {when_label} {kind_label}.\n"
            "Rules:\n"
            "- Do NOT mention numbers, ordering, or how to respond.\n"
            "- Do NOT include emojis.\n"
            "- Keep it under 110 characters.\n"
            "- End with a question mark.\n"
        )
        user = "Write the follow-up question now."

        try:
            resp = await llm_client.chat([{"role": "system", "content": sys}, {"role": "user", "content": user}])
            content = ((resp or {}).get("message", {}) or {}).get("content", "") or ""
            out = re.sub(r"\s+", " ", content).strip()
            if out and not out.endswith("?"):
                out = out.rstrip(".") + "?"
            return out[:110] if out else "Which one of these are you thinking of watching first?"
        except Exception as e:
            logger.exception("[Overseerr follow-up LLM error] %s", e)
            return "Which one of these are you thinking of watching first?"

    # ---------- Core ----------
    async def _answer(self, args, llm_client, *, platform: str):
        kind = (args.get("kind") or "movies").strip()
        when = (args.get("when") or "trending").strip()

        data = self._fetch_list(kind, when)
        if "error" in data:
            return data["error"]

        titles = self._extract_titles(data, kind)
        if not titles:
            return f"No {when} results found."

        intro_prefix = await self._ask_llm_intro_prefix(kind, when, titles, llm_client)
        followup = await self._ask_llm_followup(kind, when, llm_client) if len(titles) > 2 else ""

        # ---------- TTS PLATFORMS ----------
        if self._is_tts_platform(platform):
            intro_sentence = intro_prefix.rstrip(":").strip() + "."
            spoken_titles = self._tts_title_list(titles, max_titles=8)
            if followup:
                return f"{intro_sentence} {spoken_titles} {followup}"
            return f"{intro_sentence} {spoken_titles}".strip()

        # ---------- NON-TTS PLATFORMS ----------
        # IMPORTANT: Markdown hard line breaks for Streamlit/WebUI:
        # add two spaces at end of each line so newlines render as line breaks.
        list_block = "\n".join([f"{t}  " for t in titles])

        if followup:
            return f"{intro_prefix}\n\n{list_block}\n\n{followup}"

        return f"{intro_prefix}\n\n{list_block}"

    # ---------- Platform handlers ----------
    async def handle_discord(self, message, args, llm_client):
        try:
            answer = await self._answer(args, llm_client, platform="discord")
            return await self.safe_send(message.channel, answer)
        except Exception as e:
            logger.exception("[OverseerrTrending handle_discord] %s", e)
            return await self.safe_send(message.channel, f"Error: {e}")

    async def handle_webui(self, args, llm_client):
        async def inner():
            return await self._answer(args, llm_client, platform="webui")

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        try:
            answer = await self._answer(args, llm_client, platform="irc")
            return f"{user}: {answer}"
        except Exception as e:
            logger.exception("[OverseerrTrending handle_irc] %s", e)
            return f"{user}: Error: {e}"

    async def handle_homeassistant(self, args, llm_client):
        try:
            return await self._answer(args, llm_client, platform="homeassistant")
        except Exception as e:
            logger.exception("[OverseerrTrending handle_homeassistant] %s", e)
            return "There was an error fetching results."

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        try:
            return await self._answer(args or {}, llm_client, platform="matrix")
        except Exception as e:
            logger.exception("[OverseerrTrending handle_matrix] %s", e)
            return f"Error: {e}"

    async def handle_homekit(self, args, llm_client):
        try:
            return await self._answer(args or {}, llm_client, platform="homekit")
        except Exception as e:
            logger.exception("[OverseerrTrending handle_homekit] %s", e)
            return "There was an error fetching results."

    # ---------- Utilities ----------
    def split_message(self, text, chunk_size=1500):
        chunks = []
        while len(text) > chunk_size:
            split = text.rfind("\n", 0, chunk_size) or text.rfind(" ", 0, chunk_size) or chunk_size
            chunks.append(text[:split])
            text = text[split:].strip()
        chunks.append(text)
        return chunks

    async def safe_send(self, channel, content):
        if len(content) <= 2000:
            await channel.send(content)
        else:
            for chunk in self.split_message(content, 1900):
                await channel.send(chunk)


plugin = OverseerrTrendingPlugin()