# plugins/overseerr_trending.py
import json
import logging
import requests
from datetime import datetime
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

    description = (
        "Lists trending or upcoming movies/TV from Overseerr. "
        "Use this ONLY to list titles."
    )
    plugin_dec = "List trending or upcoming movies/TV from Overseerr."
    waiting_prompt_template = "Give {mention} a short, cheerful note that you’re fetching the latest lists from Overseerr now. Only output that message."
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
    def _year(d: str | None) -> str:
        if not d:
            return ""
        try:
            return datetime.strptime(d, "%Y-%m-%d").strftime("%Y")
        except Exception:
            return ""

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

    def _format_list(self, data: dict, kind: str, when: str) -> str:
        results = (data.get("results") or data.get("items") or [])[:10]
        if not results:
            return f"No {when} results found."

        # Prefer requested kind if trending mixes
        want = "movie" if (kind or "").lower().startswith("mov") else "tv"
        filtered = [r for r in results if (r.get("mediaType") or r.get("media_type") or "").lower() == want]
        use = filtered[:10] if filtered else results

        lines = []
        for r in use[:10]:
            title = r.get("title") or r.get("name") or "Unknown"
            date = r.get("releaseDate") or r.get("firstAirDate")
            y = self._year(date)
            lines.append(f"- {title}{f' ({y})' if y else ''}")

        # gentle prompt; no numbering
        if len(lines) > 3:
            lines.append("")
            lines.append("Want details on one? Say the title (or ‘number 3’).")

        return "\n".join(lines)

    # ---------- Core ----------
    async def _answer(self, args):
        kind = (args.get("kind") or "movies").strip()
        when = (args.get("when") or "trending").strip()

        data = self._fetch_list(kind, when)
        if "error" in data:
            return data["error"]

        return self._format_list(data, kind, when)

    # ---------- Platform handlers ----------
    async def handle_discord(self, message, args, llm_client):
        answer = await self._answer(args)
        return await self.safe_send(message.channel, answer)

    async def handle_webui(self, args, llm_client):
        return await self._answer(args)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        answer = await self._answer(args)
        return f"{user}: {answer}"

    async def handle_homeassistant(self, args, llm_client):
        answer = await self._answer(args)
        # TTS-friendly
        return " ".join([ln.strip("- ").strip() for ln in answer.splitlines() if ln.strip()])[:500]

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        return await self._answer(args or {})

    async def handle_homekit(self, args, llm_client):
        answer = await self._answer(args or {})
        # Keep short for Siri
        items = [ln.strip("- ").strip() for ln in answer.splitlines() if ln.startswith("- ")]
        return ", ".join(items[:6])[:500]

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