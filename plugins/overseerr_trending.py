import logging
import re
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv

from helpers import get_tater_name, redis_client
from plugin_base import ToolPlugin

load_dotenv()
logger = logging.getLogger("overseerr_trending")
logger.setLevel(logging.INFO)


class OverseerrTrendingPlugin(ToolPlugin):
    name = "overseerr_trending"
    plugin_name = "Overseerr Trending"
    version = "1.1.0"
    min_tater_version = "50"
    pretty_name = "Overseerr: Trending & Upcoming"
    settings_category = "Overseerr"

    usage = '{"function":"overseerr_trending","arguments":{"kind":"movies|tv","when":"trending|upcoming"}}'

    description = "Lists trending or upcoming movies/TV from Overseerr. Use this ONLY to list titles."
    plugin_dec = "List trending or upcoming movies/TV from Overseerr."
    waiting_prompt_template = (
        "Give {mention} a short, cheerful note that you’re fetching the latest lists from Overseerr now. "
        "Only output that message."
    )
    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit", "telegram"]

    required_settings = {
        "OVERSEERR_BASE_URL": {
            "label": "Overseerr Base URL (e.g., http://overseerr.local:5055)",
            "type": "string",
            "default": "http://localhost:5055",
        },
        "OVERSEERR_API_KEY": {
            "label": "Overseerr API Key",
            "type": "password",
            "default": "",
        },
    }

    @staticmethod
    def _coerce_kind(kind: str) -> str:
        text = str(kind or "").strip().lower()
        if text.startswith("mov") or text.startswith("film"):
            return "movie"
        if "tv" in text or "show" in text or "series" in text:
            return "tv"
        return "movie"

    @staticmethod
    def _coerce_when(when: str) -> str:
        text = str(when or "").strip().lower()
        return "upcoming" if text == "upcoming" else "trending"

    @staticmethod
    def _get_settings() -> Tuple[str, str]:
        settings = redis_client.hgetall("plugin_settings:Overseerr") or {}

        def _val(key: str, default: str = "") -> str:
            value = settings.get(key, default)
            if isinstance(value, (bytes, bytearray)):
                return value.decode("utf-8", "ignore")
            return str(value or "")

        base = (_val("OVERSEERR_BASE_URL", "http://localhost:5055") or "http://localhost:5055").rstrip("/")
        api = _val("OVERSEERR_API_KEY", "")
        return base, api

    @staticmethod
    def _is_tts_platform(platform: str) -> bool:
        return (platform or "").lower() in {"homeassistant", "homekit"}

    def _fetch_list(self, kind: str, when: str) -> Dict[str, Any]:
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}

        norm_kind = self._coerce_kind(kind)
        norm_when = self._coerce_when(when)

        if norm_when == "trending":
            url = f"{base}/api/v1/discover/trending"
        else:
            bucket = "movies" if norm_kind == "movie" else "tv"
            url = f"{base}/api/v1/discover/{bucket}/upcoming"

        headers = {"X-Api-Key": api_key, "Accept": "application/json"}
        try:
            resp = requests.get(url, params={"page": 1}, headers=headers, timeout=12)
            if resp.status_code != 200:
                logger.error("[Overseerr %s] HTTP %s :: %s", norm_when, resp.status_code, resp.text)
                return {"error": f"Overseerr returned HTTP {resp.status_code} for {norm_when}."}
            return resp.json() or {}
        except Exception as exc:
            logger.exception("[Overseerr list fetch error] %s", exc)
            return {"error": f"Failed to reach Overseerr: {exc}"}

    def _extract_titles(self, data: Dict[str, Any], kind: str) -> List[str]:
        results = (data.get("results") or data.get("items") or [])[:25]
        if not results:
            return []

        want = self._coerce_kind(kind)
        filtered = [r for r in results if (r.get("mediaType") or r.get("media_type") or "").lower() == want]
        use = filtered[:10] if filtered else results[:10]

        out: List[str] = []
        seen = set()
        for item in use:
            title = item.get("title") or item.get("name") or ""
            clean = re.sub(r"\s+", " ", str(title)).strip()
            if not clean or clean.lower() in seen:
                continue
            seen.add(clean.lower())
            out.append(clean)
            if len(out) >= 10:
                break
        return out

    @staticmethod
    def _tts_title_list(titles: List[str], max_titles: int = 8) -> str:
        clean = [re.sub(r"\s+", " ", (t or "").strip()) for t in titles if (t or "").strip()]
        clean = clean[:max_titles]
        if not clean:
            return ""
        if len(clean) == 1:
            return f"{clean[0]}."
        head = clean[:-1]
        last = clean[-1]
        parts = [f"{title}." for title in head]
        parts.append(f"and {last}.")
        return " ".join(parts)

    async def _ask_llm_intro_prefix(self, kind: str, when: str, titles: List[str], llm_client) -> str:
        kind_label = "movies" if self._coerce_kind(kind) == "movie" else "TV shows"
        when_norm = self._coerce_when(when)
        if llm_client is None:
            if when_norm == "trending":
                return f"Here are this week’s hottest {kind_label}:"
            return f"Here are some upcoming {kind_label}:"

        first, last = get_tater_name()
        tater = f"{first} {last}"
        sample = ", ".join(titles[:5])

        system = (
            f"You are {tater}. Write one short friendly intro prefix for a list of {when_norm} {kind_label}.\n"
            "Rules:\n"
            "- Output only the prefix text.\n"
            "- Do not include any title names.\n"
            "- End with a colon.\n"
            "- No emojis.\n"
            "- Keep it under 80 characters.\n"
            "- If when is trending, prefer wording like \"Here are this week’s hottest ...:\".\n"
        )
        user = (
            f"Context: user asked for {when_norm} {kind_label}.\n"
            f"Examples (do not include in output): {sample}\n"
            "Write the intro prefix now."
        )

        try:
            resp = await llm_client.chat(
                [{"role": "system", "content": system}, {"role": "user", "content": user}]
            )
            content = ((resp or {}).get("message", {}) or {}).get("content", "") or ""
            out = re.sub(r"\s+", " ", content).strip()
            if out and not out.endswith(":"):
                out = out.rstrip(".") + ":"
            out = out[:80].strip()
            if out:
                return out
        except Exception as exc:
            logger.exception("[Overseerr intro prefix LLM error] %s", exc)

        if when_norm == "trending":
            return f"Here are this week’s hottest {kind_label}:"
        return f"Here are some upcoming {kind_label}:"

    async def _answer(self, args: Dict[str, Any], llm_client, *, platform: str) -> str:
        args = args or {}
        kind = str(args.get("kind") or "movies").strip()
        when = str(args.get("when") or "trending").strip()

        data = self._fetch_list(kind, when)
        if "error" in data:
            return str(data["error"])

        titles = self._extract_titles(data, kind)
        if not titles:
            return f"No {self._coerce_when(when)} results found."

        intro = await self._ask_llm_intro_prefix(kind, when, titles, llm_client)
        if self._is_tts_platform(platform):
            intro_sentence = intro.rstrip(":").strip() + "."
            spoken_titles = self._tts_title_list(titles, max_titles=8)
            return f"{intro_sentence} {spoken_titles}".strip()

        list_block = "\n".join(titles)
        return f"{intro}\n\n{list_block}"

    async def handle_discord(self, message, args, llm_client):
        try:
            answer = await self._answer(args or {}, llm_client, platform="discord")
            return [answer]
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_discord] %s", exc)
            return [f"Error: {exc}"]

    async def handle_webui(self, args, llm_client):
        try:
            answer = await self._answer(args or {}, llm_client, platform="webui")
            return [answer]
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_webui] %s", exc)
            return [f"Error: {exc}"]

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        try:
            answer = await self._answer(args or {}, llm_client, platform="irc")
            return [f"{user}: {answer}"]
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_irc] %s", exc)
            return [f"{user}: Error: {exc}"]

    async def handle_homeassistant(self, args, llm_client):
        try:
            answer = await self._answer(args or {}, llm_client, platform="homeassistant")
            return [answer]
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_homeassistant] %s", exc)
            return ["There was an error fetching results."]

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        try:
            answer = await self._answer(args or {}, llm_client, platform="matrix")
            return [answer]
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_matrix] %s", exc)
            return [f"Error: {exc}"]

    async def handle_telegram(self, update, args, llm_client):
        try:
            answer = await self._answer(args or {}, llm_client, platform="telegram")
            return [answer]
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_telegram] %s", exc)
            return [f"Error: {exc}"]

    async def handle_homekit(self, args, llm_client):
        try:
            answer = await self._answer(args or {}, llm_client, platform="homekit")
            return [answer]
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_homekit] %s", exc)
            return ["There was an error fetching results."]


plugin = OverseerrTrendingPlugin()
