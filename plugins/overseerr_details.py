# plugins/overseerr_details.py
import json
import asyncio
import logging
import requests
import re

from dotenv import load_dotenv
from plugin_base import ToolPlugin
from helpers import redis_client, get_tater_name

load_dotenv()
logger = logging.getLogger("overseerr_details")
logger.setLevel(logging.INFO)


class OverseerrDetailsPlugin(ToolPlugin):
    name = "overseerr_details"
    plugin_name = "Overseerr Details"
    version = "1.1.0"
    min_tater_version = "50"
    pretty_name = "Overseerr: Title Details"
    settings_category = "Overseerr"

    usage = (
        "{\n"
        '  "function": "overseerr_details",\n'
        '  "arguments": {\n'
        '    "title": "<movie or show title>",\n'
        '    "media_type": "movie|tv (optional)"\n'
        "  }\n"
        "}\n"
    )

    description = (
        "Get details for ONE movie or TV show from Overseerr. "
        "Use when the user asks for more info about a specific title. "
    )
    plugin_dec = "Fetch details for a specific title from Overseerr."
    waiting_prompt_template = (
        "Give {mention} a short, cheerful note that you’re fetching details from Overseerr now. "
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

    def _search(self, title: str):
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}

        url = f"{base}/api/v1/search"
        headers = {"X-Api-Key": api_key, "Accept": "application/json"}

        raw_title = (title or "").strip()

        try:
            resp = requests.get(url, params={"query": raw_title, "page": 1}, headers=headers, timeout=12)
            if resp.status_code != 200:
                logger.error(f"[Overseerr search] HTTP {resp.status_code} :: {resp.text}")
                return {"error": f"Overseerr returned HTTP {resp.status_code} for search."}
            return resp.json() or {}
        except Exception as e:
            logger.exception("[Overseerr search error] %s", e)
            return {"error": f"Failed to search Overseerr: {e}"}

    def _fetch_details(self, media_type: str, tmdb_id: int):
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}

        media_type = (media_type or "").lower().strip()
        endpoint = "movie" if media_type == "movie" else "tv"

        url = f"{base}/api/v1/{endpoint}/{int(tmdb_id)}"
        headers = {"X-Api-Key": api_key, "Accept": "application/json"}

        try:
            resp = requests.get(url, headers=headers, timeout=12)
            if resp.status_code != 200:
                logger.error(f"[Overseerr detail {endpoint}] HTTP {resp.status_code} :: {resp.text}")
                return {"error": f"Overseerr returned HTTP {resp.status_code} for details."}
            return resp.json() or {}
        except Exception as e:
            logger.exception("[Overseerr details fetch error] %s", e)
            return {"error": f"Failed to fetch details: {e}"}

    @staticmethod
    def _pick_result(search_data: dict, prefer_type: str | None):
        results = (search_data or {}).get("results") or []
        if not results:
            return None

        prefer_type = (prefer_type or "").lower().strip()
        if prefer_type in {"movie", "tv"}:
            for r in results:
                mt = (r.get("mediaType") or r.get("media_type") or "").lower()
                if mt == prefer_type:
                    return r

        return results[0]

    @staticmethod
    def _tame_text(text: str, max_len: int = 700) -> str:
        text = re.sub(r"\s+", " ", (text or "").strip())
        return text[:max_len]

    @staticmethod
    def _fallback_details_text(detail: dict) -> str:
        title = (
            detail.get("title")
            or detail.get("name")
            or detail.get("originalTitle")
            or detail.get("originalName")
            or "Unknown title"
        )
        year = (detail.get("releaseDate") or detail.get("firstAirDate") or "")[:4]
        rating = detail.get("voteAverage")
        media_info = detail.get("mediaInfo")
        media_info = media_info if isinstance(media_info, dict) else {}
        media_status = detail.get("status") or media_info.get("status")
        runtime = detail.get("runtime")
        overview = re.sub(r"\s+", " ", str(detail.get("overview") or "").strip())

        heading = f"{title} ({year})" if year else str(title)
        facts = []
        if isinstance(rating, (int, float)):
            facts.append(f"Rating: {float(rating):.1f}/10")
        if media_status:
            facts.append(f"Status: {media_status}")
        if isinstance(runtime, int) and runtime > 0:
            facts.append(f"Runtime: {runtime} min")
        genres = detail.get("genres") or []
        if isinstance(genres, list) and genres:
            names = [g.get("name") for g in genres if isinstance(g, dict) and g.get("name")]
            if names:
                facts.append("Genres: " + ", ".join(names[:4]))

        lines = [heading]
        if overview:
            lines.append(overview[:420])
        lines.extend([f"- {item}" for item in facts[:6]])
        return "\n".join(lines).strip()

    async def _ask_llm_details(self, detail: dict, title: str, llm_client):
        if llm_client is None:
            return self._fallback_details_text(detail)

        first, last = get_tater_name()
        tater = f"{first} {last}"

        sys = (
            f"You are {tater}, a concise media guide. "
            "You are given JSON for ONE title (movie or TV). "
            "Write: 1 short overview paragraph (2–4 sentences) + 3–6 fact bullets. "
            "Keep it friendly and not too long."
        )

        user = (
            f"Title user asked about: {title}\n\n"
            f"Overseerr details JSON:\n{json.dumps(detail, ensure_ascii=False)}\n"
        )

        try:
            resp = await llm_client.chat([{"role": "system", "content": sys}, {"role": "user", "content": user}])
            content = ((resp or {}).get("message", {}) or {}).get("content", "") or ""
            out = content.strip()
            return out if out else self._fallback_details_text(detail)
        except Exception as e:
            logger.exception("[Overseerr details LLM error] %s", e)
            return self._fallback_details_text(detail)

    async def _answer(self, args, llm_client):
        args = args or {}
        title = (args.get("title") or "").strip()
        if not title:
            return "Tell me the movie or show title you want details for."

        prefer_type = (args.get("media_type") or "").strip().lower()

        search_data = self._search(title)
        if "error" in search_data:
            return search_data["error"]

        picked = self._pick_result(search_data, prefer_type)
        if not picked:
            return f"I couldn’t find '{title}' in Overseerr."

        tmdb_id = picked.get("id")
        media_type = (picked.get("mediaType") or picked.get("media_type") or prefer_type or "movie").lower()

        if not tmdb_id:
            return f"I couldn’t resolve an ID for '{title}'."

        detail = self._fetch_details(media_type, int(tmdb_id))
        if "error" in detail:
            return detail["error"]

        return await self._ask_llm_details(detail, title, llm_client)

    # ---------- Platform handlers ----------
    async def handle_discord(self, message, args, llm_client):
        answer = await self._answer(args, llm_client)
        return [answer]

    async def handle_webui(self, args, llm_client):
        async def inner():
            return await self._answer(args, llm_client)

        try:
            asyncio.get_running_loop()
            return [await inner()]
        except RuntimeError:
            return [asyncio.run(inner())]

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        answer = await self._answer(args, llm_client)
        return [f"{user}: {answer}"]

    async def handle_homeassistant(self, args, llm_client):
        answer = await self._answer(args, llm_client)
        return [self._tame_text(answer, 700)]

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        answer = await self._answer(args or {}, llm_client)
        return [answer]

    async def handle_telegram(self, update, args, llm_client):
        return [await self._answer(args or {}, llm_client)]

    async def handle_homekit(self, args, llm_client):
        answer = await self._answer(args or {}, llm_client)
        return [self._tame_text(answer, 500)]


plugin = OverseerrDetailsPlugin()
