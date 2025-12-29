# plugins/overseerr_trending.py
import json
import asyncio
import logging
import requests
import re
from datetime import datetime
from dotenv import load_dotenv
from plugin_base import ToolPlugin
from helpers import redis_client, get_tater_name

load_dotenv()
logger = logging.getLogger("overseerr_trending")
logger.setLevel(logging.INFO)


class OverseerrTrendingPlugin(ToolPlugin):
    name = "overseerr_trending"
    usage = (
        "{\n"
        '  "function": "overseerr_trending",\n'
        '  "arguments": {\n'
        '    "kind": "movies|tv",\n'
        '    "when": "trending|upcoming",\n'
        '    "user_question": "<the user’s original question>"\n'
        "  }\n"
        "}\n"
    )
    description = (
        "Fetche Trending or Upcoming movies and TV shows from Overseerr. "
        "You can ask for current trending or upcoming titles (e.g., 'what movies are trending', 'what TV shows are coming soon'), "
        "or request details about a specific movie or show (e.g., 'tell me about Dune 2'). "
    )
    plugin_dec = "List trending or upcoming movies and shows from Overseerr and answer related questions."
    pretty_name = "Overseerr: Trending & Upcoming"
    settings_category = "Overseerr"
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
    waiting_prompt_template = "Give {mention} a short, cheerful note that you’re fetching the latest lists from Overseerr now. Only output that message."
    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit"]

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

    def _fetch_list(self, kind: str, when: str):
        """
        Fetch raw Overseerr results with minimal manipulation.
        We do NOT filter/truncate; we return what Overseerr gives us.
        """
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}

        kind_map = {"movies": "movie", "movie": "movie", "tv": "tv", "show": "tv", "shows": "tv"}
        when = (when or "trending").lower().strip()
        requested_kind = kind_map.get((kind or "").lower().strip(), None)

        if when == "trending":
            # Mixed media types; we intentionally do *not* filter here.
            url = f"{base}/api/v1/discover/trending"
        else:
            # Upcoming requires choosing the path by media type; default to movie if not specified.
            kind_api = requested_kind or "movie"
            url = f"{base}/api/v1/discover/{'movies' if kind_api == 'movie' else 'tv'}/upcoming"

        params = {"page": 1}
        headers = {"X-Api-Key": api_key, "Accept": "application/json"}

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=12)
            if resp.status_code != 200:
                logger.error(f"[Overseerr {when}] HTTP {resp.status_code} :: {resp.text}")
                return {"error": f"Overseerr returned HTTP {resp.status_code} for {when}."}
            data = resp.json() or {}
            return data
        except Exception as e:
            logger.exception(f"[Overseerr {when} fetch error] {e}")
            return {"error": f"Failed to reach Overseerr: {e}"}

    @staticmethod
    def _fmt_date(d):
        if not d:
            return ""
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            return dt.strftime("%b %d, %Y")
        except Exception:
            return d

    def _infer_kind_when(self, args):
        kind = (args.get("kind") or "").lower().strip()
        when = (args.get("when") or "").lower().strip()
        user_q = (args.get("user_question") or "").lower()

        if when not in {"trending", "upcoming"}:
            when = "upcoming" if ("upcoming" in user_q or "coming soon" in user_q) else "trending"

        if kind not in {"movies", "movie", "tv", "shows", "show"}:
            kind = "tv" if ("tv" in user_q or "show" in user_q or "series" in user_q) else "movies"

        norm_kind = "movie" if kind.startswith("movie") else "tv"
        return norm_kind, when

    def _sanitize_items_for_llm(self, raw):
        """
        Overseerr returns a lot of fields; pass only what the LLM needs,
        but do NOT truncate descriptions. Keep up to ~30 to control token cost.
        """
        results = (raw.get("results") or raw.get("items") or [])[:30]
        cleaned = []
        for r in results:
            cleaned.append({
                "id": r.get("id"),
                "mediaType": r.get("mediaType") or r.get("media_type"),
                "title": r.get("title") or r.get("name"),
                "originalTitle": r.get("originalTitle") or r.get("original_name"),
                "overview": r.get("overview"),
                "releaseDate": r.get("releaseDate") or r.get("firstAirDate"),
                "firstAirDate": r.get("firstAirDate"),
                "popularity": r.get("popularity"),
                "voteAverage": r.get("voteAverage") or r.get("vote_average"),
                "posterPath": r.get("posterPath") or r.get("poster_path"),
            })
        return cleaned

    async def _ask_llm(self, items, norm_kind, when, user_question, llm_client):
        first, last = get_tater_name()
        tater = f"{first} {last}"

        sys = (
            f"You are {tater}, a concise and helpful media guide. "
            "You're given JSON movies/TV items from Overseerr and the user's ask. "
            "Answer clearly in a natural, conversational tone. "
            "Default to an UNNUMBERED list of up to 10 titles with years only (e.g., Title (Year)), "
            "with NO descriptions. Use simple dash bullets or line breaks; never use numbered lists. "
            "Only include a brief one-liner when the user explicitly asks for 'details', 'overview', "
            "'plot', 'synopsis', or names a specific title. If the user asks for a paragraph, write a "
            "compact paragraph that still omits descriptions unless specifically requested. "
            "Keep things tight and readable."
        )

        payload = {
            "intent": {"when": when, "requested_kind": norm_kind, "max_present": 10},
            "items": items,
        }

        user = (
            "User question:\n"
            f"{user_question or '(no extra question provided)'}\n\n"
            "Overseerr items (JSON):\n"
            f"{json.dumps(payload, ensure_ascii=False)}\n\n"
            "Style rules:\n"
            f"- Prefer the requested kind ('{norm_kind}') if results mix movies and TV; otherwise use what's provided.\n"
            "- Default output: an UNNUMBERED list of up to 10 items formatted as Title (Year), no descriptions.\n"
            "- Never use numbered lists; use dash bullets or line breaks.\n"
            "- Only add a short one-liner for titles the user explicitly asked about (by name) or if they asked for 'details/overview/plot/synopsis'.\n"
            "- If the user asks for a paragraph instead of a list, keep it compact and still omit descriptions unless requested.\n"
            "- if you list more then 3 movies, end with a gentle nudge like: 'Want details on any of these?'\n"
        )

        messages = [
            {"role": "system", "content": sys},
            {"role": "user", "content": user},
        ]

        try:
            resp = await llm_client.chat(messages)
            content = (resp or {}).get("message", {}).get("content", "") or ""
            return content.strip() or "No results right now."
        except Exception as e:
            logger.exception("[Overseerr _ask_llm chat] %s", e)
            return "There was an error generating the summary."

    # ---------- Core answer ----------
    async def _answer(self, args, llm_client):
        norm_kind, when = self._infer_kind_when(args)
        user_q = args.get("user_question") or ""

        data = self._fetch_list(norm_kind, when)
        if "error" in data:
            return data["error"]

        items = self._sanitize_items_for_llm(data)
        if not items:
            return f"No {when} results found."

        return await self._ask_llm(items, norm_kind, when, user_q, llm_client)

    # ---------- Platform handlers ----------
    async def handle_discord(self, message, args, llm_client):
        try:
            answer = await self._answer(args, llm_client)
            return await self.safe_send(message.channel, answer)
        except Exception as e:
            logger.exception("[Overseerr handle_discord] %s", e)
            return await self.safe_send(message.channel, f"Error: {e}")

    async def handle_webui(self, args, llm_client):
        async def inner():
            return await self._answer(args, llm_client)
        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        try:
            answer = await self._answer(args, llm_client)
            return f"{user}: {answer}"
        except Exception as e:
            logger.exception("[Overseerr handle_irc] %s", e)
            return f"{user}: Error: {e}"

    async def handle_homeassistant(self, args, llm_client):
        try:
            # Keep HA output concise but rely on the same LLM logic.
            answer = await self._answer(args, llm_client)
            # Strip excessive whitespace for TTS friendliness
            lines = [ln.strip() for ln in answer.splitlines() if ln.strip()]
            return " ".join(lines)
        except Exception as e:
            logger.exception("[Overseerr handle_homeassistant] %s", e)
            return "There was an error fetching results."

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        """
        Matrix returns a plain string; the Matrix platform will send it (and encrypt if the room is E2EE).
        """
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        try:
            answer = await self._answer(args or {}, llm_client)
            return answer
        except Exception as e:
            logger.exception("[Overseerr handle_matrix] %s", e)
            return f"Error: {e}"

    async def handle_homekit(self, args, llm_client):
        """
        HomeKit/Siri output: concise, TTS-friendly summary of trending/upcoming.
        """
        try:
            answer = await self._answer(args or {}, llm_client)
            return self._siri_flatten(answer)
        except Exception as e:
            logger.exception("[Overseerr handle_homekit] %s", e)
            return "There was an error fetching results."

    # ---------- Utilities ----------
    def split_message(self, text, chunk_size=1500):
        chunks = []
        while len(text) > chunk_size:
            split = text.rfind('\n', 0, chunk_size) or text.rfind(' ', 0, chunk_size) or chunk_size
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

    def _siri_flatten(self, text: str | None) -> str:
        """Make responses clean for Siri TTS (no markdown noise, compact one-liner or short list)."""
        if not text:
            return "No results right now."
        out = str(text).strip()

        # Strip heavy markdown/emphasis
        out = re.sub(r"[`*_]{1,3}", "", out)

        # If it looks like a list, keep the first ~6 items as a short, comma-separated line.
        lines = [ln.strip("-• ").strip() for ln in out.splitlines() if ln.strip()]
        if len(lines) > 1:
            # Keep first 6 items to avoid long TTS
            head = lines[:6]
            # If items contain years already (Title (Year)), keep as-is.
            joined = ", ".join(head)
            if len(lines) > 6:
                joined += " …"
            return joined[:500]

        # Otherwise return a compact single line
        out = re.sub(r"\s+", " ", out)
        return out[:500]            


plugin = OverseerrTrendingPlugin()
