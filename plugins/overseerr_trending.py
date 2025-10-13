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
        '    "when": "trending|upcoming"\n'
        "  }\n"
        "}\n"
    )
    description = (
        "Fetches Trending or Upcoming movies/TV shows from Overseerr. "
        "Example: what movies are trending, what TV shows are trending, "
        "what movies are upcoming, what TV shows are upcoming."
    )
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
    platforms = ["discord", "webui", "irc", "homeassistant"]

    # ---------- Internals ----------
    @staticmethod
    def _get_settings():
        s = redis_client.hgetall("plugin_settings:Overseerr")
        base = s.get("OVERSEERR_BASE_URL", "http://localhost:5055").rstrip("/")
        api = s.get("OVERSEERR_API_KEY", "")
        return base, api

    def _fetch_list(self, kind: str, when: str):
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}

        kind_map = {"movies": "movie", "movie": "movie", "tv": "tv", "show": "tv", "shows": "tv"}
        when = (when or "trending").lower().strip()
        kind_api = kind_map.get((kind or "").lower().strip(), "movie")

        if when == "trending":
            url = f"{base}/api/v1/discover/trending"
        else:
            url = f"{base}/api/v1/discover/{'movies' if kind_api == 'movie' else 'tv'}/upcoming"

        params = {"page": 1}
        headers = {"X-Api-Key": api_key, "Accept": "application/json"}

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=12)
            if resp.status_code != 200:
                logger.error(f"[Overseerr {when}] HTTP {resp.status_code} :: {resp.text}")
                return {"error": f"Overseerr returned HTTP {resp.status_code} for {when} {kind_api}."}
            data = resp.json() or {}

            # If trending (mixed), filter to requested kind
            if when == "trending":
                results = data.get("results") or []
                media_key = "mediaType"
                filtered = [r for r in results if (r.get(media_key) or "").lower() == kind_api]
                data["results"] = filtered

            return data
        except Exception as e:
            logger.exception(f"[Overseerr {when} fetch error] {e}")
            return {"error": f"Failed to reach Overseerr: {e}"}

    @staticmethod
    def _fmt_date(d):
        if not d:
            return ""
        try:
            return datetime.strptime(d, "%Y-%m-%d").strftime("%b %-d, %Y")
        except Exception:
            return d

    def _format_items_text(self, items, kind_label: str, limit: int = 5):
        lines = []
        for i, it in enumerate(items[:limit], start=1):
            title = it.get("title") or it.get("name") or "Untitled"
            year = ""
            rd = it.get("releaseDate") or it.get("firstAirDate")
            if rd and len(rd) >= 4:
                year = rd[:4]
            blurb = it.get("overview", "")
            short_blurb = (blurb[:180] + "…") if blurb and len(blurb) > 180 else blurb

            line = f"{i}. {title}"
            if year:
                line += f" ({year})"
            if short_blurb:
                line += f"\n   {short_blurb}"
            lines.append(line)

        header = f"Top {min(limit, len(items))} {kind_label}"
        return header + "\n" + ("\n".join(lines) if lines else "No results.")

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

    # ---------- Public entry ----------
    def _build_answer(self, args):
        kind = (args.get("kind") or "").lower().strip()
        when = (args.get("when") or "").lower().strip()
        limit = 5

        if when not in {"trending", "upcoming"}:
            maybe_text = (args.get("user_question") or "").lower()
            when = "upcoming" if ("upcoming" in maybe_text or "coming soon" in maybe_text) else "trending"

        if kind not in {"movies", "movie", "tv", "shows", "show"}:
            maybe_text = (args.get("user_question") or "").lower()
            kind = "tv" if ("tv" in maybe_text or "show" in maybe_text) else "movies"

        data = self._fetch_list(kind, when)
        if "error" in data:
            return data["error"]

        items = data.get("results") or data.get("items") or []
        if not items:
            return f"No {when} {kind} found."

        kind_label = f"{'Movies' if kind.startswith('movie') else 'TV Shows'} — {when.title()}"
        return self._format_items_text(items, kind_label, limit)

    # ---------- Platform handlers ----------
    async def handle_discord(self, message, args, llm_client):
        answer = self._build_answer(args)
        return await self.safe_send(message.channel, answer)

    async def handle_webui(self, args, llm_client):
        async def inner():
            return self._build_answer(args)
        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        answer = self._build_answer(args)
        return f"{user}: {answer}"

    async def handle_homeassistant(self, args, llm_client):
        text = self._build_answer(args)
        lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            return "No results right now."

        header = lines[0]
        entries_raw = []

        item_re = re.compile(r"^\d+\.\s+(?P<title>.+)$")
        current = None
        for ln in lines[1:]:
            m = item_re.match(ln)
            if m:
                if current:
                    entries_raw.append(current)
                title_line = m.group("title").replace("(", "").replace(")", "")
                current = title_line
            else:
                if current is not None:
                    desc = ln.lstrip()
                    current = f"{current}. {desc}"

        if current:
            entries_raw.append(current)

        entries = [f"Number {i}. {e}" for i, e in enumerate(entries_raw[:5], start=1)]
        if not entries:
            return header

        return f"{header}. " + " ".join(entries)

plugin = OverseerrTrendingPlugin()