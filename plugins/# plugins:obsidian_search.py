# plugins/obsidian_search.py
import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
from urllib.parse import quote

import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import format_irc, get_tater_name, redis_client

load_dotenv()
logger = logging.getLogger("obsidian_search")
logger.setLevel(logging.INFO)


class ObsidianSearchPlugin(ToolPlugin):
    """
    Search Obsidian vault via Local REST API and summarize matches with the LLM.

    Model call shape:
    {
      "function": "obsidian_search",
      "arguments": {
        "query": "cloud flare overview",
        "max_notes": 5,            // optional (default 3)
        "max_chars_per_note": 4000 // optional (default 3000)
      }
    }
    """

    name = "obsidian_search"
    pretty_name = "Search Obsidian"
    description = "Searches your Obsidian vault and summarizes relevant notes."
    usage = (
        "{\n"
        '  "function": "obsidian_search",\n'
        '  "arguments": {"query": "<keywords>"}\n'
        "}\n"
    )

    settings_category = "Obsidian"
    required_settings = {
        "OBSIDIAN_API_BASE": {
            "label": "API Base URL",
            "type": "string",
            "default": "http://127.0.0.1:27123",
        },
        "OBSIDIAN_TOKEN": {
            "label": "Bearer Token (optional)",
            "type": "string",
            "default": "",
        },
    }
    # Optional knobs shown in WebUI (stored under plugin_settings:Obsidian)
    optional_settings_schema = {
        "DEFAULT_MAX_NOTES": {
            "label": "Default Max Notes",
            "type": "integer",
            "default": 3,
            "min": 1,
            "max": 15,
        },
        "DEFAULT_MAX_CHARS_PER_NOTE": {
            "label": "Max Characters per Note (when summarizing)",
            "type": "integer",
            "default": 3000,
            "min": 500,
            "max": 10000,
        },
    }

    waiting_prompt_template = (
        "🔎 Searching your Obsidian vault now for {mention}… one sec while I gather notes!"
    )
    platforms = ["discord", "webui", "irc"]

    # ---- HTTP helpers ----
    def _headers(self) -> Dict[str, str]:
        cfg = redis_client.hgetall(f"plugin_settings:{self.settings_category}")
        token = cfg.get("OBSIDIAN_TOKEN", "") or ""
        headers = {
            "User-Agent": "Tater-ObsidianPlugin/1.0",
            "Accept": "application/json, text/plain, */*",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _base(self) -> str:
        cfg = redis_client.hgetall(f"plugin_settings:{self.settings_category}")
        return cfg.get("OBSIDIAN_API_BASE", "http://127.0.0.1:27123").rstrip("/")

    def _search(self, query: str) -> List[Dict[str, Any]]:
        """
        POST /search  { "query": "<text>" }  -> returns matches
        Fallback to empty on error.
        """
        url = f"{self._base()}/search"
        try:
            resp = requests.post(url, headers=self._headers(), json={"query": query}, timeout=10)
            if resp.status_code != 200:
                logger.error(f"[Obsidian search] HTTP {resp.status_code}: {resp.text}")
                return []
            data = resp.json()
            # Expecting a list of results with fields like 'path', 'score' (varies by plugin version)
            if isinstance(data, list):
                return data
            # Some versions return {'results': [...]}
            if isinstance(data, dict) and isinstance(data.get("results"), list):
                return data["results"]
            return []
        except Exception as e:
            logger.exception(f"[Obsidian search error] {e}")
            return []

    def _get_note(self, path: str) -> Optional[str]:
        """
        GET /vault/<path>  -> returns raw Markdown
        """
        safe_path = quote(path)
        url = f"{self._base()}/vault/{safe_path}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=10)
            if resp.status_code != 200:
                logger.warning(f"[Obsidian get] {path} -> HTTP {resp.status_code}")
                return None
            return resp.text
        except Exception as e:
            logger.exception(f"[Obsidian get error] {path} :: {e}")
            return None

    # ---- Core flow ----
    async def _summarize(self, query: str, notes: List[Dict[str, Any]], llm_client, max_chars_per_note: int) -> str:
        first, last = get_tater_name()

        # Fetch contents (trim by char budget)
        collected = []
        for n in notes:
            path = n.get("path") or n.get("file") or n.get("href") or ""
            if not path:
                continue
            content = await asyncio.to_thread(self._get_note, path)
            if not content:
                continue
            snippet = content[:max_chars_per_note]
            collected.append({"path": path, "content": snippet})

        if not collected:
            return f"No readable notes matched '{query}'."

        # Build a summarization prompt
        joined = "\n\n".join(
            [f"# {c['path']}\n{c['content']}" for c in collected]
        )
        sys_prompt = (
            f"You are {first} {last}. Summarize the following Obsidian note excerpts that match the query: '{query}'. "
            "Provide:\n"
            "1) A concise answer (3–6 bullets max)\n"
            "2) A tiny 'Where to look' list with note paths\n"
            "Do NOT self-introduce. Be direct."
        )

        messages = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": joined},
        ]
        resp = await llm_client.chat(messages=messages)
        return (resp.get("message", {}) or {}).get("content", "").strip() or "No summary generated."

    # ---- Platform handlers ----
    async def handle_discord(self, message, args, llm_client):
        query = (args or {}).get("query", "").strip()
        if not query:
            return "No search query provided."

        cfg = redis_client.hgetall(f"plugin_settings:{self.settings_category}")
        max_notes = int((args or {}).get("max_notes") or cfg.get("DEFAULT_MAX_NOTES", 3))
        max_chars = int((args or {}).get("max_chars_per_note") or cfg.get("DEFAULT_MAX_CHARS_PER_NOTE", 3000))

        results = self._search(query)
        if not results:
            return f"No results for '{query}'."

        # Pick top-N (try to sort by 'score' descending if available)
        if results and isinstance(results[0], dict) and "score" in results[0]:
            results = sorted(results, key=lambda r: r.get("score", 0), reverse=True)
        top = results[:max_notes]

        return await self._summarize(query, top, llm_client, max_chars)

    async def handle_webui(self, args, llm_client):
        query = (args or {}).get("query", "").strip()
        if not query:
            return ["No search query provided."]

        cfg = redis_client.hgetall(f"plugin_settings:{self.settings_category}")
        max_notes = int((args or {}).get("max_notes") or cfg.get("DEFAULT_MAX_NOTES", 3))
        max_chars = int((args or {}).get("max_chars_per_note") or cfg.get("DEFAULT_MAX_CHARS_PER_NOTE", 3000))

        async def inner():
            results = self._search(query)
            if not results:
                return f"No results for '{query}'."
            if results and isinstance(results[0], dict) and "score" in results[0]:
                ordered = sorted(results, key=lambda r: r.get("score", 0), reverse=True)
            else:
                ordered = results
            top = ordered[:max_notes]
            return await self._summarize(query, top, llm_client, max_chars)

        try:
            asyncio.get_running_loop()
            text = await inner()
        except RuntimeError:
            text = asyncio.run(inner())

        return [text]

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        query = (args or {}).get("query", "").strip()
        if not query:
            return f"{user}: No search query provided."
        # Reuse Discord logic
        text = await self.handle_discord(type("M", (), {"content": raw_message})(), args, llm_client)
        return format_irc(text)


plugin = ObsidianSearchPlugin()