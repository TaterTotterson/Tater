import asyncio
import logging
from typing import List, Optional
from urllib.parse import quote

import requests
from plugin_base import ToolPlugin
from helpers import get_tater_name, redis_client

logger = logging.getLogger("obsidian_search")
logger.setLevel(logging.INFO)


class ObsidianSearchPlugin(ToolPlugin):
    """
    AI-only Obsidian search:

      1) Lists ALL files in the vault.
      2) Fetches FULL content of every .md file.
      3) For each file, asks the AI to extract ONLY content that answers the user's ask.
      4) Aggregates all extracts into a single, direct answer.

    No limits, no truncation — the AI sees everything.
    """

    name = "obsidian_search"
    plugin_name = "Obsidian Search"
    pretty_name = "Search Obsidian"
    description = "Searches all notes in your vault. Extracts relevant info from each file and combines into one answer."
    plugin_dec = "Search your Obsidian vault and summarize matching notes."
    usage = (
        "{\n"
        '  "function": "obsidian_search",\n'
        '  "arguments": {"query": "<keywords or question>", "user_question": "<original user ask, optional>"}\n'
        "}\n"
    )

    settings_category = "Obsidian"
    required_settings = {
        "OBSIDIAN_PROTOCOL": {"label": "Protocol (http/https)", "type": "string", "default": "http"},
        "OBSIDIAN_HOST": {"label": "Host", "type": "string", "default": "127.0.0.1"},
        "OBSIDIAN_PORT": {"label": "Port", "type": "integer", "default": 27123},
        "OBSIDIAN_TOKEN": {"label": "Bearer Token", "type": "string", "default": ""},
        "VERIFY_SSL": {"label": "Verify SSL certs (https only)", "type": "boolean", "default": False},
    }

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re searching the Obsidian vault for notes right now! "
        "Only output that message."
    )

    platforms = ["webui"]

    def _cfg(self):
        return redis_client.hgetall(f"plugin_settings:{self.settings_category}")

    def _base(self) -> str:
        c = self._cfg()
        proto = (c.get("OBSIDIAN_PROTOCOL") or "http").lower()
        host = c.get("OBSIDIAN_HOST") or "127.0.0.1"
        port = int(c.get("OBSIDIAN_PORT") or 27123)
        return f"{proto}://{host}:{port}".rstrip("/")

    def _verify(self) -> bool:
        v = self._cfg().get("VERIFY_SSL")
        return str(v).lower() in ("1", "true", "yes", "on")

    def _headers_json(self):
        token = self._cfg().get("OBSIDIAN_TOKEN", "") or ""
        h = {"User-Agent": "Tater-ObsidianPlugin/1.0", "Accept": "application/json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def _list_files(self) -> List[str]:
        base = self._base()
        headers = self._headers_json()
        urls = [f"{base}/vault/", f"{base}/vault"]
        for url in urls:
            try:
                r = requests.get(url, headers=headers, verify=self._verify(), timeout=(3, 6))
                if r.status_code != 200:
                    continue
                j = r.json()
                files = j.get("files") if isinstance(j, dict) else j
                paths = []
                for item in files:
                    if isinstance(item, str):
                        paths.append(item)
                    elif isinstance(item, dict):
                        p = item.get("path") or item.get("file") or item.get("name")
                        if p:
                            paths.append(p)
                return paths
            except Exception as e:
                logger.warning("[Obsidian vault list error] %s", e)
        return []

    def _get_file_contents(self, path: str) -> Optional[str]:
        safe = quote(path, safe="/")
        url = f"{self._base()}/vault/{safe}"
        try:
            r = requests.get(url, headers=self._headers_json(), verify=self._verify(), timeout=(3, 6))
            if r.status_code == 200 and r.text:
                return r.text
        except Exception as e:
            logger.debug("[Obsidian get file] %s failed: %s", path, e)
        return None

    async def _extract_from_file(self, ask: str, path: str, content: str, llm_client) -> Optional[str]:
        """Ask the model to extract ONLY info relevant to the ask from this file."""
        first, last = get_tater_name()
        system = (
            "You are a precise extractor. From ONE note, extract ONLY information that directly answers the user's ask. "
            "If nothing relevant is present, reply with the single word: NONE"
        )
        user = (
            f"User ask: {ask}\n\n"
            f"File: {path}\n"
            "Full note content follows between the markers.\n"
            "<<BEGIN NOTE>>\n"
            f"{content}\n"
            "<<END NOTE>>\n\n"
            "Output rules:\n"
            "- Provide every relevant detail, code block, list, or explanation that helps answer the ask.\n"
            "- If nothing relevant, output exactly: NONE"
        )
        resp = await llm_client.chat(messages=[{"role": "system", "content": system}, {"role": "user", "content": user}])
        text = (resp.get("message", {}) or {}).get("content", "") or ""
        cleaned = text.strip()
        if not cleaned or cleaned.upper() == "NONE":
            return None
        return f"### {path}\n{cleaned}"

    async def _aggregate_answer(self, ask: str, extracts: List[str], llm_client) -> str:
        if not extracts:
            return f"No notes found containing information related to '{ask}'."

        joined = "\n\n---\n\n".join(extracts)
        system = (
            "You are a precise assistant. Combine the provided per-file extracts into ONE direct answer to the user's ask. "
            "Use ONLY the extracts; do not invent details."
        )
        user = (
            f"User ask: {ask}\n\n"
            "From the following per-file extracts, produce:\n"
            "1) Direct Answer — give the user everything they asked for, including any commands, lists, or narrative.\n"
            "2) Key Details — bullets with important context or caveats.\n"
            "3) Sources — list all file paths used.\n\n"
            f"{joined}"
        )
        resp = await llm_client.chat(messages=[{"role": "system", "content": system}, {"role": "user", "content": user}])
        return (resp.get("message", {}) or {}).get("content", "").strip() or "No summary generated."

    async def handle_webui(self, args, llm_client):
        ask = (args or {}).get("user_question") or (args or {}).get("query")
        if not ask:
            return ["No query provided."]

        all_paths = self._list_files()
        md_paths = [p for p in all_paths if p.lower().endswith(".md")]
        logger.info("[Obsidian] Found %d .md files; scanning all", len(md_paths))

        extracts: List[str] = []
        for path in md_paths:
            content = self._get_file_contents(path)
            if not content:
                continue
            piece = await self._extract_from_file(ask, path, content, llm_client)
            if piece:
                extracts.append(piece)

        logger.info("[Obsidian] Per-file extracts: %d", len(extracts))
        answer = await self._aggregate_answer(ask, extracts, llm_client)
        return [answer]

    async def handle_discord(self, message, args, llm_client):
        return "Obsidian search is only supported in the WebUI."

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return f"{user}: Obsidian search is only supported in the WebUI."


plugin = ObsidianSearchPlugin()
