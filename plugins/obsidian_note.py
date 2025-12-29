import logging
import re
from datetime import datetime
from urllib.parse import quote
from typing import Dict, Optional

import requests
from plugin_base import ToolPlugin
from helpers import redis_client

logger = logging.getLogger("obsidian_note")
logger.setLevel(logging.INFO)


class ObsidianNotePlugin(ToolPlugin):
    """
    Always creates a new note with an AI-generated title at the vault root.

    - Ignores any provided `path`.
    - Asks the model to suggest a short, descriptive title based on `content`.
    - Sanitizes to a valid filename, ensures uniqueness, creates with PUT.
    """

    name = "obsidian_note"
    plugin_name = "Obsidian Note"
    pretty_name = "Add to Obsidian"
    description = "Always creates a new note with an AI-generated title at the vault root."
    plugin_dec = "Create a new Obsidian note with an AI-generated title and content."
    usage = (
        "{\n"
        '  "function": "obsidian_note",\n'
        '  "arguments": {"content": "<markdown>"}\n'
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
        "Write a friendly message telling {mention} you’re adding a fresh note to their Obsidian vault! "
        "Only output that message."
    )

    platforms = ["webui"]

    _INVALID_CHARS = r'[:*?"<>|\\]'

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

    def _headers_md(self) -> Dict[str, str]:
        token = self._cfg().get("OBSIDIAN_TOKEN", "") or ""
        h = {
            "User-Agent": "Tater-ObsidianPlugin/1.0",
            "Accept": "*/*",
            "Content-Type": "text/markdown",
        }
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def _headers_json(self) -> Dict[str, str]:
        token = self._cfg().get("OBSIDIAN_TOKEN", "") or ""
        h = {"User-Agent": "Tater-ObsidianPlugin/1.0", "Accept": "application/json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def _sanitize_title_to_filename(self, title: str) -> str:
        t = re.sub(self._INVALID_CHARS, "", title).strip()
        t = re.sub(r"\s+", " ", t)
        if len(t) > 80:
            t = t[:80].rstrip()
        if not t:
            t = datetime.now().strftime("Note %Y-%m-%d %H-%M-%S")
        return t

    def _ensure_md_extension(self, path: str) -> str:
        return path if path.lower().endswith(".md") else f"{path}.md"

    def _safe_url_path(self, path: str) -> str:
        return quote(path, safe="/")

    def _exists(self, path: str) -> bool:
        url = f"{self._base()}/vault/{self._safe_url_path(path)}"
        try:
            r = requests.get(url, headers=self._headers_json(), verify=self._verify(), timeout=(3, 6))
            return r.status_code == 200
        except Exception:
            return False

    def _unique_filename_at_root(self, filename_md: str) -> str:
        base_name = filename_md[:-3] if filename_md.lower().endswith(".md") else filename_md
        candidate = f"{base_name}.md"
        i = 1
        while self._exists(candidate):
            i += 1
            candidate = f"{base_name}-{i}.md"
        return candidate

    async def _ai_suggest_title(self, content: str, llm_client) -> str:
        system = "You are an assistant that generates concise, human-friendly note titles."
        user = (
            "Based on the note content below, produce a short, descriptive title for the note.\n"
            "Rules:\n"
            "- 3 to 8 words\n"
            "- Title Case (Capitalize Main Words)\n"
            "- No emojis, no quotes, no trailing punctuation\n\n"
            "<<BEGIN CONTENT>>\n"
            f"{content[:4000]}\n"
            "<<END CONTENT>>\n\n"
            "Output ONLY the title text."
        )
        resp = await llm_client.chat(messages=[{"role": "system", "content": system}, {"role": "user", "content": user}])
        title = ((resp.get("message", {}) or {}).get("content", "") or "").strip()
        if not title:
            title = datetime.now().strftime("Note %Y-%m-%d %H-%M-%S")
        return self._sanitize_title_to_filename(title)

    def _put(self, path: str, content: str) -> bool:
        url = f"{self._base()}/vault/{self._safe_url_path(path)}"
        r = requests.put(
            url,
            headers=self._headers_md(),
            data=content.encode("utf-8"),
            verify=self._verify(),
            timeout=(3, 6),
        )
        logger.debug("[Obsidian note PUT] %s -> %s", url, r.status_code)
        return r.status_code in (200, 201, 204)

    async def _save(self, content: str, llm_client) -> str:
        if not content:
            return "No content provided."

        suggested_title = await self._ai_suggest_title(content, llm_client)
        filename_md = self._ensure_md_extension(suggested_title)
        unique_path = self._unique_filename_at_root(filename_md)

        ok = self._put(unique_path, content)
        return f"Created new note: `{unique_path}`" if ok else f"Failed to create note: `{unique_path}`"

    async def handle_webui(self, args, llm_client):
        content = (args or {}).get("content", "").strip()
        msg = await self._save(content, llm_client)
        return [msg]

    async def handle_discord(self, message, args, llm_client):
        return "Obsidian note editing is only supported in the WebUI."

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return f"{user}: Obsidian note editing is only supported in the WebUI."


plugin = ObsidianNotePlugin()
