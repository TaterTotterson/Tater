# plugins/tater_gits_add_feed.py
import re
import json
import aiohttp
import logging
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from plugin_base import ToolPlugin
from helpers import extract_json

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class TaterGitsAddFeedPlugin(ToolPlugin):
    name = "tater_gits_add_feed"
    plugin_name = "Tater Gits Add Feed"
    version = "1.0.0"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "tater_gits_add_feed",\n'
        '  "arguments": {"url": "https://github.com/OWNER/REPO/releases.atom"}\n'
        "}\n"
    )
    description = "Adds a GitHub releases feed to the tater-gits watcher. Infers title prefix and category via LLM."
    plugin_dec = "Add a GitHub releases feed to the tater-gits watcher with smart naming."
    pretty_name = "Add Git Feed"
    waiting_prompt_template = "Tell {mention} I’m analyzing that repo and adding the feed now. Output only that friendly message."
    platforms = ["webui", "discord", "irc", "matrix", "telegram"]
    settings_category = "Tater Gits"

    required_settings = {
        "watcher_url": {
            "label": "Watcher URL",
            "type": "string",
            "default": "http://localhost:8787",
            "description": "Base URL for tater-gits watcher API (e.g., http://tater-gits-watcher:8787)"
        },
        "api_key": {
            "label": "API Key (optional)",
            "type": "string",
            "default": "",
            "description": "Optional shared secret; sent as X-Tater-Auth"
        },
    }

    # ───────────────────────── internals ─────────────────────────
    def _parse_repo(self, url: str):
        """From any GitHub repo/release URL, return (repo_url, owner, repo_name)."""
        m = re.match(r"https?://github\.com/([^/]+)/([^/]+)", (url or "").strip())
        if not m:
            return None, None, None
        owner, repo = m.group(1), m.group(2).replace(".git", "")
        return f"https://github.com/{owner}/{repo}", owner, repo

    def _prettify_repo_name(self, repo: str) -> str:
        """
        Nicely format a repo slug:
        - Replace -/_ with spaces
        - Split CamelCase/PascalCase
        - Title-case, preserving common acronyms
        """
        name = re.sub(r"[-_]+", " ", repo)
        name = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name).strip()

        titled = name.title()
        ACRONYMS = {"AI", "ML", "LLM", "NLP", "GPU", "CPU", "AR", "VR", "ROM", "SDK", "API", "CLI", "UI", "HTTP"}
        words = []
        for w in titled.split():
            words.append(w.upper() if w.upper() in ACRONYMS else w)
        return " ".join(words)

    async def _fetch_readme_text(self, owner: str, repo: str) -> str:
        """Grab README text for classification (HTML first, else empty)."""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
        repo_url = f"https://github.com/{owner}/{repo}"
        try:
            async with aiohttp.ClientSession(headers=headers) as s:
                async with s.get(repo_url, timeout=15) as r:
                    html = await r.text()
            soup = BeautifulSoup(html, "html.parser")
            readme = soup.select_one("#readme .markdown-body") or soup.select_one("article .markdown-body")
            if readme:
                text = readme.get_text("\n", strip=True)
                if text:
                    return text
        except Exception:
            pass
        return ""

    async def _classify_category(self, app_title: str, readme_text: str, llm_client) -> str | None:
        """
        Returns 'ai' or 'homebrew'. Returns None if classification didn't actually produce JSON.
        """
        prompt = (
            "Classify this GitHub project for a feed.\n\n"
            f"TITLE:\n{app_title}\n\n"
            f"README (excerpt):\n{(readme_text or '')[:6000]}\n\n"
            "Instructions:\n"
            "- First, (internally) summarize in your head to understand the project.\n"
            "- Then decide category:\n"
            "    * 'ai' if the project is primarily about AI/ML/LLM/agents/models or tools centered on them.\n"
            "    * otherwise 'homebrew' (retro gaming, emulators, ROM tools, handhelds, modding, utilities, etc.).\n"
            "- If ambiguous, choose 'homebrew'.\n\n"
            "Respond ONLY with strict JSON (no extra text):\n"
            "{\n"
            '  "category": "ai" | "homebrew"\n'
            "}"
        )

        if llm_client is None or not hasattr(llm_client, "chat"):
            return None

        resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
        content = (resp.get("message") or {}).get("content", "")
        content = content.strip() if isinstance(content, str) else ""
        if not content:
            return None

        try:
            obj = json.loads(content)
        except Exception:
            json_str = extract_json(content)
            if not json_str:
                return None
            try:
                obj = json.loads(json_str)
            except Exception:
                return None

        category = (obj.get("category") or "").strip().lower()
        if category not in ("ai", "homebrew"):
            return None
        return category

    async def _add_via_watcher(self, payload: dict, base: str, key: str) -> str:
        headers = {"X-Tater-Auth": key} if key else {}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(f"{base}/feeds", json=payload, headers=headers, timeout=12) as r:
                    txt = await r.text()
                    if r.status != 200:
                        return f"❌ Add failed: {r.status} {txt}"
        except Exception as e:
            return f"❌ Request error: {e}"
        # success here means watcher accepted it
        return f"✅ Added: [{payload['category']}] {payload['title_prefix']}{payload['url']}"

    async def _get_cfg(self):
        try:
            from plugin_settings import get_plugin_settings
            s = get_plugin_settings(self.settings_category)
            base = (s.get("watcher_url") or "").rstrip("/")
            key = s.get("api_key") or ""
        except Exception:
            base, key = "", ""
        return base, key

    async def _run(self, url: str, llm_client=None):
        if not url:
            return "❌ Need a GitHub releases feed URL (e.g., https://github.com/OWNER/REPO/releases.atom)."

        repo_url, owner, repo = self._parse_repo(url)
        if not repo_url:
            return "❌ That doesn’t look like a GitHub repo/releases URL."

        app_title = self._prettify_repo_name(repo)
        readme_text = await self._fetch_readme_text(owner, repo)

        # Classification must complete BEFORE posting
        category = await self._classify_category(app_title, readme_text, llm_client)
        if category is None:
            return "❌ Couldn’t classify this repo (AI didn’t return a category). Try again in a moment."

        title_prefix = f"{app_title} - "

        base, key = await self._get_cfg()
        if not base:
            return "❌ No watcher_url configured in plugin settings."

        payload = {"url": url.strip(), "title_prefix": title_prefix, "category": category}
        return await self._add_via_watcher(payload, base, key)

    # ───────────────────────── handlers ─────────────────────────
    async def handle_webui(self, args, llm_client):
        return await self._run(args.get("url"), llm_client)

    async def handle_discord(self, message, args, llm_client):
        url = (args or {}).get("url")
        if not url:
            return f"{message.author.mention}: ❌ Please provide a GitHub releases feed URL."
        result = await self._run(url, llm_client)
        return result  # Matrix/Discord layer will send/segment as needed

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        url = (args or {}).get("url")
        if not url:
            return f"{user}: ❌ Please provide a GitHub releases feed URL."
        result = await self._run(url, llm_client)
        return f"{user}: {result}"

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        """
        Return plain text; Matrix platform will post it (and chunk if needed).
        """
        url = (args or {}).get("url")
        if not url:
            return "❌ Please provide a GitHub releases feed URL."
        return await self._run(url, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args, llm_client)


plugin = TaterGitsAddFeedPlugin()
