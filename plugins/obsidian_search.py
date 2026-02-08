import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

from helpers import redis_client
from plugin_base import ToolPlugin
from plugin_result import action_failure

logger = logging.getLogger("obsidian_search")
logger.setLevel(logging.INFO)


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def _as_int(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    if parsed < min_value:
        return min_value
    if parsed > max_value:
        return max_value
    return parsed


class ObsidianSearchPlugin(ToolPlugin):
    name = "obsidian_search"
    plugin_name = "Obsidian Search"
    pretty_name = "Search Obsidian"
    version = "2.0.0"
    min_tater_version = "50"

    description = "Search markdown notes in Obsidian with bounded scan limits and ranked snippets."
    plugin_dec = "Search your Obsidian vault quickly and return relevant snippets with optional AI synthesis."
    when_to_use = "Use when the user asks to find information in Obsidian notes."
    usage = (
        "{\n"
        '  "function": "obsidian_search",\n'
        '  "arguments": {\n'
        '    "query": "<keywords or question>",\n'
        '    "limit": 6,\n'
        '    "max_files": 120,\n'
        '    "ai_synthesis": true\n'
        "  }\n"
        "}\n"
    )
    required_args = ["query"]
    optional_args = ["user_question", "limit", "max_files", "max_note_chars", "max_snippet_chars", "ai_synthesis"]

    settings_category = "Obsidian"
    required_settings = {
        "OBSIDIAN_PROTOCOL": {
            "label": "Protocol",
            "type": "select",
            "default": "http",
            "options": ["http", "https"],
            "description": "Obsidian Local REST API protocol.",
        },
        "OBSIDIAN_HOST": {
            "label": "Host",
            "type": "string",
            "default": "127.0.0.1",
            "description": "Obsidian Local REST API host.",
        },
        "OBSIDIAN_PORT": {
            "label": "Port",
            "type": "number",
            "default": 27123,
            "description": "Obsidian Local REST API port.",
        },
        "OBSIDIAN_TOKEN": {
            "label": "Bearer Token",
            "type": "password",
            "default": "",
            "description": "Optional API token if your Obsidian API requires auth.",
        },
        "VERIFY_SSL": {
            "label": "Verify SSL Certs",
            "type": "checkbox",
            "default": False,
            "description": "Enable only for valid HTTPS certificates.",
        },
        "REQUEST_TIMEOUT_SECONDS": {
            "label": "HTTP Timeout (seconds)",
            "type": "number",
            "default": 12,
            "description": "Timeout for each Obsidian API request.",
        },
        "MAX_FILES_SCAN": {
            "label": "Max Notes Scanned",
            "type": "number",
            "default": 120,
            "description": "Upper bound on markdown files scanned per search request.",
        },
        "MAX_NOTE_CHARS": {
            "label": "Max Chars Per Note",
            "type": "number",
            "default": 24000,
            "description": "Per-note read cap used for relevance scanning.",
        },
        "MAX_RESULTS": {
            "label": "Max Returned Results",
            "type": "number",
            "default": 6,
            "description": "Maximum number of matching notes returned.",
        },
        "MAX_SNIPPET_CHARS": {
            "label": "Max Snippet Chars",
            "type": "number",
            "default": 320,
            "description": "Maximum snippet length shown per result.",
        },
        "ENABLE_AI_SYNTHESIS": {
            "label": "Enable AI Synthesis",
            "type": "checkbox",
            "default": True,
            "description": "When enabled, one LLM pass summarizes top matches.",
        },
    }

    waiting_prompt_template = (
        "Write a short message telling {mention} you are searching the Obsidian vault now. "
        "Only output that message."
    )

    platforms = ["webui"]

    _TOKEN_RE = re.compile(r"[a-z0-9_]+")
    _STOPWORDS = {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "how",
        "i",
        "in",
        "is",
        "it",
        "me",
        "my",
        "of",
        "on",
        "or",
        "that",
        "the",
        "to",
        "was",
        "what",
        "when",
        "where",
        "who",
        "why",
        "with",
    }

    def _settings(self) -> Dict[str, str]:
        return redis_client.hgetall(f"plugin_settings:{self.settings_category}") or {}

    def _config(self) -> Dict[str, Any]:
        settings = self._settings()
        protocol = str(settings.get("OBSIDIAN_PROTOCOL") or "http").strip().lower()
        if protocol not in {"http", "https"}:
            protocol = "http"
        host = str(settings.get("OBSIDIAN_HOST") or "127.0.0.1").strip() or "127.0.0.1"
        port = _as_int(settings.get("OBSIDIAN_PORT"), default=27123, min_value=1, max_value=65535)
        token = str(settings.get("OBSIDIAN_TOKEN") or "").strip()
        verify_ssl = _as_bool(settings.get("VERIFY_SSL"), default=False)
        timeout_seconds = _as_int(settings.get("REQUEST_TIMEOUT_SECONDS"), default=12, min_value=2, max_value=90)

        max_files_scan = _as_int(settings.get("MAX_FILES_SCAN"), default=120, min_value=1, max_value=3000)
        max_note_chars = _as_int(settings.get("MAX_NOTE_CHARS"), default=24000, min_value=500, max_value=240000)
        max_results = _as_int(settings.get("MAX_RESULTS"), default=6, min_value=1, max_value=20)
        max_snippet_chars = _as_int(settings.get("MAX_SNIPPET_CHARS"), default=320, min_value=80, max_value=2000)
        ai_synthesis = _as_bool(settings.get("ENABLE_AI_SYNTHESIS"), default=True)

        return {
            "base_url": f"{protocol}://{host}:{port}".rstrip("/"),
            "token": token,
            "verify_ssl": verify_ssl,
            "timeout": (3, timeout_seconds),
            "max_files_scan": max_files_scan,
            "max_note_chars": max_note_chars,
            "max_results": max_results,
            "max_snippet_chars": max_snippet_chars,
            "ai_synthesis": ai_synthesis,
        }

    def _headers(self, *, accept: str, token: str) -> Dict[str, str]:
        headers = {
            "User-Agent": "Tater-ObsidianSearch/2.0",
            "Accept": accept,
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @staticmethod
    def _safe_url_path(path: str) -> str:
        return quote(path, safe="/")

    def _query_terms(self, query: str) -> List[str]:
        tokens = [t for t in self._TOKEN_RE.findall((query or "").lower()) if len(t) >= 2]
        filtered: List[str] = []
        seen = set()
        for token in tokens:
            if token in self._STOPWORDS:
                continue
            if token in seen:
                continue
            seen.add(token)
            filtered.append(token)

        if filtered:
            return filtered[:14]

        out: List[str] = []
        seen.clear()
        for token in tokens:
            if token in seen:
                continue
            seen.add(token)
            out.append(token)
            if len(out) >= 14:
                break
        return out

    def _path_hint_score(self, path: str, terms: List[str]) -> int:
        lowered = (path or "").lower()
        score = 0
        for term in terms:
            if term in lowered:
                score += 3
        return score

    def _score_note(self, path: str, content: str, query: str, terms: List[str]) -> Tuple[int, List[str]]:
        lowered_path = (path or "").lower()
        lowered_content = (content or "").lower()
        lowered_query = (query or "").strip().lower()

        score = 0
        matched: List[str] = []
        seen = set()

        if lowered_query and lowered_query in lowered_content:
            score += 8

        for term in terms:
            term_hits = lowered_content.count(term)
            if term in lowered_path:
                score += 3
                if term not in seen:
                    seen.add(term)
                    matched.append(term)
            if term_hits:
                score += min(10, 2 + term_hits)
                if term not in seen:
                    seen.add(term)
                    matched.append(term)

        return score, matched

    def _build_snippet(self, content: str, query: str, terms: List[str], max_chars: int) -> str:
        if not content:
            return ""
        lower_content = content.lower()
        lower_query = (query or "").strip().lower()

        idx = -1
        if lower_query:
            idx = lower_content.find(lower_query)
        if idx < 0:
            for term in terms:
                idx = lower_content.find(term)
                if idx >= 0:
                    break
        if idx < 0:
            idx = 0

        half = max(20, max_chars // 2)
        start = max(0, idx - half)
        end = min(len(content), start + max_chars)
        snippet = content[start:end].strip()
        snippet = re.sub(r"\s+", " ", snippet)

        if start > 0:
            snippet = "... " + snippet
        if end < len(content):
            snippet = snippet + " ..."
        return snippet

    def _list_markdown_files(self, cfg: Dict[str, Any]) -> List[str]:
        out: List[str] = []
        seen = set()
        for suffix in ("/vault/", "/vault"):
            url = f"{cfg['base_url']}{suffix}"
            try:
                resp = requests.get(
                    url,
                    headers=self._headers(accept="application/json", token=cfg["token"]),
                    verify=cfg["verify_ssl"],
                    timeout=cfg["timeout"],
                )
            except Exception as exc:
                logger.warning("[Obsidian Search] vault list error from %s: %s", url, exc)
                continue

            if resp.status_code != 200:
                continue

            try:
                payload = resp.json()
            except Exception:
                continue

            files = payload.get("files") if isinstance(payload, dict) else payload
            if not isinstance(files, list):
                continue

            for item in files:
                path = ""
                if isinstance(item, str):
                    path = item
                elif isinstance(item, dict):
                    path = item.get("path") or item.get("file") or item.get("name") or ""
                if not path:
                    continue
                normalized = str(path).replace("\\", "/").strip().strip("/")
                if not normalized or not normalized.lower().endswith(".md"):
                    continue
                if normalized in seen:
                    continue
                seen.add(normalized)
                out.append(normalized)

            if out:
                break

        return sorted(out, key=lambda p: p.lower())

    def _read_markdown(self, path: str, cfg: Dict[str, Any]) -> Optional[str]:
        url = f"{cfg['base_url']}/vault/{self._safe_url_path(path)}"
        try:
            resp = requests.get(
                url,
                headers=self._headers(accept="text/markdown, text/plain;q=0.9, */*;q=0.1", token=cfg["token"]),
                verify=cfg["verify_ssl"],
                timeout=cfg["timeout"],
            )
        except Exception as exc:
            logger.debug("[Obsidian Search] read failed for %s: %s", path, exc)
            return None
        if resp.status_code != 200:
            return None
        return resp.text or ""

    def _scan_hits(
        self,
        *,
        query: str,
        terms: List[str],
        cfg: Dict[str, Any],
        max_files: int,
        max_note_chars: int,
        max_results: int,
        max_snippet_chars: int,
    ) -> Tuple[List[Dict[str, Any]], int, int]:
        markdown_files = self._list_markdown_files(cfg)
        if not markdown_files:
            return [], 0, 0

        ranked_paths = sorted(
            markdown_files,
            key=lambda path: (-self._path_hint_score(path, terms), path.lower()),
        )
        scan_paths = ranked_paths[:max_files]

        hits: List[Dict[str, Any]] = []
        for path in scan_paths:
            content = self._read_markdown(path, cfg)
            if not content:
                continue

            scanned = content[:max_note_chars]
            score, matched_terms = self._score_note(path, scanned, query, terms)
            if score <= 0:
                continue

            snippet = self._build_snippet(scanned, query, terms, max_chars=max_snippet_chars)
            hits.append(
                {
                    "path": path,
                    "score": score,
                    "matched_terms": matched_terms,
                    "snippet": snippet,
                }
            )

        hits.sort(key=lambda item: (-int(item.get("score", 0)), str(item.get("path", "")).lower()))
        return hits[:max_results], len(scan_paths), len(markdown_files)

    def _format_hits(self, query: str, hits: List[Dict[str, Any]], scanned_count: int, total_count: int) -> str:
        lines: List[str] = [
            f"Found {len(hits)} matching note(s) for `{query}`.",
            f"Scanned {scanned_count} of {total_count} markdown files.",
            "",
        ]
        for idx, hit in enumerate(hits, start=1):
            path = str(hit.get("path") or "unknown")
            score = int(hit.get("score") or 0)
            terms = ", ".join(hit.get("matched_terms") or [])
            snippet = str(hit.get("snippet") or "").strip()
            lines.append(f"{idx}. `{path}` (score {score})")
            if terms:
                lines.append(f"   matched terms: {terms}")
            if snippet:
                lines.append(f"   {snippet}")
            lines.append("")
        return "\n".join(lines).strip()

    async def _synthesize_answer(self, query: str, hits: List[Dict[str, Any]], llm_client: Any) -> str:
        if llm_client is None or not hits:
            return ""

        chunks: List[str] = []
        for hit in hits[:8]:
            path = str(hit.get("path") or "")
            snippet = str(hit.get("snippet") or "")
            terms = ", ".join(hit.get("matched_terms") or [])
            chunks.append(
                f"File: {path}\n"
                f"Matched terms: {terms or 'n/a'}\n"
                f"Snippet:\n{snippet}"
            )

        joined = "\n\n---\n\n".join(chunks)
        system = (
            "You answer using only provided Obsidian snippets. "
            "If evidence is incomplete, say that clearly."
        )
        user = (
            f"User query: {query}\n\n"
            "Use only these snippets to answer. Keep it concise and factual.\n"
            "Then include a short 'Sources' list with file paths.\n\n"
            f"{joined}"
        )

        try:
            resp = await llm_client.chat(
                messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
                timeout=25,
            )
            text = ((resp.get("message", {}) or {}).get("content", "") or "").strip()
            return text
        except Exception as exc:
            logger.warning("[Obsidian Search] synthesis failed: %s", exc)
            return ""

    async def handle_webui(self, args, llm_client):
        args = args or {}
        query = str(args.get("query") or args.get("user_question") or "").strip()
        if not query:
            return action_failure(
                code="missing_query",
                message="No query provided.",
                needs=["Please provide a `query` argument for Obsidian search."],
                say_hint="Ask the user what they want to find in Obsidian notes.",
            )

        cfg = self._config()
        terms = self._query_terms(query)

        max_results = _as_int(args.get("limit"), default=cfg["max_results"], min_value=1, max_value=20)
        max_files = _as_int(args.get("max_files"), default=cfg["max_files_scan"], min_value=1, max_value=3000)
        max_note_chars = _as_int(args.get("max_note_chars"), default=cfg["max_note_chars"], min_value=500, max_value=240000)
        max_snippet_chars = _as_int(
            args.get("max_snippet_chars"),
            default=cfg["max_snippet_chars"],
            min_value=80,
            max_value=2000,
        )
        ai_synthesis = _as_bool(args.get("ai_synthesis"), default=cfg["ai_synthesis"])

        hits, scanned_count, total_count = self._scan_hits(
            query=query,
            terms=terms,
            cfg=cfg,
            max_files=max_files,
            max_note_chars=max_note_chars,
            max_results=max_results,
            max_snippet_chars=max_snippet_chars,
        )

        if total_count == 0:
            return ["No markdown notes were found in the Obsidian vault."]

        if not hits:
            return [f"No matching notes found for `{query}` (scanned {scanned_count} of {total_count} files)."]

        synthesis = ""
        if ai_synthesis and llm_client is not None:
            synthesis = await self._synthesize_answer(query, hits, llm_client)

        if synthesis:
            return [synthesis]

        return [self._format_hits(query, hits, scanned_count, total_count)]

    async def handle_discord(self, message, args, llm_client):
        return "Obsidian search is only supported in the WebUI."

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return f"{user}: Obsidian search is only supported in the WebUI."


plugin = ObsidianSearchPlugin()
