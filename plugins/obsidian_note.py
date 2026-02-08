import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

from helpers import redis_client
from plugin_base import ToolPlugin
from plugin_result import action_failure

logger = logging.getLogger("obsidian_note")
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


class ObsidianNotePlugin(ToolPlugin):
    name = "obsidian_note"
    plugin_name = "Obsidian Note"
    pretty_name = "Add to Obsidian"
    version = "3.0.0"
    min_tater_version = "50"

    description = "Create, append, or overwrite markdown notes in your Obsidian vault with strict path safety."
    plugin_dec = "Save markdown content to Obsidian with predictable note naming, append/overwrite controls, and tags."
    when_to_use = "Use when the user wants to save text as an Obsidian note, append to a note, or overwrite a note."
    usage = (
        "{\n"
        '  "function": "obsidian_note",\n'
        '  "arguments": {\n'
        '    "content": "<markdown content>",\n'
        '    "title": "Optional note title if path is omitted",\n'
        '    "path": "Optional relative path like inbox/today.md",\n'
        '    "folder": "Optional folder prefix when path has no folder",\n'
        '    "append": false,\n'
        '    "overwrite": false,\n'
        '    "unique": true,\n'
        '    "separator": "\\n\\n",\n'
        '    "title_mode": "ai|first_line|timestamp",\n'
        '    "tags": ["project", "notes"]\n'
        "  }\n"
        "}\n"
    )
    required_args = ["content"]
    optional_args = [
        "title",
        "path",
        "folder",
        "append",
        "overwrite",
        "unique",
        "separator",
        "title_mode",
        "tags",
    ]

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
        "DEFAULT_NOTE_FOLDER": {
            "label": "Default Note Folder",
            "type": "string",
            "default": "",
            "description": "Relative folder used when `path` does not include one.",
        },
        "DEFAULT_TITLE_MODE": {
            "label": "Default Title Mode",
            "type": "select",
            "default": "ai",
            "options": ["ai", "first_line", "timestamp"],
            "description": "How note titles are generated when title/path are omitted.",
        },
        "UNIQUE_NAMES_DEFAULT": {
            "label": "Use Unique Names By Default",
            "type": "checkbox",
            "default": True,
            "description": "When enabled, existing file names get -2/-3 suffixes unless overwrite=true.",
        },
        "DEFAULT_TAGS": {
            "label": "Default Tags",
            "type": "textarea",
            "rows": 4,
            "default": "",
            "description": "Optional tags (comma/newline separated) added to newly created notes.",
            "placeholder": "project\nnotes",
        },
        "APPEND_SEPARATOR": {
            "label": "Default Append Separator",
            "type": "textarea",
            "rows": 3,
            "default": "\\n\\n",
            "description": "Separator inserted between old and new content when append=true.",
            "placeholder": "\\n\\n",
        },
    }

    waiting_prompt_template = (
        "Write a short, friendly message telling {mention} you are saving the Obsidian note now. "
        "Only output that message."
    )

    platforms = ["webui"]

    _INVALID_PATH_CHARS_RE = re.compile(r'[\\:*?"<>|]')
    _CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")

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

        default_folder, folder_err = self._normalize_relative_path(
            settings.get("DEFAULT_NOTE_FOLDER"),
            allow_empty=True,
        )
        if folder_err:
            default_folder = ""

        title_mode_raw = settings.get("DEFAULT_TITLE_MODE")
        if title_mode_raw is None:
            title_mode_raw = settings.get("TITLE_MODE")
        default_title_mode = str(title_mode_raw or "ai").strip().lower()
        if default_title_mode not in {"ai", "first_line", "timestamp"}:
            default_title_mode = "ai"

        unique_default = _as_bool(settings.get("UNIQUE_NAMES_DEFAULT"), default=True)
        default_tags = self._parse_tags(settings.get("DEFAULT_TAGS"))

        append_separator = str(settings.get("APPEND_SEPARATOR") if settings.get("APPEND_SEPARATOR") is not None else "\\n\\n")
        append_separator = append_separator.replace("\\n", "\n")
        if not append_separator:
            append_separator = "\n\n"

        return {
            "base_url": f"{protocol}://{host}:{port}".rstrip("/"),
            "token": token,
            "verify_ssl": verify_ssl,
            "timeout": (3, timeout_seconds),
            "default_folder": default_folder,
            "default_title_mode": default_title_mode,
            "unique_default": unique_default,
            "default_tags": default_tags,
            "append_separator": append_separator,
        }

    def _headers(self, *, accept: str, content_type: Optional[str], token: str) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "User-Agent": "Tater-ObsidianNote/3.0",
            "Accept": accept,
        }
        if content_type:
            headers["Content-Type"] = content_type
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @staticmethod
    def _safe_url_path(note_path: str) -> str:
        return quote(note_path, safe="/")

    def _request_note(
        self,
        *,
        method: str,
        note_path: str,
        cfg: Dict[str, Any],
        accept: str,
        content_type: Optional[str] = None,
        data: Optional[bytes] = None,
    ) -> Tuple[Optional[requests.Response], Optional[str]]:
        url = f"{cfg['base_url']}/vault/{self._safe_url_path(note_path)}"
        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                headers=self._headers(accept=accept, content_type=content_type, token=cfg["token"]),
                data=data,
                verify=cfg["verify_ssl"],
                timeout=cfg["timeout"],
            )
            return resp, None
        except Exception as exc:
            return None, f"Obsidian API request failed: {exc}"

    def _note_exists(self, note_path: str, cfg: Dict[str, Any]) -> bool:
        resp, err = self._request_note(
            method="GET",
            note_path=note_path,
            cfg=cfg,
            accept="application/json, text/markdown;q=0.9, */*;q=0.1",
        )
        if err or resp is None:
            return False
        return resp.status_code == 200

    def _read_note(self, note_path: str, cfg: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        resp, err = self._request_note(
            method="GET",
            note_path=note_path,
            cfg=cfg,
            accept="text/markdown, text/plain;q=0.9, */*;q=0.1",
        )
        if err:
            return None, err
        if resp is None:
            return None, "Obsidian read request failed."
        if resp.status_code != 200:
            body = (resp.text or "").strip()[:220]
            return None, f"Obsidian read failed (HTTP {resp.status_code}): {body}"
        return resp.text or "", None

    def _write_note(self, note_path: str, content: str, cfg: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        resp, err = self._request_note(
            method="PUT",
            note_path=note_path,
            cfg=cfg,
            accept="*/*",
            content_type="text/markdown; charset=utf-8",
            data=(content or "").encode("utf-8"),
        )
        if err:
            return False, err
        if resp is None:
            return False, "Obsidian write request failed."
        if resp.status_code in {200, 201, 204}:
            return True, None
        body = (resp.text or "").strip()[:220]
        return False, f"Obsidian write failed (HTTP {resp.status_code}): {body}"

    def _sanitize_segment(self, value: Any) -> str:
        text = str(value or "").strip()
        text = self._CONTROL_CHARS_RE.sub("", text)
        text = text.replace("\t", " ")
        text = self._INVALID_PATH_CHARS_RE.sub("", text)
        text = text.strip().strip(".")
        text = re.sub(r"\s+", " ", text)
        if len(text) > 120:
            text = text[:120].rstrip()
        return text

    def _normalize_relative_path(self, raw_path: Any, *, allow_empty: bool) -> Tuple[str, Optional[str]]:
        path = str(raw_path or "").replace("\\", "/").strip()
        if not path:
            return ("", None) if allow_empty else ("", "Path is empty.")

        path = path.strip("/")
        parts: List[str] = []
        for raw_part in path.split("/"):
            part = raw_part.strip()
            if not part or part == ".":
                continue
            if part == "..":
                return "", "Path cannot contain '..' segments."
            safe = self._sanitize_segment(part)
            if not safe:
                return "", "Path contains invalid segments."
            parts.append(safe)

        if not parts:
            return ("", None) if allow_empty else ("", "Path contains no valid segments.")

        normalized = "/".join(parts)
        if len(normalized) > 260:
            return "", "Path is too long."
        return normalized, None

    def _normalize_note_path(self, raw_path: Any) -> Tuple[Optional[str], Optional[str]]:
        normalized, err = self._normalize_relative_path(raw_path, allow_empty=False)
        if err:
            return None, err
        if not normalized.lower().endswith(".md"):
            normalized = f"{normalized}.md"
        return normalized, None

    @staticmethod
    def _split_path(note_path: str) -> Tuple[str, str, str]:
        if "/" in note_path:
            folder, filename = note_path.rsplit("/", 1)
        else:
            folder, filename = "", note_path
        stem = filename[:-3] if filename.lower().endswith(".md") else filename
        return folder, stem, ".md"

    def _ensure_unique_path(self, note_path: str, cfg: Dict[str, Any]) -> str:
        if not self._note_exists(note_path, cfg):
            return note_path

        folder, stem, ext = self._split_path(note_path)
        for idx in range(2, 500):
            candidate_name = f"{stem}-{idx}{ext}"
            candidate_path = f"{folder}/{candidate_name}" if folder else candidate_name
            if not self._note_exists(candidate_path, cfg):
                return candidate_path

        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        fallback = f"{stem}-{stamp}{ext}"
        return f"{folder}/{fallback}" if folder else fallback

    def _extract_first_line_title(self, content: str) -> str:
        for line in (content or "").splitlines():
            text = line.strip()
            if not text:
                continue
            if text.startswith("#"):
                text = re.sub(r"^#+\s*", "", text).strip()
            return self._sanitize_segment(text)
        return ""

    async def _generate_ai_title(self, content: str, llm_client: Any) -> str:
        if llm_client is None:
            return ""
        prompt = (
            "Generate a concise Obsidian note title for the content below.\n"
            "Rules:\n"
            "- 3 to 8 words\n"
            "- no trailing punctuation\n"
            "- output title text only\n\n"
            "Content:\n"
            f"{(content or '')[:3500]}"
        )
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": "You write concise note titles."},
                    {"role": "user", "content": prompt},
                ]
            )
            raw = ((resp.get("message", {}) or {}).get("content", "") or "").strip()
            return self._sanitize_segment(raw)
        except Exception:
            return ""

    async def _resolve_title(self, content: str, args: Dict[str, Any], cfg: Dict[str, Any], llm_client: Any) -> str:
        explicit = self._sanitize_segment(args.get("title"))
        if explicit:
            return explicit

        mode = str(args.get("title_mode") or cfg.get("default_title_mode") or "ai").strip().lower()
        if mode not in {"ai", "first_line", "timestamp"}:
            mode = "ai"

        if mode == "ai":
            ai_title = await self._generate_ai_title(content, llm_client)
            if ai_title:
                return ai_title
            mode = "first_line"

        if mode == "first_line":
            first_line = self._extract_first_line_title(content)
            if first_line:
                return first_line

        return datetime.now().strftime("Note %Y-%m-%d %H-%M-%S")

    def _parse_tags(self, raw: Any) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, list):
            items = [str(item).strip() for item in raw]
        else:
            text = str(raw).replace(",", "\n")
            items = [item.strip() for item in text.splitlines()]

        out: List[str] = []
        seen = set()
        for item in items:
            if not item:
                continue
            tag = item.lstrip("#").strip()
            tag = re.sub(r"\s+", "-", tag)
            tag = re.sub(r"[^A-Za-z0-9_\-/]", "", tag)
            if not tag or tag in seen:
                continue
            seen.add(tag)
            out.append(tag)
        return out

    def _add_frontmatter_tags(self, content: str, tags: List[str]) -> str:
        if not tags:
            return content
        stripped = (content or "").lstrip()
        if stripped.startswith("---"):
            return content
        lines = ["---", "tags:"] + [f"  - {tag}" for tag in tags] + ["---", ""]
        return "\n".join(lines) + (content or "")

    async def _save_note(self, args: Dict[str, Any], llm_client: Any) -> Any:
        cfg = self._config()
        payload_content = str((args or {}).get("content") or "").strip()
        if not payload_content:
            return action_failure(
                code="missing_content",
                message="No note content provided.",
                needs=["Please provide `content` with the note text to save."],
                say_hint="Ask the user for the note content.",
            )

        append = _as_bool((args or {}).get("append"), default=False)
        overwrite = _as_bool((args or {}).get("overwrite"), default=False)
        unique = _as_bool((args or {}).get("unique"), default=cfg["unique_default"])
        if append and overwrite:
            return action_failure(
                code="invalid_mode",
                message="`append` and `overwrite` cannot both be true.",
                needs=["Use append=true to add content, or overwrite=true to replace the note."],
                say_hint="Explain the mode conflict and ask which behavior they want.",
            )

        folder_arg = (args or {}).get("folder")
        folder_source = folder_arg if folder_arg is not None else cfg["default_folder"]
        folder, folder_err = self._normalize_relative_path(folder_source, allow_empty=True)
        if folder_err:
            return action_failure(
                code="invalid_folder",
                message=folder_err,
                say_hint="Explain that folder paths must be relative and cannot contain '..'.",
            )

        raw_path = str((args or {}).get("path") or "").strip()
        if raw_path:
            note_path, path_err = self._normalize_note_path(raw_path)
            if path_err:
                return action_failure(
                    code="invalid_path",
                    message=path_err,
                    say_hint="Explain why the provided note path is invalid.",
                )
            if folder and note_path and "/" not in note_path:
                note_path = f"{folder}/{note_path}"
        else:
            title = await self._resolve_title(payload_content, args or {}, cfg, llm_client)
            safe_title = self._sanitize_segment(title)
            if not safe_title:
                safe_title = datetime.now().strftime("Note %Y-%m-%d %H-%M-%S")
            auto_path = f"{safe_title}.md"
            if folder:
                auto_path = f"{folder}/{auto_path}"
            note_path, path_err = self._normalize_note_path(auto_path)
            if path_err:
                return action_failure(
                    code="invalid_path",
                    message=path_err,
                    say_hint="Explain why the generated note path is invalid.",
                )

        if not note_path:
            return action_failure(
                code="invalid_path",
                message="Unable to resolve a valid note path.",
                say_hint="Ask for a valid path or title.",
            )

        exists = self._note_exists(note_path, cfg)
        operation = "create"
        payload = payload_content

        separator_value = (args or {}).get("separator")
        separator = str(separator_value if separator_value is not None else cfg["append_separator"])
        separator = separator.replace("\\n", "\n")
        if separator == "":
            separator = "\n\n"

        if append:
            if exists:
                existing, read_err = self._read_note(note_path, cfg)
                if read_err:
                    return action_failure(
                        code="read_failed",
                        message=read_err,
                        say_hint="Explain the read failure and suggest checking Obsidian API settings.",
                    )
                payload = (existing or "").rstrip() + separator + payload_content
                operation = "append"
            else:
                operation = "create"
        elif exists:
            if overwrite:
                operation = "overwrite"
            elif unique:
                note_path = self._ensure_unique_path(note_path, cfg)
                operation = "create"
            else:
                return action_failure(
                    code="note_exists",
                    message=f"Note already exists: `{note_path}`.",
                    needs=[
                        "Set `overwrite=true` to replace it.",
                        "Set `append=true` to append to it.",
                        "Set `unique=true` to create a new file name.",
                    ],
                    say_hint="Ask whether to overwrite, append, or create a unique name.",
                )

        tags = self._parse_tags((args or {}).get("tags"))
        if operation == "create" and not tags and cfg["default_tags"]:
            tags = list(cfg["default_tags"])
        if operation == "create" and tags:
            payload = self._add_frontmatter_tags(payload, tags)

        ok, write_err = self._write_note(note_path, payload, cfg)
        if not ok:
            return action_failure(
                code="write_failed",
                message=write_err or "Obsidian write failed.",
                say_hint="Explain the write error and suggest checking Obsidian host/port/token settings.",
            )

        tag_suffix = f" (tags: {', '.join(tags)})" if tags and operation == "create" else ""
        return [f"Saved Obsidian note ({operation}): `{note_path}`{tag_suffix}"]

    async def handle_webui(self, args, llm_client):
        return await self._save_note(args or {}, llm_client)

    async def handle_discord(self, message, args, llm_client):
        return "Obsidian note editing is only supported in the WebUI."

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return f"{user}: Obsidian note editing is only supported in the WebUI."


plugin = ObsidianNotePlugin()
