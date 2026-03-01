# telegram_platform.py
import asyncio
import html
import json
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager, suppress
from types import SimpleNamespace
from typing import Any, Dict, List

import redis
import requests
from dotenv import load_dotenv

import plugin_registry as pr
from plugin_base import ToolPlugin
from notify.media import load_queue_attachments
from notify.queue import is_expired
from helpers import (
    get_llm_client_from_env,
    build_llm_host_from_env,
)
from admin_gate import (
    is_admin_only_plugin,
)
from plugin_result import action_failure
from plugin_kernel import plugin_supports_platform
from cerberus import run_cerberus_turn, resolve_agent_limits
from emoji_responder import emoji_responder

load_dotenv()

logger = logging.getLogger("telegram")
logging.basicConfig(level=logging.INFO)

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True,
)
blob_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=False,
)

NOTIFY_QUEUE_KEY = "notifyq:telegram"
NOTIFY_POLL_INTERVAL = 0.5
TELEGRAM_TEXT_LIMIT = 4096
TELEGRAM_TYPING_PULSE_SECONDS = 4.0
TELEGRAM_CHAT_LOOKUP_HASH = "tater:telegram:chat_lookup"
TELEGRAM_BLOB_PREFIX = "tater:blob:telegram"
ROOM_LABEL_PREFIX = "tater:room_label"
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^\s)]+)\)")
_MD_FENCED_CODE_RE = re.compile(r"```(?:[A-Za-z0-9_+\-]+)?\n?([\s\S]*?)```")
_MD_INLINE_CODE_RE = re.compile(r"`([^`\n]+)`")

PLATFORM_SETTINGS = {
    "category": "Telegram Settings",
    "required": {
        "allowed_user": {
            "label": "Allowed DM User",
            "type": "string",
            "default": "",
            "description": "Private DM replies are disabled unless this is set. Use user id or @username (comma-separated supported).",
        },
        "telegram_bot_token": {
            "label": "Telegram Bot Token",
            "type": "string",
            "default": "",
            "description": "Bot token from @BotFather.",
        },
        "allowed_chat_id": {
            "label": "Allowed Chat ID",
            "type": "string",
            "default": "",
            "description": "Optional: only this chat ID can interact with the bot.",
        },
        "response_chat_id": {
            "label": "Default Response Chat ID",
            "type": "string",
            "default": "",
            "description": "Fallback chat ID for queued notifications.",
        },
        "poll_timeout_sec": {
            "label": "Poll Timeout (sec)",
            "type": "number",
            "default": 20,
            "description": "Long-poll timeout for incoming updates.",
        },
    },
}


def _stop_requested(stop_event) -> bool:
    return bool(stop_event and getattr(stop_event, "is_set", lambda: False)())


def _history_key(chat_id: str) -> str:
    return f"tater:telegram:{chat_id}:history"


def _blob_key() -> str:
    return f"{TELEGRAM_BLOB_PREFIX}:{uuid.uuid4().hex}"


def _store_blob(binary: bytes, ttl_seconds: int = 60 * 60 * 24 * 7) -> str:
    key = _blob_key()
    blob_client.set(key.encode("utf-8"), binary)
    if ttl_seconds and ttl_seconds > 0:
        blob_client.expire(key.encode("utf-8"), int(ttl_seconds))
    return key


def _normalize_chat_id(value: Any) -> str:
    raw = str(value or "").strip()
    if raw.startswith("#"):
        raw = raw[1:].strip()
    return raw


def _room_label_key(platform: str, room_id: Any) -> str:
    platform_name = str(platform or "").strip().lower() or "unknown"
    scope_id = str(room_id or "").strip()
    if not scope_id:
        return ""
    return f"{ROOM_LABEL_PREFIX}:{platform_name}:{scope_id}"


def _save_room_label(platform: str, room_id: Any, label: Any) -> None:
    key = _room_label_key(platform, room_id)
    label_text = str(label or "").strip()
    if not key or not label_text:
        return
    try:
        redis_client.set(key, label_text)
    except Exception:
        pass


def _telegram_room_label(chat: Dict[str, Any], sender: Dict[str, Any]) -> str:
    chat_type = str((chat or {}).get("type") or "").strip().lower()
    title = str((chat or {}).get("title") or "").strip()
    username = str((chat or {}).get("username") or "").strip()
    if username and not username.startswith("@"):
        username = f"@{username}"

    if chat_type == "private":
        sender_username = str((sender or {}).get("username") or "").strip()
        first = str((sender or {}).get("first_name") or "").strip()
        last = str((sender or {}).get("last_name") or "").strip()
        full_name = " ".join([part for part in (first, last) if part]).strip()
        if sender_username:
            return f"@{sender_username}" if not sender_username.startswith("@") else sender_username
        if full_name:
            return full_name
        user_id = str((sender or {}).get("id") or "").strip()
        return f"tg_{user_id}" if user_id else "DM"

    if title:
        return f"#{title}" if not title.startswith("#") else title
    if username:
        return username
    return ""


def _chat_lookup_key(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw.startswith("#") or raw.startswith("@"):
        raw = raw[1:].strip()
    return re.sub(r"\s+", " ", raw).strip()


def _remember_chat_lookup(chat: Dict[str, Any], sender: Dict[str, Any]) -> None:
    chat_id = _normalize_chat_id(chat.get("id"))
    if not chat_id:
        return

    keys = set()
    title = str(chat.get("title") or "").strip()
    username = str(chat.get("username") or "").strip()
    if title:
        keys.add(_chat_lookup_key(title))
    if username:
        keys.add(_chat_lookup_key(username))

    if str(chat.get("type") or "").strip().lower() == "private":
        sender_username = str((sender or {}).get("username") or "").strip()
        first = str((sender or {}).get("first_name") or "").strip()
        last = str((sender or {}).get("last_name") or "").strip()
        full_name = " ".join([part for part in (first, last) if part]).strip()
        for val in (sender_username, first, last, full_name):
            if val:
                keys.add(_chat_lookup_key(val))

    for key in keys:
        if key:
            try:
                redis_client.hset(TELEGRAM_CHAT_LOOKUP_HASH, key, chat_id)
            except Exception:
                pass

    label = _telegram_room_label(chat, sender)
    if label:
        _save_room_label("telegram", chat_id, label)


def _resolve_chat_target(value: Any) -> str:
    ref = _normalize_chat_id(value)
    if not ref:
        return ""

    if ref.startswith("@"):
        return ref
    if ref.isdigit() or (ref.startswith("-") and ref[1:].isdigit()):
        return ref

    lookup_key = _chat_lookup_key(ref)
    if lookup_key:
        try:
            mapped = redis_client.hget(TELEGRAM_CHAT_LOOKUP_HASH, lookup_key)
        except Exception:
            mapped = None
        if mapped:
            return _normalize_chat_id(mapped)

    for prefix in ("room ", "channel ", "chat ", "group "):
        if lookup_key.startswith(prefix):
            trimmed = lookup_key[len(prefix):].strip()
            if not trimmed:
                continue
            try:
                mapped = redis_client.hget(TELEGRAM_CHAT_LOOKUP_HASH, trimmed)
            except Exception:
                mapped = None
            if mapped:
                return _normalize_chat_id(mapped)

    if " " not in ref:
        return f"@{ref}"
    return ref


def _normalize_user_ref(value: Any) -> str:
    raw = str(value or "").strip()
    if raw.startswith("@"):
        raw = raw[1:].strip()
    return raw.lower()


def _telegram_sender_identity(message: Dict[str, Any]) -> Dict[str, str]:
    sender = message.get("from") or {}
    sender_chat = message.get("sender_chat") or {}

    username = str(sender.get("username") or "").strip()
    first = str(sender.get("first_name") or "").strip()
    last = str(sender.get("last_name") or "").strip()
    full_name = " ".join([part for part in (first, last) if part]).strip()

    sender_chat_title = str(sender_chat.get("title") or "").strip()
    sender_chat_username = str(sender_chat.get("username") or "").strip()

    user_id = str(
        sender.get("id")
        or sender_chat.get("id")
        or ""
    ).strip()

    user_handle = ""
    if username:
        user_handle = username
    elif sender_chat_username:
        user_handle = sender_chat_username
    if user_handle and not user_handle.startswith("@"):
        user_handle = f"@{user_handle}"

    display_name = (
        username
        or full_name
        or sender_chat_title
        or sender_chat_username
        or "telegram_user"
    )
    if display_name == "telegram_user" and user_id:
        display_name = f"telegram_user_{user_id}"

    return {
        "display_name": str(display_name or "telegram_user").strip() or "telegram_user",
        "user_id": user_id,
        "user_handle": user_handle,
    }


def _get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")


def _is_custom_handler(plugin: Any, method_name: str) -> bool:
    method = getattr(plugin, method_name, None)
    if not callable(method):
        return False
    base = getattr(ToolPlugin, method_name, None)
    impl = getattr(plugin.__class__, method_name, None)
    return impl is not None and impl is not base


def _to_template_msg(role, content, sender=None):
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        if content.get("phase", "final") != "final":
            return None
        payload = content.get("content")

        if isinstance(payload, str):
            txt = payload.strip()
            if len(txt) > 4000:
                txt = txt[:4000] + " ..."
            return {"role": "assistant", "content": txt}

        if isinstance(payload, dict) and payload.get("type") in ("image", "audio", "video", "file"):
            kind = payload.get("type").capitalize()
            name = payload.get("name") or ""
            return {
                "role": "assistant",
                "content": f"[{kind} from tool]{f' {name}' if name else ''}".strip(),
            }

        try:
            compact = json.dumps(payload, ensure_ascii=False)
            if len(compact) > 2000:
                compact = compact[:2000] + " ..."
            return {"role": "assistant", "content": compact}
        except Exception:
            return None

    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps(
            {
                "function": content.get("plugin"),
                "arguments": content.get("arguments", {}),
            },
            indent=2,
        )
        return {"role": "assistant" if role == "assistant" else role, "content": as_text}

    if isinstance(content, dict) and content.get("type") in ("image", "audio", "video", "file"):
        kind = str(content.get("type")).capitalize()
        name = content.get("name") or ""
        return {"role": role, "content": f"[{kind} attached]{f' {name}' if name else ''}".strip()}

    if isinstance(content, str):
        if role == "user" and sender:
            return {"role": "user", "content": f"{sender}: {content}"}
        return {"role": role, "content": content}

    return {"role": role, "content": str(content)}


def _enforce_user_assistant_alternation(loop_messages):
    merged = []
    for message in loop_messages:
        if not message:
            continue
        if not merged:
            merged.append(message)
            continue
        if merged[-1]["role"] == message["role"]:
            a, b = merged[-1]["content"], message["content"]
            if isinstance(a, str) and isinstance(b, str):
                merged[-1]["content"] = (a + "\n\n" + b).strip()
            elif isinstance(a, list) and isinstance(b, list):
                merged[-1]["content"] = a + b
            else:
                merged[-1]["content"] = (str(a) + "\n\n" + str(b)).strip()
        else:
            merged.append(message)
    return merged


def _message_text(message: Dict[str, Any]) -> str:
    text = str(message.get("text") or message.get("caption") or "").strip()
    if text:
        return text
    if message.get("photo"):
        return "[Image attached]"
    if message.get("video"):
        return "[Video attached]"
    if message.get("audio"):
        return "[Audio attached]"
    if message.get("voice"):
        return "[Voice message attached]"
    if message.get("document"):
        return "[File attached]"
    if message.get("sticker"):
        return "[Sticker]"
    return ""


def _telegram_plain_text(text: Any) -> str:
    out = str(text or "")
    if not out:
        return ""

    out = out.replace("\r\n", "\n").replace("\r", "\n")

    # Code fences / inline code -> raw text
    out = re.sub(r"```(?:[A-Za-z0-9_+\-]+)?\n?([\s\S]*?)```", lambda m: str(m.group(1) or "").strip(), out)
    out = re.sub(r"`([^`\n]+)`", r"\1", out)

    # Markdown links -> label (url)
    out = _MD_LINK_RE.sub(lambda m: f"{m.group(1)} ({m.group(2)})", out)

    # Common markdown emphasis markers
    out = re.sub(r"\*\*([^*\n]+)\*\*", r"\1", out)
    out = re.sub(r"__([^_\n]+)__", r"\1", out)
    out = re.sub(r"~~([^~\n]+)~~", r"\1", out)
    out = re.sub(r"\|\|([^|\n]+)\|\|", r"\1", out)
    out = re.sub(r"(?<![\w*])\*([^*\n]+)\*(?![\w*])", r"\1", out)
    out = re.sub(r"(?<![\w_])_([^_\n]+)_(?![\w_])", r"\1", out)

    # Headings/quotes are plain text on Telegram in this integration.
    out = re.sub(r"(?m)^\s{0,3}#{1,6}\s+", "", out)
    out = re.sub(r"(?m)^\s*>\s?", "", out)

    out = re.sub(r"[ \t]+\n", "\n", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


def _telegram_html_text(text: Any) -> str:
    raw = str(text or "")
    if not raw.strip():
        return ""
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    tokens: Dict[str, str] = {}
    token_idx = 0

    def _token(value: str) -> str:
        nonlocal token_idx
        key = f"TGFMTTOKEN{token_idx}TOKEN"
        token_idx += 1
        tokens[key] = value
        return key

    def _code_block_repl(match: re.Match[str]) -> str:
        code = str(match.group(1) or "").strip("\n")
        return _token(f"<pre>{html.escape(code)}</pre>")

    raw = _MD_FENCED_CODE_RE.sub(_code_block_repl, raw)

    def _inline_code_repl(match: re.Match[str]) -> str:
        return _token(f"<code>{html.escape(str(match.group(1) or ''))}</code>")

    raw = _MD_INLINE_CODE_RE.sub(_inline_code_repl, raw)

    def _link_repl(match: re.Match[str]) -> str:
        label = html.escape(str(match.group(1) or "").strip())
        url = html.escape(str(match.group(2) or "").strip(), quote=True)
        if not label or not url:
            return match.group(0)
        return _token(f'<a href="{url}">{label}</a>')

    raw = _MD_LINK_RE.sub(_link_repl, raw)
    out = html.escape(raw)

    # Convert common markdown markers into Telegram HTML tags.
    out = re.sub(r"\*\*([^\n*][^*\n]*?)\*\*", r"<b>\1</b>", out)
    out = re.sub(r"__([^_\n]+)__", r"<u>\1</u>", out)
    out = re.sub(r"~~([^~\n]+)~~", r"<s>\1</s>", out)
    out = re.sub(r"\|\|([^|\n]+)\|\|", r"<tg-spoiler>\1</tg-spoiler>", out)
    out = re.sub(r"(?<![\w*])\*([^*\n]+)\*(?![\w*])", r"<i>\1</i>", out)
    out = re.sub(r"(?<![\w_])_([^_\n]+)_(?![\w_])", r"<i>\1</i>", out)
    out = re.sub(r"(?m)^\s{0,3}#{1,6}\s+(.+)$", r"<b>\1</b>", out)

    for key, value in tokens.items():
        out = out.replace(key, value)

    out = re.sub(r"[ \t]+\n", "\n", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


class _TelegramMessageAdapter:
    class _NoopTyping:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _ChannelAdapter:
        def __init__(self, channel_id: Any, sink: List[Any]):
            self.id = channel_id
            self.name = str(channel_id)
            self._sink = sink

        def typing(self):
            return _TelegramMessageAdapter._NoopTyping()

        async def send(self, content=None, **kwargs):
            text = content if content is not None else kwargs.get("content")
            if isinstance(text, str) and text.strip():
                self._sink.append(text)
            return None

    def __init__(self, message: Dict[str, Any], chat_id: str, username: str):
        message_id = message.get("message_id")
        if not isinstance(message_id, int):
            message_id = 0
        sender = message.get("from") or {}
        author_id = sender.get("id")

        name = str(username or "telegram_user").strip()
        if not name:
            name = "telegram_user"
        mention = f"@{name}" if not name.startswith("@") else name

        channel_id: Any = chat_id
        cid = str(chat_id or "").strip()
        if cid and cid.lstrip("-").isdigit():
            try:
                channel_id = int(cid)
            except Exception:
                channel_id = cid

        self.id = message_id
        self.content = _message_text(message)
        self.attachments: List[Dict[str, Any]] = []
        self._sent_messages: List[Any] = []
        self.author = SimpleNamespace(
            id=author_id,
            name=name,
            display_name=name,
            mention=mention,
        )
        self.channel = _TelegramMessageAdapter._ChannelAdapter(channel_id, self._sent_messages)
        self.guild = None

    def drain_sent_messages(self) -> List[Any]:
        out = list(self._sent_messages)
        self._sent_messages.clear()
        return out


class TelegramPlatform:
    def __init__(
        self,
        token: str,
        llm_client,
        allowed_chat_id: str = "",
        allowed_user: str = "",
        response_chat_id: str = "",
        poll_timeout_sec: int = 20,
    ):
        self.token = str(token or "").strip()
        self.llm = llm_client
        self.poll_timeout_sec = max(1, int(poll_timeout_sec))
        self.response_chat_id = _normalize_chat_id(response_chat_id)
        self.offset = None
        self._typing_warn_at = 0.0

        allowed = str(allowed_chat_id or "").strip()
        normalized_allowed = [_normalize_chat_id(item) for item in allowed.split(",")]
        self.allowed_chat_ids = {item for item in normalized_allowed if item}

        allowed_user_raw = str(allowed_user or "").strip()
        normalized_users = [_normalize_user_ref(item) for item in allowed_user_raw.split(",")]
        self.allowed_dm_users = {item for item in normalized_users if item}

    def _api_url(self, method: str) -> str:
        return f"https://api.telegram.org/bot{self.token}/{method}"

    def _api_json(self, method: str, payload: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
        resp = requests.post(self._api_url(method), json=payload, timeout=timeout)
        if resp.status_code >= 300:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(str(data.get("description") or "Unknown Telegram API error"))
        return data

    async def _send_typing_action(self, chat_id: str) -> None:
        if not chat_id:
            return
        try:
            await asyncio.to_thread(
                self._api_json,
                "sendChatAction",
                {"chat_id": chat_id, "action": "typing"},
                15,
            )
        except Exception as exc:
            now = time.time()
            if now >= self._typing_warn_at:
                logger.warning("[Telegram] typing indicator failed: %s", exc)
                self._typing_warn_at = now + 60.0

    async def _typing_pulse(self, chat_id: str) -> None:
        while True:
            await asyncio.sleep(TELEGRAM_TYPING_PULSE_SECONDS)
            await self._send_typing_action(chat_id)

    @asynccontextmanager
    async def _safe_typing(self, chat_id: str):
        task = None
        try:
            await self._send_typing_action(chat_id)
            task = asyncio.create_task(self._typing_pulse(chat_id))
            yield
        finally:
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    def _api_multipart(
        self,
        method: str,
        payload: Dict[str, Any],
        files: Dict[str, Any],
        timeout: int = 30,
    ) -> Dict[str, Any]:
        resp = requests.post(self._api_url(method), data=payload, files=files, timeout=timeout)
        if resp.status_code >= 300:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(str(data.get("description") or "Unknown Telegram API error"))
        return data

    def _download_file_bytes(self, file_id: str, timeout: int = 30) -> bytes:
        resp = self._api_json("getFile", {"file_id": file_id}, timeout)
        result = resp.get("result") or {}
        file_path = str(result.get("file_path") or "").strip()
        if not file_path:
            raise RuntimeError("Telegram getFile response missing file_path.")
        file_url = f"https://api.telegram.org/file/bot{self.token}/{file_path}"
        dl = requests.get(file_url, timeout=timeout)
        if dl.status_code >= 300:
            raise RuntimeError(f"Telegram file download failed (HTTP {dl.status_code}).")
        data = dl.content or b""
        if not data:
            raise RuntimeError("Telegram file download returned no data.")
        return bytes(data)

    def _capture_incoming_artifacts_sync(self, message: Dict[str, Any], chat_id: str) -> Dict[str, Any]:
        candidates: List[Dict[str, str]] = []

        photos = message.get("photo")
        if isinstance(photos, list) and photos:
            best = photos[-1] if isinstance(photos[-1], dict) else {}
            file_id = str(best.get("file_id") or "").strip()
            if file_id:
                candidates.append(
                    {
                        "type": "image",
                        "file_id": file_id,
                        "name": str((message.get("caption") or "")).strip() or "telegram_photo.jpg",
                        "mimetype": "image/jpeg",
                    }
                )

        for key, media_type, default_name, default_mime in (
            ("video", "video", "telegram_video.mp4", "video/mp4"),
            ("audio", "audio", "telegram_audio.mp3", "audio/mpeg"),
            ("voice", "audio", "telegram_voice.ogg", "audio/ogg"),
            ("document", "file", "telegram_file", "application/octet-stream"),
        ):
            obj = message.get(key) or {}
            if not isinstance(obj, dict):
                continue
            file_id = str(obj.get("file_id") or "").strip()
            if not file_id:
                continue
            mime = str(obj.get("mime_type") or "").strip().lower() or default_mime
            entry_type = media_type
            if key == "document" and mime.startswith("image/"):
                entry_type = "image"
            candidates.append(
                {
                    "type": entry_type,
                    "file_id": file_id,
                    "name": str(obj.get("file_name") or "").strip() or default_name,
                    "mimetype": mime,
                }
            )

        refs: List[Dict[str, Any]] = []
        for candidate in candidates:
            file_id = str(candidate.get("file_id") or "").strip()
            try:
                binary = self._download_file_bytes(file_id, timeout=30)
                blob_key = _store_blob(binary)
            except Exception as e:
                logger.warning(f"[Telegram] Failed to capture media for chat {chat_id}: {e}")
                continue
            ref = {
                "type": str(candidate.get("type") or "file").strip().lower() or "file",
                "blob_key": blob_key,
                "name": str(candidate.get("name") or "telegram_file").strip() or "telegram_file",
                "mimetype": str(candidate.get("mimetype") or "application/octet-stream").strip() or "application/octet-stream",
                "source": "telegram_attachment",
                "size": len(binary),
            }
            refs.append(ref)
        return {"input_artifacts": refs}

    async def _send_text(self, chat_id: str, text: str):
        content = str(text or "").strip()
        if not content:
            return
        for idx in range(0, len(content), TELEGRAM_TEXT_LIMIT):
            raw_chunk = content[idx : idx + TELEGRAM_TEXT_LIMIT]
            html_chunk = _telegram_html_text(raw_chunk)
            payload = {
                "chat_id": chat_id,
                "text": html_chunk or _telegram_plain_text(raw_chunk),
                "disable_web_page_preview": False,
            }
            if html_chunk:
                payload["parse_mode"] = "HTML"
            try:
                await asyncio.to_thread(self._api_json, "sendMessage", payload, 20)
            except Exception:
                fallback = _telegram_plain_text(raw_chunk)
                if not fallback:
                    continue
                await asyncio.to_thread(
                    self._api_json,
                    "sendMessage",
                    {"chat_id": chat_id, "text": fallback, "disable_web_page_preview": False},
                    20,
                )

    async def _set_message_reaction(self, chat_id: str, message_id: int, emoji: str) -> bool:
        clean_emoji = str(emoji or "").strip()
        if not clean_emoji:
            return False
        payload = {
            "chat_id": chat_id,
            "message_id": int(message_id),
            "reaction": [{"type": "emoji", "emoji": clean_emoji}],
        }
        try:
            await asyncio.to_thread(self._api_json, "setMessageReaction", payload, 20)
            return True
        except Exception:
            return False

    @staticmethod
    def _extract_hook_emoji(value: Any) -> str:
        if isinstance(value, dict):
            value = value.get("emoji")
        if isinstance(value, str):
            return value.strip()
        return ""

    async def _maybe_passive_reaction(
        self,
        *,
        raw_message: Dict[str, Any],
        chat_id: str,
        user_text: str,
        assistant_text: str,
    ) -> None:
        message_id = raw_message.get("message_id")
        if not isinstance(message_id, int):
            return

        try:
            suggested = await emoji_responder.on_assistant_response(
                platform="telegram",
                user_text=user_text or "",
                assistant_text=assistant_text or "",
                llm_client=self.llm,
                scope=chat_id,
                message=raw_message,
            )
            emoji = self._extract_hook_emoji(suggested)
            if not emoji:
                return
            await self._set_message_reaction(chat_id=chat_id, message_id=message_id, emoji=emoji)
        except Exception as exc:
            logger.debug(f"[emoji_responder] passive reaction skipped: {exc}")

    def _load_blob(self, blob_key: str) -> bytes | None:
        if not blob_key:
            return None
        try:
            return blob_client.get(blob_key.encode("utf-8"))
        except Exception:
            return None

    async def _send_binary(
        self,
        chat_id: str,
        kind: str,
        filename: str,
        mimetype: str,
        binary: bytes,
        caption: str = "",
    ) -> bool:
        endpoint = "sendDocument"
        field = "document"
        kind = str(kind or "file").strip().lower()
        if kind == "image":
            endpoint = "sendPhoto"
            field = "photo"
        elif kind == "audio":
            endpoint = "sendAudio"
            field = "audio"
        elif kind == "video":
            endpoint = "sendVideo"
            field = "video"

        payload: Dict[str, Any] = {"chat_id": chat_id}
        if caption:
            caption_html = _telegram_html_text(caption)
            if caption_html:
                payload["caption"] = caption_html[:1024]
                payload["parse_mode"] = "HTML"
            else:
                payload["caption"] = _telegram_plain_text(caption)[:1024]

        files = {field: (filename, binary, mimetype or "application/octet-stream")}
        try:
            await asyncio.to_thread(self._api_multipart, endpoint, payload, files, 30)
            return True
        except Exception:
            if payload.get("parse_mode"):
                fallback_payload = dict(payload)
                fallback_payload.pop("parse_mode", None)
                fallback_payload["caption"] = _telegram_plain_text(caption)[:1024]
                try:
                    await asyncio.to_thread(self._api_multipart, endpoint, fallback_payload, files, 30)
                    return True
                except Exception:
                    pass
            if endpoint == "sendDocument":
                return False

        fallback_payload: Dict[str, Any] = {"chat_id": chat_id}
        if caption:
            fallback_payload["caption"] = _telegram_plain_text(caption)[:1024]
        files = {"document": (filename, binary, mimetype or "application/octet-stream")}
        try:
            await asyncio.to_thread(self._api_multipart, "sendDocument", fallback_payload, files, 30)
            return True
        except Exception:
            return False

    async def _send_attachment_dict(self, chat_id: str, attachment: Dict[str, Any]):
        kind = str((attachment or {}).get("type") or "file").strip().lower() or "file"
        filename = str((attachment or {}).get("name") or f"{kind}.bin").strip()
        mimetype = str((attachment or {}).get("mimetype") or "application/octet-stream").strip()

        binary = None
        blob_key = (attachment or {}).get("blob_key")
        if isinstance(blob_key, str) and blob_key.strip():
            binary = self._load_blob(blob_key.strip())
        elif isinstance((attachment or {}).get("bytes"), (bytes, bytearray)):
            binary = bytes((attachment or {}).get("bytes"))

        if not binary:
            await self._send_text(chat_id, f"[{kind.capitalize()}: {filename}]")
            return

        sent = await self._send_binary(
            chat_id=chat_id,
            kind=kind,
            filename=filename,
            mimetype=mimetype,
            binary=binary,
            caption="",
        )
        if not sent:
            await self._send_text(chat_id, f"[{kind.capitalize()}: {filename}]")

    def _save_message(
        self,
        chat_id: str,
        role: str,
        username: str,
        content: Any,
        *,
        user_id: str = "",
        user_handle: str = "",
    ):
        key = _history_key(chat_id)
        max_store = int(redis_client.get("tater:max_store") or 20)
        payload: Dict[str, Any] = {
            "role": role,
            "username": username,
            "content": content,
        }
        user_id_text = str(user_id or "").strip()
        user_handle_text = str(user_handle or "").strip()
        if user_id_text:
            payload["user_id"] = user_id_text
        if user_handle_text:
            payload["user_handle"] = user_handle_text
        redis_client.rpush(
            key,
            json.dumps(payload),
        )
        if max_store > 0:
            redis_client.ltrim(key, -max_store, -1)

    def _load_history(self, chat_id: str, limit: int | None = None):
        if limit is None:
            limit = int(redis_client.get("tater:max_llm") or 8)

        raw_history = redis_client.lrange(_history_key(chat_id), -limit, -1)
        loop_messages = []

        for entry in raw_history:
            data = json.loads(entry)
            role = data.get("role", "user")
            sender = data.get("username", role)
            content = data.get("content")

            if role not in ("user", "assistant"):
                role = "assistant"

            templ = _to_template_msg(role, content, sender=sender if role == "user" else None)
            if templ is not None:
                loop_messages.append(templ)

        return _enforce_user_assistant_alternation(loop_messages)

    def _tool_visible_on_telegram(self, plugin: Any) -> bool:
        platforms = set(getattr(plugin, "platforms", []) or [])
        return bool("telegram" in platforms or "both" in platforms)

    def build_system_prompt(self):
        # Platform preamble should be style/format only.
        return (
            "You are a Telegram-savvy AI assistant.\n"
            "Keep replies concise and natural for chat.\n"
            "Telegram supports rich formatting; markdown-style emphasis, links, and code are allowed when helpful.\n"
        )

    def _chat_allowed(self, chat_id: str) -> bool:
        normalized = _normalize_chat_id(chat_id)
        if not self.allowed_chat_ids:
            return True
        return normalized in self.allowed_chat_ids

    def _dm_user_allowed(self, sender: Dict[str, Any]) -> bool:
        if not self.allowed_dm_users:
            return False

        sender_id = str((sender or {}).get("id") or "").strip()
        sender_username = _normalize_user_ref((sender or {}).get("username"))

        candidates = set()
        if sender_id:
            candidates.add(sender_id)
        if sender_username:
            candidates.add(sender_username)
        return bool(candidates & self.allowed_dm_users)

    def _admin_user_allowed(self, sender: Dict[str, Any]) -> bool:
        if not self.allowed_dm_users:
            return False

        sender_id = str((sender or {}).get("id") or "").strip()
        sender_username = _normalize_user_ref((sender or {}).get("username"))
        sender_first = _normalize_user_ref((sender or {}).get("first_name"))

        candidates = set()
        if sender_id:
            candidates.add(sender_id)
        if sender_username:
            candidates.add(sender_username)
        if sender_first:
            candidates.add(sender_first)
        return bool(candidates & self.allowed_dm_users)

    async def _send_plugin_result(self, chat_id: str, result: Any):
        async def emit_item(item: Any):
            if item is None:
                return

            if isinstance(item, str):
                text = item.strip()
                if not text:
                    return
                await self._send_text(chat_id, text)
                self._save_message(
                    chat_id,
                    "assistant",
                    "assistant",
                    {"marker": "plugin_response", "phase": "final", "content": text},
                )
                return

            if isinstance(item, dict) and item.get("type") in ("image", "audio", "video", "file"):
                await self._send_attachment_dict(chat_id, item)
                self._save_message(
                    chat_id,
                    "assistant",
                    "assistant",
                    {
                        "marker": "plugin_response",
                        "phase": "final",
                        "content": {
                            "type": item.get("type"),
                            "name": item.get("name") or "output.bin",
                            "mimetype": item.get("mimetype") or "",
                        },
                    },
                )
                return

            try:
                text = json.dumps(item, ensure_ascii=False)
            except Exception:
                text = str(item)
            if not text.strip():
                return
            await self._send_text(chat_id, text)
            self._save_message(
                chat_id,
                "assistant",
                "assistant",
                {"marker": "plugin_response", "phase": "final", "content": text},
            )

        if isinstance(result, list):
            for item in result:
                await emit_item(item)
            return

        await emit_item(result)

    async def _run_plugin(self, plugin: Any, raw_message: Dict[str, Any], chat_id: str, username: str, args: Dict[str, Any]):
        if _is_custom_handler(plugin, "handle_telegram"):
            return await plugin.handle_telegram(raw_message, args, self.llm)

        if _is_custom_handler(plugin, "handle_discord"):
            adapter = _TelegramMessageAdapter(raw_message, chat_id, username)
            result = await plugin.handle_discord(adapter, args, self.llm)
            buffered = adapter.drain_sent_messages()
            if not buffered:
                return result
            if result is None:
                return buffered
            if isinstance(result, list):
                return buffered + result
            return buffered + [result]

        if _is_custom_handler(plugin, "handle_webui"):
            return await plugin.handle_webui(args, self.llm)

        raise RuntimeError("No Telegram-compatible handler on this plugin.")

    async def _handle_user_message(self, message: Dict[str, Any]):
        chat = message.get("chat") or {}
        sender = message.get("from") or {}

        chat_id = _normalize_chat_id(chat.get("id"))
        if not chat_id:
            return
        chat_type = str(chat.get("type") or "").strip().lower()
        if chat_type == "private":
            if not self._dm_user_allowed(sender):
                logger.info(
                    "[Telegram] Ignoring DM from user %s (not in allowed_user list).",
                    str(sender.get("id") or sender.get("username") or "unknown"),
                )
                return
        elif not self._chat_allowed(chat_id):
            logger.info("[Telegram] Ignoring message from chat %s (not in allowed_chat_id list).", chat_id)
            return

        _remember_chat_lookup(chat, sender)

        identity = _telegram_sender_identity(message)
        username = str(identity.get("display_name") or "telegram_user").strip() or "telegram_user"
        sender_user_id = str(identity.get("user_id") or "").strip()
        sender_user_handle = str(identity.get("user_handle") or "").strip()
        message_text = _message_text(message)
        if not message_text:
            return

        captured_media = await asyncio.to_thread(self._capture_incoming_artifacts_sync, message, chat_id)
        input_artifacts = captured_media.get("input_artifacts") if isinstance(captured_media, dict) else None
        self._save_message(
            chat_id,
            "user",
            username,
            message_text,
            user_id=sender_user_id,
            user_handle=sender_user_handle,
        )

        system_prompt = self.build_system_prompt()
        history = self._load_history(chat_id)
        messages = history
        merged_registry = dict(pr.get_registry_snapshot() or {})
        merged_enabled = _get_plugin_enabled

        try:
            origin = {
                "platform": "telegram",
                "chat_id": chat_id,
                "chat_type": str(chat.get("type") or "").strip(),
                "channel": chat.get("title"),
                "user": username,
                "user_id": sender_user_id,
                "request_id": str(message.get("message_id") or f"{chat_id}:{time.time():.3f}"),
            }
            if isinstance(input_artifacts, list) and input_artifacts:
                origin["input_artifacts"] = [dict(item) for item in input_artifacts if isinstance(item, dict)]
            origin = {k: v for k, v in origin.items() if v not in (None, "")}

            async def _wait_callback(func_name, plugin_obj):
                if not plugin_obj:
                    return
                if not plugin_supports_platform(plugin_obj, "telegram"):
                    return
                if not hasattr(plugin_obj, "waiting_prompt_template"):
                    return
                wait_msg = plugin_obj.waiting_prompt_template.format(mention=username)
                wait_response = await self.llm.chat(
                    messages=[
                        {"role": "system", "content": "Write one short, friendly status line."},
                        {"role": "user", "content": wait_msg},
                    ]
                )
                wait_text = str((wait_response.get("message", {}) or {}).get("content", "") or "").strip()
                if wait_text:
                    await self._send_text(chat_id, wait_text)
                    self._save_message(
                        chat_id,
                        "assistant",
                        "assistant",
                        {"marker": "plugin_wait", "content": wait_text},
                    )

            def _admin_guard(func_name):
                needs_admin = False
                if is_admin_only_plugin(func_name):
                    needs_admin = True

                if needs_admin and not self._admin_user_allowed(sender):
                    msg = (
                        "This tool is restricted to the configured admin user on Telegram."
                        if self.allowed_dm_users
                        else "This tool is disabled because no Telegram admin user is configured."
                    )
                    return action_failure(
                        code="admin_only",
                        message=msg,
                        needs=[],
                        say_hint="Explain that this tool is restricted to the admin user on this platform.",
                    )
                return None

            final_text = ""
            artifacts = []
            async with self._safe_typing(chat_id):
                agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
                result = await run_cerberus_turn(
                    llm_client=self.llm,
                    platform="telegram",
                    history_messages=messages,
                    registry=merged_registry,
                    enabled_predicate=merged_enabled,
                    context={"update": message},
                    user_text=message_text,
                    scope=f"chat:{chat_id}",
                    origin=origin,
                    wait_callback=_wait_callback,
                    admin_guard=_admin_guard,
                    redis_client=redis_client,
                    max_rounds=agent_max_rounds,
                    max_tool_calls=agent_max_tool_calls,
                    platform_preamble=system_prompt,
                )

                final_text = str(result.get("text") or "").strip()
                if final_text:
                    await self._send_text(chat_id, final_text)
                    self._save_message(
                        chat_id,
                        "assistant",
                        "assistant",
                        {"marker": "plugin_response", "phase": "final", "content": final_text},
                    )

                artifacts = result.get("artifacts") or []
                for item in artifacts:
                    await self._send_plugin_result(chat_id, item)

            if final_text or artifacts:
                assistant_summary = final_text or "[Sent attachments]"
                await self._maybe_passive_reaction(
                    raw_message=message,
                    chat_id=chat_id,
                    user_text=message_text,
                    assistant_text=assistant_summary,
                )
            return

        except Exception as e:
            logger.error(f"[Telegram] Error processing message: {e}")
            await self._send_text(chat_id, "An error occurred while processing your request.")

    async def _prime_offset(self):
        cursor = None
        while True:
            payload: Dict[str, Any] = {
                "timeout": 0,
                "limit": 100,
                "allowed_updates": ["message", "edited_message", "channel_post", "edited_channel_post"],
            }
            if cursor is not None:
                payload["offset"] = cursor
            try:
                data = await asyncio.to_thread(self._api_json, "getUpdates", payload, 20)
            except Exception:
                return
            updates = data.get("result") or []
            if not updates:
                break
            try:
                cursor = int(updates[-1].get("update_id")) + 1
            except Exception:
                break
            if len(updates) < 100:
                break
        if cursor is not None:
            self.offset = cursor

    async def _updates_worker(self, stop_event=None):
        backoff = 1.0
        max_backoff = 10.0

        while not _stop_requested(stop_event):
            payload: Dict[str, Any] = {
                "timeout": self.poll_timeout_sec,
                "allowed_updates": ["message", "edited_message", "channel_post", "edited_channel_post"],
            }
            if self.offset is not None:
                payload["offset"] = self.offset

            try:
                data = await asyncio.to_thread(
                    self._api_json,
                    "getUpdates",
                    payload,
                    self.poll_timeout_sec + 10,
                )
                updates = data.get("result") or []
                for update in updates:
                    update_id = update.get("update_id")
                    if isinstance(update_id, int):
                        self.offset = update_id + 1
                    msg = (
                        update.get("message")
                        or update.get("edited_message")
                        or update.get("channel_post")
                        or update.get("edited_channel_post")
                    )
                    if msg:
                        await self._handle_user_message(msg)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[Telegram] update poll failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)

    async def _notify_queue_worker(self, stop_event=None):
        while not _stop_requested(stop_event):
            try:
                item_json = await asyncio.to_thread(redis_client.lpop, NOTIFY_QUEUE_KEY)
            except Exception:
                item_json = None

            if not item_json:
                await asyncio.sleep(NOTIFY_POLL_INTERVAL)
                continue

            try:
                item = json.loads(item_json)
            except Exception:
                logger.warning("[notifyq] invalid JSON item; skipping.")
                continue

            if is_expired(item):
                continue

            attachments = load_queue_attachments(redis_client, item.get("id"))
            targets = item.get("targets") or {}
            chat_id = _resolve_chat_target(
                targets.get("chat_id")
                or targets.get("channel_id")
                or targets.get("channel")
                or self.response_chat_id
            )
            if not chat_id:
                logger.warning("[notifyq] Telegram missing chat_id; dropping item.")
                continue

            message = (item.get("message") or "").strip()
            title = (item.get("title") or "").strip()
            if not message and not title and not attachments:
                continue

            payload = ""
            if title and message:
                payload = f"{title}\n{message}"
            elif title:
                payload = title
            else:
                payload = message

            try:
                if payload:
                    await self._send_text(chat_id, payload)
                for media in attachments:
                    await self._send_attachment_dict(chat_id, media)
            except Exception as e:
                logger.warning(f"[notifyq] Telegram worker failed to send item: {e}")

    async def run(self, stop_event=None):
        await self._prime_offset()
        queue_task = asyncio.create_task(self._notify_queue_worker(stop_event))
        updates_task = asyncio.create_task(self._updates_worker(stop_event))
        logger.info("Telegram platform started.")

        try:
            if stop_event:
                while not _stop_requested(stop_event):
                    await asyncio.sleep(0.5)
            else:
                await asyncio.Event().wait()
        finally:
            for task in (queue_task, updates_task):
                if task and not task.done():
                    task.cancel()
            await asyncio.gather(queue_task, updates_task, return_exceptions=True)
            logger.info("Telegram platform stopped.")


def _load_platform_settings() -> Dict[str, Any]:
    settings = redis_client.hgetall("telegram_platform_settings") or {}
    legacy = redis_client.hgetall("plugin_settings:Telegram Notifier") or {}

    token = (
        str(settings.get("telegram_bot_token") or "").strip()
        or str(legacy.get("telegram_bot_token") or "").strip()
    )
    response_chat_id = _normalize_chat_id(
        str(settings.get("response_chat_id") or "").strip()
        or str(legacy.get("telegram_chat_id") or "").strip()
    )
    allowed_chat_id = str(settings.get("allowed_chat_id") or "").strip()
    allowed_user = str(settings.get("allowed_user") or settings.get("allowed_user_id") or "").strip()
    raw_timeout = settings.get("poll_timeout_sec")
    try:
        poll_timeout_sec = max(1, int(float(raw_timeout)))
    except Exception:
        poll_timeout_sec = 20

    return {
        "token": token,
        "allowed_chat_id": allowed_chat_id,
        "allowed_user": allowed_user,
        "response_chat_id": response_chat_id,
        "poll_timeout_sec": poll_timeout_sec,
    }


def run(stop_event=None):
    cfg = _load_platform_settings()
    token = cfg.get("token")
    if not token:
        logger.warning("Missing Telegram bot token in Telegram Settings.")
        return

    llm_client = get_llm_client_from_env()
    logger.info(f"[Telegram] LLM client -> {build_llm_host_from_env()}")

    platform = TelegramPlatform(
        token=token,
        llm_client=llm_client,
        allowed_chat_id=cfg.get("allowed_chat_id") or "",
        allowed_user=cfg.get("allowed_user") or "",
        response_chat_id=cfg.get("response_chat_id") or "",
        poll_timeout_sec=cfg.get("poll_timeout_sec") or 20,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(platform.run(stop_event=stop_event))
    finally:
        try:
            pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        finally:
            loop.close()
