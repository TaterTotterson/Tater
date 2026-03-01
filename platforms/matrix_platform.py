# platforms/matrix_platform.py
import os
import json
import time
import redis
import asyncio
import logging
import threading
import re
from typing import Any, Dict, Optional, List
import contextlib
import imghdr
import hashlib
import uuid
from io import BytesIO

from dotenv import load_dotenv
load_dotenv()

import plugin_registry as pr
from notify.queue import is_expired
from notify.media import load_queue_attachments

from helpers import (
    get_llm_client_from_env,
    build_llm_host_from_env,
)
from admin_gate import (
    is_admin_only_plugin,
    normalize_admin_list,
)
from plugin_result import action_failure
from plugin_kernel import plugin_supports_platform
from cerberus import run_cerberus_turn, resolve_agent_limits
from emoji_responder import emoji_responder

# Matrix SDK
from nio import (
    AsyncClient,
    AsyncClientConfig,
    LoginResponse,
    RoomMessageText,
    MegolmEvent,
    InviteMemberEvent,
)

try:
    from nio import RoomMessageImage
except Exception:
    RoomMessageImage = None

try:
    from nio import RoomMessageVideo, RoomMessageAudio, RoomMessageFile
except Exception:
    RoomMessageVideo = None
    RoomMessageAudio = None
    RoomMessageFile = None

try:
    from nio.events.room_events import RoomEncryptionEvent
except Exception:
    RoomEncryptionEvent = None

try:
    from nio.crypto import TrustState  # enum
except Exception:
    TrustState = None

# --- Markdown rendering (required) ---
from markdown_it import MarkdownIt

# Tables plugin: handle both modern and legacy module names, else no-op
try:
    from mdit_py_plugins.tables import tables_plugin as table_plugin  # modern
except Exception:
    try:
        from mdit_py_plugins.table import table_plugin  # legacy
    except Exception:
        def table_plugin(md):  # no-op fallback
            return md

# Emoji & tasklists: import if available, else no-ops
try:
    from mdit_py_plugins.emoji import emoji_plugin
except Exception:
    def emoji_plugin(md): return md

try:
    from mdit_py_plugins.tasklists import tasklists_plugin
except Exception:
    def tasklists_plugin(md): return md

_md = (
    MarkdownIt("commonmark", {"linkify": True, "typographer": True})
    .use(table_plugin)
    .use(emoji_plugin)
    .use(tasklists_plugin)
    .enable("strikethrough")
    .disable("html_block")
    .disable("html_inline")
)

# Base config
logging.basicConfig(level=logging.INFO)

# Quiet noisy libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("nio").setLevel(logging.WARNING)
logging.getLogger("nio.rooms").setLevel(logging.ERROR)
logging.getLogger("nio.client.base_client").setLevel(logging.ERROR)
logging.getLogger("nio.crypto").setLevel(logging.ERROR)
logging.getLogger("nio.crypto.log").setLevel(logging.ERROR)

logger = logging.getLogger("matrix.tater")

# ---------------- Platform settings ----------------
PLATFORM_SETTINGS = {
    "category": "Matrix Settings",
    "required": {
        "matrix_hs": {
            "label": "Homeserver URL",
            "type": "string",
            "default": "https://matrix-client.matrix.org",
            "description": "Matrix homeserver base URL (e.g., https://matrix.example.com)"
        },
        "matrix_user": {
            "label": "User ID",
            "type": "string",
            "default": "@tater:example.com",
            "description": "Matrix user id for the bot (e.g., @tater:example.com)"
        },
        "matrix_access_token": {
            "label": "Access Token (optional)",
            "type": "string",
            "default": "",
            "description": "Prefer access token; if blank, password login will be attempted."
        },
        "matrix_password": {
            "label": "Password (fallback)",
            "type": "string",
            "default": "",
            "description": "Used only if access token is empty."
        },
        "matrix_device_name": {
            "label": "Device Name",
            "type": "string",
            "default": "TaterBot",
            "description": "Device name shown in Matrix sessions"
        },
        "response_policy": {
            "label": "Response Policy",
            "type": "select",
            "options": ["mention_only", "all_messages"],
            "default": "mention_only",
            "description": "When to respond in rooms"
        },
        "resume_mode": {
            "label": "Resume Mode",
            "type": "select",
            "options": ["from_now", "from_last_sync"],
            "default": "from_now",
            "description": "from_now = ignore backlog on startup; from_last_sync = process all missed messages"
        },
        "mention_keywords": {
            "label": "Mention Keywords",
            "type": "string",
            "default": "",
            "description": "Comma-separated triggers (e.g. 'tater, taterbot') to count as mentions"
        },
        "admin_user_id": {
            "label": "Admin User ID",
            "type": "string",
            "default": "",
            "description": "Only this Matrix user can run admin-only tools (use full MXID like @user:server).",
        },
        "max_response_length": {
            "label": "Max Response Chunk Length",
            "type": "number",
            "default": 4000
        },
        "matrix_store_path": {
            "label": "Store Path",
            "type": "string",
            "default": "/app/matrix-store",
            "description": "Persistent path for nio store (devices, sessions, etc.)"
        },
        "matrix_pickle_key": {
            "label": "Pickle Key",
            "type": "string",
            "default": "",
            "description": "Secret used to encrypt local store; fallback to MATRIX_PICKLE_KEY env"
        },
        "trust_unverified_devices": {
            "label": "Trust Unverified Devices",
            "type": "select",
            "options": ["true", "false"],
            "default": "true",
            "description": "If true, the bot auto-trusts/ignores unverified devices so it can send E2EE."
        },
    },
}

# ---------------- Redis (text/json history) ----------------
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True,
)
NOTIFY_QUEUE_KEY = "notifyq:matrix"
NOTIFY_POLL_INTERVAL = 0.5

# ---------------- Redis (binary blobs, NO base64) ----------------
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
blob_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=False)

BLOB_PREFIX = "tater:blob:matrix"


def _blob_key() -> str:
    return f"{BLOB_PREFIX}:{uuid.uuid4().hex}"


def store_blob(binary: bytes, ttl_seconds: int = 60 * 60 * 24 * 7) -> str:
    key = _blob_key()
    blob_client.set(key.encode("utf-8"), binary)
    if ttl_seconds and ttl_seconds > 0:
        blob_client.expire(key.encode("utf-8"), int(ttl_seconds))
    return key


def load_blob(key: str) -> Optional[bytes]:
    if not key:
        return None
    return blob_client.get(key.encode("utf-8"))


# ---------------- LLM ----------------
llm_client = None

# ---------------- Helpers ----------------
def _md_to_html(text: str) -> str:
    return "" if not text else _md.render(text)

def _guess_mime(data: bytes) -> str:
    kind = imghdr.what(None, h=data)
    return {
        "jpeg": "image/jpeg",
        "png":  "image/png",
        "gif":  "image/gif",
        "webp": "image/webp",
        "bmp":  "image/bmp",
        "tiff": "image/tiff",
    }.get(kind, "application/octet-stream")

async def _apply_avatar_from_redis(client):
    """
    Avatar is now stored as raw bytes blob (no base64) at key 'tater:avatar_blob_key'
    which points to a blob key in Redis binary store.
    Back-compat: If 'tater:avatar' exists (base64), we still support it.
    Caches:
      - matrix:last_avatar_hash  (sha1 of bytes)
      - matrix:last_avatar_mxc   (mxc://… from homeserver)
    """
    data = None

    # Preferred: blob ref
    blob_ref = redis_client.get("tater:avatar_blob_key")
    if blob_ref:
        data = load_blob(blob_ref)

    # Back-compat: old base64 key
    if data is None:
        b64 = redis_client.get("tater:avatar")
        if b64:
            try:
                import base64
                data = base64.b64decode(b64)
            except Exception:
                logger.warning("[Matrix] Avatar in Redis is not valid base64; skipping.")
                return

    if not data:
        return

    data_hash = hashlib.sha1(data).hexdigest()
    last_hash = redis_client.get("matrix:last_avatar_hash")
    last_mxc  = redis_client.get("matrix:last_avatar_mxc")

    if last_hash and data_hash == last_hash and last_mxc:
        logger.info("[Matrix] Avatar unchanged; skipping upload.")
        return

    mime = _guess_mime(data)
    ext = {
        "image/jpeg": "jpg",
        "image/png":  "png",
        "image/gif":  "gif",
        "image/webp": "webp",
        "image/bmp":  "bmp",
        "image/tiff": "tiff",
    }.get(mime, "bin")
    filename = f"avatar.{ext}"

    try:
        bio = BytesIO(data)
        bio.seek(0)

        up = await client.upload(
            bio,
            content_type=mime,
            filename=filename,
            filesize=len(data),
        )
        if isinstance(up, tuple):
            up = up[0]

        mxc = getattr(up, "content_uri", None)
        if not mxc:
            logger.warning(f"[Matrix] Upload returned no MXC URI: {up!r}")
            return

        await client.set_avatar(mxc)
        logger.info("[Matrix] Avatar updated from Redis.")

        redis_client.set("matrix:last_avatar_hash", data_hash)
        redis_client.set("matrix:last_avatar_mxc", mxc)

    except Exception as e:
        logger.warning(f"[Matrix] Failed to upload/set avatar: {e}")

def _get_setting(key: str, fallback: str = "") -> str:
    s = redis_client.hget("matrix_platform_settings", key)
    if s is not None:
        return s
    if key in PLATFORM_SETTINGS["required"]:
        return PLATFORM_SETTINGS["required"][key]["default"]
    return fallback

def _get_int_setting(key: str, fallback: int) -> int:
    s = _get_setting(key)
    try:
        return int(str(s).strip()) if s else fallback
    except Exception:
        return fallback

def _get_bool_setting(key: str, fallback: bool) -> bool:
    s = _get_setting(key)
    if s is None or s == "":
        return fallback
    return str(s).strip().lower() in ("1", "true", "yes", "on")

def _matrix_user_tokens(value: str) -> set[str]:
    raw = str(value or "").strip().lower()
    if not raw:
        return set()
    tokens = {raw}
    if raw.startswith("@"):
        tokens.add(raw[1:])
        raw = raw[1:]
    if ":" in raw:
        tokens.add(raw.split(":", 1)[0])
    return tokens

def _admin_user_allowed(sender: str) -> bool:
    raw = _get_setting("admin_user_id", "")
    allowed: set[str] = set()
    for item in normalize_admin_list(raw):
        allowed |= _matrix_user_tokens(item)
    if not allowed:
        return False
    return bool(_matrix_user_tokens(sender) & allowed)

def get_plugin_enabled(name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", name)
    return bool(enabled and enabled.lower() == "true")

def _room_history_key(room_id: str) -> str:
    return f"tater:matrix:{room_id}:history"

def save_matrix_message(room_id: str, role: str, username: str, content: Any):
    key = _room_history_key(room_id)
    max_store = int(redis_client.get("tater:max_store") or 20)
    redis_client.rpush(key, json.dumps({"role": role, "username": username, "content": content}))
    if max_store > 0:
        redis_client.ltrim(key, -max_store, -1)


def _to_template_msg(role: str, content: Any, sender: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Matrix variant (aligned with Discord/IRC):
    - Skip plugin_wait
    - Include ONLY final plugin_response (string/placeholder or compact JSON)
    - Represent plugin_call as assistant text
    - Prefix user messages with sender when in rooms
    """
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        if content.get("phase", "final") != "final":
            return None
        payload = content.get("content")
        if isinstance(payload, str):
            txt = payload.strip()
            if len(txt) > 4000:
                txt = txt[:4000] + " …"
            return {"role": "assistant", "content": txt}
        if isinstance(payload, dict) and payload.get("type") in ("image", "audio", "video", "file"):
            kind = payload.get("type").capitalize()
            name = payload.get("name") or ""
            return {"role": "assistant", "content": f"[{kind} from tool]{f' {name}' if name else ''}".strip()}
        try:
            compact = json.dumps(payload, ensure_ascii=False)
            if len(compact) > 2000:
                compact = compact[:2000] + " …"
            return {"role": "assistant", "content": compact}
        except Exception:
            return None

    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps(
            {"function": content.get("plugin"), "arguments": content.get("arguments", {})},
            indent=2,
        )
        return {"role": "assistant", "content": as_text}

    if isinstance(content, str):
        if role == "user" and sender:
            return {"role": "user", "content": f"{sender}: {content}"}
        return {"role": role, "content": content}

    return {"role": role, "content": str(content)}

def _enforce_user_assistant_alternation(loop_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for m in loop_messages:
        if not m:
            continue
        if merged and merged[-1]["role"] == m["role"]:
            a, b = merged[-1]["content"], m["content"]
            merged[-1]["content"] = (str(a) + "\n\n" + str(b)).strip()
        else:
            merged.append(m)
    return merged

def load_matrix_history(room_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    if limit is None:
        limit = int(redis_client.get("tater:max_llm") or 8)
    key = _room_history_key(room_id)
    raw = redis_client.lrange(key, -limit, -1)
    loop_messages: List[Dict[str, Any]] = []
    for entry in raw:
        data = json.loads(entry)
        role = data.get("role", "user")
        sender = data.get("username", role)
        content = data.get("content")

        # Show placeholders for media dicts in the LLM prompt (no blobs injected)
        if isinstance(content, dict) and content.get("type") in ["image", "audio", "video", "file"]:
            name = content.get("name", "file")
            content = f"[{content['type'].capitalize()}: {name}]"

        if role not in ("user", "assistant"):
            role = "assistant"

        templ = _to_template_msg(role, content, sender=sender if role == "user" else None)
        if templ is not None:
            loop_messages.append(templ)
    return _enforce_user_assistant_alternation(loop_messages)

# ---------------- System prompt (Matrix-scoped) ----------------
def build_system_prompt() -> str:
    # Platform preamble should be style/format only.
    return (
        "You are a Matrix-savvy AI assistant.\n"
        "Keep replies concise and natural for chat.\n"
    )

# ---------------- Mention helpers & trigger policy ----------------
def _keywords() -> List[str]:
    raw = _get_setting("mention_keywords", "")
    return [s.strip().lower() for s in raw.split(",") if s.strip()]


def _matrix_bot_aliases(my_user_id: str, my_display: Optional[str]) -> List[str]:
    aliases: List[str] = []
    if my_display:
        aliases.append(my_display.strip())
    if my_user_id:
        aliases.append(my_user_id.strip())
        if my_user_id.startswith("@"):
            localpart = my_user_id[1:].split(":", 1)[0]
            if localpart:
                aliases.append(localpart)
    # de-dupe preserving order
    seen = set()
    out: List[str] = []
    for a in aliases:
        if a and a.lower() not in seen:
            out.append(a)
            seen.add(a.lower())
    return out


def _strip_matrix_mentions(text: str, my_user_id: str, my_display: Optional[str]) -> str:
    if not text:
        return ""
    cleaned = str(text).strip()
    for alias in _matrix_bot_aliases(my_user_id, my_display):
        pattern = re.compile(rf"\\b{re.escape(alias)}\\b", flags=re.IGNORECASE)
        cleaned = pattern.sub("", cleaned)
    cleaned = re.sub(r"[@,:;.!]+", " ", cleaned)
    cleaned = re.sub(r"\\s+", " ", cleaned).strip()
    return cleaned


def _is_mention_only(text: str, my_user_id: str, my_display: Optional[str]) -> bool:
    return _strip_matrix_mentions(text, my_user_id, my_display) == ""


def _find_prev_user_message(room_id: str, sender: str, my_user_id: str, my_display: Optional[str], exclude_text: str = "") -> str:
    key = _room_history_key(room_id)
    try:
        raw_history = redis_client.lrange(key, 0, -1)
    except Exception:
        return ""
    for entry in reversed(raw_history):
        try:
            data = json.loads(entry)
        except Exception:
            continue
        if data.get("role") != "user":
            continue
        if sender and data.get("username") != sender:
            continue
        content = data.get("content")
        if not isinstance(content, str):
            continue
        if exclude_text and content == exclude_text:
            continue
        if _is_mention_only(content, my_user_id, my_display):
            continue
        return content.strip()
    return ""

def _should_respond(policy: str, body: str, my_user_id: str, my_display: Optional[str]) -> bool:
    if policy == "all_messages":
        return True

    body_l = (body or "").lower()

    if my_user_id and my_user_id.lower() in body_l:
        return True

    if my_display and my_display.lower() in body_l:
        return True

    try:
        if my_user_id.startswith("@"):
            localpart = my_user_id[1:].split(":", 1)[0].lower()
            if localpart and localpart in body_l:
                return True
    except Exception:
        pass

    for kw in _keywords():
        if kw and kw in body_l:
            return True

    return False

# ---------------- Matrix Bot ----------------
class MatrixPlatform:
    def __init__(self):
        self.homeserver = _get_setting("matrix_hs")
        self.user_id = _get_setting("matrix_user")
        self.access_token = _get_setting("matrix_access_token")
        self.password = _get_setting("matrix_password")
        self.device_name = _get_setting("matrix_device_name", "TaterBot")
        self.response_policy = _get_setting("response_policy", "mention_only")
        self.max_chunk = _get_int_setting("max_response_length", 4000)
        self.store_path = _get_setting("matrix_store_path", "/app/matrix-store")
        self.pickle_key = _get_setting("matrix_pickle_key", os.getenv("MATRIX_PICKLE_KEY", ""))
        self.resume_mode = _get_setting("resume_mode", "from_now")
        self.trust_unverified_devices = _get_bool_setting("trust_unverified_devices", True)
        self.ready_ts_ms: Optional[int] = None

        try:
            os.makedirs(self.store_path, exist_ok=True)
            os.chmod(self.store_path, 0o700)
        except Exception as e:
            logger.error(f"[Matrix] Could not create store path {self.store_path}: {e}")
            self.store_path = "/tmp/matrix-store"
            os.makedirs(self.store_path, exist_ok=True)
            os.chmod(self.store_path, 0o700)
            logger.warning(f"[Matrix] Falling back to {self.store_path}")

        cfg = AsyncClientConfig(
            store_sync_tokens=True,
            encryption_enabled=True,
            pickle_key=self.pickle_key or None,
        )
        self.client = AsyncClient(
            self.homeserver,
            self.user_id,
            store_path=self.store_path,
            config=cfg,
        )
        if self.access_token:
            self.client.access_token = self.access_token

        self.display_name_cache: Optional[str] = None
        self.stop_event: Optional[threading.Event] = None
        self._sync_task: Optional[asyncio.Task] = None

    # ---------- Trust helpers ----------
    async def _keys_query_users(self, user_ids: List[str]):
        if not user_ids:
            return
        try:
            await self.client.keys_query(user_ids=user_ids)
        except Exception as e:
            logger.debug(f"[Matrix] keys_query failed: {e}")

    async def _persist_device_if_possible(self, user_id: str, dev):
        try:
            store = getattr(self.client, "store", None)
            if not store:
                return
            if hasattr(store, "save_device"):
                try:
                    store.save_device(user_id, dev)
                except TypeError:
                    store.save_device(dev)
            elif hasattr(store, "save_device_keys"):
                try:
                    store.save_device_keys(user_id, dev)
                except Exception:
                    pass
        except Exception:
            pass

    async def _auto_trust_room_devices(self, room) -> None:
        if not self.trust_unverified_devices:
            return

        try:
            member_ids = list(getattr(room, "users", {}).keys())
        except Exception:
            member_ids = []

        await self._keys_query_users(member_ids)

        store_map = getattr(self.client, "device_store", None)
        crypto = getattr(self.client, "crypto", None)
        if not store_map:
            return

        for uid in member_ids:
            try:
                devices = store_map[uid]
            except Exception:
                devices = {}
            for dev_id, dev in (devices or {}).items():
                try:
                    if hasattr(dev, "blacklisted") and getattr(dev, "blacklisted"):
                        try:
                            dev.blacklisted = False
                            logger.info(f"[Matrix] Cleared blacklist for {uid} {dev_id}")
                            await self._persist_device_if_possible(uid, dev)
                        except Exception:
                            logger.debug(f"[Matrix] Could not clear blacklist for {uid} {dev_id}")

                    if crypto and hasattr(crypto, "verify_device"):
                        try:
                            crypto.verify_device(uid, dev_id)
                            logger.info(f"[Matrix] Verified device {dev_id} for {uid} (crypto.verify_device)")
                            await self._persist_device_if_possible(uid, dev)
                            continue
                        except Exception:
                            pass

                    if hasattr(dev, "verified"):
                        try:
                            dev.verified = True
                            logger.info(f"[Matrix] Marked device {dev_id} for {uid} as verified (bool).")
                            await self._persist_device_if_possible(uid, dev)
                            continue
                        except Exception:
                            pass

                    if TrustState and hasattr(dev, "trust_state"):
                        try:
                            if dev.trust_state != TrustState.VERIFIED:
                                dev.trust_state = TrustState.VERIFIED
                                logger.info(f"[Matrix] Marked device {dev_id} for {uid} as VERIFIED (enum).")
                                await self._persist_device_if_possible(uid, dev)
                        except Exception:
                            logger.debug(f"[Matrix] Could not set trust_state for {uid} {dev_id}")

                except Exception as e:
                    logger.debug(f"[Matrix] Trust update failed for {uid} {dev_id}: {e}")

    # ---------- Sending helpers ----------
    class _TypingScope:
        def __init__(self, client, room_id: str, refresh_ms: int = 20000):
            self.client = client
            self.room_id = room_id
            self.refresh_ms = max(5000, int(refresh_ms))
            self._task = None
            self._alive = False

        async def _pinger(self):
            try:
                while self._alive:
                    try:
                        await self.client.room_typing(self.room_id, True, timeout=self.refresh_ms)
                    except Exception:
                        pass
                    await asyncio.sleep(self.refresh_ms / 1000 * 0.8)
            except asyncio.CancelledError:
                pass

        async def __aenter__(self):
            self._alive = True
            try:
                await self.client.room_typing(self.room_id, True, timeout=self.refresh_ms)
            except Exception:
                pass
            self._task = asyncio.create_task(self._pinger())
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self._alive = False
            if self._task and not self._task.done():
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._task
            try:
                await self.client.room_typing(self.room_id, False)
            except Exception:
                pass

    def typing(self, room_id: str) -> "_TypingScope":
        return MatrixPlatform._TypingScope(self.client, room_id)

    async def _send_chunks(self, room_id: str, content: str):
        if not content:
            return

        kwargs = {}
        if self.trust_unverified_devices:
            kwargs["ignore_unverified_devices"] = True

        i, n = 0, len(content)
        while i < n:
            j = min(i + self.max_chunk, n)
            k = content.rfind("\n", i, j)
            if k == -1 or k <= i:
                k = j
            part = content[i:k].rstrip("\n")
            i = k

            if not part:
                continue

            payload = {"msgtype": "m.text", "body": part}
            html_part = _md_to_html(part)
            if html_part and html_part.strip():
                payload["format"] = "org.matrix.custom.html"
                payload["formatted_body"] = html_part

            try:
                await self.client.room_send(
                    room_id=room_id,
                    message_type="m.room.message",
                    content=payload,
                    **kwargs,
                )
            except TypeError:
                await self.client.room_send(
                    room_id=room_id,
                    message_type="m.room.message",
                    content=payload,
                )
            await asyncio.sleep(0.02)

    async def _send_with_trust(self, room_id: str, content: str):
        room = self.client.rooms.get(room_id)
        if room and self.trust_unverified_devices:
            await self._auto_trust_room_devices(room)

        try:
            await self._send_chunks(room_id, content)
        except Exception as e:
            msg = (str(e) or "").lower()
            if ("not verified" in msg) or ("blacklisted" in msg) or ("unknown devices" in msg):
                if room:
                    logger.warning("[Matrix] Trust error on send; attempting to auto-trust and retry…")
                    await self._auto_trust_room_devices(room)
                    try:
                        await self._send_chunks(room_id, content)
                        return
                    except Exception as e2:
                        logger.error(f"[Matrix] Retry after trust failed: {e2}")
                else:
                    logger.error("[Matrix] Could not resolve room for trust retry.")
            else:
                logger.error(f"[Matrix] Send failed: {e}")

    async def _send_reaction(self, room_id: str, event_id: str, emoji: str) -> bool:
        clean_emoji = str(emoji or "").strip()
        event_ref = str(event_id or "").strip()
        if not clean_emoji or not event_ref:
            return False

        content = {
            "m.relates_to": {
                "rel_type": "m.annotation",
                "event_id": event_ref,
                "key": clean_emoji,
            }
        }

        kwargs = {}
        if self.trust_unverified_devices:
            kwargs["ignore_unverified_devices"] = True

        try:
            await self.client.room_send(
                room_id=room_id,
                message_type="m.reaction",
                content=content,
                **kwargs,
            )
            return True
        except TypeError:
            try:
                await self.client.room_send(
                    room_id=room_id,
                    message_type="m.reaction",
                    content=content,
                )
                return True
            except Exception:
                return False
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
        room_id: str,
        event_id: str,
        user_text: str,
        assistant_text: str,
    ) -> None:
        if not event_id:
            return

        try:
            suggested = await emoji_responder.on_assistant_response(
                platform="matrix",
                user_text=user_text or "",
                assistant_text=assistant_text or "",
                llm_client=llm_client,
                scope=room_id,
                room_id=room_id,
                event_id=event_id,
            )
            emoji = self._extract_hook_emoji(suggested)
            if not emoji:
                return
            await self._send_reaction(room_id=room_id, event_id=event_id, emoji=emoji)
        except Exception as exc:
            logger.debug(f"[emoji_responder] passive reaction skipped: {exc}")

    async def _resolve_room_ref(self, room_ref: str) -> Optional[str]:
        ref = (room_ref or "").strip()
        if not ref:
            return None

        def _norm_room_name(value: Any) -> str:
            text = str(value or "").strip()
            if not text:
                return ""
            if text.startswith("#"):
                text = text[1:]
            if ":" in text:
                text = text.split(":", 1)[0]
            return text.strip().lower()

        def _match_joined_room(value: str) -> Optional[str]:
            needle = _norm_room_name(value)
            if not needle:
                return None
            for room_id, room in (self.client.rooms or {}).items():
                candidates: List[str] = [str(room_id)]
                for attr in ("display_name", "name", "canonical_alias"):
                    v = getattr(room, attr, None)
                    if v:
                        candidates.append(str(v))
                aliases = getattr(room, "aliases", None)
                if isinstance(aliases, (list, tuple, set)):
                    candidates.extend([str(a) for a in aliases if a])
                for candidate in candidates:
                    if _norm_room_name(candidate) == needle:
                        return room_id
            return None

        # Friendly room names (for example "tater", "#tater", "General") can map to joined rooms.
        matched = _match_joined_room(ref)
        if matched:
            return matched

        if ref.startswith("#"):
            room_id = None
            try:
                resp = await self.client.room_resolve_alias(ref)
                room_id = getattr(resp, "room_id", None)
                if not room_id and isinstance(resp, dict):
                    room_id = resp.get("room_id")
            except Exception as e:
                logger.warning(f"[notifyq] Matrix failed to resolve alias {ref}: {e}")

            if not room_id:
                matched = _match_joined_room(ref)
                if matched:
                    return matched
                try:
                    join_resp = await self.client.join(ref)
                    room_id = getattr(join_resp, "room_id", None)
                    if not room_id and isinstance(join_resp, dict):
                        room_id = join_resp.get("room_id")
                except Exception as e:
                    logger.warning(f"[notifyq] Matrix join via alias failed for {ref}: {e}")
                    return None

            if room_id and room_id not in self.client.rooms:
                try:
                    await self.client.join(room_id)
                except Exception as e:
                    logger.warning(f"[notifyq] Matrix join failed for {room_id}: {e}")

            return room_id

        return ref

    async def _send_media_item(self, room_id: str, item: Dict[str, Any]):
        """
        New format (NO base64):
          {
            "type": "image"|"audio"|"video"|"file",
            "name": "foo.png",
            "mimetype": "image/png",
            "bytes": b"..."
          }
        Also supported:
          { ... "blob_key": "tater:blob:..." }   # bytes stored in Redis binary store
        Back-compat:
          { ... "data": "<base64>" }             # will still work if any old plugin returns it
        """
        try:
            try:
                from nio.crypto import attachments  # optional, for encrypted media
            except Exception:
                attachments = None

            kind = (item.get("type") or "file").lower()
            name = item.get("name") or "output.bin"
            mimetype = item.get("mimetype") or "application/octet-stream"

            raw: Optional[bytes] = None

            if "bytes" in item and isinstance(item["bytes"], (bytes, bytearray)):
                raw = bytes(item["bytes"])
            elif "blob_key" in item and isinstance(item["blob_key"], str):
                raw = load_blob(item["blob_key"])
            elif "data" in item and isinstance(item["data"], str):
                # back-compat only
                try:
                    import base64
                    raw = base64.b64decode(item["data"])
                except Exception:
                    raw = None

            if not raw:
                await self._send_with_trust(room_id, f"[{kind.capitalize()}: {name}]")
                return

            room = self.client.rooms.get(room_id)
            is_encrypted = bool(getattr(room, "encrypted", False))
            if is_encrypted and attachments is None:
                logger.warning(
                    "[Matrix] Encrypted room but nio.crypto.attachments unavailable; "
                    "sending unencrypted media (install matrix-nio[crypto] to fix)."
                )

            upload_bytes = raw
            file_obj = None
            if is_encrypted and attachments is not None:
                upload_bytes, file_obj = attachments.encrypt_attachment(raw)

            bio = BytesIO(upload_bytes)
            bio.seek(0)

            try:
                up = await self.client.upload(
                    bio,
                    content_type=mimetype,
                    filename=name,
                    filesize=len(upload_bytes),
                )
                if isinstance(up, tuple):
                    up = up[0]
                mxc = getattr(up, "content_uri", None)
                if not mxc:
                    await self._send_with_trust(room_id, f"[{kind.capitalize()}: {name}]")
                    return
            except Exception as e:
                logger.warning(f"[Matrix] media upload failed: {e}")
                await self._send_with_trust(room_id, f"[{kind.capitalize()}: {name}]")
                return

            msgtype = {
                "image": "m.image",
                "audio": "m.audio",
                "video": "m.video",
            }.get(kind, "m.file")

            content = {
                "msgtype": msgtype,
                "body": name,
                "info": {
                    "mimetype": mimetype,
                    "size": len(raw),
                },
            }

            if is_encrypted and file_obj is not None:
                file_payload = dict(file_obj)
                file_payload["url"] = mxc
                content["file"] = file_payload
            else:
                content["url"] = mxc

            kwargs = {}
            if self.trust_unverified_devices:
                kwargs["ignore_unverified_devices"] = True

            try:
                await self.client.room_send(
                    room_id=room_id,
                    message_type="m.room.message",
                    content=content,
                    **kwargs,
                )
            except TypeError:
                await self.client.room_send(
                    room_id=room_id,
                    message_type="m.room.message",
                    content=content,
                )
            except Exception as e:
                logger.warning(f"[Matrix] sending media event failed: {e}")
                await self._send_with_trust(room_id, f"[{kind.capitalize()}: {name}]")

        except Exception as e:
            logger.warning(f"[Matrix] _send_media_item unexpected error: {e}")
            await self._send_with_trust(room_id, f"[{(item.get('type') or 'file').capitalize()}: {item.get('name') or 'output'}]")

    # ---------- Login / lifecycle ----------
    async def login(self):
        if self.client.access_token:
            try:
                await self.client.whoami()
                logger.info("[Matrix] Using provided access token.")
            except Exception:
                logger.warning("[Matrix] Access token invalid; falling back to password.")
                self.client.access_token = None

        if not self.client.access_token:
            if not self.password:
                raise RuntimeError("Matrix: no valid access token or password.")
            resp = await self.client.login(password=self.password, device_name=self.device_name)
            if isinstance(resp, LoginResponse):
                logger.info(f"[Matrix] Logged in; device_id={self.client.device_id}")
            else:
                raise RuntimeError(f"[Matrix] Login error: {resp}")

        try:
            await self.client.keys_upload()
        except Exception as e:
            logger.debug(f"[Matrix] keys_upload skipped/failed: {e}")

        try:
            await self.client.keys_query()
        except Exception as e:
            logger.debug(f"[Matrix] keys_query failed: {e}")

        try:
            if self.client.device_id:
                await self.client.verify_device(self.user_id, self.client.device_id)
                logger.info(f"[Matrix] Verified own device {self.client.device_id}")
        except Exception as e:
            logger.debug(f"[Matrix] Self-verify failed (non-fatal): {e}")

        try:
            await _apply_avatar_from_redis(self.client)
        except Exception as e:
            logger.debug(f"[Matrix] Avatar apply skipped/failed: {e}")

    async def ensure_display_name(self):
        try:
            prof = await self.client.get_profile(self.user_id)
            dn = getattr(prof, "displayname", None)
            if dn:
                self.display_name_cache = dn
        except Exception:
            self.display_name_cache = None

    def _event_ts_ms(self, event) -> Optional[int]:
        ts = getattr(event, "server_timestamp", None)
        if isinstance(ts, int):
            return ts
        try:
            return int(getattr(event, "source", {}).get("origin_server_ts"))
        except Exception:
            return None

    def _should_process_event(self, event) -> bool:
        if self.resume_mode == "from_last_sync":
            return True
        ts = self._event_ts_ms(event)
        if ts is None:
            return True
        return ts >= (self.ready_ts_ms or 0)

    async def on_invite(self, room, event: InviteMemberEvent):
        try:
            logger.info(f"[Matrix] Invited to {room.room_id} by {event.sender}; joining…")
            await self.client.join(room.room_id)
            if self.trust_unverified_devices:
                await self._auto_trust_room_devices(room)
        except Exception as e:
            logger.error(f"[Matrix] Join failed: {e}")

    async def on_room_encryption(self, room, event):
        algo = getattr(event, "algorithm", None) or (getattr(event, "content", {}) or {}).get("algorithm")
        logger.info(f"[Matrix] Room {room.room_id} enabled encryption ({algo}); updating keys.")
        try:
            await self.client.keys_query()
        except Exception as e:
            logger.warning(f"[Matrix] keys_query failed: {e}")
        if self.trust_unverified_devices:
            await self._auto_trust_room_devices(room)

    @staticmethod
    def _download_response_bytes(resp) -> Optional[bytes]:
        if resp is None:
            return None
        body = getattr(resp, "body", None)
        if isinstance(body, (bytes, bytearray)):
            return bytes(body)
        data = getattr(resp, "data", None)
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        if isinstance(resp, dict):
            for key in ("body", "data", "content"):
                val = resp.get(key)
                if isinstance(val, (bytes, bytearray)):
                    return bytes(val)
        return None

    async def _capture_latest_image_from_event(self, room, event) -> Optional[Dict[str, Any]]:
        sender = getattr(event, "sender", "")
        if sender == self.user_id:
            return None

        source = getattr(event, "source", {}) or {}
        content = source.get("content") if isinstance(source, dict) else {}
        if not isinstance(content, dict):
            content = {}

        msgtype = str(content.get("msgtype") or "").strip().lower()
        media_type = {
            "m.image": "image",
            "m.video": "video",
            "m.audio": "audio",
            "m.file": "file",
        }.get(msgtype)
        if not media_type:
            return None

        mxc_url = str(content.get("url") or "").strip()
        if not mxc_url:
            return None

        default_name = {
            "image": "matrix_image",
            "video": "matrix_video",
            "audio": "matrix_audio",
            "file": "matrix_file",
        }.get(media_type, "matrix_file")
        name = str(content.get("body") or "").strip() or default_name
        info = content.get("info") if isinstance(content.get("info"), dict) else {}
        mimetype = str(info.get("mimetype") or content.get("mimetype") or "").strip().lower()

        try:
            downloaded = await self.client.download(mxc_url)
            media_bytes = self._download_response_bytes(downloaded)
            if not media_bytes:
                raise RuntimeError("Matrix download returned no bytes")
            blob_key = store_blob(media_bytes)
        except Exception as e:
            logger.warning(f"[Matrix] Failed to capture media event in {room.room_id}: {e}")
            return None

        if not mimetype:
            guessed = _guess_mime(media_bytes)
            if guessed:
                mimetype = guessed
            else:
                mimetype = {
                    "image": "image/png",
                    "video": "video/mp4",
                    "audio": "audio/mpeg",
                    "file": "application/octet-stream",
                }.get(media_type, "application/octet-stream")

        ref = {
            "type": media_type,
            "blob_key": blob_key,
            "name": name,
            "mimetype": mimetype,
            "source": "matrix_attachment",
            "size": len(media_bytes),
        }
        save_matrix_message(
            room.room_id,
            "user",
            sender,
            {
                "type": media_type,
                "name": name,
                "mimetype": mimetype,
                "blob_key": blob_key,
                "size": len(media_bytes),
            },
        )
        if media_type == "image":
            return ref
        return None

    async def _notify_queue_worker(self):
        while True:
            if self.stop_event and self.stop_event.is_set():
                break

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
            room_ref = targets.get("room_id")
            room_id = await self._resolve_room_ref(room_ref)
            if not room_id:
                logger.warning("[notifyq] Matrix missing or invalid room_id; dropping item.")
                continue

            message = (item.get("message") or "").strip()
            title = item.get("title")
            if not message and not attachments:
                continue

            if message:
                payload = f"{title}\n{message}" if title else message
            elif title:
                payload = title
            else:
                payload = ""

            try:
                if payload:
                    await self._send_with_trust(room_id, payload)
                for media in attachments:
                    await self._send_media_item(room_id, media)
            except Exception as e:
                logger.warning(f"[notifyq] Matrix send failed: {e}")

    # ---------- Message handling ----------
    async def _handle_textlike(self, room, sender, body, event_id: str = ""):
        if sender == self.user_id:
            return

        if not _should_respond(self.response_policy, body, self.user_id, self.display_name_cache):
            return

        save_matrix_message(room.room_id, "user", sender, body)

        effective_body = body
        stripped = _strip_matrix_mentions(body, self.user_id, self.display_name_cache)
        if stripped:
            effective_body = stripped
        elif _is_mention_only(body, self.user_id, self.display_name_cache):
            prev = _find_prev_user_message(
                room.room_id, sender, self.user_id, self.display_name_cache, exclude_text=body
            )
            if prev:
                effective_body = prev

        system_prompt = build_system_prompt()
        history = load_matrix_history(room.room_id)
        messages = history
        merged_registry = dict(pr.get_registry_snapshot() or {})
        merged_enabled = get_plugin_enabled

        async with self.typing(room.room_id):
            try:
                origin = {
                    "platform": "matrix",
                    "room_id": room.room_id,
                    "user": sender,
                    "request_id": str(time.time()),
                }
                origin = {k: v for k, v in origin.items() if v not in (None, "")}

                async def _wait_callback(func_name, plugin_obj):
                    if not plugin_obj:
                        return
                    if not plugin_supports_platform(plugin_obj, "matrix"):
                        return
                    if not hasattr(plugin_obj, "waiting_prompt_template"):
                        return
                    wait_msg = plugin_obj.waiting_prompt_template.format(mention=sender)
                    wait_response = await llm_client.chat(
                        messages=[
                            {"role": "system", "content": "Write one short, friendly status line."},
                            {"role": "user", "content": wait_msg},
                        ]
                    )
                    wait_text = str((wait_response.get("message", {}) or {}).get("content", "") or "").strip()
                    if wait_text:
                        await self._send_with_trust(room.room_id, wait_text)
                        save_matrix_message(
                            room.room_id, "assistant", "assistant",
                            {"marker": "plugin_wait", "content": wait_text}
                        )

                def _admin_guard(func_name):
                    needs_admin = False
                    if is_admin_only_plugin(func_name):
                        needs_admin = True

                    if needs_admin and not _admin_user_allowed(sender):
                        msg = (
                            "This tool is restricted to the configured admin user on Matrix."
                            if _get_setting("admin_user_id", "")
                            else "This tool is disabled because no Matrix admin user is configured."
                        )
                        return action_failure(
                            code="admin_only",
                            message=msg,
                            needs=[],
                            say_hint="Explain that this tool is restricted to the admin user on this platform.",
                        )
                    return None

                agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
                result = await run_cerberus_turn(
                    llm_client=llm_client,
                    platform="matrix",
                    history_messages=messages,
                    registry=merged_registry,
                    enabled_predicate=merged_enabled,
                    context={"client": self.client, "room": room, "sender": sender, "body": effective_body},
                    user_text=effective_body,
                    scope=f"room:{room.room_id}",
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
                    await self._send_with_trust(room.room_id, final_text)
                    save_matrix_message(
                        room.room_id, "assistant", "assistant",
                        {"marker": "plugin_response", "phase": "final", "content": final_text}
                    )

                artifacts = result.get("artifacts") or []
                for item in artifacts:
                    if isinstance(item, dict):
                        await self._send_media_item(room.room_id, item)

                if (final_text or artifacts) and event_id:
                    assistant_summary = final_text or "[Sent attachments]"
                    await self._maybe_passive_reaction(
                        room_id=room.room_id,
                        event_id=event_id,
                        user_text=effective_body,
                        assistant_text=assistant_summary,
                    )
                return

            except Exception as e:
                logger.error(f"[Matrix] Exception handling message: {e}", exc_info=True)
                await self._send_with_trust(room.room_id, "Sorry, I ran into an error while thinking.")

    async def on_text(self, room, event: RoomMessageText):
        if not self._should_process_event(event):
            return
        await self._handle_textlike(
            room,
            event.sender,
            event.body or "",
            str(getattr(event, "event_id", "") or ""),
        )

    async def on_image(self, room, event):
        if not self._should_process_event(event):
            return
        await self._capture_latest_image_from_event(room, event)

    async def on_media(self, room, event):
        if not self._should_process_event(event):
            return
        await self._capture_latest_image_from_event(room, event)

    async def on_megolm(self, room, event: MegolmEvent):
        if not self._should_process_event(event):
            return
        body = getattr(event, "body", None) or (getattr(event, "source", {}) or {}).get("content", {}).get("body", "")
        await self._handle_textlike(
            room,
            event.sender,
            body or "",
            str(getattr(event, "event_id", "") or ""),
        )

    async def sync_forever(self):
        self.client.add_event_callback(self.on_invite, InviteMemberEvent)
        self.client.add_event_callback(self.on_text, RoomMessageText)
        if RoomMessageImage:
            self.client.add_event_callback(self.on_image, RoomMessageImage)
        for media_cls in (RoomMessageVideo, RoomMessageAudio, RoomMessageFile):
            if media_cls:
                self.client.add_event_callback(self.on_media, media_cls)
        self.client.add_event_callback(self.on_megolm, MegolmEvent)
        if RoomEncryptionEvent:
            self.client.add_event_callback(self.on_room_encryption, RoomEncryptionEvent)

        await self.ensure_display_name()

        while True:
            try:
                await self.client.sync_forever(timeout=30000, full_state=False)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[Matrix] sync error: {e}")
                await asyncio.sleep(3)

    async def start(self, stop_event: Optional[threading.Event] = None):
        self.stop_event = stop_event
        await self.login()
        try:
            await self.client.sync(timeout=1000, full_state=False)
        except Exception:
            pass

        self.ready_ts_ms = int(time.time() * 1000)

        loop = asyncio.get_running_loop()
        self._notify_task = loop.create_task(self._notify_queue_worker())

        if self.trust_unverified_devices:
            try:
                for room in list(self.client.rooms.values()):
                    await self._auto_trust_room_devices(room)
            except Exception as e:
                logger.debug(f"[Matrix] Priming trust failed: {e}")

        self._sync_task = loop.create_task(self.sync_forever())
        if stop_event:
            while not stop_event.is_set():
                await asyncio.sleep(0.5)
            if self._sync_task and not self._sync_task.done():
                self._sync_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._sync_task
            if getattr(self, "_notify_task", None) and not self._notify_task.done():
                self._notify_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._notify_task
            await self.client.close()
        else:
            await asyncio.Event().wait()

# ---------------- Runner ----------------
def run(stop_event: Optional[threading.Event] = None):
    global llm_client
    llm_client = get_llm_client_from_env()
    logger.info(f"[Matrix] LLM client → {build_llm_host_from_env()}")

    bot = MatrixPlatform()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(bot.start(stop_event))
    finally:
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for task in pending:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
