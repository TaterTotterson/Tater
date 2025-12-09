# platforms/matrix_platform.py
import os
import re
import json
import time
import redis
import asyncio
import logging
import threading
from datetime import datetime
from typing import Any, Dict, Optional, List
import contextlib
import base64
from io import BytesIO
import imghdr
import hashlib

from dotenv import load_dotenv
load_dotenv()

from plugin_registry import plugin_registry
from helpers import LLMClientWrapper, parse_function_json, get_tater_name, get_tater_personality

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

# Most of the spam you pasted comes from these:
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

# ---------------- Redis ----------------
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True,
)

# ---------------- LLM ----------------
llm_host = os.getenv("LLM_HOST", "127.0.0.1")
llm_port = os.getenv("LLM_PORT", "11434")
llm_client = LLMClientWrapper(host=f"http://{llm_host}:{llm_port}")

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
    Upload avatar from Redis key 'tater:avatar' and set as Matrix profile picture.
    Only re-uploads if the content changed since the last successful set.
    Caches:
      - matrix:last_avatar_hash  (sha1 of bytes)
      - matrix:last_avatar_mxc   (mxc://… from homeserver)
    """
    b64 = redis_client.get("tater:avatar")
    if not b64:
        return

    # Decode and hash
    try:
        data = base64.b64decode(b64)
    except Exception:
        logger.warning("[Matrix] Avatar in Redis is not valid base64; skipping.")
        return

    data_hash = hashlib.sha1(data).hexdigest()
    last_hash = redis_client.get("matrix:last_avatar_hash")
    last_mxc  = redis_client.get("matrix:last_avatar_mxc")

    # Skip upload if unchanged
    if last_hash and data_hash == last_hash and last_mxc:
        logger.info("[Matrix] Avatar unchanged; skipping upload.")
        return

    # Guess MIME + extension
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

    # Upload & set
    try:
        bio = BytesIO(data)
        bio.seek(0)

        up = await client.upload(
            bio,
            content_type=mime,
            filename=filename,
            filesize=len(data),
        )
        # Some nio versions return (UploadResponse, None)
        if isinstance(up, tuple):
            up = up[0]

        mxc = getattr(up, "content_uri", None)
        if not mxc:
            logger.warning(f"[Matrix] Upload returned no MXC URI: {up!r}")
            return

        await client.set_avatar(mxc)
        logger.info("[Matrix] Avatar updated from WebUI Redis.")

        # Cache after success
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

        # Represent non-text payloads as placeholders if present
        if isinstance(content, dict) and content.get("type") in ["image", "audio", "video", "file"]:
            name = content.get("name", "file")
            content = f"[{content['type'].capitalize()}: {name}]"

        if role not in ("user", "assistant"):
            role = "assistant"

        templ = _to_template_msg(role, content, sender=sender if role == "user" else None)
        if templ is not None:
            loop_messages.append(templ)
    return _enforce_user_assistant_alternation(loop_messages)

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
    if merged and merged[0]["role"] != "user":
        merged.insert(0, {"role": "user", "content": ""})
    return merged

# ---------------- System prompt (Matrix-scoped) ----------------
def build_system_prompt() -> str:
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    first, last = get_tater_name()
    personality = get_tater_personality()

    persona_clause = ""
    if personality:
        persona_clause = (
            f"You should speak and behave like {personality} "
            "while still being helpful, concise, and easy to understand. "
            "Keep the style subtle rather than over-the-top. "
            "Even while staying in character, you must strictly follow the tool-calling rules below.\n\n"
        )

    base = (
        f"You are {first} {last}, an AI assistant that operates on the Matrix chat service, "
        "with access to various tools and plugins.\n\n"
        f"{persona_clause}"
        "When a user requests one of these actions, reply ONLY with a JSON object in one of the following "
        "formats (and nothing else):\n\n"
    )

    tool_blocks = []
    for plugin in plugin_registry.values():
        platforms = getattr(plugin, "platforms", [])
        if ("matrix" in platforms) and get_plugin_enabled(plugin.name):
            desc = getattr(plugin, "description", "No description provided.")
            usage = getattr(plugin, "usage", "").strip()
            tool_blocks.append(
                f"Tool: {plugin.name}\n"
                f"Description: {desc}\n"
                f"{usage}"
            )

    tools = "\n\n".join(tool_blocks) if tool_blocks else "No tools are currently available."

    guard = (
        "Only call a tool if the user's latest message clearly requests an action — such as 'generate', "
        "'summarize', or 'download'. Never call a tool in response to casual or friendly messages like 'thanks', "
        "'lol', or 'cool'\n"
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"{base}\n\n"
        f"{tools}\n\n"
        f"{guard}"
        "If no function is needed, reply normally.\n"
    )

# ---------------- Mention helpers & trigger policy ----------------
def _keywords() -> List[str]:
    raw = _get_setting("mention_keywords", "")
    return [s.strip().lower() for s in raw.split(",") if s.strip()]

def _should_respond(policy: str, body: str, my_user_id: str, my_display: Optional[str]) -> bool:
    """
    Return True if the bot should respond to this message under the current policy.
    'mention_only' -> respond only when mentioned or keyword matched.
    'all_messages' -> respond to every message.
    """
    if policy == "all_messages":
        return True

    # mention_only
    body_l = (body or "").lower()

    # explicit MXID
    if my_user_id and my_user_id.lower() in body_l:
        return True

    # display name
    if my_display and my_display.lower() in body_l:
        return True

    # localpart of MXID (@local:server)
    try:
        if my_user_id.startswith("@"):
            localpart = my_user_id[1:].split(":", 1)[0].lower()
            if localpart and localpart in body_l:
                return True
    except Exception:
        pass

    # custom keywords
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
        # strict new default, no back-compat
        self.response_policy = _get_setting("response_policy", "mention_only")
        self.max_chunk = _get_int_setting("max_response_length", 4000)
        self.store_path = _get_setting("matrix_store_path", "/app/matrix-store")
        self.pickle_key = _get_setting("matrix_pickle_key", os.getenv("MATRIX_PICKLE_KEY", ""))
        self.resume_mode = _get_setting("resume_mode", "from_now")
        self.trust_unverified_devices = _get_bool_setting("trust_unverified_devices", True)
        self.ready_ts_ms: Optional[int] = None

        # Ensure store dir exists and is writable
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
            encryption_enabled=True,          # E2EE-ready
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
        """
        Try to persist trust flag updates in the store (works on SqliteStore).
        We don't assume any specific nio version—best-effort only.
        """
        try:
            store = getattr(self.client, "store", None)
            if not store:
                return
            # Common variants across nio versions:
            if hasattr(store, "save_device"):
                try:
                    # some versions want (user_id, device)
                    store.save_device(user_id, dev)
                except TypeError:
                    # some versions want (device) only
                    store.save_device(dev)
            elif hasattr(store, "save_device_keys"):
                try:
                    store.save_device_keys(user_id, dev)
                except Exception:
                    pass
        except Exception:
            pass

    async def _auto_trust_room_devices(self, room) -> None:
        """
        If enabled, mark UNVERIFIED/blacklisted devices for all room members as
        trusted so we can encrypt to them. Also clears 'blacklisted' flags.
        """
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
                devices = store_map[uid]  # mapping device_id -> Device
            except Exception:
                devices = {}
            for dev_id, dev in (devices or {}).items():
                try:
                    # Clear blacklist if set
                    if hasattr(dev, "blacklisted") and getattr(dev, "blacklisted"):
                        try:
                            dev.blacklisted = False
                            logger.info(f"[Matrix] Cleared blacklist for {uid} {dev_id}")
                            await self._persist_device_if_possible(uid, dev)
                        except Exception:
                            logger.debug(f"[Matrix] Could not clear blacklist for {uid} {dev_id}")

                    # Preferred: official verify if available
                    if crypto and hasattr(crypto, "verify_device"):
                        try:
                            crypto.verify_device(uid, dev_id)
                            logger.info(f"[Matrix] Verified device {dev_id} for {uid} (crypto.verify_device)")
                            await self._persist_device_if_possible(uid, dev)
                            continue
                        except Exception:
                            pass

                    # Older nio: boolean flag
                    if hasattr(dev, "verified"):
                        try:
                            dev.verified = True
                            logger.info(f"[Matrix] Marked device {dev_id} for {uid} as verified (bool).")
                            await self._persist_device_if_possible(uid, dev)
                            continue
                        except Exception:
                            pass

                    # Newer nio: enum
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

    async def _send_media_item(self, room_id: str, item: Dict[str, Any]):
        """
        Accepts a dict like:
          {"type":"image"|"audio"|"video"|"file",
           "name":"foo.png",
           "data":"<base64>",
           "mimetype":"image/png"}
        Uploads to Matrix and sends the correct m.room.message event.
        If the room is encrypted and attachments helpers are available,
        uses the encrypted 'file' payload per the Matrix spec.
        """
        from io import BytesIO
        try:
            try:
                from nio.crypto import attachments  # optional, for encrypted media
            except Exception:
                attachments = None

            kind = (item.get("type") or "file").lower()
            name = item.get("name") or "output.bin"
            mimetype = item.get("mimetype") or "application/octet-stream"
            b64 = item.get("data")
            if not b64:
                await self._send_with_trust(room_id, f"[{kind.capitalize()}: {name}]")
                return

            try:
                raw = base64.b64decode(b64)
            except Exception:
                await self._send_with_trust(room_id, f"[{kind.capitalize()}: {name}]")
                return

            # is the room encrypted?
            room = self.client.rooms.get(room_id)
            is_encrypted = bool(getattr(room, "encrypted", False))
            if is_encrypted and attachments is None:
                logger.warning("[Matrix] Encrypted room but nio.crypto.attachments unavailable; "
                               "sending unencrypted media (install matrix-nio[crypto] to fix).")

            # choose bytes to upload (ciphertext if encrypted+helpers)
            upload_bytes = raw
            file_obj = None
            if is_encrypted and attachments is not None:
                # returns (ciphertext_bytes, file_info_dict{iv,key,hashes})
                upload_bytes, file_obj = attachments.encrypt_attachment(raw)

            # upload to homeserver
            bio = BytesIO(upload_bytes)
            bio.seek(0)
            try:
                up = await self.client.upload(
                    bio,
                    content_type=mimetype,
                    filename=name,
                    filesize=len(upload_bytes),
                )
                if isinstance(up, tuple):  # older nio may return (resp, None)
                    up = up[0]
                mxc = getattr(up, "content_uri", None)
                if not mxc:
                    await self._send_with_trust(room_id, f"[{kind.capitalize()}: {name}]")
                    return
            except Exception as e:
                logger.warning(f"[Matrix] media upload failed: {e}")
                await self._send_with_trust(room_id, f"[{kind.capitalize()}: {name}]")
                return

            # build the event content
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
                    "size": len(raw),  # original size
                },
            }

            if is_encrypted and file_obj is not None:
                # encrypted media: include 'file' (with url) and NO top-level 'url'
                file_payload = dict(file_obj)
                file_payload["url"] = mxc
                content["file"] = file_payload
            else:
                # unencrypted media: plain 'url'
                content["url"] = mxc

            # send
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
                # older nio without ignore_unverified_devices
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

    class _TypingScope:
        """
        Async context manager to show 'typing…' in a Matrix room.
        Periodically refreshes the typing notice until exited.
        """
        def __init__(self, client, room_id: str, refresh_ms: int = 20000):
            self.client = client
            self.room_id = room_id
            self.refresh_ms = max(5000, int(refresh_ms))  # safety floor
            self._task = None
            self._alive = False

        async def _pinger(self):
            # Keep the typing notice alive; servers typically expire it ~30s.
            try:
                while self._alive:
                    try:
                        await self.client.room_typing(self.room_id, True, timeout=self.refresh_ms)
                    except Exception:
                        # Non-fatal; try again next cycle
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
            # Best-effort send 'stopped typing'
            try:
                await self.client.room_typing(self.room_id, False)
            except Exception:
                pass

    def typing(self, room_id: str) -> "_TypingScope":
        """Convenience factory to use: `async with self.typing(room.room_id): ...`"""
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
                continue  # avoid empty message events

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
        """
        Send text to a room; if encryption errors complain about unverified/blacklisted
        devices, auto-trust (if enabled) and retry once.
        """
        room = self.client.rooms.get(room_id)
        if room and self.trust_unverified_devices:
            # Proactively trust before sending to avoid first-failure loop
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

    # ---------- Login / lifecycle ----------
    async def login(self):
        # Try access token first
        if self.client.access_token:
            try:
                await self.client.whoami()
                logger.info("[Matrix] Using provided access token.")
            except Exception:
                logger.warning("[Matrix] Access token invalid; falling back to password.")
                self.client.access_token = None  # force password path

        # Password login if needed
        if not self.client.access_token:
            if not self.password:
                raise RuntimeError("Matrix: no valid access token or password.")
            resp = await self.client.login(password=self.password, device_name=self.device_name)
            if isinstance(resp, LoginResponse):
                logger.info(f"[Matrix] Logged in; device_id={self.client.device_id}")
            else:
                raise RuntimeError(f"[Matrix] Login error: {resp}")

        # Upload our device keys (safe to call repeatedly)
        try:
            await self.client.keys_upload()
        except Exception as e:
            logger.debug(f"[Matrix] keys_upload skipped/failed: {e}")

        # Populate the local device store with latest keys
        try:
            await self.client.keys_query()
        except Exception as e:
            logger.debug(f"[Matrix] keys_query failed: {e}")

        # Mark our own device as verified (helps other clients trust us)
        try:
            if self.client.device_id:
                await self.client.verify_device(self.user_id, self.client.device_id)
                logger.info(f"[Matrix] Verified own device {self.client.device_id}")
        except Exception as e:
            logger.debug(f"[Matrix] Self-verify failed (non-fatal): {e}")

        # Auto-verify every known, non-blacklisted device in our device store
        try:
            store = getattr(self.client, "device_store", None)
            devices_by_user = getattr(store, "devices", {}) if store else {}
            for user_id, devs in devices_by_user.items():
                for dev_id, dev in devs.items():
                    if getattr(dev, "blacklisted", False):
                        continue
                    if getattr(dev, "verified", False):
                        continue
                    try:
                        await self.client.verify_device(user_id, dev_id)
                        logger.info(f"[Matrix] Auto-verified device {dev_id} for {user_id}")
                    except Exception as ve:
                        logger.debug(f"[Matrix] Could not verify {user_id} {dev_id}: {ve}")
        except Exception as e:
            logger.debug(f"[Matrix] Auto-trust pass failed (non-fatal): {e}")

        # Apply avatar from WebUI (Redis key 'tater:avatar')
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

    # ---------- Message handling ----------
    async def _handle_textlike(self, room, sender, body):
        if sender == self.user_id:
            return

        if not _should_respond(self.response_policy, body, self.user_id, self.display_name_cache):
            logger.debug(
                "[Matrix] Ignoring due to policy. policy=%s display=%s user_id=%s body=%r",
                self.response_policy, self.display_name_cache, self.user_id, body
            )
            return

        save_matrix_message(room.room_id, "user", sender, body)

        system_prompt = build_system_prompt()
        history = load_matrix_history(room.room_id)
        messages = [{"role": "system", "content": system_prompt}] + history

        # ← NEW: show typing while thinking / running plugins
        async with self.typing(room.room_id):
            try:
                resp = await llm_client.chat(messages)
                text = resp["message"].get("content", "").strip()
                if not text:
                    await self._send_with_trust(room.room_id, "I'm not sure how to respond.")
                    return

                call = parse_function_json(text)
                if call and isinstance(call, dict) and "function" in call:
                    func = call["function"]
                    args = call.get("arguments", {})

                    save_matrix_message(
                        room.room_id, "assistant", "assistant",
                        {"marker": "plugin_call", "plugin": func, "arguments": args}
                    )

                    if func in plugin_registry and get_plugin_enabled(func):
                        plugin = plugin_registry[func]

                        if hasattr(plugin, "waiting_prompt_template"):
                            wait_prompt = plugin.waiting_prompt_template.format(mention=self.display_name_cache or "there")
                            wait_resp = await llm_client.chat(
                                messages=[
                                    {"role": "system", "content": "Write one short, friendly status line."},
                                    {"role": "user", "content": wait_prompt}
                                ]
                            )
                            wait_text = wait_resp["message"].get("content", "").strip()
                            await self._send_with_trust(room.room_id, wait_text)
                            save_matrix_message(
                                room.room_id, "assistant", "assistant",
                                {"marker": "plugin_wait", "content": wait_text}
                            )

                        handler = getattr(plugin, "handle_matrix", None)
                        if not callable(handler):
                            msg = f"Function `{func}` is not available on Matrix."
                            await self._send_with_trust(room.room_id, msg)
                            save_matrix_message(room.room_id, "assistant", "assistant", msg)
                            return

                        try:
                            result = await handler(
                                client=self.client,
                                room=room,
                                sender=sender,
                                body=body,
                                args=args,
                                llm_client=llm_client
                            )

                            # --- Handle plugin returns like Discord/WebUI ---
                            if isinstance(result, list):
                                for item in result:
                                    if isinstance(item, str):
                                        await self._send_with_trust(room.room_id, item)
                                        save_matrix_message(
                                            room.room_id, "assistant", "assistant",
                                            {"marker": "plugin_response", "phase": "final", "content": item}
                                        )
                                    elif isinstance(item, dict):
                                        if item.get("type") in ("image", "audio", "video", "file"):
                                            await self._send_media_item(room.room_id, item)
                                        else:
                                            kind = item.get("type", "file").capitalize()
                                            name = item.get("name", "output")
                                            await self._send_with_trust(room.room_id, f"[{kind}: {name}]")
                                        save_matrix_message(
                                            room.room_id, "assistant", "assistant",
                                            {"marker": "plugin_response", "phase": "final", "content": item}
                                        )

                            elif isinstance(result, dict):
                                if result.get("type") in ("image", "audio", "video", "file"):
                                    await self._send_media_item(room.room_id, result)
                                else:
                                    kind = result.get("type", "file").capitalize()
                                    name = result.get("name", "output")
                                    await self._send_with_trust(room.room_id, f"[{kind}: {name}]")
                                save_matrix_message(
                                    room.room_id, "assistant", "assistant",
                                    {"marker": "plugin_response", "phase": "final", "content": result}
                                )

                            elif isinstance(result, str):
                                await self._send_with_trust(room.room_id, result)
                                save_matrix_message(
                                    room.room_id, "assistant", "assistant",
                                    {"marker": "plugin_response", "phase": "final", "content": result}
                                )
                            else:
                                logger.debug(f"[{func}] Plugin returned unrecognized type: {type(result)}")
                            # -----------------------

                        except Exception:
                            logger.exception(f"[Matrix] Plugin '{func}' error")
                            msg = f"I tried to run {func} but hit an error."
                            await self._send_with_trust(room.room_id, msg)
                            save_matrix_message(room.room_id, "assistant", "assistant", msg)
                        return

                # No function call; plain assistant text
                await self._send_with_trust(room.room_id, text)
                save_matrix_message(room.room_id, "assistant", "assistant", text)

            except Exception as e:
                logger.error(f"[Matrix] Exception handling message: {e}")
                await self._send_with_trust(room.room_id, "Sorry, I ran into an error while thinking.")

    async def on_text(self, room, event: RoomMessageText):
        if not self._should_process_event(event):
            return
        await self._handle_textlike(room, event.sender, event.body or "")

    async def on_megolm(self, room, event: MegolmEvent):
        if not self._should_process_event(event):
            return
        body = getattr(event, "body", None) or (getattr(event, "source", {}) or {}).get("content", {}).get("body", "")
        await self._handle_textlike(room, event.sender, body or "")

    async def sync_forever(self):
        self.client.add_event_callback(self.on_invite, InviteMemberEvent)
        self.client.add_event_callback(self.on_text, RoomMessageText)
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

        # Mark "now" as the line after which we will process events (from_now mode)
        self.ready_ts_ms = int(time.time() * 1000)

        # Proactively prime trust for all currently joined rooms
        if self.trust_unverified_devices:
            try:
                for room in list(self.client.rooms.values()):
                    await self._auto_trust_room_devices(room)
            except Exception as e:
                logger.debug(f"[Matrix] Priming trust failed: {e}")

        loop = asyncio.get_running_loop()
        self._sync_task = loop.create_task(self.sync_forever())
        if stop_event:
            while not stop_event.is_set():
                await asyncio.sleep(0.5)
            if self._sync_task and not self._sync_task.done():
                self._sync_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._sync_task
            await self.client.close()
        else:
            await asyncio.Event().wait()

# ---------------- Runner ----------------
def run(stop_event: Optional[threading.Event] = None):
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