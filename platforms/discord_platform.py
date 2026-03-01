# discord_platform.py
import os
import json
import asyncio
import logging
import re
from contextlib import asynccontextmanager, suppress
import redis
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
import plugin_registry as pr
import threading
import time
from io import BytesIO
import uuid
from typing import Any, Dict, Iterable
from notify.queue import is_expired
from notify.media import load_queue_attachments

from helpers import (
    get_tater_name,
    get_llm_client_from_env,
    build_llm_host_from_env,
)
from admin_gate import (
    is_admin_only_plugin,
)
from plugin_result import action_failure
from plugin_kernel import plugin_supports_platform, plugin_display_name
from cerberus import run_cerberus_turn, resolve_agent_limits
from emoji_responder import emoji_responder

load_dotenv()
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
max_response_length = int(os.getenv("MAX_RESPONSE_LENGTH", 1500))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("discord")

# NOTE: decode_responses=True means redis-py returns strings, not bytes.
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
NOTIFY_QUEUE_KEY = "notifyq:discord"
NOTIFY_POLL_INTERVAL = 0.5
ROOM_LABEL_PREFIX = "tater:room_label"
DISCORD_SETTINGS_KEY = "discord_platform_settings"
RESPONSE_CHANNEL_MAP_FIELD = "response_channel_ids_by_guild"
RESPONSE_CHANNEL_REFRESH_INTERVAL_SEC = 2.0

PLATFORM_SETTINGS = {
    "category": "Discord Settings",
    "required": {
        "discord_token": {
            "label": "Discord Bot Token",
            "type": "string",
            "default": "",
            "description": "Your Discord bot token",
        },
        "admin_user_id": {
            "label": "Admin User ID",
            "type": "string",
            "default": "",
            "description": "User ID allowed to DM the bot",
        },
        "response_channel_ids_by_guild": {
            "label": "Response Channel IDs By Guild",
            "type": "string",
            "default": "{}",
            "description": "JSON map of guild_id to comma-separated channel IDs where replies are allowed without pinging.",
        },
    },
}

# -------------------------
# Attachment storage (NO base64 in history)
# -------------------------
ATTACH_PREFIX = "tater:blob:discord"


def _blob_key():
    return f"{ATTACH_PREFIX}:{uuid.uuid4().hex}"


def store_blob(binary: bytes, ttl_seconds: int = 60 * 60 * 24 * 7) -> str:
    """
    Store raw bytes in Redis under a random key. Returns key.
    Uses redis_client with decode_responses=True, so we must use a separate bytes client
    OR encode via redis-py using a pipeline with a bytes client.

    Easiest/cleanest: create a second client with decode_responses=False for blobs.
    """
    blob_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=False)
    key = _blob_key().encode("utf-8")
    blob_client.set(key, binary)
    if ttl_seconds and ttl_seconds > 0:
        blob_client.expire(key, int(ttl_seconds))
    return key.decode("utf-8")


def load_blob(key: str) -> bytes | None:
    blob_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=False)
    return blob_client.get(key.encode("utf-8"))


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


def _discord_channel_label(channel: Any) -> str:
    if channel is None:
        return ""
    try:
        if isinstance(channel, discord.DMChannel):
            recipients = list(getattr(channel, "recipients", []) or [])
            if recipients:
                who = str(
                    getattr(recipients[0], "display_name", None)
                    or getattr(recipients[0], "name", None)
                    or ""
                ).strip()
                if who:
                    return f"DM: {who}"
            return "DM"
    except Exception:
        pass

    name = str(getattr(channel, "name", "") or "").strip()
    if not name:
        return ""
    if name.startswith("#"):
        return name
    return f"#{name}"


def parse_response_channel_ids(raw: Any) -> set[int]:
    if raw is None:
        return set()
    if isinstance(raw, (list, tuple, set)):
        parts = [str(item or "").strip() for item in raw]
    else:
        text = str(raw or "").strip()
        if not text:
            return set()
        parts = [token.strip() for token in re.split(r"[,\s]+", text) if token.strip()]

    out: set[int] = set()
    for token in parts:
        token = str(token or "").strip()
        if not token:
            continue
        token = token.strip("<>#")
        if token.startswith("!"):
            token = token[1:]
        if not token:
            continue
        try:
            parsed = int(token)
        except Exception:
            continue
        if parsed > 0:
            out.add(parsed)
    return out


def serialize_response_channel_ids(channel_ids: Iterable[int]) -> str:
    cleaned: set[int] = set()
    for item in channel_ids or []:
        try:
            parsed = int(item)
        except Exception:
            continue
        if parsed > 0:
            cleaned.add(parsed)
    return ",".join(str(item) for item in sorted(cleaned))


def parse_response_channel_map(raw: Any) -> dict[int, set[int]]:
    payload = raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except Exception:
            return {}
    if not isinstance(payload, dict):
        return {}

    out: dict[int, set[int]] = {}
    for key, value in payload.items():
        try:
            guild_id = int(str(key).strip())
        except Exception:
            continue
        if guild_id <= 0:
            continue
        out[guild_id] = parse_response_channel_ids(value)
    return out


def serialize_response_channel_map(channel_map: Dict[Any, Iterable[int]] | None) -> str:
    out: dict[str, str] = {}
    if isinstance(channel_map, dict):
        for key, value in channel_map.items():
            try:
                guild_id = int(str(key).strip())
            except Exception:
                continue
            if guild_id <= 0:
                continue
            out[str(guild_id)] = serialize_response_channel_ids(value or [])
    return json.dumps(out, sort_keys=True, separators=(",", ":"))


# ---- LM template helpers ----
def _to_template_msg(role, content, sender=None):
    """
    Shape messages for the Jinja template.
    - Strings -> keep as string (optionally prefix with sender for multi-user rooms)
    - Images  -> [{"type":"image"}] (placeholder)
    - Audio   -> [{"type":"text","text":"[Audio]"}] (placeholder)
    - plugin_wait -> skip
    - plugin_response (final) -> include text / placeholders / compact JSON
    - plugin_call -> stringify JSON as assistant text
    """

    # --- Skip waiting lines from tools ---
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    # --- Include final plugin responses in context (text only / placeholders) ---
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        phase = content.get("phase", "final")
        if phase != "final":
            return None

        payload = content.get("content")

        # 1) Plain string
        if isinstance(payload, str):
            txt = payload.strip()
            if len(txt) > 4000:
                txt = txt[:4000] + " …"
            return {"role": "assistant", "content": txt}

        # 2) Media placeholders (now stored as refs)
        if isinstance(payload, dict) and payload.get("type") in ("image", "audio", "video", "file"):
            kind = payload.get("type")
            name = payload.get("name") or ""
            return {
                "role": "assistant",
                "content": f"[{kind.capitalize()} from tool]{f' {name}' if name else ''}".strip(),
            }

        # 3) Structured text fields
        if isinstance(payload, dict):
            for key in ("summary", "text", "message", "content"):
                if isinstance(payload.get(key), str) and payload.get(key).strip():
                    txt = payload[key].strip()
                    if len(txt) > 4000:
                        txt = txt[:4000] + " …"
                    return {"role": "assistant", "content": txt}
            # Fallback: compact JSON
            try:
                compact = json.dumps(payload, ensure_ascii=False)
                if len(compact) > 2000:
                    compact = compact[:2000] + " …"
                return {"role": "assistant", "content": compact}
            except Exception:
                return None

    # --- Represent plugin calls as plain text (so history still makes sense) ---
    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps(
            {"function": content.get("plugin"), "arguments": content.get("arguments", {})},
            indent=2,
        )
        return {"role": "assistant" if role == "assistant" else role, "content": as_text}

    # --- Media placeholders ---
    if isinstance(content, dict) and content.get("type") == "image":
        name = content.get("name") or ""
        return {"role": role, "content": f"[Image attached]{f' {name}' if name else ''}".strip()}

    if isinstance(content, dict) and content.get("type") == "audio":
        name = content.get("name") or ""
        return {"role": role, "content": f"[Audio attached]{f' {name}' if name else ''}".strip()}

    if isinstance(content, dict) and content.get("type") == "video":
        name = content.get("name") or ""
        return {"role": role, "content": f"[Video attached]{f' {name}' if name else ''}".strip()}

    if isinstance(content, dict) and content.get("type") == "file":
        name = content.get("name") or ""
        return {"role": role, "content": f"[File attached]{f' {name}' if name else ''}".strip()}

    # --- Text + fallback ---
    if isinstance(content, str):
        if role == "user" and sender:
            return {"role": "user", "content": f"{sender}: {content}"}
        return {"role": role, "content": content}

    return {"role": role, "content": str(content)}


def _enforce_user_assistant_alternation(loop_messages):
    """
    Merge consecutive same-role turns to keep history compact.

    IMPORTANT:
    Do NOT insert a blank user message at the beginning.
    Some LLM backends/models (LM Studio/Qwen included) can return empty
    completions when an empty user turn (content="") appears in the prompt.
    """
    merged = []
    for m in loop_messages:
        if not m:
            continue
        if not merged:
            merged.append(m)
            continue

        if merged[-1]["role"] == m["role"]:
            a, b = merged[-1]["content"], m["content"]
            if isinstance(a, str) and isinstance(b, str):
                merged[-1]["content"] = (a + "\n\n" + b).strip()
            elif isinstance(a, list) and isinstance(b, list):
                merged[-1]["content"] = a + b
            else:
                merged[-1]["content"] = (
                    (a if isinstance(a, str) else str(a))
                    + "\n\n"
                    + (b if isinstance(b, str) else str(b))
                ).strip()
        else:
            merged.append(m)

    return merged


def get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")


def clear_channel_history(channel_id):
    key = f"tater:channel:{channel_id}:history"
    try:
        redis_client.delete(key)
        logger.info(f"Cleared chat history for channel {channel_id}.")
    except Exception as e:
        logger.error(f"Error clearing chat history for channel {channel_id}: {e}")
        raise


async def safe_send(channel, content, max_length=2000):
    content = str(content or "")
    for i in range(0, len(content), max_length):
        await channel.send(content[i : i + max_length])


TYPING_PULSE_INTERVAL_SEC = 4.0
TYPING_ERROR_LOG_COOLDOWN_SEC = 30.0


async def _trigger_typing_once(channel):
    """Best-effort one-shot typing signal across Discord channel types."""
    if hasattr(channel, "trigger_typing"):
        await channel.trigger_typing()
        return
    typing_ctx = getattr(channel, "typing", None)
    if callable(typing_ctx):
        async with channel.typing():
            return


async def _typing_pulse(channel, stop_event: asyncio.Event, *, interval_sec: float = TYPING_PULSE_INTERVAL_SEC):
    """Send Discord typing repeatedly while work is in-flight; tolerate transient network errors."""
    last_error_log = 0.0
    while not stop_event.is_set():
        try:
            await _trigger_typing_once(channel)
        except Exception as e:
            now = time.monotonic()
            if (now - last_error_log) >= TYPING_ERROR_LOG_COOLDOWN_SEC:
                logger.warning(f"[discord] typing indicator transient failure: {e}")
                last_error_log = now
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(1.0, float(interval_sec)))
        except asyncio.TimeoutError:
            continue


@asynccontextmanager
async def safe_typing(channel):
    """Best-effort typing indicator that keeps trying during long-running turns."""
    stop_event = asyncio.Event()
    try:
        await _trigger_typing_once(channel)
    except Exception as e:
        logger.warning(f"[discord] initial typing indicator failed: {e}")
    pulse_task = asyncio.create_task(_typing_pulse(channel, stop_event))
    try:
        yield
    finally:
        stop_event.set()
        pulse_task.cancel()
        with suppress(asyncio.CancelledError):
            await pulse_task


class discord_platform(commands.Bot):
    def __init__(
        self,
        llm_client,
        admin_user_id,
        response_channel_ids_by_guild=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.llm = llm_client
        self.admin_user_id = admin_user_id
        self.response_channel_ids_by_guild: dict[int, set[int]] = {}
        self._response_channel_map_raw_cache = serialize_response_channel_map(response_channel_ids_by_guild or {})
        self._response_channel_last_refresh = 0.0
        self.set_response_channel_map(response_channel_ids_by_guild)
        self.max_response_length = max_response_length

    def set_response_channel_map(self, channel_map: Any) -> dict[int, set[int]]:
        parsed = parse_response_channel_map(channel_map)
        self.response_channel_ids_by_guild = {int(k): set(v) for k, v in parsed.items()}
        self._response_channel_map_raw_cache = serialize_response_channel_map(self.response_channel_ids_by_guild)
        return {int(k): set(v) for k, v in self.response_channel_ids_by_guild.items()}

    def set_guild_response_channels(self, guild_id: Any, channel_ids: Any) -> set[int]:
        try:
            gid = int(guild_id)
        except Exception:
            return set()
        if gid <= 0:
            return set()
        normalized = parse_response_channel_ids(channel_ids)
        self.response_channel_ids_by_guild[gid] = set(normalized)
        self._response_channel_map_raw_cache = serialize_response_channel_map(self.response_channel_ids_by_guild)
        return set(normalized)

    def get_response_channels_for_guild(self, guild_id: Any) -> set[int]:
        try:
            gid = int(guild_id)
        except Exception:
            gid = 0
        if gid <= 0:
            return set()
        return set(self.response_channel_ids_by_guild.get(gid) or set())

    def is_response_channel(self, channel_id: Any, *, guild_id: Any = None) -> bool:
        try:
            parsed = int(channel_id)
        except Exception:
            return False
        allowed_channels = self.get_response_channels_for_guild(guild_id)
        return parsed in allowed_channels

    def _refresh_response_channel_map_from_redis(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - float(self._response_channel_last_refresh or 0.0)) < RESPONSE_CHANNEL_REFRESH_INTERVAL_SEC:
            return
        self._response_channel_last_refresh = now

        try:
            raw = redis_client.hget(DISCORD_SETTINGS_KEY, RESPONSE_CHANNEL_MAP_FIELD)
        except Exception:
            return
        raw_text = str(raw or "").strip()
        incoming = raw_text if raw_text else "{}"
        if incoming == self._response_channel_map_raw_cache:
            return

        parsed = parse_response_channel_map(incoming)
        if parsed == {} and raw_text and raw_text not in {"{}", "null"}:
            logger.warning("[discord] response_channel_ids_by_guild is invalid JSON; keeping current runtime map.")
            return
        self.response_channel_ids_by_guild = {int(k): set(v) for k, v in parsed.items()}
        self._response_channel_map_raw_cache = serialize_response_channel_map(self.response_channel_ids_by_guild)

    def _message_matches_response_channel(self, message: discord.Message, *, guild_id: Any) -> bool:
        channel = getattr(message, "channel", None)
        candidate_ids: set[int] = set()
        try:
            cid = int(getattr(channel, "id", 0) or 0)
            if cid > 0:
                candidate_ids.add(cid)
        except Exception:
            pass
        parent = getattr(channel, "parent", None)
        try:
            pid = int(getattr(parent, "id", 0) or 0)
            if pid > 0:
                candidate_ids.add(pid)
        except Exception:
            pass
        for cid in candidate_ids:
            if self.is_response_channel(cid, guild_id=guild_id):
                return True
        return False

    def _admin_allowed(self, user_id: int | None) -> bool:
        if not self.admin_user_id:
            return False
        try:
            return int(user_id or 0) == int(self.admin_user_id)
        except Exception:
            return False

    async def _send_notify_attachment(self, channel, attachment: dict):
        kind = str((attachment or {}).get("type") or "file").strip().lower() or "file"
        filename = str((attachment or {}).get("name") or f"{kind}.bin").strip()
        binary = None

        blob_key = (attachment or {}).get("blob_key")
        if isinstance(blob_key, str) and blob_key.strip():
            binary = load_blob(blob_key.strip())
        elif isinstance((attachment or {}).get("bytes"), (bytes, bytearray)):
            binary = bytes((attachment or {}).get("bytes"))

        if not binary:
            await safe_send(channel, f"[{kind.capitalize()}: {filename}]", self.max_response_length)
            return

        try:
            file_obj = discord.File(BytesIO(binary), filename=filename)
            await channel.send(file=file_obj)
        except Exception:
            await safe_send(channel, f"[{kind.capitalize()}: {filename}]", self.max_response_length)

    async def _notify_queue_worker(self):
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                item_json = await asyncio.to_thread(redis_client.lpop, NOTIFY_QUEUE_KEY)
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
                channel_id = targets.get("channel_id")
                channel_name = targets.get("channel")
                guild_id = targets.get("guild_id")

                # Backward compatibility for queued payloads that put names in channel_id.
                if not channel_name and channel_id:
                    raw_channel_id = str(channel_id).strip()
                    if raw_channel_id and not raw_channel_id.isdigit():
                        channel_name = raw_channel_id
                        channel_id = None

                channel = None
                if channel_id:
                    try:
                        cid = int(channel_id)
                    except Exception:
                        cid = None
                    if cid:
                        channel = self.get_channel(cid)
                        if channel is None:
                            try:
                                channel = await self.fetch_channel(cid)
                            except Exception:
                                channel = None

                if channel is None and channel_name:
                    name = str(channel_name).lstrip("#")
                    guild = None
                    if guild_id:
                        try:
                            gid = int(guild_id)
                            guild = self.get_guild(gid)
                        except Exception:
                            guild = None

                    if guild:
                        channel = discord.utils.get(guild.text_channels, name=name)
                    else:
                        for g in self.guilds:
                            channel = discord.utils.get(g.text_channels, name=name)
                            if channel:
                                break

                if channel is None:
                    logger.warning("[notifyq] Discord channel not found; dropping item.")
                    continue

                _save_room_label("discord", getattr(channel, "id", ""), _discord_channel_label(channel))

                message = (item.get("message") or "").strip()
                title = item.get("title")
                if not message and not attachments:
                    continue

                if message:
                    payload = f"**{title}**\n{message}" if title else message
                elif title:
                    payload = f"**{title}**"
                else:
                    payload = ""

                if payload:
                    await safe_send(channel, payload, self.max_response_length)

                for media in attachments:
                    await self._send_notify_attachment(channel, media)

            except Exception as e:
                logger.warning(f"[notifyq] Discord worker error: {e}")
                await asyncio.sleep(1)

    def build_system_prompt(self):
        # Platform preamble should be style/format only.
        return (
            "You are a Discord-savvy AI assistant.\n"
            "Keep replies concise and natural for chat.\n"
        )

    async def setup_hook(self):
        await self.add_cog(AdminCommands(self))
        try:
            synced = await self.tree.sync()
            logger.info(f"Synced {len(synced)} app commands.")
        except Exception as e:
            logger.error(f"Failed to sync app commands: {e}")
        # Start notifier queue worker
        try:
            self.loop.create_task(self._notify_queue_worker())
        except Exception as e:
            logger.warning(f"[notifyq] Failed to start Discord worker: {e}")

    async def on_ready(self):
        first, last = get_tater_name()
        activity = discord.Activity(
            name=first.lower(), state=last, type=discord.ActivityType.custom
        )
        await self.change_presence(activity=activity)
        try:
            for guild in list(self.guilds or []):
                for channel in list(getattr(guild, "text_channels", []) or []):
                    _save_room_label("discord", getattr(channel, "id", ""), _discord_channel_label(channel))
        except Exception:
            pass
        guild_override_count = len(self.response_channel_ids_by_guild)
        logger.info(
            f"Bot is ready. Admin: {self.admin_user_id}, "
            f"Guild-specific response configs: {guild_override_count}"
        )

    async def generate_error_message(self, prompt: str, fallback: str, message: discord.Message):
        try:
            error_response = await self.llm.chat(
                messages=[
                    {"role": "system", "content": "Write a short, friendly, plain-English error note."},
                    {"role": "user", "content": prompt},
                ]
            )
            return str(error_response["message"].get("content", "") or "").strip() or fallback
        except Exception as e:
            logger.error(f"Error generating error message: {e}")
            return fallback

    @staticmethod
    def _extract_hook_emoji(value) -> str:
        if isinstance(value, dict):
            value = value.get("emoji")
        if isinstance(value, str):
            return value.strip()
        return ""

    async def _maybe_passive_reaction(
        self,
        message: discord.Message,
        user_text: str,
        assistant_text: str,
    ):
        try:
            suggested = await emoji_responder.on_assistant_response(
                platform="discord",
                user_text=user_text or "",
                assistant_text=assistant_text or "",
                llm_client=self.llm,
                scope=str(getattr(message.channel, "id", "") or ""),
                message=message,
                user=message.author,
            )
            emoji = self._extract_hook_emoji(suggested)
            if emoji:
                await message.add_reaction(emoji)
        except Exception as exc:
            logger.debug(f"[emoji_responder] passive reaction skipped: {exc}")

    async def load_history(self, channel_id, limit=None):
        if limit is None:
            limit = int(redis_client.get("tater:max_llm") or 8)

        history_key = f"tater:channel:{channel_id}:history"
        raw_history = redis_client.lrange(history_key, -limit, -1)
        loop_messages = []

        for entry in raw_history:
            data = json.loads(entry)
            role = data.get("role", "user")
            sender = data.get("username", role)
            content = data.get("content")

            # Only user/assistant roles are meaningful for the template
            if role not in ("user", "assistant"):
                role = "assistant"

            templ = _to_template_msg(role, content, sender=sender if role == "user" else None)
            if templ is not None:
                loop_messages.append(templ)

        return _enforce_user_assistant_alternation(loop_messages)

    async def save_message(self, channel_id, role, username, content, *, user_id: str = ""):
        key = f"tater:channel:{channel_id}:history"
        max_store = int(redis_client.get("tater:max_store") or 20)
        payload: Dict[str, Any] = {"role": role, "username": username, "content": content}
        user_id_text = str(user_id or "").strip()
        if user_id_text:
            payload["user_id"] = user_id_text
        redis_client.rpush(key, json.dumps(payload))
        if max_store > 0:
            redis_client.ltrim(key, -max_store, -1)

    async def on_message(self, message: discord.Message):
        if message.author == self.user:
            return

        _save_room_label("discord", message.channel.id, _discord_channel_label(message.channel))
        input_artifacts: list[Dict[str, Any]] = []

        # -------------------------
        # Save user message + attachments (NO base64 in history)
        # -------------------------
        if message.attachments:
            for attachment in message.attachments:
                try:
                    if not attachment.content_type:
                        continue

                    file_bytes = await attachment.read()

                    if attachment.content_type.startswith("image/"):
                        file_type = "image"
                    elif attachment.content_type.startswith("audio/"):
                        file_type = "audio"
                    elif attachment.content_type.startswith("video/"):
                        file_type = "video"
                    else:
                        file_type = "file"

                    blob_key = store_blob(file_bytes)

                    file_obj = {
                        "type": file_type,
                        "name": attachment.filename,
                        "mimetype": attachment.content_type,
                        "blob_key": blob_key,
                        "size": len(file_bytes),
                    }

                    await self.save_message(
                        message.channel.id,
                        "user",
                        message.author.name,
                        file_obj,
                        user_id=str(message.author.id),
                    )
                    input_artifacts.append(
                        {
                            "type": file_type,
                            "blob_key": blob_key,
                            "name": attachment.filename or f"{file_type}.bin",
                            "mimetype": attachment.content_type or "application/octet-stream",
                            "source": "discord_attachment",
                            "size": len(file_bytes),
                        }
                    )
                except Exception as e:
                    logger.warning(f"Failed to store attachment ({attachment.filename}): {e}")
        else:
            await self.save_message(
                message.channel.id,
                "user",
                message.author.display_name,
                message.content,
                user_id=str(message.author.id),
            )

        # -------------------------
        # Permission checks
        # -------------------------
        if isinstance(message.channel, discord.DMChannel):
            if message.author.id != self.admin_user_id:
                return
        else:
            self._refresh_response_channel_map_from_redis()
            guild_id = getattr(getattr(message, "guild", None), "id", None)
            if not self._message_matches_response_channel(message, guild_id=guild_id) and not self.user.mentioned_in(message):
                return

        history = await self.load_history(message.channel.id)
        messages_list = history
        platform_preamble = self.build_system_prompt()
        merged_registry = dict(pr.get_registry_snapshot() or {})
        merged_enabled = get_plugin_enabled

        async with safe_typing(message.channel):
            try:
                is_dm = isinstance(message.channel, discord.DMChannel)
                scope_value = f"dm:{message.author.id}" if is_dm else f"channel:{message.channel.id}"
                origin = {
                    "platform": "discord",
                    "channel_id": str(message.channel.id),
                    "guild_id": str(message.guild.id) if message.guild else None,
                    "channel": f"#{message.channel.name}" if getattr(message.channel, "name", None) else None,
                    "user": message.author.display_name or message.author.name,
                    "user_id": str(message.author.id),
                    "chat_type": "dm" if is_dm else "channel",
                    "dm_user_id": str(message.author.id) if is_dm else None,
                    "request_id": str(message.id),
                }
                if input_artifacts:
                    origin["input_artifacts"] = [dict(item) for item in input_artifacts]
                origin = {k: v for k, v in origin.items() if v not in (None, "")}

                async def _wait_callback(func_name, plugin_obj):
                    if not plugin_obj:
                        return
                    if not plugin_supports_platform(plugin_obj, "discord"):
                        return
                    if not hasattr(plugin_obj, "waiting_prompt_template"):
                        return
                    wait_msg = plugin_obj.waiting_prompt_template.format(mention=message.author.mention)
                    wait_response = await self.llm.chat(
                        messages=[
                            {"role": "system", "content": "Write one short, friendly status line."},
                            {"role": "user", "content": wait_msg},
                        ]
                    )
                    wait_text = str((wait_response.get("message", {}) or {}).get("content", "") or "").strip()
                    if wait_text:
                        await self.save_message(
                            message.channel.id,
                            "assistant",
                            "assistant",
                            {"marker": "plugin_wait", "content": wait_text},
                        )
                        await safe_send(message.channel, wait_text, self.max_response_length)

                def _admin_guard(func_name):
                    needs_admin = False
                    if is_admin_only_plugin(func_name):
                        needs_admin = True

                    if needs_admin and not self._admin_allowed(getattr(message.author, "id", None)):
                        plugin_obj = merged_registry.get(func_name)
                        pretty = plugin_display_name(plugin_obj) if plugin_obj else func_name
                        msg = (
                            "This tool is restricted to the configured admin user on Discord."
                            if self.admin_user_id
                            else "This tool is disabled because no Discord admin user is configured."
                        )
                        return action_failure(
                            code="admin_only",
                            message=f"{pretty}: {msg}",
                            needs=[],
                            say_hint="Explain that this tool is restricted to the admin user on this platform.",
                        )
                    return None

                agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
                result = await run_cerberus_turn(
                    llm_client=self.llm,
                    platform="discord",
                    history_messages=messages_list,
                    registry=merged_registry,
                    enabled_predicate=merged_enabled,
                    context={"message": message},
                    user_text=message.content or "",
                    scope=scope_value,
                    origin=origin,
                    wait_callback=_wait_callback,
                    admin_guard=_admin_guard,
                    redis_client=redis_client,
                    max_rounds=agent_max_rounds,
                    max_tool_calls=agent_max_tool_calls,
                    platform_preamble=platform_preamble,
                )
                final_text = str(result.get("text") or "").strip()
                if final_text:
                    await safe_send(message.channel, final_text, self.max_response_length)
                    await self.save_message(
                        message.channel.id,
                        "assistant",
                        "assistant",
                        {"marker": "plugin_response", "phase": "final", "content": final_text},
                    )
                artifacts = result.get("artifacts") or []
                for item in artifacts:
                    if not isinstance(item, dict):
                        continue
                    content_type = item.get("type", "file")
                    filename = item.get("name", "output.bin")
                    mimetype = item.get("mimetype", "")
                    try:
                        binary = None
                        if "bytes" in item and isinstance(item["bytes"], (bytes, bytearray)):
                            binary = bytes(item["bytes"])
                        elif "blob_key" in item and isinstance(item["blob_key"], str):
                            binary = load_blob(item["blob_key"])
                        if binary is None:
                            await safe_send(
                                message.channel,
                                f"[{content_type.capitalize()}: {filename}]",
                                self.max_response_length,
                            )
                            continue

                        file = discord.File(BytesIO(binary), filename=filename)
                        await message.channel.send(file=file)

                        blob_key = store_blob(binary)
                        content_obj = {
                            "type": content_type,
                            "name": filename,
                            "mimetype": mimetype,
                            "blob_key": blob_key,
                            "size": len(binary),
                        }
                        await self.save_message(
                            message.channel.id,
                            "assistant",
                            "assistant",
                            {"marker": "plugin_response", "phase": "final", "content": content_obj},
                        )
                    except Exception as e:
                        logger.warning(f"Failed to send artifact {content_type}: {e}")

                if final_text or artifacts:
                    assistant_summary = final_text or "[Sent attachments]"
                    await self._maybe_passive_reaction(
                        message=message,
                        user_text=message.content or "",
                        assistant_text=assistant_summary,
                    )
                return

            except Exception as e:
                logger.exception(f"Exception in message handler: {e}")
                fallback = "An error occurred while processing your request."
                error_prompt = (
                    f"Generate a friendly error message to {message.author.mention} "
                    "explaining that an error occurred while processing the request."
                )
                error_msg = await self.generate_error_message(error_prompt, fallback, message)
                await message.channel.send(error_msg)

    async def on_reaction_add(self, reaction, user):
        try:
            await emoji_responder.on_reaction_add(reaction, user, llm_client=self.llm)
        except Exception as e:
            logger.debug(f"[emoji_responder] reaction handler skipped: {e}")


class AdminCommands(commands.Cog):
    def __init__(self, bot: discord_platform):
        self.bot = bot

    @app_commands.command(name="wipe", description="Clear chat history for this channel.")
    async def wipe(self, interaction: discord.Interaction):
        try:
            clear_channel_history(interaction.channel.id)
            await interaction.response.send_message("🧠 Wait What!?! What Just Happened!?!😭")
        except Exception as e:
            await interaction.response.send_message("Failed to clear channel history.")
            logger.error(f"Error in /wipe command: {e}")


async def setup_commands(client: commands.Bot):
    logger.info("Commands setup complete.")


def run(stop_event=None):
    token = redis_client.hget(DISCORD_SETTINGS_KEY, "discord_token")
    admin_id = redis_client.hget(DISCORD_SETTINGS_KEY, "admin_user_id")
    response_channel_ids_by_guild_raw = redis_client.hget(
        DISCORD_SETTINGS_KEY, RESPONSE_CHANNEL_MAP_FIELD
    )

    llm_client = get_llm_client_from_env()
    logger.info(f"[Discord] LLM client → {build_llm_host_from_env()}")

    if not (token and admin_id):
        print("⚠️ Missing Discord settings in Redis. Bot not started.")
        return

    try:
        admin_user_id = int(str(admin_id).strip())
    except Exception:
        print("⚠️ Invalid Discord admin_user_id in Redis. Bot not started.")
        return

    response_channel_ids_by_guild = parse_response_channel_map(response_channel_ids_by_guild_raw)

    client = discord_platform(
        llm_client=llm_client,
        admin_user_id=admin_user_id,
        response_channel_ids_by_guild=response_channel_ids_by_guild,
        command_prefix="!",
        intents=discord.Intents.all(),
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def run_bot():
        try:
            await client.start(token)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"❌ Discord bot crashed: {e}")

    def monitor_stop():
        if not stop_event:
            return
        while not stop_event.is_set():
            time.sleep(1)
        logger.info("🛑 Stop signal received for Discord platform. Logging out.")

        shutdown_complete = threading.Event()

        async def shutdown():
            try:
                await client.close()
                if hasattr(client, "http") and getattr(client.http, "session", None):
                    await client.http.session.close()
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error during Discord shutdown: {e}")
            finally:
                shutdown_complete.set()

        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(shutdown()))
        shutdown_complete.wait(timeout=15)

    if stop_event:
        threading.Thread(target=monitor_stop, daemon=True).start()

    try:
        loop.run_until_complete(run_bot())
    finally:
        if not loop.is_closed():
            loop.close()
