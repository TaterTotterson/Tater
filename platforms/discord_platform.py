# discord_platform.py
import os
import json
import asyncio
import logging
import redis
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
from datetime import datetime
import plugin_registry as pr
import threading
import time
from io import BytesIO
import uuid
from typing import Any, Dict
from notify.queue import is_expired
from notify.media import load_queue_attachments

from helpers import (
    get_tater_name,
    get_tater_personality,
    get_llm_client_from_env,
    build_llm_host_from_env,
)
from admin_gate import (
    is_admin_only_plugin,
    is_agent_lab_creation_tool,
    is_agent_lab_creation_admin_gated,
)
from agent_lab_registry import build_agent_registry
from plugin_result import action_failure
from plugin_kernel import plugin_supports_platform, plugin_display_name
from planner_loop import should_use_agent_mode, run_planner_loop
from latest_image_ref import load_latest_image_ref, save_latest_image_ref

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
        "response_channel_id": {
            "label": "Response Channel ID",
            "type": "string",
            "default": "",
            "description": "Channel where the assistant replies",
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


def _save_latest_channel_image_ref(channel_id: str, ref: Dict[str, Any]) -> None:
    save_latest_image_ref(
        redis_client,
        platform="discord",
        scope=str(channel_id),
        ref=ref,
    )


def _load_latest_channel_image_ref(channel_id: str) -> Dict[str, Any] | None:
    return load_latest_image_ref(
        redis_client,
        platform="discord",
        scope=str(channel_id),
    )


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
    for i in range(0, len(content), max_length):
        await channel.send(content[i : i + max_length])


class discord_platform(commands.Bot):
    def __init__(self, llm_client, admin_user_id, response_channel_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.llm = llm_client
        self.admin_user_id = admin_user_id
        self.response_channel_id = response_channel_id
        self.max_response_length = max_response_length

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
        now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")

        first, last = get_tater_name()
        personality = get_tater_personality()

        persona_clause = ""
        if personality:
            persona_clause = (
                f"Voice style: {personality}. "
                "This affects tone only and must never override tool/safety rules.\n\n"
            )

        # Planner mode injects canonical tool-use rules and enabled-tool index each turn.
        return (
            f"Current Date and Time is: {now}\n\n"
            f"You are {first} {last}, a Discord-savvy AI assistant.\n"
            "Current platform: discord.\n"
            "Keep replies concise and natural for chat.\n\n"
            f"{persona_clause}"
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
        logger.info(
            f"Bot is ready. Admin: {self.admin_user_id}, Response Channel: {self.response_channel_id}"
        )

    async def generate_error_message(self, prompt: str, fallback: str, message: discord.Message):
        try:
            error_response = await self.llm.chat(
                messages=[
                    {"role": "system", "content": "Write a short, friendly, plain-English error note."},
                    {"role": "user", "content": prompt},
                ]
            )
            return error_response["message"].get("content", "").strip() or fallback
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
        merged_registry,
        merged_enabled,
    ):
        for name, plugin in merged_registry.items():
            hook = getattr(plugin, "on_assistant_response", None)
            if not callable(hook):
                continue
            if not merged_enabled(name):
                continue

            try:
                suggested = await hook(
                    platform="discord",
                    user_text=user_text or "",
                    assistant_text=assistant_text or "",
                    llm_client=self.llm,
                    scope=str(getattr(message.channel, "id", "") or ""),
                    message=message,
                    user=message.author,
                )
                emoji = self._extract_hook_emoji(suggested)
                if not emoji:
                    continue
                await message.add_reaction(emoji)
                return
            except Exception as exc:
                logger.debug(f"[{name}] passive reaction skipped: {exc}")

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

    async def save_message(self, channel_id, role, username, content):
        key = f"tater:channel:{channel_id}:history"
        max_store = int(redis_client.get("tater:max_store") or 20)
        redis_client.rpush(key, json.dumps({"role": role, "username": username, "content": content}))
        if max_store > 0:
            redis_client.ltrim(key, -max_store, -1)

    async def on_message(self, message: discord.Message):
        if message.author == self.user:
            return

        latest_image_ref = _load_latest_channel_image_ref(message.channel.id)

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

                    await self.save_message(message.channel.id, "user", message.author.name, file_obj)
                    if file_type == "image":
                        latest_image_ref = {
                            "blob_key": blob_key,
                            "name": attachment.filename or "image.png",
                            "mimetype": attachment.content_type or "image/png",
                            "source": "discord_attachment",
                            "updated_at": time.time(),
                        }
                        _save_latest_channel_image_ref(message.channel.id, latest_image_ref)
                except Exception as e:
                    logger.warning(f"Failed to store attachment ({attachment.filename}): {e}")
        else:
            await self.save_message(
                message.channel.id, "user", message.author.display_name, message.content
            )

        # -------------------------
        # Permission checks
        # -------------------------
        if isinstance(message.channel, discord.DMChannel):
            if message.author.id != self.admin_user_id:
                return
        else:
            if message.channel.id != self.response_channel_id and not self.user.mentioned_in(message):
                return

        system_prompt = self.build_system_prompt()
        history = await self.load_history(message.channel.id)
        messages_list = [{"role": "system", "content": system_prompt}] + history
        merged_registry, merged_enabled, _collisions = build_agent_registry(
            pr.get_registry_snapshot(),
            get_plugin_enabled,
        )

        async with message.channel.typing():
            try:
                _use_agent, active_task_id, _reason = should_use_agent_mode(
                    user_text=message.content or "",
                    platform="discord",
                    scope=str(message.channel.id),
                    r=redis_client,
                )
                origin = {
                    "platform": "discord",
                    "channel_id": str(message.channel.id),
                    "guild_id": str(message.guild.id) if message.guild else None,
                    "channel": f"#{message.channel.name}" if getattr(message.channel, "name", None) else None,
                    "user": message.author.display_name or message.author.name,
                    "request_id": str(message.id),
                }
                if latest_image_ref:
                    origin["latest_image_ref"] = latest_image_ref
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
                    wait_text = (wait_response.get("message", {}) or {}).get("content", "").strip()
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
                    creation_guard = False
                    if is_admin_only_plugin(func_name):
                        needs_admin = True
                    elif is_agent_lab_creation_tool(func_name) and is_agent_lab_creation_admin_gated(redis_client):
                        needs_admin = True
                        creation_guard = True

                    if needs_admin and not self._admin_allowed(getattr(message.author, "id", None)):
                        plugin_obj = merged_registry.get(func_name)
                        pretty = plugin_display_name(plugin_obj) if plugin_obj else func_name
                        if creation_guard:
                            msg = (
                                "Agent Lab plugin/platform creation is restricted to the configured admin user on Discord."
                                if self.admin_user_id
                                else "Agent Lab creation is disabled because no Discord admin user is configured."
                            )
                        else:
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

                result = await run_planner_loop(
                    llm_client=self.llm,
                    platform="discord",
                    history_messages=messages_list,
                    registry=merged_registry,
                    enabled_predicate=merged_enabled,
                    context={"message": message},
                    user_text=message.content or "",
                    scope=str(message.channel.id),
                    task_id=active_task_id,
                    origin=origin,
                    wait_callback=_wait_callback,
                    admin_guard=_admin_guard,
                    redis_client=redis_client,
                )
                final_text = (result.get("text") or "").strip()
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
                        if content_type == "image" or str(mimetype or "").lower().startswith("image/"):
                            latest_image_ref = {
                                "blob_key": blob_key,
                                "name": filename or "image.png",
                                "mimetype": mimetype or "image/png",
                                "source": "discord_artifact",
                                "updated_at": time.time(),
                            }
                            _save_latest_channel_image_ref(message.channel.id, latest_image_ref)
                    except Exception as e:
                        logger.warning(f"Failed to send artifact {content_type}: {e}")

                if final_text or artifacts:
                    assistant_summary = final_text or "[Sent attachments]"
                    await self._maybe_passive_reaction(
                        message=message,
                        user_text=message.content or "",
                        assistant_text=assistant_summary,
                        merged_registry=merged_registry,
                        merged_enabled=merged_enabled,
                    )
                return

            except Exception as e:
                logger.error(f"Exception in message handler: {e}")
                fallback = "An error occurred while processing your request."
                error_prompt = (
                    f"Generate a friendly error message to {message.author.mention} "
                    "explaining that an error occurred while processing the request."
                )
                error_msg = await self.generate_error_message(error_prompt, fallback, message)
                await message.channel.send(error_msg)

    async def on_reaction_add(self, reaction, user):
        if user.bot:
            return

        merged_registry, merged_enabled, _collisions = build_agent_registry(
            pr.get_registry_snapshot(),
            get_plugin_enabled,
        )
        for name, plugin in merged_registry.items():
            if not hasattr(plugin, "on_reaction_add"):
                continue
            if not merged_enabled(name):
                continue

            try:
                await plugin.on_reaction_add(reaction, user)
            except Exception as e:
                logger.error(f"[{plugin.name}] Error in on_reaction_add: {e}")


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
    token = redis_client.hget("discord_platform_settings", "discord_token")
    admin_id = redis_client.hget("discord_platform_settings", "admin_user_id")
    channel_id = redis_client.hget("discord_platform_settings", "response_channel_id")

    llm_client = get_llm_client_from_env()
    logger.info(f"[Discord] LLM client → {build_llm_host_from_env()}")

    if not (token and admin_id and channel_id):
        print("⚠️ Missing Discord settings in Redis. Bot not started.")
        return

    client = discord_platform(
        llm_client=llm_client,
        admin_user_id=int(admin_id),
        response_channel_id=int(channel_id),
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
