import asyncio
import inspect
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import redis

import plugin_registry as pr
from plugin_base import ToolPlugin
from plugin_kernel import plugin_supports_platform
from agent_lab_registry import build_agent_registry
from helpers import (
    get_llm_client_from_env,
    get_tater_name,
    get_tater_personality,
)
from planner_loop import should_use_agent_mode, run_planner_loop

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("ai_task_platform")
logger.setLevel(logging.INFO)

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True,
)

PLATFORM_SETTINGS = {
    "category": "AI Task Settings",
    "required": {},
}

REMINDER_KEY_PREFIX = "reminders:"
REMINDER_DUE_ZSET = "reminders:due"
SCHEDULER_EXCLUDED_TOOLS = {"send_message", "reminder", "ai_tasks"}
MEDIA_TYPES = {"image", "audio", "video", "file"}


class _StubObject:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class _StubChannel:
    def __init__(self, channel_id: str, name: str | None = None):
        self.id = int(channel_id) if str(channel_id).isdigit() else 0
        self.name = name or "scheduled"

    async def send(self, *args, **kwargs):
        return None


class _StubAuthor:
    def __init__(self, name: str):
        self.name = name or "scheduler"
        self.display_name = self.name
        self.mention = f"@{self.name}"


class _StubGuild:
    def __init__(self, guild_id: str | None):
        self.id = int(guild_id) if str(guild_id or "").isdigit() else 0


class _StubMessage:
    def __init__(self, content: str, channel: _StubChannel, author: _StubAuthor, guild: _StubGuild | None = None):
        self.content = content
        self.channel = channel
        self.author = author
        self.guild = guild
        self.attachments = []
        self.id = int(time.time() * 1000)


class _StubIRCBot:
    def privmsg(self, target, message):
        return None


class _StubMatrixRoom:
    def __init__(self, room_id: str):
        self.room_id = room_id or "scheduled"


class _StubMatrixClient:
    pass


def _has_platform_handler(plugin: ToolPlugin, platform: str) -> bool:
    handler_name = f"handle_{platform}"
    method = getattr(plugin.__class__, handler_name, None)
    if not callable(method):
        return False
    base = getattr(ToolPlugin, handler_name, None)
    if base is None:
        return True
    return method is not base


def _build_platform_context(
    platform: str,
    *,
    origin: Dict[str, Any],
    targets: Dict[str, Any],
    task_prompt: str,
    reminder_id: str,
) -> Dict[str, Any]:
    platform = (platform or "").strip().lower()
    user = (origin or {}).get("user") or "scheduler"

    if platform == "discord":
        channel_id = str(targets.get("channel_id") or origin.get("channel_id") or "0")
        channel_name = str(targets.get("channel") or origin.get("channel") or "scheduled").lstrip("#")
        guild_id = str(targets.get("guild_id") or origin.get("guild_id") or "0")
        channel = _StubChannel(channel_id, channel_name)
        author = _StubAuthor(user)
        guild = _StubGuild(guild_id) if guild_id else None
        message = _StubMessage(task_prompt, channel, author, guild)
        return {"message": message}

    if platform == "irc":
        return {
            "bot": _StubIRCBot(),
            "channel": str(targets.get("channel") or origin.get("channel") or "#scheduled"),
            "user": user,
            "raw_message": task_prompt,
            "raw": task_prompt,
        }

    if platform == "matrix":
        room_id = str(targets.get("room_id") or origin.get("room_id") or "scheduled")
        return {
            "client": _StubMatrixClient(),
            "room": _StubMatrixRoom(room_id),
            "sender": user,
            "body": task_prompt,
        }

    if platform == "telegram":
        chat_id = str(targets.get("chat_id") or origin.get("chat_id") or "0")
        update = {
            "message": {
                "message_id": reminder_id,
                "text": task_prompt,
                "chat": {"id": chat_id, "type": "private", "title": targets.get("channel")},
                "from": {"id": user, "username": user},
            }
        }
        return {"update": update}

    if platform == "homeassistant":
        return {"context": origin or {}}

    return {}


def get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")


def _load_reminder(reminder_id: str) -> Optional[Dict[str, Any]]:
    raw = redis_client.get(f"{REMINDER_KEY_PREFIX}{reminder_id}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _save_reminder(reminder_id: str, reminder: Dict[str, Any]) -> None:
    redis_client.set(f"{REMINDER_KEY_PREFIX}{reminder_id}", json.dumps(reminder))


def _delete_reminder(reminder_id: str) -> None:
    redis_client.delete(f"{REMINDER_KEY_PREFIX}{reminder_id}")


def _peek_next_due() -> Optional[Tuple[str, float]]:
    items = redis_client.zrange(REMINDER_DUE_ZSET, 0, 0, withscores=True)
    if not items:
        return None
    reminder_id, score = items[0]
    try:
        return str(reminder_id), float(score)
    except Exception:
        return None


def _pop_due(reminder_id: str) -> None:
    redis_client.zrem(REMINDER_DUE_ZSET, reminder_id)


def _format_due_sleep(now: float, due_ts: float) -> float:
    if due_ts <= now:
        return 0.0
    return min(1.0, max(0.1, due_ts - now))


def _is_media_dict(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    return str(item.get("type") or "").strip().lower() in MEDIA_TYPES


def _extract_structured_text(payload: Dict[str, Any]) -> str:
    for key in ("message", "content", "text", "summary"):
        val = payload.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def _flatten_result_payload(res: Any) -> Tuple[str, List[Dict[str, Any]]]:
    if res is None:
        return "", []

    if isinstance(res, str):
        return res.strip(), []

    if isinstance(res, list):
        text_parts: List[str] = []
        attachments: List[Dict[str, Any]] = []
        for item in res:
            if isinstance(item, str):
                if item.strip():
                    text_parts.append(item.strip())
                continue

            if _is_media_dict(item):
                attachments.append(dict(item))
                continue

            if isinstance(item, dict):
                extracted = _extract_structured_text(item)
                if extracted:
                    text_parts.append(extracted)
                else:
                    try:
                        text_parts.append(json.dumps(item, ensure_ascii=False))
                    except Exception:
                        text_parts.append(str(item))
                continue

            as_text = str(item).strip()
            if as_text:
                text_parts.append(as_text)

        return "\n".join(text_parts).strip(), attachments

    if isinstance(res, dict):
        if _is_media_dict(res):
            caption = _extract_structured_text(res)
            return caption, [dict(res)]

        extracted = _extract_structured_text(res)
        if extracted:
            return extracted, []
        try:
            return json.dumps(res, ensure_ascii=False), []
        except Exception:
            return str(res), []

    return str(res).strip(), []


def _has_webui_handler(plugin: ToolPlugin) -> bool:
    method = getattr(plugin.__class__, "handle_webui", None)
    base = getattr(ToolPlugin, "handle_webui", None)
    return callable(method) and method is not base


def _has_automation_handler(plugin: ToolPlugin) -> bool:
    return callable(getattr(plugin, "handle_automation", None))


def _supports_scheduled_tools(plugin_name: str, plugin: ToolPlugin, platform: str) -> bool:
    if plugin_name in SCHEDULER_EXCLUDED_TOOLS:
        return False
    if getattr(plugin, "notifier", False):
        return False

    if not plugin_supports_platform(plugin, platform):
        return False
    return _has_platform_handler(plugin, platform)


async def _render_scheduled_message(
    llm_client,
    reminder_id: str,
    task_prompt: str,
    origin: Dict[str, Any],
    platform: str,
    targets: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]]]:
    task_prompt = (task_prompt or "").strip()
    if not task_prompt:
        return "", []

    first, last = get_tater_name()
    personality = get_tater_personality().strip()
    persona = (
        f"Persona style: {personality}\n"
        if personality
        else ""
    )

    system_prompt = (
        f"You are {first} {last}, running a scheduled task.\n"
        f"{persona}"
        f"Current platform: {platform}.\n"
        "Tool strategy:\n"
        "- Answer directly when no external action/live data is required.\n"
        "- Examples that require list_plugins: weather/forecast, news, stocks, sports scores, downloads, music/song generation, image/video generation, camera feeds/snapshots (front/back yard, porch, driveway, garage), camera/sensor status, smart-home actions.\n"
        "- The user does not need to explicitly request tool use; if a tool is appropriate, use it.\n"
        "- Prefer using a tool over attempting to answer from scratch when a tool could fulfill the request.\n"
        "- If a tool may be needed, call list_plugins first.\n- If the user asks to control devices or services or interact with external systems, call list_plugins first.\n"
        "- If the user asks about a specific tool/plugin by name or asks what a tool can do, call list_plugins or get_plugin_help instead of guessing.\n"
        "- If the user asks to run a plugin by name (even approximate), call list_plugins and pick the closest match (ignore minor typos/plurals). If a close match exists, use it and do not claim it’s unavailable; optionally confirm.\n"
        "- When calling a plugin, use its id from list_plugins (not the display name).\n"
        "- If the user asks to schedule or run a recurring task (daily/weekly/every), use the `ai_tasks` plugin; do not create a platform or tool.\n"
        "- If you might need a tool or are unsure a capability exists, call list_plugins before saying it is unavailable.\n"
        "- If the user asks for multiple independent actions, you may call tools one at a time until all actions are complete, then respond.\n"
        "- Optionally call get_plugin_help before calling a plugin.\n"
        "- Ask concise follow-up questions for missing required inputs.\n"
        "- Only ask for inputs a tool explicitly requires (from list_plugins needs or get_plugin_help required_args). If defaults exist, proceed without asking.\n"
        "- Call only plugins compatible with the destination platform.\n"
        "- If unsupported here, explain and list supported platforms.\n"
        "- Tool calls must be JSON only: {\"function\":\"name\",\"arguments\":{...}}\n"
        "- Do NOT use repo_browser.* tool syntax.\n"
        "- Meta-tools: list_plugins, get_plugin_help, list_platforms_for_plugin.\n"
        "- Never claim success unless tool output confirms success.\n"
    )

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    user_prompt = (
        f"Scheduled task id: {reminder_id}\n"
        f"Current local time: {now_str}\n"
        f"Origin: {json.dumps(origin or {}, ensure_ascii=False)}\n\n"
        f"Task:\n{task_prompt}\n"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    merged_registry, merged_enabled, _collisions = build_agent_registry(
        pr.get_registry_snapshot(),
        get_plugin_enabled,
    )

    def _enabled(name: str) -> bool:
        plugin = merged_registry.get(name)
        if not plugin:
            return False
        if not merged_enabled(name):
            return False
        return _supports_scheduled_tools(name, plugin, platform)

    _use_agent, active_task_id, _reason = should_use_agent_mode(
        user_text=task_prompt,
        platform=platform,
        scope=f"ai_task:{reminder_id}",
        r=redis_client,
    )
    origin_payload = dict(origin or {})
    origin_payload = {k: v for k, v in origin_payload.items() if v not in (None, "")}
    context = _build_platform_context(
        platform,
        origin=origin_payload,
        targets=targets or {},
        task_prompt=task_prompt,
        reminder_id=reminder_id,
    )
    result = await run_planner_loop(
        llm_client=llm_client,
        platform=platform,
        history_messages=messages,
        registry=merged_registry,
        enabled_predicate=_enabled,
        context=context,
        user_text=task_prompt,
        scope=f"ai_task:{reminder_id}",
        task_id=active_task_id,
        origin=origin_payload,
        redis_client=redis_client,
    )
    text = (result.get("text") or "").strip()
    attachments = result.get("artifacts") or []
    if not text and not attachments:
        text = "Scheduled task completed."
    return text, attachments


def _notifier_supports_attachments(plugin) -> bool:
    notify_fn = getattr(plugin, "notify", None)
    if not callable(notify_fn):
        return False
    try:
        sig = inspect.signature(notify_fn)
    except Exception:
        return False
    if "attachments" in sig.parameters:
        return True
    return any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())


async def _call_notifier(plugin, title, message, targets, origin, meta, attachments=None):
    kwargs = {
        "title": title,
        "content": message,
        "targets": targets,
        "origin": origin,
        "meta": meta,
    }
    if attachments and _notifier_supports_attachments(plugin):
        kwargs["attachments"] = attachments
    result = plugin.notify(**kwargs)
    if asyncio.iscoroutine(result):
        result = await result
    return result


def run(stop_event: Optional[object] = None):
    logger.info("[AI Tasks] Platform started.")
    llm_client = get_llm_client_from_env()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        while True:
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                break

            now = time.time()
            next_due = _peek_next_due()
            if not next_due:
                time.sleep(0.5)
                continue

            reminder_id, due_ts = next_due
            sleep_for = _format_due_sleep(now, due_ts)
            if sleep_for > 0:
                time.sleep(sleep_for)
                continue

            _pop_due(reminder_id)
            reminder = _load_reminder(reminder_id)
            if not reminder:
                continue

            dest = str(reminder.get("platform") or "").strip().lower()
            title = reminder.get("title")
            message = reminder.get("message")
            task_prompt = reminder.get("task_prompt")
            targets = reminder.get("targets") or {}
            origin = reminder.get("origin") or {}
            meta = reminder.get("meta") or {}
            schedule = reminder.get("schedule") or {}

            if not dest or (not message and not task_prompt):
                logger.warning(f"[AI Tasks] Invalid reminder {reminder_id}; dropping.")
                _delete_reminder(reminder_id)
                continue

            notifier_name = f"notify_{dest}"
            merged_registry, merged_enabled, _collisions = build_agent_registry(
                pr.get_registry_snapshot(),
                get_plugin_enabled,
            )
            notifier = merged_registry.get(notifier_name)
            if not notifier or not getattr(notifier, "notifier", False):
                logger.warning(f"[AI Tasks] Missing notifier for {dest}; dropping reminder {reminder_id}.")
                _delete_reminder(reminder_id)
                continue

            if not merged_enabled(notifier_name):
                logger.info(f"[AI Tasks] Notifier {notifier_name} disabled; skipping reminder {reminder_id}.")
            else:
                try:
                    outbound_message = (message or "").strip()
                    rendered_text, rendered_attachments = loop.run_until_complete(
                        _render_scheduled_message(
                            llm_client=llm_client,
                            reminder_id=reminder_id,
                            task_prompt=(task_prompt or message or ""),
                            origin=origin,
                            platform=dest,
                            targets=targets,
                        )
                    )
                    if rendered_text:
                        outbound_message = rendered_text
                    else:
                        outbound_message = (message or "").strip()

                    if not outbound_message:
                        if rendered_attachments:
                            outbound_message = "Scheduled task completed with attachment."
                        else:
                            outbound_message = "Scheduled task completed."

                    result = loop.run_until_complete(
                        _call_notifier(
                            notifier,
                            title,
                            outbound_message,
                            targets,
                            origin,
                            meta,
                            attachments=rendered_attachments,
                        )
                    )
                    if isinstance(result, str) and result.startswith("Cannot queue"):
                        logger.warning(f"[AI Tasks] {result} (reminder {reminder_id})")
                except Exception as e:
                    logger.error(f"[AI Tasks] Failed to enqueue reminder {reminder_id}: {e}")

            interval = 0.0
            try:
                interval = float(schedule.get("interval_sec") or 0.0)
            except Exception:
                interval = 0.0

            if interval > 0:
                prev_next = float(schedule.get("next_run_ts") or time.time())
                next_run = prev_next + interval
                if next_run <= time.time():
                    next_run = time.time() + interval

                schedule["next_run_ts"] = next_run
                reminder["schedule"] = schedule
                _save_reminder(reminder_id, reminder)
                redis_client.zadd(REMINDER_DUE_ZSET, {reminder_id: next_run})
            else:
                _delete_reminder(reminder_id)

    finally:
        try:
            loop.close()
        except Exception:
            pass
        logger.info("[AI Tasks] Platform stopped.")
