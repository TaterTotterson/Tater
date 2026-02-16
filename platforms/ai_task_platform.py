import asyncio
import json
import logging
import os
import time
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import redis

import plugin_registry as pr
from plugin_base import ToolPlugin
from plugin_kernel import plugin_supports_platform
from agent_lab_registry import build_agent_registry
from helpers import (
    get_llm_client_from_env,
)
from cerberus import run_cerberus_turn, resolve_agent_limits
from notify import dispatch_notification

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


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _normalize_weekdays(raw: Any) -> List[int]:
    if not isinstance(raw, list):
        return []
    out: List[int] = []
    for item in raw:
        day = _as_int(item, -1)
        if 0 <= day <= 6:
            out.append(day)
    if not out:
        return []
    return sorted(set(out))


def _next_local_time_occurrence(
    *,
    now_ts: float,
    hour: int,
    minute: int,
    second: int,
    weekdays: Optional[List[int]] = None,
) -> float:
    now_local = datetime.fromtimestamp(float(now_ts)).astimezone()
    h = min(23, max(0, int(hour)))
    m = min(59, max(0, int(minute)))
    s = min(59, max(0, int(second)))

    candidate = now_local.replace(hour=h, minute=m, second=s, microsecond=0)
    if candidate.timestamp() <= now_ts:
        candidate = candidate + timedelta(days=1)

    day_filter = _normalize_weekdays(weekdays or [])
    if not day_filter:
        return candidate.timestamp()

    while candidate.weekday() not in day_filter:
        candidate = candidate + timedelta(days=1)
    return candidate.timestamp()


def _next_run_for_schedule(schedule: Dict[str, Any], now_ts: float) -> float:
    interval = _as_float(schedule.get("interval_sec"), 0.0)
    if interval <= 0:
        return 0.0

    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    recurrence_kind = str(recurrence.get("kind") or "").strip().lower()
    if recurrence_kind in {"daily_local_time", "weekly_local_time"}:
        hour = _as_int(recurrence.get("hour"), 0)
        minute = _as_int(recurrence.get("minute"), 0)
        second = _as_int(recurrence.get("second"), 0)
        weekdays = recurrence.get("weekdays") if recurrence_kind == "weekly_local_time" else recurrence.get("weekdays")
        return _next_local_time_occurrence(
            now_ts=now_ts,
            hour=hour,
            minute=minute,
            second=second,
            weekdays=weekdays if isinstance(weekdays, list) else None,
        )

    # Fallback recurring schedule: preserve phase and avoid restart drift.
    # Example: if due at 06:00 daily and process restarts at 21:00, next remains 06:00 next day.
    prev_next = _as_float(schedule.get("next_run_ts"), 0.0)
    anchor = _as_float(schedule.get("anchor_ts"), 0.0)
    base = prev_next if prev_next > 0 else anchor
    if base <= 0:
        base = now_ts

    if now_ts < base:
        return base

    steps = int(math.floor((now_ts - base) / interval)) + 1
    return base + (interval * float(max(1, steps)))


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


def _normalize_runtime_task_prompt(task_prompt: str) -> str:
    text = str(task_prompt or "").strip()
    if not text:
        return ""
    try:
        from kernel_tools import _ai_tasks_clean_task_prompt
        normalized = str(_ai_tasks_clean_task_prompt(text) or "").strip()
        if normalized:
            return normalized
    except Exception:
        pass
    return text


async def _render_scheduled_message(
    llm_client,
    reminder_id: str,
    task_prompt: str,
    origin: Dict[str, Any],
    platform: str,
    targets: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]]]:
    task_prompt = _normalize_runtime_task_prompt(task_prompt)
    if not task_prompt:
        return "", []

    system_prompt = (
        "You are running a scheduled task.\n"
        "Keep replies concise and task-focused.\n"
        "Do not use repo_browser.* tool syntax.\n"
        "Execute the task now; do not create, modify, or cancel schedules.\n"
    )

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    user_prompt = (
        f"Scheduled task id: {reminder_id}\n"
        f"Current local time: {now_str}\n"
        f"Origin: {json.dumps(origin or {}, ensure_ascii=False)}\n\n"
        f"Task:\n{task_prompt}\n"
    )

    messages = [
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

    origin_payload = dict(origin or {})
    origin_payload = {k: v for k, v in origin_payload.items() if v not in (None, "")}
    context = _build_platform_context(
        platform,
        origin=origin_payload,
        targets=targets or {},
        task_prompt=task_prompt,
        reminder_id=reminder_id,
    )
    blocked_scheduler_tools = {str(name).strip().lower() for name in SCHEDULER_EXCLUDED_TOOLS}

    def _scheduler_admin_guard(tool_name: str) -> Optional[Dict[str, Any]]:
        func = str(tool_name or "").strip().lower()
        if func in blocked_scheduler_tools:
            return {
                "tool": func,
                "ok": False,
                "error": {
                    "code": "scheduler_tool_blocked",
                    "message": "That scheduling tool is unavailable while running a scheduled task.",
                },
                "summary_for_user": "Running the scheduled task now.",
            }
        return None

    agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
    result = await run_cerberus_turn(
        llm_client=llm_client,
        platform=platform,
        history_messages=messages,
        registry=merged_registry,
        enabled_predicate=_enabled,
        context=context,
        user_text=task_prompt,
        scope=f"ai_task:{reminder_id}",
        origin=origin_payload,
        redis_client=redis_client,
        max_rounds=agent_max_rounds,
        max_tool_calls=agent_max_tool_calls,
        platform_preamble=system_prompt,
        admin_guard=_scheduler_admin_guard,
    )
    text = str(result.get("text") or "").strip()
    attachments = result.get("artifacts") or []
    if not text and not attachments:
        text = "Scheduled task completed."
    return text, attachments


def run(stop_event: Optional[object] = None):
    logger.info("[AI Tasks] Scheduler service started.")
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
                    dispatch_notification(
                        platform=dest,
                        title=title,
                        content=outbound_message,
                        targets=targets,
                        origin=origin,
                        meta=meta,
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
                now_ts = time.time()
                old_next = _as_float(schedule.get("next_run_ts"), 0.0)
                old_anchor = _as_float(schedule.get("anchor_ts"), 0.0)
                next_run = _next_run_for_schedule(schedule, now_ts)
                if next_run <= now_ts:
                    next_run = now_ts + max(1.0, interval)

                schedule["next_run_ts"] = next_run
                if old_anchor > 0:
                    schedule["anchor_ts"] = old_anchor
                elif old_next > 0:
                    schedule["anchor_ts"] = old_next
                else:
                    schedule["anchor_ts"] = float(due_ts)
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
        logger.info("[AI Tasks] Scheduler service stopped.")
