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
from helpers import (
    get_llm_client_from_env,
    get_tater_name,
    get_tater_personality,
    parse_function_json,
)

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


def _supports_scheduled_tools(plugin_name: str, plugin: ToolPlugin) -> bool:
    if plugin_name in SCHEDULER_EXCLUDED_TOOLS:
        return False
    if getattr(plugin, "notifier", False):
        return False

    platforms = set(getattr(plugin, "platforms", []) or [])
    supports_webui = ("webui" in platforms) and _has_webui_handler(plugin)
    supports_automation = ("automation" in platforms) and _has_automation_handler(plugin)
    return supports_webui or supports_automation


def _tool_prompt_block() -> str:
    lines = []
    plugins = pr.get_registry_snapshot()
    for name, plugin in sorted(plugins.items(), key=lambda kv: kv[0]):
        if not get_plugin_enabled(name):
            continue
        if not _supports_scheduled_tools(name, plugin):
            continue

        usage = (getattr(plugin, "usage", "") or "").strip()
        desc = (
            getattr(plugin, "plugin_dec", None)
            or getattr(plugin, "description", "")
            or "No description."
        ).strip()
        lines.append(f"Tool: {name}\nDescription: {desc}\nUsage:\n{usage}\n")

    return "\n".join(lines)


async def _execute_tool_call(
    llm_client,
    func_name: str,
    args: Dict[str, Any],
    origin: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]]]:
    plugins = pr.get_registry_snapshot()
    plugin = plugins.get(func_name)
    if not plugin:
        return f"Tool `{func_name}` is not available.", []
    if not get_plugin_enabled(func_name):
        return f"Tool `{func_name}` is disabled.", []
    if not _supports_scheduled_tools(func_name, plugin):
        return f"Tool `{func_name}` is not available in scheduler tasks.", []

    args = dict(args or {})
    args["origin"] = dict(origin or {})

    try:
        platforms = set(getattr(plugin, "platforms", []) or [])
        if "webui" in platforms and _has_webui_handler(plugin):
            result = plugin.handle_webui(args, llm_client)
        elif "automation" in platforms and _has_automation_handler(plugin):
            result = plugin.handle_automation(args, llm_client)
        else:
            return f"Tool `{func_name}` cannot run from scheduler context.", []

        if asyncio.iscoroutine(result):
            result = await result
        text, attachments = _flatten_result_payload(result)
        if not text and not attachments:
            text = f"Completed `{func_name}`."
        return text, attachments
    except Exception as e:
        logger.warning(f"[AI Tasks] Tool `{func_name}` failed: {e}")
        return f"Tool `{func_name}` failed.", []


async def _render_scheduled_message(
    llm_client,
    reminder_id: str,
    task_prompt: str,
    origin: Dict[str, Any],
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

    tool_block = _tool_prompt_block()
    if not tool_block:
        return task_prompt, []

    system_prompt = (
        f"You are {first} {last}, running a scheduled task.\n"
        f"{persona}"
        "Goal:\n"
        "- Either answer directly, OR call exactly one tool if needed.\n"
        "- If you call a tool, output ONLY JSON: {\"function\": \"name\", \"arguments\": {...}}\n"
        "- If no tool is needed, output only the final user-facing message text.\n"
        "- Keep output concise and useful.\n"
    )

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    user_prompt = (
        f"Scheduled task id: {reminder_id}\n"
        f"Current local time: {now_str}\n"
        f"Origin: {json.dumps(origin or {}, ensure_ascii=False)}\n\n"
        f"Task:\n{task_prompt}\n\n"
        f"Available tools:\n{tool_block}"
    )

    try:
        resp = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            stream=False,
        )
    except Exception as e:
        logger.warning(f"[AI Tasks] Scheduler LLM failed: {e}")
        return task_prompt, []

    text = (
        ((resp or {}).get("message") or {}).get("content", "")
        if isinstance(resp, dict) else ""
    )
    text = (text or "").strip()
    if not text:
        return task_prompt, []

    call = parse_function_json(text)
    if not call:
        return text, []

    func = (call.get("function") or "").strip()
    args = call.get("arguments") or {}
    if not func:
        return task_prompt, []
    return await _execute_tool_call(llm_client, func, args, origin or {})


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
            plugins = pr.get_registry_snapshot()
            notifier = plugins.get(notifier_name)
            if not notifier or not getattr(notifier, "notifier", False):
                logger.warning(f"[AI Tasks] Missing notifier for {dest}; dropping reminder {reminder_id}.")
                _delete_reminder(reminder_id)
                continue

            if not get_plugin_enabled(notifier_name):
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
