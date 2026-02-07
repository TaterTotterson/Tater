import json
import logging
import time
import uuid
import re
from datetime import datetime, timedelta
from typing import Any, Dict

from plugin_base import ToolPlugin
from helpers import redis_client
from notify_queue import (
    ALLOWED_PLATFORMS,
    load_default_targets,
    normalize_origin,
    normalize_platform,
    resolve_targets,
)

logger = logging.getLogger("ai_tasks")
logger.setLevel(logging.INFO)

REMINDER_KEY_PREFIX = "reminders:"
REMINDER_DUE_ZSET = "reminders:due"


class AITasksPlugin(ToolPlugin):
    name = "ai_tasks"
    required_args = ["message"]
    optional_args = [
        "task_prompt",
        "title",
        "platform",
        "targets",
        "when_ts",
        "when",
        "in_seconds",
        "every_seconds",
        "priority",
        "tags",
        "ttl_sec",
        "origin",
    ]
    version = "1.0.1"
    usage = (
        "{\n"
        "  \"function\": \"ai_tasks\",\n"
        "  \"arguments\": {\n"
        "    \"message\": \"Required task prompt\",\n"
        "    \"task_prompt\": \"Optional explicit scheduled task prompt\",\n"
        "    \"title\": \"Optional short title\",\n"
        "    \"platform\": \"discord|irc|matrix|homeassistant|ntfy|telegram (optional; defaults to origin)\",\n"
        "    \"targets\": {\n"
        "      \"channel\": \"optional destination (discord/irc channel, matrix room/alias, or HA notify service)\",\n"
        "      \"chat_id\": \"optional telegram destination chat id\"\n"
        "    },\n"
        "    \"when_ts\": 1730000000.0,\n"
        "    \"when\": \"2026-02-03 15:04:05 or 10am\",\n"
        "    \"in_seconds\": 3600,\n"
        "    \"every_seconds\": 0,\n"
        "    \"priority\": \"normal|high\",\n"
        "    \"tags\": [\"optional\", \"strings\"],\n"
        "    \"ttl_sec\": 0\n"
        "  }\n"
        "}\n"
    )
    description = (
        "Schedule an AI task. Supports one-shot or recurring runs via every_seconds. "
        "At run time, AI can answer directly or call one tool before sending the final response."
    )
    pretty_name = "AI Tasks"

    platforms = ["discord", "irc", "matrix", "homeassistant", "telegram", "webui"]
    waiting_prompt_template = (
        "Write a short, friendly message telling {mention} you’re scheduling the task now. "
        "Only output that message."
    )

    @staticmethod
    def _normalize_channel_targets(dest: str, targets: Dict[str, Any]) -> Dict[str, Any]:
        t = dict(targets or {})
        channel_ref = t.get("channel")

        if not channel_ref:
            if dest == "discord":
                channel_ref = t.get("channel_id")
            elif dest == "matrix":
                channel_ref = t.get("room_id")
            elif dest == "homeassistant":
                channel_ref = t.get("device_service")
            elif dest == "telegram":
                channel_ref = t.get("chat_id")

        if not channel_ref:
            return t

        ref = str(channel_ref).strip()
        if not ref:
            return {}

        if dest == "discord":
            if ref.isdigit():
                return {"channel_id": ref}
            return {"channel": ref}

        if dest == "matrix":
            if not ref.startswith(("!", "#")) and ":" in ref:
                ref = f"#{ref}"
            return {"room_id": ref}

        if dest == "homeassistant":
            return {"device_service": ref}

        if dest == "telegram":
            return {"chat_id": ref}

        return {"channel": ref}

    def _extract_args(self, args: Dict[str, Any]):
        args = args or {}
        title = args.get("title")
        message = args.get("message") or args.get("content")
        task_prompt = args.get("task_prompt")
        if task_prompt is None:
            task_prompt = message

        platform = args.get("platform")

        targets = dict(args.get("targets") or {})
        for key in ("channel", "channel_id", "room_id", "device_service", "chat_id"):
            if args.get(key) and key not in targets:
                targets[key] = args.get(key)

        when_ts = args.get("when_ts")
        when_txt = args.get("when")
        in_seconds = args.get("in_seconds")
        every_seconds = args.get("every_seconds")

        # Optional convenience fields
        if in_seconds is None and args.get("in_minutes") is not None:
            in_seconds = float(args.get("in_minutes")) * 60
        if in_seconds is None and args.get("in_hours") is not None:
            in_seconds = float(args.get("in_hours")) * 3600

        if every_seconds is None and args.get("every_minutes") is not None:
            every_seconds = float(args.get("every_minutes")) * 60
        if every_seconds is None and args.get("every_hours") is not None:
            every_seconds = float(args.get("every_hours")) * 3600

        meta = {
            "priority": args.get("priority"),
            "tags": args.get("tags"),
            "ttl_sec": args.get("ttl_sec"),
        }
        origin = args.get("origin")
        return (
            title,
            message,
            task_prompt,
            platform,
            targets,
            when_ts,
            when_txt,
            in_seconds,
            every_seconds,
            origin,
            meta,
        )

    def _parse_when(self, when_ts, when_txt, in_seconds) -> float | None:
        now = time.time()

        if when_ts is not None:
            try:
                return float(when_ts)
            except Exception:
                pass

        if in_seconds is not None:
            try:
                return now + float(in_seconds)
            except Exception:
                pass

        if isinstance(when_txt, str) and when_txt.strip():
            text = when_txt.strip()
            text_lower = text.lower()
            # numeric string -> epoch
            if text.isdigit():
                try:
                    return float(text)
                except Exception:
                    return None

            # ISO-ish string
            try:
                if text.endswith("Z"):
                    text = text[:-1] + "+00:00"
                dt = datetime.fromisoformat(text)
            except Exception:
                dt = None

            if dt is None:
                # try a common fallback format
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                    try:
                        dt = datetime.strptime(text, fmt)
                        break
                    except Exception:
                        dt = None

            if dt is None:
                # time-only formats like "10am", "10:30 pm", "at 22:15"
                m = re.match(r"^(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*$", text_lower)
                if m:
                    hour = int(m.group(1))
                    minute = int(m.group(2) or 0)
                    mer = m.group(3)
                    if hour < 1 or hour > 12 or minute > 59:
                        return None
                    if mer == "am":
                        hour = 0 if hour == 12 else hour
                    else:
                        hour = 12 if hour == 12 else hour + 12
                    now_dt = datetime.now().astimezone()
                    dt = now_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    if dt.timestamp() <= now_dt.timestamp():
                        dt = dt + timedelta(days=1)
                else:
                    m24 = re.match(r"^(?:at\s+)?(\d{1,2})(?::(\d{2}))\s*$", text_lower)
                    if m24:
                        hour = int(m24.group(1))
                        minute = int(m24.group(2))
                        if hour < 0 or hour > 23 or minute > 59:
                            return None
                        now_dt = datetime.now().astimezone()
                        dt = now_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
                        if dt.timestamp() <= now_dt.timestamp():
                            dt = dt + timedelta(days=1)

            if dt is None:
                return None

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)

            return dt.timestamp()

        return None

    async def _schedule(self, args: Dict[str, Any]):
        (
            title,
            message,
            task_prompt,
            platform,
            targets,
            when_ts,
            when_txt,
            in_seconds,
            every_seconds,
            origin,
            meta,
        ) = self._extract_args(args)

        message = (message or "").strip()
        task_prompt = (task_prompt or "").strip()

        if not task_prompt:
            task_prompt = message
        if not task_prompt:
            return "Cannot queue: missing task prompt"
        if not message:
            message = task_prompt

        dest = normalize_platform(platform)
        if not dest:
            if isinstance(origin, dict):
                origin_platform = normalize_platform(origin.get("platform"))
                if origin_platform in ALLOWED_PLATFORMS:
                    dest = origin_platform

        if dest not in ALLOWED_PLATFORMS:
            return "Cannot queue: missing destination platform"

        now = time.time()

        try:
            interval = float(every_seconds) if every_seconds is not None else 0.0
        except Exception:
            interval = 0.0
        if interval < 0:
            interval = 0.0

        next_run = self._parse_when(when_ts, when_txt, in_seconds)
        if next_run is None and interval > 0:
            next_run = now + max(1.0, interval)
        if next_run is None:
            return "Cannot schedule: missing or invalid time"

        if next_run < now:
            next_run = now

        normalized_targets = self._normalize_channel_targets(dest, targets)
        defaults = load_default_targets(dest, redis_client)
        resolved_targets, err = resolve_targets(dest, normalized_targets, origin, defaults)
        if err:
            return err

        reminder_id = str(uuid.uuid4())
        reminder = {
            "id": reminder_id,
            "created_at": float(now),
            "platform": dest,
            "title": title,
            "message": message,
            "task_prompt": (task_prompt or "").strip(),
            "targets": resolved_targets or {},
            "origin": normalize_origin(origin),
            "meta": meta or {},
            "schedule": {
                "next_run_ts": float(next_run),
                "interval_sec": float(interval),
            },
        }

        redis_client.set(f"{REMINDER_KEY_PREFIX}{reminder_id}", json.dumps(reminder))
        redis_client.zadd(REMINDER_DUE_ZSET, {reminder_id: float(next_run)})

        human = datetime.fromtimestamp(next_run).strftime("%Y-%m-%d %H:%M:%S")
        if interval > 0:
            return f"Recurring AI task scheduled every {int(interval)}s (next at {human})."
        return f"AI task scheduled for {human}."

    async def handle_discord(self, message, args, llm_client):
        return await self._schedule(args)

    async def handle_webui(self, args, llm_client):
        return await self._schedule(args)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._schedule(args)

    async def handle_homeassistant(self, args, llm_client):
        return await self._schedule(args)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self._schedule(args)

    async def handle_telegram(self, update, args, llm_client):
        return await self._schedule(args)


plugin = AITasksPlugin()
