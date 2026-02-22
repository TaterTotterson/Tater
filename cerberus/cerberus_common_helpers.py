import json
import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Pattern, Sequence


def coerce_text(content: Any) -> str:
    if isinstance(content, (bytes, bytearray)):
        try:
            return content.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        for key in ("content", "text", "value", "message"):
            value = content.get(key)
            if isinstance(value, str):
                return value
        try:
            return json.dumps(content, ensure_ascii=False)
        except Exception:
            return str(content)
    if isinstance(content, list):
        parts = [coerce_text(item).strip() for item in content]
        return "\n".join([p for p in parts if p]).strip()
    if content is None:
        return ""
    return str(content)


def short_text(value: Any, *, limit: int = 280) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


def is_low_information_text(value: Any) -> bool:
    text = " ".join(str(value or "").strip().lower().split())
    if not text:
        return True
    if re.fullmatch(r"(ok|okay|done|complete|completed|success|successful|all set|finished)[.!]?", text):
        return True
    if len(text) <= 6 and text in {"yes", "no", "maybe"}:
        return True
    return False


def first_json_object(
    text: str,
    *,
    coerce_text_fn: Callable[[Any], str],
) -> Dict[str, Any] | None:
    raw = coerce_text_fn(text).strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(raw):
        if ch != "{":
            continue
        try:
            parsed, _ = decoder.raw_decode(raw[idx:])
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def sanitize_user_text(
    text: str,
    *,
    platform: str,
    tool_used: bool,
    default_clarification: str,
    looks_like_tool_markup_fn: Callable[[str], bool],
    parse_function_json_fn: Callable[[Any], Any],
    checker_decision_prefix_re: Pattern[str],
    ascii_only_platforms: Sequence[str],
) -> str:
    del tool_used
    out = str(text or "").strip()
    if not out:
        return default_clarification

    if out.startswith("{") and out.endswith("}"):
        lowered = out.lower()
        if '"goal"' in lowered and '"plan"' in lowered and '"facts"' in lowered:
            return default_clarification
        try:
            parsed = json.loads(out)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            keys = {str(k or "").strip().lower() for k in parsed.keys() if str(k or "").strip()}
            if len(keys & {"goal", "plan", "facts", "open_questions", "next_step", "tool_history"}) >= 3:
                return default_clarification
            content_val = parsed.get("content")
            if isinstance(content_val, str):
                content_text = content_val.strip()
                content_low = content_text.lower()
                if content_text.startswith("{") and '"goal"' in content_low and '"plan"' in content_low and '"facts"' in content_low:
                    return default_clarification

    if looks_like_tool_markup_fn(out):
        return default_clarification
    if parse_function_json_fn(out):
        return default_clarification
    if re.search(
        r"\{[^{}]*\"function\"\s*:\s*\"[^\"]+\"[^{}]*\"arguments\"\s*:\s*\{",
        out,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        return default_clarification
    match = checker_decision_prefix_re.match(out)
    if match:
        out = str(match.group(2) or "").strip()

    if re.search(
        r"\b(planner head|doer head|critic head|internal orchestration|tool runtime|repair prompt|orchestration roles?)\b",
        out,
        flags=re.IGNORECASE,
    ):
        return "I'm your assistant."

    if platform in ascii_only_platforms:
        out = out.encode("ascii", "ignore").decode().strip()

    return out or default_clarification


def compact_history(
    history_messages: List[Dict[str, Any]],
    *,
    coerce_text_fn: Callable[[Any], str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for msg in history_messages or []:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip()
        if role not in {"system", "user", "assistant", "tool"}:
            continue
        content = coerce_text_fn(msg.get("content")).strip()
        if not content:
            continue
        out.append({"role": role, "content": content})
    return out[-12:]


def platform_label(
    platform: str,
    *,
    platform_display_map: Dict[str, str],
) -> str:
    key = str(platform or "").strip().lower()
    if key in platform_display_map:
        return platform_display_map[key]
    return key or "this platform"


def strip_user_sender_prefix(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    first_line = raw.splitlines()[0]
    if ":" not in first_line:
        return raw
    left, right = first_line.split(":", 1)
    left = left.strip()
    right = right.strip()
    if not left or not right:
        return raw
    if len(left) <= 40 and " " not in left and "/" not in left and "@" not in left:
        rest = raw[len(first_line) :].strip()
        return (right + ("\n" + rest if rest else "")).strip()
    return raw


def latest_user_text(
    history_messages: List[Dict[str, Any]],
    *,
    strip_user_sender_prefix_fn: Callable[[str], str],
    coerce_text_fn: Callable[[Any], str],
) -> str:
    for msg in reversed(history_messages or []):
        if not isinstance(msg, dict):
            continue
        if str(msg.get("role") or "").strip() != "user":
            continue
        content = strip_user_sender_prefix_fn(coerce_text_fn(msg.get("content")).strip())
        if content:
            return content
    return ""


def latest_url_from_history(
    history_messages: List[Dict[str, Any]],
    *,
    coerce_text_fn: Callable[[Any], str],
    url_re: Pattern[str],
) -> str:
    for msg in reversed(history_messages or []):
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip().lower()
        if role not in {"assistant", "user"}:
            continue
        content = coerce_text_fn(msg.get("content")).strip()
        if not content:
            continue
        matches = url_re.findall(content)
        if matches:
            return str(matches[-1]).strip()
    return ""


def effective_user_text(
    user_text: str,
    history_messages: List[Dict[str, Any]],
    *,
    looks_like_short_followup_fn: Callable[[str], bool],
    latest_user_text_fn: Callable[[List[Dict[str, Any]]], str],
    looks_like_download_followup_fn: Callable[[str], bool],
    latest_url_from_history_fn: Callable[[List[Dict[str, Any]]], str],
) -> str:
    current = str(user_text or "").strip()
    if not current:
        return ""
    out = current
    if looks_like_short_followup_fn(current):
        previous_user = latest_user_text_fn(history_messages)
        if previous_user and previous_user.strip().lower() != current.lower():
            out = f"{previous_user}\nFollow-up: {current}"

    if looks_like_download_followup_fn(current):
        recent_url = latest_url_from_history_fn(history_messages)
        if recent_url and recent_url not in out:
            out = f"{out}\nRecent URL reference: {recent_url}"

    return out


def user_disallows_overwrite(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(don't overwrite|do not overwrite|without overwrite|keep existing|leave existing|new name|different name)\b",
            lowered,
        )
    )


def looks_like_schedule_request(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(remind|reminder|schedule|scheduled|task|tasks|timer|alarm|every day|everyday|daily|weekly|weekdays?|weekends?|every\s+\d+\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks)|at\s+(?:\d{3,4}|\d{1,2}(?::\d{2})?)\s*(am|pm)?)\b",
            lowered,
        )
    )


def looks_like_weather_request(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(weather|forecast|rain|precip|temperature|temp|humidity|wind|storm|snow)\b",
            lowered,
        )
    )


def mentions_explicit_weather_location(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if re.search(r"\b(my location|current location|here)\b", lowered):
        return True
    if re.search(r"\b-?\d{1,3}\.\d+\s*,\s*-?\d{1,3}\.\d+\b", lowered):
        return True
    if re.search(r"\b\d{5}(?:-\d{4})?\b", lowered):
        return True
    for match in re.finditer(
        r"\b(?:in|for|at|near|around)\s+([a-z0-9][a-z0-9'._-]{1,})(?:\s+[a-z0-9][a-z0-9'._-]{1,}){0,2}",
        lowered,
    ):
        token = str(match.group(1) or "").strip().lower()
        if token in {
            "the",
            "a",
            "an",
            "today",
            "tomorrow",
            "tonight",
            "day",
            "week",
            "weekend",
            "weekdays",
            "daily",
            "hourly",
            "forecast",
            "weather",
            "rain",
            "chance",
            "chances",
        }:
            continue
        return True
    return False


def mentions_explicit_timezone(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if re.search(r"\b(timezone|time zone|utc|gmt|z)\b", lowered):
        return True
    if re.search(r"\b(est|edt|cst|cdt|mst|mdt|pst|pdt)\b", lowered):
        return True
    if re.search(r"\b(america|europe|asia|africa|australia|pacific|etc)/[a-z0-9_+\-]+\b", lowered):
        return True
    if re.search(r"\b(?:utc|gmt)\s*[+-]\s*\d{1,2}\b", lowered):
        return True
    return False


def looks_like_explicit_ai_task_request(
    text: str,
    *,
    looks_like_schedule_request_fn: Callable[[str], bool],
) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    if not looks_like_schedule_request_fn(lowered):
        return False

    has_recurrence = bool(
        re.search(
            r"\b(every day|everyday|daily|weekly|weekdays?|weekends?|every\s+\d+\s*(seconds?|minutes?|hours?|days?|weeks?))\b",
            lowered,
        )
    )
    has_time = bool(
        re.search(
            r"\b(\d{1,2}(?::\d{2})?\s*(am|pm)|at\s+\d{1,2}(?::\d{2})?\s*(am|pm)?|(?:in|for)\s+\d+\s*(seconds?|minutes?|hours?|days?|weeks?))\b",
            lowered,
        )
    )
    has_action = bool(
        re.search(
            r"\b(send|post|remind|reminder|notify|check|run|task|tasks|timer|alarm|schedule|scheduled|tell|say|give|turn|set|open|close|start|stop|lock|unlock|arm|disarm|dim|brighten|play|pause)\b",
            lowered,
        )
    )
    has_schedule_intent = bool(
        re.search(
            r"\b(schedule|scheduled|set up|setup|create|add|remind me|set a reminder|task|tasks|timer|alarm|recurring)\b",
            lowered,
        )
    )
    starts_like_recurrence_command = bool(
        re.search(
            r"^(?:hey\s+\w+\s+|please\s+)?(?:every day|everyday|daily|weekly|weekdays?|weekends?|every\s+\d+\s*(seconds?|minutes?|hours?|days?|weeks?)|at\s+\d{1,2}(?::\d{2})?\s*(am|pm)?)\b",
            lowered,
        )
    )
    polite_recurrence_command = bool(
        re.search(
            r"\b(?:can|could|would|will)\s+you\b.*\b(?:every day|everyday|daily|weekly|weekdays?|weekends?|every\s+\d+\s*(seconds?|minutes?|hours?|days?|weeks?)|at\s+\d{1,2}(?::\d{2})?\s*(am|pm)?)\b.*\b(send|post|tell|say|give|run|notify|remind|turn|set|open|close|start|stop|lock|unlock|arm|disarm|dim|brighten|play|pause)\b",
            lowered,
        )
    )
    return bool(
        (has_recurrence or has_time)
        and has_action
        and (has_schedule_intent or starts_like_recurrence_command or polite_recurrence_command)
    )


def send_message_allowed(
    *,
    user_text: str,
    tool_args: Dict[str, Any] | None,
    origin: Dict[str, Any] | None,
    platform: str,
    history_messages: List[Dict[str, Any]] | None,
    context: Dict[str, Any] | None,
    looks_like_send_message_intent_fn: Callable[[str], bool],
) -> tuple[bool, str]:
    del platform, history_messages, context
    lowered = " ".join(str(user_text or "").strip().lower().split())
    if not looks_like_send_message_intent_fn(lowered):
        return False, "no_delivery_intent"

    args = tool_args if isinstance(tool_args, dict) else {}
    has_explicit_destination = any(
        str(args.get(key) or "").strip()
        for key in ("platform", "channel", "channel_id", "room", "room_id", "chat_id", "user_id", "target")
    )
    if has_explicit_destination:
        return True, "explicit_destination"

    here_requested = bool(re.search(r"\b(here|this chat|this channel|this room)\b", lowered))
    src = origin if isinstance(origin, dict) else {}
    has_origin_destination = any(
        str(src.get(key) or "").strip()
        for key in ("channel_id", "channel", "room_id", "room", "chat_id", "target", "user_id", "user")
    )
    if here_requested and has_origin_destination:
        return True, "implicit_here_destination"
    if has_origin_destination:
        return True, "origin_destination"
    return False, "missing_destination"


def ai_tasks_schedule_status(
    *,
    payload: Dict[str, Any] | None,
    checker_result: Dict[str, Any] | None,
    short_text_fn: Callable[..., str],
    is_low_information_text_fn: Callable[[str], bool],
) -> Dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    ok = bool(src.get("ok"))
    payload_data = src.get("data") if isinstance(src.get("data"), dict) else {}
    reminder_id = short_text_fn(
        src.get("reminder_id") or payload_data.get("reminder_id"),
        limit=96,
    )

    next_run_text = ""
    try:
        next_run_ts = float(src.get("next_run_ts") or payload_data.get("next_run_ts"))
        if next_run_ts > 0:
            next_run_text = datetime.fromtimestamp(next_run_ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        next_run_text = ""

    result_line = short_text_fn(
        src.get("result") or src.get("summary_for_user") or payload_data.get("result"),
        limit=260,
    )
    if not result_line or is_low_information_text_fn(result_line):
        result_line = "Scheduled task created."

    if ok:
        parts: List[str] = [result_line.rstrip(".") + "."]
        if next_run_text and "next" not in result_line.lower() and "scheduled for" not in result_line.lower():
            parts.append(f"Next run: {next_run_text}.")
        if reminder_id:
            parts.append(f"Task ID: {reminder_id}.")
        return {
            "created": True,
            "code": "",
            "success_text": " ".join(parts).strip(),
            "failure_text": "",
        }

    err_code = ""
    err_message = ""
    err = src.get("error")
    if isinstance(err, dict):
        err_code = short_text_fn(err.get("code"), limit=64)
        err_message = short_text_fn(err.get("message"), limit=220)
    elif isinstance(err, str):
        err_message = short_text_fn(err, limit=220)

    if not err_message and isinstance(checker_result, dict):
        errors = checker_result.get("errors")
        if isinstance(errors, list):
            for item in errors:
                text = short_text_fn(item, limit=220)
                if text:
                    err_message = text
                    break

    if not err_message:
        err_message = "I could not confirm task creation."

    return {
        "created": False,
        "code": err_code or "task_not_created",
        "success_text": "",
        "failure_text": f"I couldn't create that scheduled task: {err_message}",
    }
