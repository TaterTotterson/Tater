import json
import re
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
