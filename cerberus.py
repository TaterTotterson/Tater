import json
import hashlib
import re
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from helpers import (
    TOOL_MARKUP_REPAIR_PROMPT,
    get_tater_name,
    get_tater_personality,
    looks_like_tool_markup,
    parse_function_json,
    redis_client as default_redis,
)
from plugin_kernel import (
    expand_plugin_platforms,
    normalize_platform,
    plugin_display_name,
    plugin_supports_platform,
    plugin_when_to_use,
)
from plugin_result import action_failure, narrate_result, normalize_plugin_result, result_for_llm
from tool_runtime import META_TOOLS, execute_plugin_call, is_meta_tool, run_meta_tool

TOOL_NAME_ALIASES = {
    "web_search": "search_web",
    "google_search": "search_web",
    "google_cse_search": "search_web",
    "inspect_page": "inspect_webpage",
    "inspect_website": "inspect_webpage",
    "describe_image": "vision_describer",
    "describe_latest_image": "vision_describer",
    "vision_describe": "vision_describer",
    "vision_describe_image": "vision_describer",
}

_KERNEL_TOOL_PRIORITY = [
    "search_web",
    "inspect_webpage",
    "send_message",
    "ai_tasks",
    "read_url",
    "download_file",
    "read_file",
    "search_files",
    "list_directory",
    "list_archive",
    "extract_archive",
    "memory_get",
    "memory_set",
    "memory_search",
    "list_workspace",
    "vision_describer",
    "get_plugin_help",
]

_KERNEL_TOOL_PURPOSE_HINTS = {
    "get_plugin_help": "show plugin schema and required args",
    "list_platforms_for_plugin": "list platforms supported by a plugin",
    "read_file": "read local file contents",
    "search_web": "web search for current information",
    "search_files": "search text across local files",
    "write_file": "write content to a local file",
    "list_directory": "list files and folders",
    "delete_file": "delete a local file",
    "read_url": "fetch and read webpage text",
    "inspect_webpage": "inspect webpage structure, links, and image candidates",
    "send_message": "send cross-platform messages/media via notifier delivery",
    "ai_tasks": "schedule one-off or recurring AI tasks/reminders",
    "download_file": "download files from URLs",
    "list_archive": "inspect archive entries",
    "extract_archive": "extract archives to workspace",
    "list_stable_plugins": "list stable built-in plugins",
    "list_stable_platforms": "list stable built-in platforms",
    "inspect_plugin": "inspect plugin metadata and methods",
    "validate_plugin": "validate an Agent Lab plugin file",
    "test_plugin": "run plugin test harness",
    "validate_platform": "validate an Agent Lab platform file",
    "create_plugin": "create/update an Agent Lab plugin",
    "create_platform": "create/update an Agent Lab platform",
    "write_workspace_note": "append a workspace note",
    "list_workspace": "list workspace notes",
    "memory_get": "read saved memory",
    "memory_set": "save memory entries",
    "memory_list": "list saved memory keys",
    "memory_delete": "delete saved memory keys",
    "memory_explain": "explain memory value/source",
    "memory_search": "search saved memory",
    "truth_get_last": "get latest truth snapshot",
    "truth_list": "list truth snapshots",
    "vision_describer": "describe an image from explicit source",
}

ASCII_ONLY_PLATFORMS = {"irc", "homeassistant", "homekit", "xbmc"}
DEFAULT_CLARIFICATION = "Could you clarify exactly what you want me to do next?"
DEFAULT_MAX_ROUNDS = 1
DEFAULT_MAX_TOOL_CALLS = 1
DEFAULT_MAX_LEDGER_ITEMS = 500
DEFAULT_PLANNER_MAX_TOKENS = 1100
DEFAULT_CHECKER_MAX_TOKENS = 850
DEFAULT_DOER_MAX_TOKENS = 900
DEFAULT_TOOL_REPAIR_MAX_TOKENS = 750
DEFAULT_OVERCLAR_REPAIR_MAX_TOKENS = 900
DEFAULT_SEND_REPAIR_MAX_TOKENS = 600
DEFAULT_RECOVERY_MAX_TOKENS = 350
AGENT_MAX_ROUNDS_KEY = "tater:agent:max_rounds"
AGENT_MAX_TOOL_CALLS_KEY = "tater:agent:max_tool_calls"
CERBERUS_AGENT_STATE_TTL_SECONDS_KEY = "tater:cerberus:agent_state_ttl_seconds"
CERBERUS_PLANNER_MAX_TOKENS_KEY = "tater:cerberus:planner_max_tokens"
CERBERUS_CHECKER_MAX_TOKENS_KEY = "tater:cerberus:checker_max_tokens"
CERBERUS_DOER_MAX_TOKENS_KEY = "tater:cerberus:doer_max_tokens"
CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY = "tater:cerberus:tool_repair_max_tokens"
CERBERUS_OVERCLAR_REPAIR_MAX_TOKENS_KEY = "tater:cerberus:overclar_repair_max_tokens"
CERBERUS_SEND_REPAIR_MAX_TOKENS_KEY = "tater:cerberus:send_repair_max_tokens"
CERBERUS_RECOVERY_MAX_TOKENS_KEY = "tater:cerberus:recovery_max_tokens"
CERBERUS_MAX_LEDGER_ITEMS_KEY = "tater:cerberus:max_ledger_items"
AGENT_STATE_PROMPT_MAX_CHARS = 800
AGENT_STATE_LEDGER_MAX_CHARS = 900
AGENT_STATE_KEY_PREFIX = "tater:cerberus:state:"
DEFAULT_AGENT_STATE_TTL_SECONDS = 7 * 24 * 60 * 60
AGENT_STATE_TTL_SECONDS = DEFAULT_AGENT_STATE_TTL_SECONDS
CERBERUS_LEDGER_SCHEMA_VERSION = "2"

_PLATFORM_DISPLAY = {
    "webui": "WebUI",
    "discord": "Discord",
    "irc": "IRC",
    "telegram": "Telegram",
    "matrix": "Matrix",
    "homeassistant": "Home Assistant",
    "homekit": "HomeKit",
    "xbmc": "XBMC",
    "automation": "automation",
}

_OVER_CLARIFICATION_MARKERS = (
    "could you clarify",
    "what do you mean",
    "what specific issue",
    "what platform are you referring to",
    "what platform or environment",
    "which platform or environment",
    "what would you like to do",
    "what would you like me to do",
    "which platform should i send to",
    "what room/channel/chat should i send this to",
    "which channel should i send this to",
    "which room should i send this to",
    "what time format should i use",
    "should i use 12-hour or 24-hour",
    "do you want 12-hour or 24-hour format",
    "am or pm",
)

_URL_RE = re.compile(r"https?://[^\s<>)\]\"']+", flags=re.IGNORECASE)
_GENERIC_SCOPE_TOKENS = {"", "default", "chat", "unknown", "none", "null", "n/a"}


def _redis_config_non_negative_int(
    key: str,
    default: int,
    *,
    redis_client: Any = None,
) -> int:
    r = redis_client or default_redis
    try:
        raw = r.get(key)
    except Exception:
        return max(0, int(default))
    return _coerce_non_negative_int(raw, default)


def _redis_config_positive_int(
    key: str,
    default: int,
    *,
    redis_client: Any = None,
) -> int:
    value = _redis_config_non_negative_int(key, default, redis_client=redis_client)
    if value <= 0:
        return max(1, int(default))
    return value


def _configured_agent_state_ttl_seconds(redis_client: Any = None) -> int:
    global AGENT_STATE_TTL_SECONDS
    AGENT_STATE_TTL_SECONDS = _redis_config_non_negative_int(
        CERBERUS_AGENT_STATE_TTL_SECONDS_KEY,
        DEFAULT_AGENT_STATE_TTL_SECONDS,
        redis_client=redis_client,
    )
    return AGENT_STATE_TTL_SECONDS


def _configured_max_ledger_items(redis_client: Any = None) -> int:
    return _redis_config_positive_int(
        CERBERUS_MAX_LEDGER_ITEMS_KEY,
        DEFAULT_MAX_LEDGER_ITEMS,
        redis_client=redis_client,
    )


def _configured_planner_max_tokens(redis_client: Any = None) -> int:
    return _redis_config_positive_int(
        CERBERUS_PLANNER_MAX_TOKENS_KEY,
        DEFAULT_PLANNER_MAX_TOKENS,
        redis_client=redis_client,
    )


def _configured_checker_max_tokens(redis_client: Any = None) -> int:
    return _redis_config_positive_int(
        CERBERUS_CHECKER_MAX_TOKENS_KEY,
        DEFAULT_CHECKER_MAX_TOKENS,
        redis_client=redis_client,
    )


def _configured_doer_max_tokens(redis_client: Any = None) -> int:
    return _redis_config_positive_int(
        CERBERUS_DOER_MAX_TOKENS_KEY,
        DEFAULT_DOER_MAX_TOKENS,
        redis_client=redis_client,
    )


def _configured_tool_repair_max_tokens(redis_client: Any = None) -> int:
    return _redis_config_positive_int(
        CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY,
        DEFAULT_TOOL_REPAIR_MAX_TOKENS,
        redis_client=redis_client,
    )


def _configured_overclar_repair_max_tokens(redis_client: Any = None) -> int:
    return _redis_config_positive_int(
        CERBERUS_OVERCLAR_REPAIR_MAX_TOKENS_KEY,
        DEFAULT_OVERCLAR_REPAIR_MAX_TOKENS,
        redis_client=redis_client,
    )


def _configured_send_repair_max_tokens(redis_client: Any = None) -> int:
    return _redis_config_positive_int(
        CERBERUS_SEND_REPAIR_MAX_TOKENS_KEY,
        DEFAULT_SEND_REPAIR_MAX_TOKENS,
        redis_client=redis_client,
    )


def _configured_recovery_max_tokens(redis_client: Any = None) -> int:
    return _redis_config_positive_int(
        CERBERUS_RECOVERY_MAX_TOKENS_KEY,
        DEFAULT_RECOVERY_MAX_TOKENS,
        redis_client=redis_client,
    )


def _coerce_text(content: Any) -> str:
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
        parts = [_coerce_text(item).strip() for item in content]
        return "\n".join([p for p in parts if p]).strip()
    if content is None:
        return ""
    return str(content)


def _contains_tool_json_pattern(text: str) -> bool:
    raw = str(text or "")
    if not raw:
        return False
    if re.search(
        r"\{[^{}]*\"function\"\s*:\s*\"[^\"]+\"[^{}]*\"arguments\"\s*:\s*\{",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        return True
    return bool(
        re.search(
            r"\bfunction\b\s*[:=]\s*['\"][^'\"]+['\"].{0,120}\barguments\b\s*[:=]\s*\{",
            raw,
            flags=re.IGNORECASE | re.DOTALL,
        )
    )


def _sanitize_platform_preamble(platform: str, platform_preamble: Any) -> str:
    text = _coerce_text(platform_preamble).strip()
    if not text:
        return ""
    text = text[:900].strip()
    if platform in ASCII_ONLY_PLATFORMS:
        text = text.encode("ascii", "ignore").decode().strip()
    if not text:
        return ""
    if looks_like_tool_markup(text):
        return ""
    if _parse_strict_tool_json(text) is not None:
        return ""
    if parse_function_json(text):
        return ""
    if _contains_tool_json_pattern(text):
        return ""
    return text


def _with_platform_preamble(
    messages: List[Dict[str, Any]],
    *,
    platform_preamble: str,
) -> List[Dict[str, Any]]:
    if not platform_preamble:
        return list(messages)
    out = list(messages)
    insert_at = 1 if out and out[0].get("role") == "system" else 0
    out.insert(insert_at, {"role": "system", "content": platform_preamble})
    return out


def _clean_scope_text(value: Any) -> str:
    text = _coerce_text(value).strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return _short_text(text, limit=180)


def _scope_is_generic(scope: str) -> bool:
    return str(scope or "").strip().lower() in _GENERIC_SCOPE_TOKENS


def _unknown_scope(platform: str, origin: Optional[Dict[str, Any]]) -> str:
    source: Dict[str, Any] = {"platform": normalize_platform(platform)}
    if isinstance(origin, dict):
        for key, value in origin.items():
            if value in (None, ""):
                continue
            source[key] = value
    try:
        payload = json.dumps(source, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        payload = str(source)
    base = payload or normalize_platform(platform) or "unknown"
    digest = hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"unknown:{digest}"


def _derive_scope_from_origin(platform: str, origin: Optional[Dict[str, Any]]) -> str:
    src = origin if isinstance(origin, dict) else {}
    p = normalize_platform(platform)

    def _v(*keys: str) -> str:
        for key in keys:
            val = _clean_scope_text(src.get(key))
            if val and not _scope_is_generic(val):
                return val
        return ""

    if p == "discord":
        chat_type = _v("chat_type").lower()
        dm_user = _v("dm_user_id")
        if dm_user:
            return f"dm:{dm_user}"
        if chat_type in {"dm", "direct", "direct_message", "private", "private_chat"}:
            user_id = _v("user_id", "author_id", "sender_id")
            if user_id:
                return f"dm:{user_id}"
            dm_chat = _v("chat_id", "channel_id")
            if dm_chat:
                return f"dm:{dm_chat}"
        channel_id = _v("channel_id", "thread_id", "chat_id")
        if channel_id:
            return f"channel:{channel_id}"
    elif p == "irc":
        target = _v("target", "channel", "room", "channel_name", "nick", "user")
        if target:
            if target.startswith(("chan:", "pm:")):
                return target
            if target.startswith(("#", "&")):
                return f"chan:{target}"
            if _v("chat_type").lower() in {"pm", "dm", "direct"}:
                return f"pm:{target}"
            return f"pm:{target}"
    elif p == "homeassistant":
        device_id = _v("device_id")
        if device_id:
            return f"device:{device_id}"
        area_id = _v("area_id")
        if area_id:
            return f"area:{area_id}"
        session_id = _v("session_id", "conversation_id", "request_id", "source_id")
        if session_id:
            return f"session:{session_id}"
    elif p == "webui":
        session_id = _v("session_id", "conversation_id")
        if session_id:
            return f"session:{session_id}"
        user_id = _v("user_id", "user", "username")
        if user_id:
            return f"user:{user_id}"
    elif p == "telegram":
        chat_id = _v("chat_id", "room_id")
        if chat_id:
            return f"chat:{chat_id}"
    elif p == "matrix":
        room_id = _v("room_id", "chat_id")
        if room_id:
            return f"room:{room_id}"
    elif p in {"homekit", "xbmc"}:
        session_id = _v("session_id", "request_id", "device_id", "user_id", "user")
        if session_id:
            return f"session:{session_id}"

    fallback = _v(
        "scope",
        "channel_id",
        "room_id",
        "chat_id",
        "device_id",
        "area_id",
        "session_id",
        "conversation_id",
        "request_id",
        "user_id",
        "user",
    )
    if fallback:
        return fallback
    return _unknown_scope(p, src)


def _resolve_cerberus_scope(platform: str, scope: Any, origin: Optional[Dict[str, Any]]) -> str:
    p = normalize_platform(platform)
    raw = _clean_scope_text(scope)

    if not raw or _scope_is_generic(raw):
        return _derive_scope_from_origin(p, origin)

    if p == "discord":
        if raw.startswith(("channel:", "dm:")):
            return raw
        if _clean_scope_text((origin or {}).get("chat_type")).lower() == "dm":
            return f"dm:{raw}"
        return f"channel:{raw}"

    if p == "irc":
        if raw.startswith(("chan:", "pm:")):
            return raw
        if raw.startswith(("#", "&")):
            return f"chan:{raw}"
        return f"pm:{raw}"

    if p == "telegram":
        return raw if raw.startswith("chat:") else f"chat:{raw}"

    if p == "matrix":
        return raw if raw.startswith("room:") else f"room:{raw}"

    if p == "webui":
        if raw.startswith(("session:", "user:")):
            return raw
        return f"user:{raw}"

    if p == "homeassistant":
        if raw.startswith(("device:", "area:", "session:")):
            return raw
        if ":" in raw:
            return raw
        return f"session:{raw}"

    if p in {"homekit", "xbmc"}:
        if raw.startswith("session:"):
            return raw
        return f"session:{raw}"

    return raw


def _coerce_non_negative_int(value: Any, default: int) -> int:
    candidate: Any = value
    if isinstance(candidate, (bytes, bytearray)):
        try:
            candidate = candidate.decode("utf-8", errors="ignore")
        except Exception:
            candidate = ""
    try:
        out = int(str(candidate).strip())
    except Exception:
        out = int(default)
    if out < 0:
        return 0
    return out


def resolve_agent_limits(
    redis_client: Any = None,
    *,
    max_rounds: Optional[int] = None,
    max_tool_calls: Optional[int] = None,
) -> tuple[int, int]:
    r = redis_client or default_redis
    stored_rounds = DEFAULT_MAX_ROUNDS
    stored_tool_calls = DEFAULT_MAX_TOOL_CALLS
    try:
        stored_rounds = _coerce_non_negative_int(
            r.get(AGENT_MAX_ROUNDS_KEY),
            DEFAULT_MAX_ROUNDS,
        )
    except Exception:
        stored_rounds = DEFAULT_MAX_ROUNDS
    try:
        stored_tool_calls = _coerce_non_negative_int(
            r.get(AGENT_MAX_TOOL_CALLS_KEY),
            DEFAULT_MAX_TOOL_CALLS,
        )
    except Exception:
        stored_tool_calls = DEFAULT_MAX_TOOL_CALLS

    effective_rounds = (
        stored_rounds
        if max_rounds is None
        else _coerce_non_negative_int(max_rounds, stored_rounds)
    )
    effective_tool_calls = (
        stored_tool_calls
        if max_tool_calls is None
        else _coerce_non_negative_int(max_tool_calls, stored_tool_calls)
    )
    return effective_rounds, effective_tool_calls


def _canonical_tool_name(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    alias = TOOL_NAME_ALIASES.get(lowered)
    if alias:
        return alias
    if lowered in TOOL_NAME_ALIASES.values() or lowered in META_TOOLS or lowered in _KERNEL_TOOL_PRIORITY:
        return lowered
    return raw


def _looks_like_invalid_tool_call_text(text: str) -> bool:
    s = str(text or "").strip()
    if not s:
        return False
    lower = s.lower()
    if re.search(r"['\"]function['\"]\s*:", s, flags=re.IGNORECASE) and re.search(
        r"['\"]arguments['\"]\s*:", s, flags=re.IGNORECASE
    ):
        return True
    if s.startswith("{") and re.search(r"\bfunction\b\s*:", lower) and re.search(r"\barguments\b\s*:", lower):
        return True
    return False


def _tool_purpose(plugin: Any) -> str:
    text = str(plugin_when_to_use(plugin) or "").strip()
    if not text:
        text = str(getattr(plugin, "description", "") or "").strip()
    text = " ".join(text.split())
    if not text:
        return "no description"
    if len(text) > 80:
        text = text[:77].rstrip() + "..."
    return text


def _kernel_tool_purpose(tool_id: str) -> str:
    text = _KERNEL_TOOL_PURPOSE_HINTS.get(str(tool_id or "").strip(), "")
    if text:
        return text
    fallback = str(tool_id or "").strip().replace("_", " ")
    return fallback or "kernel tool"


def _ordered_kernel_tool_ids() -> List[str]:
    preferred = [tool_id for tool_id in _KERNEL_TOOL_PRIORITY if tool_id in META_TOOLS]
    preferred_set = set(preferred)
    remainder = sorted(tool_id for tool_id in META_TOOLS if tool_id not in preferred_set)
    return preferred + remainder


def _enabled_tool_mini_index(
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> str:
    kernel_rows: List[str] = []
    for tool_id in _ordered_kernel_tool_ids():
        kernel_rows.append(f"- {tool_id} - {_kernel_tool_purpose(tool_id)}")
    if not kernel_rows:
        kernel_rows.append("- (none)")

    enabled_check = enabled_predicate or (lambda _name: True)
    plugin_rows: List[str] = []
    for plugin_id, plugin in sorted(registry.items(), key=lambda kv: str(kv[0]).lower()):
        if not enabled_check(plugin_id):
            continue
        if not plugin_supports_platform(plugin, platform):
            continue
        plugin_rows.append(f"- {plugin_id} - {_tool_purpose(plugin)}")
    if not plugin_rows:
        plugin_rows.append("- (none)")

    return (
        "Kernel tools:\n"
        + "\n".join(kernel_rows)
        + "\nEnabled plugin tools on this platform:\n"
        + "\n".join(plugin_rows)
    )


def _compact_history(history_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for msg in history_messages or []:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip()
        if role not in {"system", "user", "assistant", "tool"}:
            continue
        content = _coerce_text(msg.get("content")).strip()
        if not content:
            continue
        out.append({"role": role, "content": content})
    return out[-12:]


def _platform_label(platform: str) -> str:
    key = str(platform or "").strip().lower()
    if key in _PLATFORM_DISPLAY:
        return _PLATFORM_DISPLAY[key]
    return key or "this platform"


def _contains_action_intent(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if _URL_RE.search(lowered):
        return True
    return bool(
        re.search(
            r"\b(can|could|would|please|make|build|create|do|turn|set|check|find|search|summarize|download|upload|send|post|add|share|inspect|read|run|open|explain|help)\b",
            lowered,
        )
    )


def _is_acknowledgement_only(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    if _contains_action_intent(lowered):
        return False
    if "?" in lowered:
        return False
    if re.search(r"\b(thanks|thank you|thx|ty|appreciate it|got it|sounds good|all good|perfect|awesome|great|cool|nice)\b", lowered):
        return True
    return False


def _is_stop_only(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    if re.search(r"\b(do not|don't)\s+(send|post|add|upload|share|continue|keep going|go on)\b", lowered):
        return True
    if re.search(r"\b(stop|cancel|nevermind|never mind|do not|don't)\b", lowered):
        if _contains_action_intent(lowered):
            # Allow explicit cancels even with a trailing polite phrase, but avoid
            # swallowing new actionable requests.
            if re.search(r"\b(stop|cancel|nevermind|never mind)\b", lowered):
                return True
            return False
        return True
    if lowered in {"no", "nope", "nah", "no thanks", "that's all", "thats all"}:
        return True
    return False


def _looks_like_send_message_intent(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    has_send_verb = bool(
        re.search(
            r"\b(send|post|share|upload|forward|deliver|broadcast|publish|attach|dm|message|tell)\b",
            lowered,
        )
    )
    if not has_send_verb:
        return False

    has_destination_hint = bool(
        re.search(
            r"\b("
            r"to discord|to irc|to matrix|to telegram|to homeassistant|to home assistant|to homekit|to xbmc|to ntfy|to wordpress|"
            r"here|this chat|this channel|this room|in this chat|in this channel|in this room|"
            r"to this chat|to this channel|to this room|in channel|in room|in chat|"
            r"to channel|to room|to chat"
            r")\b",
            lowered,
        )
    )

    explicit_target_ref = bool(
        re.search(
            r"(?:<#\d+>|#[a-z0-9_.-]+|\b(channel|room|chat)\s*[:#]?\s*[a-z0-9_.-]{2,}\b)",
            lowered,
        )
    )

    has_deliverable = bool(_URL_RE.search(lowered)) or bool(
        re.search(
            r"\b(logo|image|photo|picture|file|video|audio|document|pdf|zip|link|url|attachment|attached|screenshot|clip)\b",
            lowered,
        )
    )
    short_reference_send = bool(
        re.search(
            r"\b(send|post|share|upload|forward|attach|dm|message|tell)\s+(it|this|that|them|those|the (image|file|link|logo|attachment))\b",
            lowered,
        )
    )

    if has_destination_hint or explicit_target_ref:
        return True
    if has_deliverable:
        return True
    if short_reference_send:
        return True
    return False


def _send_message_has_concrete_destination(
    *,
    arguments: Any,
    origin: Optional[Dict[str, Any]],
    platform: str,
    scope: str,
) -> bool:
    _ = origin
    _ = platform
    _ = scope
    args = arguments if isinstance(arguments, dict) else {}
    return _find_concrete_destination(args)


_DESTINATION_FIELD_KEYS = {
    "channel_id",
    "thread_id",
    "room_id",
    "chat_id",
    "user_id",
    "dm_user_id",
    "target",
    "room",
    "channel",
    "chat",
    "scope",
    "destination",
    "to",
}
_DESTINATION_CONTAINER_KEYS = {
    "destination",
    "to",
    "targets",
    "payload",
    "message",
    "meta",
    "context",
    "origin",
}
_DESTINATION_ROUTE_KEYS = {
    "discord",
    "irc",
    "matrix",
    "telegram",
    "homeassistant",
    "homekit",
    "xbmc",
    "ntfy",
    "wordpress",
    "webui",
}
_DESTINATION_EXCLUSION_KEYS = {
    "message_id",
    "task_id",
    "request_id",
    "conversation_id",
    "session_id",
    "turn_id",
    "args_hash",
    "tool_args_hash",
    "sha256",
    "digest",
    "id",
    "url",
    "link",
    "filename",
}
_DESTINATION_MAX_DEPTH = 6
_DESTINATION_MAX_NODES = 80
_HEXLIKE_RE = re.compile(r"^[0-9a-f]{24,}$", re.IGNORECASE)
_HASH_PREFIX_RE = re.compile(r"^(?:sha1|sha224|sha256|sha384|sha512):", re.IGNORECASE)


def _has_concrete_destination_value(value: Any, *, key_hint: str = "") -> bool:
    key_norm = str(key_hint or "").strip().lower()
    if key_norm in _DESTINATION_EXCLUSION_KEYS:
        return False
    if isinstance(value, (dict, list, tuple, set)):
        return False
    cleaned = _clean_scope_text(value)
    if not cleaned:
        return False
    if len(cleaned) < 2:
        return False
    lowered = cleaned.lower()
    if _HASH_PREFIX_RE.match(lowered):
        return False
    if lowered.startswith(("http://", "https://")):
        return False
    if _HEXLIKE_RE.fullmatch(cleaned) and key_norm not in _DESTINATION_FIELD_KEYS:
        return False
    return not _scope_is_generic(cleaned)


def _find_concrete_destination(
    arguments_dict: Any,
    *,
    _depth: int = 0,
    _max_depth: int = _DESTINATION_MAX_DEPTH,
    _max_nodes: int = _DESTINATION_MAX_NODES,
) -> bool:
    state = {"nodes": 0}

    def _route_object_has_destination(node: Dict[str, Any]) -> bool:
        platform_val = str(node.get("platform") or "").strip().lower()
        type_val = str(node.get("type") or "").strip().lower()
        has_route_hint = bool(platform_val in _DESTINATION_ROUTE_KEYS or type_val in {"channel", "thread", "room", "chat", "dm", "user", "target"})

        for field_key in _DESTINATION_FIELD_KEYS:
            if field_key not in node:
                continue
            if _has_concrete_destination_value(node.get(field_key), key_hint=field_key):
                return True

        # Allow id only in explicit routing objects (platform+type), never globally.
        if has_route_hint and platform_val and type_val and _has_concrete_destination_value(node.get("id"), key_hint="channel_id"):
            return True
        return False

    def _walk_list(values: List[Any], *, depth: int, in_destination_context: bool, parent_key: str) -> bool:
        if depth > _max_depth:
            return False
        state["nodes"] += 1
        if state["nodes"] > _max_nodes:
            return False
        for item in values:
            if isinstance(item, dict):
                if _walk_dict(item, depth=depth, in_destination_context=in_destination_context, parent_key=parent_key):
                    return True
                continue
            if isinstance(item, list):
                if _walk_list(item, depth=depth + 1, in_destination_context=in_destination_context, parent_key=parent_key):
                    return True
                continue
            # Explicit route map support: {"targets":{"discord":["123","456"]}}
            if in_destination_context and parent_key in _DESTINATION_ROUTE_KEYS:
                if _has_concrete_destination_value(item, key_hint="channel_id"):
                    return True
        return False

    def _walk_dict(node: Dict[str, Any], *, depth: int, in_destination_context: bool, parent_key: str = "") -> bool:
        if depth > _max_depth:
            return False
        state["nodes"] += 1
        if state["nodes"] > _max_nodes:
            return False

        if _route_object_has_destination(node):
            return True

        for key, value in node.items():
            key_norm = str(key or "").strip().lower()
            if not key_norm:
                continue

            if key_norm in _DESTINATION_EXCLUSION_KEYS:
                continue

            is_dest_key = key_norm in _DESTINATION_FIELD_KEYS
            is_container_key = key_norm in _DESTINATION_CONTAINER_KEYS
            is_route_key = key_norm in _DESTINATION_ROUTE_KEYS
            child_context = bool(in_destination_context or is_container_key or is_route_key or is_dest_key)

            if is_dest_key and _has_concrete_destination_value(value, key_hint=key_norm):
                return True

            if isinstance(value, dict):
                if _walk_dict(value, depth=depth + 1, in_destination_context=child_context, parent_key=key_norm):
                    return True
                continue

            if isinstance(value, list):
                if _walk_list(value, depth=depth + 1, in_destination_context=child_context, parent_key=key_norm):
                    return True
                continue

            # Explicit route map support: {"targets":{"discord":"123"}}
            if child_context and key_norm in _DESTINATION_ROUTE_KEYS and parent_key in {"targets", "destination", "to"}:
                if _has_concrete_destination_value(value, key_hint="channel_id"):
                    return True

        return False

    if isinstance(arguments_dict, dict):
        return _walk_dict(arguments_dict, depth=max(0, int(_depth)), in_destination_context=False, parent_key="")
    if isinstance(arguments_dict, list):
        return _walk_list(arguments_dict, depth=max(0, int(_depth)), in_destination_context=False, parent_key="")
    return False


def _looks_like_over_clarification(text: str, *, user_text: str = "") -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if "?" not in lowered:
        return False
    if not any(marker in lowered for marker in _OVER_CLARIFICATION_MARKERS):
        return False

    user_lowered = " ".join(str(user_text or "").strip().lower().split())
    if not user_lowered:
        return False

    if re.search(r"\b(platform|environment)\b", lowered):
        return bool(
            re.search(
                r"\b(discord|irc|matrix|telegram|homeassistant|home assistant|homekit|xbmc|webui|here|this chat|this channel|this room)\b",
                user_lowered,
            )
        )

    if re.search(r"\b(room|channel|chat|where should i send|which .* send)\b", lowered):
        return bool(
            re.search(
                r"\b(here|this chat|this channel|this room|in this|to discord|to irc|to matrix|to telegram|to homeassistant|to home assistant|to homekit|to xbmc|channel|room|chat|dm)\b",
                user_lowered,
            )
        )

    if re.search(r"\b(time format|12-hour|24-hour|am or pm|a\.m\.|p\.m\.)\b", lowered):
        return bool(
            re.search(
                r"\b(\d{1,2}(?::\d{2})?\s*(am|pm)?|every day|everyday|daily|weekly|tomorrow|today|at\s+\d{1,2})\b",
                user_lowered,
            )
        )

    if re.search(r"\b(what would you like me to do|what would you like to do)\b", lowered):
        return _contains_action_intent(user_lowered)

    if re.search(r"\b(could you clarify|what do you mean|what specific issue)\b", lowered):
        tokens = [tok for tok in re.split(r"\s+", user_lowered) if tok]
        if len(tokens) <= 2 and not _contains_action_intent(user_lowered):
            return False
        return True

    return False


def _strip_user_sender_prefix(text: str) -> str:
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


def _latest_user_text(history_messages: List[Dict[str, Any]]) -> str:
    for msg in reversed(history_messages or []):
        if not isinstance(msg, dict):
            continue
        if str(msg.get("role") or "").strip() != "user":
            continue
        content = _strip_user_sender_prefix(_coerce_text(msg.get("content")).strip())
        if content:
            return content
    return ""


def _looks_like_standalone_request(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    lower = raw.lower()
    if re.match(r"^(?:what|how)\s+about\b", lower):
        return False
    if _is_acknowledgement_only(lower) or _is_stop_only(lower):
        return False
    if _URL_RE.search(raw):
        return True
    if "?" in raw:
        return True
    if re.match(
        r"^(?:hey|hi|hello|please|can|could|will|would|what|who|where|when|why|how|tell|show|describe|explain)\b",
        lower,
    ):
        return True
    if any(phrase in lower for phrase in ("can you", "could you", "will you", "would you", "help me", "i need ")):
        return True
    return False


def _looks_like_short_followup(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if re.match(r"^(?:what|how)\s+about\b", lowered):
        tokens = re.findall(r"[a-z0-9']+", lowered)
        return 2 <= len(tokens) <= 7
    if _is_acknowledgement_only(lowered) or _is_stop_only(lowered):
        return False
    if _looks_like_standalone_request(lowered):
        return False
    if lowered in {
        "yes",
        "no",
        "yep",
        "nope",
        "do it",
        "do that",
        "go ahead",
        "check",
        "check state",
        "state",
        "same",
        "again",
        "retry",
        "on",
        "off",
    }:
        return True
    tokens = re.findall(r"[a-z0-9']+", lowered)
    if not tokens or len(tokens) > 6:
        return False
    referential = {"it", "that", "this", "them", "those", "same"}
    return any(tok in referential for tok in tokens)


def _looks_like_download_followup(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if _URL_RE.search(lowered):
        return False
    has_action = bool(
        re.search(
            r"\b(download|save|get|grab|fetch|retrieve|pull|attach|post|send|share)\b",
            lowered,
        )
    )
    if not has_action:
        return False
    has_ref = bool(
        re.search(
            r"\b(link|url|file|image|photo|video|audio|zip|document|pdf|it|that|this|them|those)\b",
            lowered,
        )
    )
    return has_ref


def _latest_url_from_history(history_messages: List[Dict[str, Any]]) -> str:
    for msg in reversed(history_messages or []):
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip().lower()
        if role not in {"assistant", "user"}:
            continue
        content = _coerce_text(msg.get("content")).strip()
        if not content:
            continue
        matches = _URL_RE.findall(content)
        if matches:
            return str(matches[-1]).strip()
    return ""


def _effective_user_text(user_text: str, history_messages: List[Dict[str, Any]]) -> str:
    current = str(user_text or "").strip()
    if not current:
        return ""
    out = current
    if _looks_like_short_followup(current):
        previous_user = _latest_user_text(history_messages)
        if previous_user and previous_user.strip().lower() != current.lower():
            out = f"{previous_user}\nFollow-up: {current}"

    if _looks_like_download_followup(current):
        recent_url = _latest_url_from_history(history_messages)
        if recent_url and recent_url not in out:
            out = f"{out}\nRecent URL reference: {recent_url}"
    return out


def _planner_focus_prompt(*, current_user_text: str, resolved_user_text: str) -> str:
    current = str(current_user_text or "").strip()
    resolved = str(resolved_user_text or "").strip() or current
    if resolved and current and resolved != current:
        return (
            "Turn focus:\n"
            f"- Current user message (highest priority): {current}\n"
            f"- Resolved request for this turn: {resolved}\n"
            "- Use earlier history only for explicit references (it/that/this/here/again)."
        )
    return (
        "Turn focus:\n"
        f"- Current user message (highest priority): {resolved or current}\n"
        "- Do not continue prior topics unless the current message explicitly asks to continue."
    )


def _planner_system_prompt(platform: str) -> str:
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    first, last = get_tater_name()
    personality = (get_tater_personality() or "").strip()
    personality_block = ""
    if personality:
        personality_block = f"Voice style (tone only): {personality}\n"

    plain_text_rule = ""
    if platform in ASCII_ONLY_PLATFORMS:
        plain_text_rule = "When answering normally, use plain ASCII text only.\n"

    return (
        f"Current Date and Time: {now}\n"
        f"You are {first} {last}, a {_platform_label(platform)}-savvy AI assistant.\n"
        f"{personality_block}"
        f"Current platform: {platform}\n"
        "Choose exactly one next action for this turn.\n"
        "Output either:\n"
        "1) A normal assistant response (no tool call), OR\n"
        "2) Exactly ONE strict JSON object: {\"function\":\"tool_id\",\"arguments\":{...}}\n"
        "Rules:\n"
        "- The latest user message in this turn is the authoritative request.\n"
        "- Use earlier history only for explicit references; if topic changed, follow the latest message.\n"
        "- Never output multiple tool calls.\n"
        "- Use only tool ids from the enabled tool index.\n"
        "- Prefer action over clarification; answer directly whenever possible.\n"
        "- Ask a clarification only when one required value is truly missing and cannot be safely assumed.\n"
        "- Never ask what platform this chat is on; the current platform is already known.\n"
        "- Use send_message ONLY for explicit delivery/posting requests (send/post/share/upload/forward).\n"
        "- Never use send_message for normal conversational replies in the current chat.\n"
        "- For requests like 'send/post/add ... here' or 'in this chat/channel', use send_message and current context.\n"
        "- Do not ask for platform/room when user says 'here' unless they explicitly ask for another destination.\n"
        "- For scheduling/time requests, assume local time and parse common formats (6am, 18:00, at 6) without asking 12h vs 24h.\n"
        "- For memory_set, default personal facts/preferences to scope='user' unless user explicitly asks room/global.\n"
        "- For memory_get/memory_list/memory_delete about 'me/my', default to scope='user' unless user explicitly asks room/global.\n"
        "- Use scope='global' only when user clearly asks to store something for everyone/all chats.\n"
        "- For requests asking what a website/page is about, prefer inspect_webpage over read_url.\n"
        "- For plugin/platform creation requests, default to the current platform unless the user explicitly asks for another.\n"
        "- Never mention internal orchestration roles/codenames in user-facing replies.\n"
        "- No markdown fences around tool JSON.\n"
        f"{plain_text_rule}"
    ).strip()


def _checker_system_prompt(platform: str, retry_allowed: bool) -> str:
    retry_rule = (
        "RETRY_TOOL is allowed if one additional tool call should continue progress toward the goal.\n"
        if retry_allowed
        else "RETRY_TOOL is not allowed in this step.\n"
    )
    plain_text_rule = ""
    if platform in ASCII_ONLY_PLATFORMS:
        plain_text_rule = "Use plain ASCII text in FINAL_ANSWER/NEED_USER_INFO.\n"
    return (
        "You are the Critic head.\n"
        "Judge whether the user goal is satisfied right now.\n"
        "Output exactly ONE of these formats:\n"
        "FINAL_ANSWER: <text>\n"
        "RETRY_TOOL: {\"function\":\"tool_id\",\"arguments\":{...}}\n"
        "NEED_USER_INFO: <one short question>\n"
        "Rules:\n"
        "- Use payload.agent_state as primary context.\n"
        "- If goal is complete, return FINAL_ANSWER.\n"
        "- If more tool work is needed, return RETRY_TOOL with one next tool call.\n"
        "- If blocked by missing required user data, return NEED_USER_INFO.\n"
        "- Never output more than one tool call.\n"
        "- Never include markdown fences.\n"
        "- Never include raw tool JSON in FINAL_ANSWER.\n"
        "- Treat payload.current_user_message as highest priority.\n"
        "- Use payload.resolved_request_for_this_turn only to expand explicit follow-ups.\n"
        "- Prefer FINAL_ANSWER when sufficient facts already exist.\n"
        "- Do not ask which platform this chat is on; current platform is already known.\n"
        "- If original request says 'here'/'this chat'/'this channel', do not ask destination platform/room.\n"
        "- Never mention internal orchestration roles/codenames in FINAL_ANSWER/NEED_USER_INFO.\n"
        f"{retry_rule}"
        f"{plain_text_rule}"
    ).strip()


def _attach_origin(
    args: Dict[str, Any],
    *,
    origin: Optional[Dict[str, Any]],
    platform: str,
    scope: str,
    request_text: str = "",
) -> Dict[str, Any]:
    out = dict(args or {})
    base_origin = dict(origin) if isinstance(origin, dict) else {}
    trusted_origin: Dict[str, str] = {}
    if platform:
        trusted_origin["platform"] = str(platform)
    if scope:
        trusted_origin["scope"] = str(scope)
    if request_text:
        trusted_origin["request_text"] = str(request_text)
    for key, value in trusted_origin.items():
        base_origin[key] = value

    if not base_origin:
        return out

    existing = out.get("origin")
    if not isinstance(existing, dict):
        out["origin"] = base_origin
        return out

    merged: Dict[str, Any] = {}
    for key, value in existing.items():
        if value not in (None, ""):
            merged[key] = value
    for key, value in base_origin.items():
        if value not in (None, ""):
            if key in trusted_origin:
                continue
            if key not in merged or merged.get(key) in (None, ""):
                merged[key] = value
    for key, value in trusted_origin.items():
        if value not in (None, ""):
            merged[key] = value
    out["origin"] = merged
    return out


def _parse_strict_tool_json(response_text: str) -> Optional[Dict[str, Any]]:
    raw = _coerce_text(response_text).strip()
    if not raw:
        return None
    if not raw.startswith("{") or not raw.endswith("}"):
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    if set(parsed.keys()) != {"function", "arguments"}:
        return None
    return parsed


def _validate_tool_call_dict(
    *,
    parsed: Any,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> Dict[str, Any]:
    if not isinstance(parsed, dict):
        return {"ok": False, "reason": "not_object"}

    raw_func = parsed.get("function")
    if not isinstance(raw_func, str) or not raw_func.strip():
        return {"ok": False, "reason": "missing_function"}
    func = _canonical_tool_name(raw_func)

    raw_args = parsed.get("arguments", {})
    if raw_args is None:
        raw_args = {}
    if not isinstance(raw_args, dict):
        return {"ok": False, "reason": "arguments_not_object"}
    args = dict(raw_args)

    if is_meta_tool(func):
        return {
            "ok": True,
            "reason": "ok",
            "tool_call": {"function": func, "arguments": args},
            "platform_supported": True,
        }

    plugin = registry.get(func)
    if plugin is None:
        return {"ok": False, "reason": "unknown_tool", "tool_call": {"function": func, "arguments": args}}

    if enabled_predicate and not enabled_predicate(func):
        return {"ok": False, "reason": "tool_disabled", "tool_call": {"function": func, "arguments": args}}

    return {
        "ok": True,
        "reason": "ok",
        "tool_call": {"function": func, "arguments": args},
        "platform_supported": bool(plugin_supports_platform(plugin, platform)),
    }


async def _repair_tool_call_text(
    *,
    llm_client: Any,
    platform: str,
    original_text: str,
    reason: str,
    tool_index: str,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> str:
    prompt = (
        f"{TOOL_MARKUP_REPAIR_PROMPT}\n"
        "Repair the invalid planner output.\n"
        f"Current platform: {platform}\n"
        "Return only one of:\n"
        "- strict JSON tool call: {\"function\":\"tool_id\",\"arguments\":{...}}\n"
        "- NO_TOOL\n"
        "Do not include markdown."
    )
    user_payload = (
        f"Reason: {reason}\n"
        f"Enabled tool index:\n{tool_index}\n\n"
        f"Original planner output:\n{original_text}"
    )
    try:
        token_limit = int(max_tokens) if max_tokens is not None else _configured_tool_repair_max_tokens()
        response = await llm_client.chat(
            messages=_with_platform_preamble([
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_payload},
            ], platform_preamble=platform_preamble),
            max_tokens=max(1, token_limit),
            temperature=0.1,
        )
        return _coerce_text((response.get("message", {}) or {}).get("content", "")).strip()
    except Exception:
        return ""


async def _repair_over_clarification_text(
    *,
    llm_client: Any,
    platform: str,
    user_text: str,
    planner_text: str,
    tool_index: str,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> str:
    prompt = (
        "You are repairing an over-clarifying planner response.\n"
        f"Current platform: {platform}\n"
        "Do not ask what platform this chat is on.\n"
        "If the user says 'here'/'this chat'/'this channel', do not ask destination platform/room.\n"
        "Do not mention internal orchestration roles/codenames.\n"
        "Return only one of:\n"
        "- a direct assistant response (no prefix), OR\n"
        "- exactly one strict JSON tool call: {\"function\":\"tool_id\",\"arguments\":{...}}\n"
        "Prefer action over clarification.\n"
        "Ask a clarifying question only if one required value is truly missing and no safe assumption exists.\n"
        "No markdown."
    )
    user_payload = (
        f"Original user request:\n{user_text}\n\n"
        f"Over-clarifying planner output:\n{planner_text}\n\n"
        f"Enabled tool index:\n{tool_index}"
    )
    try:
        token_limit = int(max_tokens) if max_tokens is not None else _configured_overclar_repair_max_tokens()
        response = await llm_client.chat(
            messages=_with_platform_preamble([
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_payload},
            ], platform_preamble=platform_preamble),
            max_tokens=max(1, token_limit),
            temperature=0.1,
        )
        return _coerce_text((response.get("message", {}) or {}).get("content", "")).strip()
    except Exception:
        return ""


async def _repair_send_message_misfire_text(
    *,
    llm_client: Any,
    platform: str,
    user_text: str,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> str:
    prompt = (
        "You are correcting a planner mistake.\n"
        "The planner selected send_message, but the user did not clearly ask to deliver/post content.\n"
        f"Current platform: {platform}\n"
        "Reply directly to the user in this chat.\n"
        "Do not call tools.\n"
        "Do not ask destination platform/room unless the user explicitly asks to send/post elsewhere.\n"
        "Never mention internal orchestration roles/codenames.\n"
        "Return only the final assistant response text."
    )
    try:
        token_limit = int(max_tokens) if max_tokens is not None else _configured_send_repair_max_tokens()
        response = await llm_client.chat(
            messages=_with_platform_preamble([
                {"role": "system", "content": prompt},
                {"role": "user", "content": str(user_text or "").strip()},
            ], platform_preamble=platform_preamble),
            max_tokens=max(1, token_limit),
            temperature=0.2,
        )
        return _coerce_text((response.get("message", {}) or {}).get("content", "")).strip()
    except Exception:
        return ""


async def _validate_tool_contract(
    *,
    llm_client: Any,
    response_text: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    tool_index: str,
    platform_preamble: str = "",
    repair_max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    strict_parsed = _parse_strict_tool_json(response_text)
    if strict_parsed is not None:
        base = _validate_tool_call_dict(
            parsed=strict_parsed,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
        )
    else:
        loose_parsed = parse_function_json(response_text)
        if isinstance(loose_parsed, dict):
            loose_valid = _validate_tool_call_dict(
                parsed=loose_parsed,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
            )
            base = {
                "ok": False,
                "reason": "non_strict_json",
                "tool_call": loose_valid.get("tool_call"),
            }
        else:
            base = {"ok": False, "reason": "invalid_json"}

    if base.get("ok") and base.get("platform_supported", True):
        return {**base, "repair_used": False}

    reason = base.get("reason") or "invalid_tool_call"
    unsupported_only = bool(base.get("ok")) and not bool(base.get("platform_supported", True))
    if unsupported_only:
        reason = "unsupported_platform"

    repaired_text = await _repair_tool_call_text(
        llm_client=llm_client,
        platform=platform,
        original_text=response_text,
        reason=str(reason),
        tool_index=tool_index,
        platform_preamble=platform_preamble,
        max_tokens=repair_max_tokens,
    )
    if str(repaired_text).strip().upper() == "NO_TOOL":
        if unsupported_only:
            return {
                **base,
                "reason": "unsupported_platform",
                "repair_used": True,
            }
        return {"ok": False, "reason": "repair_returned_no_tool", "repair_used": True}

    repaired_strict = _parse_strict_tool_json(repaired_text)
    if repaired_strict is not None:
        repaired = _validate_tool_call_dict(
            parsed=repaired_strict,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
        )
    else:
        repaired_loose = parse_function_json(repaired_text)
        repaired = {
            "ok": False,
            "reason": "non_strict_json" if isinstance(repaired_loose, dict) else "invalid_json",
            "tool_call": (
                _validate_tool_call_dict(
                    parsed=repaired_loose,
                    platform=platform,
                    registry=registry,
                    enabled_predicate=enabled_predicate,
                ).get("tool_call")
                if isinstance(repaired_loose, dict)
                else None
            ),
        }
    if repaired.get("ok") and repaired.get("platform_supported", True):
        return {**repaired, "repair_used": True}
    if repaired.get("ok") and not repaired.get("platform_supported", True):
        return {
            **repaired,
            "reason": "unsupported_platform",
            "repair_used": True,
        }

    repaired_reason = repaired.get("reason") or "invalid_after_repair"
    if unsupported_only:
        return {
            **base,
            "reason": "unsupported_platform",
            "repair_used": True,
        }
    return {
        "ok": False,
        "reason": repaired_reason,
        "repair_used": True,
        "tool_call": repaired.get("tool_call"),
    }


def _has_implicit_here_destination_request(user_text: str) -> bool:
    lowered = " ".join(str(user_text or "").strip().lower().split())
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(here|this chat|this channel|this room|in this chat|in this channel|in this room|to this chat|to this channel|to this room)\b",
            lowered,
        )
    )


def _origin_has_current_destination(origin: Optional[Dict[str, Any]], platform: str) -> bool:
    src = origin if isinstance(origin, dict) else {}
    p = normalize_platform(platform)
    if p == "discord":
        return bool(_clean_scope_text(src.get("channel_id")) or _clean_scope_text(src.get("dm_user_id")) or _clean_scope_text(src.get("chat_id")))
    if p == "irc":
        target = _clean_scope_text(src.get("target") or src.get("channel") or src.get("room"))
        return bool(target)
    if p == "matrix":
        return bool(_clean_scope_text(src.get("room_id")))
    if p == "telegram":
        return bool(_clean_scope_text(src.get("chat_id")))
    if p == "webui":
        return bool(_clean_scope_text(src.get("session_id")) or _clean_scope_text(src.get("user_id")) or _clean_scope_text(src.get("user")))
    if p == "homeassistant":
        return bool(
            _clean_scope_text(src.get("device_id"))
            or _clean_scope_text(src.get("area_id"))
            or _clean_scope_text(src.get("session_id"))
            or _clean_scope_text(src.get("request_id"))
        )
    if p in {"homekit", "xbmc"}:
        return bool(_clean_scope_text(src.get("session_id")) or _clean_scope_text(src.get("device_id")) or _clean_scope_text(src.get("user_id")))
    return False


def _tool_args_have_deliverable_payload(tool_call: Optional[Dict[str, Any]]) -> bool:
    call = tool_call if isinstance(tool_call, dict) else {}
    args = call.get("arguments") if isinstance(call.get("arguments"), dict) else {}
    if not isinstance(args, dict):
        return False
    text_keys = ("message", "text", "body", "content", "caption", "title", "url", "link", "file", "file_path", "blob_key")
    for key in text_keys:
        value = args.get(key)
        if isinstance(value, str) and _clean_scope_text(value):
            if _scope_is_generic(_clean_scope_text(value)):
                continue
            return True
    nested = args.get("payload")
    if isinstance(nested, dict):
        for key in text_keys:
            value = nested.get(key)
            if isinstance(value, str) and _clean_scope_text(value):
                if _scope_is_generic(_clean_scope_text(value)):
                    continue
                return True
    attachments = args.get("attachments")
    if isinstance(attachments, list) and any(isinstance(item, dict) for item in attachments):
        return True
    return False


def send_message_allowed(
    *,
    user_text: str,
    tool_args: Any,
    origin: Optional[Dict[str, Any]],
    platform: str,
    history_messages: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
) -> tuple[bool, str]:
    _ = history_messages
    _ = context
    tool_call = {"function": "send_message", "arguments": tool_args if isinstance(tool_args, dict) else {}}
    has_destination = _send_message_has_concrete_destination(
        arguments=tool_call.get("arguments"),
        origin=origin,
        platform=platform,
        scope=_clean_scope_text((tool_call.get("arguments") or {}).get("scope")),
    )
    has_deliverable = _tool_args_have_deliverable_payload(tool_call)
    if has_destination:
        if not has_deliverable:
            return False, "no_deliverable_payload"
        return True, "explicit_destination"

    send_intent = _looks_like_send_message_intent(user_text)
    if not send_intent:
        return False, "no_delivery_intent"

    if _has_implicit_here_destination_request(user_text):
        if not _origin_has_current_destination(origin, platform):
            return False, "missing_destination"
        if not has_deliverable:
            return False, "no_deliverable_payload"
        return True, "implicit_here_destination"

    if not has_deliverable:
        return False, "no_deliverable_payload"
    return False, "missing_destination"


def _send_message_allowed_for_turn(
    *,
    tool_call: Optional[Dict[str, Any]],
    user_text: str,
    origin: Optional[Dict[str, Any]],
    platform: str,
    scope: str,
    history_messages: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
) -> tuple[bool, str]:
    call = tool_call if isinstance(tool_call, dict) else {}
    func = _canonical_tool_name(call.get("function"))
    if func != "send_message":
        return True, "not_send_message"
    allowed, reason = send_message_allowed(
        user_text=user_text,
        tool_args=call.get("arguments"),
        origin=origin,
        platform=platform,
        history_messages=history_messages,
        context=context,
    )
    _ = scope
    return allowed, reason


async def _validate_or_recover_tool_call(
    *,
    llm_client: Any,
    text: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    tool_index: str,
    user_text: str,
    origin: Optional[Dict[str, Any]],
    scope: str,
    history_messages: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
    platform_preamble: str = "",
    repair_max_tokens: Optional[int] = None,
    recovery_max_tokens: Optional[int] = None,
    send_repair_max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not _is_tool_candidate(raw):
        return {
            "ok": False,
            "tool_call": None,
            "repair_used": False,
            "reason": "not_tool_candidate",
            "recovery_text_if_blocked": None,
            "attempted_tool": None,
            "validation_status": None,
            "send_reason": "",
        }

    validation_status = await _validate_tool_contract(
        llm_client=llm_client,
        response_text=raw,
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        tool_index=tool_index,
        platform_preamble=platform_preamble,
        repair_max_tokens=repair_max_tokens,
    )
    tool_call = validation_status.get("tool_call") if isinstance(validation_status.get("tool_call"), dict) else None
    attempted_tool = str((tool_call or {}).get("function") or "").strip() or None
    if not validation_status.get("ok"):
        reason = str(validation_status.get("reason") or "invalid_tool_call")
        recovery_text = await _generate_recovery_text(
            llm_client=llm_client,
            platform=platform,
            user_text=user_text,
            error_kind="validation",
            reason=reason,
            fallback=_validation_failure_text(reason=reason, platform=platform),
            platform_preamble=platform_preamble,
            max_tokens=recovery_max_tokens,
        )
        return {
            "ok": False,
            "tool_call": tool_call,
            "repair_used": bool(validation_status.get("repair_used")),
            "reason": reason,
            "recovery_text_if_blocked": recovery_text,
            "attempted_tool": attempted_tool,
            "validation_status": validation_status,
            "send_reason": "",
        }

    if not isinstance(tool_call, dict):
        reason = "invalid_tool_call"
        recovery_text = await _generate_recovery_text(
            llm_client=llm_client,
            platform=platform,
            user_text=user_text,
            error_kind="validation",
            reason=reason,
            fallback=_validation_failure_text(reason=reason, platform=platform),
            platform_preamble=platform_preamble,
            max_tokens=recovery_max_tokens,
        )
        return {
            "ok": False,
            "tool_call": None,
            "repair_used": bool(validation_status.get("repair_used")),
            "reason": reason,
            "recovery_text_if_blocked": recovery_text,
            "attempted_tool": attempted_tool,
            "validation_status": validation_status,
            "send_reason": "",
        }

    send_allowed, send_reason = _send_message_allowed_for_turn(
        tool_call=tool_call,
        user_text=user_text,
        origin=origin,
        platform=platform,
        scope=scope,
        history_messages=history_messages,
        context=context,
    )
    if send_allowed:
        return {
            "ok": True,
            "tool_call": tool_call,
            "repair_used": bool(validation_status.get("repair_used")),
            "reason": str(validation_status.get("reason") or "ok"),
            "recovery_text_if_blocked": None,
            "attempted_tool": attempted_tool,
            "validation_status": validation_status,
            "send_reason": send_reason,
        }

    misfire_reply = await _repair_send_message_misfire_text(
        llm_client=llm_client,
        platform=platform,
        user_text=user_text,
        platform_preamble=platform_preamble,
        max_tokens=send_repair_max_tokens,
    )
    if misfire_reply and not _is_tool_candidate(misfire_reply):
        recovery_text = misfire_reply
    else:
        recovery_text = await _generate_recovery_text(
            llm_client=llm_client,
            platform=platform,
            user_text=user_text,
            error_kind="send_message_misfire",
            reason=send_reason,
            fallback="How can I help?",
            platform_preamble=platform_preamble,
            max_tokens=recovery_max_tokens,
        )
    return {
        "ok": False,
        "tool_call": tool_call,
        "repair_used": bool(validation_status.get("repair_used")),
        "reason": "send_message_misfire",
        "recovery_text_if_blocked": recovery_text,
        "attempted_tool": attempted_tool,
        "validation_status": validation_status,
        "send_reason": send_reason,
    }


def _validation_failure_text(reason: str, platform: str) -> str:
    reason = str(reason or "").strip().lower()
    if reason == "unsupported_platform":
        return f"That action is not supported on {platform}. What should I do instead?"
    if reason == "tool_disabled":
        return "That tool is currently disabled. What would you like me to do instead?"
    if reason == "unknown_tool":
        return "I couldn't find a matching tool for that request. Could you rephrase what action you want?"
    return "I couldn't safely execute a tool for that request. Could you clarify the exact action?"


async def _generate_recovery_text(
    *,
    llm_client: Any,
    platform: str,
    user_text: str,
    error_kind: str,
    reason: str = "",
    fallback: str,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> str:
    fallback_text = str(fallback or DEFAULT_CLARIFICATION).strip() or DEFAULT_CLARIFICATION
    payload = {
        "platform": platform,
        "user_message": str(user_text or ""),
        "error_kind": str(error_kind or "").strip().lower(),
        "reason": str(reason or "").strip().lower(),
    }
    prompt = (
        "Write one short user-facing recovery message.\n"
        "Rules:\n"
        "- One concise sentence.\n"
        "- Plain text only.\n"
        "- Do not mention internal systems, tools, JSON, or orchestration roles.\n"
        "- If error_kind is 'validation', ask for exactly the missing clarification needed to proceed.\n"
        "- If error_kind is 'planner_empty', ask the user to restate briefly.\n"
        "- Do not include markdown."
    )
    try:
        token_limit = int(max_tokens) if max_tokens is not None else _configured_recovery_max_tokens()
        response = await llm_client.chat(
            messages=_with_platform_preamble([
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ], platform_preamble=platform_preamble),
            max_tokens=max(1, token_limit),
            temperature=0.2,
        )
        out = _coerce_text((response.get("message", {}) or {}).get("content", "")).strip()
    except Exception:
        return fallback_text

    if not out:
        return fallback_text
    if looks_like_tool_markup(out) or parse_function_json(out):
        return fallback_text
    if re.match(r"^\s*(FINAL_ANSWER|RETRY_TOOL|NEED_USER_INFO)\s*:", out, flags=re.IGNORECASE):
        out = re.sub(
            r"^\s*(FINAL_ANSWER|RETRY_TOOL|NEED_USER_INFO)\s*:\s*",
            "",
            out,
            count=1,
            flags=re.IGNORECASE,
        ).strip()
    return out or fallback_text


async def _normalize_tool_result_for_checker(
    *,
    result_payload: Any,
    llm_client: Any,
    platform: str,
) -> Dict[str, Any]:
    normalized = normalize_plugin_result(result_payload)
    summary = await narrate_result(normalized, llm_client=llm_client, platform=platform)
    summary_hint = str(normalized.get("summary_for_user") or "").strip()
    flair_hint = str(normalized.get("flair") or "").strip()
    if summary_hint and not summary:
        summary = summary_hint

    out: Dict[str, Any] = {
        "ok": bool(normalized.get("ok")),
        "summary_for_user": str(summary or "").strip(),
    }
    if flair_hint:
        out["flair"] = flair_hint

    safe_data = result_for_llm(normalized) if isinstance(normalized, dict) else {}
    if isinstance(safe_data, dict):
        out["data"] = safe_data

    artifacts = normalized.get("artifacts")
    if isinstance(artifacts, list):
        compact_artifacts = [item for item in artifacts if isinstance(item, dict)]
        if compact_artifacts:
            out["artifacts"] = compact_artifacts[:12]

    errors: List[str] = []
    if not out["ok"]:
        err = normalized.get("error")
        if isinstance(err, dict):
            message = str(err.get("message") or "").strip()
            if message:
                errors.append(message)
        needs = normalized.get("needs")
        if isinstance(needs, list):
            for item in needs:
                text = str(item).strip()
                if text:
                    errors.append(text)
    if errors:
        out["errors"] = errors[:5]
    return out


async def _execute_tool_call(
    *,
    llm_client: Any,
    tool_call: Dict[str, Any],
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    context: Optional[Dict[str, Any]],
    user_text: str,
    origin: Optional[Dict[str, Any]],
    scope: str,
    wait_callback: Optional[Callable[[str, Any], Any]],
    admin_guard: Optional[Callable[[str], Optional[Dict[str, Any]]]],
) -> Dict[str, Any]:
    func = str(tool_call.get("function") or "").strip()
    args = dict(tool_call.get("arguments") or {})
    args = _attach_origin(
        args,
        origin=origin,
        platform=platform,
        scope=scope,
        request_text=user_text,
    )

    plugin_obj = registry.get(func)
    if admin_guard:
        guard_result = admin_guard(func)
        if guard_result:
            payload = normalize_plugin_result(guard_result)
            checker_result = await _normalize_tool_result_for_checker(
                result_payload=payload,
                llm_client=llm_client,
                platform=platform,
            )
            return {"payload": payload, "checker_result": checker_result}

    if wait_callback:
        try:
            await wait_callback(func, plugin_obj)
        except Exception:
            pass

    runtime_context: Dict[str, Any] = {}
    if isinstance(context, dict):
        runtime_context.update(context)
    if str(user_text or "").strip():
        runtime_context.setdefault("request_text", str(user_text).strip())

    if is_meta_tool(func):
        payload = run_meta_tool(
            func=func,
            args=args,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
        normalized_payload = normalize_plugin_result(payload)
    else:
        if plugin_obj and not plugin_supports_platform(plugin_obj, platform):
            available_on = expand_plugin_platforms(getattr(plugin_obj, "platforms", []) or [])
            normalized_payload = action_failure(
                code="unsupported_platform",
                message=f"`{plugin_display_name(plugin_obj)}` is not available on {platform}.",
                available_on=available_on,
                say_hint="Explain this tool is unavailable on this platform and list supported platforms.",
            )
        else:
            exec_result = await execute_plugin_call(
                func=func,
                args=args,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                llm_client=llm_client,
                context=runtime_context,
            )
            normalized_payload = normalize_plugin_result(exec_result.get("result"))

    checker_result = await _normalize_tool_result_for_checker(
        result_payload=normalized_payload,
        llm_client=llm_client,
        platform=platform,
    )
    return {"payload": normalized_payload, "checker_result": checker_result}


def _parse_checker_decision(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {"kind": "FINAL_ANSWER", "text": ""}

    match = re.match(
        r"^\s*(FINAL_ANSWER|RETRY_TOOL|NEED_USER_INFO)\s*:\s*(.*)$",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        if _is_tool_candidate(raw):
            parsed = parse_function_json(raw)
            if isinstance(parsed, dict):
                return {"kind": "RETRY_TOOL", "tool_call": parsed, "text": raw}
        return {"kind": "FINAL_ANSWER", "text": raw}

    kind = str(match.group(1) or "").upper().strip()
    body = str(match.group(2) or "").strip()
    if kind == "RETRY_TOOL":
        parsed = parse_function_json(body)
        return {"kind": "RETRY_TOOL", "tool_call": parsed, "text": body}
    if kind == "NEED_USER_INFO":
        return {"kind": "NEED_USER_INFO", "text": body}
    return {"kind": "FINAL_ANSWER", "text": body}


async def _run_checker(
    *,
    llm_client: Any,
    platform: str,
    current_user_text: str,
    resolved_user_text: str,
    agent_state: Optional[Dict[str, Any]],
    planned_tool: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    draft_response: str,
    retry_allowed: bool,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    payload = {
        "current_user_message": current_user_text,
        "resolved_request_for_this_turn": resolved_user_text,
        "original_user_request": resolved_user_text,
        "agent_state": _normalize_agent_state(agent_state, fallback_goal=resolved_user_text),
        "planned_tool": planned_tool,
        "tool_result": tool_result,
        "draft_response": draft_response,
    }
    try:
        token_limit = int(max_tokens) if max_tokens is not None else _configured_checker_max_tokens()
        response = await llm_client.chat(
            messages=_with_platform_preamble([
                {"role": "system", "content": _checker_system_prompt(platform, retry_allowed=retry_allowed)},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ], platform_preamble=platform_preamble),
            max_tokens=max(1, token_limit),
            temperature=0.1,
        )
        text = _coerce_text((response.get("message", {}) or {}).get("content", "")).strip()
        return _parse_checker_decision(text)
    except Exception:
        return {"kind": "FINAL_ANSWER", "text": str(draft_response or "").strip()}


def _sanitize_user_text(text: str, *, platform: str, tool_used: bool) -> str:
    out = str(text or "").strip()
    if not out:
        return DEFAULT_CLARIFICATION

    if looks_like_tool_markup(out):
        return DEFAULT_CLARIFICATION
    if parse_function_json(out):
        return DEFAULT_CLARIFICATION
    if re.search(
        r"\{[^{}]*\"function\"\s*:\s*\"[^\"]+\"[^{}]*\"arguments\"\s*:\s*\{",
        out,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        return DEFAULT_CLARIFICATION
    if re.match(r"^\s*(FINAL_ANSWER|RETRY_TOOL|NEED_USER_INFO)\s*:", out, flags=re.IGNORECASE):
        out = re.sub(
            r"^\s*(FINAL_ANSWER|RETRY_TOOL|NEED_USER_INFO)\s*:\s*",
            "",
            out,
            count=1,
            flags=re.IGNORECASE,
        ).strip()

    if re.search(
        r"\b(planner head|doer head|critic head|internal orchestration|tool runtime|repair prompt|orchestration roles?)\b",
        out,
        flags=re.IGNORECASE,
    ):
        return "I'm your assistant."

    if platform in ASCII_ONLY_PLATFORMS:
        out = out.encode("ascii", "ignore").decode().strip()

    return out or DEFAULT_CLARIFICATION


def _short_text(value: Any, *, limit: int = 280) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


def _is_low_information_text(value: Any) -> bool:
    text = " ".join(str(value or "").strip().lower().split())
    if not text:
        return True
    if re.fullmatch(r"(ok|okay|done|complete|completed|success|successful|all set|finished)[.!]?", text):
        return True
    if len(text) <= 6 and text in {"yes", "no", "maybe"}:
        return True
    return False


def _first_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = _coerce_text(text).strip()
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


def _state_list(value: Any, *, max_items: int, item_limit: int) -> List[str]:
    items: List[Any]
    if isinstance(value, list):
        items = value
    elif isinstance(value, str) and value.strip():
        items = [value]
    else:
        items = []
    out: List[str] = []
    for item in items:
        line = " ".join(_coerce_text(item).split())
        line = _short_text(line, limit=item_limit)
        if not line:
            continue
        out.append(line)
        if len(out) >= max_items:
            break
    return out


def _state_next_step(value: Any) -> str:
    if isinstance(value, dict):
        func = str(value.get("function") or "").strip()
        args = value.get("arguments") if isinstance(value.get("arguments"), dict) else {}
        if not func:
            return ""
        args_hint: List[str] = []
        for key in list(args.keys())[:2]:
            val = args.get(key)
            text = _short_text(_coerce_text(val), limit=40)
            if text:
                args_hint.append(f"{key}={text}")
        if args_hint:
            return _short_text(f"{func}({', '.join(args_hint)})", limit=180)
        return _short_text(func, limit=180)
    return _short_text(" ".join(_coerce_text(value).split()), limit=180)


def _normalize_agent_state(state: Optional[Dict[str, Any]], *, fallback_goal: str) -> Dict[str, Any]:
    source = state if isinstance(state, dict) else {}
    goal = _short_text(" ".join(_coerce_text(source.get("goal")).split()), limit=180)
    if not goal:
        goal = _short_text(" ".join(str(fallback_goal or "").split()), limit=180) or "Fulfill the user request."
    out = {
        "goal": goal,
        "plan": _state_list(source.get("plan"), max_items=3, item_limit=120),
        "facts": _state_list(source.get("facts"), max_items=8, item_limit=140),
        "open_questions": _state_list(source.get("open_questions"), max_items=4, item_limit=160),
        "next_step": _state_next_step(source.get("next_step")),
        "tool_history": _state_list(source.get("tool_history"), max_items=8, item_limit=150),
    }
    return out


def _compact_agent_state_json(state: Optional[Dict[str, Any]], *, fallback_goal: str, limit: int) -> str:
    compact = _normalize_agent_state(state, fallback_goal=fallback_goal)

    def _dump() -> str:
        return json.dumps(compact, ensure_ascii=False, separators=(",", ":"))

    payload = _dump()
    if len(payload) <= limit:
        return payload

    for key in ("tool_history", "facts", "plan", "open_questions"):
        while len(payload) > limit and isinstance(compact.get(key), list) and len(compact.get(key) or []) > 1:
            compact[key] = (compact.get(key) or [])[1:]
            payload = _dump()
    if len(payload) <= limit:
        return payload

    compact["goal"] = _short_text(compact.get("goal"), limit=120)
    compact["next_step"] = _short_text(compact.get("next_step"), limit=100)
    for key in ("facts", "tool_history", "plan", "open_questions"):
        lines = compact.get(key) if isinstance(compact.get(key), list) else []
        compact[key] = [_short_text(line, limit=80) for line in lines[:3]]
    payload = _dump()
    if len(payload) <= limit:
        return payload

    compact["facts"] = compact.get("facts", [])[:2]
    compact["tool_history"] = compact.get("tool_history", [])[:2]
    compact["plan"] = compact.get("plan", [])[:2]
    compact["open_questions"] = compact.get("open_questions", [])[:1]
    payload = _dump()
    if len(payload) <= limit:
        return payload
    return _short_text(payload, limit=limit)


def _agent_state_prompt_message(state: Optional[Dict[str, Any]], *, fallback_goal: str) -> str:
    payload = _compact_agent_state_json(
        state,
        fallback_goal=fallback_goal,
        limit=AGENT_STATE_PROMPT_MAX_CHARS,
    )
    return "Current agent state (compact JSON):\n" + payload


def _agent_state_hash(state: Optional[Dict[str, Any]], *, fallback_goal: str) -> str:
    payload = _compact_agent_state_json(
        state,
        fallback_goal=fallback_goal,
        limit=AGENT_STATE_LEDGER_MAX_CHARS,
    )
    digest = hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()
    return f"sha256:{digest}"


def _agent_state_key(*, platform: str, scope: str) -> str:
    normalized_platform = normalize_platform(platform)
    normalized_scope = _clean_scope_text(scope)
    if not normalized_scope or _scope_is_generic(normalized_scope):
        normalized_scope = _unknown_scope(normalized_platform, {"platform": normalized_platform})
    return f"{AGENT_STATE_KEY_PREFIX}{normalized_platform}:{normalized_scope}"


_AGENT_STATE_REQUIRED_KEYS = ("goal", "plan", "facts", "open_questions", "next_step", "tool_history")


def _has_required_agent_state_keys(state: Any) -> bool:
    if not isinstance(state, dict):
        return False
    for key in _AGENT_STATE_REQUIRED_KEYS:
        if key not in state:
            return False
    return True


def _load_persistent_agent_state(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
) -> Optional[Dict[str, Any]]:
    if redis_client is None:
        return None
    key = _agent_state_key(platform=platform, scope=scope)
    try:
        raw = redis_client.get(key)
    except Exception:
        return None
    text = _coerce_text(raw).strip()
    if not text:
        return None
    parsed = _first_json_object(text)
    if not _has_required_agent_state_keys(parsed):
        return None
    normalized = _normalize_agent_state(parsed, fallback_goal=str(parsed.get("goal") or ""))
    if not str(normalized.get("goal") or "").strip():
        return None
    if not _has_required_agent_state_keys(normalized):
        return None
    try:
        json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return None
    return normalized


def _save_persistent_agent_state(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
    state: Optional[Dict[str, Any]],
) -> None:
    if redis_client is None or not isinstance(state, dict):
        return
    key = _agent_state_key(platform=platform, scope=scope)
    ttl_seconds = _configured_agent_state_ttl_seconds(redis_client)
    normalized = _normalize_agent_state(state, fallback_goal=str(state.get("goal") or ""))
    if not _has_required_agent_state_keys(normalized):
        return
    if not str(normalized.get("goal") or "").strip():
        return
    try:
        payload = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return
    if not payload or payload in {"{}", "null"}:
        return
    try:
        if ttl_seconds > 0:
            try:
                redis_client.set(key, payload, ex=ttl_seconds)
            except TypeError:
                redis_client.set(key, payload)
                if hasattr(redis_client, "expire"):
                    redis_client.expire(key, ttl_seconds)
        else:
            redis_client.set(key, payload)
    except Exception:
        return


def _references_previous_work(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(it|that|this|those|them|again|continue|same|still|as before|last one|previous|earlier|above)\b",
            lowered,
        )
    )


def _looks_like_short_followup_request(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    tokens = [tok for tok in lowered.split(" ") if tok]
    if len(tokens) > 3:
        return False
    if _references_previous_work(lowered):
        return True
    if lowered in {"ok", "okay", "do it", "go ahead", "again", "same", "same thing"}:
        return True
    return bool(re.search(r"^(ok|okay|yes|yep|sure)\b", lowered))


def _should_reset_state_for_topic_change(current_user_text: str) -> bool:
    current = str(current_user_text or "").strip()
    if not current:
        return False
    if _contains_new_domain_reset_keywords(current):
        if _references_explicit_prior_work(current):
            return False
        return True
    if _looks_like_short_followup_request(current):
        return False
    if _references_previous_work(current):
        return False
    if not _looks_like_standalone_request(current):
        return False
    return True


def _contains_new_domain_reset_keywords(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(weather|download|summarize|summary|create plugin|build plugin|create platform|reminder|task|search web|inspect webpage|memory|upload|send message)\b",
            lowered,
        )
    )


def _references_explicit_prior_work(text: str) -> bool:
    lowered = " ".join(str(text or "").strip().lower().split())
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(it|that|again|previous|as before|same as before)\b",
            lowered,
        )
    )


def _new_agent_state(goal: str) -> Dict[str, Any]:
    return _normalize_agent_state(
        {
            "goal": goal,
            "plan": [],
            "facts": [],
            "open_questions": [],
            "next_step": "",
            "tool_history": [],
        },
        fallback_goal=goal,
    )


def _initial_agent_state_for_turn(
    *,
    prior_state: Optional[Dict[str, Any]],
    current_user_text: str,
    resolved_user_text: str,
) -> Dict[str, Any]:
    goal = _short_text((resolved_user_text or current_user_text or "").strip(), limit=180)
    goal = goal or "Fulfill the user request."
    if not isinstance(prior_state, dict):
        return _new_agent_state(goal)
    if _should_reset_state_for_topic_change(current_user_text):
        return _new_agent_state(goal)
    merged = _normalize_agent_state(prior_state, fallback_goal=goal)
    merged["goal"] = goal
    return merged


def _state_add_line(state_list: List[str], line: str, *, max_items: int) -> List[str]:
    text = _short_text(" ".join(str(line or "").split()), limit=150)
    if not text:
        return state_list
    lowered = text.lower()
    for existing in state_list:
        if str(existing).strip().lower() == lowered:
            return state_list
    out = list(state_list)
    out.append(text)
    return out[-max_items:]


def _tool_history_line(
    *,
    tool_call: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
) -> str:
    func = ""
    if isinstance(tool_call, dict):
        func = str(tool_call.get("function") or "").strip()
    func = func or "tool"
    ok = bool((tool_result or {}).get("ok"))
    summary = _short_text((tool_result or {}).get("summary_for_user"), limit=120)
    if not summary:
        errors = (tool_result or {}).get("errors")
        if isinstance(errors, list) and errors:
            summary = _short_text(errors[0], limit=120)
    if not summary:
        summary = "no summary"
    status = "ok" if ok else "failed"
    return f"{func}:{status}:{summary}"


def _compact_tool_result_for_doer(tool_result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    source = tool_result if isinstance(tool_result, dict) else {}
    out: Dict[str, Any] = {
        "ok": bool(source.get("ok")),
        "summary_for_user": _short_text(source.get("summary_for_user"), limit=260),
    }
    errors: List[str] = []
    raw_errors = source.get("errors")
    if isinstance(raw_errors, list):
        for item in raw_errors:
            line = _short_text(item, limit=180)
            if line:
                errors.append(line)
            if len(errors) >= 3:
                break
    if errors:
        out["errors"] = errors
    data = source.get("data")
    if isinstance(data, dict) and data:
        preview: Dict[str, Any] = {}
        for key in list(data.keys())[:8]:
            value = data.get(key)
            if isinstance(value, (str, int, float, bool)) or value is None:
                preview[key] = value
                continue
            try:
                preview[key] = _short_text(json.dumps(value, ensure_ascii=False), limit=160)
            except Exception:
                preview[key] = _short_text(str(value), limit=160)
        out["data_preview"] = preview
    return out


async def _run_doer_state_update(
    *,
    llm_client: Any,
    platform: str,
    user_request: str,
    prior_state: Optional[Dict[str, Any]],
    tool_call: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    previous = _normalize_agent_state(prior_state, fallback_goal=user_request)
    payload = {
        "platform": platform,
        "user_request": str(user_request or ""),
        "prior_state": previous,
        "tool_call": {
            "function": str((tool_call or {}).get("function") or "").strip(),
            "arguments": (tool_call or {}).get("arguments") if isinstance((tool_call or {}).get("arguments"), dict) else {},
        },
        "tool_result": _compact_tool_result_for_doer(tool_result),
    }
    prompt = (
        "You are the Doer head in a Planner/Doer/Critic loop.\n"
        "Update only the agent state.\n"
        "Return exactly one compact JSON object with keys:\n"
        "goal, plan, facts, open_questions, next_step, tool_history\n"
        "Rules:\n"
        "- Use short plain text snippets.\n"
        "- Keep facts stable and deterministic.\n"
        "- Keep open_questions only for real blockers.\n"
        "- next_step is a short tool sketch or empty.\n"
        "- No markdown."
    )
    merged: Dict[str, Any] = dict(previous)
    try:
        token_limit = int(max_tokens) if max_tokens is not None else _configured_doer_max_tokens()
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            max_tokens=max(1, token_limit),
            temperature=0.1,
        )
        text = _coerce_text((response.get("message", {}) or {}).get("content", "")).strip()
        patch_state = _first_json_object(text)
        if isinstance(patch_state, dict):
            merged = _normalize_agent_state(patch_state, fallback_goal=previous.get("goal") or user_request)
    except Exception:
        merged = dict(previous)

    merged["tool_history"] = _state_add_line(
        list(merged.get("tool_history") or []),
        _tool_history_line(tool_call=tool_call, tool_result=tool_result),
        max_items=8,
    )

    summary = _short_text((tool_result or {}).get("summary_for_user"), limit=150)
    if summary and not _is_low_information_text(summary):
        merged["facts"] = _state_add_line(list(merged.get("facts") or []), summary, max_items=8)

    if not bool((tool_result or {}).get("ok")):
        errors = (tool_result or {}).get("errors")
        if isinstance(errors, list) and errors:
            merged["open_questions"] = _state_add_line(
                list(merged.get("open_questions") or []),
                _short_text(errors[0], limit=150),
                max_items=4,
            )
    elif merged.get("open_questions"):
        merged["open_questions"] = _state_list(merged.get("open_questions"), max_items=4, item_limit=140)

    return _normalize_agent_state(merged, fallback_goal=previous.get("goal") or user_request)


def _state_first_open_question(state: Optional[Dict[str, Any]]) -> str:
    source = state if isinstance(state, dict) else {}
    open_questions = source.get("open_questions")
    if isinstance(open_questions, list):
        for item in open_questions:
            line = _short_text(item, limit=220)
            if line:
                if line.endswith("?"):
                    return line
                return f"{line}?"
    return ""


def _state_best_effort_answer(
    *,
    state: Optional[Dict[str, Any]],
    draft_response: str,
    tool_result: Optional[Dict[str, Any]],
) -> str:
    draft = _short_text(draft_response, limit=320)
    if draft and not _is_low_information_text(draft):
        return draft
    source = state if isinstance(state, dict) else {}
    facts = source.get("facts") if isinstance(source.get("facts"), list) else []
    compact_facts = [_short_text(item, limit=180) for item in facts if _short_text(item, limit=180)]
    if compact_facts:
        return "; ".join(compact_facts[:3])
    summary = _short_text((tool_result or {}).get("summary_for_user"), limit=220)
    if summary and not _is_low_information_text(summary):
        return summary
    return "Completed."


def _tool_failure_checker_reason(tool_result: Optional[Dict[str, Any]]) -> str:
    if not isinstance(tool_result, dict):
        return ""
    if bool(tool_result.get("ok")):
        return ""
    code = ""
    data = tool_result.get("data")
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            code = str(err.get("code") or "").strip().lower()
    if code:
        return f"tool_failed:{code}"
    return "tool_failed"


def _hash_tool_args(args: Any) -> str:
    if not isinstance(args, dict):
        return ""
    try:
        payload = json.dumps(args, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        payload = str(args)
    digest = hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()
    return f"sha256:{digest}"


def _compact_tool_ref(tool_call: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(tool_call, dict):
        return None
    func = str(tool_call.get("function") or "").strip()
    if not func:
        return None
    args = tool_call.get("arguments") if isinstance(tool_call.get("arguments"), dict) else {}
    out: Dict[str, Any] = {"function": func}
    args_hash = _hash_tool_args(args)
    if args_hash:
        out["args_hash"] = args_hash
    return out


def _validation_status_for_ledger(
    *,
    validation_status: Optional[Dict[str, Any]],
    planned_tool: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    source = validation_status if isinstance(validation_status, dict) else {}
    has_planned_tool = isinstance(planned_tool, dict) and bool(str(planned_tool.get("function") or "").strip())

    if not has_planned_tool:
        raw_status = str(source.get("status") or "").strip().lower()
        if raw_status == "skipped" or "reason" in source:
            repair_used = bool(source.get("repair_used"))
            attempts = source.get("attempts")
            try:
                attempts_i = int(attempts) if attempts is not None else 0
            except Exception:
                attempts_i = 0
            out: Dict[str, Any] = {
                "status": "skipped",
                "repair_used": repair_used,
                "reason": _short_text(source.get("reason") or "no_tool", limit=64),
                "attempts": max(0, attempts_i),
            }
            error_text = _short_text(source.get("error"), limit=180)
            if error_text:
                out["error"] = error_text
            return out

        if "ok" in source and not bool(source.get("ok")):
            reason = _short_text(source.get("reason") or "no_tool", limit=64)
            out = {
                "status": "skipped",
                "repair_used": False,
                "reason": reason or "no_tool",
                "attempts": 0,
            }
            error_text = _short_text(source.get("error"), limit=180)
            if error_text:
                out["error"] = error_text
            return out

        return {
            "status": "skipped",
            "repair_used": False,
            "reason": "no_tool",
            "attempts": 0,
        }

    raw_status = str(source.get("status") or "").strip().lower()
    if raw_status in {"ok", "failed", "skipped"}:
        status = raw_status
        repair_used = bool(source.get("repair_used"))
        attempts = source.get("attempts")
        try:
            attempts_i = int(attempts) if attempts is not None else (2 if repair_used else 1)
        except Exception:
            attempts_i = 2 if repair_used else 1
        reason = _short_text(source.get("reason") or ("repaired" if status == "ok" and repair_used else status), limit=64)
        out: Dict[str, Any] = {
            "status": status,
            "repair_used": repair_used,
            "reason": reason or ("failed" if status == "failed" else "ok"),
            "attempts": max(0, attempts_i),
        }
        error_text = _short_text(source.get("error"), limit=180)
        if error_text:
            out["error"] = error_text
        return out

    ok = bool(source.get("ok"))
    repair_used = bool(source.get("repair_used"))
    reason = _short_text(source.get("reason"), limit=64)
    error_text = _short_text(source.get("error"), limit=180)
    attempts_i = 2 if repair_used else 1
    if ok:
        out = {
            "status": "ok",
            "repair_used": repair_used,
            "reason": "repaired" if repair_used else (reason or "ok"),
            "attempts": attempts_i,
        }
        if error_text:
            out["error"] = error_text
        return out

    out = {
        "status": "failed",
        "repair_used": repair_used,
        "reason": reason or "invalid_tool_call",
        "attempts": attempts_i,
    }
    if error_text:
        out["error"] = error_text
    return out


def _llm_backend_label(llm_client: Any) -> str:
    model = _short_text(getattr(llm_client, "model", ""), limit=120)
    host = _short_text(getattr(llm_client, "host", ""), limit=180)
    if not host:
        client_obj = getattr(llm_client, "client", None)
        host = _short_text(getattr(client_obj, "base_url", ""), limit=180)
    host = re.sub(r"^https?://", "", str(host or "").strip(), flags=re.IGNORECASE).rstrip("/")
    host = host.split("/", 1)[0].strip()
    if host and model:
        return f"{host}:{model}"
    return model or host


def _origin_preview_for_ledger(origin: Optional[Dict[str, Any]]) -> Dict[str, str]:
    src = origin if isinstance(origin, dict) else {}
    keys = (
        "channel_id",
        "chat_id",
        "room_id",
        "device_id",
        "area_id",
        "user_id",
        "session_id",
        "chat_type",
        "target",
        "request_id",
    )
    out: Dict[str, str] = {}
    for key in keys:
        value = _short_text(src.get(key), limit=72)
        if value:
            out[key] = value
        if len(out) >= 6:
            break
    return out


def _normalize_outcome(status: str, checker_reason: str) -> tuple[str, str]:
    status_key = str(status or "").strip().lower()
    reason_code = _short_text(checker_reason, limit=96) or "unknown"
    if status_key == "done":
        return "done", reason_code if reason_code else "complete"
    if status_key == "blocked":
        return "blocked", reason_code
    return "failed", reason_code


def _write_cerberus_metrics(
    *,
    redis_client: Any,
    platform: str,
    total_tools_called: int,
    total_repairs: int,
    validation_failures: int,
    tool_failures: int,
) -> None:
    if redis_client is None:
        return
    p = normalize_platform(platform)
    counters = {
        "total_turns": 1,
        "total_tools_called": max(0, int(total_tools_called or 0)),
        "total_repairs": max(0, int(total_repairs or 0)),
        "validation_failures": max(0, int(validation_failures or 0)),
        "tool_failures": max(0, int(tool_failures or 0)),
    }
    for name, amount in counters.items():
        if amount <= 0:
            continue
        keys = [
            f"tater:cerberus:metrics:{name}",
            f"tater:cerberus:metrics:{name}:{p}",
        ]
        for key in keys:
            try:
                redis_client.incrby(key, amount)
            except Exception:
                continue


def _write_cerberus_ledger(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
    turn_id: str,
    llm: str,
    user_message: str,
    planned_tool: Optional[Dict[str, Any]],
    validation_status: Dict[str, Any],
    tool_result: Optional[Dict[str, Any]],
    checker_action: str,
    retry_count: int = 0,
    checker_reason: str = "",
    planner_kind: str = "",
    planner_text_is_tool_candidate: Optional[bool] = None,
    outcome: str = "",
    outcome_reason: str = "",
    planner_ms: int = 0,
    tool_ms: int = 0,
    checker_ms: int = 0,
    total_ms: int = 0,
    retry_tool: Optional[Dict[str, Any]] = None,
    rounds_used: int = 0,
    tool_calls_used: int = 0,
    agent_state: Optional[Dict[str, Any]] = None,
    origin_preview: Optional[Dict[str, Any]] = None,
    attempted_tool: str = "",
) -> None:
    if redis_client is None:
        return
    compact_planned_tool = _compact_tool_ref(planned_tool)
    compact_retry_tool = _compact_tool_ref(retry_tool)
    compact_validation = _validation_status_for_ledger(
        validation_status=validation_status,
        planned_tool=planned_tool,
    )
    planner_kind_value = str(planner_kind or "").strip().lower()
    if planner_kind_value not in {"tool", "answer", "repaired_tool", "repaired_answer", "send_message_fix"}:
        planner_kind_value = "answer"
    entry = {
        "schema_version": CERBERUS_LEDGER_SCHEMA_VERSION,
        "timestamp": time.time(),
        "platform": platform,
        "scope": scope,
        "turn_id": str(turn_id or uuid.uuid4()),
        "llm": str(llm or ""),
        "user_message": str(user_message or "")[:1500],
        "planned_tool": compact_planned_tool,
        "validation": compact_validation,
        "tool_result": None,
        "checker_action": str(checker_action or ""),
        "planner_kind": planner_kind_value,
        "planner_text_is_tool_candidate": bool(planner_text_is_tool_candidate)
        if planner_text_is_tool_candidate is not None
        else bool(compact_planned_tool),
        "validation_reason": _short_text(compact_validation.get("reason"), limit=64),
        "outcome": _short_text(outcome, limit=16) or "done",
        "outcome_reason": _short_text(outcome_reason, limit=96),
        "planner_ms": max(0, int(planner_ms or 0)),
        "tool_ms": max(0, int(tool_ms or 0)),
        "checker_ms": max(0, int(checker_ms or 0)),
        "total_ms": max(0, int(total_ms or 0)),
        "retry_count": 1 if int(retry_count or 0) > 0 else 0,
        "rounds_used": max(0, int(rounds_used or 0)),
        "tool_calls_used": max(0, int(tool_calls_used or 0)),
    }
    if compact_planned_tool and compact_planned_tool.get("args_hash"):
        entry["tool_args_hash"] = compact_planned_tool.get("args_hash")
    checker_reason_text = _short_text(checker_reason, limit=72)
    if checker_reason_text:
        entry["checker_reason"] = checker_reason_text
    attempted_tool_text = _short_text(attempted_tool, limit=80)
    if attempted_tool_text:
        entry["attempted_tool"] = attempted_tool_text
    if compact_retry_tool:
        entry["retry_tool"] = compact_retry_tool
    if isinstance(origin_preview, dict) and origin_preview:
        compact_origin: Dict[str, str] = {}
        for key, value in origin_preview.items():
            k = _short_text(key, limit=24)
            v = _short_text(value, limit=72)
            if not k or not v:
                continue
            compact_origin[k] = v
            if len(compact_origin) >= 6:
                break
        if compact_origin:
            entry["origin_preview"] = compact_origin
    if isinstance(agent_state, dict):
        state_payload = _compact_agent_state_json(
            agent_state,
            fallback_goal=str(user_message or ""),
            limit=AGENT_STATE_LEDGER_MAX_CHARS,
        )
        if state_payload:
            entry["state_snapshot"] = state_payload
            entry["state_hash"] = _agent_state_hash(agent_state, fallback_goal=str(user_message or ""))
    if isinstance(tool_result, dict):
        result_ok = bool(tool_result.get("ok"))
        summary = _short_text(tool_result.get("summary_for_user"), limit=320)
        errors: List[str] = []
        raw_errors = tool_result.get("errors")
        if isinstance(raw_errors, list):
            for item in raw_errors:
                text = _short_text(item, limit=180)
                if text:
                    errors.append(text)
                if len(errors) >= 3:
                    break
        compact_result: Dict[str, Any] = {"ok": result_ok}
        if summary:
            compact_result["summary"] = summary
        if errors:
            compact_result["errors"] = errors
        entry["tool_result"] = compact_result
        entry["tool_result_ok"] = result_ok
        if summary:
            entry["tool_result_summary"] = summary
    payload = json.dumps(entry, ensure_ascii=False)

    keys = ["tater:cerberus:ledger", f"tater:cerberus:ledger:{platform}"]
    max_items = _configured_max_ledger_items(redis_client)
    for key in keys:
        try:
            redis_client.rpush(key, payload)
            redis_client.ltrim(key, -max_items, -1)
        except Exception:
            continue


def _is_tool_candidate(text: str) -> bool:
    if _parse_strict_tool_json(text) is not None:
        return True
    if parse_function_json(text):
        return True
    if looks_like_tool_markup(text):
        return True
    if _looks_like_invalid_tool_call_text(text):
        return True
    return False


async def run_cerberus_turn(
    *,
    llm_client: Any,
    platform: str,
    history_messages: List[Dict[str, Any]],
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]] = None,
    context: Optional[Dict[str, Any]] = None,
    user_text: str,
    scope: str,
    task_id: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
    wait_callback: Optional[Callable[[str, Any], Any]] = None,
    admin_guard: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    redis_client: Any = None,
    max_rounds: Optional[int] = None,
    max_tool_calls: Optional[int] = None,
    platform_preamble: str = "",
) -> Dict[str, Any]:
    r = redis_client or default_redis
    platform = normalize_platform(platform)
    origin_payload = dict(origin) if isinstance(origin, dict) else {}
    scope = _resolve_cerberus_scope(platform, scope, origin_payload)
    platform_preamble = _sanitize_platform_preamble(platform, platform_preamble)
    origin_preview = _origin_preview_for_ledger(origin_payload)
    user_text = str(user_text or "")
    effective_max_rounds, effective_max_tool_calls = resolve_agent_limits(
        redis_client=r,
        max_rounds=max_rounds,
        max_tool_calls=max_tool_calls,
    )
    planner_max_tokens = _configured_planner_max_tokens(r)
    checker_max_tokens = _configured_checker_max_tokens(r)
    doer_max_tokens = _configured_doer_max_tokens(r)
    tool_repair_max_tokens = _configured_tool_repair_max_tokens(r)
    overclar_repair_max_tokens = _configured_overclar_repair_max_tokens(r)
    send_repair_max_tokens = _configured_send_repair_max_tokens(r)
    recovery_max_tokens = _configured_recovery_max_tokens(r)
    turn_started_at = time.perf_counter()
    planner_ms_total = 0.0
    tool_ms_total = 0.0
    checker_ms_total = 0.0
    repairs_used_count = 0
    validation_failures_count = 0
    tool_failures_count = 0
    turn_id = str(uuid.uuid4())
    llm_label = _llm_backend_label(llm_client)

    validation_status: Dict[str, Any] = {
        "status": "skipped",
        "repair_used": False,
        "reason": "no_tool",
        "attempts": 0,
    }
    planned_tool: Optional[Dict[str, Any]] = None
    checker_action = "FINAL_ANSWER"
    checker_reason = "complete"
    tool_result_for_checker: Optional[Dict[str, Any]] = None
    raw_tool_payload_out: Optional[Dict[str, Any]] = None
    normalized_checker_result_out: Optional[Dict[str, Any]] = None
    artifacts_out: List[Dict[str, Any]] = []
    rounds_used = 0
    tool_calls_used = 0
    critic_continue_count = 0
    draft_response = ""
    tool_used = False
    planner_kind = "answer"
    planner_text_is_tool_candidate = False
    attempted_tool_for_ledger = ""

    history = _compact_history(history_messages)
    effective_user_text = _effective_user_text(user_text, history)
    resolved_user_text = effective_user_text or user_text
    current_user_turn_text = _strip_user_sender_prefix(user_text).strip() or str(user_text or "").strip()
    tool_index = _enabled_tool_mini_index(
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
    )
    prior_state = _load_persistent_agent_state(
        redis_client=r,
        platform=platform,
        scope=scope,
    )
    agent_state: Dict[str, Any] = _initial_agent_state_for_turn(
        prior_state=prior_state,
        current_user_text=current_user_turn_text,
        resolved_user_text=resolved_user_text,
    )
    queued_tool_call: Optional[Dict[str, Any]] = None
    queued_retry_tool_for_ledger: Optional[Dict[str, Any]] = None

    def _retry_allowed_within_limits() -> bool:
        rounds_left = effective_max_rounds == 0 or rounds_used < effective_max_rounds
        tools_left = effective_max_tool_calls == 0 or tool_calls_used < effective_max_tool_calls
        return rounds_left and tools_left

    def _finish(
        *,
        text: str,
        status: str,
        checker_action_value: str,
        checker_reason_value: str,
        planner_kind_value: Optional[str] = None,
        planner_text_is_tool_candidate_value: Optional[bool] = None,
        planned_tool_override: Optional[Dict[str, Any]] = None,
        validation_status_override: Optional[Dict[str, Any]] = None,
        retry_tool: Optional[Dict[str, Any]] = None,
        attempted_tool_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        final_text = _sanitize_user_text(text, platform=platform, tool_used=tool_used)
        outcome_value, outcome_reason_value = _normalize_outcome(status, checker_reason_value)
        total_ms = int(max(0.0, (time.perf_counter() - turn_started_at) * 1000.0))
        _save_persistent_agent_state(
            redis_client=r,
            platform=platform,
            scope=scope,
            state=agent_state,
        )
        _write_cerberus_ledger(
            redis_client=r,
            platform=platform,
            scope=scope,
            turn_id=turn_id,
            llm=llm_label,
            user_message=user_text,
            planned_tool=planned_tool_override if planned_tool_override is not None else planned_tool,
            validation_status=validation_status_override if validation_status_override is not None else validation_status,
            tool_result=tool_result_for_checker,
            checker_action=checker_action_value,
            retry_count=max(0, int(critic_continue_count)),
            checker_reason=checker_reason_value,
            planner_kind=planner_kind_value if planner_kind_value is not None else planner_kind,
            planner_text_is_tool_candidate=(
                planner_text_is_tool_candidate_value
                if planner_text_is_tool_candidate_value is not None
                else planner_text_is_tool_candidate
            ),
            outcome=outcome_value,
            outcome_reason=outcome_reason_value,
            planner_ms=int(max(0.0, planner_ms_total)),
            tool_ms=int(max(0.0, tool_ms_total)),
            checker_ms=int(max(0.0, checker_ms_total)),
            total_ms=total_ms,
            retry_tool=retry_tool,
            rounds_used=rounds_used,
            tool_calls_used=tool_calls_used,
            agent_state=agent_state,
            origin_preview=origin_preview,
            attempted_tool=attempted_tool_override if attempted_tool_override is not None else attempted_tool_for_ledger,
        )
        _write_cerberus_metrics(
            redis_client=r,
            platform=platform,
            total_tools_called=tool_calls_used,
            total_repairs=repairs_used_count,
            validation_failures=validation_failures_count,
            tool_failures=tool_failures_count,
        )
        return {
            "text": final_text,
            "status": status,
            "task_id": task_id,
            "artifacts": artifacts_out,
            "raw_tool_payload": raw_tool_payload_out,
            "normalized_checker_result": normalized_checker_result_out,
        }

    while (
        (effective_max_rounds == 0 or rounds_used < effective_max_rounds)
        and (effective_max_tool_calls == 0 or tool_calls_used < effective_max_tool_calls)
    ):
        rounds_used += 1
        planned_tool = None
        planner_text = ""
        round_planner_kind = "answer"

        if isinstance(queued_tool_call, dict):
            planned_tool = dict(queued_tool_call)
            attempted_tool_for_ledger = str((planned_tool or {}).get("function") or attempted_tool_for_ledger or "")
            planner_text_is_tool_candidate = True
            validation_status = {
                "status": "ok",
                "repair_used": bool(validation_status.get("repair_used")),
                "reason": str(validation_status.get("reason") or "ok"),
                "attempts": int(validation_status.get("attempts") or 1),
                "ok": True,
                "tool_call": planned_tool,
            }
            round_planner_kind = "repaired_tool" if bool(validation_status.get("repair_used")) else "tool"
            queued_tool_call = None
        else:
            planner_text_repaired = False
            send_message_fix_applied = False
            state_message = _agent_state_prompt_message(agent_state, fallback_goal=resolved_user_text or user_text)
            planner_messages: List[Dict[str, Any]] = [
                {"role": "system", "content": _planner_system_prompt(platform)},
                {"role": "system", "content": "Enabled tools on this platform:\n" + tool_index},
                {
                    "role": "system",
                    "content": _planner_focus_prompt(
                        current_user_text=user_text,
                        resolved_user_text=resolved_user_text,
                    ),
                },
                {"role": "system", "content": state_message},
            ]
            planner_messages = _with_platform_preamble(
                planner_messages,
                platform_preamble=platform_preamble,
            )
            planner_messages.extend(history)
            planner_messages.append({"role": "user", "content": resolved_user_text})

            try:
                planner_started = time.perf_counter()
                planner_resp = await llm_client.chat(
                    messages=planner_messages,
                    max_tokens=max(1, int(planner_max_tokens)),
                    temperature=0.2,
                )
                planner_ms_total += (time.perf_counter() - planner_started) * 1000.0
                planner_text = _coerce_text((planner_resp.get("message", {}) or {}).get("content", "")).strip()
            except Exception:
                planner_text = ""

            if planner_text and not _is_tool_candidate(planner_text) and _looks_like_over_clarification(
                planner_text,
                user_text=resolved_user_text or user_text,
            ):
                original_planner_text = planner_text
                repaired_text = await _repair_over_clarification_text(
                    llm_client=llm_client,
                    platform=platform,
                    user_text=effective_user_text or user_text,
                    planner_text=planner_text,
                    tool_index=tool_index,
                    platform_preamble=platform_preamble,
                    max_tokens=overclar_repair_max_tokens,
                )
                if repaired_text:
                    planner_text = repaired_text
                    if planner_text.strip() != original_planner_text.strip():
                        planner_text_repaired = True
                        repairs_used_count += 1

            if _is_tool_candidate(planner_text):
                round_planner_kind = "tool"
            elif send_message_fix_applied:
                round_planner_kind = "send_message_fix"
            elif planner_text_repaired:
                round_planner_kind = "repaired_answer"
            else:
                round_planner_kind = "answer"
            planner_text_is_tool_candidate = _is_tool_candidate(planner_text)

            if not _is_tool_candidate(planner_text):
                planner_kind = round_planner_kind
                draft_response = str(planner_text or "").strip()
                checker_started = time.perf_counter()
                checker_decision = await _run_checker(
                    llm_client=llm_client,
                    platform=platform,
                    current_user_text=user_text,
                    resolved_user_text=resolved_user_text,
                    agent_state=agent_state,
                    planned_tool=None,
                    tool_result=tool_result_for_checker,
                    draft_response=draft_response,
                    retry_allowed=_retry_allowed_within_limits(),
                    platform_preamble=platform_preamble,
                    max_tokens=checker_max_tokens,
                )
                checker_ms_total += (time.perf_counter() - checker_started) * 1000.0
                checker_action = str(checker_decision.get("kind") or "FINAL_ANSWER")

                if checker_action == "NEED_USER_INFO":
                    checker_reason = "needs_user_input"
                    return _finish(
                        text=str(checker_decision.get("text") or DEFAULT_CLARIFICATION).strip(),
                        status="blocked",
                        checker_action_value="NEED_USER_INFO",
                        checker_reason_value=checker_reason,
                    )

                if checker_action == "RETRY_TOOL":
                    retry_text = str(checker_decision.get("text") or "").strip()
                    if not _retry_allowed_within_limits():
                        queued_retry_tool_for_ledger = parse_function_json(retry_text)
                        planner_text_is_tool_candidate = True
                        checker_reason = "budget_exhausted"
                        break
                    retry_eval = await _validate_or_recover_tool_call(
                        llm_client=llm_client,
                        text=retry_text,
                        platform=platform,
                        registry=registry,
                        enabled_predicate=enabled_predicate,
                        tool_index=tool_index,
                        user_text=user_text,
                        origin=origin_payload,
                        scope=scope,
                        history_messages=history,
                        context=context if isinstance(context, dict) else {},
                        platform_preamble=platform_preamble,
                        repair_max_tokens=tool_repair_max_tokens,
                        recovery_max_tokens=recovery_max_tokens,
                        send_repair_max_tokens=send_repair_max_tokens,
                    )
                    retry_validation = (
                        retry_eval.get("validation_status")
                        if isinstance(retry_eval.get("validation_status"), dict)
                        else {"status": "failed", "reason": str(retry_eval.get("reason") or "invalid_tool_call")}
                    )
                    attempted_tool_for_ledger = str(retry_eval.get("attempted_tool") or attempted_tool_for_ledger or "")
                    if bool(retry_eval.get("repair_used")):
                        repairs_used_count += 1
                    if not bool(retry_eval.get("ok")):
                        reason = str(retry_eval.get("reason") or "invalid_tool_call")
                        failed_retry_tool = retry_eval.get("tool_call")
                        if not isinstance(failed_retry_tool, dict):
                            failed_retry_tool = {"function": "invalid_tool_call", "arguments": {}}
                        if reason == "send_message_misfire":
                            checker_reason = f"send_message_misfire:{str(retry_eval.get('send_reason') or 'unknown')}"
                            return _finish(
                                text=str(retry_eval.get("recovery_text_if_blocked") or "How can I help?").strip(),
                                status="done",
                                checker_action_value="FINAL_ANSWER",
                                checker_reason_value=checker_reason,
                                planned_tool_override=failed_retry_tool,
                                validation_status_override=retry_validation,
                                planner_kind_value="send_message_fix",
                                attempted_tool_override=str(retry_eval.get("attempted_tool") or ""),
                            )
                        validation_failures_count += 1
                        checker_reason = f"validation_failed:{reason}"
                        return _finish(
                            text=str(retry_eval.get("recovery_text_if_blocked") or DEFAULT_CLARIFICATION).strip(),
                            status="blocked",
                            checker_action_value="NEED_USER_INFO",
                            checker_reason_value=checker_reason,
                            planned_tool_override=failed_retry_tool,
                            validation_status_override=retry_validation,
                            attempted_tool_override=str(retry_eval.get("attempted_tool") or ""),
                        )
                    queued = retry_eval.get("tool_call")
                    if not isinstance(queued, dict):
                        validation_failures_count += 1
                        checker_reason = "validation_failed:invalid_tool_call"
                        return _finish(
                            text=DEFAULT_CLARIFICATION,
                            status="blocked",
                            checker_action_value="NEED_USER_INFO",
                            checker_reason_value=checker_reason,
                            planned_tool_override={"function": "invalid_tool_call", "arguments": {}},
                            validation_status_override=retry_validation,
                        )
                    queued_tool_call = queued
                    queued_retry_tool_for_ledger = queued
                    validation_status = retry_validation
                    critic_continue_count += 1
                    checker_reason = "continue"
                    planner_text_is_tool_candidate = True
                    continue

                checker_reason = _tool_failure_checker_reason(tool_result_for_checker) or "complete"
                return _finish(
                    text=str(checker_decision.get("text") or draft_response or DEFAULT_CLARIFICATION).strip(),
                    status="done",
                    checker_action_value="FINAL_ANSWER",
                    checker_reason_value=checker_reason,
                )

            tool_eval = await _validate_or_recover_tool_call(
                llm_client=llm_client,
                text=planner_text,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                tool_index=tool_index,
                user_text=user_text,
                origin=origin_payload,
                scope=scope,
                history_messages=history,
                context=context if isinstance(context, dict) else {},
                platform_preamble=platform_preamble,
                repair_max_tokens=tool_repair_max_tokens,
                recovery_max_tokens=recovery_max_tokens,
                send_repair_max_tokens=send_repair_max_tokens,
            )
            validation_status = (
                tool_eval.get("validation_status")
                if isinstance(tool_eval.get("validation_status"), dict)
                else validation_status
            )
            attempted_tool_for_ledger = str(tool_eval.get("attempted_tool") or attempted_tool_for_ledger or "")
            if bool(tool_eval.get("repair_used")):
                repairs_used_count += 1
            if bool(tool_eval.get("repair_used")):
                round_planner_kind = "repaired_tool"
            else:
                round_planner_kind = "tool"

            if not bool(tool_eval.get("ok")):
                planner_text_is_tool_candidate = True
                reason = str(tool_eval.get("reason") or "invalid_tool_call")
                recovery_text = str(tool_eval.get("recovery_text_if_blocked") or DEFAULT_CLARIFICATION).strip()
                failed_planned_tool = tool_eval.get("tool_call")
                if not isinstance(failed_planned_tool, dict):
                    failed_planned_tool = {"function": "invalid_tool_call", "arguments": {}}

                if reason == "send_message_misfire":
                    send_message_fix_applied = True
                    planner_kind = "send_message_fix"
                    send_reason = str(tool_eval.get("send_reason") or "unknown")
                    checker_reason = f"send_message_misfire:{send_reason}"
                    return _finish(
                        text=recovery_text,
                        status="done",
                        checker_action_value="FINAL_ANSWER",
                        checker_reason_value=checker_reason,
                        planned_tool_override=failed_planned_tool,
                        validation_status_override=validation_status,
                        planner_kind_value="send_message_fix",
                        attempted_tool_override=str(tool_eval.get("attempted_tool") or ""),
                    )

                validation_failures_count += 1
                planner_kind = round_planner_kind
                checker_reason = f"validation_failed:{reason}"
                return _finish(
                    text=recovery_text,
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    planned_tool_override=failed_planned_tool,
                    validation_status_override=validation_status,
                    attempted_tool_override=str(tool_eval.get("attempted_tool") or ""),
                )

            planned_tool = tool_eval.get("tool_call") if isinstance(tool_eval.get("tool_call"), dict) else None
            planner_text_is_tool_candidate = True
            if not planned_tool:
                validation_failures_count += 1
                checker_reason = "validation_failed:invalid_tool_call"
                return _finish(
                    text=DEFAULT_CLARIFICATION,
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    planned_tool_override={"function": "invalid_tool_call", "arguments": {}},
                    validation_status_override=validation_status,
                )
            attempted_tool_for_ledger = str((planned_tool or {}).get("function") or attempted_tool_for_ledger or "")
            planner_kind = round_planner_kind

        planner_kind = round_planner_kind
        if not isinstance(planned_tool, dict):
            planner_kind = round_planner_kind
            validation_failures_count += 1
            checker_reason = "validation_failed:invalid_tool_call"
            return _finish(
                text=DEFAULT_CLARIFICATION,
                status="blocked",
                checker_action_value="NEED_USER_INFO",
                checker_reason_value=checker_reason,
                planned_tool_override={"function": "invalid_tool_call", "arguments": {}},
                validation_status_override=validation_status,
            )

        send_allowed, send_reason = _send_message_allowed_for_turn(
            tool_call=planned_tool,
            user_text=resolved_user_text,
            origin=origin_payload,
            platform=platform,
            scope=scope,
            history_messages=history,
            context=context if isinstance(context, dict) else {},
        )
        if not send_allowed:
            repairs_used_count += 1
            planner_kind = "send_message_fix"
            checker_reason = f"send_message_misfire:{send_reason or 'unknown'}"
            repaired_text = await _repair_send_message_misfire_text(
                llm_client=llm_client,
                platform=platform,
                user_text=user_text,
                platform_preamble=platform_preamble,
                max_tokens=send_repair_max_tokens,
            )
            if repaired_text and not _is_tool_candidate(repaired_text):
                return _finish(
                    text=repaired_text,
                    status="done",
                    checker_action_value="FINAL_ANSWER",
                    checker_reason_value=checker_reason,
                    planner_kind_value="send_message_fix",
                    planned_tool_override=planned_tool,
                    attempted_tool_override=str((planned_tool or {}).get("function") or ""),
                )
            fallback_text = await _generate_recovery_text(
                llm_client=llm_client,
                platform=platform,
                user_text=user_text,
                error_kind="send_message_misfire",
                reason=send_reason or "send_message_not_requested",
                fallback="How can I help?",
                platform_preamble=platform_preamble,
                max_tokens=recovery_max_tokens,
            )
            return _finish(
                text=fallback_text,
                status="done",
                checker_action_value="FINAL_ANSWER",
                checker_reason_value=checker_reason,
                planner_kind_value="send_message_fix",
                planned_tool_override=planned_tool,
                attempted_tool_override=str((planned_tool or {}).get("function") or ""),
            )

        tool_used = True
        tool_started = time.perf_counter()
        doer_exec = await _execute_tool_call(
            llm_client=llm_client,
            tool_call=planned_tool,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
            context=context,
            user_text=resolved_user_text,
            origin=origin_payload,
            scope=scope,
            wait_callback=wait_callback,
            admin_guard=admin_guard,
        )
        tool_ms_total += (time.perf_counter() - tool_started) * 1000.0
        raw_payload = doer_exec.get("payload")
        raw_tool_payload_out = raw_payload if isinstance(raw_payload, dict) else None
        tool_result_for_checker = doer_exec.get("checker_result")
        normalized_checker_result_out = (
            tool_result_for_checker if isinstance(tool_result_for_checker, dict) else None
        )
        if isinstance(tool_result_for_checker, dict) and not bool(tool_result_for_checker.get("ok")):
            tool_failures_count += 1
        draft_response = str((tool_result_for_checker or {}).get("summary_for_user") or "").strip()
        artifacts = ((tool_result_for_checker or {}).get("artifacts") or [])
        if isinstance(artifacts, list):
            for item in artifacts:
                if not isinstance(item, dict):
                    continue
                artifacts_out.append(item)
                if len(artifacts_out) >= 12:
                    break
        tool_calls_used += 1

        agent_state = await _run_doer_state_update(
            llm_client=llm_client,
            platform=platform,
            user_request=resolved_user_text,
            prior_state=agent_state,
            tool_call=planned_tool,
            tool_result=tool_result_for_checker,
            max_tokens=doer_max_tokens,
        )
        _save_persistent_agent_state(
            redis_client=r,
            platform=platform,
            scope=scope,
            state=agent_state,
        )

        checker_started = time.perf_counter()
        checker_decision = await _run_checker(
            llm_client=llm_client,
            platform=platform,
            current_user_text=user_text,
            resolved_user_text=resolved_user_text,
            agent_state=agent_state,
            planned_tool=planned_tool,
            tool_result=tool_result_for_checker,
            draft_response=draft_response,
            retry_allowed=_retry_allowed_within_limits(),
            platform_preamble=platform_preamble,
            max_tokens=checker_max_tokens,
        )
        checker_ms_total += (time.perf_counter() - checker_started) * 1000.0
        checker_action = str(checker_decision.get("kind") or "FINAL_ANSWER")

        if checker_action == "FINAL_ANSWER":
            checker_reason = _tool_failure_checker_reason(tool_result_for_checker) or "complete"
            return _finish(
                text=str(checker_decision.get("text") or draft_response or DEFAULT_CLARIFICATION).strip(),
                status="done",
                checker_action_value="FINAL_ANSWER",
                checker_reason_value=checker_reason,
                retry_tool=queued_retry_tool_for_ledger,
            )

        if checker_action == "NEED_USER_INFO":
            checker_reason = "needs_user_input"
            return _finish(
                text=str(checker_decision.get("text") or DEFAULT_CLARIFICATION).strip(),
                status="blocked",
                checker_action_value="NEED_USER_INFO",
                checker_reason_value=checker_reason,
                retry_tool=queued_retry_tool_for_ledger,
            )

        if checker_action == "RETRY_TOOL":
            retry_text = str(checker_decision.get("text") or "").strip()
            if not _retry_allowed_within_limits():
                queued_retry_tool_for_ledger = parse_function_json(retry_text)
                planner_text_is_tool_candidate = True
                checker_reason = "budget_exhausted"
                break
            retry_eval = await _validate_or_recover_tool_call(
                llm_client=llm_client,
                text=retry_text,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                tool_index=tool_index,
                user_text=user_text,
                origin=origin_payload,
                scope=scope,
                history_messages=history,
                context=context if isinstance(context, dict) else {},
                platform_preamble=platform_preamble,
                repair_max_tokens=tool_repair_max_tokens,
                recovery_max_tokens=recovery_max_tokens,
                send_repair_max_tokens=send_repair_max_tokens,
            )
            retry_validation = (
                retry_eval.get("validation_status")
                if isinstance(retry_eval.get("validation_status"), dict)
                else {"status": "failed", "reason": str(retry_eval.get("reason") or "invalid_tool_call")}
            )
            attempted_tool_for_ledger = str(retry_eval.get("attempted_tool") or attempted_tool_for_ledger or "")
            if bool(retry_eval.get("repair_used")):
                repairs_used_count += 1
            if not bool(retry_eval.get("ok")):
                reason = str(retry_eval.get("reason") or "invalid_tool_call")
                failed_retry_tool = retry_eval.get("tool_call")
                if not isinstance(failed_retry_tool, dict):
                    failed_retry_tool = {"function": "invalid_tool_call", "arguments": {}}
                if reason == "send_message_misfire":
                    checker_reason = f"send_message_misfire:{str(retry_eval.get('send_reason') or 'unknown')}"
                    return _finish(
                        text=str(retry_eval.get("recovery_text_if_blocked") or "How can I help?").strip(),
                        status="done",
                        checker_action_value="FINAL_ANSWER",
                        checker_reason_value=checker_reason,
                        planned_tool_override=failed_retry_tool,
                        validation_status_override=retry_validation,
                        planner_kind_value="send_message_fix",
                        attempted_tool_override=str(retry_eval.get("attempted_tool") or ""),
                    )
                validation_failures_count += 1
                checker_reason = f"validation_failed:{reason}"
                return _finish(
                    text=str(retry_eval.get("recovery_text_if_blocked") or DEFAULT_CLARIFICATION).strip(),
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    planned_tool_override=failed_retry_tool,
                    validation_status_override=retry_validation,
                    attempted_tool_override=str(retry_eval.get("attempted_tool") or ""),
                )
            queued = retry_eval.get("tool_call")
            if not isinstance(queued, dict):
                validation_failures_count += 1
                checker_reason = "validation_failed:invalid_tool_call"
                return _finish(
                    text=DEFAULT_CLARIFICATION,
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    planned_tool_override={"function": "invalid_tool_call", "arguments": {}},
                    validation_status_override=retry_validation,
                )
            queued_tool_call = queued
            queued_retry_tool_for_ledger = queued
            validation_status = retry_validation
            critic_continue_count += 1
            checker_reason = "continue"
            planner_text_is_tool_candidate = True
            continue

    pending_question = _state_first_open_question(agent_state)
    if pending_question:
        checker_reason = "needs_user_input"
        return _finish(
            text=pending_question,
            status="blocked",
            checker_action_value="NEED_USER_INFO",
            checker_reason_value=checker_reason,
            retry_tool=queued_retry_tool_for_ledger,
        )

    best_effort = _state_best_effort_answer(
        state=agent_state,
        draft_response=draft_response,
        tool_result=tool_result_for_checker,
    )
    checker_started = time.perf_counter()
    checker_decision = await _run_checker(
        llm_client=llm_client,
        platform=platform,
        current_user_text=user_text,
        resolved_user_text=resolved_user_text,
        agent_state=agent_state,
        planned_tool=planned_tool,
        tool_result=tool_result_for_checker,
        draft_response=best_effort,
        retry_allowed=_retry_allowed_within_limits(),
        platform_preamble=platform_preamble,
        max_tokens=checker_max_tokens,
    )
    checker_ms_total += (time.perf_counter() - checker_started) * 1000.0
    checker_action = str(checker_decision.get("kind") or "FINAL_ANSWER")

    if checker_action == "NEED_USER_INFO":
        checker_reason = "needs_user_input"
        return _finish(
            text=str(checker_decision.get("text") or pending_question or DEFAULT_CLARIFICATION).strip(),
            status="blocked",
            checker_action_value="NEED_USER_INFO",
            checker_reason_value=checker_reason,
            retry_tool=queued_retry_tool_for_ledger,
        )

    if checker_action == "RETRY_TOOL":
        retry_tool = parse_function_json(str(checker_decision.get("text") or ""))
        if isinstance(retry_tool, dict):
            queued_retry_tool_for_ledger = retry_tool
            planner_text_is_tool_candidate = True
        checker_reason = "budget_exhausted"
        return _finish(
            text=best_effort or "Completed.",
            status="done",
            checker_action_value="FINAL_ANSWER",
            checker_reason_value=checker_reason,
            retry_tool=queued_retry_tool_for_ledger,
        )

    checker_reason = _tool_failure_checker_reason(tool_result_for_checker) or checker_reason or "complete"
    return _finish(
        text=str(checker_decision.get("text") or best_effort or DEFAULT_CLARIFICATION).strip(),
        status="done",
        checker_action_value="FINAL_ANSWER",
        checker_reason_value=checker_reason,
        retry_tool=queued_retry_tool_for_ledger,
    )
