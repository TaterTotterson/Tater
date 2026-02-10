import json
import time
import uuid
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from helpers import (
    get_tater_name,
    parse_function_json,
    looks_like_tool_markup,
    TOOL_MARKUP_REPAIR_PROMPT,
    TOOL_MARKUP_FAILURE_TEXT,
)
from plugin_kernel import (
    normalize_platform,
    plugin_display_name,
    plugin_supports_platform,
    expand_plugin_platforms,
    plugin_when_to_use,
)
from plugin_result import (
    action_failure,
    narrate_result,
    redis_truth_payload,
    result_for_llm,
    result_needs_questions,
)
from tool_runtime import execute_plugin_call, is_meta_tool, run_meta_tool
from truth_store import save_truth_snapshot
from helpers import redis_client as default_redis


AGENT_MODE_KEY = "tater:agent_mode"
TASK_KEY_PREFIX = "tater:tasks:"
ACTIVE_TASK_PREFIX = "tater:tasks:active:"

DEFAULT_MAX_ROUNDS = 15
DEFAULT_MAX_TOOL_CALLS = 6

AGENT_MODE_TRIGGERS = (
    "agent mode",
    "autopilot",
    "do this step-by-step",
    "do this step by step",
    "set this up fully",
)

CREATION_VERBS = (
    "create",
    "make",
    "build",
    "set up",
    "setup",
    "generate",
    "write",
    "scaffold",
)

CREATION_PLUGIN_KEYWORDS = (
    "plugin",
    "tool",
)

CREATION_PLATFORM_KEYWORDS = (
    "platform",
    "server",
    "endpoint",
    "api",
    "service",
    "website",
)

CREATION_PLUGIN_PHRASES = (
    "create plugin",
    "create a plugin",
    "make plugin",
    "build plugin",
    "new plugin",
    "create tool",
    "build tool",
    "create_plugin",
)

CREATION_PLATFORM_PHRASES = (
    "create platform",
    "create a platform",
    "build platform",
    "new platform",
    "create server",
    "build server",
    "create endpoint",
    "build endpoint",
    "create api",
    "build api",
    "create service",
    "build service",
    "create website",
    "build website",
    "create_platform",
)

CREATION_NEGATIVE_GUARDS = (
    "review",
    "debug",
    "fix",
    "explain",
    "use existing plugin",
    "use existing tools",
    "list plugins",
    "run",
)

CREATION_EXPLICIT_ONLY_KEY = "tater:agent_creation:explicit_only"

HIGH_IMPACT_KEYWORDS = (
    "delete",
    "remove",
    "wipe",
    "erase",
    "format",
    "reset",
    "factory reset",
    "purchase",
    "buy",
    "pay",
    "charge",
    "transfer",
    "send money",
    "shutdown",
    "shut down",
    "reboot",
    "disable",
    "disarm",
    "unlock",
    "open door",
    "mass",
    "promote",
)

AGENT_CREATION_SHARED_REPAIR_PROMPT = (
    "For Agent Lab creation requests, use kernel tools to write files under agent_lab/. "
    "Do not reply with manual steps or code blocks alone. "
    "Use create_plugin/create_platform (not write_file for plugins/platforms). "
    "Always include full file content via code_lines (preferred) or code/code_b64. "
    "Each code_lines entry must be a single line (no embedded \\n). "
    "Do NOT split list/dict literals across multiple code_lines entries. "
    "If you call llm_client.chat in generated code, keep the messages list on ONE line: "
    "messages=[{\"role\":\"system\",\"content\":\"...\"},{\"role\":\"user\",\"content\":\"...\"}]."
)
AGENT_CREATION_PLUGIN_REPAIR_PROMPT = (
    "Plugin-specific rules: subclass ToolPlugin from plugin_base and expose a module-level `plugin` instance (not a dict). "
    "Set `name` to the exact create_plugin id and filename stem (<name>.py), and use `plugin_name` for display. "
    "Set `platforms` to include the current platform and implement matching handle_<platform> methods. "
    "Keep `usage` as single-line JSON with function id equal to plugin name "
    "(example: usage = '{\"function\":\"my_plugin\",\"arguments\":{}}'). "
    "Include `when_to_use` and `waiting_prompt_template`; waiting_prompt_template must instruct output "
    "(for example: 'Write ...' and 'Only output that message.')."
)
AGENT_CREATION_PLATFORM_REPAIR_PROMPT = (
    "Platform-specific rules: include a module-level PLATFORM dict and run(stop_event=None). "
    "Keep the run loop cooperative: check stop_event regularly and avoid long blocking operations."
)
AGENT_CREATION_FAILURE_TEXT = "Sorry, I couldn't generate the required tool calls. Please try again."
AGENT_UNKNOWN_TOOL_REPAIR_PROMPT = (
    "The tool id is invalid for this turn. Choose a valid id from the enabled tool index and "
    "return a strict JSON tool call only."
)
AGENT_UNKNOWN_TOOL_FAILURE_TEXT = "I don't have that tool available. Please rephrase or choose another tool."
CREATION_MAX_REPROMPTS = 4

TOOL_NAME_ALIASES = {
    "web_search": "search_web",
    "google_search": "search_web",
    "google_cse_search": "search_web",
    "describe_image": "vision_describer",
    "describe_latest_image": "vision_describer",
    "vision_describe": "vision_describer",
    "vision_describe_image": "vision_describer",
}

DELIVERY_PLATFORMS = {"discord", "irc", "matrix", "telegram", "homeassistant"}

PLUGIN_REQUIREMENTS_HINT = (
    "Plugin must subclass ToolPlugin imported from plugin_base and assign an instance to module-level `plugin` (not a dict). "
    "Required attributes: name, plugin_name, version, description, platforms, usage (string). "
    "name must exactly match the create_plugin name/id and filename stem (<name>.py); use plugin_name as display text. "
    "platforms must be a list of supported platform ids: webui, discord, irc, homeassistant, "
    "homekit, matrix, telegram, xbmc, automation, rss (or 'both'). "
    "Include when_to_use and waiting_prompt_template (required for Agent Lab plugins). "
    "Keep usage as a single-line JSON string, e.g., usage = '{\"function\":\"my_plugin\",\"arguments\":{}}'. "
    "Each code_lines entry must be a single line (no embedded \\n). "
    "If you call llm_client.chat, put the messages list on ONE line with a comma between dicts."
)

PLUGIN_ADVANCED_REFERENCE_RULES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    (
        "skills/agent_lab/references/plugin_api_auth.md",
        (
            "api",
            "oauth",
            "token",
            "auth",
            "http",
            "rest",
            "graphql",
            "webhook",
            "endpoint",
        ),
    ),
    (
        "skills/agent_lab/references/plugin_ai_generation.md",
        (
            "llm",
            "ai-generated",
            "summarize",
            "summary",
            "rewrite",
            "caption",
            "joke",
            "story",
        ),
    ),
    (
        "skills/agent_lab/references/plugin_artifacts.md",
        (
            "image",
            "audio",
            "video",
            "file",
            "attachment",
            "artifact",
            "screenshot",
            "thumbnail",
        ),
    ),
    (
        "skills/agent_lab/references/plugin_result_contract.md",
        (
            "action_failure",
            "action_success",
            "needs",
            "follow-up question",
            "error code",
            "say_hint",
            "facts",
        ),
    ),
    (
        "skills/agent_lab/references/plugin_http_resilience.md",
        (
            "timeout",
            "retry",
            "backoff",
            "rate limit",
            "429",
            "requests",
            "httpx",
            "network failure",
        ),
    ),
    (
        "skills/agent_lab/references/plugin_settings_and_secrets.md",
        (
            "required_settings",
            "settings",
            "api key",
            "token",
            "secret",
            "password",
            "credentials",
            "auth header",
        ),
    ),
    (
        "skills/agent_lab/references/plugin_multiplatform_handlers.md",
        (
            "multi-platform",
            "multiplatform",
            "cross-platform",
            "handle_webui",
            "handle_discord",
            "handle_telegram",
            "handle_matrix",
            "all platforms",
        ),
    ),
    (
        "skills/agent_lab/references/plugin_notification_delivery.md",
        (
            "notify",
            "notification",
            "send message",
            "send_message",
            "room",
            "channel",
            "target",
            "origin",
        ),
    ),
    (
        "skills/agent_lab/references/plugin_argument_schema.md",
        (
            "argument_schema",
            "schema",
            "required args",
            "optional args",
            "typed arguments",
            "validation schema",
            "json schema",
        ),
    ),
)

PLATFORM_ADVANCED_REFERENCE_RULES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    (
        "skills/agent_lab/references/platform_network_events.md",
        (
            "webhook",
            "socket",
            "websocket",
            "mqtt",
            "tcp",
            "udp",
            "server",
            "endpoint",
            "stream",
            "bridge",
        ),
    ),
    (
        "skills/agent_lab/references/platform_pollers_workers.md",
        (
            "poll",
            "watch",
            "monitor",
            "feed",
            "queue",
            "worker",
            "cron",
            "schedule",
            "interval",
            "retry",
            "backoff",
        ),
    ),
)


def _contains_creation_phrase(text: str, phrase: str) -> bool:
    s = str(text or "").strip().lower()
    p = str(phrase or "").strip().lower()
    if not s or not p:
        return False
    if " " in p or "-" in p or "_" in p:
        return p in s
    return re.search(rf"\b{re.escape(p)}\b", s) is not None


def _creation_advanced_reference_paths(
    *,
    need_plugin: bool,
    need_platform: bool,
    request_text: str,
) -> List[str]:
    text = str(request_text or "").strip().lower()
    if not text:
        return []

    out: List[str] = []
    if need_plugin:
        for path, triggers in PLUGIN_ADVANCED_REFERENCE_RULES:
            if any(_contains_creation_phrase(text, t) for t in triggers):
                out.append(path)
    if need_platform:
        for path, triggers in PLATFORM_ADVANCED_REFERENCE_RULES:
            if any(_contains_creation_phrase(text, t) for t in triggers):
                out.append(path)

    seen = set()
    deduped: List[str] = []
    for item in out:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _creation_repair_prompt_for_intent(intent: Optional[Dict[str, Any]]) -> str:
    info = intent if isinstance(intent, dict) else {}
    need_plugin = bool(info.get("need_plugin"))
    need_platform = bool(info.get("need_platform"))

    parts: List[str] = [AGENT_CREATION_SHARED_REPAIR_PROMPT]
    if need_plugin:
        parts.append(AGENT_CREATION_PLUGIN_REPAIR_PROMPT)
    if need_platform:
        parts.append(AGENT_CREATION_PLATFORM_REPAIR_PROMPT)
    if not need_plugin and not need_platform:
        parts.append(AGENT_CREATION_PLUGIN_REPAIR_PROMPT)
        parts.append(AGENT_CREATION_PLATFORM_REPAIR_PROMPT)
    return "\n".join(parts)


def agent_mode_enabled(r=None) -> bool:
    r = r or default_redis
    raw = r.get(AGENT_MODE_KEY)
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def detect_agent_mode_request(text: str) -> bool:
    s = (text or "").lower()
    return any(trigger in s for trigger in AGENT_MODE_TRIGGERS)


def _active_key(platform: str, scope: str) -> str:
    return f"{ACTIVE_TASK_PREFIX}{normalize_platform(platform)}:{scope}"


def _task_key(task_id: str) -> str:
    return f"{TASK_KEY_PREFIX}{task_id}"


def get_active_task_id(platform: str, scope: str, r=None) -> Optional[str]:
    r = r or default_redis
    key = _active_key(platform, scope)
    task_id = r.get(key)
    return str(task_id).strip() if task_id else None


def set_active_task_id(platform: str, scope: str, task_id: str, r=None) -> None:
    r = r or default_redis
    if not task_id:
        return
    r.set(_active_key(platform, scope), task_id)


def clear_active_task_id(platform: str, scope: str, r=None) -> None:
    r = r or default_redis
    r.delete(_active_key(platform, scope))


def load_task_state(task_id: str, r=None) -> Optional[Dict[str, Any]]:
    r = r or default_redis
    if not task_id:
        return None
    raw = r.get(_task_key(task_id))
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def save_task_state(state: Dict[str, Any], r=None) -> None:
    r = r or default_redis
    if not isinstance(state, dict):
        return
    task_id = state.get("task_id")
    if not task_id:
        return
    state["updated_at"] = time.time()
    r.set(_task_key(task_id), json.dumps(state, ensure_ascii=False))


def should_use_agent_mode(
    *,
    user_text: str,
    platform: str,
    scope: str,
    r=None,
) -> Tuple[bool, Optional[str], str]:
    r = r or default_redis
    active_task_id = get_active_task_id(platform, scope, r=r)
    if active_task_id:
        state = load_task_state(active_task_id, r=r) or {}
        if state.get("status") == "blocked":
            return True, active_task_id, "resume"

    # Agent mode is always on.
    return True, None, "always"


def _clean_args_for_signature(args: Dict[str, Any]) -> Dict[str, Any]:
    def _strip(obj):
        if isinstance(obj, dict):
            cleaned = {}
            for k, v in obj.items():
                key = str(k)
                if key in {"origin", "request_id", "timestamp", "ts", "context"}:
                    continue
                cleaned[key] = _strip(v)
            return cleaned
        if isinstance(obj, list):
            return [_strip(x) for x in obj]
        return obj

    return _strip(args or {})


def _signature_for_attempt(plugin_id: str, args: Dict[str, Any]) -> str:
    base = {"plugin": plugin_id, "args": _clean_args_for_signature(args)}
    return json.dumps(base, sort_keys=True, ensure_ascii=False, default=str)


def _merge_facts(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(existing, dict):
        existing = {}
    if not isinstance(incoming, dict):
        return existing
    merged = dict(existing)
    for k, v in incoming.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _merge_facts(merged.get(k) or {}, v)
        else:
            merged[k] = v
    return merged


def _build_progress_summary(state: Dict[str, Any]) -> str:
    summary = (state.get("progress_summary") or "").strip()
    if summary:
        return summary
    attempts = state.get("attempts") or []
    if attempts:
        try:
            plugins = []
            for item in attempts[-5:]:
                data = json.loads(item) if isinstance(item, str) else {}
                plugin = data.get("plugin")
                if plugin:
                    plugins.append(plugin)
            if plugins:
                return "Tried: " + ", ".join(plugins)
        except Exception:
            pass
    return "Progress updated."


def _update_progress_summary(state: Dict[str, Any], line: str) -> None:
    if not line:
        return
    current = (state.get("progress_summary") or "").strip()
    if not current:
        state["progress_summary"] = line.strip()
        return
    joined = f"{current}\n{line.strip()}"
    # keep it reasonably short
    if len(joined) > 1200:
        joined = joined[-1200:]
    state["progress_summary"] = joined


def _render_needs(needs: List[str]) -> str:
    if not isinstance(needs, list):
        return ""
    lines = [str(n).strip() for n in needs if str(n).strip()]
    return "\n".join(lines).strip()


def _is_empty_arg(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _missing_required_args(plugin: Any, args: Dict[str, Any]) -> List[str]:
    required = getattr(plugin, "required_args", None) or []
    missing: List[str] = []
    if not isinstance(required, list):
        return missing
    for name in required:
        key = str(name)
        if key not in args or _is_empty_arg(args.get(key)):
            missing.append(key)
    return missing


def _needs_for_missing_args(plugin: Any, missing: List[str]) -> List[str]:
    prompts = getattr(plugin, "missing_info_prompts", None)
    if isinstance(prompts, list) and any(str(p).strip() for p in prompts):
        return [str(p).strip() for p in prompts if str(p).strip()]

    common = getattr(plugin, "common_needs", None)
    if isinstance(common, list) and any(str(p).strip() for p in common):
        return [str(p).strip() for p in common if str(p).strip()]

    if not missing:
        return []
    if len(missing) == 1:
        return [f"Please provide `{missing[0]}`."]
    return [f"Please provide: {', '.join(missing)}."]


def _short_tool_purpose(plugin: Any) -> str:
    text = str(plugin_when_to_use(plugin) or "").strip()
    if not text:
        text = str(getattr(plugin, "description", "") or "").strip()
    if not text:
        return "no description"
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > 80:
        text = text[:77].rstrip() + "..."
    return text


def _enabled_tool_mini_index(
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> str:
    enabled_check = enabled_predicate or (lambda _name: True)
    rows: List[str] = []
    for plugin_id, plugin in sorted(registry.items(), key=lambda kv: str(kv[0]).lower()):
        if not enabled_check(plugin_id):
            continue
        if not plugin_supports_platform(plugin, platform):
            continue
        rows.append(f"- {plugin_id} — {_short_tool_purpose(plugin)}")
    if not rows:
        rows.append("- (none)")
    return (
        "Enabled tools on this platform:\n"
        + "\n".join(rows)
        + "\nMeta tools:\n"
        + "- get_plugin_help(plugin_id)\n"
        + "- vision_describer(prompt?, path?|url?|blob_key?|file_id?)"
    )


def _upsert_tool_index_message(
    messages: List[Dict[str, Any]],
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> None:
    prefix = "Enabled tools on this platform:"
    for i in range(len(messages) - 1, -1, -1):
        m = messages[i]
        if m.get("role") != "system":
            continue
        if str(m.get("content") or "").startswith(prefix):
            messages.pop(i)
    messages.append(
        {
            "role": "system",
            "content": _enabled_tool_mini_index(
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
            ),
        }
    )


def _agent_system_instructions(max_rounds: int, max_tool_calls: int) -> str:
    return (
        "Agent mode is ON.\n"
        f"Budget: rounds={max_rounds}, tool_calls={max_tool_calls}.\n"
        "Use only tool ids listed in the enabled tool index.\n"
        "If a tool matches intent, call it directly.\n"
        "If args are unclear, call get_plugin_help(plugin_id) once.\n"
        "Tool calls must be strict single-line JSON: {\"function\":\"tool_id\",\"arguments\":{...}}\n"
        "No markdown fences or extra text around tool calls.\n"
        "If a tool returns needs[], ask exactly those questions.\n"
        "Do not claim success unless the tool result confirms success.\n"
        "Ask confirmation before destructive/high-impact actions.\n"
        "Final user replies must be plain text.\n"
    )


def _task_context_message(state: Dict[str, Any]) -> str:
    payload = {
        "goal": state.get("goal"),
        "progress_summary": state.get("progress_summary"),
        "facts": state.get("facts"),
    }
    return "Task context (read-only):\n" + json.dumps(payload, ensure_ascii=False)


def _compact_planner_history(
    history_messages: List[Dict[str, Any]],
    *,
    platform: str,
) -> List[Dict[str, Any]]:
    first, last = get_tater_name()
    out: List[Dict[str, Any]] = [
        {
            "role": "system",
            "content": f"You are {first} {last}, an AI assistant.\nCurrent platform: {platform}.",
        }
    ]
    for msg in list(history_messages or []):
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip().lower()
        content = msg.get("content")
        if role not in {"system", "user", "assistant", "tool"}:
            continue
        if role == "system":
            text = str(content or "").strip()
            if not text:
                continue
            # Keep only lightweight contextual system notes.
            if text.startswith("The user's name is "):
                out.append({"role": "system", "content": text})
            continue
        out.append({"role": role, "content": content})
    return out


def _looks_high_impact(plugin_id: str, args: Dict[str, Any]) -> bool:
    blob = f"{plugin_id} {json.dumps(args or {}, ensure_ascii=False)}".lower()
    return any(k in blob for k in HIGH_IMPACT_KEYWORDS)


def _has_phrase(text: str, phrase: str) -> bool:
    needle = str(phrase or "").strip().lower()
    if not needle:
        return False
    if " " in needle or "_" in needle:
        return needle in text
    return re.search(rf"\b{re.escape(needle)}\b", text) is not None


def _canonical_tool_name(name: str) -> str:
    key = str(name or "").strip()
    if not key:
        return ""
    return TOOL_NAME_ALIASES.get(key, key)


def _looks_like_send_intent(text: str) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return False
    if "send" in s and "message" in s:
        return True
    return "notify" in s


def _infer_destination_platform(text: str) -> str:
    s = str(text or "").strip().lower()
    if not s:
        return ""
    if re.search(r"\bhome\s*assistant\b|\bhomeassistant\b", s):
        return "homeassistant"
    if re.search(r"\bdiscord\b", s):
        return "discord"
    if re.search(r"\bmatrix\b", s):
        return "matrix"
    if re.search(r"\btelegram\b", s):
        return "telegram"
    if re.search(r"\birc\b", s):
        return "irc"
    return ""


def _looks_like_platform_followup(text: str) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return False
    if "which platform" in s or "what platform" in s:
        return True
    if "platform to use" in s or "specify" in s and "platform" in s:
        return True
    if "need to know" in s and "platform" in s:
        return True
    return False


def _flatten_text_values(value: Any) -> List[str]:
    out: List[str] = []
    if isinstance(value, str):
        text = value.strip()
        if text:
            out.append(text)
        return out
    if isinstance(value, dict):
        for item in value.values():
            out.extend(_flatten_text_values(item))
        return out
    if isinstance(value, (list, tuple, set)):
        for item in value:
            out.extend(_flatten_text_values(item))
        return out
    return out


def _infer_destination_platform_from_args(args: Dict[str, Any]) -> str:
    data = dict(args or {})
    platform = normalize_platform(data.get("platform"))
    if platform in DELIVERY_PLATFORMS:
        return platform

    targets = data.get("targets")
    if isinstance(targets, dict):
        if targets.get("room_id") or targets.get("room_alias"):
            return "matrix"
        if targets.get("chat_id"):
            return "telegram"
        if targets.get("device_service") is not None or targets.get("persistent") is not None:
            return "homeassistant"

    for text in _flatten_text_values(data):
        hint = _infer_destination_platform(text)
        if hint:
            return hint
    return ""


def _extract_target_ref(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return ""

    for pattern in (r"![^\s,]+", r"#[A-Za-z0-9][A-Za-z0-9._:-]*", r"@[A-Za-z0-9_]+"):
        match = re.search(pattern, s)
        if match:
            return match.group(0)

    match = re.search(
        r"\b(?:room|channel|chat)\s+([^\n,]+?)(?:\s+(?:in|on|via)\s+\w+|\s+(?:saying|say)\b|$)",
        s,
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    ref = str(match.group(1) or "").strip().strip("\"'“”")
    return ref.strip(" .")


def _extract_message_text(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return ""

    for pattern in (
        r"\bsaying\s+[\"'“”]?(.+?)[\"'”]?\s*$",
        r"\bsay\s+[\"'“”]?(.+?)[\"'”]?\s*$",
        r"\bmessage\s+[\"'“”]?(.+?)[\"'”]?\s*$",
    ):
        match = re.search(pattern, s, flags=re.IGNORECASE)
        if match:
            content = str(match.group(1) or "").strip().strip("\"'“”")
            if content:
                return content

    quoted = re.search(r"[\"'“”]([^\"'“”]{1,500})[\"'“”]", s)
    if quoted:
        return str(quoted.group(1) or "").strip()
    return ""


def _inject_platform_into_request(request: str, platform: str) -> str:
    base = str(request or "").strip()
    target_platform = str(platform or "").strip().lower()
    if not base or target_platform not in DELIVERY_PLATFORMS:
        return base
    if _infer_destination_platform(base) == target_platform:
        return base

    split_match = re.search(r"\s+(saying|say)\b", base, flags=re.IGNORECASE)
    if split_match:
        idx = split_match.start()
        return f"{base[:idx]} in {target_platform}{base[idx:]}"
    return f"{base} in {target_platform}"


def _autofill_delivery_args(
    func: str,
    args: Dict[str, Any],
    *,
    user_text: str,
    origin: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if func not in {"send_message", "ai_tasks"}:
        return dict(args or {})

    out = dict(args or {})
    platform = normalize_platform(out.get("platform"))
    if platform not in DELIVERY_PLATFORMS:
        hint = _infer_destination_platform_from_args(out)
        if not hint:
            hint = _infer_destination_platform(user_text)
        if not hint and isinstance(origin, dict):
            origin_platform = normalize_platform(origin.get("platform"))
            if origin_platform in DELIVERY_PLATFORMS:
                hint = origin_platform
        if hint:
            out["platform"] = hint
            platform = hint

    if func == "send_message" and not out.get("targets") and platform:
        target_ref = _extract_target_ref(user_text)
        if target_ref:
            if platform == "matrix":
                out["targets"] = {"room_id": target_ref}
            elif platform == "telegram":
                out["targets"] = {"chat_id": target_ref}
            elif platform == "homeassistant":
                out["targets"] = {"device_service": target_ref}
            else:
                out["targets"] = {"channel": target_ref}

    if func == "send_message" and not out.get("message"):
        message = _extract_message_text(user_text)
        if message:
            out["message"] = message

    return out


def _force_send_message_call(user_text: str) -> Optional[Dict[str, Any]]:
    if not _looks_like_send_intent(user_text):
        return None

    platform = _infer_destination_platform(user_text)
    if platform not in DELIVERY_PLATFORMS:
        return None

    args: Dict[str, Any] = {"platform": platform}
    target_ref = _extract_target_ref(user_text)
    if target_ref:
        if platform == "matrix":
            args["targets"] = {"room_id": target_ref}
        elif platform == "telegram":
            args["targets"] = {"chat_id": target_ref}
        elif platform == "homeassistant":
            args["targets"] = {"device_service": target_ref}
        else:
            args["targets"] = {"channel": target_ref}

    message = _extract_message_text(user_text)
    if not message:
        return None
    args["message"] = message

    return {"function": "send_message", "arguments": args}


def _platform_reply_token(text: str) -> str:
    raw = str(text or "").strip().lower()
    if not raw:
        return ""
    raw = raw.strip(" .!?")
    raw = re.sub(r"^(?:on|in|via)\s+", "", raw)
    raw = re.sub(r"\s+please$", "", raw).strip()
    raw = re.sub(r"\s+", " ", raw)

    if raw in {"discord", "matrix", "telegram", "irc"}:
        return raw
    if raw in {"home assistant", "homeassistant"}:
        return "homeassistant"
    return ""


def _history_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, dict):
        for key in ("text", "message", "content", "summary"):
            val = content.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return ""


def _is_short_followup_value(text: str) -> bool:
    s = str(text or "").strip()
    if not s:
        return False
    if len(s) > 80:
        return False
    lower = s.lower().strip(" .!?")
    if lower in {"yes", "y", "no", "n", "ok", "okay", "sure", "cancel", "stop"}:
        return False
    if re.search(r"[.!?]\s+[A-Za-z]", s):
        return False
    words = re.findall(r"\b[\w@#:-]+\b", s)
    if len(words) > 8:
        return False
    if re.match(r"^(send|set|turn|run|create|build|list|show|tell)\b", lower):
        return False
    return True


def _looks_like_clarification_prompt(text: str) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return False
    if "?" in s:
        return True
    cues = (
        "need more info",
        "need a bit more",
        "please provide",
        "could you provide",
        "which ",
        "what ",
        "where ",
        "when ",
        "specify",
        "missing",
        "clarify",
    )
    return any(cue in s for cue in cues)


def _resolve_generic_followup_user_text(
    history_messages: List[Dict[str, Any]],
    user_text: str,
) -> Tuple[str, bool]:
    if not _is_short_followup_value(user_text):
        return str(user_text or ""), False
    if not isinstance(history_messages, list) or not history_messages:
        return str(user_text or ""), False

    current = str(user_text or "").strip()
    saw_current_user = False
    prior_user = ""
    prior_assistant = ""

    for msg in reversed(history_messages):
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip().lower()
        content = _history_text(msg.get("content"))
        if not content:
            continue

        if role == "assistant" and not prior_assistant:
            prior_assistant = content
            continue

        if role == "user":
            if not saw_current_user and content == current:
                saw_current_user = True
                continue
            prior_user = content
            break

    if not prior_user:
        return str(user_text or ""), False
    if prior_assistant and not _looks_like_clarification_prompt(prior_assistant):
        return str(user_text or ""), False
    if prior_user.strip().lower() == current.lower():
        return str(user_text or ""), False

    rebuilt = f"{prior_user}\n\nAdditional detail from user: {current}"
    return rebuilt, True


def _resolve_delivery_followup_user_text(
    history_messages: List[Dict[str, Any]],
    user_text: str,
) -> Tuple[str, bool]:
    platform = _platform_reply_token(user_text)
    if platform not in DELIVERY_PLATFORMS:
        return str(user_text or ""), False
    if not isinstance(history_messages, list) or not history_messages:
        return str(user_text or ""), False

    current = str(user_text or "").strip()
    saw_current_user = False
    prior_user = ""
    prior_assistant = ""

    for msg in reversed(history_messages):
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip().lower()
        content = _history_text(msg.get("content"))
        if not content:
            continue

        if role == "assistant" and not prior_assistant:
            prior_assistant = content
            continue

        if role == "user":
            if not saw_current_user and content == current:
                saw_current_user = True
                continue
            prior_user = content
            break

    if not prior_user or not _looks_like_send_intent(prior_user):
        return str(user_text or ""), False

    if prior_assistant and not _looks_like_platform_followup(prior_assistant):
        return str(user_text or ""), False

    inferred = _infer_destination_platform(prior_user)
    if inferred == platform:
        return prior_user, True

    if inferred in DELIVERY_PLATFORMS and inferred != platform:
        return _inject_platform_into_request(prior_user, platform), True

    return _inject_platform_into_request(prior_user, platform), True


def _should_try_search_fallback(user_text: str, func: str, needs_creation: bool) -> bool:
    if needs_creation:
        return False
    text = str(user_text or "").strip().lower()
    if not text:
        return False

    action_markers = (
        "turn on",
        "turn off",
        "set ",
        "start ",
        "stop ",
        "run ",
        "open ",
        "close ",
        "unlock ",
        "lock ",
    )
    if any(marker in text for marker in action_markers):
        return False

    info_markers = (
        "latest",
        "news",
        "what's going on",
        "whats going on",
        "happening",
        "update",
        "current",
        "world",
        "what ",
        "when ",
        "where ",
        "who ",
        "why ",
        "how ",
        "tell me",
        "explain",
        "summarize",
    )
    if any(marker in text for marker in info_markers):
        return True

    f = str(func or "").strip().lower()
    if any(marker in f for marker in ("news", "search", "lookup", "browse")):
        return True
    return False


def _int_or(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _search_web_should_retry(
    payload: Dict[str, Any],
    *,
    retry_count: int,
) -> bool:
    if retry_count >= 1:
        return False
    if not isinstance(payload, dict) or not payload.get("ok"):
        return False

    count = _int_or(payload.get("count"), 0)
    results = payload.get("results")
    if not isinstance(results, list):
        results = []
    has_more = bool(payload.get("has_more"))

    snippet_chars = 0
    nonempty_snippets = 0
    for item in results:
        if not isinstance(item, dict):
            continue
        snippet = str(item.get("snippet") or "").strip()
        if snippet:
            nonempty_snippets += 1
            snippet_chars += len(snippet)

    avg_snippet_len = (snippet_chars / nonempty_snippets) if nonempty_snippets else 0.0
    thin_evidence = count < 2 or (count < 3 and avg_snippet_len < 80)
    if thin_evidence:
        return True
    if has_more and count < 4:
        return True
    return False


def _search_web_retry_args(args: Dict[str, Any], payload: Dict[str, Any], user_text: str) -> Optional[Dict[str, Any]]:
    if not isinstance(args, dict):
        args = {}
    if not isinstance(payload, dict):
        return None

    retry = dict(args)
    query = str(retry.get("query") or user_text or "").strip()
    if not query:
        return None

    next_start = _int_or(payload.get("next_start"), 0)
    has_more = bool(payload.get("has_more")) and next_start > 0
    requested = _int_or(retry.get("num_results") or retry.get("max_results"), 5)
    requested = max(1, min(requested, 10))

    if has_more:
        retry["query"] = query
        retry["start"] = next_start
        retry["num_results"] = requested
        return retry

    # Broaden once when first page was too thin.
    broadened = query
    lowered = query.lower()
    if "latest" not in lowered:
        broadened += " latest"
    if "overview" not in lowered:
        broadened += " overview"
    retry["query"] = broadened.strip()
    retry["start"] = 1
    retry["num_results"] = min(10, max(requested + 2, 5))
    retry.pop("site", None)
    retry.pop("domain", None)
    retry.pop("siteSearch", None)
    return retry


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    sval = str(value).strip().lower()
    if not sval:
        return default
    if sval in {"1", "true", "yes", "on"}:
        return True
    if sval in {"0", "false", "no", "off"}:
        return False
    return default


def _creation_explicit_only(r=None) -> bool:
    env = os.getenv("TATER_CREATION_EXPLICIT_ONLY")
    if env is not None and str(env).strip() != "":
        return _as_bool(env, default=True)
    r = r or default_redis
    raw = None
    try:
        raw = r.get(CREATION_EXPLICIT_ONLY_KEY)
    except Exception:
        raw = None
    return _as_bool(raw, default=True)


def _creation_request_analysis(text: str) -> Dict[str, Any]:
    s = (text or "").lower()
    if not s:
        return {
            "mode": "none",
            "confidence": 0.0,
            "explicit": False,
            "need_plugin": False,
            "need_platform": False,
            "scores": {"plugin": 0, "platform": 0},
            "guards": [],
        }

    s = re.sub(r"\s+", " ", s).strip()
    has_agent_lab_context = "agent lab" in s or "agent mode" in s
    has_verbs = any(v in s for v in CREATION_VERBS)
    plugin_keyword_hit = any(k in s for k in CREATION_PLUGIN_KEYWORDS)
    platform_keyword_hit = any(k in s for k in CREATION_PLATFORM_KEYWORDS)
    plugin_phrase_hit = any(p in s for p in CREATION_PLUGIN_PHRASES) or bool(
        re.search(
            r"\b(?:create|make|build|set\s+up|setup|generate|write|scaffold)\b(?:\W+\w+){0,3}\W+\b(?:plugin|tool)\b",
            s,
        )
    )
    platform_phrase_hit = any(p in s for p in CREATION_PLATFORM_PHRASES) or bool(
        re.search(
            r"\b(?:create|make|build|set\s+up|setup|generate|write|scaffold)\b(?:\W+\w+){0,3}\W+\b(?:platform|server|endpoint|api|service|website)\b",
            s,
        )
    )

    plugin_score = 0
    platform_score = 0

    if plugin_keyword_hit:
        plugin_score += 1
    if platform_keyword_hit:
        platform_score += 1
    if has_verbs and plugin_keyword_hit:
        plugin_score += 1
    if has_verbs and platform_keyword_hit:
        platform_score += 1
    if plugin_phrase_hit:
        plugin_score += 3
    if platform_phrase_hit:
        platform_score += 3
    if has_agent_lab_context:
        if plugin_keyword_hit or plugin_phrase_hit:
            plugin_score += 2
        if platform_keyword_hit or platform_phrase_hit:
            platform_score += 2

    guards = [g for g in CREATION_NEGATIVE_GUARDS if _has_phrase(s, g)]
    if guards:
        plugin_score = max(0, plugin_score - 2)
        platform_score = max(0, platform_score - 2)

    explicit_request = bool(plugin_phrase_hit or platform_phrase_hit)

    need_plugin = plugin_phrase_hit or (plugin_keyword_hit and has_verbs)
    need_platform = platform_phrase_hit or (platform_keyword_hit and has_verbs)
    if not need_plugin and plugin_score >= 4 and plugin_score > platform_score:
        need_plugin = True
    if not need_platform and platform_score >= 4 and platform_score > plugin_score:
        need_platform = True

    best_score = max(plugin_score, platform_score)
    if best_score >= 5 and not guards and (need_plugin or need_platform):
        mode = "create"
        confidence = 0.85
    elif best_score >= 3 and (plugin_keyword_hit or platform_keyword_hit or plugin_phrase_hit or platform_phrase_hit):
        mode = "ask"
        confidence = 0.60 if not guards else 0.45
    else:
        mode = "none"
        confidence = 0.20

    if mode in {"ask", "create"} and not (need_plugin or need_platform):
        if plugin_score > platform_score:
            need_plugin = True
        elif platform_score > plugin_score:
            need_platform = True

    return {
        "mode": mode,
        "confidence": confidence,
        "explicit": explicit_request,
        "need_plugin": bool(need_plugin),
        "need_platform": bool(need_platform),
        "scores": {"plugin": int(plugin_score), "platform": int(platform_score)},
        "guards": guards,
    }


def _needs_agent_lab_creation(text: str) -> bool:
    return _creation_request_analysis(text).get("mode") == "create"


def _creation_intent(text: str) -> Dict[str, bool]:
    analysis = _creation_request_analysis(text)
    return {
        "need_platform": bool(analysis.get("need_platform")),
        "need_plugin": bool(analysis.get("need_plugin")),
    }


def _creation_confirmation_prompt(intent: Dict[str, bool]) -> str:
    targets: List[str] = []
    if intent.get("need_plugin"):
        targets.append("plugin")
    if intent.get("need_platform"):
        targets.append("platform")
    if not targets:
        targets = ["plugin or platform"]
    target_text = " and ".join(targets)
    return (
        f"I can create a new Agent Lab {target_text}, but I want to confirm first. "
        f"Do you want me to generate a new {target_text} now? (yes/no)"
    )


def _creation_state(state: Dict[str, Any]) -> Dict[str, List[str]]:
    created = state.get("created_items")
    if not isinstance(created, dict):
        created = {}
    plugins = created.get("plugins")
    platforms = created.get("platforms")
    files = created.get("files")
    return {
        "plugins": list(plugins) if isinstance(plugins, list) else [],
        "platforms": list(platforms) if isinstance(platforms, list) else [],
        "files": list(files) if isinstance(files, list) else [],
    }


def _record_created(state: Dict[str, Any], kind: str, value: str) -> None:
    if not value:
        return
    created = _creation_state(state)
    if value not in created.get(kind, []):
        created[kind].append(value)
    state["created_items"] = created


def _creation_summary(state: Dict[str, Any]) -> str:
    created = _creation_state(state)
    plugins = created.get("plugins") or []
    platforms = created.get("platforms") or []
    files = created.get("files") or []
    missing = _missing_creation_parts(state, _creation_intent(state.get("goal") or ""))

    lines: List[str] = []
    if platforms:
        lines.append("Created Agent Lab platform(s): " + ", ".join(platforms) + ".")
    if plugins:
        lines.append("Created Agent Lab plugin(s): " + ", ".join(plugins) + ".")
    if files and not (plugins or platforms):
        lines.append("Wrote file(s): " + ", ".join(files) + ".")
    if missing:
        lines.append("Still missing: " + ", ".join(missing) + ".")
    if plugins or platforms:
        lines.append("Open the Agent Lab tab to validate/start platforms and enable plugins.")
    return "\n".join(lines).strip()


def _has_valid_created(items: List[str]) -> bool:
    for item in items or []:
        if isinstance(item, str) and "(invalid)" not in item:
            return True
    return False


def _log_creation_response(task_id: str, user_text: str, response_text: str) -> None:
    if not response_text:
        return
    try:
        base_dir = os.path.join(os.getcwd(), "agent_lab", "logs")
        os.makedirs(base_dir, exist_ok=True)
        path = os.path.join(base_dir, "agent_creation.log")
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{ts} | task={task_id}\n")
            f.write(f"USER: {user_text}\n")
            f.write("MODEL:\n")
            f.write(response_text.rstrip() + "\n")
            f.write("-" * 60 + "\n")
    except Exception:
        return


async def _force_creation_tool_call(
    *,
    llm_client: Any,
    user_text: str,
    missing_parts: List[str],
) -> Optional[Dict[str, Any]]:
    """
    Last-resort attempt to coerce a valid create_plugin/create_platform tool call.
    Returns parsed tool call dict or None.
    """
    if not llm_client or not user_text:
        return None

    wants_plugin = "plugin" in (missing_parts or [])
    wants_platform = "platform" in (missing_parts or [])
    if not (wants_plugin or wants_platform):
        return None

    tool_name = "create_plugin" if wants_plugin else "create_platform"
    system = (
        "You MUST return ONLY valid JSON with a single tool call.\n"
        f"Tool to call: {tool_name}.\n"
        "Use fields: name, code_lines (preferred) or code/code_b64.\n"
        "Do NOT use manifest/code_files.\n"
        "Avoid triple-quoted docstrings and unescaped double quotes in code_lines.\n"
        "If the user asked for AI-generated content, the plugin must call llm_client at runtime (no static lists).\n"
        "Return ONLY JSON. No extra text."
    )
    user = (
        "User request:\n"
        f"{user_text}\n\n"
        "Pick a short, safe id for name (snake_case)."
    )
    try:
        resp = await llm_client.chat(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
        )
    except Exception:
        return None

    content = (resp.get("message", {}) or {}).get("content", "").strip()
    if not content:
        return None
    parsed = parse_function_json(content)
    if not parsed:
        return None
    func = str(parsed.get("function") or "").strip()
    if func not in {"create_plugin", "create_platform"}:
        return None
    return parsed

def _missing_creation_parts(state: Dict[str, Any], intent: Dict[str, bool]) -> List[str]:
    created = _creation_state(state)
    missing: List[str] = []
    if intent.get("need_platform") and not _has_valid_created(created.get("platforms") or []):
        missing.append("platform")
    if intent.get("need_plugin") and not _has_valid_created(created.get("plugins") or []):
        missing.append("plugin")
    return missing


def _agent_lab_name_from_path(path: str, kind: str) -> Optional[str]:
    if not path:
        return None
    p = str(path).replace("\\", "/")
    marker = f"agent_lab/{kind}/"
    if marker not in p:
        return None
    tail = p.split(marker, 1)[1]
    if tail.endswith(".py"):
        tail = tail[:-3]
    if "/" in tail or not tail:
        return None
    return tail


def _confirm_from_text(text: str) -> Optional[bool]:
    s = (text or "").strip().lower()
    if not s:
        return None
    yes = {"yes", "y", "yep", "sure", "ok", "okay", "confirm", "do it", "proceed"}
    no = {"no", "n", "nope", "stop", "cancel", "don't", "do not"}
    if any(w == s or w in s for w in yes):
        return True
    if any(w == s or w in s for w in no):
        return False
    return None


async def run_planner_loop(
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
    max_rounds: int = DEFAULT_MAX_ROUNDS,
    max_tool_calls: int = DEFAULT_MAX_TOOL_CALLS,
    wait_callback: Optional[Callable[[str, Any], Any]] = None,
    admin_guard: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    redis_client: Any = None,
) -> Dict[str, Any]:
    r = redis_client or default_redis
    platform = normalize_platform(platform)
    scope = str(scope or "default")

    async def _emit_wait(func_name: str, plugin_obj: Any = None) -> None:
        if not wait_callback:
            return
        try:
            await wait_callback(func_name, plugin_obj)
        except Exception:
            pass

    state = None
    if task_id:
        state = load_task_state(task_id, r=r)

    if not state:
        task_id = task_id or str(uuid.uuid4())
        state = {
            "task_id": task_id,
            "platform": platform,
            "goal": user_text,
            "rounds_used": 0,
            "tool_calls_used": 0,
            "facts": {},
            "attempts": [],
            "pending_needs": [],
            "status": "running",
            "progress_summary": "",
            "created_at": time.time(),
        }
        set_active_task_id(platform, scope, task_id, r=r)
        save_task_state(state, r=r)

    # Resume logic
    if state.get("status") == "blocked":
        pending_action = state.get("pending_action")
        if pending_action:
            decision = _confirm_from_text(user_text)
            if decision is False:
                clear_active_task_id(platform, scope, r=r)
                state["status"] = "stopped"
                state["pending_action"] = None
                state["pending_needs"] = []
                _update_progress_summary(state, "User declined the requested action.")
                save_task_state(state, r=r)
                return {"text": "Okay, I won’t proceed.", "status": "stopped", "task_id": task_id, "artifacts": []}
            if decision is None:
                question = _render_needs(state.get("pending_needs") or [])
                if not question:
                    question = "Please confirm whether I should proceed (yes/no)."
                save_task_state(state, r=r)
                return {"text": question, "status": "blocked", "task_id": task_id, "artifacts": []}
            # decision is True -> continue with pending action
            state["pending_action"] = None
            state["pending_needs"] = []
            state["status"] = "running"
            save_task_state(state, r=r)
            forced_call = pending_action
        else:
            # clear pending needs and continue with user's reply
            state["pending_needs"] = []
            state["status"] = "running"
            save_task_state(state, r=r)
            forced_call = None
    else:
        forced_call = None

    effective_user_text = str(user_text or "")
    followup_recovered = False
    generic_followup_recovered = False
    if not forced_call:
        effective_user_text, followup_recovered = _resolve_delivery_followup_user_text(
            history_messages,
            user_text,
        )
        if not followup_recovered:
            effective_user_text, generic_followup_recovered = _resolve_generic_followup_user_text(
                history_messages,
                user_text,
            )

    creation_analysis = _creation_request_analysis(effective_user_text or "")
    explicit_only = _creation_explicit_only(r=r)
    if explicit_only and not state.get("creation_user_confirmed"):
        if creation_analysis.get("mode") == "create" and not creation_analysis.get("explicit"):
            creation_analysis = dict(creation_analysis)
            creation_analysis["mode"] = "ask"
            creation_analysis["confidence"] = 0.55
    creation_intent = {
        "need_plugin": bool(creation_analysis.get("need_plugin")),
        "need_platform": bool(creation_analysis.get("need_platform")),
    }
    needs_creation = creation_analysis.get("mode") == "create"
    creation_user_confirmed = bool(state.get("creation_user_confirmed")) or needs_creation

    if state.get("pending_creation_confirmation"):
        decision = _confirm_from_text(user_text)
        question = str(state.get("pending_creation_question") or "").strip()
        if not question:
            question = _creation_confirmation_prompt(state.get("pending_creation_intent") or creation_intent)
        if decision is None:
            save_task_state(state, r=r)
            return {"text": question, "status": "blocked", "task_id": task_id, "artifacts": []}
        if decision is False:
            clear_active_task_id(platform, scope, r=r)
            state["status"] = "stopped"
            state["pending_creation_confirmation"] = False
            state["pending_creation_intent"] = {}
            state["pending_creation_source_text"] = ""
            state["pending_creation_question"] = ""
            state["creation_user_confirmed"] = False
            _update_progress_summary(state, "User declined Agent Lab code generation.")
            save_task_state(state, r=r)
            return {
                "text": "Okay, I won't generate a new Agent Lab plugin/platform. Tell me what to do with existing tools.",
                "status": "stopped",
                "task_id": task_id,
                "artifacts": [],
            }
        source_text = str(state.get("pending_creation_source_text") or state.get("goal") or user_text or "")
        pending_intent = state.get("pending_creation_intent")
        if not isinstance(pending_intent, dict):
            pending_intent = _creation_intent(source_text)
        creation_analysis = _creation_request_analysis(source_text)
        creation_intent = {
            "need_plugin": bool(pending_intent.get("need_plugin")),
            "need_platform": bool(pending_intent.get("need_platform")),
        }
        needs_creation = True
        creation_user_confirmed = True
        state["pending_creation_confirmation"] = False
        state["pending_creation_intent"] = {}
        state["pending_creation_source_text"] = ""
        state["pending_creation_question"] = ""
        state["creation_user_confirmed"] = True
        state["status"] = "running"
        save_task_state(state, r=r)
        user_text = source_text
        effective_user_text = source_text
    elif creation_analysis.get("mode") == "ask":
        question = _creation_confirmation_prompt(creation_intent)
        state["pending_creation_confirmation"] = True
        state["pending_creation_intent"] = creation_intent
        state["pending_creation_source_text"] = effective_user_text or ""
        state["pending_creation_question"] = question
        save_task_state(state, r=r)
        return {"text": question, "status": "blocked", "task_id": task_id, "artifacts": []}

    messages = _compact_planner_history(history_messages or [], platform=platform)
    agent_msg = {"role": "system", "content": _agent_system_instructions(max_rounds, max_tool_calls)}
    if messages and messages[0].get("role") == "system":
        messages.insert(1, agent_msg)
    else:
        messages.insert(0, agent_msg)

    if state.get("progress_summary") or state.get("facts"):
        context_msg = {"role": "system", "content": _task_context_message(state)}
        if messages and messages[0].get("role") == "system":
            messages.insert(1, context_msg)
        else:
            messages.insert(0, context_msg)

    if user_text:
        last_role = messages[-1].get("role") if messages else None
        last_content = str(messages[-1].get("content") or "").strip() if messages else ""
        if last_role != "user" or last_content != user_text.strip():
            messages.append({"role": "user", "content": user_text})
        explicit_delivery_platform = _infer_destination_platform(effective_user_text)
        if _looks_like_send_intent(effective_user_text) and explicit_delivery_platform in DELIVERY_PLATFORMS:
            messages.append(
                {
                    "role": "system",
                    "content": (
                        f"The active delivery request uses destination platform '{explicit_delivery_platform}'. "
                        "For send_message/ai_tasks, use that platform and do not ask which platform."
                    ),
                }
            )
        if followup_recovered:
            token = _platform_reply_token(user_text)
            messages.append(
                {
                    "role": "system",
                    "content": (
                        f"Interpret the latest user reply '{str(user_text or '').strip()}' as selecting platform "
                        f"'{token}' for the prior delivery request: {effective_user_text}"
                    ),
                }
            )
        elif generic_followup_recovered:
            messages.append(
                {
                    "role": "system",
                    "content": (
                        "Interpret the latest user reply as additional details for the previous request. "
                        "Use those details to complete the same task; do not treat it as a new unrelated request."
                    ),
                }
            )

    artifacts_out: List[Dict[str, Any]] = []
    rounds_used = int(state.get("rounds_used") or 0)
    tool_calls_used = int(state.get("tool_calls_used") or 0)
    attempts: List[str] = list(state.get("attempts") or [])
    format_fix_used = False
    missing_args_fix_used = 0
    creation_fix_used = 0
    unknown_tool_fix_used = False
    unknown_tool_search_fallback_used = False
    search_web_retry_count = 0
    platform_followup_fix_used = 0
    created_snapshot = _creation_state(state)
    creation_followup_issued = bool(state.get("creation_followup_issued"))
    creation_precheck_done = bool(state.get("creation_precheck_done"))

    async def _append_creation_read_file(path: str) -> None:
        await _emit_wait("read_file", None)
        meta_payload = run_meta_tool(
            func="read_file",
            args={"path": path},
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
            origin=origin,
        )
        messages.append(
            {
                "role": "assistant",
                "content": json.dumps({"function": "read_file", "arguments": {"path": path}}, ensure_ascii=False),
            }
        )
        messages.append(
            {
                "role": "tool",
                "content": json.dumps({"tool": "read_file", "result": meta_payload}, ensure_ascii=False),
            }
        )

    if needs_creation:
        base_skill_paths: List[str] = []
        if creation_intent.get("need_plugin"):
            base_skill_paths.append("skills/agent_lab/plugin_authoring.md")
        if creation_intent.get("need_platform"):
            base_skill_paths.append("skills/agent_lab/platform_authoring.md")

        reference_paths = _creation_advanced_reference_paths(
            need_plugin=bool(creation_intent.get("need_plugin")),
            need_platform=bool(creation_intent.get("need_platform")),
            request_text="\n".join(
                [
                    str(effective_user_text or ""),
                    str(user_text or ""),
                    str(state.get("goal") or ""),
                ]
            ),
        )

        desired_paths = list(base_skill_paths) + list(reference_paths)
        loaded_paths = state.get("skills_paths_loaded") or []
        if not isinstance(loaded_paths, list):
            loaded_paths = []
        loaded_set = {str(p).strip() for p in loaded_paths if str(p).strip()}
        to_load = [p for p in desired_paths if p not in loaded_set]

        for spath in to_load:
            await _append_creation_read_file(spath)
            loaded_set.add(spath)

        changed = False
        skills_loaded_now = bool(base_skill_paths) and all(p in loaded_set for p in base_skill_paths)
        if bool(state.get("skills_loaded")) != skills_loaded_now:
            state["skills_loaded"] = skills_loaded_now
            changed = True

        new_paths_sorted = sorted(loaded_set)
        if state.get("skills_paths_loaded") != new_paths_sorted:
            state["skills_paths_loaded"] = new_paths_sorted
            changed = True

        refs_loaded = state.get("skills_refs_loaded") or []
        if not isinstance(refs_loaded, list):
            refs_loaded = []
        refs_set = {str(p).strip() for p in refs_loaded if str(p).strip()}
        for ref_path in reference_paths:
            if ref_path in loaded_set:
                refs_set.add(ref_path)
        new_refs_sorted = sorted(refs_set)
        if state.get("skills_refs_loaded") != new_refs_sorted:
            state["skills_refs_loaded"] = new_refs_sorted
            changed = True

        if changed:
            save_task_state(state, r=r)

    if needs_creation and not state.get("creation_guidance_loaded"):
        messages.append(
            {
                "role": "system",
                "content": _creation_repair_prompt_for_intent(creation_intent),
            }
        )
        state["creation_guidance_loaded"] = True
        save_task_state(state, r=r)

    # No automatic stable-example reads for creation requests.

    while rounds_used < max_rounds:
        rounds_used += 1
        state["rounds_used"] = rounds_used

        if forced_call:
            func = _canonical_tool_name(str(forced_call.get("function") or "").strip())
            args = forced_call.get("arguments", {}) or {}
            forced_call = None
        else:
            _upsert_tool_index_message(
                messages,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
            )
            response = await llm_client.chat(messages)
            text = (response.get("message", {}) or {}).get("content", "").strip()
            if not text:
                break

            parsed = parse_function_json(text)
            if needs_creation:
                parsed_func = str(parsed.get("function") or "").strip() if parsed else ""
                if parsed_func not in {"create_plugin", "create_platform"}:
                    _log_creation_response(task_id or "unknown", user_text or "", text)
            if not parsed:
                explicit_delivery_platform = _infer_destination_platform(effective_user_text)
                if (
                    _looks_like_send_intent(effective_user_text)
                    and explicit_delivery_platform in DELIVERY_PLATFORMS
                    and _looks_like_platform_followup(text)
                ):
                    if platform_followup_fix_used < 1:
                        messages.append(
                            {
                                "role": "system",
                                "content": (
                                    f"The user already provided destination platform '{explicit_delivery_platform}' in the request. "
                                    "Do not ask follow-up questions about platform. "
                                    "Return only a valid JSON tool call to `send_message`."
                                ),
                            }
                        )
                        platform_followup_fix_used += 1
                        continue
                    forced = _force_send_message_call(effective_user_text or "")
                    if forced:
                        forced_call = forced
                        platform_followup_fix_used += 1
                        continue

                created_snapshot = _creation_state(state)
                already_created = bool(
                    created_snapshot.get("plugins")
                    or created_snapshot.get("platforms")
                    or created_snapshot.get("files")
                )
                missing_parts = _missing_creation_parts(state, creation_intent) if needs_creation else []
                if ("create_plugin" in text or "create_platform" in text) and not format_fix_used:
                    messages.append({
                        "role": "system",
                        "content": (
                            "Your tool-call JSON was invalid. Return ONLY valid JSON.\n"
                            "For create_plugin/create_platform, do NOT use manifest/code_files. "
                            "Use name plus code_lines (preferred) or code/code_b64."
                        ),
                    })
                    format_fix_used = True
                    continue
                if looks_like_tool_markup(text):
                    if not format_fix_used:
                        messages.append({"role": "system", "content": TOOL_MARKUP_REPAIR_PROMPT})
                        format_fix_used = True
                        continue
                    text = TOOL_MARKUP_FAILURE_TEXT
                elif needs_creation and missing_parts:
                    if creation_fix_used < CREATION_MAX_REPROMPTS:
                        need_line = " and ".join(missing_parts)
                        messages.append(
                            {
                                "role": "system",
                                "content": _creation_repair_prompt_for_intent(creation_intent),
                            }
                        )
                        messages.append(
                            {
                                "role": "system",
                                "content": (
                                    f"You still need to create the {need_line}. "
                                    "Call create_plugin/create_platform now "
                                    "and include required metadata (ToolPlugin + PLATFORM dict)."
                                ),
                            }
                        )
                        creation_fix_used += 1
                        continue
                    if creation_user_confirmed:
                        forced = await _force_creation_tool_call(
                            llm_client=llm_client,
                            user_text=user_text or "",
                            missing_parts=missing_parts,
                        )
                        if forced:
                            forced_call = forced
                            continue
                    text = _creation_summary(state) or AGENT_CREATION_FAILURE_TEXT
                elif needs_creation and already_created:
                    text = _creation_summary(state) or text
                state["status"] = "done"
                _update_progress_summary(state, "Completed response.")
                save_task_state(state, r=r)
                clear_active_task_id(platform, scope, r=r)
                return {
                    "text": text,
                    "status": "done",
                    "task_id": task_id,
                    "artifacts": artifacts_out,
                }

            func = _canonical_tool_name(str(parsed.get("function") or "").strip())
            args = parsed.get("arguments", {}) or {}

        args = _autofill_delivery_args(
            func,
            args,
            user_text=effective_user_text or "",
            origin=origin,
        )

        if not func:
            break

        if not is_meta_tool(func) and func not in registry:
            forced_delivery = _force_send_message_call(effective_user_text or "")
            if forced_delivery and not unknown_tool_fix_used:
                forced_call = forced_delivery
                unknown_tool_fix_used = True
                continue
            if not unknown_tool_fix_used:
                prompt = AGENT_UNKNOWN_TOOL_REPAIR_PROMPT
                if needs_creation:
                    prompt = (
                        prompt
                        + " If the user wants a plugin/platform/server, use "
                        "create_plugin/create_platform."
                    )
                messages.append({"role": "system", "content": prompt})
                unknown_tool_fix_used = True
                continue
            if not unknown_tool_search_fallback_used and _should_try_search_fallback(effective_user_text or "", func, needs_creation):
                fallback_query = str(
                    (args or {}).get("query")
                    or (args or {}).get("q")
                    or (args or {}).get("topic")
                    or (effective_user_text or "")
                ).strip()
                if not fallback_query:
                    fallback_query = effective_user_text or "latest world news"
                messages.append(
                    {
                        "role": "system",
                        "content": (
                            "Unknown tool detected. Use search_web with the user's request to gather sources, "
                            "then answer based on those results."
                        ),
                    }
                )
                forced_call = {
                    "function": "search_web",
                    "arguments": {"query": fallback_query, "num_results": 5},
                }
                unknown_tool_search_fallback_used = True
                continue
            state["status"] = "done"
            _update_progress_summary(state, f"Unknown tool: {func}")
            save_task_state(state, r=r)
            clear_active_task_id(platform, scope, r=r)
            summary = _creation_summary(state)
            if summary:
                return {
                    "text": summary,
                    "status": "done",
                    "task_id": task_id,
                    "artifacts": artifacts_out,
                }
            return {
                "text": AGENT_UNKNOWN_TOOL_FAILURE_TEXT,
                "status": "done",
                "task_id": task_id,
                "artifacts": artifacts_out,
            }

        if needs_creation and func in {"create_platform", "create_plugin"}:
            if not creation_precheck_done:
                messages.append({
                    "role": "system",
                    "content": (
                        "Before creating new Agent Lab code, check the enabled tool index already provided. "
                        "If an existing tool fits, use it. Otherwise continue with create_plugin/create_platform."
                    ),
                })
                state["creation_precheck_prompted"] = True
                creation_precheck_done = True
                state["creation_precheck_done"] = True
                save_task_state(state, r=r)
                continue
            if func == "create_plugin":
                name_hint = args.get("name") or args.get("plugin_id") or args.get("plugin_name")
            else:
                name_hint = args.get("name") or args.get("platform_name") or args.get("platform_key")
            missing_name = not str(name_hint or "").strip()
            has_code = args.get("code") is not None
            has_code_lines = isinstance(args.get("code_lines"), list) and len(args.get("code_lines")) > 0
            has_code_b64 = bool(args.get("code_b64"))
            if missing_name or not (has_code or has_code_lines or has_code_b64):
                if creation_fix_used < CREATION_MAX_REPROMPTS:
                    prompt = (
                        f"{func} requires a name and full file content. "
                        "Provide name plus code_lines (preferred) or code/code_b64."
                    )
                    messages.append({"role": "system", "content": prompt})
                    creation_fix_used += 1
                    continue
                state["status"] = "done"
                _update_progress_summary(state, f"{func} missing required content.")
                save_task_state(state, r=r)
                clear_active_task_id(platform, scope, r=r)
                return {
                    "text": _creation_summary(state) or AGENT_CREATION_FAILURE_TEXT,
                    "status": "done",
                    "task_id": task_id,
                    "artifacts": artifacts_out,
                }

        if needs_creation and func == "write_file":
            path = str((args or {}).get("path") or "")
            pnorm = path.replace("\\", "/").lstrip("/")
            kind = None
            if pnorm.startswith("agent_lab/platforms/") or "/agent_lab/platforms/" in pnorm:
                kind = "platform"
            elif pnorm.startswith("agent_lab/plugins/") or "/agent_lab/plugins/" in pnorm:
                kind = "plugin"

            if kind:
                if creation_fix_used < 2:
                    messages.append({
                        "role": "system",
                        "content": (
                            f"Do not use write_file for Agent Lab {kind}s. "
                            f"Use create_{kind} so validation runs and required metadata is included."
                        ),
                    })
                    creation_fix_used += 1
                    continue
                summary = _creation_summary(state)
                state["status"] = "done"
                _update_progress_summary(state, f"Failed to create {kind} via proper tool.")
                save_task_state(state, r=r)
                clear_active_task_id(platform, scope, r=r)
                return {
                    "text": summary or AGENT_CREATION_FAILURE_TEXT,
                    "status": "done",
                    "task_id": task_id,
                    "artifacts": artifacts_out,
                }

        # Attach origin for both kernel tools and plugins (preserves room/user context).
        if origin:
            args = dict(args or {})
            existing = args.get("origin")
            base_origin = dict(origin)
            if platform and not str(base_origin.get("platform") or "").strip():
                base_origin["platform"] = platform
            if scope and not str(base_origin.get("scope") or "").strip():
                base_origin["scope"] = scope

            if not isinstance(existing, dict) or not existing:
                args["origin"] = base_origin
            else:
                merged = dict(base_origin)
                for k, v in existing.items():
                    if v not in (None, ""):
                        merged[k] = v
                args["origin"] = merged

        if func == "memory_set" and "request_text" not in args and effective_user_text:
            args = dict(args)
            args["request_text"] = effective_user_text

        if is_meta_tool(func):
            await _emit_wait(func, None)
            meta_payload = run_meta_tool(
                func=func,
                args=args,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
            )
            auto_search_retry_args = None
            if isinstance(meta_payload, dict):
                ok = bool(meta_payload.get("ok"))
                if func == "list_plugins":
                    creation_precheck_done = True
                    state["creation_precheck_done"] = True
                if func == "search_web" and _search_web_should_retry(
                    meta_payload, retry_count=search_web_retry_count
                ):
                    retry_args = _search_web_retry_args(args, meta_payload, effective_user_text or "")
                    if retry_args:
                        auto_search_retry_args = retry_args
                        search_web_retry_count += 1
                if ok:
                    if func == "create_plugin":
                        name = meta_payload.get("name") or args.get("name")
                        if name:
                            _record_created(state, "plugins", str(name))
                            _update_progress_summary(state, f"Created Agent Lab plugin {name}.")
                            await _emit_wait("validate_plugin", None)
                            validation = run_meta_tool(
                                func="validate_plugin",
                                args={"name": str(name), "auto_install": True},
                                platform=platform,
                                registry=registry,
                                enabled_predicate=enabled_predicate,
                                origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
                            )
                            if not (validation or {}).get("ok"):
                                _record_created(state, "plugins", f"{name} (invalid)")
                                detail = validation.get("missing_fields") if isinstance(validation, dict) else None
                                missing = f" Missing fields: {detail}." if detail else ""
                                messages.append({
                                    "role": "system",
                                    "content": (
                                        "Plugin validation failed after creation."
                                        + missing
                                        + " Rewrite it with create_plugin. "
                                        + PLUGIN_REQUIREMENTS_HINT
                                    ),
                                })
                    elif func == "create_platform":
                        name = meta_payload.get("name") or args.get("name")
                        if name:
                            _record_created(state, "platforms", str(name))
                            _update_progress_summary(state, f"Created Agent Lab platform {name}.")
                            await _emit_wait("validate_platform", None)
                            validation = run_meta_tool(
                                func="validate_platform",
                                args={"name": str(name), "auto_install": True},
                                platform=platform,
                                registry=registry,
                                enabled_predicate=enabled_predicate,
                                origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
                            )
                            if not (validation or {}).get("ok"):
                                _record_created(state, "platforms", f"{name} (invalid)")
                                messages.append({
                                    "role": "system",
                                    "content": (
                                        "Platform validation failed after creation. "
                                        "Rewrite it with create_platform and include a PLATFORM dict and run(stop_event)."
                                    ),
                                })
                    elif func == "write_file":
                        path = meta_payload.get("path") or args.get("path")
                        if path:
                            _record_created(state, "files", str(path))
                            _update_progress_summary(state, f"Wrote file {path}.")
                else:
                    err = meta_payload.get("error") or ""
                    missing = meta_payload.get("missing_fields") or []
                    path_hint = meta_payload.get("path")
                    overwrite_required = bool(meta_payload.get("overwrite_required"))
                    error_code = str(meta_payload.get("error_code") or "").strip().lower()
                    if (
                        func in {"create_plugin", "create_platform"}
                        and overwrite_required
                        and error_code == "already_exists"
                    ):
                        overwrite_args = dict(args or {})
                        overwrite_args["overwrite"] = True
                        default_prompt = (
                            f"{func} target already exists. Overwrite it? (yes/no)"
                        )
                        needs = meta_payload.get("needs")
                        if not isinstance(needs, list) or not any(str(x).strip() for x in needs):
                            needs = [default_prompt]
                        state["status"] = "blocked"
                        state["pending_action"] = {"function": func, "arguments": overwrite_args}
                        state["pending_needs"] = [str(x).strip() for x in needs if str(x).strip()]
                        save_task_state(state, r=r)
                        return {
                            "text": _render_needs(state["pending_needs"]),
                            "status": "blocked",
                            "task_id": task_id,
                            "artifacts": artifacts_out,
                        }
                    if func in {"create_platform", "validate_platform"}:
                        name = meta_payload.get("name") or args.get("name")
                        if name and path_hint:
                            _record_created(state, "platforms", f"{name} (invalid)")
                        detail = err or f"Missing fields: {missing}" if missing else "Validation failed."
                        messages.append({
                            "role": "system",
                            "content": "Platform validation failed: "
                            + str(detail)
                            + " Rewrite the platform file using create_platform with a PLATFORM dict and run(stop_event).",
                        })
                    elif func in {"create_plugin", "validate_plugin"}:
                        name = meta_payload.get("name") or args.get("name")
                        if name and path_hint:
                            _record_created(state, "plugins", f"{name} (invalid)")
                        detail = err or f"Missing fields: {missing}" if missing else "Validation failed."
                        messages.append({
                            "role": "system",
                            "content": "Plugin validation failed: "
                            + str(detail)
                            + " Rewrite the plugin file using create_plugin. "
                            + PLUGIN_REQUIREMENTS_HINT,
                        })

            # After creating one piece, encourage finishing the other if needed.
            if needs_creation and not creation_followup_issued:
                created = _creation_state(state)
                if creation_intent.get("need_platform") and not created.get("platforms"):
                    messages.append({
                        "role": "system",
                        "content": "Next create the Agent Lab platform using create_platform.",
                    })
                    creation_followup_issued = True
                    state["creation_followup_issued"] = True
                elif creation_intent.get("need_plugin") and not created.get("plugins"):
                    messages.append({
                        "role": "system",
                        "content": "Next create the Agent Lab plugin using create_plugin.",
                    })
                    creation_followup_issued = True
                    state["creation_followup_issued"] = True
            messages.append(
                {
                    "role": "assistant",
                    "content": json.dumps({"function": func, "arguments": args}, ensure_ascii=False),
                }
            )
            messages.append(
                {
                    "role": "tool",
                    "content": json.dumps({"tool": func, "result": meta_payload}, ensure_ascii=False),
                }
            )
            if auto_search_retry_args:
                messages.append(
                    {
                        "role": "system",
                        "content": (
                            "Search results were thin. Run one more search_web call (next page or broadened query) "
                            "before drafting the final answer."
                        ),
                    }
                )
                forced_call = {"function": "search_web", "arguments": auto_search_retry_args}
                save_task_state(state, r=r)
                continue
            save_task_state(state, r=r)
            continue

        # Loop detection
        signature = _signature_for_attempt(func, args)
        if signature in attempts:
            state["status"] = "stopped"
            _update_progress_summary(state, "Loop detected; repeated the same tool call.")
            save_task_state(state, r=r)
            clear_active_task_id(platform, scope, r=r)
            summary = _build_progress_summary(state)
            return {
                "text": f"Loop detected. {summary} Tell me what to change so I can continue.",
                "status": "stopped",
                "task_id": task_id,
                "artifacts": artifacts_out,
            }
        attempts.append(signature)
        state["attempts"] = attempts

        if _looks_high_impact(func, args):
            state["status"] = "blocked"
            state["pending_action"] = {"function": func, "arguments": args}
            state["pending_needs"] = ["Please confirm you want me to proceed (yes/no)."]
            save_task_state(state, r=r)
            return {
                "text": _render_needs(state["pending_needs"]),
                "status": "blocked",
                "task_id": task_id,
                "artifacts": artifacts_out,
            }

        if admin_guard:
            guard_result = admin_guard(func)
            if guard_result:
                state["status"] = "stopped"
                save_task_state(state, r=r)
                clear_active_task_id(platform, scope, r=r)
                text = await narrate_result(guard_result, llm_client=llm_client, platform=platform)
                return {"text": text, "status": "stopped", "task_id": task_id, "artifacts": artifacts_out}

        plugin = registry.get(func)
        if plugin and not plugin_supports_platform(plugin, platform):
            available_on = expand_plugin_platforms(getattr(plugin, "platforms", []) or [])
            result_payload = action_failure(
                code="unsupported_platform",
                message=f"`{plugin_display_name(plugin)}` is not available on {platform}.",
                available_on=available_on,
                say_hint="Explain that this tool is unavailable on the current platform and list where it works.",
            )
            state["status"] = "stopped"
            save_task_state(state, r=r)
            clear_active_task_id(platform, scope, r=r)
            text = await narrate_result(result_payload, llm_client=llm_client, platform=platform)
            return {"text": text, "status": "stopped", "task_id": task_id, "artifacts": artifacts_out}

        if plugin:
            missing_args = _missing_required_args(plugin, args)
            if missing_args:
                if missing_args_fix_used < 2:
                    messages.append({
                        "role": "system",
                        "content": (
                            f"You called `{func}` but missed required args: {', '.join(missing_args)}. "
                            "Return a corrected tool call JSON with all required args filled from the user's request. "
                            "Do not ask the user; output only the tool call."
                        ),
                    })
                    missing_args_fix_used += 1
                    continue
                needs = _needs_for_missing_args(plugin, missing_args)
                state["status"] = "blocked"
                state["pending_needs"] = needs
                save_task_state(state, r=r)
                return {
                    "text": _render_needs(needs),
                    "status": "blocked",
                    "task_id": task_id,
                    "artifacts": artifacts_out,
                }

        if tool_calls_used >= max_tool_calls:
            break

        await _emit_wait(func, plugin)

        tool_calls_used += 1
        state["tool_calls_used"] = tool_calls_used
        save_task_state(state, r=r)

        exec_result = await execute_plugin_call(
            func=func,
            args=args,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
            llm_client=llm_client,
            context=context,
        )
        result_payload = exec_result.get("result") or {}

        # Save truth snapshot
        try:
            truth = redis_truth_payload(result_payload)
            save_truth_snapshot(
                redis_client=r,
                platform=platform,
                scope=scope,
                plugin_id=func,
                truth=truth,
            )
        except Exception:
            pass

        if isinstance(result_payload, dict):
            if result_payload.get("ok"):
                facts = result_payload.get("facts")
                if isinstance(facts, dict):
                    state["facts"] = _merge_facts(state.get("facts") or {}, facts)
                hint = result_payload.get("say_hint") or ""
                if hint:
                    _update_progress_summary(state, hint.strip())
            else:
                err = (result_payload.get("error") or {}).get("message")
                if err:
                    _update_progress_summary(state, f"{func} failed: {err}")

        artifacts = result_payload.get("artifacts")
        if isinstance(artifacts, list):
            for item in artifacts:
                if isinstance(item, dict):
                    artifacts_out.append(item)

        needs = result_needs_questions(result_payload)
        if needs:
            state["status"] = "blocked"
            state["pending_needs"] = needs
            save_task_state(state, r=r)
            return {
                "text": _render_needs(needs),
                "status": "blocked",
                "task_id": task_id,
                "artifacts": artifacts_out,
            }

        if isinstance(result_payload, dict):
            err = result_payload.get("error") or {}
            if err.get("code") == "unsupported_platform":
                state["status"] = "stopped"
                save_task_state(state, r=r)
                clear_active_task_id(platform, scope, r=r)
                text = await narrate_result(result_payload, llm_client=llm_client, platform=platform)
                return {"text": text, "status": "stopped", "task_id": task_id, "artifacts": artifacts_out}

        messages.append(
            {
                "role": "assistant",
                "content": json.dumps({"function": func, "arguments": args}, ensure_ascii=False),
            }
        )
        messages.append(
            {
                "role": "tool",
                "content": json.dumps({"tool": func, "result": result_for_llm(result_payload)}, ensure_ascii=False),
            }
        )

        save_task_state(state, r=r)

        if tool_calls_used >= max_tool_calls:
            break

    # Budget reached
    state["status"] = "stopped"
    save_task_state(state, r=r)
    clear_active_task_id(platform, scope, r=r)
    summary = _build_progress_summary(state)
    goal = (state.get("goal") or "").strip()
    remain = f" Remaining goal: {goal}." if goal else ""
    return {
        "text": f"I reached my planning limit. {summary}{remain} Tell me what to do next to continue.",
        "status": "stopped",
        "task_id": task_id,
        "artifacts": artifacts_out,
    }
