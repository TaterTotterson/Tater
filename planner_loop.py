import json
import time
import uuid
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from helpers import (
    parse_function_json,
    looks_like_tool_markup,
    TOOL_MARKUP_REPAIR_PROMPT,
    TOOL_MARKUP_FAILURE_TEXT,
)
from plugin_kernel import normalize_platform, plugin_display_name, plugin_supports_platform, expand_plugin_platforms
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

AGENT_CREATION_REPAIR_PROMPT = (
    "If the user asks to create a plugin, platform, server, API endpoint, website, or tool, "
    "you must use Agent Lab kernel tools to create the files under agent_lab/. "
    "Do not respond with manual steps or code blocks alone. "
    "Use create_plugin/create_platform (not write_file for plugins/platforms). "
    "Agent Lab platforms require a PLATFORM dict and a run(stop_event) function. "
    "Agent Lab plugins must subclass ToolPlugin and expose a module-level `plugin` instance (not a dict). "
    "Always include full file content via code_lines (preferred) or code/code_b64. "
    "When using code_lines, keep `usage` as a single-line JSON string to avoid quoting errors "
    "(example: usage = '{\"function\":\"my_plugin\",\"arguments\":{}}'). "
    "Each code_lines entry must be a single line (no embedded \\n). Use single-quoted strings to avoid escaping. "
    "Do NOT split list/dict literals across multiple code_lines entries. "
    "If you call llm_client.chat, keep the messages list on ONE line: "
    "messages=[{\"role\":\"system\",\"content\":\"...\"},{\"role\":\"user\",\"content\":\"...\"}]. "
    "For Agent Lab plugins, set `platforms` to include the current platform, and implement the matching handler "
    "(e.g., handle_webui for webui, handle_discord for discord). "
    "Always include `when_to_use` and a waiting_prompt_template for Agent Lab plugins. "
    "waiting_prompt_template must be an instruction to the LLM and should include wording like "
    "'Write ...' and 'Only output that message.'"
)
AGENT_CREATION_FAILURE_TEXT = "Sorry, I couldn't generate the required tool calls. Please try again."
AGENT_UNKNOWN_TOOL_REPAIR_PROMPT = (
    "The tool you tried does not exist. Call list_plugins to see available tools, "
    "then choose a valid tool and call it with proper arguments."
)
AGENT_UNKNOWN_TOOL_FAILURE_TEXT = "I don't have that tool available. Please rephrase or choose another tool."
CREATION_MAX_REPROMPTS = 4

TOOL_NAME_ALIASES = {
    "web_search": "search_web",
    "google_search": "search_web",
    "google_cse_search": "search_web",
}

PLUGIN_REQUIREMENTS_HINT = (
    "Plugin must subclass ToolPlugin imported from plugin_base and assign an instance to module-level `plugin` (not a dict). "
    "Required attributes: name, plugin_name, version, description, platforms, usage (string). "
    "platforms must be a list of supported platform ids: webui, discord, irc, homeassistant, "
    "homekit, matrix, telegram, xbmc, automation, rss (or 'both'). "
    "Include when_to_use and waiting_prompt_template (required for Agent Lab plugins). "
    "Keep usage as a single-line JSON string, e.g., usage = '{\"function\":\"my_plugin\",\"arguments\":{}}'. "
    "Each code_lines entry must be a single line (no embedded \\n). "
    "If you call llm_client.chat, put the messages list on ONE line with a comma between dicts."
)


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


def _agent_system_instructions(max_rounds: int, max_tool_calls: int) -> str:
    return (
        "Agent Mode is ON. You may take multiple steps to complete the user's goal.\n"
        "Rules:\n"
        "- Decide the next step each round: tool call, question, or finish.\n"
        "- Use list_plugins to discover tools; use get_plugin_help if arguments are unclear.\n"
        "- If the user asks to schedule or run a recurring task (daily/weekly/every), use the `ai_tasks` plugin; do not create a platform or tool.\n"
        "- For scheduled tasks, assume local timezone if none is provided. If no destination is given, use the current channel/room from origin (do not ask for channel IDs).\n"
        "- If the user asks to run a plugin by name (even approximate), call list_plugins and pick the closest match (ignore minor typos/plurals). If a close match exists, use it and do not claim it’s unavailable; optionally confirm.\n"
        "- When calling a plugin, use its id from list_plugins (not the display name).\n"
        "- Examples that require list_plugins: weather/forecast, news, stocks, sports scores, downloads, music/song generation, image/video generation, camera feeds/snapshots (front/back yard, porch, driveway, garage), camera/sensor status, smart-home actions.\n"
        "- The user does not need to explicitly request tool use; if a tool is appropriate, use it.\n"
        "- Prefer using a tool over attempting to answer from scratch when a tool could fulfill the request.\n"
        "- Only call tools compatible with this platform.\n"
        "- For any create/generate request (content, media, files, or other artifacts), always call list_plugins before responding.\n"
        "- Do not provide a creative or alternative response until you have verified no compatible tool exists.\n"
        "- Before saying you cannot do something in this environment, call list_plugins to verify tool availability.\n"
        "- Before creating Agent Lab plugins/platforms, read the authoring skills via read_file:\n"
        "  skills/agent_lab/plugin_authoring.md and skills/agent_lab/platform_authoring.md.\n"
        "- If a tool returns needs[], stop and ask exactly those questions.\n"
        "- If an action is destructive/high-impact, ask for explicit confirmation first.\n"
        f"- Budget: max rounds={max_rounds}, max tool calls={max_tool_calls}.\n"
        "- Tool calls must be JSON only. Final replies should be plain text.\n"
        "Kernel tools available: read_file, search_web, search_files, write_file, list_directory, delete_file, read_url, download_file, "
        "list_archive, extract_archive, list_stable_plugins, list_stable_platforms, inspect_plugin, "
        "create_plugin, validate_plugin, "
        "create_platform, validate_platform, write_workspace_note, list_workspace.\n"
        "For external/current facts, API docs, or library references, call search_web first and then read_url if deeper content is needed.\n"
        "For broad current-events questions (for example: 'what's going on in the world'), call search_web before answering.\n"
        "read_file supports plain text and structured extraction for .pdf, .docx, .xlsx/.xlsm, .csv/.tsv, and .pptx files.\n"
        "For large files, call read_file with `start` and `max_chars` to page through content.\n"
        "Archive helpers support zip/tar plus 7z/rar when dependencies are installed.\n"
        "For kernel file paths, `/agent_lab/...` maps to the Agent Lab root, and shortcuts like "
        "`documents/...`, `downloads/...`, `workspace/...`, `artifacts/...`, and `logs/...` "
        "resolve under `agent_lab/`.\n"
        "When writing files or code, prefer `content_lines`/`code_lines` arrays (one string per line) to avoid JSON escaping issues; "
        "base64 fields (`content_b64`/`code_b64`) are also supported.\n"
        "For create_plugin/create_platform, do NOT use manifest/code_files. Provide `name` plus `code_lines` (preferred) "
        "or `code`/`code_b64`. Avoid triple-quoted docstrings or unescaped double quotes inside code_lines; "
        "use single quotes or comments instead.\n"
        "If the user explicitly asks for AI-generated content (e.g., AI-generated jokes, stories, summaries), "
        "the plugin must call llm_client at runtime to generate output. Do NOT hardcode a static list.\n"
        "File writes are restricted to agent_lab/; stable code is read-only.\n"
        "Use read_url for small text downloads. Use download_file to save files under agent_lab/downloads.\n"
        "You cannot start/stop platforms yourself; after creating one, instruct the user to enable/start it from the Agent Lab tab.\n"
        "Do not refuse by claiming you can't create plugins or servers here; create the Agent Lab code and explain how to activate it.\n"
    )


def _task_context_message(state: Dict[str, Any]) -> str:
    payload = {
        "goal": state.get("goal"),
        "progress_summary": state.get("progress_summary"),
        "facts": state.get("facts"),
    }
    return "Task context (read-only):\n" + json.dumps(payload, ensure_ascii=False)


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

    creation_analysis = _creation_request_analysis(user_text or "")
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
    elif creation_analysis.get("mode") == "ask":
        question = _creation_confirmation_prompt(creation_intent)
        state["pending_creation_confirmation"] = True
        state["pending_creation_intent"] = creation_intent
        state["pending_creation_source_text"] = user_text or ""
        state["pending_creation_question"] = question
        save_task_state(state, r=r)
        return {"text": question, "status": "blocked", "task_id": task_id, "artifacts": []}

    messages = list(history_messages or [])
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

    artifacts_out: List[Dict[str, Any]] = []
    rounds_used = int(state.get("rounds_used") or 0)
    tool_calls_used = int(state.get("tool_calls_used") or 0)
    attempts: List[str] = list(state.get("attempts") or [])
    format_fix_used = False
    missing_args_fix_used = 0
    creation_fix_used = 0
    unknown_tool_fix_used = False
    unknown_tool_search_fallback_used = False
    created_snapshot = _creation_state(state)
    creation_followup_issued = bool(state.get("creation_followup_issued"))
    creation_precheck_done = bool(state.get("creation_precheck_done"))

    if needs_creation and not state.get("skills_loaded"):
        skill_paths = []
        if creation_intent.get("need_plugin"):
            skill_paths.append("skills/agent_lab/plugin_authoring.md")
        if creation_intent.get("need_platform"):
            skill_paths.append("skills/agent_lab/platform_authoring.md")
        for spath in skill_paths:
            meta_payload = run_meta_tool(
                func="read_file",
                args={"path": spath},
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
            )
            messages.append(
                {
                    "role": "assistant",
                    "content": json.dumps({"function": "read_file", "arguments": {"path": spath}}, ensure_ascii=False),
                }
            )
            messages.append(
                {
                    "role": "tool",
                    "content": json.dumps({"tool": "read_file", "result": meta_payload}, ensure_ascii=False),
                }
            )
        state["skills_loaded"] = True
        save_task_state(state, r=r)

    if needs_creation and not state.get("creation_guidance_loaded"):
        messages.append({"role": "system", "content": AGENT_CREATION_REPAIR_PROMPT})
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
                        messages.append({"role": "system", "content": AGENT_CREATION_REPAIR_PROMPT})
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

        if not func:
            break

        if not is_meta_tool(func) and func not in registry:
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
            if not unknown_tool_search_fallback_used and _should_try_search_fallback(user_text or "", func, needs_creation):
                fallback_query = str(
                    (args or {}).get("query")
                    or (args or {}).get("q")
                    or (args or {}).get("topic")
                    or (user_text or "")
                ).strip()
                if not fallback_query:
                    fallback_query = user_text or "latest world news"
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
                        "Before creating new Agent Lab code, call list_plugins first. "
                        "If an existing tool can satisfy the request, use that tool instead of create_plugin/create_platform."
                    ),
                })
                state["creation_precheck_prompted"] = True
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

        if is_meta_tool(func):
            meta_payload = run_meta_tool(
                func=func,
                args=args,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
            )
            if isinstance(meta_payload, dict):
                ok = bool(meta_payload.get("ok"))
                if func == "list_plugins":
                    creation_precheck_done = True
                    state["creation_precheck_done"] = True
                if ok:
                    if func == "create_plugin":
                        name = meta_payload.get("name") or args.get("name")
                        if name:
                            _record_created(state, "plugins", str(name))
                            _update_progress_summary(state, f"Created Agent Lab plugin {name}.")
                            validation = run_meta_tool(
                                func="validate_plugin",
                                args={"name": str(name), "auto_install": True},
                                platform=platform,
                                registry=registry,
                                enabled_predicate=enabled_predicate,
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
                            validation = run_meta_tool(
                                func="validate_platform",
                                args={"name": str(name), "auto_install": True},
                                platform=platform,
                                registry=registry,
                                enabled_predicate=enabled_predicate,
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
            save_task_state(state, r=r)
            continue

        # Attach origin if provided (merge to preserve channel/room context)
        if origin:
            args = dict(args or {})
            existing = args.get("origin")
            if not isinstance(existing, dict) or not existing:
                args["origin"] = origin
            else:
                merged = dict(origin)
                for k, v in existing.items():
                    if v not in (None, ""):
                        merged[k] = v
                args["origin"] = merged

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

        if wait_callback:
            try:
                await wait_callback(func, plugin)
            except Exception:
                pass

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
