import json
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from helpers import (
    get_tater_name,
    get_tater_personality,
    parse_function_json,
    looks_like_tool_markup,
    TOOL_MARKUP_REPAIR_PROMPT,
    TOOL_MARKUP_FAILURE_TEXT,
)
from plugin_result import narrate_result, redis_truth_payload
from tool_runtime import META_TOOLS, execute_plugin_call, is_meta_tool, run_meta_tool
from plugin_kernel import plugin_supports_platform, plugin_when_to_use

TOOL_NAME_ALIASES = {
    "web_search": "search_web",
    "google_search": "search_web",
    "google_cse_search": "search_web",
    "inspect_page": "inspect_webpage",
    "inspect_website": "inspect_webpage",
    "describe_image": "image_describe",
    "describe_latest_image": "image_describe",
}

_KERNEL_TOOL_PRIORITY = [
    "search_web",
    "inspect_webpage",
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
    "download_file": "download files from URLs",
    "list_archive": "inspect archive entries",
    "extract_archive": "extract archives to a target directory",
    "list_stable_plugins": "list stable built-in plugins",
    "list_stable_platforms": "list stable built-in platforms",
    "inspect_plugin": "inspect plugin metadata and methods",
    "validate_plugin": "validate a plugin file",
    "test_plugin": "run plugin test harness",
    "validate_platform": "validate a platform file",
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
}


def _canonical_tool_name(name: str) -> str:
    key = str(name or "").strip()
    if not key:
        return ""
    return TOOL_NAME_ALIASES.get(key, key)


def _looks_like_invalid_tool_call_text(text: str) -> bool:
    s = str(text or "").strip()
    if not s:
        return False
    lower = s.lower()
    if "\"function\"" in lower and ("\"arguments\"" in lower or "'arguments'" in lower):
        return True
    if s.startswith("{") and ("function" in lower or "tool" in lower):
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


def _enabled_tool_index(
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> str:
    kernel_rows: List[str] = []
    for tool_id in _ordered_kernel_tool_ids():
        kernel_rows.append(f"- {tool_id} — {_kernel_tool_purpose(tool_id)}")
    if not kernel_rows:
        kernel_rows.append("- (none)")

    enabled_check = enabled_predicate or (lambda _name: True)
    plugin_rows: List[str] = []
    for plugin_id, plugin in sorted(registry.items(), key=lambda kv: str(kv[0]).lower()):
        if not enabled_check(plugin_id):
            continue
        if not plugin_supports_platform(plugin, platform):
            continue
        plugin_rows.append(f"- {plugin_id} — {_tool_purpose(plugin)}")
    if not plugin_rows:
        plugin_rows.append("- (none)")

    return (
        "Kernel tools (prefer first for generic tasks):\n"
        + "\n".join(kernel_rows)
        + "\nEnabled plugin tools on this platform:\n"
        + "\n".join(plugin_rows)
    )


def _upsert_tool_index_message(
    messages: List[Dict[str, Any]],
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> None:
    prefixes = (
        "Enabled tools on this platform:",
        "Kernel tools (prefer first for generic tasks):",
    )
    for i in range(len(messages) - 1, -1, -1):
        m = messages[i]
        if m.get("role") != "system":
            continue
        if str(m.get("content") or "").startswith(prefixes):
            messages.pop(i)
    messages.append(
        {
            "role": "system",
            "content": _enabled_tool_index(
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
            ),
        }
    )


def build_compact_system_prompt(platform: str, extra_instructions: str = "") -> str:
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    first, last = get_tater_name()
    personality = (get_tater_personality() or "").strip()

    persona_clause = ""
    if personality:
        persona_clause = (
            f"Voice style: {personality}. This affects tone only. "
            "Never violate tool-use or safety rules.\n\n"
        )

    core_rules = (
        f"Current platform: {platform}.\n"
        "Use only tool ids from the tool index message.\n"
        "Prefer kernel tools first for generic tasks (web/file/download/search/memory/workspace).\n"
        "Use plugin tools for platform/service actions (devices and service APIs).\n"
        "If both can solve the request, choose the kernel tool.\n"
        "If a tool matches user intent, call it directly.\n"
        "If args are unclear, call get_plugin_help(plugin_id) once.\n"
        "File tools are rooted at workspace '/'; use /downloads and /documents for normal files.\n"
        "Tool call format must be strict JSON only:\n"
        "{\"function\":\"tool_id\",\"arguments\":{...}}\n"
        "No markdown fences or extra commentary around tool calls.\n"
        "Do not claim success unless tool output confirms success.\n"
    )

    plain_text_rule = ""
    if platform in {"irc", "homeassistant", "homekit", "xbmc"}:
        plain_text_rule = "Output plain ASCII text only (no markdown or emoji).\n"

    return (
        f"Current Date and Time is: {now}\n\n"
        f"You are {first} {last}, an AI assistant.\n\n"
        f"{persona_clause}"
        f"{core_rules}\n"
        f"{plain_text_rule}"
        f"{extra_instructions}".strip()
    )


def _tool_call_message(func: str, args: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "role": "assistant",
        "content": json.dumps({"function": func, "arguments": args}, ensure_ascii=False),
    }


def _tool_result_message(func: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "role": "tool",
        "content": json.dumps({"tool": func, "result": payload}, ensure_ascii=False),
    }


async def run_tool_loop(
    *,
    llm_client: Any,
    platform: str,
    history_messages: List[Dict[str, Any]],
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]] = None,
    tool_context: Optional[Dict[str, Any]] = None,
    extra_system_instructions: str = "",
    max_steps: int = 6,
) -> Dict[str, Any]:
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": build_compact_system_prompt(platform, extra_system_instructions)}
    ]
    messages.extend(history_messages)

    tool_context = tool_context or {}
    last_tool_result: Optional[Dict[str, Any]] = None
    tool_calls: List[Dict[str, Any]] = []
    format_fix_used = False

    for _ in range(max_steps):
        _upsert_tool_index_message(
            messages,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
        )
        response = await llm_client.chat(messages)
        text = (response.get("message", {}) or {}).get("content", "").strip()
        if not text:
            return {"text": "Sorry, I couldn't generate a response.", "tool_calls": tool_calls}

        parsed = parse_function_json(text)
        if not parsed:
            if looks_like_tool_markup(text):
                if not format_fix_used:
                    messages.append({"role": "system", "content": TOOL_MARKUP_REPAIR_PROMPT})
                    format_fix_used = True
                    continue
                return {
                    "text": TOOL_MARKUP_FAILURE_TEXT,
                    "tool_calls": tool_calls,
                    "last_tool_result": last_tool_result,
                }
            if _looks_like_invalid_tool_call_text(text):
                if not format_fix_used:
                    messages.append(
                        {
                            "role": "system",
                            "content": (
                                "Invalid tool-call JSON detected. Return only strict JSON: "
                                "{\"function\":\"tool_id\",\"arguments\":{...}}."
                            ),
                        }
                    )
                    format_fix_used = True
                    continue
                return {
                    "text": "I had trouble formatting a tool call. Please try again.",
                    "tool_calls": tool_calls,
                    "last_tool_result": last_tool_result,
                }
            return {
                "text": text,
                "tool_calls": tool_calls,
                "last_tool_result": last_tool_result,
            }

        func = _canonical_tool_name(str(parsed.get("function") or "").strip())
        args = parsed.get("arguments", {}) or {}
        if not func:
            return {"text": text, "tool_calls": tool_calls, "last_tool_result": last_tool_result}

        tool_calls.append({"function": func, "arguments": args})

        if is_meta_tool(func):
            origin = tool_context.get("origin") if isinstance(tool_context.get("origin"), dict) else None
            meta_payload = run_meta_tool(
                func=func,
                args=args,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                origin=origin,
            )
            messages.append(_tool_call_message(func, args))
            messages.append(_tool_result_message(func, meta_payload))
            last_tool_result = {"function": func, "result": meta_payload}
            continue

        plugin_exec = await execute_plugin_call(
            func=func,
            args=args,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
            llm_client=llm_client,
            context=tool_context,
        )
        result_payload = plugin_exec["result"]
        last_tool_result = {"function": func, "result": result_payload}
        final_text = await narrate_result(result_payload, llm_client=llm_client, platform=platform)
        return {
            "text": final_text,
            "tool_calls": tool_calls,
            "plugin_call": {
                "function": func,
                "arguments": args,
                "result": result_payload,
                "raw": plugin_exec.get("raw"),
            },
            "truth": redis_truth_payload(result_payload),
        }

    return {
        "text": "I need a bit more information before I can continue.",
        "tool_calls": tool_calls,
        "last_tool_result": last_tool_result,
    }
