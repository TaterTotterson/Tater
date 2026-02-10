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
from tool_runtime import execute_plugin_call, is_meta_tool, run_meta_tool
from plugin_kernel import plugin_supports_platform, plugin_when_to_use

TOOL_NAME_ALIASES = {
    "web_search": "search_web",
    "google_search": "search_web",
    "google_cse_search": "search_web",
    "describe_image": "vision_describer",
    "describe_latest_image": "vision_describer",
    "vision_describe": "vision_describer",
    "vision_describe_image": "vision_describer",
}


def _canonical_tool_name(name: str) -> str:
    key = str(name or "").strip()
    if not key:
        return ""
    return TOOL_NAME_ALIASES.get(key, key)


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


def _enabled_tool_index(
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
        rows.append(f"- {plugin_id} — {_tool_purpose(plugin)}")
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
        "Use only tool ids from the enabled tool index message.\n"
        "If a tool matches user intent, call it directly.\n"
        "If args are unclear, call get_plugin_help(plugin_id) once.\n"
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
