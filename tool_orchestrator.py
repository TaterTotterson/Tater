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

    platform_rule = (
        f"Current platform: {platform}.\n"
        "Only use plugins compatible with this platform.\n"
        "If a user asks for an unsupported capability, explain it is unavailable here and list platforms where it works.\n"
    )

    tool_rules = (
        "Tool use policy:\n"
        "- Answer directly when no external action or live lookup is needed.\n"
        "- Tools are discovered on-demand; not all tools are described here. If unsure, call list_plugins.\n"
        "- The user does not need to explicitly request tool use; if a tool is appropriate, use it.\n"
        "- Prefer using a tool over attempting to answer from scratch when a tool could fulfill the request.\n"
        "- If a tool may be needed, call `list_plugins` first.\n"
        "- If the user asks to control devices or services or interact with external systems, call list_plugins first.\n"
        "- If the user asks about a specific tool/plugin by name or asks what a tool can do, call `list_plugins` or `get_plugin_help` instead of guessing.\n"
        "- If you might need a tool or are unsure a capability exists, call list_plugins before saying it is unavailable.\n"
        "- Before saying you cannot do something in this environment, call list_plugins to verify availability.\n"
        "- For any create/generate request (content, media, files, or other artifacts), call list_plugins before answering.\n"
        "- Do not provide a creative or alternative response until you have verified no compatible tool exists.\n"
        "- If the user asks for multiple independent actions, you may call tools one at a time until all actions are complete, then respond.\n"
        "- After choosing a tool, call `get_plugin_help` if args are unclear.\n"
        "- Ask concise follow-up questions when required args are missing.\n"
        "- Return tool calls strictly as JSON: {\"function\":\"name\",\"arguments\":{...}}\n"
        "- Meta tools: list_plugins, get_plugin_help, list_platforms_for_plugin.\n"
        "- Do not claim success unless tool result confirms success.\n"
    )

    plain_text_rule = ""
    if platform in {"irc", "homeassistant", "homekit", "xbmc"}:
        plain_text_rule = "Output plain ASCII text only (no markdown or emoji).\n"

    return (
        f"Current Date and Time is: {now}\n\n"
        f"You are {first} {last}, an AI assistant.\n\n"
        f"{persona_clause}"
        f"{platform_rule}\n"
        f"{tool_rules}\n"
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

        func = str(parsed.get("function") or "").strip()
        args = parsed.get("arguments", {}) or {}
        if not func:
            return {"text": text, "tool_calls": tool_calls, "last_tool_result": last_tool_result}

        tool_calls.append({"function": func, "arguments": args})

        if is_meta_tool(func):
            meta_payload = run_meta_tool(
                func=func,
                args=args,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
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
