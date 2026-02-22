import json
import re
from typing import Any, Callable, Dict, List, Optional


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def parse_strict_tool_json(response_text: str) -> Optional[Dict[str, Any]]:
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


def validate_tool_call_dict(
    *,
    parsed: Any,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    canonical_tool_name_fn: Callable[[Any], str],
    is_meta_tool_fn: Callable[[str], bool],
    plugin_supports_platform_fn: Callable[[Any, str], bool],
    meta_tool_args_reason_fn: Callable[[str, Dict[str, Any]], str],
) -> Dict[str, Any]:
    if not isinstance(parsed, dict):
        return {"ok": False, "reason": "not_object"}

    raw_func = parsed.get("function")
    if not isinstance(raw_func, str) or not raw_func.strip():
        return {"ok": False, "reason": "missing_function"}
    func = canonical_tool_name_fn(raw_func)

    raw_args = parsed.get("arguments", {})
    if raw_args is None:
        raw_args = {}
    if not isinstance(raw_args, dict):
        return {"ok": False, "reason": "arguments_not_object"}
    args = dict(raw_args)

    if is_meta_tool_fn(func):
        meta_reason = meta_tool_args_reason_fn(func, args)
        if meta_reason:
            return {
                "ok": False,
                "reason": meta_reason,
                "tool_call": {"function": func, "arguments": args},
            }
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
        "platform_supported": bool(plugin_supports_platform_fn(plugin, platform)),
    }


async def repair_tool_call_text(
    *,
    llm_client: Any,
    platform: str,
    original_text: str,
    reason: str,
    tool_index: str,
    tool_markup_repair_prompt: str,
    with_platform_preamble_fn: Callable[[List[Dict[str, Any]], str], List[Dict[str, Any]]],
    configured_tool_repair_max_tokens_fn: Callable[[], int],
    coerce_text_fn: Callable[[Any], str] = _coerce_text,
    user_text: str = "",
    tool_name_hint: str = "",
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> str:
    del user_text, tool_name_hint
    prompt = (
        f"{tool_markup_repair_prompt}\n"
        "Repair invalid planner output.\n"
        f"Current platform: {platform}\n"
        "Return only one of:\n"
        "- strict JSON tool call: {\"function\":\"tool_id\",\"arguments\":{...}}\n"
        "- NO_TOOL\n"
        "Rules:\n"
        "- Latest user message is the only execution authorization; history/memory/prior outputs are context only.\n"
        "- Do not continue prior tool actions unless current message explicitly requests execution.\n"
        "- Treat acknowledgement/reaction/chatter as NO_TOOL unless there is an explicit action request.\n"
        "- If intent is ambiguous or conversational-only, return NO_TOOL.\n"
        "- For observational scene questions, prefer camera/snapshot tools over limitation text.\n"
        "- Do not return NO_TOOL if a relevant camera tool is available for that request.\n"
        "- Use only tool ids and argument keys from the enabled tool index.\n"
        "- No markdown."
    )
    user_payload = (
        f"Reason: {reason}\n"
        f"Enabled tool index:\n{tool_index}\n\n"
        + f"Original planner output:\n{original_text}"
    )
    try:
        token_limit = int(max_tokens) if max_tokens is not None else configured_tool_repair_max_tokens_fn()
        response = await llm_client.chat(
            messages=with_platform_preamble_fn(
                [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_payload},
                ],
                platform_preamble,
            ),
            max_tokens=max(1, token_limit),
            temperature=0.1,
        )
        return coerce_text_fn((response.get("message", {}) or {}).get("content", "")).strip()
    except Exception:
        return ""


async def validate_tool_contract(
    *,
    llm_client: Any,
    response_text: str,
    user_text: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    tool_index: str,
    parse_function_json_fn: Callable[[str], Any],
    validate_tool_call_dict_fn: Callable[[Any, str, Dict[str, Any], Optional[Callable[[str], bool]]], Dict[str, Any]],
    repair_tool_call_text_fn: Callable[..., Any],
    platform_preamble: str = "",
    repair_max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    raw_response = _coerce_text(response_text).strip()
    is_single_non_strict_object = False
    if raw_response.startswith("{") and raw_response.endswith("}"):
        try:
            parsed_raw = json.loads(raw_response)
        except Exception:
            parsed_raw = None
        if isinstance(parsed_raw, dict) and set(parsed_raw.keys()) != {"function", "arguments"}:
            is_single_non_strict_object = True

    strict_parsed = parse_strict_tool_json(response_text)
    if strict_parsed is not None:
        base = validate_tool_call_dict_fn(strict_parsed, platform, registry, enabled_predicate)
    else:
        loose_parsed = parse_function_json_fn(response_text)
        if isinstance(loose_parsed, dict):
            loose_valid = validate_tool_call_dict_fn(loose_parsed, platform, registry, enabled_predicate)
            if (
                not is_single_non_strict_object
                and loose_valid.get("ok")
                and loose_valid.get("platform_supported", True)
            ):
                return {
                    **loose_valid,
                    "reason": "non_strict_json_salvaged",
                    "repair_used": True,
                }
            if is_single_non_strict_object:
                base = {
                    "ok": False,
                    "reason": "non_strict_json_object_shape",
                    "tool_call": loose_valid.get("tool_call"),
                    "platform_supported": bool(loose_valid.get("platform_supported", True)),
                }
            else:
                base = {
                    "ok": bool(loose_valid.get("ok")),
                    "reason": str(loose_valid.get("reason") or "non_strict_json"),
                    "tool_call": loose_valid.get("tool_call"),
                    "platform_supported": bool(loose_valid.get("platform_supported", True)),
                }
        else:
            base = {"ok": False, "reason": "invalid_json"}

    if base.get("ok") and base.get("platform_supported", True):
        return {**base, "repair_used": False}

    reason = base.get("reason") or "invalid_tool_call"
    unsupported_only = bool(base.get("ok")) and not bool(base.get("platform_supported", True))
    if unsupported_only:
        reason = "unsupported_platform"
    base_tool = base.get("tool_call") if isinstance(base.get("tool_call"), dict) else {}
    base_tool_name = str((base_tool or {}).get("function") or "").strip()

    repaired_text = await repair_tool_call_text_fn(
        llm_client=llm_client,
        platform=platform,
        original_text=response_text,
        reason=str(reason),
        tool_index=tool_index,
        user_text=user_text or response_text,
        tool_name_hint=base_tool_name,
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

    repaired_candidate_text = str(repaired_text or "").strip()
    repaired_strict = parse_strict_tool_json(repaired_text)
    if repaired_strict is not None:
        repaired = validate_tool_call_dict_fn(repaired_strict, platform, registry, enabled_predicate)
    else:
        repaired_loose = parse_function_json_fn(repaired_text)
        if isinstance(repaired_loose, dict):
            repaired = {
                "ok": False,
                "reason": "non_strict_json",
                "tool_call": validate_tool_call_dict_fn(
                    repaired_loose, platform, registry, enabled_predicate
                ).get("tool_call"),
            }
        else:
            repaired = {
                "ok": False,
                "reason": "repair_returned_answer" if repaired_candidate_text else "invalid_json",
                "tool_call": base.get("tool_call"),
                "assistant_text": repaired_candidate_text,
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
        "assistant_text": repaired.get("assistant_text"),
    }


def looks_like_shell_tool_name(
    value: Any,
    *,
    canonical_tool_name_fn: Callable[[Any], str],
) -> bool:
    func = canonical_tool_name_fn(value)
    if not func:
        return False
    shell_like = {
        "run_shell",
        "shell",
        "terminal",
        "bash",
        "sh",
        "exec",
        "execute_command",
        "command",
    }
    if func in shell_like:
        return True
    return ("shell" in func) or ("terminal" in func)


def workspace_discovery_query(user_text: str, *, stopwords: set[str]) -> str:
    lowered = str(user_text or "").strip().lower()
    if not lowered:
        return "plugin"
    tokens = re.findall(r"[a-z0-9_.-]+", lowered)
    picked: List[str] = []
    for token in tokens:
        if not token or token in stopwords:
            continue
        if token.isdigit():
            continue
        picked.append(token)
        if len(picked) >= 8:
            break
    if picked:
        return " ".join(picked)
    return "plugin"


def redirect_unknown_tool_to_search_files(
    *,
    reason: str,
    user_text: str,
    tool_call: Optional[Dict[str, Any]],
    canonical_tool_name_fn: Callable[[Any], str],
    workspace_discovery_hint_re: Any,
    workspace_query_stopwords: set[str],
) -> Optional[Dict[str, Any]]:
    if str(reason or "").strip().lower() != "unknown_tool":
        return None
    text = str(user_text or "").strip()
    lowered = text.lower()
    call = tool_call if isinstance(tool_call, dict) else {}
    func = canonical_tool_name_fn(call.get("function"))
    shell_like_unknown = looks_like_shell_tool_name(
        func,
        canonical_tool_name_fn=canonical_tool_name_fn,
    )
    if not lowered:
        lowered = func
    if not lowered:
        return None
    if not shell_like_unknown and not workspace_discovery_hint_re.search(lowered):
        return None
    query = workspace_discovery_query(lowered, stopwords=workspace_query_stopwords)
    args: Dict[str, Any] = {
        "query": query,
        "path": ".",
        "max_results": 20,
    }
    if "readme" in lowered:
        args["path"] = "."
        args["file_glob"] = "README*.md"
    return {"function": "search_files", "arguments": args}


def validation_failure_text(reason: str, platform: str) -> str:
    lowered = str(reason or "").strip().lower()
    if lowered == "unsupported_platform":
        return f"That action is not supported on {platform}. What should I do instead?"
    if lowered == "tool_disabled":
        return "That tool is currently disabled. What would you like me to do instead?"
    if lowered == "unknown_tool":
        return "I couldn't find a matching tool for that request. Could you rephrase what action you want?"
    return "I couldn't safely execute a tool for that request. Could you clarify the exact action?"


async def generate_recovery_text(
    *,
    llm_client: Any,
    platform: str,
    user_text: str,
    error_kind: str,
    reason: str,
    fallback: str,
    with_platform_preamble_fn: Callable[[List[Dict[str, Any]], str], List[Dict[str, Any]]],
    configured_recovery_max_tokens_fn: Callable[[], int],
    looks_like_tool_markup_fn: Callable[[str], bool],
    parse_function_json_fn: Callable[[str], Any],
    checker_decision_prefix_re: Any,
    default_clarification: str,
    coerce_text_fn: Callable[[Any], str] = _coerce_text,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> str:
    fallback_text = str(fallback or default_clarification).strip() or str(default_clarification)
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
        token_limit = int(max_tokens) if max_tokens is not None else configured_recovery_max_tokens_fn()
        response = await llm_client.chat(
            messages=with_platform_preamble_fn(
                [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                platform_preamble,
            ),
            max_tokens=max(1, token_limit),
            temperature=0.2,
        )
        out = coerce_text_fn((response.get("message", {}) or {}).get("content", "")).strip()
    except Exception:
        return fallback_text

    if not out:
        return fallback_text
    if looks_like_tool_markup_fn(out) or parse_function_json_fn(out):
        return fallback_text
    match = checker_decision_prefix_re.match(out)
    if match:
        out = str(match.group(2) or "").strip()
    return out or fallback_text
