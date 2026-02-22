import json
from typing import Any, Awaitable, Callable, Dict, List, Optional


async def build_structured_routed_tool_call(
    *,
    llm_client: Any,
    plugin_id: str,
    plugin_obj: Any,
    slice_text: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    tool_index: str,
    origin: Optional[Dict[str, Any]],
    scope: str,
    history_messages: Optional[List[Dict[str, Any]]],
    context: Optional[Dict[str, Any]],
    platform_preamble: str,
    repair_max_tokens: Optional[int],
    recovery_max_tokens: Optional[int],
    plugin_usage_argument_keys_fn: Callable[[Any], List[str]],
    plugin_required_argument_keys_fn: Callable[[Any], List[str]],
    parse_function_json_fn: Callable[[Any], Any],
    with_platform_preamble_fn: Callable[..., List[Dict[str, Any]]],
    default_tool_repair_max_tokens: int,
    coerce_text_fn: Callable[[Any], str],
    validate_or_recover_tool_call_fn: Callable[..., Awaitable[Dict[str, Any]]],
    canonical_tool_name_fn: Callable[[str], str],
    multi_intent_route_user_text_key: str,
    multi_intent_route_flag_key: str,
) -> Optional[Dict[str, Any]]:
    plugin_name = str(plugin_id or "").strip()
    request_slice = str(slice_text or "").strip()
    if not plugin_name or not request_slice:
        return None

    usage_keys = plugin_usage_argument_keys_fn(plugin_obj)
    required_keys = plugin_required_argument_keys_fn(plugin_obj)
    usage_example = str(getattr(plugin_obj, "usage", "") or "").strip()
    usage_payload = parse_function_json_fn(usage_example)
    usage_arguments = (
        usage_payload.get("arguments")
        if isinstance(usage_payload, dict) and isinstance(usage_payload.get("arguments"), dict)
        else {}
    )

    prompt_payload = {
        "plugin_id": plugin_name,
        "user_slice": request_slice,
        "allowed_argument_keys": usage_keys,
        "required_argument_keys": required_keys,
        "usage_arguments_example": usage_arguments,
        "rules": [
            "Output exactly one strict JSON object with keys function and arguments.",
            f"function must be exactly '{plugin_name}'.",
            "Only include argument keys declared for this plugin.",
            "Use only information from user_slice.",
        ],
    }
    prompt_messages = with_platform_preamble_fn(
        [
            {
                "role": "system",
                "content": (
                    "Generate exactly one strict JSON tool call.\n"
                    "No markdown, no extra text.\n"
                    "Output format: {\"function\":\"tool_id\",\"arguments\":{...}}"
                ),
            },
            {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False)},
        ],
        platform_preamble=platform_preamble,
    )
    try:
        response = await llm_client.chat(
            messages=prompt_messages,
            max_tokens=max(1, int(repair_max_tokens or default_tool_repair_max_tokens)),
            temperature=0.1,
        )
        candidate_text = coerce_text_fn((response.get("message", {}) or {}).get("content", "")).strip()
    except Exception:
        return None

    if not candidate_text:
        return None

    validated = await validate_or_recover_tool_call_fn(
        llm_client=llm_client,
        text=candidate_text,
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        tool_index=tool_index,
        user_text=request_slice,
        origin=origin,
        scope=scope,
        history_messages=history_messages,
        context=context,
        platform_preamble=platform_preamble,
        repair_max_tokens=repair_max_tokens,
        recovery_max_tokens=recovery_max_tokens,
    )
    if not bool(validated.get("ok")):
        return None
    tool_call = validated.get("tool_call") if isinstance(validated.get("tool_call"), dict) else None
    if not isinstance(tool_call, dict):
        return None
    if canonical_tool_name_fn(tool_call.get("function")) != canonical_tool_name_fn(plugin_name):
        return None
    routed_call = dict(tool_call)
    routed_call[multi_intent_route_user_text_key] = request_slice
    routed_call[multi_intent_route_flag_key] = True
    return routed_call
