from typing import Any, Awaitable, Callable, Dict, List, Optional


async def validate_or_recover_tool_call(
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
    is_tool_candidate_fn: Callable[[str], bool],
    validate_tool_contract_fn: Callable[..., Awaitable[Dict[str, Any]]],
    short_text_fn: Callable[..., str],
    generate_recovery_text_fn: Callable[..., Awaitable[str]],
    validation_failure_text_fn: Callable[[str, str], str],
    normalize_tool_call_for_user_request_fn: Callable[..., Dict[str, Any]],
    enrich_tool_call_for_user_request_fn: Optional[Callable[..., Awaitable[Dict[str, Any]]]] = None,
    resolver_max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not is_tool_candidate_fn(raw):
        return {
            "ok": False,
            "tool_call": None,
            "repair_used": False,
            "reason": "not_tool_candidate",
            "recovery_text_if_blocked": None,
            "attempted_tool": None,
            "validation_status": None,
        }

    validation_status = await validate_tool_contract_fn(
        llm_client=llm_client,
        response_text=raw,
        user_text=user_text,
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
        assistant_text = short_text_fn(validation_status.get("assistant_text"), limit=320)
        recovery_text = assistant_text
        if not recovery_text:
            recovery_text = await generate_recovery_text_fn(
                llm_client=llm_client,
                platform=platform,
                user_text=user_text,
                error_kind="validation",
                reason=reason,
                fallback=validation_failure_text_fn(reason=reason, platform=platform),
                platform_preamble=platform_preamble,
                max_tokens=recovery_max_tokens,
            )
        return {
            "ok": False,
            "tool_call": tool_call,
            "repair_used": bool(validation_status.get("repair_used")),
            "reason": reason,
            "recovery_text_if_blocked": recovery_text,
            "assistant_text": assistant_text,
            "attempted_tool": attempted_tool,
            "validation_status": validation_status,
        }

    if not isinstance(tool_call, dict):
        reason = "invalid_tool_call"
        recovery_text = await generate_recovery_text_fn(
            llm_client=llm_client,
            platform=platform,
            user_text=user_text,
            error_kind="validation",
            reason=reason,
            fallback=validation_failure_text_fn(reason=reason, platform=platform),
            platform_preamble=platform_preamble,
            max_tokens=recovery_max_tokens,
        )
        return {
            "ok": False,
            "tool_call": None,
            "repair_used": bool(validation_status.get("repair_used")),
            "reason": reason,
            "recovery_text_if_blocked": recovery_text,
            "assistant_text": "",
            "attempted_tool": attempted_tool,
            "validation_status": validation_status,
        }

    tool_call = normalize_tool_call_for_user_request_fn(
        tool_call=tool_call,
        registry=registry,
        user_text=user_text,
    )
    if callable(enrich_tool_call_for_user_request_fn):
        try:
            enriched_tool_call = await enrich_tool_call_for_user_request_fn(
                llm_client=llm_client,
                tool_call=tool_call,
                user_text=user_text,
                platform=platform,
                origin=(origin if isinstance(origin, dict) else {}),
                scope=scope,
                history_messages=(history_messages if isinstance(history_messages, list) else []),
                context=(context if isinstance(context, dict) else {}),
                platform_preamble=platform_preamble,
                max_tokens=resolver_max_tokens,
            )
            if isinstance(enriched_tool_call, dict):
                tool_call = enriched_tool_call
        except Exception:
            pass
    if isinstance(validation_status, dict):
        validation_status["tool_call"] = tool_call

    return {
        "ok": True,
        "tool_call": tool_call,
        "repair_used": bool(validation_status.get("repair_used")),
        "reason": str(validation_status.get("reason") or "ok"),
        "recovery_text_if_blocked": None,
        "assistant_text": "",
        "attempted_tool": attempted_tool,
        "validation_status": validation_status,
    }
