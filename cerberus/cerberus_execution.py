from typing import Any, Callable, Dict, List, Optional


async def normalize_tool_result_for_checker(
    *,
    result_payload: Any,
    llm_client: Any,
    platform: str,
    normalize_plugin_result_fn: Callable[[Any], Dict[str, Any]],
    narrate_result_fn: Callable[..., Any],
    result_for_llm_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    short_text_fn: Callable[..., str],
) -> Dict[str, Any]:
    normalized = normalize_plugin_result_fn(result_payload)
    summary = await narrate_result_fn(normalized, llm_client=llm_client, platform=platform)
    summary_hint = str(normalized.get("summary_for_user") or "").strip()
    flair_hint = str(normalized.get("flair") or "").strip()
    if summary_hint and not summary:
        summary = summary_hint

    out: Dict[str, Any] = {
        "ok": bool(normalized.get("ok")),
        "summary_for_user": str(summary or "").strip(),
    }
    say_hint = short_text_fn(normalized.get("say_hint"), limit=320)
    if say_hint:
        out["say_hint"] = say_hint
    if flair_hint:
        out["flair"] = flair_hint

    safe_data = result_for_llm_fn(normalized) if isinstance(normalized, dict) else {}
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


async def execute_tool_call(
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
    canonical_tool_name_fn: Callable[[str], str],
    attach_origin_fn: Callable[..., Dict[str, Any]],
    apply_full_user_request_requirement_fn: Callable[..., Dict[str, Any]],
    normalize_plugin_result_fn: Callable[[Any], Dict[str, Any]],
    normalize_tool_result_for_checker_fn: Callable[..., Any],
    action_failure_fn: Callable[..., Dict[str, Any]],
    plugin_display_name_fn: Callable[[Any], str],
    expand_plugin_platforms_fn: Callable[[Any], List[str]],
    plugin_supports_platform_fn: Callable[[Any, str], bool],
    is_meta_tool_fn: Callable[[str], bool],
    run_meta_tool_fn: Callable[..., Any],
    execute_plugin_call_fn: Callable[..., Any],
) -> Dict[str, Any]:
    func = str(tool_call.get("function") or "").strip()
    canonical_tool_name_fn(func)  # keep parity with existing side-effect-free canonicalization step
    plugin_obj = registry.get(func)
    args = dict(tool_call.get("arguments") or {})
    args = attach_origin_fn(
        args,
        origin=origin,
        platform=platform,
        scope=scope,
        request_text=user_text,
    )
    args = apply_full_user_request_requirement_fn(
        plugin_obj=plugin_obj,
        args=args,
        user_text=user_text,
    )

    if admin_guard:
        guard_result = admin_guard(func)
        if guard_result:
            payload = normalize_plugin_result_fn(guard_result)
            checker_result = await normalize_tool_result_for_checker_fn(
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

    origin_context = args.get("origin") if isinstance(args.get("origin"), dict) else {}
    if isinstance(origin_context, dict) and origin_context:
        runtime_context.setdefault("origin", origin_context)

    if platform == "irc":
        channel_value = str(
            runtime_context.get("channel")
            or origin_context.get("channel")
            or origin_context.get("target")
            or ""
        ).strip()
        if channel_value:
            runtime_context.setdefault("channel", channel_value)

        user_value = str(
            runtime_context.get("user")
            or origin_context.get("user")
            or origin_context.get("user_id")
            or ""
        ).strip()
        if user_value:
            runtime_context.setdefault("user", user_value)

        raw_value = str(
            runtime_context.get("raw_message")
            or runtime_context.get("raw")
            or user_text
            or ""
        ).strip()
        if raw_value:
            runtime_context.setdefault("raw_message", raw_value)
            runtime_context.setdefault("raw", raw_value)

        runtime_context.setdefault("bot", runtime_context.get("irc_bot"))

    if is_meta_tool_fn(func):
        payload = run_meta_tool_fn(
            func=func,
            args=args,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
        normalized_payload = normalize_plugin_result_fn(payload)
    else:
        if plugin_obj and not plugin_supports_platform_fn(plugin_obj, platform):
            available_on = expand_plugin_platforms_fn(getattr(plugin_obj, "platforms", []) or [])
            normalized_payload = action_failure_fn(
                code="unsupported_platform",
                message=f"`{plugin_display_name_fn(plugin_obj)}` is not available on {platform}.",
                available_on=available_on,
                say_hint="Explain this tool is unavailable on this platform and list supported platforms.",
            )
        else:
            exec_result = await execute_plugin_call_fn(
                func=func,
                args=args,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                llm_client=llm_client,
                context=runtime_context,
            )
            normalized_payload = normalize_plugin_result_fn(exec_result.get("result"))

    checker_result = await normalize_tool_result_for_checker_fn(
        result_payload=normalized_payload,
        llm_client=llm_client,
        platform=platform,
    )
    return {"payload": normalized_payload, "checker_result": checker_result}
