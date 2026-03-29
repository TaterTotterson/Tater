from typing import Any, Callable, Dict, List, Optional


def _is_awaitable(value: Any) -> bool:
    return hasattr(value, "__await__")


async def _dispatch_wait_callback(
    wait_callback: Optional[Callable[..., Any]],
    *,
    func: str,
    plugin_obj: Any,
    wait_text: str,
    wait_payload: Optional[Dict[str, Any]],
) -> None:
    if not callable(wait_callback):
        return
    payload = dict(wait_payload) if isinstance(wait_payload, dict) else {}
    text = str(wait_text or "").strip()
    attempts = [
        (func, plugin_obj, text, payload),
        (func, plugin_obj, text),
        (func, plugin_obj),
    ]
    for args in attempts:
        try:
            result = wait_callback(*args)
            if _is_awaitable(result):
                await result
            return
        except TypeError:
            continue
        except Exception:
            return


async def normalize_tool_result_for_minos(
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
    if summary_hint and not summary:
        summary = summary_hint

    safe_data = result_for_llm_fn(normalized) if isinstance(normalized, dict) else {}
    execution_data: Dict[str, Any] = {}
    if isinstance(safe_data, dict):
        # Keep execution-relevant data compact for Thanatos/Minos; drop presentation-only duplicates.
        core_data = safe_data.get("data")
        if isinstance(core_data, dict):
            execution_data.update(core_data)
        for key, value in safe_data.items():
            if key in {
                "ok",
                "summary_for_user",
                "flair",
                "say_hint",
                "suggested_followups",
                "artifacts",
                "data",
            }:
                continue
            if value in (None, "", [], {}):
                continue
            execution_data[key] = value

    out: Dict[str, Any] = {
        "ok": bool(normalized.get("ok")),
        "summary_for_user": str(summary or "").strip(),
    }

    if execution_data:
        out["data"] = execution_data

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
    wait_callback: Optional[Callable[..., Any]],
    wait_text: str,
    wait_payload: Optional[Dict[str, Any]],
    admin_guard: Optional[Callable[[str], Optional[Dict[str, Any]]]],
    canonical_tool_name_fn: Callable[[str], str],
    attach_origin_fn: Callable[..., Dict[str, Any]],
    normalize_plugin_result_fn: Callable[[Any], Dict[str, Any]],
    normalize_tool_result_for_minos_fn: Callable[..., Any],
    action_failure_fn: Callable[..., Dict[str, Any]],
    plugin_display_name_fn: Callable[[Any], str],
    expand_plugin_platforms_fn: Callable[[Any], List[str]],
    plugin_supports_platform_fn: Callable[[Any, str], bool],
    is_meta_tool_fn: Callable[[str], bool],
    run_meta_tool_fn: Callable[..., Any],
    execute_plugin_call_fn: Callable[..., Any],
) -> Dict[str, Any]:
    raw_func = str(tool_call.get("function") or "").strip()
    func = canonical_tool_name_fn(raw_func) or raw_func
    plugin_obj = registry.get(func)
    args = dict(tool_call.get("arguments") or {})
    args = attach_origin_fn(
        args,
        origin=origin,
        platform=platform,
        scope=scope,
        request_text=user_text,
    )

    if admin_guard:
        guard_result = admin_guard(func)
        if guard_result:
            payload = normalize_plugin_result_fn(guard_result)
            minos_result = await normalize_tool_result_for_minos_fn(
                result_payload=payload,
                llm_client=llm_client,
                platform=platform,
            )
            return {"payload": payload, "minos_result": minos_result, "checker_result": minos_result}

    await _dispatch_wait_callback(
        wait_callback,
        func=func,
        plugin_obj=plugin_obj,
        wait_text=wait_text,
        wait_payload=wait_payload,
    )

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
        meta_result = run_meta_tool_fn(
            func=func,
            args=args,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
            llm_client=llm_client,
        )
        payload = await meta_result if _is_awaitable(meta_result) else meta_result
        normalize_source = payload
        if func == "get_verba_help" and isinstance(payload, dict) and "ok" not in payload:
            normalize_source = dict(payload)
            normalize_source["ok"] = not bool(payload.get("error"))
            if not str(normalize_source.get("summary_for_user") or "").strip():
                verba_id = str(normalize_source.get("verba_id") or "").strip()
                if verba_id:
                    normalize_source["summary_for_user"] = (
                        f"Loaded verba help for {verba_id}. Use usage_example for exact call shape."
                    )
        normalized_payload = normalize_plugin_result_fn(normalize_source)
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

    minos_result = await normalize_tool_result_for_minos_fn(
        result_payload=normalized_payload,
        llm_client=llm_client,
        platform=platform,
    )
    return {"payload": normalized_payload, "minos_result": minos_result, "checker_result": minos_result}


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
    return await normalize_tool_result_for_minos(
        result_payload=result_payload,
        llm_client=llm_client,
        platform=platform,
        normalize_plugin_result_fn=normalize_plugin_result_fn,
        narrate_result_fn=narrate_result_fn,
        result_for_llm_fn=result_for_llm_fn,
        short_text_fn=short_text_fn,
    )
