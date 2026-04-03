import inspect
import json
from typing import Any, Callable, Dict, Optional

from verba_base import ToolVerba
from verba_kernel import (
    get_verba_help,
    normalize_platform,
    verba_display_name,
    verba_supports_platform,
    infer_needs_from_verba,
)
from kernel_tools import (
    read_file,
    search_web,
    search_files,
    write_file,
    list_directory,
    delete_file,
    inspect_webpage,
    download_file,
    list_archive,
    extract_archive,
    write_workspace_note,
    list_workspace,
    image_describe,
    attach_file,
    send_message,
)
from verba_result import action_failure, action_success, normalize_verba_result
from helpers import redis_client as default_redis
from hydra_core_extensions import (
    get_hydra_kernel_tools,
    has_hydra_kernel_tool,
    hydra_kernel_tool_purpose,
    hydra_kernel_tool_usage,
    run_hydra_kernel_tool,
)


META_TOOLS = {
    "list_tools",
    "get_verba_help",
    "rewrite_text",
    "read_file",
    "search_web",
    "search_files",
    "write_file",
    "list_directory",
    "delete_file",
    "inspect_webpage",
    "download_file",
    "list_archive",
    "extract_archive",
    "write_workspace_note",
    "list_workspace",
    "image_describe",
    "attach_file",
    "send_message",
}

_KERNEL_TOOL_PURPOSE_HINTS = {
    "list_tools": "list kernel and enabled verba tools for current platform",
    "get_verba_help": "show verba usage example and guidance",
    "rewrite_text": "rewrite provided text according to natural-language instruction for downstream use",
    "read_file": "read local file contents",
    "search_web": "retrieve ranked link candidates with snippet metadata only (discovery-only; no full-page fetch and no file retrieval)",
    "search_files": "search text across local files",
    "write_file": "write content to a local file",
    "list_directory": "list files and folders",
    "delete_file": "delete a local file",
    "inspect_webpage": "inspect and extract content from a specific webpage URL (title, summary text, links, images)",
    "download_file": "download a file from a concrete file URL after discovery/inspection (actual file retrieval)",
    "list_archive": "inspect archive entries",
    "extract_archive": "extract archives to a target directory",
    "write_workspace_note": "append a workspace note",
    "list_workspace": "list workspace notes",
    "image_describe": "describe an explicit image using an artifact_id, URL, blob, or local path",
    "attach_file": "attach an available artifact/local file and optionally send it to a destination platform/target",
    "send_message": "queue a cross-portal notification/message only when the user explicitly asks to notify or message a destination (never for normal chat replies)",
}

_KERNEL_TOOL_USAGE_HINTS = {
    "list_tools": '{"function":"list_tools","arguments":{}}',
    "get_verba_help": '{"function":"get_verba_help","arguments":{"verba_id":"<verba_id>"}}',
    "rewrite_text": '{"function":"rewrite_text","arguments":{"instruction":"rewrite this to be funny","text":"the dog ran over the cow"}}',
    "read_file": '{"function":"read_file","arguments":{"path":"<path>"}}',
    "search_web": '{"function":"search_web","arguments":{"query":"<query>"}}',
    "search_files": '{"function":"search_files","arguments":{"query":"<query>","path":"/"}}',
    "write_file": '{"function":"write_file","arguments":{"path":"<path>","content":"<content>"}}',
    "list_directory": '{"function":"list_directory","arguments":{"path":"<path>"}}',
    "delete_file": '{"function":"delete_file","arguments":{"path":"<path>"}}',
    "inspect_webpage": '{"function":"inspect_webpage","arguments":{"url":"https://example.com"}}',
    "download_file": '{"function":"download_file","arguments":{"url":"https://example.com/file"}}',
    "list_archive": '{"function":"list_archive","arguments":{"path":"<archive_path>"}}',
    "extract_archive": '{"function":"extract_archive","arguments":{"path":"<archive_path>","destination":"<dest_path>"}}',
    "write_workspace_note": '{"function":"write_workspace_note","arguments":{"content":"<note_text>"}}',
    "list_workspace": '{"function":"list_workspace","arguments":{}}',
    "image_describe": '{"function":"image_describe","arguments":{"artifact_id":"<artifact_id>","query":"Describe this image."}}',
    "attach_file": '{"function":"attach_file","arguments":{"artifact_id":"<artifact_id>","message":"Attachment"}}',
    "send_message": '{"function":"send_message","arguments":{"message":"<message>"}}',
}


def _kernel_tool_rows(*, platform: str) -> list[Dict[str, str]]:
    rows: list[Dict[str, str]] = []
    for tool_id in sorted(META_TOOLS):
        token = str(tool_id or "").strip()
        if not token:
            continue
        rows.append(
            {
                "id": token,
                "description": str(_KERNEL_TOOL_PURPOSE_HINTS.get(token) or "").strip(),
                "usage": str(_KERNEL_TOOL_USAGE_HINTS.get(token) or "").strip(),
            }
        )

    core_rows = get_hydra_kernel_tools(platform=platform, redis_client=default_redis)
    for row in core_rows:
        if not isinstance(row, dict):
            continue
        token = str(row.get("id") or "").strip()
        if not token:
            continue
        rows.append(
            {
                "id": token,
                "description": str(row.get("description") or "").strip(),
                "usage": str(row.get("usage") or "").strip(),
            }
        )

    out: list[Dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        token = str(row.get("id") or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(row)
    return out


def kernel_tool_ids(*, platform: str) -> list[str]:
    return [str(row.get("id") or "").strip() for row in _kernel_tool_rows(platform=platform) if str(row.get("id") or "").strip()]


def kernel_tool_purpose_hint(*, tool_id: str, platform: str) -> str:
    key = str(tool_id or "").strip()
    if not key:
        return ""
    direct = str(_KERNEL_TOOL_PURPOSE_HINTS.get(key) or "").strip()
    if direct:
        return direct
    core_hint = hydra_kernel_tool_purpose(
        key,
        platform=platform,
        redis_client=default_redis,
    )
    if core_hint:
        return str(core_hint).strip()
    return ""


def kernel_tool_usage_hint(*, tool_id: str, platform: str) -> str:
    key = str(tool_id or "").strip()
    if not key:
        return ""
    direct = str(_KERNEL_TOOL_USAGE_HINTS.get(key) or "").strip()
    if direct:
        return direct
    core_hint = hydra_kernel_tool_usage(
        key,
        platform=platform,
        redis_client=default_redis,
    )
    if core_hint:
        return str(core_hint).strip()
    return ""

def _verba_enabled_from_settings(verba_id: str) -> bool:
    vid = str(verba_id or "").strip()
    if not vid:
        return False
    try:
        raw = default_redis.hget("verba_enabled", vid) if default_redis is not None else None
    except Exception:
        raw = None
    value = str(raw or "").strip().lower()
    if not value:
        return True
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return True


def _effective_enabled_predicate(
    enabled_predicate: Optional[Callable[[str], bool]],
) -> Callable[[str], bool]:
    if callable(enabled_predicate):
        return enabled_predicate
    return _verba_enabled_from_settings


def is_meta_tool(name: Optional[str]) -> bool:
    token = str(name or "").strip()
    if not token:
        return False
    if token in META_TOOLS:
        return True
    return has_hydra_kernel_tool(
        token,
        platform="",
        redis_client=default_redis,
    )


def list_tools(
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]] = None,
) -> Dict[str, Any]:
    normalized_platform = normalize_platform(platform) or str(platform or "").strip().lower() or "webui"

    kernel_tools = kernel_tool_ids(platform=normalized_platform)

    enabled_check = _effective_enabled_predicate(enabled_predicate)
    verba_tools: list[Dict[str, str]] = []
    for verba_id, verba in sorted((registry or {}).items(), key=lambda item: str(item[0] or "").lower()):
        vid = str(verba_id or "").strip()
        if not vid or verba is None:
            continue
        if not enabled_check(vid):
            continue
        if not verba_supports_platform(verba, normalized_platform):
            continue
        description = str(
            getattr(verba, "description", "")
            or getattr(verba, "verba_dec", "")
            or ""
        ).strip()
        if len(description) > 260:
            description = description[:257].rstrip() + "..."
        verba_tools.append(
            {
                "id": vid,
                "description": description,
            }
        )

    return {
        "tool": "list_tools",
        "ok": True,
        "platform": normalized_platform,
        "kernel_tools": kernel_tools,
        "verba_tools": verba_tools,
        "summary_for_user": f"Found {len(kernel_tools)} kernel tools and {len(verba_tools)} enabled verba tools on {normalized_platform}.",
    }


def _first_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""


def _first_non_empty_text(*values: Any) -> str:
    for value in values:
        text = _first_text(value)
        if text:
            return text
    return ""


def _strict_json_dict(text: Any) -> Optional[Dict[str, Any]]:
    raw = _first_text(text)
    if not raw:
        return None
    if not (raw.startswith("{") and raw.endswith("}")):
        return None
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


async def _rewrite_text_source_from_origin_with_llm(
    *,
    origin: Optional[Dict[str, Any]],
    llm_client: Any,
    instruction: str,
) -> str:
    payload = dict(origin) if isinstance(origin, dict) else {}
    tool_results = payload.get("tool_results_full")
    if not isinstance(tool_results, list):
        return ""
    if llm_client is None or not hasattr(llm_client, "chat"):
        return ""

    recent_results = [item for item in tool_results[-8:] if isinstance(item, dict)]
    if not recent_results:
        return ""

    system_prompt = (
        "You extract source text for a rewrite step.\n"
        "Select the single best text snippet from prior tool results that should be rewritten now.\n"
        "Return exactly one strict JSON object: {\"source_text\":\"...\"}\n"
        "Rules:\n"
        "- Do not rewrite content.\n"
        "- Do not add explanation.\n"
        "- If no valid source text exists, return {\"source_text\":\"\"}.\n"
    )
    extraction_payload = {
        "instruction": _first_text(instruction),
        "tool_results_full": recent_results,
    }
    try:
        resp = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(extraction_payload, ensure_ascii=False, default=str)},
            ],
            temperature=0.0,
        )
    except Exception:
        return ""

    obj = _strict_json_dict(((resp.get("message", {}) or {}).get("content", "")))
    if isinstance(obj, dict):
        return _first_text(obj.get("source_text"))
    return ""


async def _run_rewrite_text_tool(
    *,
    args: Dict[str, Any],
    llm_client: Any,
    platform: str,
    origin: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    instruction = _first_non_empty_text(
        args.get("instruction"),
        args.get("query"),
        args.get("request"),
        args.get("prompt"),
        args.get("style"),
        args.get("nl"),
    )
    source_text = _first_non_empty_text(
        args.get("text"),
        args.get("source_text"),
        args.get("source"),
        args.get("content"),
        args.get("input"),
        args.get("base_text"),
        args.get("original_text"),
    )

    if not instruction:
        instruction = "Rewrite the source text clearly while preserving the intended meaning."

    if llm_client is None or not hasattr(llm_client, "chat"):
        return action_failure(
            code="rewrite_text_model_unavailable",
            message="rewrite_text requires an available language model client.",
            say_hint="Explain rewrite service is unavailable right now and suggest retrying.",
        )

    if not source_text:
        source_text = await _rewrite_text_source_from_origin_with_llm(
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
            llm_client=llm_client,
            instruction=instruction,
        )

    if not source_text:
        return action_failure(
            code="rewrite_text_missing_source",
            message="rewrite_text needs source text to rewrite.",
            needs=[
                "Provide source text in arguments.text (or source_text/content/input).",
                "Or ensure prior tool results include concrete text to extract.",
            ],
            say_hint="Ask for the exact text to rewrite.",
        )

    system_prompt = (
        "You are a text rewriting tool.\n"
        "Task: rewrite source_text according to instruction.\n"
        "Rules:\n"
        "- Return rewritten text only.\n"
        "- Do not include prefaces, notes, labels, markdown fences, or analysis.\n"
        "- Preserve factual content unless instruction explicitly asks to transform it.\n"
        "- Keep names/entities consistent unless instruction asks to change them.\n"
    )
    payload = {
        "platform": str(platform or "").strip(),
        "instruction": instruction,
        "source_text": source_text,
    }
    try:
        resp = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0.2,
        )
    except Exception as exc:
        return action_failure(
            code="rewrite_text_failed",
            message=f"rewrite_text failed: {exc}",
            say_hint="Explain rewriting failed and offer to retry.",
        )

    rewritten = _first_text(((resp.get("message", {}) or {}).get("content", "")))
    if rewritten.startswith("{") and rewritten.endswith("}"):
        try:
            obj = json.loads(rewritten)
        except Exception:
            obj = None
        if isinstance(obj, dict):
            rewritten = _first_non_empty_text(
                obj.get("rewritten_text"),
                obj.get("text"),
                obj.get("message"),
                obj.get("content"),
            )
    if not rewritten:
        return action_failure(
            code="rewrite_text_empty",
            message="rewrite_text returned empty output.",
            say_hint="Explain rewriting returned no text and offer to retry.",
        )

    return action_success(
        facts={
            "action": "rewrite_text",
            "instruction": instruction,
            "source_chars": len(source_text),
            "rewritten_chars": len(rewritten),
        },
        data={
            "instruction": instruction,
            "source_text": source_text,
            "rewritten_text": rewritten,
        },
        summary_for_user=rewritten,
        say_hint="Return the rewritten text directly as the result.",
    )


async def run_meta_tool(
    *,
    func: str,
    args: Dict[str, Any],
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]] = None,
    origin: Optional[Dict[str, Any]] = None,
    llm_client: Any = None,
) -> Dict[str, Any]:
    if func == "list_tools":
        return list_tools(
            platform=args.get("platform") or platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
        )

    if func == "get_verba_help":
        verba_id = str(args.get("verba_id") or "").strip()
        return get_verba_help(
            verba_id=verba_id,
            platform=args.get("platform") or platform,
            registry=registry,
        )
    if func == "rewrite_text":
        return await _run_rewrite_text_tool(
            args=args,
            llm_client=llm_client,
            platform=platform,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )

    if func == "read_file":
        return read_file(
            str(args.get("path") or ""),
            start=args.get("start", 0),
            max_chars=args.get("max_chars"),
        )
    if func == "search_web":
        return search_web(
            str(args.get("query") or ""),
            num_results=int(args.get("num_results") or args.get("max_results") or 5),
            start=int(args.get("start") or 1),
            site=args.get("site") or args.get("domain"),
            safe=str(args.get("safe") or "active"),
            country=args.get("country"),
            language=args.get("language"),
            timeout_sec=int(args.get("timeout_sec") or 15),
        )
    if func == "search_files":
        raw_case = args.get("case_sensitive", False)
        case_sensitive = (
            raw_case.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(raw_case, str)
            else bool(raw_case)
        )
        raw_hidden = args.get("include_hidden", False)
        include_hidden = (
            raw_hidden.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(raw_hidden, str)
            else bool(raw_hidden)
        )
        search_query = str(
            args.get("query")
            or args.get("pattern")
            or args.get("text")
            or ""
        )
        return search_files(
            search_query,
            path=args.get("path"),
            max_results=int(args.get("max_results") or 100),
            case_sensitive=case_sensitive,
            include_hidden=include_hidden,
            file_glob=args.get("file_glob"),
        )
    if func == "write_file":
        return write_file(
            str(args.get("path") or ""),
            args.get("content"),
            content_b64=args.get("content_b64"),
            content_lines=args.get("content_lines"),
        )
    if func == "list_directory":
        return list_directory(str(args.get("path") or ""))
    if func == "delete_file":
        return delete_file(str(args.get("path") or ""))
    if func == "inspect_webpage":
        return inspect_webpage(
            str(args.get("url") or ""),
            timeout_sec=int(args.get("timeout_sec") or 20),
            max_links=int(args.get("max_links") or 20),
            max_images=int(args.get("max_images") or 20),
            platform=platform,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "download_file":
        return download_file(
            str(args.get("url") or ""),
            filename=args.get("filename"),
            subdir=args.get("subdir"),
            timeout_sec=int(args.get("timeout_sec") or 30),
            platform=platform,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "list_archive":
        return list_archive(
            str(args.get("path") or ""),
            max_entries=int(args.get("max_entries") or 1000),
        )
    if func == "extract_archive":
        raw_overwrite = args.get("overwrite", False)
        overwrite = (
            raw_overwrite.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(raw_overwrite, str)
            else bool(raw_overwrite)
        )
        return extract_archive(
            str(args.get("path") or ""),
            destination=args.get("destination"),
            overwrite=overwrite,
            max_files=int(args.get("max_files") or 1000),
        )

    if func == "write_workspace_note":
        return write_workspace_note(str(args.get("content") or ""))
    if func == "list_workspace":
        return list_workspace()
    if func == "image_describe":
        return image_describe(
            request=args.get("request"),
            query=args.get("query"),
            prompt=args.get("prompt"),
            artifact_id=args.get("artifact_id"),
            url=args.get("url"),
            path=args.get("path"),
            blob_key=args.get("blob_key"),
            file_id=args.get("file_id"),
            image_ref=args.get("image_ref"),
            source=args.get("source"),
            file=args.get("file"),
            name=args.get("name"),
            mimetype=args.get("mimetype"),
            platform=platform,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "attach_file":
        return attach_file(
            artifact_id=args.get("artifact_id"),
            path=args.get("path"),
            message=args.get("message"),
            content=args.get("content"),
            title=args.get("title"),
            platform=args.get("platform"),
            targets=args.get("targets"),
            priority=args.get("priority"),
            tags=args.get("tags"),
            ttl_sec=args.get("ttl_sec"),
            channel_id=args.get("channel_id"),
            channel=args.get("channel"),
            guild_id=args.get("guild_id"),
            room_id=args.get("room_id"),
            room_alias=args.get("room_alias"),
            device_service=args.get("device_service"),
            persistent=args.get("persistent"),
            api_notification=args.get("api_notification"),
            chat_id=args.get("chat_id"),
            device_id=args.get("device_id"),
            scope=args.get("scope"),
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "send_message":
        return send_message(
            message=args.get("message"),
            content=args.get("content"),
            title=args.get("title"),
            platform=args.get("platform"),
            targets=args.get("targets"),
            attachments=args.get("attachments"),
            priority=args.get("priority"),
            tags=args.get("tags"),
            ttl_sec=args.get("ttl_sec"),
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
            channel_id=args.get("channel_id"),
            channel=args.get("channel"),
            guild_id=args.get("guild_id"),
            room_id=args.get("room_id"),
            room_alias=args.get("room_alias"),
            device_service=args.get("device_service"),
            persistent=args.get("persistent"),
            api_notification=args.get("api_notification"),
            chat_id=args.get("chat_id"),
        )

    core_scope = str(
        args.get("scope")
        or (
            (args.get("origin") or {}).get("scope")
            if isinstance(args.get("origin"), dict)
            else ""
        )
        or (origin or {}).get("scope")
        or ""
    ).strip()
    core_result = await run_hydra_kernel_tool(
        tool_id=func,
        args=args,
        platform=platform,
        scope=core_scope,
        origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        llm_client=llm_client,
        redis_client=default_redis,
    )
    if isinstance(core_result, dict):
        return core_result

    return {"ok": False, "error": {"code": "unknown_meta_tool", "message": f"Unknown meta tool: {func}"}}


def unsupported_platform_result(plugin: Any, platform: str) -> Dict[str, Any]:
    available_on = list(getattr(plugin, "platforms", []) or [])
    if "both" in available_on:
        available_on = [
            "webui",
            "discord",
            "irc",
            "homeassistant",
            "homekit",
            "matrix",
            "telegram",
            "xbmc",
        ]
    return action_failure(
        code="unsupported_platform",
        message=f"`{verba_display_name(plugin)}` is not available on {platform}.",
        needs=[],
        available_on=available_on,
        say_hint="Explain that this tool is unavailable on the current platform and list where it works.",
    )


def _extract_request_text(context: Optional[Dict[str, Any]]) -> str:
    if not isinstance(context, dict):
        return ""

    for key in ("request_text", "raw_message", "raw", "user_text", "task_prompt", "body"):
        val = context.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    msg = context.get("message")
    if msg is not None:
        for attr in ("content", "text", "message"):
            val = getattr(msg, attr, None)
            if isinstance(val, str) and val.strip():
                return val.strip()

    update = context.get("update")
    if isinstance(update, dict):
        text = ((update.get("message") or {}).get("text") or "")
        if isinstance(text, str) and text.strip():
            return text.strip()

    ctx = context.get("context")
    if isinstance(ctx, dict):
        for key in ("raw_message", "text", "message"):
            val = ctx.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()

    return ""


def _normalize_request_value(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "content", "value", "request"):
            val = value.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    if isinstance(value, list):
        parts = [v.strip() for v in value if isinstance(v, str) and v.strip()]
        if parts:
            return " ".join(parts).strip()
    return ""


def _needs_request_arg(needs: Any) -> bool:
    if not isinstance(needs, list):
        return False
    for item in needs:
        if not isinstance(item, str):
            continue
        norm = item.strip().lower()
        if norm == "request" or norm.startswith("request "):
            return True
        if "request" in norm and "request" == norm.split("(", 1)[0].strip():
            return True
    return False


def _autofill_request_arg(plugin: Any, args: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    try:
        needs = infer_needs_from_verba(plugin)
    except Exception:
        needs = []

    if not _needs_request_arg(needs):
        return args

    req = args.get("request")
    normalized = _normalize_request_value(req)
    if normalized:
        args["request"] = normalized
        return args

    text = _extract_request_text(context)
    if text:
        args["request"] = text
    return args


async def _invoke_plugin_handler(
    plugin: Any,
    handler_name: str,
    args: Dict[str, Any],
    llm_client: Any,
    context: Optional[Dict[str, Any]] = None,
) -> Any:
    handler = getattr(plugin, handler_name)
    context = context or {}

    try:
        sig = inspect.signature(handler)
    except Exception:
        sig = None

    if sig is None:
        result = handler(args, llm_client)
        return await result if inspect.isawaitable(result) else result

    call_kwargs: Dict[str, Any] = {}
    missing = []
    for name, param in sig.parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            # Variadic params are optional capture buckets, not required call context.
            continue
        if name == "args":
            call_kwargs[name] = args
            continue
        if name in {"llm_client", "llm"}:
            call_kwargs[name] = llm_client
            continue
        if name == "context":
            call_kwargs[name] = context.get("context", context)
            continue
        if name in context:
            call_kwargs[name] = context[name]
            continue
        if param.default is inspect._empty:
            missing.append(name)

    if missing:
        raise RuntimeError(f"Missing platform call context for {plugin.name}: {', '.join(missing)}")

    result = handler(**call_kwargs)
    return await result if inspect.isawaitable(result) else result


def _handler_name_candidates(platform: str) -> list[str]:
    normalized = normalize_platform(platform)
    if not normalized:
        return []
    return [f"handle_{normalized}"]


def _plugin_has_handler(plugin: Any, handler_name: str) -> bool:
    method = getattr(plugin.__class__, handler_name, None)
    if not callable(method):
        return False
    base = getattr(ToolVerba, handler_name, None)
    if base is None:
        return True
    return method is not base


async def execute_plugin_call(
    *,
    func: str,
    args: Dict[str, Any],
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    llm_client: Any,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    plugin = registry.get(func)
    if not plugin:
        return {
            "plugin_id": func,
            "plugin_name": func,
            "result": action_failure(
                code="unknown_plugin",
                message=f"Plugin `{func}` is not installed.",
                say_hint="Explain that this plugin is unavailable and ask for a different action.",
            ),
            "raw": None,
        }

    args = dict(args or {})
    args = _autofill_request_arg(plugin, args, context)

    enabled_check = _effective_enabled_predicate(enabled_predicate)
    if not enabled_check(func):
        return {
            "plugin_id": func,
            "plugin_name": verba_display_name(plugin),
            "result": action_failure(
                code="plugin_disabled",
                message=f"Plugin `{func}` is currently disabled.",
                say_hint="Explain that this plugin is disabled and ask if the user wants an alternative.",
            ),
            "raw": None,
        }

    if not verba_supports_platform(plugin, platform):
        return {
            "plugin_id": func,
            "plugin_name": verba_display_name(plugin),
            "result": unsupported_platform_result(plugin, platform),
            "raw": None,
        }

    handler_candidates = _handler_name_candidates(platform)
    chosen_handler = next((name for name in handler_candidates if _plugin_has_handler(plugin, name)), "")
    if not chosen_handler:
        display_handlers = ", ".join(handler_candidates) if handler_candidates else f"handle_{platform}"
        return {
            "plugin_id": func,
            "plugin_name": verba_display_name(plugin),
            "result": action_failure(
                code="unsupported_platform",
                message=f"`{verba_display_name(plugin)}` does not expose {display_handlers}.",
                available_on=list(getattr(plugin, "platforms", []) or []),
                say_hint="Explain this tool cannot run on the current platform and list supported platforms.",
            ),
            "raw": None,
        }

    try:
        raw = await _invoke_plugin_handler(
            plugin=plugin,
            handler_name=chosen_handler,
            args=args,
            llm_client=llm_client,
            context=context,
        )
    except Exception as e:
        return {
            "plugin_id": func,
            "plugin_name": verba_display_name(plugin),
            "result": action_failure(
                code="plugin_exception",
                message=f"{verba_display_name(plugin)} failed: {e}",
                say_hint="Explain that execution failed and ask whether to retry after checking settings.",
            ),
            "raw": None,
        }

    return {
        "plugin_id": func,
        "plugin_name": verba_display_name(plugin),
        "result": normalize_verba_result(raw),
        "raw": raw,
    }
