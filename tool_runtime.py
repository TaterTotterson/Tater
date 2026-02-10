import inspect
import re
from typing import Any, Callable, Dict, Optional

from plugin_kernel import (
    get_plugin_help,
    list_platforms_for_plugin,
    list_plugins_metadata,
    plugin_display_name,
    plugin_supports_platform,
    infer_needs_from_plugin,
)
from kernel_tools import (
    read_file,
    search_web,
    search_files,
    write_file,
    list_directory,
    delete_file,
    read_url,
    download_file,
    list_archive,
    extract_archive,
    list_stable_plugins,
    list_stable_platforms,
    inspect_plugin,
    validate_plugin,
    test_plugin,
    validate_platform,
    create_plugin,
    create_platform,
    write_workspace_note,
    list_workspace,
    memory_get,
    memory_set,
    memory_list,
    memory_delete,
    memory_explain,
    memory_search,
    truth_get_last,
    truth_list,
    vision_describer,
)
from plugin_result import action_failure, normalize_plugin_result


META_TOOLS = {
    "list_plugins",
    "get_plugin_help",
    "vision_describer",
    "list_platforms_for_plugin",
    "read_file",
    "search_web",
    "search_files",
    "write_file",
    "list_directory",
    "delete_file",
    "read_url",
    "download_file",
    "list_archive",
    "extract_archive",
    "list_stable_plugins",
    "list_stable_platforms",
    "inspect_plugin",
    "validate_plugin",
    "test_plugin",
    "validate_platform",
    "create_plugin",
    "create_platform",
    "write_workspace_note",
    "list_workspace",
    "memory_get",
    "memory_set",
    "memory_list",
    "memory_delete",
    "memory_explain",
    "memory_search",
    "truth_get_last",
    "truth_list",
}


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def is_meta_tool(name: Optional[str]) -> bool:
    return (name or "").strip() in META_TOOLS


def run_meta_tool(
    *,
    func: str,
    args: Dict[str, Any],
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]] = None,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if func == "list_plugins":
        arg_platform = str(args.get("platform") or platform or "webui")
        raw_flag = args.get("include_incompatible", False)
        if isinstance(raw_flag, str):
            include_incompatible = raw_flag.strip().lower() in {"1", "true", "yes", "on"}
        else:
            include_incompatible = bool(raw_flag)
        return list_plugins_metadata(
            platform=arg_platform,
            include_incompatible=include_incompatible,
            registry=registry,
            enabled_predicate=enabled_predicate,
        )

    if func == "get_plugin_help":
        return get_plugin_help(
            plugin_id=str(args.get("plugin_id") or ""),
            platform=args.get("platform") or platform,
            registry=registry,
        )

    if func == "vision_describer":
        return vision_describer(
            prompt=str(args.get("prompt") or args.get("query") or ""),
            path=args.get("path"),
            url=args.get("url"),
            blob_key=args.get("blob_key"),
            file_id=args.get("file_id"),
            image_ref=args.get("image_ref") if isinstance(args.get("image_ref"), dict) else None,
            history_key=args.get("history_key"),
            platform=args.get("platform") or platform,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )

    if func == "list_platforms_for_plugin":
        return list_platforms_for_plugin(
            plugin_id=str(args.get("plugin_id") or ""),
            registry=registry,
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
        return search_files(
            str(args.get("query") or ""),
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
    if func == "read_url":
        return read_url(
            str(args.get("url") or ""),
            max_bytes=int(args.get("max_bytes") or 200000),
            timeout_sec=int(args.get("timeout_sec") or 15),
        )
    if func == "download_file":
        return download_file(
            str(args.get("url") or ""),
            filename=args.get("filename"),
            subdir=args.get("subdir"),
            max_bytes=int(args.get("max_bytes") or 25000000),
            timeout_sec=int(args.get("timeout_sec") or 30),
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
            max_total_bytes=int(args.get("max_total_bytes") or 100000000),
        )

    if func == "list_stable_plugins":
        return list_stable_plugins()
    if func == "list_stable_platforms":
        return list_stable_platforms()
    if func == "inspect_plugin":
        return inspect_plugin(str(args.get("plugin_id") or ""))
    if func == "test_plugin":
        return test_plugin(
            str(args.get("name") or args.get("plugin_id") or ""),
            platform=args.get("platform"),
            auto_install=bool(args.get("auto_install", False)),
        )

    if func == "create_plugin":
        if not args.get("code") and not args.get("code_b64") and not args.get("code_lines"):
            manifest = args.get("manifest")
            code_files = args.get("code_files")
            if isinstance(manifest, dict) and not args.get("name"):
                args = dict(args)
                args["name"] = (
                    manifest.get("id")
                    or manifest.get("name")
                    or manifest.get("plugin_id")
                    or manifest.get("plugin_name")
                    or ""
                )
            if isinstance(code_files, list) and code_files:
                selected = None
                for entry in code_files:
                    if isinstance(entry, dict):
                        path = str(entry.get("path") or "").lower()
                        if path.endswith("main.py") or path.endswith(".py"):
                            selected = entry
                            break
                if selected is None:
                    selected = code_files[0] if isinstance(code_files[0], dict) else None
                if isinstance(selected, dict):
                    if isinstance(selected.get("content_lines"), list):
                        args = dict(args)
                        args["code_lines"] = selected.get("content_lines")
                    elif isinstance(selected.get("content"), str):
                        args = dict(args)
                        args["code"] = selected.get("content")
        if args.get("plugin_id"):
            args = dict(args)
            args["name"] = args.get("plugin_id")
        if not args.get("name") and args.get("plugin_name"):
            args = dict(args)
            args["name"] = args.get("plugin_name")
        name = str(args.get("name") or "").strip()
        if name and not re.match(r"^[A-Za-z0-9_-]+$", name) and args.get("plugin_id"):
            args = dict(args)
            args["name"] = str(args.get("plugin_id") or "").strip()
        raw_overwrite = args.get("overwrite", False)
        overwrite = (
            raw_overwrite.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(raw_overwrite, str)
            else bool(raw_overwrite)
        )
        return create_plugin(
            str(args.get("name") or ""),
            args.get("code"),
            code_b64=args.get("code_b64"),
            code_lines=args.get("code_lines"),
            overwrite=overwrite,
        )
    if func == "validate_plugin":
        return validate_plugin(
            str(args.get("name") or ""),
            bool(args.get("auto_install", True)),
        )
    if func == "create_platform":
        if not args.get("code") and not args.get("code_b64") and not args.get("code_lines"):
            manifest = args.get("manifest")
            code_files = args.get("code_files")
            if isinstance(manifest, dict) and not args.get("name"):
                args = dict(args)
                args["name"] = (
                    manifest.get("id")
                    or manifest.get("name")
                    or manifest.get("platform_key")
                    or manifest.get("platform_name")
                    or ""
                )
            if isinstance(code_files, list) and code_files:
                selected = None
                for entry in code_files:
                    if isinstance(entry, dict):
                        path = str(entry.get("path") or "").lower()
                        if path.endswith("main.py") or path.endswith(".py"):
                            selected = entry
                            break
                if selected is None:
                    selected = code_files[0] if isinstance(code_files[0], dict) else None
                if isinstance(selected, dict):
                    if isinstance(selected.get("content_lines"), list):
                        args = dict(args)
                        args["code_lines"] = selected.get("content_lines")
                    elif isinstance(selected.get("content"), str):
                        args = dict(args)
                        args["code"] = selected.get("content")
        if not args.get("name") and args.get("platform_name"):
            args = dict(args)
            args["name"] = args.get("platform_name")
        if not args.get("name") and args.get("platform_key"):
            args = dict(args)
            args["name"] = args.get("platform_key")
        raw_overwrite = args.get("overwrite", False)
        overwrite = (
            raw_overwrite.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(raw_overwrite, str)
            else bool(raw_overwrite)
        )
        return create_platform(
            str(args.get("name") or ""),
            args.get("code"),
            code_b64=args.get("code_b64"),
            code_lines=args.get("code_lines"),
            overwrite=overwrite,
        )
    if func == "validate_platform":
        return validate_platform(
            str(args.get("name") or ""),
            bool(args.get("auto_install", True)),
        )
    if func == "write_workspace_note":
        return write_workspace_note(str(args.get("content") or ""))
    if func == "list_workspace":
        return list_workspace()
    if func == "memory_get":
        raw_include = args.get("include_meta", True)
        include_meta = (
            raw_include.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(raw_include, str)
            else bool(raw_include)
        )
        return memory_get(
            keys=args.get("keys"),
            prefix=args.get("prefix"),
            scope=str(args.get("scope") or "global"),
            user_id=args.get("user_id"),
            room_id=args.get("room_id"),
            platform=args.get("platform") or platform,
            limit=_to_int(args.get("limit") or 50, 50),
            include_meta=include_meta,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "memory_set":
        entries = args.get("entries")
        if not isinstance(entries, dict):
            entries = args.get("values") if isinstance(args.get("values"), dict) else {}
        raw_confirmed = args.get("confirmed", False)
        confirmed = (
            raw_confirmed.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(raw_confirmed, str)
            else bool(raw_confirmed)
        )
        return memory_set(
            entries=entries,
            scope=str(args.get("scope") or "global"),
            user_id=args.get("user_id"),
            room_id=args.get("room_id"),
            platform=args.get("platform") or platform,
            ttl_sec=args.get("ttl_sec"),
            source=args.get("source"),
            request_text=args.get("request_text"),
            confirmed=confirmed,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "memory_list":
        return memory_list(
            prefix=args.get("prefix"),
            scope=str(args.get("scope") or "global"),
            user_id=args.get("user_id"),
            room_id=args.get("room_id"),
            platform=args.get("platform") or platform,
            limit=_to_int(args.get("limit") or 50, 50),
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "memory_delete":
        return memory_delete(
            keys=args.get("keys"),
            scope=str(args.get("scope") or "global"),
            user_id=args.get("user_id"),
            room_id=args.get("room_id"),
            platform=args.get("platform") or platform,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "memory_explain":
        key = args.get("key")
        if not key and args.get("keys"):
            if isinstance(args.get("keys"), list) and args.get("keys"):
                key = args.get("keys")[0]
            elif isinstance(args.get("keys"), str):
                key = args.get("keys").split(",", 1)[0]
        return memory_explain(
            str(key or ""),
            scope=str(args.get("scope") or "auto"),
            user_id=args.get("user_id"),
            room_id=args.get("room_id"),
            platform=args.get("platform") or platform,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "memory_search":
        raw_include_truth = args.get("include_truth", True)
        include_truth = (
            raw_include_truth.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(raw_include_truth, str)
            else bool(raw_include_truth)
        )
        return memory_search(
            str(args.get("query") or ""),
            scope=str(args.get("scope") or "auto"),
            user_id=args.get("user_id"),
            room_id=args.get("room_id"),
            platform=args.get("platform") or platform,
            include_truth=include_truth,
            limit=_to_int(args.get("limit") or 8, 8),
            min_score=_to_int(args.get("min_score") or 1, 1),
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "truth_get_last":
        return truth_get_last(
            platform=args.get("platform") or platform,
            scope=args.get("scope"),
            plugin_id=args.get("plugin_id"),
            scan_limit=_to_int(args.get("scan_limit") or 200, 200),
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "truth_list":
        return truth_list(
            platform=args.get("platform") or platform,
            scope=args.get("scope"),
            plugin_id=args.get("plugin_id"),
            limit=_to_int(args.get("limit") or 10, 10),
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )

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
        message=f"`{plugin_display_name(plugin)}` is not available on {platform}.",
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
        needs = infer_needs_from_plugin(plugin)
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

    if enabled_predicate and not enabled_predicate(func):
        return {
            "plugin_id": func,
            "plugin_name": plugin_display_name(plugin),
            "result": action_failure(
                code="plugin_disabled",
                message=f"Plugin `{func}` is currently disabled.",
                say_hint="Explain that this plugin is disabled and ask if the user wants an alternative.",
            ),
            "raw": None,
        }

    if not plugin_supports_platform(plugin, platform):
        return {
            "plugin_id": func,
            "plugin_name": plugin_display_name(plugin),
            "result": unsupported_platform_result(plugin, platform),
            "raw": None,
        }

    handler_name = f"handle_{platform}"
    if not hasattr(plugin, handler_name):
        return {
            "plugin_id": func,
            "plugin_name": plugin_display_name(plugin),
            "result": action_failure(
                code="unsupported_platform",
                message=f"`{plugin_display_name(plugin)}` does not expose `{handler_name}`.",
                available_on=list(getattr(plugin, "platforms", []) or []),
                say_hint="Explain this tool cannot run on the current platform and list supported platforms.",
            ),
            "raw": None,
        }

    try:
        raw = await _invoke_plugin_handler(
            plugin=plugin,
            handler_name=handler_name,
            args=args,
            llm_client=llm_client,
            context=context,
        )
    except Exception as e:
        return {
            "plugin_id": func,
            "plugin_name": plugin_display_name(plugin),
            "result": action_failure(
                code="plugin_exception",
                message=f"{plugin_display_name(plugin)} failed: {e}",
                say_hint="Explain that execution failed and ask whether to retry after checking settings.",
            ),
            "raw": None,
        }

    return {
        "plugin_id": func,
        "plugin_name": plugin_display_name(plugin),
        "result": normalize_plugin_result(raw),
        "raw": raw,
    }
