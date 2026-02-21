import inspect
from typing import Any, Callable, Dict, Optional

from plugin_kernel import (
    get_plugin_help,
    list_platforms_for_plugin,
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
    inspect_webpage,
    download_file,
    list_archive,
    extract_archive,
    list_stable_plugins,
    list_stable_platforms,
    inspect_plugin,
    validate_plugin,
    test_plugin,
    validate_platform,
    write_workspace_note,
    list_workspace,
    memory_get,
    memory_set,
    memory_list,
    memory_delete,
    memory_explain,
    memory_search,
)
from plugin_result import action_failure, normalize_plugin_result
from helpers import redis_client as default_redis
from memory_platform_store import (
    forget_fact_keys,
    load_doc as load_memory_doc,
    resolve_user_doc_key as resolve_memory_user_doc_key,
    room_doc_key,
    save_doc as save_memory_doc,
    summarize_doc,
    user_doc_key,
    value_to_text,
)


META_TOOLS = {
    "get_plugin_help",
    "list_platforms_for_plugin",
    "read_file",
    "search_web",
    "search_files",
    "write_file",
    "list_directory",
    "delete_file",
    "read_url",
    "inspect_webpage",
    "download_file",
    "list_archive",
    "extract_archive",
    "list_stable_plugins",
    "list_stable_platforms",
    "inspect_plugin",
    "validate_plugin",
    "test_plugin",
    "validate_platform",
    "write_workspace_note",
    "list_workspace",
    "memory_get",
    "memory_set",
    "memory_list",
    "memory_explain",
    "memory_search",
}

_PLUGIN_ID_ALIASES = {
    "describe_image": "image_describe",
    "describe_latest_image": "image_describe",
}


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _coerce_memory_entries(args: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(args, dict):
        return {}

    direct_entries = args.get("entries")
    if isinstance(direct_entries, dict) and direct_entries:
        return dict(direct_entries)

    values = args.get("values")
    if isinstance(values, dict) and values:
        return dict(values)

    memory_obj = args.get("memory")
    if isinstance(memory_obj, dict) and memory_obj:
        return dict(memory_obj)

    entry_obj = args.get("entry")
    if isinstance(entry_obj, dict) and entry_obj:
        entry_key = str(entry_obj.get("key") or "").strip()
        if entry_key:
            return {entry_key: entry_obj.get("value")}
        # If no explicit key/value shape, accept dict payload directly.
        return dict(entry_obj)

    out: Dict[str, Any] = {}
    items = args.get("items")
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            item_key = str(item.get("key") or "").strip()
            if not item_key:
                continue
            out[item_key] = item.get("value")
    if out:
        return out

    key_name = str(args.get("key") or args.get("memory_key") or "").strip()
    if key_name:
        if "value" in args:
            return {key_name: args.get("value")}
        if "memory_value" in args:
            return {key_name: args.get("memory_value")}

    return {}


_MEMORY_SCOPE_GLOBAL_HINTS = (
    "for everyone",
    "for all users",
    "all users",
    "global",
)
_MEMORY_SCOPE_ROOM_HINTS = (
    "this room",
    "this channel",
    "this chat",
    "in this room",
    "in this channel",
    "in this chat",
    "for this room",
    "for this channel",
    "for this chat",
)


def _memory_platform_key_list(args: Dict[str, Any]) -> list[str]:
    key_value = args.get("key")
    keys_value = args.get("keys")
    raw_items: list[str] = []
    if isinstance(keys_value, list):
        for item in keys_value:
            text = str(item or "").strip()
            if text:
                raw_items.append(text)
    elif isinstance(keys_value, str):
        for part in keys_value.split(","):
            text = str(part or "").strip()
            if text:
                raw_items.append(text)
    if isinstance(key_value, str) and key_value.strip():
        raw_items.append(key_value.strip())

    out: list[str] = []
    seen = set()
    for item in raw_items:
        if item in seen:
            continue
        out.append(item)
        seen.add(item)
    return out


def _memory_platform_user_id(args: Dict[str, Any], origin: Optional[Dict[str, Any]]) -> str:
    explicit = str(args.get("user_id") or "").strip()
    if explicit:
        return explicit
    merged = dict(origin or {})
    if isinstance(args.get("origin"), dict):
        merged.update(args.get("origin") or {})
    return (
        _origin_text(merged, "user_id", "user", "username", "sender")
        or ""
    ).strip()


def _memory_platform_user_display_name(args: Dict[str, Any], origin: Optional[Dict[str, Any]]) -> str:
    explicit = str(args.get("display_name") or "").strip()
    if explicit:
        return explicit
    explicit_user = str(args.get("user_id") or "").strip()
    if explicit_user:
        return explicit_user
    merged = dict(origin or {})
    if isinstance(args.get("origin"), dict):
        merged.update(args.get("origin") or {})
    return (
        _origin_text(merged, "username", "user", "sender", "display_name", "nick", "nickname")
        or ""
    ).strip()


def _memory_platform_room_id(args: Dict[str, Any], origin: Optional[Dict[str, Any]]) -> str:
    explicit = str(args.get("room_id") or args.get("scope") or "").strip()
    if not explicit:
        merged = dict(origin or {})
        if isinstance(args.get("origin"), dict):
            merged.update(args.get("origin") or {})
        explicit = _origin_text(
            merged,
            "room_id",
            "room",
            "channel_id",
            "channel",
            "chat_id",
            "scope",
        )
    explicit = str(explicit or "").strip()
    if ":" in explicit:
        prefix, _, suffix = explicit.partition(":")
        if prefix.lower() in {"room", "channel", "chat", "session", "dm", "chan", "pm", "device", "area"} and suffix:
            explicit = suffix
    return explicit.strip()


def _memory_show_user(args: Dict[str, Any], platform: str, origin: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    p = str(args.get("platform") or platform or "webui").strip().lower() or "webui"
    user_id = _memory_platform_user_id(args, origin)
    if not user_id:
        return {"tool": "memory_show", "ok": False, "error": "user_id is required for memory_show."}

    limit = _to_int(args.get("limit") or 20, 20)
    try:
        min_confidence = float(args.get("min_confidence") or 0.0)
    except Exception:
        min_confidence = 0.0
    display_name = _memory_platform_user_display_name(args, origin) or user_id
    redis_key = resolve_memory_user_doc_key(
        default_redis,
        p,
        user_id,
        create=False,
        display_name=display_name,
        auto_link_name=True,
    ) or user_doc_key(p, user_id)
    doc = load_memory_doc(default_redis, redis_key)
    items = summarize_doc(doc, max_items=max(1, limit), min_confidence=max(0.0, min(1.0, min_confidence)))
    return {
        "tool": "memory_show",
        "ok": True,
        "platform": p,
        "scope": "user",
        "user_id": user_id,
        "redis_key": redis_key,
        "count": len(items),
        "items": [
            {
                "key": item.get("key"),
                "value": item.get("value"),
                "confidence": item.get("confidence"),
                "ttl_sec": item.get("ttl_sec"),
                "evidence": item.get("evidence"),
            }
            for item in items
        ],
        "summary": "; ".join(
            [
                f"{str(item.get('key') or '')}={value_to_text(item.get('value'), max_chars=80)} ({float(item.get('confidence') or 0.0):.2f})"
                for item in items
            ]
        ),
    }


def _memory_show_room(args: Dict[str, Any], platform: str, origin: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    p = str(args.get("platform") or platform or "webui").strip().lower() or "webui"
    room_id = _memory_platform_room_id(args, origin)
    explicit_room = str(args.get("room_id") or "").strip()
    if p == "webui" and not explicit_room:
        room_id = "chat"
    if not room_id:
        return {"tool": "memory_show_room", "ok": False, "error": "room_id is required for memory_show_room."}

    limit = _to_int(args.get("limit") or 20, 20)
    try:
        min_confidence = float(args.get("min_confidence") or 0.0)
    except Exception:
        min_confidence = 0.0
    redis_key = room_doc_key(p, room_id)
    doc = load_memory_doc(default_redis, redis_key)
    items = summarize_doc(doc, max_items=max(1, limit), min_confidence=max(0.0, min(1.0, min_confidence)))
    return {
        "tool": "memory_show_room",
        "ok": True,
        "platform": p,
        "scope": "room",
        "room_id": room_id,
        "redis_key": redis_key,
        "count": len(items),
        "items": [
            {
                "key": item.get("key"),
                "value": item.get("value"),
                "confidence": item.get("confidence"),
                "ttl_sec": item.get("ttl_sec"),
                "evidence": item.get("evidence"),
            }
            for item in items
        ],
        "summary": "; ".join(
            [
                f"{str(item.get('key') or '')}={value_to_text(item.get('value'), max_chars=80)} ({float(item.get('confidence') or 0.0):.2f})"
                for item in items
            ]
        ),
    }


def _memory_forget_key(args: Dict[str, Any], platform: str, origin: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    scope = str(args.get("scope") or "user").strip().lower() or "user"
    p = str(args.get("platform") or platform or "webui").strip().lower() or "webui"
    keys = _memory_platform_key_list(args)
    if not keys:
        return {"tool": "memory_forget_key", "ok": False, "error": "key or keys is required."}

    if scope == "room":
        room_id = _memory_platform_room_id(args, origin)
        explicit_room = str(args.get("room_id") or "").strip()
        if p == "webui" and not explicit_room:
            room_id = "chat"
        if not room_id:
            return {"tool": "memory_forget_key", "ok": False, "error": "room_id is required for room scope."}
        redis_key = room_doc_key(p, room_id)
        identity: Dict[str, Any] = {"room_id": room_id}
    else:
        scope = "user"
        user_id = _memory_platform_user_id(args, origin)
        if not user_id:
            return {"tool": "memory_forget_key", "ok": False, "error": "user_id is required for user scope."}
        display_name = _memory_platform_user_display_name(args, origin) or user_id
        redis_key = resolve_memory_user_doc_key(
            default_redis,
            p,
            user_id,
            create=False,
            display_name=display_name,
            auto_link_name=True,
        ) or user_doc_key(p, user_id)
        identity = {"user_id": user_id}

    doc = load_memory_doc(default_redis, redis_key)
    deleted = forget_fact_keys(doc, keys)
    if deleted > 0:
        save_memory_doc(default_redis, redis_key, doc)
    return {
        "tool": "memory_forget_key",
        "ok": True,
        "scope": scope,
        "platform": p,
        "redis_key": redis_key,
        "deleted": deleted,
        **identity,
    }


def _memory_forget_all(args: Dict[str, Any], platform: str, origin: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    scope = str(args.get("scope") or "user").strip().lower() or "user"
    p = str(args.get("platform") or platform or "webui").strip().lower() or "webui"

    if scope == "room":
        room_id = _memory_platform_room_id(args, origin)
        explicit_room = str(args.get("room_id") or "").strip()
        if p == "webui" and not explicit_room:
            room_id = "chat"
        if not room_id:
            return {"tool": "memory_forget_all", "ok": False, "error": "room_id is required for room scope."}
        redis_key = room_doc_key(p, room_id)
        identity: Dict[str, Any] = {"room_id": room_id}
    else:
        scope = "user"
        user_id = _memory_platform_user_id(args, origin)
        if not user_id:
            return {"tool": "memory_forget_all", "ok": False, "error": "user_id is required for user scope."}
        display_name = _memory_platform_user_display_name(args, origin) or user_id
        redis_key = resolve_memory_user_doc_key(
            default_redis,
            p,
            user_id,
            create=False,
            display_name=display_name,
            auto_link_name=True,
        ) or user_doc_key(p, user_id)
        identity = {"user_id": user_id}

    deleted = 0
    try:
        deleted = int(default_redis.delete(redis_key) or 0)
    except Exception:
        deleted = 0

    if deleted <= 0:
        try:
            save_memory_doc(default_redis, redis_key, {"schema_version": 1, "last_updated": 0, "facts": {}})
        except Exception:
            pass

    return {
        "tool": "memory_forget_all",
        "ok": True,
        "scope": scope,
        "platform": p,
        "redis_key": redis_key,
        "deleted": deleted,
        **identity,
    }


def _origin_text(origin: Any, *keys: str) -> str:
    if not isinstance(origin, dict):
        return ""
    for key in keys:
        value = str(origin.get(key) or "").strip()
        if value:
            return value
    return ""


def _resolve_memory_scope(args: Dict[str, Any], origin: Optional[Dict[str, Any]]) -> str:
    raw_scope = str(args.get("scope") or "").strip().lower()
    if raw_scope in {"global", "user", "room"}:
        return raw_scope
    if raw_scope:
        # Preserve invalid explicit values so kernel validation can return a useful error.
        return raw_scope

    args_origin = args.get("origin") if isinstance(args.get("origin"), dict) else None
    merged_origin = dict(origin or {})
    if isinstance(args_origin, dict):
        merged_origin.update(args_origin)

    request_text = str(
        args.get("request_text")
        or merged_origin.get("request_text")
        or ""
    ).strip().lower()
    if request_text:
        if any(marker in request_text for marker in _MEMORY_SCOPE_GLOBAL_HINTS):
            return "global"
        if any(marker in request_text for marker in _MEMORY_SCOPE_ROOM_HINTS):
            return "room"

    if str(args.get("user_id") or "").strip():
        return "user"
    if str(args.get("room_id") or "").strip():
        return "room"

    if _origin_text(merged_origin, "user_id", "user", "username", "sender"):
        return "user"
    if _origin_text(merged_origin, "room_id", "room", "channel_id", "channel", "chat_id", "scope"):
        return "room"
    return "global"


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
    if func == "get_plugin_help":
        plugin_id = str(args.get("plugin_id") or "").strip()
        plugin_id = _PLUGIN_ID_ALIASES.get(plugin_id, plugin_id)
        return get_plugin_help(
            plugin_id=plugin_id,
            platform=args.get("platform") or platform,
            registry=registry,
        )

    if func == "list_platforms_for_plugin":
        return list_platforms_for_plugin(
            plugin_id=_PLUGIN_ID_ALIASES.get(
                str(args.get("plugin_id") or "").strip(),
                str(args.get("plugin_id") or "").strip(),
            ),
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
    if func == "read_url":
        return read_url(
            str(args.get("url") or ""),
            max_bytes=int(args.get("max_bytes") or 200000),
            timeout_sec=int(args.get("timeout_sec") or 15),
        )
    if func == "inspect_webpage":
        return inspect_webpage(
            str(args.get("url") or ""),
            max_bytes=int(args.get("max_bytes") or 300000),
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
            max_bytes=int(args.get("max_bytes") or 25000000),
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

    if func == "validate_plugin":
        return validate_plugin(
            str(args.get("name") or ""),
            bool(args.get("auto_install", True)),
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
        try:
            min_confidence = float(args.get("min_confidence") or 0.0)
        except Exception:
            min_confidence = 0.0
        scope_value = _resolve_memory_scope(args, origin)
        return memory_get(
            keys=args.get("keys"),
            prefix=args.get("prefix"),
            scope=scope_value,
            user_id=args.get("user_id"),
            room_id=args.get("room_id"),
            platform=args.get("platform") or platform,
            store=args.get("store") or "auto",
            limit=_to_int(args.get("limit") or 50, 50),
            min_confidence=min_confidence,
            include_meta=include_meta,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "memory_set":
        entries = _coerce_memory_entries(args)
        raw_confirmed = args.get("confirmed", False)
        confirmed = (
            raw_confirmed.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(raw_confirmed, str)
            else bool(raw_confirmed)
        )
        request_text = args.get("request_text")
        if not str(request_text or "").strip():
            if isinstance(args.get("origin"), dict):
                request_text = args["origin"].get("request_text")
            if (not str(request_text or "").strip()) and isinstance(origin, dict):
                request_text = origin.get("request_text")
        scope_value = _resolve_memory_scope(args, origin)
        return memory_set(
            entries=entries,
            scope=scope_value,
            user_id=args.get("user_id"),
            room_id=args.get("room_id"),
            platform=args.get("platform") or platform,
            ttl_sec=args.get("ttl_sec"),
            source=args.get("source"),
            request_text=request_text,
            confirmed=confirmed,
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "memory_list":
        scope_value = _resolve_memory_scope(args, origin)
        return memory_list(
            prefix=args.get("prefix"),
            scope=scope_value,
            user_id=args.get("user_id"),
            room_id=args.get("room_id"),
            platform=args.get("platform") or platform,
            limit=_to_int(args.get("limit") or 50, 50),
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "memory_delete":
        scope_value = _resolve_memory_scope(args, origin)
        return memory_delete(
            keys=args.get("keys"),
            scope=scope_value,
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
        return memory_search(
            str(args.get("query") or ""),
            scope=str(args.get("scope") or "auto"),
            user_id=args.get("user_id"),
            room_id=args.get("room_id"),
            platform=args.get("platform") or platform,
            limit=_to_int(args.get("limit") or 8, 8),
            min_score=_to_int(args.get("min_score") or 1, 1),
            origin=args.get("origin") if isinstance(args.get("origin"), dict) else origin,
        )
    if func == "memory_show":
        return _memory_show_user(args, platform, origin)
    if func == "memory_show_room":
        return _memory_show_room(args, platform, origin)
    if func == "memory_forget_key":
        return _memory_forget_key(args, platform, origin)
    if func == "memory_forget_all":
        return _memory_forget_all(args, platform, origin)

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


def _required_arg_names(plugin: Any) -> list[str]:
    raw = getattr(plugin, "required_args", None)
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        text = str(item or "").strip().lower()
        if text:
            out.append(text)
    return out


def _has_non_empty_arg(args: Dict[str, Any], key: str) -> bool:
    if key not in args:
        return False
    value = args.get(key)
    if isinstance(value, str):
        return bool(value.strip())
    if value is None:
        return False
    return True


def _autofill_textual_required_args(plugin: Any, args: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    If planner omitted required text-like args (query/prompt/etc), copy the user's
    request text from context so execution can proceed instead of looping on
    clarification.
    """
    request_text = _extract_request_text(context).strip()
    if not request_text:
        return args

    required = _required_arg_names(plugin)
    if not required:
        return args

    text_keys = {"query", "request", "prompt", "text", "content", "message"}
    for key in required:
        if key not in text_keys:
            continue
        if _has_non_empty_arg(args, key):
            continue
        args[key] = request_text
    return args


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
    args = _autofill_textual_required_args(plugin, args, context)

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
