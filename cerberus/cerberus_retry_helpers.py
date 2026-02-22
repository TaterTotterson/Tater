import hashlib
from typing import Any, Callable, Dict, List, Optional


BAD_ARGS_FAILURE_CODES = {
    "bad_args",
    "invalid_args",
    "invalid_argument",
    "invalid_arguments",
    "missing_args",
    "missing_argument",
    "missing_arguments",
    "missing_required",
    "missing_required_arg",
    "missing_required_argument",
    "missing_required_field",
    "unknown_arg",
    "unknown_args",
    "unknown_argument",
    "unknown_arguments",
    "unknown_field",
    "validation_error",
    "schema_error",
    "type_error",
}


BAD_ARGS_FAILURE_TEXT_MARKERS = (
    "missing required",
    "missing field",
    "required field",
    "required argument",
    "required args",
    "required parameter",
    "missing argument",
    "invalid argument",
    "invalid args",
    "unknown field",
    "unknown argument",
    "unexpected keyword",
    "unexpected argument",
    "validation error",
    "schema validation",
    "failed validation",
    "typeerror",
    "valueerror",
    "keyerror",
    "exception",
)


def tool_failure_code_and_text(
    *,
    tool_result: Optional[Dict[str, Any]],
    payload: Optional[Dict[str, Any]],
) -> tuple[str, str]:
    code = ""
    text_parts: List[str] = []

    src_payload = payload if isinstance(payload, dict) else {}
    payload_error = src_payload.get("error")
    if isinstance(payload_error, dict):
        code = str(payload_error.get("code") or "").strip().lower()
        message = str(payload_error.get("message") or "").strip()
        if message:
            text_parts.append(message)

    src_result = tool_result if isinstance(tool_result, dict) else {}
    data = src_result.get("data")
    if not code and isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            code = str(err.get("code") or "").strip().lower()
            msg = str(err.get("message") or "").strip()
            if msg:
                text_parts.append(msg)

    summary = str(src_result.get("summary_for_user") or "").strip()
    if summary:
        text_parts.append(summary)

    errors = src_result.get("errors")
    if isinstance(errors, list):
        for item in errors:
            line = str(item or "").strip()
            if line:
                text_parts.append(line)

    compact_text = " | ".join(part for part in text_parts if part).strip().lower()
    return code, compact_text


def looks_like_bad_args_plugin_failure(
    *,
    tool_call: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    payload: Optional[Dict[str, Any]],
    registry: Dict[str, Any],
    plugin_tool_id_for_call_fn: Callable[[Optional[Dict[str, Any]], Dict[str, Any]], str],
    bad_args_failure_codes: Optional[set[str]] = None,
    bad_args_failure_text_markers: Optional[tuple[str, ...]] = None,
) -> tuple[bool, str]:
    plugin_id = plugin_tool_id_for_call_fn(tool_call, registry)
    if not plugin_id:
        return False, ""
    if not isinstance(tool_result, dict) or bool(tool_result.get("ok")):
        return False, ""

    codes = bad_args_failure_codes or BAD_ARGS_FAILURE_CODES
    markers = bad_args_failure_text_markers or BAD_ARGS_FAILURE_TEXT_MARKERS
    code, text = tool_failure_code_and_text(tool_result=tool_result, payload=payload)
    if code:
        if code in codes:
            return True, code
        if code.startswith("bad_args"):
            return True, code
        if code in {"plugin_error", "plugin_failed"}:
            if any(marker in text for marker in markers):
                return True, code
    if any(marker in text for marker in markers):
        return True, "bad_args_text"
    return False, ""


def help_arg_names(
    help_payload: Optional[Dict[str, Any]],
    *,
    parse_function_json_fn: Callable[[str], Any],
) -> List[str]:
    src = help_payload if isinstance(help_payload, dict) else {}
    out: List[str] = []
    seen: set[str] = set()

    def _add(name: Any) -> None:
        key = str(name or "").strip()
        lowered = key.lower()
        if not key or lowered in seen:
            return
        seen.add(lowered)
        out.append(key)

    for field in ("required_args", "optional_args"):
        items = src.get(field)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                _add(item.get("name"))
            else:
                _add(item)

    usage_example = str(src.get("usage_example") or "").strip()
    usage_parsed = parse_function_json_fn(usage_example)
    usage_args = usage_parsed.get("arguments") if isinstance(usage_parsed, dict) else None
    if isinstance(usage_args, dict):
        for key in usage_args.keys():
            _add(key)

    return out


def constrain_args_from_plugin_help(
    *,
    args: Optional[Dict[str, Any]],
    help_payload: Optional[Dict[str, Any]],
    help_arg_names_fn: Callable[[Optional[Dict[str, Any]]], List[str]],
) -> Dict[str, Any]:
    source_args = args if isinstance(args, dict) else {}
    allowed = help_arg_names_fn(help_payload)
    if not allowed:
        return {}
    canonical_lookup = {name.lower(): name for name in allowed}
    out: Dict[str, Any] = {}
    for key, value in source_args.items():
        raw_key = str(key or "").strip()
        if not raw_key:
            continue
        canonical = canonical_lookup.get(raw_key.lower())
        if not canonical:
            continue
        out[canonical] = value
    return out


def tool_call_signature(
    tool_call: Optional[Dict[str, Any]],
    *,
    canonical_tool_name_fn: Callable[[Any], str],
    hash_tool_args_fn: Callable[[Any], str],
    tool_call_route_metadata_fn: Callable[[Optional[Dict[str, Any]]], Dict[str, Any]],
    route_user_text_key: str,
) -> str:
    if not isinstance(tool_call, dict):
        return ""
    func = canonical_tool_name_fn(tool_call.get("function"))
    if not func:
        return ""
    args = tool_call.get("arguments") if isinstance(tool_call.get("arguments"), dict) else {}
    args_hash = hash_tool_args_fn(args)
    route_meta = tool_call_route_metadata_fn(tool_call)
    route_text = str(route_meta.get(route_user_text_key) or "").strip()
    route_hash = ""
    if route_text:
        digest = hashlib.sha256(route_text.encode("utf-8", errors="ignore")).hexdigest()
        route_hash = f"route:{digest}"
    base = f"{func}:{args_hash}" if args_hash else func
    if route_hash:
        return f"{base}:{route_hash}"
    return base


def build_help_constrained_retry_tool_call(
    *,
    failed_tool_call: Optional[Dict[str, Any]],
    help_payload: Optional[Dict[str, Any]],
    registry: Dict[str, Any],
    default_user_text: str,
    plugin_tool_id_for_call_fn: Callable[[Optional[Dict[str, Any]], Dict[str, Any]], str],
    constrain_args_from_plugin_help_fn: Callable[[Optional[Dict[str, Any]], Optional[Dict[str, Any]]], Dict[str, Any]],
    tool_call_effective_user_text_fn: Callable[[Optional[Dict[str, Any]], str], str],
    apply_full_user_request_requirement_fn: Callable[[Any, Dict[str, Any], str], Dict[str, Any]],
    tool_call_route_metadata_fn: Callable[[Optional[Dict[str, Any]]], Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    plugin_id = plugin_tool_id_for_call_fn(failed_tool_call, registry)
    if not plugin_id:
        return None
    source_args = (
        failed_tool_call.get("arguments")
        if isinstance(failed_tool_call, dict) and isinstance(failed_tool_call.get("arguments"), dict)
        else {}
    )
    constrained_args = constrain_args_from_plugin_help_fn(source_args, help_payload)
    plugin_obj = registry.get(plugin_id)
    effective_user_text = tool_call_effective_user_text_fn(failed_tool_call, default_user_text)
    normalized_args = apply_full_user_request_requirement_fn(
        plugin_obj,
        constrained_args,
        effective_user_text,
    )
    retry_call: Dict[str, Any] = {"function": plugin_id, "arguments": normalized_args}
    retry_call.update(tool_call_route_metadata_fn(failed_tool_call))
    return retry_call
