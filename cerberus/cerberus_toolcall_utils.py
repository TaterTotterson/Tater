import re
from typing import Any, Callable, Dict, List, Optional


def canonical_tool_name(name: str, *, tool_name_aliases: Dict[str, str]) -> str:
    raw = str(name or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    alias = tool_name_aliases.get(lowered)
    if alias:
        return alias
    return lowered


def plugin_usage_argument_keys(
    plugin: Any,
    *,
    parse_function_json_fn: Callable[[Any], Any],
) -> List[str]:
    usage = str(getattr(plugin, "usage", "") or "").strip()
    if not usage:
        return []
    parsed = parse_function_json_fn(usage)
    if not isinstance(parsed, dict):
        return []
    args = parsed.get("arguments")
    if not isinstance(args, dict):
        return []
    out: List[str] = []
    for key in args.keys():
        k = str(key or "").strip()
        if k and k not in out:
            out.append(k)
    return out


def plugin_required_argument_keys(plugin: Any) -> List[str]:
    raw_required = getattr(plugin, "required_args", [])
    if not isinstance(raw_required, list):
        return []
    out: List[str] = []
    for item in raw_required:
        if isinstance(item, dict):
            key = str(item.get("name") or "").strip()
        else:
            key = str(item or "").strip()
        if not key or key in out:
            continue
        out.append(key)
    return out


def normalize_tool_call_for_user_request(
    *,
    tool_call: Dict[str, Any],
    registry: Dict[str, Any],
    user_text: str,
    canonical_tool_name_fn: Callable[[str], str],
    apply_full_user_request_requirement_fn: Callable[[Any, Dict[str, Any], str], Dict[str, Any]],
    tool_call_route_metadata_fn: Callable[[Optional[Dict[str, Any]]], Dict[str, Any]],
) -> Dict[str, Any]:
    call = tool_call if isinstance(tool_call, dict) else {}
    func = canonical_tool_name_fn(str(call.get("function") or "").strip())
    args = call.get("arguments")
    if not isinstance(args, dict):
        args = {}
    plugin_obj = registry.get(func)
    normalized_args = apply_full_user_request_requirement_fn(
        plugin_obj,
        dict(args),
        user_text,
    )
    normalized_call: Dict[str, Any] = {"function": func, "arguments": normalized_args}
    normalized_call.update(tool_call_route_metadata_fn(call))
    return normalized_call


def plugin_tool_id_for_call(
    tool_call: Optional[Dict[str, Any]],
    registry: Dict[str, Any],
    *,
    canonical_tool_name_fn: Callable[[str], str],
    is_meta_tool_fn: Callable[[str], bool],
) -> str:
    if not isinstance(tool_call, dict):
        return ""
    func = canonical_tool_name_fn(str(tool_call.get("function") or "").strip())
    if not func or is_meta_tool_fn(func):
        return ""
    if func not in registry:
        return ""
    return func


def looks_like_invalid_tool_call_text(text: str) -> bool:
    s = str(text or "").strip()
    if not s:
        return False
    lower = s.lower()
    if re.search(r"['\"]function['\"]\s*:", s, flags=re.IGNORECASE) and re.search(
        r"['\"]arguments['\"]\s*:", s, flags=re.IGNORECASE
    ):
        return True
    if s.startswith("{") and re.search(r"\bfunction\b\s*:", lower) and re.search(r"\barguments\b\s*:", lower):
        return True
    return False


def is_tool_candidate(
    text: str,
    *,
    parse_strict_tool_json_fn: Callable[[str], Optional[Dict[str, Any]]],
    parse_function_json_fn: Callable[[Any], Any],
    looks_like_tool_markup_fn: Callable[[str], bool],
    looks_like_invalid_tool_call_text_fn: Callable[[str], bool],
) -> bool:
    if parse_strict_tool_json_fn(text) is not None:
        return True
    if parse_function_json_fn(text):
        return True
    if looks_like_tool_markup_fn(text):
        return True
    if looks_like_invalid_tool_call_text_fn(text):
        return True
    return False
