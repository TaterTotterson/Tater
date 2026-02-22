import re
from typing import Any, Callable, Dict, List, Optional


def contains_tool_json_pattern(text: str) -> bool:
    raw = str(text or "")
    if not raw:
        return False
    if re.search(
        r"\{[^{}]*\"function\"\s*:\s*\"[^\"]+\"[^{}]*\"arguments\"\s*:\s*\{",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        return True
    return bool(
        re.search(
            r"\bfunction\b\s*[:=]\s*['\"][^'\"]+['\"].{0,120}\barguments\b\s*[:=]\s*\{",
            raw,
            flags=re.IGNORECASE | re.DOTALL,
        )
    )


def sanitize_platform_preamble(
    platform: str,
    platform_preamble: Any,
    *,
    coerce_text_fn: Callable[[Any], str],
    ascii_only_platforms: set[str],
    looks_like_tool_markup_fn: Callable[[str], bool],
    parse_strict_tool_json_fn: Callable[[str], Optional[Dict[str, Any]]],
    parse_function_json_fn: Callable[[Any], Any],
    contains_tool_json_pattern_fn: Callable[[str], bool],
) -> str:
    text = coerce_text_fn(platform_preamble).strip()
    if not text:
        return ""
    text = text[:900].strip()
    if platform in ascii_only_platforms:
        text = text.encode("ascii", "ignore").decode().strip()
    if not text:
        return ""
    if looks_like_tool_markup_fn(text):
        return ""
    if parse_strict_tool_json_fn(text) is not None:
        return ""
    if parse_function_json_fn(text):
        return ""
    if contains_tool_json_pattern_fn(text):
        return ""
    return text


def with_platform_preamble(
    messages: List[Dict[str, Any]],
    *,
    platform_preamble: str,
) -> List[Dict[str, Any]]:
    if not platform_preamble:
        return list(messages)
    out = list(messages)
    insert_at = 1 if out and out[0].get("role") == "system" else 0
    out.insert(insert_at, {"role": "system", "content": platform_preamble})
    return out
