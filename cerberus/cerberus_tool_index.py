from typing import Any, Callable, Dict, List, Optional


def tool_purpose(
    plugin: Any,
    *,
    plugin_when_to_use_fn: Callable[[Any], Any],
) -> str:
    def _meta_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (list, tuple, set)):
            parts = [str(item).strip() for item in value if str(item).strip()]
            return " ".join(parts).strip()
        return str(value).strip()

    text = ""
    try:
        text = _meta_text(plugin_when_to_use_fn(plugin))
    except Exception:
        text = ""
    if not text:
        text = _meta_text(getattr(plugin, "description", "") or "")
    if not text:
        text = _meta_text(getattr(plugin, "verba_dec", "") or "")
    text = " ".join(text.split())
    if not text:
        return "no description"
    if len(text) > 80:
        text = text[:77].rstrip() + "..."
    return text


def kernel_tool_purpose(
    tool_id: str,
    *,
    kernel_tool_purpose_hints: Dict[str, str],
) -> str:
    text = kernel_tool_purpose_hints.get(str(tool_id or "").strip(), "")
    if text:
        return text
    fallback = str(tool_id or "").strip().replace("_", " ")
    return fallback or "kernel tool"


def ordered_kernel_tool_ids(
    *,
    meta_tools: Dict[str, Any],
) -> List[str]:
    return sorted(str(tool_id) for tool_id in meta_tools)


def _usage_text(value: Any, fallback_tool_id: str) -> str:
    text = str(value or "").strip()
    if text:
        return " ".join(text.split())
    tool_id = str(fallback_tool_id or "").strip()
    if not tool_id:
        return '{"function":"","arguments":{}}'
    return f'{{"function":"{tool_id}","arguments":{{}}}}'


def enabled_tool_mini_index(
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    ordered_kernel_tool_ids_fn: Callable[[], List[str]],
    kernel_tool_purpose_fn: Callable[[str], str],
    kernel_tool_usage_fn: Callable[[str], str],
    plugin_supports_platform_fn: Callable[[Any, str], bool],
    plugin_usage_text_fn: Callable[[Any], str],
    tool_purpose_fn: Callable[[Any], str],
) -> str:
    kernel_rows: List[str] = []
    for tool_id in ordered_kernel_tool_ids_fn():
        description = kernel_tool_purpose_fn(tool_id)
        usage = _usage_text(kernel_tool_usage_fn(tool_id), tool_id)
        kernel_rows.append(f"- id: {tool_id} | description: {description} | usage: {usage}")
    if not kernel_rows:
        kernel_rows.append("- (none)")

    enabled_check = enabled_predicate or (lambda _name: True)
    plugin_rows: List[str] = []
    for plugin_id, plugin in sorted(registry.items(), key=lambda kv: str(kv[0]).lower()):
        if not enabled_check(plugin_id):
            continue
        if not plugin_supports_platform_fn(plugin, platform):
            continue
        description = tool_purpose_fn(plugin)
        usage = _usage_text(plugin_usage_text_fn(plugin), plugin_id)
        plugin_rows.append(f"- id: {plugin_id} | description: {description} | usage: {usage}")
    if not plugin_rows:
        plugin_rows.append("- (none)")

    return (
        "Kernel tools:\n"
        + "\n".join(kernel_rows)
        + "\nEnabled plugin tools on this platform:\n"
        + "\n".join(plugin_rows)
    )
