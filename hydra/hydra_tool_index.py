import copy
import json
import re
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


def _usage_arguments(value: Any) -> Dict[str, Any]:
    text = str(value or "").strip()
    if not text:
        return {}
    start = text.find("{")
    if start < 0:
        return {}
    decoder = json.JSONDecoder()
    try:
        parsed, _end = decoder.raw_decode(text[start:])
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    arguments = parsed.get("arguments")
    return dict(arguments) if isinstance(arguments, dict) else {}


def _schema_type_for_example(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    text = str(value or "").strip().lower()
    if any(token in text for token in ("true/false", "boolean", "<bool")):
        return "boolean"
    if any(token in text for token in ("<integer", "<int", "number", "percent")):
        return "integer"
    return "string"


def _schema_property_for_example(value: Any) -> Dict[str, Any]:
    item: Dict[str, Any] = {"type": _schema_type_for_example(value)}
    if isinstance(value, list):
        example = value[0] if value else ""
        item["items"] = {"type": _schema_type_for_example(example)}
    elif isinstance(value, dict):
        properties = {
            str(key): _schema_property_for_example(child)
            for key, child in value.items()
            if str(key).strip()
        }
        item["properties"] = properties
    elif isinstance(value, str):
        description = value.strip().strip("<>").strip()
        if description:
            item["description"] = description
        if "|" in description:
            enum_values = []
            for chunk in description.split("|"):
                match = re.match(r"^[A-Za-z0-9_.:-]+", chunk.strip())
                token = match.group(0) if match else ""
                if token and token.lower() not in {"optional", "required"} and token not in enum_values:
                    enum_values.append(token)
            if len(enum_values) >= 2:
                item["enum"] = enum_values
    return item


def tool_parameters_schema(*, plugin: Any = None, usage: Any = "") -> Dict[str, Any]:
    """Return the canonical JSON Schema used by native tool-capable providers."""
    explicit = getattr(plugin, "argument_schema", None) if plugin is not None else None
    if isinstance(explicit, dict):
        schema = copy.deepcopy(explicit)
        schema.setdefault("type", "object")
        schema.setdefault("properties", {})
        schema.setdefault("required", [])
        return schema

    arguments = _usage_arguments(usage)
    properties: Dict[str, Any] = {}
    required: List[str] = []
    for raw_key, value in arguments.items():
        key = str(raw_key or "").strip()
        if not key or key == "origin":
            continue
        properties[key] = _schema_property_for_example(value)
        if not (isinstance(value, str) and "optional" in value.lower()):
            required.append(key)
    return {
        "type": "object",
        "properties": properties,
        "required": required,
    }


def native_function_tool(
    *,
    tool_id: str,
    description: str,
    usage: Any = "",
    plugin: Any = None,
) -> Dict[str, Any]:
    """Build an OpenAI-compatible function definition shared by all adapters."""
    return {
        "type": "function",
        "function": {
            "name": str(tool_id or "").strip(),
            "description": " ".join(str(description or "").split()).strip() or "Execute this tool.",
            "parameters": tool_parameters_schema(plugin=plugin, usage=usage),
        },
    }


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
        "Enabled verba tools on this platform (use one when it can do the task):\n"
        + "\n".join(plugin_rows)
        + "\nKernel tools (built-ins; terminal console tools provide full terminal console access to run commands when no Verba can do the task):\n"
        + "\n".join(kernel_rows)
    )
