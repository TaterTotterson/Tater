import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple


KNOWN_PLATFORMS: Tuple[str, ...] = (
    "webui",
    "discord",
    "irc",
    "homeassistant",
    "homekit",
    "matrix",
    "telegram",
    "xbmc",
    "automation",
    "rss",
)

_BOTH_EXPANSION: Tuple[str, ...] = (
    "webui",
    "discord",
    "irc",
    "homeassistant",
    "homekit",
    "matrix",
    "telegram",
    "xbmc",
)


def _coerce_attr_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple, set)):
        parts: List[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                parts.append(text)
        return " ".join(parts).strip()
    return str(value).strip()


def normalize_platform(platform: Optional[str]) -> str:
    return (platform or "").strip().lower()


def _normalize_platforms(platforms: Iterable[str]) -> List[str]:
    out: List[str] = []
    for item in platforms or []:
        p = normalize_platform(item)
        if p and p not in out:
            out.append(p)
    return out


def expand_plugin_platforms(platforms: Iterable[str]) -> List[str]:
    raw = _normalize_platforms(platforms)
    expanded: List[str] = []
    for p in raw:
        if p == "both":
            for v in _BOTH_EXPANSION:
                if v not in expanded:
                    expanded.append(v)
            continue
        if p not in expanded:
            expanded.append(p)
    return expanded


def plugin_supports_platform(plugin: Any, platform: str) -> bool:
    p = normalize_platform(platform)
    if not p:
        return False
    supported = expand_plugin_platforms(getattr(plugin, "platforms", []) or [])
    return p in supported


def plugin_display_name(plugin: Any) -> str:
    return (
        _coerce_attr_text(getattr(plugin, "plugin_name", None))
        or _coerce_attr_text(getattr(plugin, "name", None))
        or "Unknown Plugin"
    )


def plugin_when_to_use(plugin: Any) -> str:
    explicit = _coerce_attr_text(getattr(plugin, "when_to_use", None))
    if explicit:
        return explicit
    desc = (
        _coerce_attr_text(getattr(plugin, "description", None))
        or _coerce_attr_text(getattr(plugin, "plugin_dec", None))
    )
    if not desc:
        return "Use this plugin when the user asks for this capability."
    first_sentence = re.split(r"(?<=[.!?])\s+", desc, maxsplit=1)[0].strip()
    return first_sentence or desc


def plugin_how_to_use(plugin: Any) -> str:
    explicit = _coerce_attr_text(getattr(plugin, "how_to_use", None))
    if explicit:
        return explicit
    return "Use usage_example for exact call shape."


def _find_first_json_object(text: str) -> Optional[str]:
    if not text:
        return None
    s = text.strip()
    start = s.find("{")
    if start < 0:
        return None
    depth = 0
    for i in range(start, len(s)):
        ch = s[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return s[start : i + 1]
    return None


def _infer_type(value: Any) -> str:
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
    if isinstance(value, str):
        v = value.strip().lower()
        if "true/false" in v or "bool" in v or "boolean" in v:
            return "boolean"
        if "int" in v or "number" in v or "percent" in v:
            return "number"
        return "string"
    return "string"


def _extract_usage_arguments(plugin: Any) -> Dict[str, Any]:
    usage = _coerce_attr_text(getattr(plugin, "usage", None))
    if not usage:
        return {}

    obj_txt = _find_first_json_object(usage)
    if not obj_txt:
        return {}
    try:
        parsed = json.loads(obj_txt)
    except Exception:
        return {}

    if not isinstance(parsed, dict):
        return {}
    args = parsed.get("arguments")
    return args if isinstance(args, dict) else {}


def _canonical_usage_example(plugin: Any, plugin_id: str) -> str:
    usage = _coerce_attr_text(getattr(plugin, "usage", None))
    obj_txt = _find_first_json_object(usage)
    data: Dict[str, Any] = {}
    if obj_txt:
        try:
            parsed = json.loads(obj_txt)
            if isinstance(parsed, dict):
                data = dict(parsed)
        except Exception:
            data = {}
    if not data:
        data = {"function": plugin_id, "arguments": {}}
    data["function"] = str(plugin_id)
    if not isinstance(data.get("arguments"), dict):
        data["arguments"] = {}
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _required_settings_summary(plugin: Any) -> Dict[str, Dict[str, Any]]:
    raw = getattr(plugin, "required_settings", None)
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for key in sorted(raw.keys()):
        meta = raw.get(key)
        if not isinstance(meta, dict):
            out[str(key)] = {}
            continue
        item: Dict[str, Any] = {}
        for field in ("type", "label", "description", "default"):
            if field in meta and meta.get(field) not in (None, ""):
                item[field] = meta.get(field)
        out[str(key)] = item
    return out


def infer_needs_from_plugin(plugin: Any) -> List[str]:
    explicit = getattr(plugin, "common_needs", None)
    if isinstance(explicit, list) and explicit:
        return [str(x).strip() for x in explicit if str(x).strip()]

    args = _extract_usage_arguments(plugin)
    needs: List[str] = []
    for key in args.keys():
        k = str(key).strip()
        if not k or k == "origin":
            continue
        needs.append(k)
    return needs[:6]


def plugin_arguments_help(plugin: Any) -> Dict[str, List[Dict[str, str]]]:
    if hasattr(plugin, "argument_schema") and isinstance(plugin.argument_schema, dict):
        schema = plugin.argument_schema
        required = schema.get("required", []) if isinstance(schema.get("required"), list) else []
        props = schema.get("properties", {}) if isinstance(schema.get("properties"), dict) else {}
        req_items: List[Dict[str, str]] = []
        opt_items: List[Dict[str, str]] = []

        for key, meta in props.items():
            if not isinstance(meta, dict):
                meta = {}
            item = {
                "name": str(key),
                "type": str(meta.get("type") or "string"),
                "description": str(meta.get("description") or "").strip(),
            }
            if key in required:
                req_items.append(item)
            else:
                opt_items.append(item)
        return {"required": req_items, "optional": opt_items}

    usage_args = _extract_usage_arguments(plugin)
    req_items: List[Dict[str, str]] = []
    opt_items: List[Dict[str, str]] = []

    for key, val in usage_args.items():
        name = str(key)
        if name == "origin":
            continue
        item = {"name": name, "type": _infer_type(val), "description": ""}
        req_items.append(item)

    return {"required": req_items, "optional": opt_items}


def get_plugin_help(
    *,
    plugin_id: str,
    platform: Optional[str],
    registry: Dict[str, Any],
) -> Dict[str, Any]:
    pid = (plugin_id or "").strip()
    plugin = registry.get(pid)
    if not plugin:
        return {
            "tool": "get_plugin_help",
            "ok": False,
            "error": {"code": "unknown_plugin", "message": f"Plugin '{pid}' was not found."},
        }

    usage = _canonical_usage_example(plugin, pid)
    examples = getattr(plugin, "example_calls", None)
    if not isinstance(examples, list) or not examples:
        examples = [usage] if usage else []
    how_to_use = plugin_how_to_use(plugin)

    payload: Dict[str, Any] = {
        "plugin_id": pid,
        "how_to_use": str(how_to_use or ""),
        "usage_example": usage,
        "example_calls": [str(x).strip() for x in examples if str(x).strip()],
    }
    return payload


def list_platforms_for_plugin(
    *,
    plugin_id: str,
    registry: Dict[str, Any],
    known_platforms: Iterable[str] = KNOWN_PLATFORMS,
) -> Dict[str, Any]:
    pid = (plugin_id or "").strip()
    plugin = registry.get(pid)
    if not plugin:
        return {
            "tool": "list_platforms_for_plugin",
            "ok": False,
            "error": {"code": "unknown_plugin", "message": f"Plugin '{pid}' was not found."},
        }

    available_on = expand_plugin_platforms(getattr(plugin, "platforms", []) or [])
    known = _normalize_platforms(known_platforms)
    for p in available_on:
        if p not in known:
            known.append(p)
    not_available_on = [p for p in known if p not in available_on]

    return {
        "tool": "list_platforms_for_plugin",
        "ok": True,
        "plugin_id": pid,
        "plugin_name": plugin_display_name(plugin),
        "available_on": available_on,
        "not_available_on": not_available_on,
    }
