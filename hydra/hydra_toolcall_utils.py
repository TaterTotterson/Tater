import re
from difflib import SequenceMatcher
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


def normalize_tool_call_for_user_request(
    *,
    tool_call: Dict[str, Any],
    registry: Dict[str, Any],
    user_text: str,
    canonical_tool_name_fn: Callable[[str], str],
    parse_function_json_fn: Callable[[Any], Any],
) -> Dict[str, Any]:
    call = tool_call if isinstance(tool_call, dict) else {}
    func = canonical_tool_name_fn(str(call.get("function") or "").strip())
    args = call.get("arguments")
    if not isinstance(args, dict):
        args = {}
    normalized_args = dict(args)

    plugin = registry.get(func) if isinstance(registry, dict) else None
    if plugin is not None:
        normalized_args = _normalize_plugin_args(
            plugin=plugin,
            args=normalized_args,
            user_text=str(user_text or ""),
            parse_function_json_fn=parse_function_json_fn,
        )
    if func == "send_message":
        normalized_args = _normalize_send_message_args(
            args=normalized_args,
            user_text=str(user_text or ""),
        )

    return {"function": func, "arguments": normalized_args}


_TEXTUAL_ARG_KEYS = {"query", "request", "prompt", "text", "content", "message"}
_SEND_MESSAGE_TARGET_KEYS = (
    "channel_id",
    "channel",
    "guild_id",
    "room_id",
    "room_alias",
    "chat_id",
    "scope",
    "device_id",
)
_SEND_MESSAGE_PLATFORM_ALIASES = {
    "home assistant": "homeassistant",
    "home-assistant": "homeassistant",
    "home_assistant": "homeassistant",
    "web ui": "webui",
    "web-ui": "webui",
    "web_ui": "webui",
    "mac os": "macos",
    "mac-os": "macos",
    "mac_os": "macos",
    "my mac": "macos",
}


def _normalize_send_message_platform(value: Any) -> str:
    raw = str(value or "").strip().lower().strip(" .,!?:;\"'")
    if not raw:
        return ""
    squashed = " ".join(raw.replace("-", " ").replace("_", " ").split())
    if raw in _SEND_MESSAGE_PLATFORM_ALIASES:
        return _SEND_MESSAGE_PLATFORM_ALIASES[raw]
    if squashed in _SEND_MESSAGE_PLATFORM_ALIASES:
        return _SEND_MESSAGE_PLATFORM_ALIASES[squashed]
    return squashed


def _send_message_has_target(args: Dict[str, Any]) -> bool:
    if not isinstance(args, dict):
        return False
    targets = args.get("targets")
    if isinstance(targets, dict):
        for key in _SEND_MESSAGE_TARGET_KEYS:
            if str(targets.get(key) or "").strip():
                return True
    for key in _SEND_MESSAGE_TARGET_KEYS:
        if str(args.get(key) or "").strip():
            return True
    return False


def _normalize_send_message_args(*, args: Dict[str, Any], user_text: str) -> Dict[str, Any]:
    del user_text
    normalized = dict(args or {})

    platform = _normalize_send_message_platform(normalized.get("platform"))
    if platform:
        normalized["platform"] = platform

    targets = dict(normalized.get("targets") or {}) if isinstance(normalized.get("targets"), dict) else {}
    for key in _SEND_MESSAGE_TARGET_KEYS:
        if str(targets.get(key) or "").strip():
            continue
        value = normalized.get(key)
        if value in (None, ""):
            continue
        targets[key] = value
    if targets or _send_message_has_target(normalized):
        normalized["targets"] = targets
    return normalized


def _required_arg_names(plugin: Any) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()

    schema = getattr(plugin, "argument_schema", None)
    if isinstance(schema, dict):
        required = schema.get("required")
        if isinstance(required, list):
            for item in required:
                key = str(item or "").strip()
                lowered = key.lower()
                if not key or lowered in seen:
                    continue
                seen.add(lowered)
                out.append(key)

    return out


def _usage_arguments(plugin: Any, parse_function_json_fn: Callable[[Any], Any]) -> Dict[str, Any]:
    usage = str(getattr(plugin, "usage", "") or "").strip()
    if not usage:
        return {}
    parsed = parse_function_json_fn(usage)
    if not isinstance(parsed, dict):
        return {}
    args = parsed.get("arguments")
    return args if isinstance(args, dict) else {}


def _parse_enum_candidates_from_usage_value(value: Any) -> List[str]:
    if isinstance(value, list):
        out = []
        for item in value:
            token = str(item or "").strip()
            if token and token not in out:
                out.append(token)
        return out
    if isinstance(value, dict):
        enum_values = value.get("enum")
        if isinstance(enum_values, list):
            out = []
            for item in enum_values:
                token = str(item or "").strip()
                if token and token not in out:
                    out.append(token)
            return out
        return []
    if not isinstance(value, str):
        return []
    text = value.strip()
    if "|" not in text:
        return []
    out: List[str] = []
    seen: set[str] = set()
    for chunk in text.split("|"):
        piece = str(chunk or "").strip()
        if not piece:
            continue
        match = re.match(r"^[A-Za-z0-9_.:-]+", piece)
        token = match.group(0) if match else ""
        lowered = token.lower()
        if not token or lowered in seen:
            continue
        seen.add(lowered)
        out.append(token)
    return out


def _enum_candidates_by_arg(
    plugin: Any,
    parse_function_json_fn: Callable[[Any], Any],
) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}

    schema = getattr(plugin, "argument_schema", None)
    if isinstance(schema, dict):
        props = schema.get("properties")
        if isinstance(props, dict):
            for key, meta in props.items():
                if not isinstance(meta, dict):
                    continue
                enum_values = meta.get("enum")
                if not isinstance(enum_values, list):
                    continue
                values: List[str] = []
                seen: set[str] = set()
                for item in enum_values:
                    token = str(item or "").strip()
                    lowered = token.lower()
                    if not token or lowered in seen:
                        continue
                    seen.add(lowered)
                    values.append(token)
                if values:
                    out[str(key).strip()] = values

    usage_args = _usage_arguments(plugin, parse_function_json_fn)
    for key, value in usage_args.items():
        parsed_values = _parse_enum_candidates_from_usage_value(value)
        if len(parsed_values) >= 2 and str(key).strip() not in out:
            out[str(key).strip()] = parsed_values

    return out


def _textual_context_blob(args: Dict[str, Any], user_text: str) -> str:
    chunks: List[str] = []
    user = str(user_text or "").strip().lower()
    if user:
        chunks.append(user)
    for key, value in (args or {}).items():
        if str(key or "").strip().lower() not in _TEXTUAL_ARG_KEYS:
            continue
        if isinstance(value, str) and value.strip():
            chunks.append(value.strip().lower())
    return " ".join(chunks).strip()


def _candidate_score(
    candidate: str,
    *,
    context: str,
    arg_key: str,
    raw_value: str,
) -> float:
    cand = str(candidate or "").strip().lower()
    if not cand:
        return -1.0

    score = 0.0
    context_text = str(context or "").strip().lower()
    canonical_phrase = cand.replace("_", " ").replace("-", " ")

    if context_text and re.search(rf"\b{re.escape(canonical_phrase)}\b", context_text):
        score += 8.0

    tokens = [t for t in re.split(r"[_\-\s]+", cand) if t]
    for token in tokens:
        if not context_text:
            continue
        if re.search(rf"\b{re.escape(token)}s?\b", context_text):
            score += 2.0

    key = str(arg_key or "").strip().lower()
    if key in {"action", "op", "operation"} and context_text:
        if cand.startswith("find") and any(
            marker in context_text
            for marker in ("ip address", "mac address", "where is", "lookup", "look up", "search", "find")
        ):
            score += 3.0
        if cand.startswith("list") and any(
            marker in context_text
            for marker in ("list", "show", "online", "offline", "all")
        ):
            score += 2.0

    raw = str(raw_value or "").strip().lower()
    if raw:
        score += SequenceMatcher(None, raw, cand).ratio()
        if cand == raw:
            score += 10.0
        if cand.startswith(f"{raw}_") or cand.startswith(f"{raw}-"):
            score += 2.5
        if raw in cand:
            score += 0.5

    return score


def _coerce_enum_arg_value(
    *,
    arg_key: str,
    raw_value: Any,
    candidates: List[str],
    context: str,
) -> Any:
    if not candidates:
        return raw_value

    raw = str(raw_value or "").strip()
    if not raw:
        return raw_value

    for candidate in candidates:
        if raw.lower() == str(candidate or "").strip().lower():
            return candidate

    scored: List[tuple[float, str]] = []
    for candidate in candidates:
        score = _candidate_score(
            candidate,
            context=context,
            arg_key=arg_key,
            raw_value=raw,
        )
        scored.append((score, candidate))
    scored.sort(key=lambda item: item[0], reverse=True)
    if not scored:
        return raw_value

    top_score = scored[0][0]
    top_candidates = [cand for score, cand in scored if score == top_score]
    if top_score <= 0.75:
        return raw_value
    if len(top_candidates) > 1 and top_score < 4.0:
        return raw_value
    return scored[0][1]


def _fill_missing_text_args(
    *,
    args: Dict[str, Any],
    required_arg_names: List[str],
    user_text: str,
) -> Dict[str, Any]:
    text = str(user_text or "").strip()
    if not text:
        return args

    out = dict(args)
    for key, value in list(out.items()):
        lowered = str(key or "").strip().lower()
        if lowered not in _TEXTUAL_ARG_KEYS:
            continue
        if isinstance(value, str) and not value.strip():
            out[key] = text

    existing_lower = {str(k).strip().lower() for k in out.keys()}
    for key in required_arg_names:
        lowered = str(key or "").strip().lower()
        if lowered not in _TEXTUAL_ARG_KEYS:
            continue
        if lowered in existing_lower:
            continue
        out[key] = text
        existing_lower.add(lowered)
    return out


def _normalize_plugin_args(
    *,
    plugin: Any,
    args: Dict[str, Any],
    user_text: str,
    parse_function_json_fn: Callable[[Any], Any],
) -> Dict[str, Any]:
    normalized = dict(args or {})
    required = _required_arg_names(plugin)
    enum_map = _enum_candidates_by_arg(plugin, parse_function_json_fn)
    usage_args = _usage_arguments(plugin, parse_function_json_fn)
    context = _textual_context_blob(normalized, user_text)
    arg_key_lookup = {str(k).strip().lower(): k for k in normalized.keys()}

    _rewrite_action_to_call_service_if_needed(
        args=normalized,
        enum_map=enum_map,
        usage_args=usage_args,
    )

    for key, candidates in enum_map.items():
        source_key = arg_key_lookup.get(str(key).strip().lower(), key)
        raw_val = normalized.get(source_key)
        if raw_val is None:
            continue
        normalized[source_key] = _coerce_enum_arg_value(
            arg_key=source_key,
            raw_value=raw_val,
            candidates=candidates,
            context=context,
        )

    normalized = _fill_missing_text_args(
        args=normalized,
        required_arg_names=required,
        user_text=user_text,
    )
    return normalized


def _rewrite_action_to_call_service_if_needed(
    *,
    args: Dict[str, Any],
    enum_map: Dict[str, List[str]],
    usage_args: Dict[str, Any],
) -> None:
    if not isinstance(args, dict):
        return

    action_key = ""
    action_candidates: List[str] = []
    for key, candidates in (enum_map or {}).items():
        lowered = str(key or "").strip().lower()
        if lowered in {"action", "op", "operation"}:
            action_key = key
            action_candidates = list(candidates or [])
            break
    if not action_key or not action_candidates:
        return

    call_service_candidate = ""
    candidate_lookup = {str(item or "").strip().lower(): str(item or "").strip() for item in action_candidates}
    for preferred in ("call_service", "service_call"):
        if preferred in candidate_lookup:
            call_service_candidate = candidate_lookup[preferred]
            break
    if not call_service_candidate:
        return

    source_action_key = ""
    for key in args.keys():
        if str(key or "").strip().lower() == str(action_key or "").strip().lower():
            source_action_key = key
            break
    if not source_action_key:
        return

    raw_action = str(args.get(source_action_key) or "").strip()
    if not raw_action:
        return
    if raw_action.lower() in candidate_lookup:
        return

    usage_keys_lower = {str(k or "").strip().lower() for k in (usage_args or {}).keys()}
    if "service" not in usage_keys_lower and "service" not in {str(k).strip().lower() for k in args.keys()}:
        return

    raw_service = str(args.get("service") or "").strip()
    if raw_service:
        return

    if not re.fullmatch(r"[a-zA-Z0-9_]+", raw_action):
        return
    if raw_action.lower() in {"list", "help", "unknown"}:
        return

    args["service"] = raw_action.lower()
    args[source_action_key] = call_service_candidate


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
