import hashlib
import json
from typing import Any, Callable, Dict, Optional, Sequence


def compact_agent_state_json(
    state: Optional[Dict[str, Any]],
    *,
    fallback_goal: str,
    limit: int,
    normalize_agent_state_fn: Callable[[Optional[Dict[str, Any]], str], Dict[str, Any]],
    short_text_fn: Callable[..., str],
) -> str:
    del limit, short_text_fn
    full_state = normalize_agent_state_fn(state, fallback_goal=fallback_goal)
    try:
        return json.dumps(full_state, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return "{}"


def agent_state_prompt_message(
    state: Optional[Dict[str, Any]],
    *,
    fallback_goal: str,
    prompt_max_chars: int,
    compact_agent_state_json_fn: Callable[..., str] = compact_agent_state_json,
) -> str:
    payload = compact_agent_state_json_fn(
        state,
        fallback_goal=fallback_goal,
        limit=prompt_max_chars,
    )
    return "Current agent state (full JSON):\n" + payload


def agent_state_hash(
    state: Optional[Dict[str, Any]],
    *,
    fallback_goal: str,
    ledger_max_chars: int,
    compact_agent_state_json_fn: Callable[..., str] = compact_agent_state_json,
) -> str:
    payload = compact_agent_state_json_fn(
        state,
        fallback_goal=fallback_goal,
        limit=ledger_max_chars,
    )
    digest = hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()
    return f"sha256:{digest}"


def agent_state_key(
    *,
    platform: str,
    scope: str,
    normalize_platform_fn: Callable[[str], str],
    clean_scope_text_fn: Callable[[Any], str],
    scope_is_generic_fn: Callable[[str], bool],
    unknown_scope_fn: Callable[[str, Optional[Dict[str, Any]]], str],
    agent_state_key_prefix: str,
) -> str:
    normalized_platform = normalize_platform_fn(platform)
    normalized_scope = clean_scope_text_fn(scope)
    if not normalized_scope or scope_is_generic_fn(normalized_scope):
        normalized_scope = unknown_scope_fn(normalized_platform, {"platform": normalized_platform})
    return f"{agent_state_key_prefix}{normalized_platform}:{normalized_scope}"


def has_required_agent_state_keys(
    state: Any,
    *,
    required_keys: Sequence[str],
) -> bool:
    if not isinstance(state, dict):
        return False
    for key in required_keys:
        if key not in state:
            return False
    return True


def load_persistent_agent_state(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
    normalize_platform_fn: Callable[[str], str],
    clean_scope_text_fn: Callable[[Any], str],
    scope_is_generic_fn: Callable[[str], bool],
    unknown_scope_fn: Callable[[str, Optional[Dict[str, Any]]], str],
    agent_state_key_prefix: str,
    coerce_text_fn: Callable[[Any], str],
    first_json_object_fn: Callable[[str], Optional[Dict[str, Any]]],
    normalize_agent_state_fn: Callable[[Optional[Dict[str, Any]], str], Dict[str, Any]],
    required_keys: Sequence[str],
) -> Optional[Dict[str, Any]]:
    if redis_client is None:
        return None
    key = agent_state_key(
        platform=platform,
        scope=scope,
        normalize_platform_fn=normalize_platform_fn,
        clean_scope_text_fn=clean_scope_text_fn,
        scope_is_generic_fn=scope_is_generic_fn,
        unknown_scope_fn=unknown_scope_fn,
        agent_state_key_prefix=agent_state_key_prefix,
    )
    try:
        raw = redis_client.get(key)
    except Exception:
        return None
    text = coerce_text_fn(raw).strip()
    if not text:
        return None
    parsed = first_json_object_fn(text)
    if not has_required_agent_state_keys(parsed, required_keys=required_keys):
        return None
    normalized = normalize_agent_state_fn(parsed, fallback_goal=str(parsed.get("goal") or ""))
    if not str(normalized.get("goal") or "").strip():
        return None
    if not has_required_agent_state_keys(normalized, required_keys=required_keys):
        return None
    try:
        json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return None
    return normalized


def save_persistent_agent_state(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
    state: Optional[Dict[str, Any]],
    normalize_platform_fn: Callable[[str], str],
    clean_scope_text_fn: Callable[[Any], str],
    scope_is_generic_fn: Callable[[str], bool],
    unknown_scope_fn: Callable[[str, Optional[Dict[str, Any]]], str],
    agent_state_key_prefix: str,
    configured_agent_state_ttl_seconds_fn: Callable[[Any], int],
    normalize_agent_state_fn: Callable[[Optional[Dict[str, Any]], str], Dict[str, Any]],
    required_keys: Sequence[str],
) -> None:
    if redis_client is None or not isinstance(state, dict):
        return
    key = agent_state_key(
        platform=platform,
        scope=scope,
        normalize_platform_fn=normalize_platform_fn,
        clean_scope_text_fn=clean_scope_text_fn,
        scope_is_generic_fn=scope_is_generic_fn,
        unknown_scope_fn=unknown_scope_fn,
        agent_state_key_prefix=agent_state_key_prefix,
    )
    ttl_seconds = configured_agent_state_ttl_seconds_fn(redis_client)
    normalized = normalize_agent_state_fn(state, fallback_goal=str(state.get("goal") or ""))
    if not has_required_agent_state_keys(normalized, required_keys=required_keys):
        return
    if not str(normalized.get("goal") or "").strip():
        return
    try:
        payload = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return
    if not payload or payload in {"{}", "null"}:
        return
    try:
        if ttl_seconds > 0:
            try:
                redis_client.set(key, payload, ex=ttl_seconds)
            except TypeError:
                redis_client.set(key, payload)
                if hasattr(redis_client, "expire"):
                    redis_client.expire(key, ttl_seconds)
        else:
            redis_client.set(key, payload)
    except Exception:
        return
