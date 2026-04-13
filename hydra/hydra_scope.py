import hashlib
import json
import re
from typing import Any, Callable, Dict, Optional


GENERIC_SCOPE_TOKENS = {"", "default", "chat", "unknown", "none", "null", "n/a"}


def clean_scope_text(
    value: Any,
    *,
    coerce_text_fn: Callable[[Any], str],
    short_text_fn: Callable[..., str],
) -> str:
    text = coerce_text_fn(value).strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return short_text_fn(text, limit=180)


def scope_is_generic(scope: str) -> bool:
    return str(scope or "").strip().lower() in GENERIC_SCOPE_TOKENS


def unknown_scope(
    platform: str,
    origin: Optional[Dict[str, Any]],
    *,
    normalize_platform_fn: Callable[[str], str],
) -> str:
    source: Dict[str, Any] = {"platform": normalize_platform_fn(platform)}
    if isinstance(origin, dict):
        for key, value in origin.items():
            if value in (None, ""):
                continue
            source[key] = value
    try:
        payload = json.dumps(source, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        payload = str(source)
    base = payload or normalize_platform_fn(platform) or "unknown"
    digest = hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"unknown:{digest}"


def derive_scope_from_origin(
    platform: str,
    origin: Optional[Dict[str, Any]],
    *,
    normalize_platform_fn: Callable[[str], str],
    clean_scope_text_fn: Callable[[Any], str],
    scope_is_generic_fn: Callable[[str], bool],
    unknown_scope_fn: Callable[[str, Optional[Dict[str, Any]]], str],
) -> str:
    src = origin if isinstance(origin, dict) else {}
    normalized_platform = normalize_platform_fn(platform)

    def _v(*keys: str) -> str:
        for key in keys:
            val = clean_scope_text_fn(src.get(key))
            if val and not scope_is_generic_fn(val):
                return val
        return ""

    if normalized_platform == "discord":
        chat_type = _v("chat_type").lower()
        dm_user = _v("dm_user_id")
        if dm_user:
            return f"dm:{dm_user}"
        if chat_type in {"dm", "direct", "direct_message", "private", "private_chat"}:
            user_id = _v("user_id", "author_id", "sender_id")
            if user_id:
                return f"dm:{user_id}"
            dm_chat = _v("chat_id", "channel_id")
            if dm_chat:
                return f"dm:{dm_chat}"
        channel_id = _v("channel_id", "thread_id", "chat_id")
        if channel_id:
            return f"channel:{channel_id}"
    elif normalized_platform == "irc":
        target = _v("target", "channel", "room", "channel_name", "nick", "user")
        if target:
            if target.startswith(("chan:", "pm:")):
                return target
            if target.startswith(("#", "&")):
                return f"chan:{target}"
            if _v("chat_type").lower() in {"pm", "dm", "direct"}:
                return f"pm:{target}"
            return f"pm:{target}"
    elif normalized_platform in {"homeassistant", "voice_core"}:
        device_id = _v("device_id")
        if device_id:
            return f"device:{device_id}"
        area_id = _v("area_id")
        if area_id:
            return f"area:{area_id}"
        session_id = _v("session_id", "conversation_id", "request_id", "source_id")
        if session_id:
            return f"session:{session_id}"
    elif normalized_platform == "webui":
        session_id = _v("session_id", "conversation_id")
        if session_id:
            return f"session:{session_id}"
        user_id = _v("user_id", "user", "username")
        if user_id:
            return f"user:{user_id}"
    elif normalized_platform == "telegram":
        chat_id = _v("chat_id", "room_id")
        if chat_id:
            return f"chat:{chat_id}"
    elif normalized_platform == "matrix":
        room_id = _v("room_id", "chat_id")
        if room_id:
            return f"room:{room_id}"
    elif normalized_platform in {"homekit", "xbmc"}:
        session_id = _v("session_id", "request_id", "device_id", "user_id", "user")
        if session_id:
            return f"session:{session_id}"

    fallback = _v(
        "scope",
        "channel_id",
        "room_id",
        "chat_id",
        "device_id",
        "area_id",
        "session_id",
        "conversation_id",
        "request_id",
        "user_id",
        "user",
    )
    if fallback:
        return fallback
    return unknown_scope_fn(normalized_platform, src)


def resolve_hydra_scope(
    platform: str,
    scope: Any,
    origin: Optional[Dict[str, Any]],
    *,
    normalize_platform_fn: Callable[[str], str],
    clean_scope_text_fn: Callable[[Any], str],
    scope_is_generic_fn: Callable[[str], bool],
    derive_scope_from_origin_fn: Callable[[str, Optional[Dict[str, Any]]], str],
) -> str:
    normalized_platform = normalize_platform_fn(platform)
    raw_scope = clean_scope_text_fn(scope)

    if not raw_scope or scope_is_generic_fn(raw_scope):
        return derive_scope_from_origin_fn(normalized_platform, origin)

    if normalized_platform == "discord":
        if raw_scope.startswith(("channel:", "dm:")):
            return raw_scope
        if clean_scope_text_fn((origin or {}).get("chat_type")).lower() == "dm":
            return f"dm:{raw_scope}"
        return f"channel:{raw_scope}"

    if normalized_platform == "irc":
        if raw_scope.startswith(("chan:", "pm:")):
            return raw_scope
        if raw_scope.startswith(("#", "&")):
            return f"chan:{raw_scope}"
        return f"pm:{raw_scope}"

    if normalized_platform == "telegram":
        return raw_scope if raw_scope.startswith("chat:") else f"chat:{raw_scope}"

    if normalized_platform == "matrix":
        return raw_scope if raw_scope.startswith("room:") else f"room:{raw_scope}"

    if normalized_platform == "webui":
        if raw_scope.startswith(("session:", "user:")):
            return raw_scope
        return f"user:{raw_scope}"

    if normalized_platform in {"homeassistant", "voice_core"}:
        if raw_scope.startswith(("device:", "area:", "session:")):
            return raw_scope
        if ":" in raw_scope:
            return raw_scope
        return f"session:{raw_scope}"

    if normalized_platform in {"homekit", "xbmc"}:
        if raw_scope.startswith("session:"):
            return raw_scope
        return f"session:{raw_scope}"

    return raw_scope
