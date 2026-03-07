import time
import uuid
import re
from typing import Any, Dict, Optional, Tuple

ALLOWED_PLATFORMS = ("discord", "irc", "matrix", "homeassistant", "ntfy", "telegram", "macos")

QUEUE_KEYS = {
    "discord": "notifyq:discord",
    "irc": "notifyq:irc",
    "matrix": "notifyq:matrix",
    "homeassistant": "notifyq:homeassistant",
    "ntfy": "notifyq:ntfy",
    "telegram": "notifyq:telegram",
    "macos": "notifyq:macos",
}

_DEFAULT_SETTINGS = {
    "discord": {
        "category": "Discord Notifier",
        "fields": {
            "channel_id": "DEFAULT_CHANNEL_ID",
        },
    },
    "irc": {
        "category": "IRC Notifier",
        "fields": {
            "channel": "DEFAULT_CHANNEL",
        },
    },
    "matrix": {
        "category": "Matrix Notifier",
        "fields": {
            "room_id": "DEFAULT_ROOM_ID",
        },
    },
    "telegram": {
        "category": "Telegram Notifier",
        "fields": {
            "chat_id": "DEFAULT_CHAT_ID",
        },
    },
}


_PLATFORM_ALIASES = {
    "home assistant": "homeassistant",
    "home-assistant": "homeassistant",
    "home_assistant": "homeassistant",
    "ha": "homeassistant",
    "mac os": "macos",
    "mac-os": "macos",
    "mac_os": "macos",
    "macosx": "macos",
    "osx": "macos",
    "my mac": "macos",
    "my macs": "macos",
    "my macos": "macos",
    "this mac": "macos",
    "this macs": "macos",
    "current mac": "macos",
    "current macs": "macos",
    "mac": "macos",
    "macs": "macos",
}


def normalize_platform(platform: Optional[str]) -> str:
    raw = str(platform or "").strip().lower().strip(" .,!?:;\"'")
    if not raw:
        return ""
    if raw in _PLATFORM_ALIASES:
        return _PLATFORM_ALIASES[raw]
    squashed = " ".join(raw.replace("-", " ").replace("_", " ").split())
    return _PLATFORM_ALIASES.get(squashed, squashed)


def queue_key(platform: str) -> Optional[str]:
    return QUEUE_KEYS.get(normalize_platform(platform))


def normalize_origin(origin: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(origin, dict):
        origin = {}

    cleaned = {k: v for k, v in origin.items() if v not in (None, "")}
    if not cleaned.get("platform"):
        cleaned["platform"] = "automation"
    return cleaned


def normalize_meta(meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(meta, dict):
        meta = {}

    priority = str(meta.get("priority") or "normal").lower()
    if priority not in ("normal", "high"):
        priority = "normal"

    tags = meta.get("tags") or []
    if not isinstance(tags, list):
        tags = [str(tags)]

    ttl_sec = meta.get("ttl_sec") or 0
    try:
        ttl_sec = int(ttl_sec)
    except Exception:
        ttl_sec = 0

    return {
        "priority": priority,
        "tags": [str(t) for t in tags if str(t).strip()],
        "ttl_sec": ttl_sec,
    }


def load_default_targets(platform: str, redis_client) -> Dict[str, str]:
    info = _DEFAULT_SETTINGS.get(normalize_platform(platform))
    if not info:
        return {}

    settings = redis_client.hgetall(f"plugin_settings:{info['category']}") or {}
    defaults: Dict[str, str] = {}
    for target_key, setting_key in info.get("fields", {}).items():
        val = settings.get(setting_key)
        if val:
            defaults[target_key] = val
    return defaults


def _coerce_targets(targets: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(targets, dict):
        return {}
    return {k: v for k, v in targets.items() if v not in (None, "")}


_HASHTAG_TOKEN_RE = re.compile(r"#([A-Za-z0-9][A-Za-z0-9._-]{0,99})")
_AT_TOKEN_RE = re.compile(r"@([A-Za-z0-9_]{1,64})")


def _cleanup_room_ref(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        text = text[1:-1].strip()
    text = " ".join(text.split())
    return text


def _extract_channel_name(raw: Any) -> str:
    text = _cleanup_room_ref(raw)
    if not text:
        return ""
    match = _HASHTAG_TOKEN_RE.search(text)
    if match:
        return f"#{match.group(1)}"
    return text


def _extract_chat_ref(raw: Any) -> str:
    text = _cleanup_room_ref(raw)
    if not text:
        return ""
    match = _AT_TOKEN_RE.search(text)
    if match:
        return f"@{match.group(1)}"
    match = _HASHTAG_TOKEN_RE.search(text)
    if match:
        return match.group(1)
    if text.startswith("-") and text[1:].isdigit():
        return text
    if text.isdigit():
        return text
    if " " not in text and not text.startswith("@"):
        # Telegram channel/usernames are often provided without "@". Accept that shorthand.
        return f"@{text}"
    return text


def resolve_targets(
    platform: str,
    targets: Optional[Dict[str, Any]],
    origin: Optional[Dict[str, Any]],
    defaults: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    platform = normalize_platform(platform)
    resolved = _coerce_targets(targets)
    defaults = defaults or {}
    origin = normalize_origin(origin)

    if platform == "discord":
        # Tolerate non-numeric channel_id values and treat them as channel names.
        channel_id = str(resolved.get("channel_id") or "").strip()
        if channel_id and not channel_id.isdigit():
            resolved.pop("channel_id", None)
            if not resolved.get("channel"):
                resolved["channel"] = channel_id

        if not resolved.get("channel_id") and not resolved.get("channel"):
            if origin.get("platform") == "discord":
                if origin.get("channel_id"):
                    resolved["channel_id"] = origin.get("channel_id")
                elif origin.get("channel"):
                    resolved["channel"] = origin.get("channel")
            elif defaults.get("channel_id"):
                resolved["channel_id"] = defaults["channel_id"]
            elif defaults.get("channel"):
                resolved["channel"] = defaults["channel"]

        if not (resolved.get("channel_id") or resolved.get("channel")):
            return None, "Cannot queue: missing target channel/room"

        if resolved.get("channel"):
            channel = _extract_channel_name(resolved.get("channel"))
            if channel and not channel.startswith("#"):
                resolved["channel"] = f"#{channel}"
            elif channel:
                resolved["channel"] = channel

        if not resolved.get("guild_id"):
            if origin.get("platform") == "discord" and origin.get("guild_id"):
                resolved["guild_id"] = origin.get("guild_id")
            elif defaults.get("guild_id"):
                resolved["guild_id"] = defaults["guild_id"]

    elif platform == "irc":
        if not resolved.get("channel"):
            if origin.get("platform") == "irc" and origin.get("channel"):
                resolved["channel"] = origin.get("channel")
            elif defaults.get("channel"):
                resolved["channel"] = defaults["channel"]

        if not resolved.get("channel"):
            return None, "Cannot queue: missing target channel/room"

        channel = _extract_channel_name(resolved.get("channel"))
        if channel and not channel.startswith("#"):
            resolved["channel"] = f"#{channel}"
        elif channel:
            resolved["channel"] = channel

    elif platform == "matrix":
        if not resolved.get("room_id"):
            if origin.get("platform") == "matrix" and origin.get("room_id"):
                resolved["room_id"] = origin.get("room_id")
            elif defaults.get("room_id"):
                resolved["room_id"] = defaults["room_id"]

        if not resolved.get("room_id"):
            return None, "Cannot queue: missing target channel/room"

    elif platform == "telegram":
        if resolved.get("chat_id"):
            resolved["chat_id"] = _extract_chat_ref(resolved.get("chat_id"))

        if not resolved.get("chat_id"):
            if origin.get("platform") == "telegram" and origin.get("chat_id"):
                resolved["chat_id"] = origin.get("chat_id")
            elif defaults.get("chat_id"):
                resolved["chat_id"] = defaults["chat_id"]

        if not resolved.get("chat_id"):
            return None, "Cannot queue: missing target channel/room"

    elif platform == "macos":
        if not resolved.get("scope"):
            if origin.get("platform") == "macos" and origin.get("scope"):
                resolved["scope"] = origin.get("scope")

        if not resolved.get("device_id"):
            if origin.get("platform") == "macos" and origin.get("device_id"):
                resolved["device_id"] = origin.get("device_id")

        if not (resolved.get("scope") or resolved.get("device_id")):
            return None, "Cannot queue: missing target device/scope"

    elif platform == "homeassistant":
        # No required target for persistent notifications
        pass

    return resolved, None


def build_queue_item(
    platform: str,
    title: Optional[str],
    message: str,
    targets: Optional[Dict[str, Any]],
    origin: Optional[Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "created_at": float(time.time()),
        "platform": normalize_platform(platform),
        "kind": "notify",
        "message": str(message).strip(),
        "targets": _coerce_targets(targets),
        "origin": normalize_origin(origin),
        "meta": normalize_meta(meta),
    }

    if title is not None and str(title).strip():
        item["title"] = str(title).strip()

    return item


def is_expired(item: Dict[str, Any], now: Optional[float] = None) -> bool:
    meta = item.get("meta") or {}
    ttl_sec = meta.get("ttl_sec") or 0
    try:
        ttl_sec = int(ttl_sec)
    except Exception:
        ttl_sec = 0
    if ttl_sec <= 0:
        return False
    created_at = item.get("created_at")
    try:
        created_at = float(created_at)
    except Exception:
        return False
    now_ts = float(now) if now is not None else time.time()
    return now_ts > (created_at + ttl_sec)
