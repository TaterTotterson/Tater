import hashlib
import json
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from notify.queue import load_default_targets, normalize_platform, queue_key

_PLATFORM_ORDER: Tuple[str, ...] = (
    "discord",
    "irc",
    "matrix",
    "telegram",
    "macos",
    "homeassistant",
    "ntfy",
    "webui",
    "wordpress",
)

_PLATFORM_META: Dict[str, Dict[str, Any]] = {
    "discord": {
        "label": "Discord",
        "requires_target": True,
        "fields": ("guild_id", "guild_name", "channel_id", "channel"),
    },
    "irc": {
        "label": "IRC",
        "requires_target": True,
        "fields": ("channel",),
    },
    "matrix": {
        "label": "Matrix",
        "requires_target": True,
        "fields": ("room_id", "room_alias", "channel"),
    },
    "telegram": {
        "label": "Telegram",
        "requires_target": True,
        "fields": ("chat_id", "channel"),
    },
    "macos": {
        "label": "macOS",
        "requires_target": True,
        "fields": ("scope", "device_id"),
    },
    "homeassistant": {
        "label": "Home Assistant",
        "requires_target": False,
        "fields": ("device_service", "persistent", "api_notification"),
    },
    "ntfy": {
        "label": "ntfy",
        "requires_target": False,
        "fields": ("topic", "server"),
    },
    "webui": {
        "label": "WebUI",
        "requires_target": False,
        "fields": (),
    },
    "wordpress": {
        "label": "WordPress",
        "requires_target": False,
        "fields": ("site_url", "post_status", "category_id"),
    },
}

_MAX_RECENT_QUEUE_ITEMS = 250
_MAX_RECENT_KEYS = 160
_ROOM_LABEL_PREFIX = "tater:room_label"
_ROOM_META_PREFIX = "tater:room_meta"


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="ignore").strip()
    return str(value).strip()


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _scan_keys(redis_client: Any, pattern: str, *, max_keys: int) -> List[str]:
    if redis_client is None:
        return []
    out: List[str] = []
    try:
        iterator = redis_client.scan_iter(match=str(pattern))
    except Exception:
        return []
    for raw in iterator:
        key = _to_text(raw)
        if not key:
            continue
        out.append(key)
        if len(out) >= int(max_keys):
            break
    out.sort()
    return out


def _room_meta_key(platform: str, room_id: Any) -> str:
    normalized = normalize_platform(platform)
    scope_id = _to_text(room_id)
    if not normalized or not scope_id:
        return ""
    return f"{_ROOM_META_PREFIX}:{normalized}:{scope_id}"


def _load_room_meta(redis_client: Any, platform: str, room_id: Any) -> Dict[str, str]:
    if redis_client is None:
        return {}
    key = _room_meta_key(platform, room_id)
    if not key:
        return {}
    try:
        raw = redis_client.get(key)
    except Exception:
        return {}
    text = _to_text(raw)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: Dict[str, str] = {}
    for raw_key, raw_value in parsed.items():
        key_text = _to_text(raw_key)
        value_text = _to_text(raw_value)
        if not key_text or not value_text:
            continue
        out[key_text] = value_text
    return out


def _targets_key(targets: Dict[str, Any]) -> str:
    clean = {str(k): _to_text(v) for k, v in (targets or {}).items() if _to_text(v)}
    payload = json.dumps(clean, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()


def _normalize_targets_for_platform(platform: str, raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    allowed = set(_PLATFORM_META.get(platform, {}).get("fields") or ())
    out: Dict[str, str] = {}
    for key, value in raw.items():
        token = str(key or "").strip()
        if not token:
            continue
        if allowed and token not in allowed:
            continue
        text = _to_text(value)
        if not text:
            continue
        out[token] = text
    return out


def _discord_label(targets: Dict[str, str]) -> str:
    channel = _to_text(targets.get("channel") or targets.get("channel_id"))
    guild = _to_text(targets.get("guild_id"))
    guild_name = _to_text(targets.get("guild_name"))
    if channel and guild_name:
        return f"{channel} • {guild_name}"
    if channel and guild:
        return f"{channel} • guild {guild}"
    if channel:
        return channel
    if guild_name:
        return guild_name
    if guild:
        return f"guild {guild}"
    return "Discord target"


def _platform_target_label(platform: str, targets: Dict[str, str]) -> str:
    if platform == "discord":
        return _discord_label(targets)
    if platform == "irc":
        return _to_text(targets.get("channel")) or "IRC channel"
    if platform == "matrix":
        return _to_text(targets.get("room_alias") or targets.get("room_id") or targets.get("channel")) or "Matrix room"
    if platform == "telegram":
        channel = _to_text(targets.get("channel"))
        chat_id = _to_text(targets.get("chat_id"))
        if channel and chat_id and channel != chat_id:
            return f"{channel} • {chat_id}"
        return channel or chat_id or "Telegram chat"
    if platform == "macos":
        scope = _to_text(targets.get("scope"))
        device = _to_text(targets.get("device_id"))
        if scope and device:
            return f"{scope} • {device}"
        return scope or device or "macOS target"
    if platform == "homeassistant":
        target = _to_text(targets.get("device_service"))
        return target or "Home Assistant defaults"
    if platform == "ntfy":
        topic = _to_text(targets.get("topic"))
        server = _to_text(targets.get("server"))
        if topic and server:
            return f"{topic} @ {server}"
        return topic or server or "ntfy target"
    if platform == "wordpress":
        return _to_text(targets.get("site_url")) or "WordPress site"
    if platform == "webui":
        return "WebUI chat"
    return "Destination"


def _discord_destination_groups(destinations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    ungrouped: List[Dict[str, Any]] = []
    for row in destinations:
        targets = row.get("targets") if isinstance(row, dict) else {}
        if not isinstance(targets, dict):
            targets = {}
        guild_id = _to_text(targets.get("guild_id"))
        if not guild_id:
            ungrouped.append(row)
            continue
        guild_name = _to_text(targets.get("guild_name"))
        group = grouped.get(guild_id)
        if not isinstance(group, dict):
            group = {
                "id": f"guild:{guild_id}",
                "label": f"{guild_name} ({guild_id})" if guild_name else f"Guild {guild_id}",
                "guild_id": guild_id,
                "guild_name": guild_name,
                "destinations": [],
            }
            grouped[guild_id] = group
        elif guild_name and not _to_text(group.get("guild_name")):
            group["guild_name"] = guild_name
            group["label"] = f"{guild_name} ({guild_id})"
        group["destinations"].append(row)

    out = sorted(grouped.values(), key=lambda row: _to_text(row.get("label")).lower())
    if ungrouped:
        out.append(
            {
                "id": "ungrouped",
                "label": "Other Discord Targets",
                "destinations": ungrouped,
            }
        )
    return out


def _append_candidate(
    *,
    platform: str,
    targets: Dict[str, str],
    source: str,
    out: List[Dict[str, Any]],
    seen: set[str],
) -> None:
    clean = _normalize_targets_for_platform(platform, targets)
    if _PLATFORM_META.get(platform, {}).get("requires_target") and not clean:
        return
    if not clean and platform != "webui":
        return
    key = _targets_key(clean)
    if key in seen:
        return
    seen.add(key)
    out.append(
        {
            "id": key,
            "label": _platform_target_label(platform, clean),
            "targets": clean,
            "source": str(source or "inferred"),
        }
    )


def _default_targets_for_platform(platform: str, redis_client: Any) -> Dict[str, str]:
    try:
        raw_defaults = load_default_targets(platform, redis_client)
    except Exception:
        raw_defaults = {}
    defaults = _normalize_targets_for_platform(platform, raw_defaults)
    if platform == "macos":
        scope = ""
        device_id = ""
        if redis_client is not None:
            try:
                scope = _to_text(redis_client.get("tater:macos:last_scope"))
            except Exception:
                scope = ""
            try:
                device_id = _to_text(redis_client.get("tater:macos:last_device_id"))
            except Exception:
                device_id = ""
        if scope and "scope" not in defaults:
            defaults["scope"] = scope
        if device_id and "device_id" not in defaults:
            defaults["device_id"] = device_id
    elif platform == "homeassistant":
        settings = {}
        if redis_client is not None:
            try:
                settings = redis_client.hgetall("verba_settings:Home Assistant Notifier") or {}
            except Exception:
                settings = {}
        device_service = _to_text((settings or {}).get("DEFAULT_DEVICE_SERVICE"))
        if device_service:
            defaults["device_service"] = device_service
    elif platform == "ntfy":
        settings = {}
        if redis_client is not None:
            try:
                settings = redis_client.hgetall("verba_settings:NTFY Notifier") or {}
            except Exception:
                settings = {}
        topic = _to_text((settings or {}).get("ntfy_topic"))
        server = _to_text((settings or {}).get("ntfy_server"))
        if topic:
            defaults["topic"] = topic
        if server:
            defaults["server"] = server
    elif platform == "wordpress":
        settings = {}
        if redis_client is not None:
            try:
                settings = redis_client.hgetall("verba_settings:WordPress Poster") or {}
            except Exception:
                settings = {}
        site_url = _to_text((settings or {}).get("wordpress_site_url"))
        post_status = _to_text((settings or {}).get("post_status"))
        category_id = _to_text((settings or {}).get("category_id"))
        if site_url:
            defaults["site_url"] = site_url
        if post_status:
            defaults["post_status"] = post_status
        if category_id:
            defaults["category_id"] = category_id
    return defaults


def _recent_queue_targets_for_platform(platform: str, redis_client: Any, *, max_items: int) -> List[Dict[str, str]]:
    key = queue_key(platform)
    if not key or redis_client is None:
        return []
    limit = max(1, min(_MAX_RECENT_QUEUE_ITEMS, int(max_items)))
    try:
        raw_rows = redis_client.lrange(key, -limit, -1) or []
    except Exception:
        return []
    out: List[Dict[str, str]] = []
    for raw in reversed(raw_rows):
        text = _to_text(raw)
        if not text:
            continue
        try:
            parsed = json.loads(text)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        targets = _normalize_targets_for_platform(platform, parsed.get("targets"))
        if targets:
            out.append(targets)
    return out


def _recent_history_targets_for_platform(platform: str, redis_client: Any) -> List[Dict[str, str]]:
    if redis_client is None:
        return []

    out: List[Dict[str, str]] = []
    if platform == "discord":
        patterns = ("tater:discord:*:history", "tater:channel:*:history")
        for pattern in patterns:
            for key in _scan_keys(redis_client, pattern, max_keys=_MAX_RECENT_KEYS):
                token = key.rsplit(":history", 1)[0].split(":")[-1]
                token = _to_text(token)
                if not token:
                    continue
                if token.isdigit():
                    out.append({"channel_id": token})
                elif pattern != "tater:channel:*:history":
                    out.append({"channel": token})
    elif platform == "irc":
        patterns = ("tater:irc:*:history", "tater:channel:*:history")
        for pattern in patterns:
            for key in _scan_keys(redis_client, pattern, max_keys=_MAX_RECENT_KEYS):
                token = key.rsplit(":history", 1)[0].split(":")[-1]
                token = _to_text(token)
                if not token:
                    continue
                if pattern == "tater:channel:*:history" and token.isdigit():
                    continue
                channel = token if token.startswith("#") else f"#{token}"
                out.append({"channel": channel})
    elif platform == "telegram":
        for key in _scan_keys(redis_client, "tater:telegram:*:history", max_keys=_MAX_RECENT_KEYS):
            token = key.split("tater:telegram:", 1)[-1]
            token = token.rsplit(":history", 1)[0]
            token = _to_text(token)
            if token:
                out.append({"chat_id": token})
    elif platform == "matrix":
        for key in _scan_keys(redis_client, "tater:matrix:*:history", max_keys=_MAX_RECENT_KEYS):
            token = key.split("tater:matrix:", 1)[-1]
            token = token.rsplit(":history", 1)[0]
            token = _to_text(token)
            if token:
                out.append({"room_id": token})
    return out


def _room_label_targets_for_platform(platform: str, redis_client: Any) -> List[Dict[str, str]]:
    if redis_client is None:
        return []
    normalized = normalize_platform(platform)
    if normalized not in {"discord", "irc", "telegram"}:
        return []

    pattern = f"{_ROOM_LABEL_PREFIX}:{normalized}:*"
    out: List[Dict[str, str]] = []
    for key in _scan_keys(redis_client, pattern, max_keys=_MAX_RECENT_KEYS):
        room_id = key.split(f"{_ROOM_LABEL_PREFIX}:{normalized}:", 1)[-1]
        room_id = _to_text(room_id)
        if not room_id:
            continue
        try:
            label_raw = redis_client.get(key)
        except Exception:
            label_raw = ""
        label = _to_text(label_raw)
        targets: Dict[str, str] = {}
        if normalized == "discord":
            meta = _load_room_meta(redis_client, normalized, room_id)
            if room_id.isdigit():
                targets["channel_id"] = room_id
            guild_id = _to_text(meta.get("guild_id"))
            if guild_id.isdigit():
                targets["guild_id"] = guild_id
            guild_name = _to_text(meta.get("guild_name"))
            if guild_name:
                targets["guild_name"] = guild_name
            if label.startswith("#"):
                targets["channel"] = label
        elif normalized == "irc":
            if label.startswith("#"):
                targets["channel"] = label
            elif room_id.startswith("#"):
                targets["channel"] = room_id
        elif normalized == "telegram":
            if room_id:
                targets["chat_id"] = room_id
            if label:
                targets["channel"] = label
        if targets:
            out.append(targets)
    return out


def _platform_entry(
    *,
    platform: str,
    redis_client: Any,
    limit: int,
) -> Dict[str, Any]:
    meta = _PLATFORM_META.get(platform, {})
    out_rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    source_tokens: set[str] = set()

    defaults = _default_targets_for_platform(platform, redis_client)
    if defaults or platform == "webui":
        _append_candidate(
            platform=platform,
            targets=defaults,
            source="default",
            out=out_rows,
            seen=seen,
        )
        source_tokens.add("default")

    for targets in _recent_queue_targets_for_platform(platform, redis_client, max_items=limit):
        _append_candidate(
            platform=platform,
            targets=targets,
            source="recent_queue",
            out=out_rows,
            seen=seen,
        )
        source_tokens.add("recent_queue")
        if len(out_rows) >= int(limit):
            break

    if len(out_rows) < int(limit):
        for targets in _recent_history_targets_for_platform(platform, redis_client):
            _append_candidate(
                platform=platform,
                targets=targets,
                source="recent_history",
                out=out_rows,
                seen=seen,
            )
            source_tokens.add("recent_history")
            if len(out_rows) >= int(limit):
                break

    if len(out_rows) < int(limit):
        for targets in _room_label_targets_for_platform(platform, redis_client):
            _append_candidate(
                platform=platform,
                targets=targets,
                source="room_label",
                out=out_rows,
                seen=seen,
            )
            source_tokens.add("room_label")
            if len(out_rows) >= int(limit):
                break

    discovery_sources = sorted(token for token in source_tokens if token)
    supports_live_discovery = bool({"recent_queue", "recent_history"} & set(discovery_sources))
    destination_groups: List[Dict[str, Any]] = []
    if platform == "discord":
        destination_groups = _discord_destination_groups(out_rows[: int(limit)])

    return {
        "platform": platform,
        "label": _to_text(meta.get("label") or platform.title()),
        "requires_target": bool(meta.get("requires_target")),
        "fields": [str(field) for field in (meta.get("fields") or ())],
        "destinations": out_rows[: int(limit)],
        "destination_groups": destination_groups,
        "discovery_sources": discovery_sources,
        "supports_live_discovery": supports_live_discovery,
    }


def notifier_destination_catalog(
    *,
    redis_client: Any,
    platform: Optional[str] = None,
    limit: int = 80,
) -> Dict[str, Any]:
    normalized = normalize_platform(platform) if platform is not None else ""
    limit_value = max(1, min(500, _to_int(limit, 80)))
    targets: Sequence[str]
    if normalized:
        targets = (normalized,)
    else:
        targets = _PLATFORM_ORDER

    rows: List[Dict[str, Any]] = []
    for plat in targets:
        if plat not in _PLATFORM_META:
            continue
        rows.append(
            _platform_entry(
                platform=plat,
                redis_client=redis_client,
                limit=limit_value,
            )
        )

    return {
        "generated_at": float(time.time()),
        "platforms": rows,
    }
