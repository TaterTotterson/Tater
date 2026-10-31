from __future__ import annotations

import asyncio
import inspect
import logging
import re
import threading
import time
import uuid
from typing import Any, Dict
from urllib.parse import urlparse


_lock = threading.RLock()
_sessions: Dict[str, Dict[str, Any]] = {}
_provider_handlers: Dict[str, Any] = {}
_SCREEN_VIDEO_CAPABILITY = "screen_video_playback"
logger = logging.getLogger("tater.display_video")


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _canonical_selector(value: Any) -> str:
    token = _text(value)
    if token.lower().startswith("voice_core:"):
        token = token.split(":", 1)[1].strip()
    if token and not token.lower().startswith(("native:", "host:", "integration:", "ha:")):
        token = f"native:{token}"
    return token


def _normalized_phrase(value: Any) -> str:
    return " ".join(part for part in re.split(r"[^a-z0-9]+", _text(value).casefold()) if part)


def _validated_url(value: Any) -> str:
    raw = _text(value)
    if not raw or len(raw) > 4096 or any(ord(char) <= 32 or ord(char) == 127 for char in raw):
        raise ValueError("A valid direct video URL is required.")
    parsed = urlparse(raw)
    if parsed.scheme.lower() not in {"http", "https", "rtsp", "rtsps"} or not parsed.netloc:
        raise ValueError("Video playback supports direct HTTP(S), HLS, RTSP, and RTSPS URLs.")
    return raw


def _clamped_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = default
    return max(minimum, min(maximum, parsed))


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled", ""}:
        return False
    return default


def _client_rows() -> Dict[str, Dict[str, Any]]:
    from . import native_satellite

    snapshot = native_satellite.status_snapshot_sync()
    clients = snapshot.get("clients") if isinstance(snapshot, dict) else {}
    return clients if isinstance(clients, dict) else {}


def _client_room(selector: Any) -> str:
    token = _canonical_selector(selector)
    for raw_selector, raw in _client_rows().items():
        if _canonical_selector(raw_selector) != token:
            continue
        client = raw if isinstance(raw, dict) else {}
        return _text(client.get("room") or client.get("area"))
    return ""


def _integration_video_rows() -> list[Dict[str, Any]]:
    try:
        from announcement_targets import integration_target_value
        from integration_registry import get_integration_devices_by_capability

        devices = get_integration_devices_by_capability("media_player")
    except Exception:
        return []

    rows: list[Dict[str, Any]] = []
    for raw in devices or []:
        if not isinstance(raw, dict):
            continue
        integration_id = _text(raw.get("integration_id")).lower()
        device_id = _text(raw.get("id") or raw.get("device_id") or raw.get("ref"))
        if device_id.lower().startswith("media_player:"):
            device_id = device_id.split(":", 1)[1]
        if not integration_id or not device_id:
            continue
        state = _text(raw.get("status") or raw.get("state")).lower()
        if state in {"offline", "unavailable", "unknown", "disconnected"}:
            continue
        actions = {_text(value).lower() for value in raw.get("actions") or [] if _text(value)}
        features = {_text(value).lower() for value in raw.get("features") or [] if _text(value)}
        capabilities = {
            _text(value).lower() for value in raw.get("capabilities") or [] if _text(value)
        }
        if not ({"play_media", "play_url"} & (actions | features | capabilities)):
            continue
        details = raw.get("details") if isinstance(raw.get("details"), dict) else {}
        device_type = _text(raw.get("type")).lower()
        cast_type = _text(details.get("cast_type")).lower()
        text_hint = " ".join(
            _text(value).casefold()
            for value in (
                raw.get("name"), raw.get("display_name"), raw.get("reported_name"),
                details.get("device_class"), details.get("model"), details.get("manufacturer"),
                details.get("homeassistant_platform"),
            )
            if _text(value)
        )
        if integration_id == "google_cast":
            video_capable = device_type != "speaker" and cast_type not in {"audio", "group"}
        else:
            video_capable = bool(
                {"tv", "television", "video", "video_output"}
                & (capabilities | features | {device_type, _text(details.get("device_class")).lower()})
            ) or any(
                marker in text_hint
                for marker in (
                    "android tv", "apple tv", "chromecast", "fire tv", "google tv",
                    "jellyfin", "plex", "roku", "shield tv", "television",
                )
            ) or bool(re.search(r"(?:^|\s)tv(?:\s|$)", text_hint))
        if not video_capable:
            continue
        selector = integration_target_value(integration_id, device_id)
        if not selector:
            continue
        aliases = raw.get("aliases") if isinstance(raw.get("aliases"), (list, tuple, set)) else []
        rows.append(
            {
                "selector": selector,
                "name": _text(raw.get("name") or raw.get("display_name") or device_id),
                "room": _text(raw.get("room") or raw.get("area")),
                "kind": "integration",
                "integration_id": integration_id,
                "device_id": device_id,
                "aliases": [_text(value) for value in aliases if _text(value)],
                "transcode_profile": "echo_show_720p",
                "output_container": "hls",
                "media_type": "application/vnd.apple.mpegurl",
                "width": 1280,
                "height": 720,
            }
        )
    return rows


def target_catalog() -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for selector, raw in _client_rows().items():
        client = raw if isinstance(raw, dict) else {}
        capabilities = client.get("capabilities") if isinstance(client.get("capabilities"), dict) else {}
        if not bool(client.get("connected")) or not bool(capabilities.get(_SCREEN_VIDEO_CAPABILITY)):
            continue
        rows.append(
            {
                "selector": _canonical_selector(selector),
                "name": _text(client.get("device_name") or client.get("name") or selector),
                "room": _text(client.get("room")),
                "kind": "native",
                "aliases": [],
                "transcode_profile": _text(capabilities.get("screen_video_transcode_profile")) or "echo_show_720p",
                "output_container": "mpegts",
                "media_type": "video/mp2t",
                "width": _clamped_int(capabilities.get("screen_video_width"), 1280, 1, 4096),
                "height": _clamped_int(capabilities.get("screen_video_height"), 720, 1, 4096),
            }
        )
    rows.extend(_integration_video_rows())
    rows.sort(
        key=lambda row: (
            row["room"].casefold(),
            0 if row.get("kind") == "native" else 1,
            row["name"].casefold(),
            row["selector"],
        )
    )
    return rows


def match_target(value: Any) -> str:
    requested = _text(value)
    if not requested:
        return ""
    direct = _canonical_selector(requested)
    catalog = target_catalog()
    if direct and any(row["selector"] == direct for row in catalog):
        return direct

    query = _normalized_phrase(requested)
    matches = []
    for row in catalog:
        aliases = {
            row["name"],
            row["room"],
            row["selector"],
            row["selector"].split(":", 1)[-1],
            *(row.get("aliases") or []),
        }
        normalized = {_normalized_phrase(alias) for alias in aliases if _text(alias)}
        if any(query == alias or query in alias or alias in query for alias in normalized):
            matches.append(row["selector"])
    matches = list(dict.fromkeys(matches))
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"More than one video display matched {requested!r}; use its full device or room name.")
    return ""


def match_request_target(value: Any) -> str:
    """Resolve only a destination explicitly expressed in a natural-language request."""
    request = _normalized_phrase(value)
    if not request:
        return ""
    matches: list[str] = []
    device_words = r"(?:tv|television|screen|display|satellite|show|spot)"
    for row in target_catalog():
        room = _normalized_phrase(row.get("room"))
        names = {
            _normalized_phrase(row.get("name")),
            *(_normalized_phrase(alias) for alias in row.get("aliases") or []),
        }
        patterns: list[str] = []
        if room:
            escaped_room = re.escape(room)
            patterns.extend(
                [
                    rf"\b(?:in|on|to|at)\s+(?:the\s+)?{escaped_room}(?:\s+{device_words})?\b",
                    rf"\b{escaped_room}\s+{device_words}\b",
                ]
            )
        for name in names:
            if not name:
                continue
            escaped_name = re.escape(name)
            patterns.append(rf"\b(?:on|to|using)\s+(?:the\s+)?{escaped_name}\b")
            if re.search(rf"\b{device_words}\b", name):
                patterns.append(rf"\b{escaped_name}\b")
        if any(re.search(pattern, request) for pattern in patterns):
            matches.append(row["selector"])
    matches = list(dict.fromkeys(matches))
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError("More than one video destination was named; use one full display or room name.")
    return ""


def _preferred_room_target(room: str, catalog: list[Dict[str, Any]]) -> str:
    try:
        from integration_registry import get_integration_room_preferred_media_player

        preference = get_integration_room_preferred_media_player(room)
    except Exception:
        preference = {}
    requested = _text((preference or {}).get("target"))
    if not requested:
        return ""
    if requested.lower().startswith("ha:"):
        try:
            from announcement_targets import integration_target_value

            requested = integration_target_value("homeassistant", requested.split(":", 1)[1])
        except Exception:
            return ""
    direct = _canonical_selector(requested)
    for row in catalog:
        if row.get("kind") != "native" and row.get("selector") == direct:
            return direct
    return ""


def resolve_target(value: Any = "", *, current_selector: Any = "") -> str:
    requested = _text(value)
    if requested:
        matched = match_target(requested)
        if matched:
            return matched
        raise ValueError(f"No connected screen or compatible TV matched {requested!r}.")

    catalog = target_catalog()
    current = _canonical_selector(current_selector)
    if current and any(row["selector"] == current for row in catalog):
        return current
    room = _client_room(current)
    if room:
        room_key = _normalized_phrase(room)
        same_room = [row for row in catalog if _normalized_phrase(row.get("room")) == room_key]
        native = [row for row in same_room if row.get("kind") == "native"]
        if len(native) == 1:
            return native[0]["selector"]
        if len(native) > 1:
            raise ValueError(f"More than one screen satellite is available in {room}; name the display to use.")
        preferred = _preferred_room_target(room, same_room)
        if preferred:
            return preferred
        external = [row for row in same_room if row.get("kind") != "native"]
        if len(external) == 1:
            return external[0]["selector"]
        if len(external) > 1:
            raise ValueError(f"More than one compatible TV is available in {room}; name the TV to use.")
        raise ValueError(f"There is no screen or compatible TV available in {room} to play the video.")
    raise ValueError("There is no screen or compatible TV available to play the video.")


def _target_row(selector: Any) -> Dict[str, Any]:
    token = _canonical_selector(selector)
    for row in target_catalog():
        if row.get("selector") == token:
            return row
    return {}


def target_playback_profile(selector: Any) -> Dict[str, Any]:
    token = resolve_target(selector)
    row = _target_row(token)
    if row:
        return {
            "selector": token,
            "kind": _text(row.get("kind")) or "native",
            "name": _text(row.get("name")) or token,
            "room": _text(row.get("room")),
            "transcode_profile": _text(row.get("transcode_profile")) or "echo_show_720p",
            "output_container": _text(row.get("output_container")) or "mpegts",
            "media_type": _text(row.get("media_type")) or "video/mp2t",
            "width": _clamped_int(row.get("width"), 1280, 1, 4096),
            "height": _clamped_int(row.get("height"), 720, 1, 4096),
        }
    raise ValueError("That video display is no longer connected.")


def register_provider_handler(provider: Any, handler: Any) -> None:
    name = _text(provider).lower()
    if not name or not callable(handler):
        raise ValueError("A video provider name and event handler are required.")
    with _lock:
        _provider_handlers[name] = handler


def _device_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if key not in {"provider", "provider_data", "target_kind"} and not key.startswith("_")
    }


async def _notify_provider(selector: str, event: str, session: Dict[str, Any]) -> None:
    provider = _text(session.get("provider")).lower()
    if not provider:
        return
    with _lock:
        handler = _provider_handlers.get(provider)
    if not callable(handler):
        return
    try:
        result = handler(selector, event, dict(session))
        if inspect.isawaitable(result):
            await result
    except Exception as exc:
        logger.warning("Video provider %s event %s failed: %s", provider, event, exc)


def session_payload(selector: Any) -> Dict[str, Any]:
    token = _canonical_selector(selector)
    with _lock:
        row = _sessions.get(token)
        return _device_payload(row) if isinstance(row, dict) else {"active": False}


async def _send(selector: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    from . import native_satellite

    return await native_satellite.send_command(selector, "display.video", _device_payload(payload))


async def _play_external(selector: str, url: str, payload: Dict[str, Any], media_type: str) -> Dict[str, Any]:
    from media_playback import play_media_url_targets

    result = await asyncio.to_thread(
        play_media_url_targets,
        selector,
        url,
        media_type=media_type or "application/vnd.apple.mpegurl",
        media_content_type="video",
        filename="tater-tube.m3u8" if "mpegurl" in media_type else "tater-tube.ts",
        title=_text(payload.get("title")) or "Tater Tube",
        start_position_seconds=float(payload.get("position_ms") or 0) / 1000.0,
        volume_percent=None,
        timeout_s=60.0,
        respect_reply_playback=False,
        source_owner="display_video",
    )
    if not isinstance(result, dict) or result.get("ok") is not True:
        error = _text((result or {}).get("error")) if isinstance(result, dict) else ""
        raise RuntimeError(error or "The TV could not start video playback.")
    return result


async def _control_external(selector: str, action: str) -> Dict[str, Any]:
    from announcement_targets import parse_integration_target
    from integration_registry import run_integration_device_action

    target = parse_integration_target(selector)
    if not target:
        raise ValueError("That external video target is unavailable.")
    integration_action = "play" if action == "resume" else action
    result = await asyncio.to_thread(
        run_integration_device_action,
        target["integration_id"],
        integration_action,
        target["device_id"],
        {},
    )
    if isinstance(result, dict) and result.get("ok") is False:
        raise RuntimeError(_text(result.get("error")) or f"The TV could not {action} playback.")
    return result if isinstance(result, dict) else {"ok": True}


async def play(
    selector: Any,
    video_url: Any,
    *,
    title: Any = "",
    subtitle: Any = "",
    loop: Any = False,
    live: Any = False,
    fps: Any = 30,
    duration_ms: Any = 0,
    position_ms: Any = 0,
    server_seek: Any = False,
    media_type: Any = "",
    provider: Any = "",
    provider_data: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    token = resolve_target(selector)
    target = _target_row(token)
    url = _validated_url(video_url)
    payload: Dict[str, Any] = {
        "active": True,
        "session_id": uuid.uuid4().hex,
        "url": url,
        "title": _text(title)[:240],
        "subtitle": _text(subtitle)[:240],
        "state": "playing",
        "position_ms": _clamped_int(position_ms, 0, 0, 2_147_483_647),
        "duration_ms": _clamped_int(duration_ms, 0, 0, 2_147_483_647),
        "fps": 24 if _clamped_int(fps, 30, 1, 120) == 24 else 30,
        "loop": _as_bool(loop),
        "live": _as_bool(live),
        "server_seek": _as_bool(server_seek),
        "target_kind": _text(target.get("kind")) or "native",
        "provider": _text(provider).lower(),
        "provider_data": dict(provider_data) if isinstance(provider_data, dict) else {},
        "error": "",
        "updated_at": time.time(),
    }
    with _lock:
        _sessions[token] = payload
    try:
        if payload["target_kind"] == "native":
            result = await _send(token, payload)
        else:
            result = await _play_external(
                token,
                url,
                payload,
                _text(media_type) or _text(target.get("media_type")),
            )
    except Exception as exc:
        payload.update({"active": False, "state": "error", "error": _text(exc)[:240], "updated_at": time.time()})
        with _lock:
            _sessions[token] = payload
        await _notify_provider(token, "error", payload)
        raise
    await _notify_provider(token, "play", payload)
    return {"ok": True, "selector": token, "session": _device_payload(payload), "delivery": result}


async def control(selector: Any, action: Any, *, position_ms: Any = None) -> Dict[str, Any]:
    token = resolve_target(selector)
    command = _text(action).lower()
    if command not in {"pause", "resume", "stop"}:
        raise ValueError("Video action must be play, pause, resume, or stop.")
    with _lock:
        current = dict(_sessions.get(token) or {})
        if not current.get("active"):
            raise ValueError("That display does not have an active video.")
        if position_ms is not None:
            current["position_ms"] = _clamped_int(position_ms, int(current.get("position_ms") or 0), 0, 2_147_483_647)
        current["updated_at"] = time.time()
        if command == "pause":
            current["state"] = "paused"
        elif command == "resume":
            current["state"] = "playing"
            current["error"] = ""
        else:
            current.update({"active": False, "state": "stopped"})
    if _text(current.get("target_kind")) == "integration":
        result = await _control_external(token, command)
    else:
        result = await _send(token, current)
    with _lock:
        _sessions[token] = current
    await _notify_provider(token, command, current)
    return {"ok": True, "selector": token, "action": command, "session": _device_payload(current), "delivery": result}


async def handle_device_action(selector: Any, message_type: Any, payload: Dict[str, Any] | None) -> Dict[str, Any]:
    token = _canonical_selector(selector)
    action = _text(message_type).lower().removeprefix("display.video.")
    values = payload if isinstance(payload, dict) else {}
    with _lock:
        current = dict(_sessions.get(token) or {})
    if not current.get("active") or _text(values.get("session_id")) != _text(current.get("session_id")):
        return {"ok": False, "ignored": True, "reason": "stale_session"}

    position = _clamped_int(values.get("position_ms"), int(current.get("position_ms") or 0), 0, 2_147_483_647)
    if action == "progress":
        current["position_ms"] = position
        current["updated_at"] = time.time()
        with _lock:
            _sessions[token] = current
        await _notify_provider(token, action, current)
        return {"ok": True, "selector": token, "action": action, "session": _device_payload(current)}
    if action in {"pause", "resume"}:
        return await control(token, action, position_ms=position)
    if action in {"stop", "ended", "error"}:
        with _lock:
            current.update({
                "active": False, "state": action, "position_ms": position,
                "error": _text(values.get("error"))[:240] if action == "error" else "",
                "updated_at": time.time(),
            })
            _sessions[token] = current
            stopped = dict(_sessions[token])
        await _send(token, stopped)
        await _notify_provider(token, action, stopped)
        return {"ok": True, "selector": token, "action": action, "session": _device_payload(stopped)}
    return {"ok": False, "ignored": True, "reason": "unsupported_action"}


def play_sync(selector: Any, video_url: Any, **kwargs: Any) -> Dict[str, Any]:
    from . import native_satellite

    return native_satellite.run_on_runtime_loop(play(selector, video_url, **kwargs), timeout=15.0)


def control_sync(selector: Any, action: Any, **kwargs: Any) -> Dict[str, Any]:
    from . import native_satellite

    return native_satellite.run_on_runtime_loop(control(selector, action, **kwargs), timeout=10.0)
