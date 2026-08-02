from __future__ import annotations

import base64
import contextlib
import logging
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote

import requests

from announcement_targets import split_announcement_targets
from helpers import redis_client

logger = logging.getLogger("media_playback")

DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS = 360.0
NATIVE_GROUP_START_LEAD_MS = 750
MIXED_SONOS_NATIVE_START_LEAD_MS = 1000
RUNTIME_MEDIA_PROXY_TTL_SECONDS = 8 * 60 * 60

_runtime_media_proxy_lock = threading.RLock()
_runtime_media_proxy_sources: Dict[str, Dict[str, Any]] = {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_float(value: Any, default: float, *, minimum: float | None = None) -> float:
    try:
        result = float(value)
    except Exception:
        result = float(default)
    if minimum is not None:
        result = max(float(minimum), result)
    return result


def _main_app_port() -> int:
    raw = _text(os.getenv("HTMLUI_PORT") or "8501") or "8501"
    try:
        port = int(raw)
    except Exception:
        port = 8501
    if port < 1 or port > 65535:
        port = 8501
    return int(port)


def _voice_core_base_url() -> str:
    return f"http://127.0.0.1:{_main_app_port()}"


def _voice_core_auth_headers() -> Dict[str, str]:
    with contextlib.suppress(Exception):
        settings = redis_client.hgetall("voice_core_settings") or {}
        if isinstance(settings, dict):
            enabled = _text(settings.get("API_AUTH_ENABLED")).lower()
            token = _text(settings.get("API_AUTH_KEY"))
            auth_enabled = enabled in {"1", "true", "yes", "on"} or (bool(token) and enabled == "")
            if auth_enabled and token:
                return {"X-Tater-Token": token}
    return {}


def _runtime_media_source_url(
    audio: bytes | None,
    *,
    content_type: str,
    filename: str,
) -> str:
    payload = bytes(audio or b"")
    if not payload:
        return ""
    try:
        from speech_tts import build_runtime_tts_asset_url, store_runtime_tts_wav

        asset_id = store_runtime_tts_wav(payload, content_type=content_type)
        if not asset_id:
            return ""
        return build_runtime_tts_asset_url(asset_id, filename=Path(_text(filename) or "media.bin").name)
    except Exception as exc:
        logger.warning("[media_playback] failed to store runtime media URL: %s", exc)
        return ""


def _prune_runtime_media_proxy_sources_locked(*, now_ts: float | None = None) -> None:
    now = float(now_ts if now_ts is not None else time.time())
    for asset_id, row in list(_runtime_media_proxy_sources.items()):
        if not isinstance(row, dict) or float(row.get("expires_ts") or 0.0) <= now:
            _runtime_media_proxy_sources.pop(asset_id, None)


def store_runtime_media_proxy_source(
    source_url: str,
    *,
    content_type: str,
    filename: str,
    ttl_s: float = RUNTIME_MEDIA_PROXY_TTL_SECONDS,
) -> str:
    url = _text(source_url)
    if not url:
        return ""
    asset_id = uuid.uuid4().hex
    with _runtime_media_proxy_lock:
        _prune_runtime_media_proxy_sources_locked()
        _runtime_media_proxy_sources[asset_id] = {
            "source_url": url,
            "content_type": _text(content_type) or "application/octet-stream",
            "filename": Path(_text(filename) or "media.bin").name,
            "expires_ts": time.time() + max(300.0, float(ttl_s or RUNTIME_MEDIA_PROXY_TTL_SECONDS)),
        }
    return asset_id


def get_runtime_media_proxy_source(asset_id: Any) -> Dict[str, Any]:
    token = _text(asset_id)
    if not token:
        return {}
    with _runtime_media_proxy_lock:
        _prune_runtime_media_proxy_sources_locked()
        row = _runtime_media_proxy_sources.get(token)
        return dict(row) if isinstance(row, dict) else {}


def _runtime_media_proxy_source_url(
    source_url: str,
    *,
    content_type: str,
    filename: str,
    ttl_s: float = RUNTIME_MEDIA_PROXY_TTL_SECONDS,
) -> str:
    asset_id = store_runtime_media_proxy_source(
        source_url,
        content_type=content_type,
        filename=filename,
        ttl_s=ttl_s,
    )
    if not asset_id:
        return ""
    try:
        from speech_tts import _service_base_url_for_peer

        base_url = _service_base_url_for_peer().rstrip("/")
    except Exception:
        base_url = f"http://127.0.0.1:{_main_app_port()}"
    safe_filename = Path(_text(filename) or "media.bin").name
    return f"{base_url}/api/media/runtime/{asset_id}/{quote(safe_filename)}"


def _voice_core_play_media_sync(
    *,
    selectors: List[str],
    source_url: str,
    audio_bytes: bytes | None = None,
    text: str = "",
    media_type: str = "audio/mpeg",
    media_content_type: str = "music",
    filename: str = "media.mp3",
    volume_percent: int = 100,
    start_position_seconds: float = 0.0,
    start_lead_ms: int = 0,
    timeout_s: float = DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS,
    respect_reply_playback: bool = False,
) -> Dict[str, Any]:
    clean_selectors = [_text(item) for item in list(selectors or []) if _text(item)]
    if not clean_selectors:
        return {"ok": False, "sent_count": 0, "error": "No Voice Core satellites selected."}

    payload_template = {
        "source_url": _text(source_url),
        "text": _text(text),
        "media_type": _text(media_type) or "audio/mpeg",
        "media_content_type": _text(media_content_type) or "music",
        "playback_role": "media",
        "filename": Path(_text(filename) or "media.mp3").name,
        "volume_percent": max(0, min(100, int(_as_float(volume_percent, 100.0)))),
        "start_position_ms": max(
            0,
            int(round(_as_float(start_position_seconds, 0.0, minimum=0.0) * 1000.0)),
        ),
        "timeout_s": _as_float(timeout_s, DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS, minimum=30.0),
        "respect_reply_playback": bool(respect_reply_playback),
    }
    if isinstance(audio_bytes, (bytes, bytearray)) and audio_bytes:
        payload_template["audio_b64"] = base64.b64encode(bytes(audio_bytes)).decode("ascii")

    sent_count = 0
    failures: List[str] = []
    media_session_sent_count = 0
    media_session_fallback_count = 0
    media_session_warnings: List[str] = []
    voice_core_sessions: List[Dict[str, Any]] = []
    base_url = _voice_core_base_url().rstrip("/")
    headers = _voice_core_auth_headers()

    if len(clean_selectors) > 1 or int(start_lead_ms or 0) > 0:
        group_payload = dict(payload_template)
        group_payload["selectors"] = clean_selectors
        group_payload["start_lead_ms"] = max(
            250,
            min(5000, int(start_lead_ms or NATIVE_GROUP_START_LEAD_MS)),
        )
        try:
            response = requests.post(
                f"{base_url}/api/tater/satellite/v1/play-group",
                json=group_payload,
                headers=headers,
                timeout=180,
            )
            response_payload: Dict[str, Any] = {}
            with contextlib.suppress(Exception):
                parsed = response.json()
                if isinstance(parsed, dict):
                    response_payload = parsed
            if response.status_code < 400 and bool(response_payload.get("media_session_started")):
                session_id = _text(response_payload.get("session_id"))
                members = [
                    _text(row.get("selector"))
                    for row in list(response_payload.get("members") or [])
                    if isinstance(row, dict) and _text(row.get("selector"))
                ]
                played_selectors = [
                    _text(selector)
                    for selector in list(response_payload.get("played_selectors") or [])
                    if _text(selector)
                ]
                logical_sent_count = len(played_selectors) if played_selectors else len(clean_selectors)
                result: Dict[str, Any] = {
                    "ok": True,
                    "sent_count": logical_sent_count,
                    "media_session_sent_count": logical_sent_count,
                    "media_session_fallback_count": 0,
                    "synchronized_group": True,
                    "start_lead_ms": int(response_payload.get("start_lead_ms") or group_payload["start_lead_ms"]),
                }
                skipped_destinations = [
                    dict(row)
                    for row in list(response_payload.get("skipped_destinations") or [])
                    if isinstance(row, dict)
                ]
                if skipped_destinations:
                    result["skipped_destinations"] = skipped_destinations
                warnings = [
                    _text(value)
                    for value in list(response_payload.get("warnings") or [])
                    if _text(value)
                ]
                if warnings:
                    result["warnings"] = warnings
                if session_id:
                    result["voice_core_sessions"] = [
                        {
                            "target": _text(response_payload.get("group_id")) or "synchronized-group",
                            "session_id": session_id,
                            "selectors": members or played_selectors or clean_selectors,
                        }
                    ]
                return result
            detail = _text(response_payload.get("detail") or response_payload.get("error"))
            if response.status_code not in {404, 405}:
                return {
                    "ok": False,
                    "sent_count": 0,
                    "error": detail or f"Synchronized satellite playback failed (HTTP {response.status_code}).",
                }
        except Exception as exc:
            return {"ok": False, "sent_count": 0, "error": f"Synchronized satellite playback failed: {exc}"}

    for selector in clean_selectors:
        payload = dict(payload_template)
        payload["selector"] = selector
        try:
            response = requests.post(
                f"{base_url}/api/tater/satellite/v1/play",
                json=payload,
                headers=headers,
                timeout=90,
            )
            if response.status_code < 400:
                sent_count += 1
                response_payload: Dict[str, Any] = {}
                with contextlib.suppress(Exception):
                    parsed = response.json()
                    if isinstance(parsed, dict):
                        response_payload = parsed
                if bool(response_payload.get("media_session_started")):
                    media_session_sent_count += 1
                    message = (
                        response_payload.get("message")
                        if isinstance(response_payload.get("message"), dict)
                        else {}
                    )
                    message_payload = (
                        message.get("payload")
                        if isinstance(message.get("payload"), dict)
                        else {}
                    )
                    session_id = _text(
                        response_payload.get("session_id") or message_payload.get("session_id")
                    )
                    members = [
                        _text(row.get("selector"))
                        for row in list(response_payload.get("members") or [])
                        if isinstance(row, dict) and _text(row.get("selector"))
                    ]
                    if session_id:
                        voice_core_sessions.append(
                            {
                                "target": selector,
                                "session_id": session_id,
                                "selectors": members or [selector],
                            }
                        )
                else:
                    media_session_fallback_count += 1
                    reason = _text(response_payload.get("media_session_fallback_reason"))
                    if reason:
                        media_session_warnings.append(f"{selector} ({reason})")
                continue
            detail = ""
            with contextlib.suppress(Exception):
                parsed = response.json()
                detail = _text(parsed.get("detail"))
            failures.append(f"{selector} ({detail or f'HTTP {response.status_code}'})")
        except Exception as exc:
            failures.append(f"{selector} ({exc})")

    if sent_count:
        result: Dict[str, Any] = {
            "ok": True,
            "sent_count": sent_count,
            "media_session_sent_count": media_session_sent_count,
            "media_session_fallback_count": media_session_fallback_count,
        }
        if voice_core_sessions:
            result["voice_core_sessions"] = voice_core_sessions
        if media_session_warnings:
            result["media_session_warnings"] = media_session_warnings
        if failures:
            result["warnings"] = failures
        return result
    return {"ok": False, "sent_count": 0, "error": "; ".join(failures) or "Voice Core playback failed."}


def _integration_device_playback_action(integration_id: str, device_id: str) -> str:
    try:
        from integration_registry import get_integration_devices_by_capability

        devices = get_integration_devices_by_capability("media_player", redis_client)
    except Exception:
        devices = []

    wanted_integration = _text(integration_id).lower()
    wanted_device = _text(device_id)
    for row in devices if isinstance(devices, list) else []:
        if not isinstance(row, dict):
            continue
        if _text(row.get("integration_id")).lower() != wanted_integration:
            continue
        ids = {_text(row.get("id")), _text(row.get("ref")), _text(row.get("device_id"))}
        if wanted_device not in ids:
            continue
        actions = {_text(value).lower() for value in row.get("actions") or [] if _text(value)}
        features = {_text(value).lower() for value in row.get("features") or [] if _text(value)}
        supported = actions | features
        if "play_url" in supported:
            return "play_url"
        if "play_media" in supported:
            return "play_media"
        if "announce" in supported:
            return "announce"
    return ""


def _integration_playback_sync(
    *,
    targets: List[Dict[str, str]],
    source_url: str,
    media_content_type: str = "music",
    media_type: str = "audio/mpeg",
    start_position_seconds: float = 0.0,
    timeout_s: float = DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    clean_targets = [
        {
            "integration_id": _text(item.get("integration_id")).lower(),
            "device_id": _text(item.get("device_id")),
        }
        for item in list(targets or [])
        if isinstance(item, dict) and _text(item.get("integration_id")) and _text(item.get("device_id"))
    ]
    if not clean_targets:
        return {"ok": False, "sent_count": 0, "error": "No integration playback targets selected."}
    if not _text(source_url):
        return {"ok": False, "sent_count": 0, "error": "Integration playback URL is missing."}

    from integration_registry import run_integration_device_action

    sent_count = 0
    failures: List[str] = []
    for target in clean_targets:
        integration_id = target["integration_id"]
        device_id = target["device_id"]
        action = _integration_device_playback_action(integration_id, device_id) or "play_url"
        payload = {
            "source_url": source_url,
            "url": source_url,
            "media_url": source_url,
            "media_content_id": source_url,
            "media_content_type": _text(media_content_type) or "music",
            "media_type": _text(media_type) or "audio/mpeg",
            "start_position_seconds": _as_float(
                start_position_seconds,
                0.0,
                minimum=0.0,
            ),
            "timeout_s": _as_float(timeout_s, DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS, minimum=1.0),
        }

        try:
            result = run_integration_device_action(integration_id, action, device_id, payload)
            if isinstance(result, dict) and result.get("ok") is False:
                failures.append(f"{integration_id}:{device_id} ({_text(result.get('error')) or 'failed'})")
                continue
            sent_count += int((result or {}).get("sent_count") or 1) if isinstance(result, dict) else 1
        except Exception as first_exc:
            if action != "play_media":
                try:
                    result = run_integration_device_action(integration_id, "play_media", device_id, payload)
                    if isinstance(result, dict) and result.get("ok") is False:
                        failures.append(f"{integration_id}:{device_id} ({_text(result.get('error')) or 'failed'})")
                        continue
                    sent_count += int((result or {}).get("sent_count") or 1) if isinstance(result, dict) else 1
                    continue
                except Exception as second_exc:
                    failures.append(f"{integration_id}:{device_id} ({second_exc})")
                    continue
            failures.append(f"{integration_id}:{device_id} ({first_exc})")

    if sent_count:
        result: Dict[str, Any] = {"ok": True, "sent_count": sent_count}
        if failures:
            result["warnings"] = failures
        return result
    return {"ok": False, "sent_count": 0, "error": "; ".join(failures) or "Integration playback failed."}


def _sonos_playback_sync(
    *,
    speakers: List[str],
    source_url: str,
    media_content_type: str = "music",
    volume_percent: int | None = None,
    start_position_seconds: float = 0.0,
    timeout_s: float = DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    try:
        from speech_tts import sonos_play_media_sync

        return sonos_play_media_sync(
            speakers=speakers,
            source_url=source_url,
            media_content_type=media_content_type,
            volume_percent=volume_percent,
            restore_after=_text(media_content_type).lower() not in {"music", "audio", "song", "media"},
            start_position_seconds=_as_float(
                start_position_seconds,
                0.0,
                minimum=0.0,
            ),
            timeout_s=_as_float(timeout_s, DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS, minimum=1.0),
        )
    except Exception as exc:
        return {"ok": False, "sent_count": 0, "error": str(exc)}


def play_media_url_targets(
    targets: Any,
    source_url: str,
    *,
    audio_bytes: bytes | None = None,
    media_type: str = "audio/mpeg",
    media_content_type: str = "music",
    filename: str = "media.mp3",
    text: str = "",
    volume_percent: int = 100,
    start_position_seconds: float = 0.0,
    mixed_sync_adjustment_ms: int = 0,
    timeout_s: float = DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS,
    respect_reply_playback: bool = False,
) -> Dict[str, Any]:
    grouped = split_announcement_targets(targets)
    homeassistant_players = list(grouped.get("homeassistant_media_players") or [])
    voice_core_selectors = list(grouped.get("voice_core_selectors") or [])
    sonos_speakers = list(grouped.get("sonos_speakers") or [])
    parsed_integration_devices = [
        item for item in list(grouped.get("integration_devices") or []) if isinstance(item, dict)
    ]
    sonos_integration_speakers = [
        _text(item.get("device_id"))
        for item in parsed_integration_devices
        if _text(item.get("integration_id")).lower() == "sonos" and _text(item.get("device_id"))
    ]
    sonos_speakers.extend(item for item in sonos_integration_speakers if item not in sonos_speakers)
    integration_devices = [
        item for item in parsed_integration_devices if _text(item.get("integration_id")).lower() != "sonos"
    ]
    unifi_protect_cameras = list(grouped.get("unifi_protect_cameras") or [])

    target_count = (
        len(homeassistant_players)
        + len(voice_core_selectors)
        + len(sonos_speakers)
        + len(integration_devices)
        + len(unifi_protect_cameras)
    )
    result: Dict[str, Any] = {
        "target_count": target_count,
        "homeassistant_target_count": len(homeassistant_players),
        "voice_core_target_count": len(voice_core_selectors),
        "sonos_target_count": len(sonos_speakers),
        "integration_target_count": len(integration_devices),
        "unifi_protect_target_count": len(unifi_protect_cameras),
    }
    if target_count <= 0:
        result.update({"ok": False, "sent_count": 0, "error": "No media playback targets selected."})
        return result

    clean_media_type = _text(media_type).split(";", 1)[0].strip().lower() or "audio/mpeg"
    safe_filename = Path(_text(filename) or "media.mp3").name
    runtime_source_url = _runtime_media_source_url(
        bytes(audio_bytes or b"") if isinstance(audio_bytes, (bytes, bytearray)) else None,
        content_type=clean_media_type,
        filename=safe_filename,
    )
    playback_source_url = runtime_source_url or _text(source_url)
    result["source_url"] = playback_source_url
    if runtime_source_url:
        result["runtime_source_url"] = runtime_source_url

    warnings: List[str] = []
    sent_count = 0

    if voice_core_selectors:
        native_start_lead_ms = NATIVE_GROUP_START_LEAD_MS if len(voice_core_selectors) > 1 else 0
        if sonos_speakers:
            adjustment_ms = max(-750, min(3000, int(_as_float(mixed_sync_adjustment_ms, 0.0))))
            native_start_lead_ms = max(
                250,
                min(5000, MIXED_SONOS_NATIVE_START_LEAD_MS + adjustment_ms),
            )
            result["mixed_sync_adjustment_ms"] = adjustment_ms
            result["mixed_native_start_lead_ms"] = native_start_lead_ms
        voice_result = _voice_core_play_media_sync(
            selectors=voice_core_selectors,
            source_url=playback_source_url,
            audio_bytes=bytes(audio_bytes or b"") if isinstance(audio_bytes, (bytes, bytearray)) else None,
            text=text,
            media_type=clean_media_type,
            media_content_type=media_content_type,
            filename=safe_filename,
            volume_percent=volume_percent,
            start_position_seconds=start_position_seconds,
            start_lead_ms=native_start_lead_ms,
            timeout_s=timeout_s,
            respect_reply_playback=respect_reply_playback,
        )
        result["voice_core_sent_count"] = int(voice_result.get("sent_count") or 0)
        result["media_session_sent_count"] = int(voice_result.get("media_session_sent_count") or 0)
        result["media_session_fallback_count"] = int(voice_result.get("media_session_fallback_count") or 0)
        result["media_session_warnings"] = [
            _text(item)
            for item in list(voice_result.get("media_session_warnings") or [])
            if _text(item)
        ]
        result["voice_core_sessions"] = [
            dict(item)
            for item in list(voice_result.get("voice_core_sessions") or [])
            if isinstance(item, dict)
        ]
        sent_count += int(voice_result.get("sent_count") or 0)
        warnings.extend([_text(item) for item in list(voice_result.get("warnings") or []) if _text(item)])
        if not voice_result.get("ok") and _text(voice_result.get("error")):
            warnings.append(_text(voice_result.get("error")))

    if sonos_speakers:
        sonos_source_url = playback_source_url
        if sonos_source_url and not runtime_source_url:
            sonos_source_url = _runtime_media_proxy_source_url(
                sonos_source_url,
                content_type=clean_media_type,
                filename=safe_filename,
                ttl_s=max(RUNTIME_MEDIA_PROXY_TTL_SECONDS, float(timeout_s or 0.0) + 600.0),
            )
            if sonos_source_url:
                result["sonos_proxy_used"] = True
        if not sonos_source_url:
            warnings.append("Sonos playback URL is missing.")
        else:
            sonos_result = _sonos_playback_sync(
                speakers=sonos_speakers,
                source_url=sonos_source_url,
                media_content_type=media_content_type,
                volume_percent=max(0, min(100, int(_as_float(volume_percent, 100.0)))),
                start_position_seconds=start_position_seconds,
                timeout_s=timeout_s,
            )
            result["sonos_sent_count"] = int(sonos_result.get("sent_count") or 0)
            if isinstance(sonos_result.get("group"), dict):
                result["sonos_group"] = dict(sonos_result["group"])
            sent_count += int(sonos_result.get("sent_count") or 0)
            warnings.extend([_text(item) for item in list(sonos_result.get("warnings") or []) if _text(item)])
            if not sonos_result.get("ok") and _text(sonos_result.get("error")):
                warnings.append(_text(sonos_result.get("error")))

    if integration_devices or homeassistant_players:
        integration_targets = list(integration_devices)
        integration_targets.extend(
            {"integration_id": "homeassistant", "device_id": player}
            for player in homeassistant_players
            if _text(player)
        )
        if not playback_source_url:
            warnings.append("Integration playback URL is missing.")
        else:
            integration_result = _integration_playback_sync(
                targets=integration_targets,
                source_url=playback_source_url,
                media_content_type=media_content_type,
                media_type=clean_media_type,
                start_position_seconds=start_position_seconds,
                timeout_s=timeout_s,
            )
            result["integration_sent_count"] = int(integration_result.get("sent_count") or 0)
            sent_count += int(integration_result.get("sent_count") or 0)
            warnings.extend([_text(item) for item in list(integration_result.get("warnings") or []) if _text(item)])
            if not integration_result.get("ok") and _text(integration_result.get("error")):
                warnings.append(_text(integration_result.get("error")))

    if unifi_protect_cameras:
        warnings.append("UniFi Protect camera speaker targets require generated audio bytes and are not used for music playback.")

    result["sent_count"] = sent_count
    if sent_count > 0:
        result["ok"] = True
        if warnings:
            result["warnings"] = warnings
        return result

    result["ok"] = False
    result["error"] = "; ".join(warnings) or "Media playback failed."
    return result
