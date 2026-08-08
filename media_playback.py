from __future__ import annotations

import base64
import contextlib
import logging
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import quote

import requests

from announcement_targets import split_announcement_targets
from helpers import redis_client

logger = logging.getLogger("media_playback")

DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS = 360.0
NATIVE_GROUP_START_LEAD_MS = 750
MIXED_SONOS_NATIVE_START_LEAD_MS = 1000
AIRPLAY_NATIVE_START_LEAD_MS = 750
AIRPLAY_SOLO_START_LEAD_MS = 500
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


def _target_setting(
    settings: Dict[str, Any] | None,
    *target_ids: Any,
    default: float = 0.0,
) -> float:
    source = settings if isinstance(settings, dict) else {}
    aliases: List[str] = []
    for raw_target in target_ids:
        target = _text(raw_target)
        if not target:
            continue
        candidates = [target]
        if target.startswith("voice_core:"):
            candidates.append(target.removeprefix("voice_core:"))
        else:
            candidates.append(f"voice_core:{target}")
        if target.startswith("airplay:"):
            candidates.append(target.removeprefix("airplay:"))
        else:
            candidates.append(f"airplay:{target}")
        if target.startswith("integration:sonos:"):
            speaker = target.removeprefix("integration:sonos:")
            candidates.extend((speaker, f"sonos:{speaker}"))
        elif target.startswith("sonos:"):
            speaker = target.removeprefix("sonos:")
            candidates.extend((speaker, f"integration:sonos:{speaker}"))
        else:
            candidates.extend((f"sonos:{target}", f"integration:sonos:{target}"))
        for candidate in candidates:
            if candidate and candidate not in aliases:
                aliases.append(candidate)
    for alias in aliases:
        if alias in source:
            return _as_float(source.get(alias), default)
    return float(default)


def _target_transport_mode(
    settings: Dict[str, Any] | None,
    *target_ids: Any,
    default: str = "native",
) -> str:
    source = settings if isinstance(settings, dict) else {}
    aliases: List[str] = []
    for raw_target in target_ids:
        target = _text(raw_target)
        if not target:
            continue
        speaker = target.removeprefix("sonos:").removeprefix("integration:sonos:")
        for alias in (target, speaker, f"sonos:{speaker}", f"integration:sonos:{speaker}"):
            if alias and alias not in aliases:
                aliases.append(alias)
    for alias in aliases:
        if alias not in source:
            continue
        mode = _text(source.get(alias)).lower()
        if mode in {"auto", "native", "airplay"}:
            return mode
    return default if default in {"auto", "native", "airplay"} else "native"


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
    prefer_loopback: bool = False,
) -> str:
    asset_id = store_runtime_media_proxy_source(
        source_url,
        content_type=content_type,
        filename=filename,
        ttl_s=ttl_s,
    )
    if not asset_id:
        return ""
    if prefer_loopback:
        base_url = f"http://127.0.0.1:{_main_app_port()}"
    else:
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
    target_volume_percent: Dict[str, Any] | None = None,
    target_sync_offset_ms: Dict[str, Any] | None = None,
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
        group_payload["player_settings"] = {
            selector: {
                "volume_percent": max(
                    0,
                    min(
                        100,
                        int(
                            _target_setting(
                                target_volume_percent,
                                selector,
                                default=volume_percent,
                            )
                        ),
                    ),
                ),
                "sync_offset_ms": max(
                    -1000,
                    min(
                        1000,
                        int(
                            _target_setting(
                                target_sync_offset_ms,
                                selector,
                                default=0,
                            )
                        ),
                    ),
                ),
            }
            for selector in clean_selectors
        }
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
                for timing_key in ("start_server_us", "start_unix_ms"):
                    if response_payload.get(timing_key) is not None:
                        result[timing_key] = int(response_payload[timing_key])
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
        payload["volume_percent"] = max(
            0,
            min(
                100,
                int(_target_setting(target_volume_percent, selector, default=volume_percent)),
            ),
        )
        payload["sync_offset_ms"] = max(
            -1000,
            min(1000, int(_target_setting(target_sync_offset_ms, selector, default=0))),
        )
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


def _voice_core_stop_media_sync(selectors: Iterable[Any]) -> List[str]:
    warnings: List[str] = []
    try:
        from tater_voice import native_satellite, stereo_pairs

        members: List[str] = []
        for raw_selector in selectors:
            selector = _text(raw_selector)
            if not selector:
                continue
            pair = stereo_pairs.get_pair(selector) if stereo_pairs.is_stereo_selector(selector) else {}
            candidates = (
                [_text(pair.get("left_selector")), _text(pair.get("right_selector"))]
                if isinstance(pair, dict) and pair
                else [selector]
            )
            for member in candidates:
                if member and member not in members:
                    members.append(member)
        for member in members:
            try:
                native_satellite.run_on_runtime_loop(
                    native_satellite.send_command(
                        member,
                        "media.session.stop",
                        {"reason": "synchronized_route_abort"},
                    ),
                    timeout=8.0,
                )
            except Exception as exc:
                warnings.append(f"{member}: {exc}")
    except Exception as exc:
        warnings.append(_text(exc))
    return warnings


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
    volume_percent: int = 100,
    target_volume_percent: Dict[str, Any] | None = None,
    target_sync_offset_ms: Dict[str, Any] | None = None,
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
            "volume_percent": max(
                0,
                min(
                    100,
                    int(
                        _target_setting(
                            target_volume_percent,
                            f"integration:{integration_id}:{device_id}",
                            f"{integration_id}:{device_id}",
                            f"ha:{device_id}" if integration_id == "homeassistant" else "",
                            device_id,
                            default=volume_percent,
                        )
                    ),
                ),
            ),
            "sync_offset_ms": max(
                -1000,
                min(
                    1000,
                    int(
                        _target_setting(
                            target_sync_offset_ms,
                            f"integration:{integration_id}:{device_id}",
                            f"{integration_id}:{device_id}",
                            f"ha:{device_id}" if integration_id == "homeassistant" else "",
                            device_id,
                            default=0,
                        )
                    ),
                ),
            ),
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
    volume_by_speaker: Dict[str, Any] | None = None,
    start_position_seconds: float = 0.0,
    timeout_s: float = DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    try:
        from speech_tts import sonos_play_media_sync

        playback_kwargs = dict(
            speakers=speakers,
            source_url=source_url,
            media_content_type=media_content_type,
            volume_percent=volume_percent,
            volume_by_speaker=volume_by_speaker,
            restore_after=_text(media_content_type).lower() not in {"music", "audio", "song", "media"},
            start_position_seconds=_as_float(
                start_position_seconds,
                0.0,
                minimum=0.0,
            ),
            timeout_s=_as_float(timeout_s, DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS, minimum=1.0),
        )
        try:
            return sonos_play_media_sync(**playback_kwargs)
        except TypeError as exc:
            if "volume_by_speaker" not in str(exc):
                raise
            playback_kwargs.pop("volume_by_speaker", None)
            return sonos_play_media_sync(**playback_kwargs)
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
    title: str = "",
    artist: str = "",
    album: str = "",
    duration_seconds: float = 0.0,
    volume_percent: int = 100,
    start_position_seconds: float = 0.0,
    mixed_sync_adjustment_ms: int = 0,
    target_volume_percent: Dict[str, Any] | None = None,
    target_sync_offset_ms: Dict[str, Any] | None = None,
    target_transport_mode: Dict[str, Any] | None = None,
    airplay_group_id: str = "",
    timeout_s: float = DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS,
    respect_reply_playback: bool = False,
    _resume_fallback_attempted: bool = False,
) -> Dict[str, Any]:
    grouped = split_announcement_targets(targets)
    homeassistant_players = list(grouped.get("homeassistant_media_players") or [])
    voice_core_selectors = list(grouped.get("voice_core_selectors") or [])
    sonos_speakers = list(grouped.get("sonos_speakers") or [])
    airplay_players = list(grouped.get("airplay_players") or [])
    routing_warnings: List[str] = []
    sonos_airplay_routes: Dict[str, str] = {}
    direct_sonos_speakers: List[str] = []
    for speaker in sonos_speakers:
        sonos_target = f"sonos:{speaker}"
        transport_mode = _target_transport_mode(
            target_transport_mode,
            sonos_target,
            speaker,
        )
        use_airplay = transport_mode == "airplay" or (
            transport_mode == "auto" and bool(voice_core_selectors)
        )
        bridge_target = ""
        if use_airplay:
            try:
                from announcement_targets import resolve_sonos_airplay_target

                bridge_target = _text(resolve_sonos_airplay_target(sonos_target))
            except Exception as exc:
                logger.debug("[media_playback] Sonos AirPlay matching failed: %s", exc)
        if bridge_target:
            bridge_id = bridge_target.removeprefix("airplay:")
            if bridge_id and bridge_id not in airplay_players:
                airplay_players.append(bridge_id)
            sonos_airplay_routes[sonos_target] = bridge_target
            continue
        direct_sonos_speakers.append(speaker)
        if transport_mode == "airplay":
            routing_warnings.append(
                f"{sonos_target} has no available AirPlay endpoint; using native Sonos."
            )
    sonos_speakers = direct_sonos_speakers
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
        + len(airplay_players)
        + len(integration_devices)
        + len(unifi_protect_cameras)
    )
    result: Dict[str, Any] = {
        "target_count": target_count,
        "homeassistant_target_count": len(homeassistant_players),
        "voice_core_target_count": len(voice_core_selectors),
        "sonos_target_count": len(sonos_speakers),
        "sonos_airplay_target_count": len(sonos_airplay_routes),
        "airplay_bridge_target_count": len(airplay_players),
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

    warnings: List[str] = list(routing_warnings)
    sent_count = 0
    airplay_prepared: Dict[str, Any] = {}
    airplay_primed: Dict[str, Any] = {}
    airplay_ready_at_unix_ms = 0
    airplay_minimum_start_unix_ms = 0
    airplay_reused = False
    effective_target_volume_percent = dict(target_volume_percent or {})
    effective_target_sync_offset_ms = dict(target_sync_offset_ms or {})
    for sonos_target, bridge_target in sonos_airplay_routes.items():
        effective_target_volume_percent[bridge_target] = _target_setting(
            target_volume_percent,
            sonos_target,
            default=volume_percent,
        )
        effective_target_sync_offset_ms[bridge_target] = _target_setting(
            target_sync_offset_ms,
            sonos_target,
            default=0,
        )
    if sonos_airplay_routes:
        result["sonos_airplay_routes"] = dict(sonos_airplay_routes)

    airplay_offsets = {
        f"airplay:{player}": max(
            -1000,
            min(
                1000,
                int(
                    _target_setting(
                        effective_target_sync_offset_ms,
                        f"airplay:{player}",
                        player,
                        default=0,
                    )
                ),
            ),
        )
        for player in airplay_players
    }
    native_reference_offset_ms = min(
        (
            max(
                -1000,
                min(
                    1000,
                    int(
                        _target_setting(
                            effective_target_sync_offset_ms,
                            selector,
                            default=0,
                        )
                    ),
                ),
            )
            for selector in voice_core_selectors
        ),
        default=min(airplay_offsets.values(), default=0),
    )

    if _text(airplay_group_id) and not airplay_players:
        with contextlib.suppress(Exception):
            from airplay_bridge import stop_airplay_group_sync

            stop_airplay_group_sync(_text(airplay_group_id))

    # AirPlay receivers need time to connect and buffer. Do that before the
    # satellite timeline is committed, then start both transports from the
    # same Unix-millisecond anchor below.
    if airplay_players:
        airplay_source_url = playback_source_url

        if _text(airplay_group_id):
            try:
                from airplay_bridge import reuse_airplay_group_sync

                airplay_prepared = reuse_airplay_group_sync(
                    group_id=_text(airplay_group_id),
                    targets=airplay_players,
                    source_url=airplay_source_url,
                    start_position_seconds=start_position_seconds,
                    volume_percent=max(0, min(100, int(_as_float(volume_percent, 100.0)))),
                    target_volume_percent=effective_target_volume_percent,
                    target_sync_offset_ms=airplay_offsets,
                    reference_sync_offset_ms=native_reference_offset_ms,
                    title=_text(title) or _text(text) or Path(safe_filename).stem or "Tater Music",
                    artist=_text(artist) or "Tater",
                    album=_text(album) or "Tater Music",
                    duration_seconds=max(0.0, _as_float(duration_seconds, 0.0)),
                    timeout_s=min(30.0, max(5.0, float(timeout_s or 0.0))),
                )
            except Exception as exc:
                airplay_prepared = {
                    "ok": False,
                    "reusable": False,
                    "error": str(exc),
                    "prepared_count": 0,
                }
            if airplay_prepared.get("ok"):
                airplay_reused = True
                airplay_primed = dict(airplay_prepared)
                result["airplay_bridge_reused"] = True
            else:
                result["airplay_bridge_reuse_fallback"] = (
                    _text(airplay_prepared.get("error")) or "warm session unavailable"
                )
                logger.info(
                    "[media_playback] AirPlay warm replacement was unavailable for %s: %s; using a fresh session",
                    ", ".join(airplay_players),
                    result["airplay_bridge_reuse_fallback"],
                )
                airplay_prepared = {}

        def _prepare_airplay(source: str) -> Dict[str, Any]:
            from airplay_bridge import prepare_airplay_group_sync

            return prepare_airplay_group_sync(
                targets=airplay_players,
                source_url=source,
                start_position_seconds=start_position_seconds,
                volume_percent=max(0, min(100, int(_as_float(volume_percent, 100.0)))),
                target_volume_percent=effective_target_volume_percent,
                title=_text(title) or _text(text) or Path(safe_filename).stem or "Tater Music",
                artist=_text(artist) or "Tater",
                album=_text(album) or "Tater Music",
                duration_seconds=max(0.0, _as_float(duration_seconds, 0.0)),
                timeout_s=min(30.0, max(5.0, float(timeout_s or 0.0))),
            )

        if not airplay_reused:
            try:
                airplay_prepared = _prepare_airplay(airplay_source_url)
            except Exception as exc:
                airplay_prepared = {"ok": False, "error": str(exc), "prepared_count": 0}
        if not airplay_reused and not airplay_prepared.get("ok"):
            first_error = _text(airplay_prepared.get("error"))
            logger.warning(
                "[media_playback] AirPlay preparation failed for %s: %s; retrying once",
                ", ".join(airplay_players),
                first_error or "unknown error",
            )
            time.sleep(0.25)
            try:
                # A receiver may still be releasing its previous native Sonos
                # session. One fresh AirPlay session avoids leaving the other
                # synchronized destinations playing alone on that transient.
                airplay_prepared = _prepare_airplay(airplay_source_url)
                result["airplay_prepare_retried"] = True
            except Exception as exc:
                airplay_prepared = {"ok": False, "error": str(exc), "prepared_count": 0}
        result["airplay_bridge_prepared_count"] = int(
            airplay_prepared.get("prepared_count") or 0
        )
        if _text(airplay_prepared.get("group_id")):
            result["airplay_bridge_group_id"] = _text(airplay_prepared.get("group_id"))
        warnings.extend(
            _text(item)
            for item in list(airplay_prepared.get("warnings") or [])
            if _text(item)
        )
        if not airplay_prepared.get("ok") and _text(airplay_prepared.get("error")):
            warnings.append(f"AirPlay Bridge: {_text(airplay_prepared.get('error'))}")
            logger.warning(
                "[media_playback] AirPlay preparation failed after retry for %s: %s",
                ", ".join(airplay_players),
                _text(airplay_prepared.get("error")),
            )

    # Feed AirPlay before committing any native-satellite timeline. The sender
    # owns its PCM pacing and reports when the receiver clock will be usable;
    # scheduling the sat first can leave that fixed timeline behind a corrected
    # AirPlay start and can also make a failed AirPlay member play the sat alone.
    if airplay_prepared.get("ok") and not airplay_reused:
        group_id = _text(airplay_prepared.get("group_id"))
        try:
            from airplay_bridge import prime_airplay_group_sync

            airplay_primed = prime_airplay_group_sync(
                group_id=group_id,
                timeout_s=min(30.0, max(5.0, float(timeout_s or 0.0))),
            )
        except Exception as exc:
            airplay_primed = {"ok": False, "primed_count": 0, "error": str(exc)}
        result["airplay_bridge_primed_count"] = int(
            airplay_primed.get("primed_count") or 0
        )
        if airplay_primed.get("ok"):
            airplay_ready_at_unix_ms = max(
                (
                    int(row.get("ready_at_unix_ms") or 0)
                    for row in dict(airplay_primed.get("clock_readiness") or {}).values()
                    if isinstance(row, dict)
                ),
                default=0,
            )
            if airplay_ready_at_unix_ms:
                result["airplay_ready_at_unix_ms"] = airplay_ready_at_unix_ms
        else:
            detail = _text(airplay_primed.get("error")) or "AirPlay audio priming failed."
            warnings.append(f"AirPlay Bridge: {detail}")
            logger.warning(
                "[media_playback] AirPlay audio priming failed for %s: %s",
                ", ".join(airplay_players),
                detail,
            )
            airplay_prepared = {}

    if airplay_prepared.get("ok") and airplay_reused:
        result["airplay_bridge_primed_count"] = int(
            airplay_primed.get("primed_count") or 0
        )

    if airplay_primed.get("ok"):
        airplay_ready_at_unix_ms = max(
            (
                int(row.get("ready_at_unix_ms") or 0)
                for row in dict(airplay_primed.get("clock_readiness") or {}).values()
                if isinstance(row, dict)
            ),
            default=0,
        )
        airplay_minimum_start_unix_ms = int(
            airplay_primed.get("minimum_start_unix_ms") or 0
        )
        if not airplay_minimum_start_unix_ms:
            airplay_minimum_start_unix_ms = int(time.time() * 1000) + int(
                airplay_primed.get("minimum_start_lead_ms") or 0
            )
        if airplay_ready_at_unix_ms:
            result["airplay_ready_at_unix_ms"] = airplay_ready_at_unix_ms
        if airplay_minimum_start_unix_ms:
            result["airplay_minimum_start_unix_ms"] = airplay_minimum_start_unix_ms

    voice_result: Dict[str, Any] = {}
    if voice_core_selectors and (not airplay_players or airplay_prepared.get("ok")):
        native_start_lead_ms = NATIVE_GROUP_START_LEAD_MS if len(voice_core_selectors) > 1 else 0
        if sonos_speakers:
            adjustment_ms = max(-750, min(3000, int(_as_float(mixed_sync_adjustment_ms, 0.0))))
            native_offsets = [
                max(
                    -1000,
                    min(
                        1000,
                        int(
                            _target_setting(
                                target_sync_offset_ms,
                                selector,
                                default=0,
                            )
                        ),
                    ),
                )
                for selector in voice_core_selectors
            ]
            if native_offsets:
                normalized_native_average_ms = round(
                    sum(offset - min(native_offsets) for offset in native_offsets)
                    / len(native_offsets)
                )
                adjustment_ms = max(
                    -750,
                    min(3000, adjustment_ms - normalized_native_average_ms),
                )
            native_start_lead_ms = max(
                250,
                min(5000, MIXED_SONOS_NATIVE_START_LEAD_MS + adjustment_ms),
            )
            result["mixed_sync_adjustment_ms"] = adjustment_ms
            result["mixed_native_start_lead_ms"] = native_start_lead_ms
        if airplay_prepared.get("ok"):
            minimum_start_lead_ms = max(
                0,
                airplay_minimum_start_unix_ms - int(time.time() * 1000),
            )
            clock_ready_lead_ms = max(
                0,
                airplay_ready_at_unix_ms - int(time.time() * 1000) + 500,
            )
            native_start_lead_ms = max(
                native_start_lead_ms,
                AIRPLAY_NATIVE_START_LEAD_MS,
                minimum_start_lead_ms,
                clock_ready_lead_ms,
            )
            native_start_lead_ms = min(5000, native_start_lead_ms)
            result["airplay_native_start_lead_ms"] = native_start_lead_ms
        voice_result = _voice_core_play_media_sync(
            selectors=voice_core_selectors,
            source_url=playback_source_url,
            audio_bytes=bytes(audio_bytes or b"") if isinstance(audio_bytes, (bytes, bytearray)) else None,
            text=text,
            media_type=clean_media_type,
            media_content_type=media_content_type,
            filename=safe_filename,
            volume_percent=volume_percent,
            target_volume_percent=effective_target_volume_percent,
            target_sync_offset_ms=effective_target_sync_offset_ms,
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

    if voice_core_selectors and not voice_result.get("ok"):
        if airplay_prepared.get("ok"):
            with contextlib.suppress(Exception):
                from airplay_bridge import stop_airplay_targets

                stop_airplay_targets(airplay_players)
            airplay_prepared = {}
        if start_position_seconds > 0 and not _resume_fallback_attempted:
            logger.warning(
                "[media_playback] Synchronized resume at %.3fs failed; retrying the track from its beginning",
                start_position_seconds,
            )
            retry_result = play_media_url_targets(
                targets,
                source_url,
                audio_bytes=audio_bytes,
                media_type=media_type,
                media_content_type=media_content_type,
                filename=filename,
                text=text,
                title=title,
                artist=artist,
                album=album,
                duration_seconds=duration_seconds,
                volume_percent=volume_percent,
                start_position_seconds=0.0,
                mixed_sync_adjustment_ms=mixed_sync_adjustment_ms,
                target_volume_percent=target_volume_percent,
                target_sync_offset_ms=target_sync_offset_ms,
                target_transport_mode=target_transport_mode,
                timeout_s=timeout_s,
                respect_reply_playback=respect_reply_playback,
                _resume_fallback_attempted=True,
            )
            retry_result["resume_fallback_used"] = True
            retry_warnings = [
                "The synchronized players could not resume at the saved position, so Tater restarted the track."
            ]
            retry_warnings.extend(
                _text(item)
                for item in list(retry_result.get("warnings") or [])
                if _text(item)
            )
            retry_result["warnings"] = retry_warnings
            return retry_result

    if airplay_prepared.get("ok"):
        group_id = _text(airplay_prepared.get("group_id"))
        native_anchor_ms = int(voice_result.get("start_unix_ms") or 0)
        if native_anchor_ms > 0:
            start_unix_ms = native_anchor_ms
            reference_offset_ms = native_reference_offset_ms
            allow_reanchor = False
        else:
            start_unix_ms = max(
                int(time.time() * 1000) + AIRPLAY_SOLO_START_LEAD_MS,
                airplay_ready_at_unix_ms + 500,
                airplay_minimum_start_unix_ms,
            )
            reference_offset_ms = min(airplay_offsets.values(), default=0)
            allow_reanchor = True
        try:
            from airplay_bridge import commit_airplay_group_sync

            airplay_result = commit_airplay_group_sync(
                group_id=group_id,
                start_unix_ms=start_unix_ms,
                reference_sync_offset_ms=reference_offset_ms,
                target_sync_offset_ms=airplay_offsets,
                allow_reanchor=allow_reanchor,
            )
        except Exception as exc:
            airplay_result = {"ok": False, "sent_count": 0, "error": str(exc)}
            with contextlib.suppress(Exception):
                from airplay_bridge import stop_airplay_targets

                stop_airplay_targets(airplay_players)
        result["airplay_bridge_sent_count"] = int(airplay_result.get("sent_count") or 0)
        if airplay_result.get("start_unix_ms") is not None:
            result["airplay_bridge_start_unix_ms"] = int(airplay_result["start_unix_ms"])
        sent_count += int(airplay_result.get("sent_count") or 0)
        warnings.extend(
            _text(item)
            for item in list(airplay_result.get("warnings") or [])
            if _text(item)
        )
        if not airplay_result.get("ok") and _text(airplay_result.get("error")):
            warnings.append(f"AirPlay Bridge: {_text(airplay_result.get('error'))}")
            logger.warning(
                "[media_playback] AirPlay synchronized start failed for %s: %s",
                ", ".join(airplay_players),
                _text(airplay_result.get("error")),
            )
            if voice_result.get("ok"):
                warnings.extend(_voice_core_stop_media_sync(voice_core_selectors))
                sent_count -= int(voice_result.get("sent_count") or 0)
                result["voice_core_sent_count"] = 0
                result["media_session_sent_count"] = 0

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
                volume_by_speaker={
                    speaker: max(
                        0,
                        min(
                            100,
                            int(
                                _target_setting(
                                    target_volume_percent,
                                    speaker,
                                    default=volume_percent,
                                )
                            ),
                        ),
                    )
                    for speaker in sonos_speakers
                },
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
                volume_percent=max(0, min(100, int(_as_float(volume_percent, 100.0)))),
                target_volume_percent=target_volume_percent,
                target_sync_offset_ms=target_sync_offset_ms,
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
