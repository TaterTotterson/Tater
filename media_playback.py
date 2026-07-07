from __future__ import annotations

import base64
import contextlib
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

import requests

from announcement_targets import split_announcement_targets
from helpers import redis_client

logger = logging.getLogger("media_playback")

DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS = 360.0


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


def _voice_core_play_media_sync(
    *,
    selectors: List[str],
    source_url: str,
    audio_bytes: bytes | None = None,
    text: str = "",
    media_type: str = "audio/mpeg",
    filename: str = "media.mp3",
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
        "filename": Path(_text(filename) or "media.mp3").name,
        "timeout_s": _as_float(timeout_s, DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS, minimum=30.0),
        "respect_reply_playback": bool(respect_reply_playback),
    }
    if isinstance(audio_bytes, (bytes, bytearray)) and audio_bytes:
        payload_template["audio_b64"] = base64.b64encode(bytes(audio_bytes)).decode("ascii")

    sent_count = 0
    failures: List[str] = []
    base_url = _voice_core_base_url().rstrip("/")
    headers = _voice_core_auth_headers()

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
                continue
            detail = ""
            with contextlib.suppress(Exception):
                parsed = response.json()
                detail = _text(parsed.get("detail"))
            failures.append(f"{selector} ({detail or f'HTTP {response.status_code}'})")
        except Exception as exc:
            failures.append(f"{selector} ({exc})")

    if sent_count:
        result: Dict[str, Any] = {"ok": True, "sent_count": sent_count}
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
        capabilities = {_text(value).lower() for value in row.get("capabilities") or [] if _text(value)}
        supported = actions | features
        if "play_url" in supported:
            return "play_url"
        if "announcement_target" in capabilities and "play_media" in supported:
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
    timeout_s: float = DEFAULT_MEDIA_PLAY_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    try:
        from speech_tts import sonos_play_media_sync

        return sonos_play_media_sync(
            speakers=speakers,
            source_url=source_url,
            media_content_type=media_content_type,
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
        voice_result = _voice_core_play_media_sync(
            selectors=voice_core_selectors,
            source_url=playback_source_url,
            audio_bytes=bytes(audio_bytes or b"") if isinstance(audio_bytes, (bytes, bytearray)) else None,
            text=text,
            media_type=clean_media_type,
            filename=safe_filename,
            timeout_s=timeout_s,
            respect_reply_playback=respect_reply_playback,
        )
        result["voice_core_sent_count"] = int(voice_result.get("sent_count") or 0)
        sent_count += int(voice_result.get("sent_count") or 0)
        warnings.extend([_text(item) for item in list(voice_result.get("warnings") or []) if _text(item)])
        if not voice_result.get("ok") and _text(voice_result.get("error")):
            warnings.append(_text(voice_result.get("error")))

    if sonos_speakers:
        if not playback_source_url:
            warnings.append("Sonos playback URL is missing.")
        else:
            sonos_result = _sonos_playback_sync(
                speakers=sonos_speakers,
                source_url=playback_source_url,
                media_content_type=media_content_type,
                timeout_s=timeout_s,
            )
            result["sonos_sent_count"] = int(sonos_result.get("sent_count") or 0)
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
