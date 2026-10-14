from __future__ import annotations

import asyncio
import hashlib
from typing import Any, Dict, List, Tuple

from helpers import redis_client
from integration_registry import get_integration_device_registry, run_integration_device_action


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _actions(row: Dict[str, Any]) -> set[str]:
    return {_text(item).lower() for item in list(row.get("actions") or []) if _text(item)}


def _camera_key(row: Dict[str, Any]) -> str:
    integration_id = _text(row.get("integration_id"))
    device_id = _text(row.get("id") or row.get("ref"))
    return hashlib.sha256(f"{integration_id}\0{device_id}".encode("utf-8")).hexdigest()[:24]


def _camera_rows(client: Any = None) -> List[Dict[str, Any]]:
    registry = get_integration_device_registry(client or redis_client)
    rows = registry.get("devices") if isinstance(registry, dict) else registry
    result: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        row = raw if isinstance(raw, dict) else {}
        actions = _actions(row)
        category = _text(row.get("type") or row.get("category") or row.get("category_id")).lower()
        if category != "camera" and not ({"camera_snapshot", "snapshot"} & actions):
            continue
        if not ({"camera_snapshot", "snapshot"} & actions):
            continue
        integration_id = _text(row.get("integration_id"))
        device_id = _text(row.get("id") or row.get("ref"))
        if not integration_id or not device_id:
            continue
        result.append(row)
    return result


def build_camera_directory(*, client: Any = None) -> Dict[str, Any]:
    cameras: List[Dict[str, Any]] = []
    for row in _camera_rows(client):
        actions = _actions(row)
        capabilities = {_text(item).lower() for item in list(row.get("capabilities") or []) if _text(item)}
        features = {_text(item).lower() for item in list(row.get("features") or []) if _text(item)}
        cameras.append(
            {
                "id": _camera_key(row),
                "name": _text(row.get("name") or row.get("display_name") or "Camera"),
                "room": _text(row.get("room") or row.get("area")),
                "doorbell": "doorbell" in capabilities or "doorbell" in features,
                "talkback": "camera_talkback" in actions,
                "available": _text(row.get("status")).lower() not in {"offline", "unavailable", "disconnected"},
            }
        )
    cameras.sort(key=lambda row: (_text(row.get("room")).lower(), _text(row.get("name")).lower(), _text(row.get("id"))))
    return {"available": bool(cameras), "cameras": cameras}


def _resolve_camera(camera_id: Any, client: Any = None) -> Dict[str, Any]:
    token = _text(camera_id)
    for row in _camera_rows(client):
        if _camera_key(row) == token:
            return row
    raise KeyError("Camera is unavailable or no longer configured.")


def capture_frame(camera_id: Any, *, client: Any = None) -> Tuple[bytes, str, Dict[str, Any]]:
    row = _resolve_camera(camera_id, client)
    actions = _actions(row)
    action = "camera_snapshot" if "camera_snapshot" in actions else "snapshot"
    result = run_integration_device_action(
        _text(row.get("integration_id")),
        action,
        _text(row.get("id") or row.get("ref")),
        {},
    )
    if isinstance(result, tuple) and len(result) >= 2:
        image, content_type = result[0], result[1]
    elif isinstance(result, dict):
        image = result.get("bytes") or result.get("image") or b""
        content_type = result.get("content_type") or "image/jpeg"
    else:
        image, content_type = b"", "image/jpeg"
    image = bytes(image or b"")
    if not image:
        raise RuntimeError("The camera returned an empty image.")
    return image, _text(content_type) or "image/jpeg", row


def send_talkback(camera_id: Any, audio_bytes: bytes, audio_format: Dict[str, Any], *, client: Any = None) -> Dict[str, Any]:
    row = _resolve_camera(camera_id, client)
    if "camera_talkback" not in _actions(row):
        raise RuntimeError("This camera does not advertise two-way audio.")
    from .intercom import _pcm_to_wav_bytes

    wav_bytes = _pcm_to_wav_bytes(bytes(audio_bytes or b""), dict(audio_format or {}))
    if not wav_bytes:
        raise RuntimeError("No talkback audio was captured.")
    result = run_integration_device_action(
        _text(row.get("integration_id")),
        "camera_talkback",
        _text(row.get("id") or row.get("ref")),
        {"audio_bytes": wav_bytes},
    )
    if isinstance(result, dict):
        return result
    return {"ok": True, "result": result}


def is_talkback_wake_word(value: Any) -> bool:
    return _text(value).lower().startswith("camera talkback:")


def talkback_camera_id(value: Any) -> str:
    text = _text(value)
    return text.split(":", 1)[1].strip() if is_talkback_wake_word(text) else ""


async def handle_talkback_voice_turn(*, wake_word: Any, audio_bytes: bytes, audio_format: Dict[str, Any]) -> Dict[str, Any]:
    camera_id = talkback_camera_id(wake_word)
    if not camera_id:
        raise ValueError("Camera talkback target is missing.")
    result = await asyncio.to_thread(send_talkback, camera_id, audio_bytes, audio_format)
    return {
        "transcript": "",
        "response_text": "",
        "no_op": True,
        "camera_talkback": True,
        "camera_talkback_sent": bool(result.get("ok", True)) if isinstance(result, dict) else True,
    }
