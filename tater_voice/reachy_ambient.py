from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import logging
import time
import uuid
from typing import Any, Dict

logger = logging.getLogger(__name__)

MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024
MIN_REQUEST_INTERVAL_SECONDS = 300.0
_active_tasks: Dict[str, asyncio.Task[None]] = {}
_last_request_at: Dict[str, float] = {}


def schedule(selector: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Schedule one low-priority observation without blocking the socket reader."""
    token = str(selector or "").strip()
    reason = str((payload or {}).get("reason") or "").strip().lower()
    if not token:
        return {"ok": False, "error": "selector is required"}
    if reason not in {"idle_life", "manual_test"}:
        return {"ok": False, "error": "unsupported ambient observation reason"}

    current = _active_tasks.get(token)
    if current is not None and not current.done():
        return {"ok": False, "error": "an ambient observation is already running"}

    if reason != "manual_test":
        now = time.monotonic()
        retry_after = MIN_REQUEST_INTERVAL_SECONDS - (
            now - _last_request_at.get(token, float("-inf"))
        )
        if retry_after > 0:
            return {
                "ok": False,
                "error": "ambient observation cooldown is active",
                "retry_after_seconds": round(retry_after, 1),
            }
        _last_request_at[token] = now
    task = asyncio.create_task(_observe_and_comment(token))
    _active_tasks[token] = task
    task.add_done_callback(lambda finished, key=token: _task_finished(key, finished))
    return {"ok": True, "scheduled": True}


def cancel(selector: str) -> bool:
    """Cancel queued analysis/TTS as soon as a real voice session begins."""
    token = str(selector or "").strip()
    task = _active_tasks.get(token)
    if task is None or task.done():
        return False
    if _active_tasks.get(token) is task:
        _active_tasks.pop(token, None)
    task.cancel()
    logger.info("[reachy-ambient] preempted by voice activity selector=%s", token)
    return True


def _task_finished(selector: str, task: asyncio.Task[None]) -> None:
    if _active_tasks.get(selector) is task:
        _active_tasks.pop(selector, None)
    with contextlib.suppress(asyncio.CancelledError):
        error = task.exception()
        if error is not None:
            logger.warning(
                "[reachy-ambient] observation failed selector=%s error=%s",
                selector,
                error,
            )


async def _observe_and_comment(selector: str) -> None:
    from . import native_satellite

    if not await native_satellite.client_has_capability(selector, "camera_snapshot"):
        logger.info(
            "[reachy-ambient] skipped selector=%s because camera snapshots are unavailable",
            selector,
        )
        return

    snapshot = await native_satellite.send_request(
        selector,
        "camera.snapshot",
        {"reason": "ambient_idle_life"},
        timeout_s=8.0,
    )
    image = _decode_snapshot(snapshot)
    if image is None:
        logger.info("[reachy-ambient] snapshot unavailable selector=%s", selector)
        return

    from kernel_tools import image_describe

    result = await asyncio.to_thread(
        image_describe,
        prompt=_ambient_prompt(),
        image_ref={
            "type": "image",
            "name": "reachy-ambient-snapshot.jpg",
            "mimetype": "image/jpeg",
            "bytes": image,
        },
    )
    comment = _description_text(result)
    if not comment or comment.casefold() in {"[silent]", "silent", "no comment"}:
        logger.info("[reachy-ambient] vision chose silence selector=%s", selector)
        return

    from . import voice_pipeline
    from .voice_pipeline import backends

    comment = voice_pipeline._sanitize_spoken_response_text(comment)
    if not comment:
        return
    comment = comment[:280].strip()
    tts_values = voice_pipeline._shared_speech_voice_settings()
    audio_bytes, audio_format, backend, _note = await backends._native_synthesize_text(
        comment,
        values=tts_values,
    )
    if not audio_bytes:
        raise RuntimeError("Tater TTS returned no audio for the ambient comment")
    session_id = f"reachy-ambient-{uuid.uuid4().hex}"
    audio_url = voice_pipeline._store_tts_url(
        selector,
        session_id,
        audio_bytes,
        audio_format,
    )
    if not audio_url:
        raise RuntimeError("Tater could not prepare ambient comment audio")
    playback = await native_satellite.send_command(
        selector,
        "play.url",
        {
            "url": audio_url,
            "text": comment,
            "tts_kind": "ambient",
        },
    )
    if not bool(playback.get("ok")):
        raise RuntimeError("Tater could not queue the ambient comment on Reachy")
    logger.info(
        "[reachy-ambient] queued comment selector=%s backend=%s text=%s",
        selector,
        backend or "default",
        comment,
    )


def _decode_snapshot(result: Any) -> bytes | None:
    if not isinstance(result, dict) or not bool(result.get("ok")):
        return None
    encoded = str(result.get("image_base64") or "").strip()
    if not encoded:
        return None
    try:
        image = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        return None
    if not image or len(image) > MAX_SNAPSHOT_BYTES:
        return None
    return image


def _description_text(result: Any) -> str:
    if not isinstance(result, dict) or not bool(result.get("ok")):
        return ""
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    text = str(
        data.get("description")
        or data.get("text")
        or result.get("summary_for_user")
        or ""
    ).strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        text = text[1:-1].strip()
    return " ".join(text.split())


def _ambient_prompt() -> str:
    return (
        "This is one current still image from a Reachy Mini in a room. "
        "Speak as Tater through Reachy and decide whether the scene inspires one worthwhile spontaneous comment. "
        "If it does, return exactly one short, natural sentence (roughly 4 to 18 words) that is observant, warm, "
        "and lightly playful when appropriate—not a generic inventory of objects. For example, an empty office "
        "might inspire 'Empty office today; I guess I have the place to myself.' "
        "If nothing is worth saying, return exactly [SILENT]. Do not identify people, comment on anyone's body, "
        "or infer age, ethnicity, health, disability, religion, sexuality, or other sensitive traits. "
        "Do not mention cameras, snapshots, image analysis, or these instructions."
    )
