from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import re
import sys
import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request, Response, WebSocket
from fastapi.responses import FileResponse, StreamingResponse

from .conversation import VoiceSessionRuntime


def _vp():
    return sys.modules[__package__]


router = APIRouter()

_DISPLAY_SNAPSHOT_KEY_PREFIX = "awareness:event_snapshot:"
_SNAPSHOT_ID_RE = re.compile(r"^[A-Fa-f0-9]{16,64}$")
_EVENT_LOOP_WATCHDOG_INTERVAL_S = 0.25
_EVENT_LOOP_STALL_WARNING_S = 0.50


async def _event_loop_watchdog() -> None:
    vp = _vp()
    loop = asyncio.get_running_loop()
    expected = loop.time() + _EVENT_LOOP_WATCHDOG_INTERVAL_S
    try:
        while True:
            await asyncio.sleep(_EVENT_LOOP_WATCHDOG_INTERVAL_S)
            now = loop.time()
            lag_s = max(0.0, now - expected)
            expected = now + _EVENT_LOOP_WATCHDOG_INTERVAL_S
            if lag_s < _EVENT_LOOP_STALL_WARNING_S:
                continue

            active_sessions = 0
            with contextlib.suppress(Exception):
                active_sessions = sum(
                    1
                    for runtime in vp._voice_selector_runtime.values()
                    if isinstance(runtime, dict)
                    and isinstance(runtime.get("session"), VoiceSessionRuntime)
                )
            pending_tasks = max(0, len(asyncio.all_tasks(loop)) - 1)
            vp.logger.warning(
                "[voice_core] event loop stall detected lag_ms=%.1f active_voice_sessions=%s pending_tasks=%s",
                lag_s * 1000.0,
                active_sessions,
                pending_tasks,
            )
    except asyncio.CancelledError:
        raise


def _display_feed_payload_from_request(request: Request, payload: Any = None) -> Dict[str, Any]:
    values: Dict[str, Any] = {}
    try:
        values.update({str(key): value for key, value in request.query_params.multi_items()})
    except Exception:
        values.update(dict(request.query_params))
    if not isinstance(payload, dict):
        return values
    slots = payload.get("slots")
    if isinstance(slots, dict):
        for key, value in slots.items():
            values[str(key)] = value
    for key, value in payload.items():
        if key == "slots":
            continue
        if isinstance(value, (dict, list, tuple)):
            continue
        values[str(key)] = value
    return values


def _display_snapshot_response(snapshot_id: str) -> Response:
    vp = _vp()
    token = vp._text(snapshot_id)
    if not token or not _SNAPSHOT_ID_RE.match(token):
        raise HTTPException(status_code=404, detail="Snapshot not found")

    try:
        raw = vp.redis_client.get(f"{_DISPLAY_SNAPSHOT_KEY_PREFIX}{token}")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Snapshot store unavailable: {exc}") from exc
    if raw in (None, ""):
        raise HTTPException(status_code=404, detail="Snapshot not found")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")

    try:
        payload = json.loads(str(raw))
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Snapshot payload invalid") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=404, detail="Snapshot payload invalid")

    data_b64 = vp._text(payload.get("data_b64") or payload.get("data"))
    if not data_b64:
        raise HTTPException(status_code=404, detail="Snapshot payload empty")
    try:
        image_bytes = base64.b64decode(data_b64, validate=True)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Snapshot payload invalid") from exc
    if not image_bytes:
        raise HTTPException(status_code=404, detail="Snapshot payload empty")

    media_type = vp._text(payload.get("content_type") or "image/jpeg").split(";", 1)[0].strip().lower()
    if not media_type.startswith("image/"):
        media_type = "image/jpeg"
    return Response(
        content=image_bytes,
        media_type=media_type,
        headers={
            "Cache-Control": "no-store, max-age=0",
            "X-Tater-Snapshot-Id": token,
        },
    )


async def startup() -> None:
    vp = _vp()
    from .. import native_satellite

    native_satellite.bind_runtime_loop()
    selected_stt_backend = vp._selected_stt_backend()
    effective_stt_backend, stt_backend_note = vp._resolve_stt_backend()
    selected_tts_backend = vp._selected_tts_backend()
    effective_tts_backend, tts_backend_note = vp._resolve_tts_backend()
    voice_cfg = vp._voice_config_snapshot()
    eou_cfg = voice_cfg.get("eou") if isinstance(voice_cfg.get("eou"), dict) else {}
    selected_vad_backend = vp._normalize_vad_backend(eou_cfg.get("backend"))
    vp.logger.info(
        "[voice_core] startup version=%s backend=tater_native_satellite legacy_api=false",
        vp.__version__,
    )
    vp.logger.info(
        "[native-voice] pcm path audioop=%s input_gain=%.2f",
        "enabled" if vp._audioop is not None else "fallback",
        vp._as_float(eou_cfg.get("input_gain"), vp.DEFAULT_AUDIO_INPUT_GAIN, minimum=0.5, maximum=16.0),
    )
    if vp._audioop is None:
        if sys.version_info < (3, 13):
            vp.logger.warning(
                "[native-voice] audioop unavailable on Python %s.%s; fallback PCM math is slower and may add VAD latency",
                sys.version_info.major,
                sys.version_info.minor,
            )
        else:
            vp.logger.warning(
                "[native-voice] audioop unavailable (expected on Python 3.13+); fallback PCM math is active"
            )
    else:
        vp.logger.info("[native-voice] audioop fast path active")
    vp.logger.info(
        "[native-voice] vad backend selected=%s threshold=%.2f neg_threshold=%.2f webrtc_aggressiveness=%s",
        selected_vad_backend,
        vp._as_float(eou_cfg.get("silero_threshold"), vp.DEFAULT_SILERO_THRESHOLD, minimum=0.01, maximum=0.99),
        vp._as_float(eou_cfg.get("silero_neg_threshold"), vp.DEFAULT_SILERO_NEG_THRESHOLD, minimum=0.0, maximum=0.99),
        int(eou_cfg.get("webrtc_aggressiveness") or vp.DEFAULT_WEBRTC_VAD_AGGRESSIVENESS),
    )
    vp.logger.info(
        "[native-voice] stt backend selected=%s effective=%s faster_whisper=%s mlx_whisper=%s parakeet_onnx=%s vosk=%s wyoming=%s",
        selected_stt_backend,
        effective_stt_backend,
        "available" if vp.WhisperModel is not None else "missing",
        "available" if vp.MLXWhisper is not None else "missing",
        "available" if vp.OnnxASR is not None else "missing",
        "available" if vp.VoskModel is not None else "missing",
        "available" if vp.AsyncTcpClient is not None else "missing",
    )
    vp.logger.info("[native-voice] stt model root=%s", vp._stt_model_root())
    vp.logger.info(
        "[native-voice] acceleration selected=%s effective=%s cuda_available=%s rocm_available=%s ctranslate2_cuda=%s onnx_cuda=%s onnx_rocm=%s torch_cuda=%s torch_rocm=%s mps_available=%s faster_whisper_device=%s kokoro_provider=%s",
        vp.normalize_speech_acceleration(vp._voice_settings_with_shared_speech().get("VOICE_ACCELERATION")),
        vp._effective_speech_acceleration(),
        vp._cuda_runtime_available(),
        vp._torch_rocm_available() or vp._onnx_rocm_available(),
        vp._ctranslate2_cuda_available(),
        vp._onnx_cuda_available(),
        vp._onnx_rocm_available(),
        vp._torch_cuda_available(),
        vp._torch_rocm_available(),
        vp._mps_runtime_available(),
        vp._faster_whisper_device(),
        vp._kokoro_provider(),
    )
    vp.logger.info(
        "[native-voice] tts backend selected=%s effective=%s openai_compatible=%s chatterbox=%s kokoro=%s kokoro_torch=%s pocket_tts=%s piper=%s wyoming=%s",
        selected_tts_backend,
        effective_tts_backend,
        "configured" if vp._text(((vp._tts_config_snapshot().get("openai_compatible") or {}).get("base_url"))) else "missing",
        "configured" if vp._text(((vp._tts_config_snapshot().get("chatterbox") or {}).get("base_url"))) else "missing",
        "available" if vp.build_kokoro_pipeline is not None else "missing",
        "available" if vp.KokoroTorchPipeline is not None else "missing",
        "available" if vp.PocketTTSModel is not None else "missing",
        "available" if vp.PiperVoice is not None else "missing",
        "available" if vp.AsyncTcpClient is not None else "missing",
    )
    vp.logger.info("[native-voice] tts model root=%s", vp._tts_model_root())
    if stt_backend_note:
        vp.logger.warning("[native-voice] stt backend note: %s", stt_backend_note)
    if tts_backend_note:
        vp.logger.warning("[native-voice] tts backend note: %s", tts_backend_note)

    if selected_vad_backend in {"silero", "auto"}:
        try:
            vp.SileroVadBackend._ensure_shared()
            if vp.SileroVadBackend._shared_ready:
                owner_keys = []
                with vp.contextlib.suppress(Exception):
                    for row in vp._load_satellite_registry():
                        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                        if not vp._as_bool(meta.get("native_selected"), False):
                            continue
                        selector = vp._text(row.get("selector"))
                        if selector:
                            owner_keys.append(selector)
                if not owner_keys:
                    owner_keys = ["default"]
                preloaded = 0
                for owner_key in owner_keys:
                    if vp.SileroVadBackend.preload_owner(eou_cfg, owner_key=owner_key):
                        preloaded += 1
                vp.logger.info(
                    "[native-voice] silero VAD model pre-loaded successfully owners=%s",
                    preloaded,
                )
            else:
                vp.logger.warning("[native-voice] silero VAD model pre-load failed: %s", vp.SileroVadBackend._shared_error)
        except Exception as exc:
            vp.logger.warning("[native-voice] silero VAD model pre-load error: %s", exc)
    if selected_vad_backend in {"webrtc", "auto"}:
        try:
            vp.importlib.import_module("webrtcvad")
            vp.logger.info("[native-voice] webrtc VAD dependency available")
        except Exception as exc:
            vp.logger.warning("[native-voice] webrtc VAD dependency unavailable: %s", exc)

    vp.logger.info("[native-voice] satellite transport active=native_websocket legacy_satellite_api=disabled")
    watchdog = vp._background_tasks.get("event_loop_watchdog")
    if not isinstance(watchdog, asyncio.Task) or watchdog.done():
        vp._background_tasks["event_loop_watchdog"] = asyncio.create_task(
            _event_loop_watchdog(),
            name="voice-event-loop-watchdog",
        )


async def shutdown() -> None:
    vp = _vp()
    tasks = [task for task in list(vp._background_tasks.values()) if isinstance(task, asyncio.Task)]
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    vp._background_tasks.clear()


@router.get("/tater-ha/v1/health")
async def health() -> Dict[str, Any]:
    vp = _vp()
    return {"ok": True, "service": "voice_core", "version": vp.__version__, "ts": vp._now()}


@router.get("/tater-ha/v1/voice/config")
async def voice_config(x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    cfg = vp._voice_config_snapshot()
    return {
        "version": vp.__version__,
        "settings": vp._voice_ui_setting_fields(),
        "snapshot": cfg,
    }


@router.get("/tater-ha/v1/voice/native/status")
async def native_status(x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    selected_stt_backend = vp._selected_stt_backend()
    effective_stt_backend, stt_backend_note = vp._resolve_stt_backend()
    selected_tts_backend = vp._selected_tts_backend()
    effective_tts_backend, tts_backend_note = vp._resolve_tts_backend()
    voice_cfg = vp._voice_config_snapshot()
    eou_cfg = voice_cfg.get("eou") if isinstance(voice_cfg.get("eou"), dict) else {}
    selected_vad_backend = vp._normalize_vad_backend(eou_cfg.get("backend"))
    webrtc_vad_error = ""
    try:
        vp.importlib.import_module("webrtcvad")
    except Exception as exc:
        webrtc_vad_error = str(exc)
    selectors = []
    for key, row in vp._voice_selector_runtime.items():
        if not isinstance(row, dict):
            continue
        session = row.get("session")
        selectors.append(
            {
                "selector": key,
                "active_session_id": session.session_id if isinstance(session, VoiceSessionRuntime) else "",
                "state": session.state if isinstance(session, VoiceSessionRuntime) else "idle",
                "awaiting_announcement": bool(row.get("awaiting_announcement")),
            }
        )

    native_satellite_status: Dict[str, Any] = {}
    with contextlib.suppress(Exception):
        from .. import native_satellite

        native_satellite_status = await native_satellite.status()

    return {
        "ok": True,
        "version": vp.__version__,
        "stt_backend_selected": selected_stt_backend,
        "stt_backend_effective": effective_stt_backend,
        "stt_backend_note": vp._text(stt_backend_note),
        "acceleration": voice_cfg.get("acceleration"),
        "vad": eou_cfg,
        "vad_backend_selected": selected_vad_backend,
        "silero_vad_available": bool(vp.SileroVadBackend._shared_ready),
        "silero_vad_error": vp._text(vp.SileroVadBackend._shared_error),
        "webrtc_vad_available": not bool(webrtc_vad_error),
        "webrtc_vad_error": vp._text(webrtc_vad_error),
        "stt_model_root": vp._stt_model_root(),
        "tts_backend_selected": selected_tts_backend,
        "tts_backend_effective": effective_tts_backend,
        "tts_backend_note": vp._text(tts_backend_note),
        "tts_model_root": vp._tts_model_root(),
        "faster_whisper_available": vp.FASTER_WHISPER_IMPORT_ERROR is None,
        "faster_whisper_error": vp._text(vp.FASTER_WHISPER_IMPORT_ERROR),
        "mlx_whisper_available": vp.MLX_WHISPER_IMPORT_ERROR is None,
        "mlx_whisper_error": vp._text(vp.MLX_WHISPER_IMPORT_ERROR),
        "parakeet_onnx_available": vp.PARAKEET_ONNX_IMPORT_ERROR is None,
        "parakeet_onnx_error": vp._text(vp.PARAKEET_ONNX_IMPORT_ERROR),
        "parakeet_onnx_providers": vp._parakeet_onnx_providers(),
        "parakeet_onnx_quantization": vp._parakeet_onnx_quantization() or "fp32",
        "vosk_available": vp.VOSK_IMPORT_ERROR is None,
        "vosk_error": vp._text(vp.VOSK_IMPORT_ERROR),
        "kokoro_available": vp.KOKORO_IMPORT_ERROR is None,
        "kokoro_error": vp._text(vp.KOKORO_IMPORT_ERROR),
        "kokoro_torch_available": vp.KOKORO_TORCH_IMPORT_ERROR is None,
        "kokoro_torch_error": vp._text(vp.KOKORO_TORCH_IMPORT_ERROR),
        "pocket_tts_available": vp.POCKET_TTS_IMPORT_ERROR is None,
        "pocket_tts_error": vp._text(vp.POCKET_TTS_IMPORT_ERROR),
        "piper_available": vp.PIPER_IMPORT_ERROR is None,
        "piper_error": vp._text(vp.PIPER_IMPORT_ERROR),
        "wyoming_available": vp.WYOMING_IMPORT_ERROR is None,
        "wyoming_error": vp._text(vp.WYOMING_IMPORT_ERROR),
        "openai_compatible_available": bool(vp._text(((vp._tts_config_snapshot().get("openai_compatible") or {}).get("base_url")))),
        "selectors": selectors,
        "legacy_satellite_api": {
            "enabled": False,
            "available": False,
            "message": "Legacy satellite API support has been removed. Use Tater Native satellite firmware.",
        },
        "native_satellites": native_satellite_status,
    }


@router.websocket("/api/tater/satellite/v1/ws")
async def native_satellite_ws(websocket: WebSocket) -> None:
    from .. import native_satellite

    await native_satellite.handle_websocket(websocket)


@router.get("/api/tater/satellite/v1/status")
async def native_satellite_status(x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import native_satellite

    return await native_satellite.status()


@router.get("/api/tater/satellite/v1/firmware/{artifact_id}/{relative_path:path}")
async def native_satellite_firmware_artifact(artifact_id: str, relative_path: str) -> FileResponse:
    from .. import firmware as firmware_module

    try:
        path = firmware_module.native_ota_artifact_path(artifact_id, relative_path)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return FileResponse(path, media_type="application/octet-stream")


@router.get("/api/tater/satellite/v1/settings")
async def native_satellite_settings(selector: str = "", x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import native_satellite

    return await native_satellite.live_settings(selector=selector)


@router.post("/api/tater/satellite/v1/settings")
async def native_satellite_settings_save(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_configured_api_auth(x_tater_token)
    from .. import native_satellite

    body = payload if isinstance(payload, dict) else {}
    selector = vp._text(body.get("selector"))
    values = body.get("settings") if isinstance(body.get("settings"), dict) else body
    try:
        return await native_satellite.save_live_settings(values, selector=selector)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/tater/satellite/v1/trainer/wake-word")
async def linked_trainer_wake_word_save(
    payload: Dict[str, Any],
    x_tater_trainer_token: Optional[str] = Header(None),
) -> Dict[str, Any]:
    from .. import native_satellite, wake_trainer_link

    try:
        link = wake_trainer_link.authorize(x_tater_trainer_token)
        body = payload if isinstance(payload, dict) else {}
        wake_word_name = str(body.get("wake_word_name") or body.get("wake_word") or "").strip()
        wake_word_url = wake_trainer_link.validate_wake_word_url(body.get("wake_word_url"), link)
        result = await native_satellite.save_live_settings(
            {
                "wake_word": "custom_url",
                "wake_word_url": wake_word_url,
            }
        )
        wake_trainer_link.record_publish(
            wake_word=wake_word_name,
            wake_word_url=wake_word_url,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "ok": True,
        "wake_word": wake_word_name,
        "wake_word_url": wake_word_url,
        **result,
        "trainer_link": wake_trainer_link.status(),
    }


@router.post("/api/tater/satellite/v1/trainer/link/claim")
async def linked_trainer_claim(payload: Dict[str, Any]) -> Dict[str, Any]:
    from .. import wake_trainer_link

    body = payload if isinstance(payload, dict) else {}
    try:
        return wake_trainer_link.claim_pairing(
            pairing_code=body.get("pairing_code"),
            trainer_id=body.get("trainer_id"),
            trainer_name=body.get("trainer_name"),
            trainer_url=body.get("trainer_url"),
            publish_base_url=body.get("publish_base_url"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


@router.post("/api/tater/satellite/v1/trainer/link/unlink")
async def linked_trainer_unlink(
    x_tater_trainer_token: Optional[str] = Header(None),
) -> Dict[str, Any]:
    from .. import wake_trainer_link

    try:
        wake_trainer_link.authorize(x_tater_trainer_token)
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    return wake_trainer_link.unlink()


@router.get("/api/tater/satellite/v1/logs")
async def native_satellite_logs(
    selector: str,
    after_seq: int = 0,
    limit: int = 100,
    x_tater_token: Optional[str] = Header(None),
) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import native_satellite

    return await native_satellite.logs(selector, after_seq=after_seq, limit=limit)


@router.post("/api/tater/satellite/v1/command")
async def native_satellite_command(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import native_satellite

    selector = vp._text((payload or {}).get("selector"))
    message_type = vp._text((payload or {}).get("type") or (payload or {}).get("message_type"))
    message_payload = (payload or {}).get("payload")
    if not isinstance(message_payload, dict):
        message_payload = {}
    try:
        return await native_satellite.send_command(selector, message_type, message_payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/api/tater/satellite/v1/timers")
async def native_satellite_timers(
    selector: str = "",
    timer_id: str = "",
    room: str = "",
    name: str = "",
    duration_s: int = 0,
    x_tater_token: Optional[str] = Header(None),
) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import native_timers

    return await native_timers.status(
        selector=selector,
        timer_id=timer_id,
        room=room,
        name=name,
        duration_s=duration_s,
    )


@router.post("/api/tater/satellite/v1/timers")
async def native_satellite_timer_create(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import native_timers

    body = payload if isinstance(payload, dict) else {}
    selector = vp._text(body.get("selector"))
    duration_s = vp._as_int(body.get("duration_s") or body.get("seconds"), 0, minimum=0)
    if not selector:
        raise HTTPException(status_code=400, detail="selector is required")
    if duration_s <= 0:
        raise HTTPException(status_code=400, detail="duration_s is required")
    result = await native_timers.create_timer(
        selector,
        duration_s,
        name=vp._text(body.get("name") or body.get("label")),
        room=vp._text(body.get("room") or body.get("area_name")),
        source="api",
    )
    if not bool(result.get("ok")):
        raise HTTPException(status_code=400, detail=result)
    return result


@router.post("/api/tater/satellite/v1/timers/cancel")
async def native_satellite_timer_cancel(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import native_timers

    body = payload if isinstance(payload, dict) else {}
    return await native_timers.cancel_timer(
        timer_id=vp._text(body.get("timer_id") or body.get("id")),
        selector=vp._text(body.get("selector")),
        room=vp._text(body.get("room") or body.get("area_name")),
        name=vp._text(body.get("name") or body.get("label")),
        duration_s=vp._as_int(body.get("original_duration_s") or body.get("duration_s"), 0, minimum=0),
        cancel_all=vp._as_bool(body.get("all") or body.get("cancel_all"), False),
        source="api",
    )


@router.post("/api/tater/satellite/v1/timers/snooze")
async def native_satellite_timer_snooze(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import native_timers

    body = payload if isinstance(payload, dict) else {}
    duration_s = vp._as_int(body.get("duration_s") or body.get("seconds"), 300, minimum=1)
    return await native_timers.snooze_timer(
        timer_id=vp._text(body.get("timer_id") or body.get("id")),
        selector=vp._text(body.get("selector")),
        room=vp._text(body.get("room") or body.get("area_name")),
        name=vp._text(body.get("name") or body.get("label")),
        original_duration_s=vp._as_int(body.get("original_duration_s"), 0, minimum=0),
        duration_s=duration_s,
        source="api",
    )


@router.get("/tater-ha/v1/display/feed")
async def display_feed(request: Request, x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import display_feed as display_feed_module

    return display_feed_module.build_display_feed(request.query_params, version=vp.__version__)


@router.post("/tater-ha/v1/display/feed")
async def display_feed_post(request: Request, x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    from .. import display_feed as display_feed_module

    return display_feed_module.build_display_feed(
        _display_feed_payload_from_request(request, payload),
        version=vp.__version__,
    )


@router.get("/tater-ha/v1/display/events")
async def display_events(
    after_seq: int = 0,
    target: str = "",
    device: str = "",
    selector: str = "",
    limit: int = 20,
    x_tater_token: Optional[str] = Header(None),
) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import display_bus

    return display_bus.list_display_events(
        after_seq=after_seq,
        target=target or device or selector,
        limit=limit,
    )


@router.post("/tater-ha/v1/display/events")
async def display_events_post(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import display_bus

    return display_bus.publish_display_event(payload if isinstance(payload, dict) else {})


@router.get("/tater-ha/v1/display/snapshots/{snapshot_id}")
async def display_snapshot(snapshot_id: str, x_tater_token: Optional[str] = Header(None)) -> Response:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    return _display_snapshot_response(snapshot_id)


@router.get("/tater-ha/v1/voice/satellites")
async def satellites(x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    rows = vp._load_satellite_registry()
    return {"items": rows, "count": len(rows)}


@router.post("/tater-ha/v1/voice/satellites/select")
async def satellites_select(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    selector = vp._text((payload or {}).get("selector"))
    selected = vp._as_bool((payload or {}).get("selected"), True)
    if not selector:
        raise HTTPException(status_code=400, detail="selector is required")
    vp._set_satellite_selected(selector, selected)
    return {"ok": True, "selector": selector, "selected": selected}


@router.get("/tater-ha/v1/voice/wyoming/tts/voices")
async def wyoming_tts_voices(x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    rows, meta = vp._load_wyoming_tts_voice_catalog()
    return {"voices": rows, "meta": meta, "count": len(rows)}


@router.post("/tater-ha/v1/voice/wyoming/tts/voices/refresh")
async def wyoming_tts_voices_refresh(x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    result = await vp._native_wyoming_refresh_tts_voices()
    return {"ok": True, **result}


def _native_audio_scene_payload(raw: Any) -> Dict[str, Any]:
    vp = _vp()
    scene = raw if isinstance(raw, dict) else {}
    background = scene.get("background") if isinstance(scene.get("background"), dict) else {}
    ducking = scene.get("ducking") if isinstance(scene.get("ducking"), dict) else {}
    finish = scene.get("finish") if isinstance(scene.get("finish"), dict) else {}
    background_url = vp._text(background.get("url") or scene.get("background_url"))
    if not background_url:
        return {}

    def _percent(value: Any, default: int) -> int:
        return max(0, min(100, int(vp._as_float(value, float(default)))))

    def _milliseconds(value: Any, default: int) -> int:
        return max(0, min(10000, int(vp._as_float(value, float(default)))))

    scene_id = vp._text(scene.get("scene_id"))[:64]
    normalized: Dict[str, Any] = {
        "background": {
            "url": background_url,
            "loop": vp._as_bool(background.get("loop"), True),
            "volume_percent": _percent(background.get("volume_percent"), 60),
        },
        "foreground": {
            "volume_percent": _percent(
                (scene.get("foreground") or {}).get("volume_percent")
                if isinstance(scene.get("foreground"), dict)
                else None,
                100,
            ),
        },
        "ducking": {
            "target_percent": _percent(ducking.get("target_percent"), 35),
            "attack_ms": _milliseconds(ducking.get("attack_ms"), 150),
            "release_ms": _milliseconds(ducking.get("release_ms"), 350),
        },
        "finish": {
            "fade_ms": _milliseconds(finish.get("fade_ms"), 500),
        },
    }
    if scene_id:
        normalized["scene_id"] = scene_id
    return normalized


def _native_ducking_payload(raw: Any = None) -> Dict[str, int]:
    vp = _vp()
    source = raw if isinstance(raw, dict) else {}
    try:
        from speech_settings import get_speech_settings

        settings = get_speech_settings()
    except Exception:
        settings = {}

    def _number(key: str, default: int, maximum: int) -> int:
        value = source.get(key)
        if value is None:
            value = settings.get(f"satellite_ducking_{key}")
        return max(0, min(maximum, int(vp._as_float(value, float(default)))))

    return {
        "target_percent": _number("target_percent", 20, 100),
        "attack_ms": _number("attack_ms", 150, 10000),
        "release_ms": _number("release_ms", 350, 10000),
    }


@router.post("/api/tater/satellite/v1/play")
async def native_satellite_play(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import stereo_pairs

    selector = vp._text(payload.get("selector"))
    source_url = vp._text(payload.get("source_url"))
    audio_b64 = vp._text(payload.get("audio_b64"))
    announce_text = vp._text(payload.get("text"))
    tts_kind = vp._text(payload.get("tts_kind"))
    continue_conversation = vp._as_bool(payload.get("continue_conversation"), False)
    respect_reply_playback = vp._as_bool(payload.get("respect_reply_playback"), True)
    conversation_id = vp._text(payload.get("conversation_id"))
    filename = vp._text(payload.get("filename")) or "audio.bin"
    requested_media_type = vp._text(payload.get("media_type")).split(";", 1)[0].strip().lower()
    media_content_type = vp._text(payload.get("media_content_type")).lower()
    playback_role = vp._text(payload.get("playback_role")).lower()
    persistent_media_requested = playback_role in {"media", "music", "background"}
    media_loop = vp._as_bool(payload.get("loop"), False)
    media_volume_percent = max(
        0,
        min(100, int(vp._as_float(payload.get("volume_percent"), 100.0))),
    )
    ducking = _native_ducking_payload(payload.get("ducking"))
    timeout_s = vp._as_float(payload.get("timeout_s"), 180.0)
    audio_scene = _native_audio_scene_payload(payload.get("audio_scene"))

    if not selector:
        raise HTTPException(status_code=400, detail="selector is required")
    stereo_pair = stereo_pairs.get_pair(selector) if stereo_pairs.is_stereo_selector(selector) else {}
    if stereo_pairs.is_stereo_selector(selector) and not stereo_pair:
        raise HTTPException(status_code=404, detail="Stereo pair was not found")
    if not source_url and not audio_b64:
        raise HTTPException(status_code=400, detail="source_url or audio_b64 is required")

    fetched_media_type = ""
    media_bytes = b""
    if audio_b64:
        try:
            media_bytes = base64.b64decode(audio_b64, validate=True)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"audio_b64 is invalid: {exc}") from exc
        if not media_bytes:
            raise HTTPException(status_code=400, detail="audio_b64 decoded to empty content")
    else:
        try:
            media_bytes, fetched_media_type = await vp._download_media_source(source_url)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Failed to fetch audio source: {exc}") from exc

    media_type = requested_media_type or fetched_media_type or "application/octet-stream"

    reply_playback_target = vp.reply_playback.REPLY_PLAYBACK_DEVICE
    if respect_reply_playback:
        try:
            satellite_row = vp._satellite_lookup(selector)
            client_row: Dict[str, Any] = {}
            with contextlib.suppress(Exception):
                client_row = vp._esphome_client_row_snapshot_sync(selector)
            reply_playback_target = vp.reply_playback.resolve_reply_playback_target(
                satellite_row,
                client_row=client_row,
            )
        except Exception as exc:
            vp.logger.warning(
                "[voice_core] failed resolving reply playback target selector=%s error=%s",
                selector,
                exc,
            )
            reply_playback_target = vp.reply_playback.REPLY_PLAYBACK_DEVICE

    if respect_reply_playback and reply_playback_target == vp.reply_playback.REPLY_PLAYBACK_SILENT:
        return {
            "ok": True,
            "selector": selector,
            "source_url": source_url,
            "media_type": media_type,
            "media_content_type": media_content_type,
            "playback_mode": "silent",
            "reply_playback_target": reply_playback_target,
        }

    if respect_reply_playback and reply_playback_target != vp.reply_playback.REPLY_PLAYBACK_DEVICE:
        try:
            ha_config = vp.load_homeassistant_config(required=False)
        except Exception:
            ha_config = {"base": "", "token": ""}
        from speech_tts import play_announcement_audio_targets

        result = await play_announcement_audio_targets(
            text=announce_text or "Playing audio.",
            wav_bytes=media_bytes,
            ha_base=vp._text(ha_config.get("base")),
            token=vp._text(ha_config.get("token")),
            targets=[reply_playback_target],
            public_base_url=vp._text(vp.os.getenv("VOICE_CORE_PUBLIC_BASE_URL")),
            backend="",
            tts_kind=tts_kind,
            continue_conversation=continue_conversation,
            conversation_id=conversation_id,
            media_type=media_type,
            filename=filename,
        )
        external_ok = bool(result.get("ok")) if isinstance(result, dict) else False
        if not external_ok:
            detail = vp._text(result.get("error") if isinstance(result, dict) else "") or "External reply playback failed."
            raise HTTPException(status_code=409, detail=detail)
        return {
            "ok": True,
            "selector": selector,
            "source_url": source_url,
            "media_type": media_type,
            "media_content_type": media_content_type,
            "playback_mode": "reply_playback_external",
            "reply_playback_target": reply_playback_target,
            "audio_scene_started": False,
            "audio_scene_fallback_reason": "External reply playback does not support satellite audio scenes."
            if audio_scene
            else "",
            **result,
        }

    playback_id = uuid.uuid4().hex
    playback_url = vp._store_media_url(
        selector,
        playback_id,
        media_bytes,
        media_type=media_type,
        filename=filename,
    )
    if not playback_url:
        raise HTTPException(status_code=500, detail="Failed to store media for playback")

    audio_scene_started = False
    audio_scene_fallback_reason = ""
    media_session_started = False
    media_session_fallback_reason = ""
    audio_overlay_started = False
    if stereo_pair:
        try:
            from .. import native_satellite

            if audio_scene:
                background = (
                    audio_scene.get("background")
                    if isinstance(audio_scene.get("background"), dict)
                    else {}
                )
                background_source_url = vp._text(background.get("url"))
                background_bytes, background_media_type = await vp._download_media_source(
                    background_source_url
                )
                background_url = vp._store_media_url(
                    selector,
                    f"{playback_id}-background",
                    background_bytes,
                    media_type=background_media_type or "application/octet-stream",
                    filename="background-audio",
                )
                if not background_url:
                    raise RuntimeError("Failed to store stereo background audio for playback")
                background_result = await native_satellite.prepare_stereo_media_session(
                    stereo_pair,
                    session_id=f"{playback_id}-background",
                    media_url=background_url,
                    volume_percent=int(background.get("volume_percent", 60)),
                    loop=bool(background.get("loop", True)),
                    content_type="background",
                    channel_mode="stereo",
                )
                foreground = (
                    audio_scene.get("foreground")
                    if isinstance(audio_scene.get("foreground"), dict)
                    else {}
                )
                overlay_result = await native_satellite.start_stereo_overlay(
                    stereo_pair,
                    overlay_id=playback_id,
                    foreground_url=playback_url,
                    foreground_kind=tts_kind or "tts",
                    foreground_volume_percent=int(foreground.get("volume_percent", 100)),
                    ducking=dict(audio_scene.get("ducking") or {}),
                    start_server_us=int(background_result.get("start_server_us") or 0),
                    stop_media_when_finished=True,
                )
                result = {
                    **background_result,
                    "overlay": overlay_result,
                    "stereo_audio_scene_started": True,
                }
                audio_scene_started = True
                media_session_started = True
                audio_overlay_started = True
            elif persistent_media_requested:
                result = await native_satellite.prepare_stereo_media_session(
                    stereo_pair,
                    session_id=playback_id,
                    media_url=playback_url,
                    volume_percent=media_volume_percent,
                    loop=media_loop,
                    content_type=media_content_type or "music",
                    channel_mode="stereo",
                )
                media_session_started = True
            elif native_satellite.stereo_pair_media_active(stereo_pair):
                result = await native_satellite.start_stereo_overlay(
                    stereo_pair,
                    overlay_id=playback_id,
                    foreground_url=playback_url,
                    foreground_kind=tts_kind or "tts",
                    ducking=ducking,
                )
                audio_overlay_started = True
            else:
                result = await native_satellite.prepare_stereo_media_session(
                    stereo_pair,
                    session_id=playback_id,
                    media_url=playback_url,
                    volume_percent=100,
                    loop=False,
                    content_type="tts",
                    channel_mode="mono",
                )
                media_session_started = True
        except Exception as exc:
            raise HTTPException(status_code=409, detail=f"Failed to queue stereo-pair playback: {exc}") from exc
    elif selector.startswith("native:"):
        try:
            from .. import native_satellite

            scene_supported = bool(audio_scene) and await native_satellite.client_has_capability(
                selector,
                "audio_scenes",
            )
            if audio_scene and scene_supported:
                try:
                    background = audio_scene.get("background") if isinstance(audio_scene.get("background"), dict) else {}
                    background_source_url = vp._text(background.get("url"))
                    background_bytes, background_media_type = await vp._download_media_source(background_source_url)
                    background_url = vp._store_media_url(
                        selector,
                        playback_id,
                        background_bytes,
                        media_type=background_media_type or "application/octet-stream",
                        filename="background-audio",
                    )
                    if not background_url:
                        raise RuntimeError("Failed to store background audio for playback")

                    command_payload = {
                        "scene_id": vp._text(audio_scene.get("scene_id")) or playback_id,
                        "foreground": {
                            "url": playback_url,
                            "kind": tts_kind or "tts",
                            "volume_percent": int((audio_scene.get("foreground") or {}).get("volume_percent", 100)),
                        },
                        "background": {
                            "url": background_url,
                            "loop": bool(background.get("loop", True)),
                            "volume_percent": int(background.get("volume_percent", 60)),
                        },
                        "ducking": dict(audio_scene.get("ducking") or {}),
                        "finish": dict(audio_scene.get("finish") or {}),
                    }
                    result = await native_satellite.send_command(
                        selector,
                        "audio.scene.start",
                        command_payload,
                    )
                    audio_scene_started = True
                except Exception as exc:
                    audio_scene_fallback_reason = f"Background audio unavailable; played TTS only: {exc}"
                    vp.logger.warning(
                        "[voice_core] audio scene fallback selector=%s error=%s",
                        selector,
                        exc,
                    )
            elif audio_scene:
                audio_scene_fallback_reason = "Satellite firmware does not advertise audio scene support."

            media_session_supported = (
                persistent_media_requested
                and not audio_scene_started
                and await native_satellite.client_has_capability(
                    selector,
                    "persistent_media_sessions",
                )
            )
            if persistent_media_requested and not audio_scene_started and media_session_supported:
                command_payload = {
                    "session_id": playback_id,
                    "media": {
                        "url": playback_url,
                        "volume_percent": media_volume_percent,
                        "loop": media_loop,
                        "content_type": media_content_type or "music",
                    },
                }
                result = await native_satellite.send_command(
                    selector,
                    "media.session.start",
                    command_payload,
                )
                media_session_started = True
            elif persistent_media_requested and not audio_scene_started:
                media_session_fallback_reason = (
                    "Satellite firmware does not advertise persistent media-session support."
                )

            overlay_supported = (
                not persistent_media_requested
                and not audio_scene_started
                and not media_session_started
                and await native_satellite.client_has_capability(selector, "tts_overlays")
                and await native_satellite.client_media_session_active(selector)
            )
            if overlay_supported:
                command_payload = {
                    "overlay_id": playback_id,
                    "foreground": {
                        "url": playback_url,
                        "kind": tts_kind or "tts",
                        "volume_percent": 100,
                    },
                    "ducking": dict(ducking),
                }
                if continue_conversation:
                    command_payload["continue_conversation"] = True
                if conversation_id:
                    command_payload["conversation_id"] = conversation_id
                result = await native_satellite.send_command(
                    selector,
                    "audio.overlay.start",
                    command_payload,
                )
                audio_overlay_started = True

            if not audio_scene_started and not media_session_started and not audio_overlay_started:
                command_payload = {"url": playback_url}
                if tts_kind:
                    command_payload["tts_kind"] = tts_kind
                if not persistent_media_requested:
                    command_payload["ducking"] = dict(ducking)
                if continue_conversation:
                    command_payload["continue_conversation"] = True
                if conversation_id:
                    command_payload["conversation_id"] = conversation_id
                result = await native_satellite.send_command(selector, "play.url", command_payload)
        except Exception as exc:
            raise HTTPException(status_code=409, detail=f"Failed to queue native satellite playback: {exc}") from exc
    else:
        raise HTTPException(status_code=410, detail="Legacy satellite playback has been removed. Use a native satellite selector.")

    return {
        "ok": True,
        "selector": selector,
        "source_url": source_url,
        "playback_url": playback_url,
        "media_type": media_type,
        "media_content_type": media_content_type,
        "playback_mode": "device",
        "reply_playback_target": reply_playback_target,
        "respect_reply_playback": respect_reply_playback,
        "audio_scene_started": audio_scene_started,
        "audio_scene_fallback_reason": audio_scene_fallback_reason,
        "media_session_started": media_session_started,
        "media_session_fallback_reason": media_session_fallback_reason,
        "audio_overlay_started": audio_overlay_started,
        **result,
    }


@router.get("/api/tater/satellite/v1/intercom/targets")
@router.get("/tater-ha/v1/voice/intercom/targets")
async def intercom_targets(x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import intercom

    rows = intercom.target_options()
    return {"ok": True, "targets": rows, "count": len(rows)}


@router.get("/api/tater/satellite/v1/intercom/status")
@router.get("/tater-ha/v1/voice/intercom/status")
async def intercom_status(x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import intercom

    return await intercom.status()


@router.post("/api/tater/satellite/v1/intercom/start")
@router.post("/tater-ha/v1/voice/intercom/start")
async def intercom_start(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import intercom

    result = await intercom.start_intercom(
        source_selector=vp._text((payload or {}).get("source_selector") or (payload or {}).get("selector")),
        target_query=vp._text((payload or {}).get("target") or (payload or {}).get("target_query")),
        timeout_s=vp._as_float((payload or {}).get("timeout_s"), intercom.DEFAULT_INTERCOM_CAPTURE_TIMEOUT_S),
    )
    if not bool(result.get("ok")):
        detail = vp._text(result.get("message")) or vp._text(result.get("error")) or "Intercom failed"
        raise HTTPException(status_code=400, detail=detail)
    return result


@router.post("/api/tater/satellite/v1/intercom/cancel")
@router.post("/tater-ha/v1/voice/intercom/cancel")
async def intercom_cancel(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    from .. import intercom

    selector = vp._text((payload or {}).get("source_selector") or (payload or {}).get("selector"))
    if not selector:
        raise HTTPException(status_code=400, detail="selector is required")
    return await intercom.cancel_for_selector(selector)


@router.get("/api/tater/satellite/v1/tts/{stream_id}.wav")
async def native_tts_stream(stream_id: str) -> Response:
    vp = _vp()
    row = vp._fetch_tts_url(stream_id)
    if not isinstance(row, dict):
        raise HTTPException(status_code=404, detail="TTS stream not found or expired")

    headers = {
        "Cache-Control": "no-store, max-age=0",
        "Pragma": "no-cache",
    }
    if vp._text(row.get("stream_kind")) == "chatterbox":
        try:
            upstream_response = await asyncio.to_thread(vp._open_chatterbox_tts_stream_response, row)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=vp._text(exc) or "Chatterbox TTS stream failed") from exc
        vp._native_debug(
            f"native chatterbox tts stream fetch stream_id={vp._text(stream_id)} "
            f"session_id={vp._text(row.get('session_id'))} selector={vp._text(row.get('selector'))}"
        )
        return StreamingResponse(
            vp._iter_chatterbox_tts_stream_response(upstream_response, row),
            media_type="audio/wav",
            headers=headers,
        )

    wav_bytes = row.get("wav_bytes") if isinstance(row.get("wav_bytes"), (bytes, bytearray)) else b""
    if not wav_bytes:
        raise HTTPException(status_code=404, detail="TTS stream has no audio data")

    vp._native_debug(
        f"native tts url fetch stream_id={vp._text(stream_id)} session_id={vp._text(row.get('session_id'))} "
        f"selector={vp._text(row.get('selector'))} bytes={len(wav_bytes)}"
    )
    return Response(content=bytes(wav_bytes), media_type="audio/wav", headers=headers)


@router.get("/api/tater/satellite/v1/media/{stream_id}")
async def native_media_stream(stream_id: str) -> Response:
    vp = _vp()
    row = vp._fetch_tts_url(stream_id)
    if not isinstance(row, dict):
        raise HTTPException(status_code=404, detail="Media stream not found or expired")

    body_bytes = row.get("body_bytes") if isinstance(row.get("body_bytes"), (bytes, bytearray)) else b""
    if not body_bytes:
        raise HTTPException(status_code=404, detail="Media stream has no audio data")

    media_type = vp._text(row.get("media_type")).split(";", 1)[0].strip().lower() or "application/octet-stream"
    vp._native_debug(
        f"native media url fetch stream_id={vp._text(stream_id)} session_id={vp._text(row.get('session_id'))} "
        f"selector={vp._text(row.get('selector'))} bytes={len(body_bytes)} media_type={media_type}"
    )

    headers = {
        "Cache-Control": "no-store, max-age=0",
        "Pragma": "no-cache",
    }
    return Response(content=bytes(body_bytes), media_type=media_type, headers=headers)
