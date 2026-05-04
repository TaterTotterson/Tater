from __future__ import annotations

import asyncio
import base64
import contextlib
import sys
import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Response

from .conversation import VoiceSessionRuntime


def _vp():
    return sys.modules[__package__]


router = APIRouter()


async def startup() -> None:
    vp = _vp()
    selected_stt_backend = vp._selected_stt_backend()
    effective_stt_backend, stt_backend_note = vp._resolve_stt_backend()
    selected_tts_backend = vp._selected_tts_backend()
    effective_tts_backend, tts_backend_note = vp._resolve_tts_backend()
    voice_cfg = vp._voice_config_snapshot()
    eou_cfg = voice_cfg.get("eou") if isinstance(voice_cfg.get("eou"), dict) else {}
    selected_vad_backend = vp._normalize_vad_backend(eou_cfg.get("backend"))
    vp.logger.info(
        "[voice_core] startup version=%s backend=native_voice_pipeline esphome_native=true",
        vp.__version__,
    )
    vp.logger.info(
        "[native-voice] pcm path audioop=%s input_gain=%.2f",
        "enabled" if vp._audioop is not None else "fallback",
        float(vp.DEFAULT_AUDIO_INPUT_GAIN),
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
        float(eou_cfg.get("silero_threshold") or vp.DEFAULT_SILERO_THRESHOLD),
        float(eou_cfg.get("silero_neg_threshold") or vp.DEFAULT_SILERO_NEG_THRESHOLD),
        int(eou_cfg.get("webrtc_aggressiveness") or vp.DEFAULT_WEBRTC_VAD_AGGRESSIVENESS),
    )
    vp.logger.info(
        "[native-voice] stt backend selected=%s effective=%s faster_whisper=%s vosk=%s wyoming=%s",
        selected_stt_backend,
        effective_stt_backend,
        "available" if vp.WhisperModel is not None else "missing",
        "available" if vp.VoskModel is not None else "missing",
        "available" if vp.AsyncTcpClient is not None else "missing",
    )
    vp.logger.info("[native-voice] stt model root=%s", vp._stt_model_root())
    vp.logger.info(
        "[native-voice] acceleration selected=%s effective=%s cuda_available=%s faster_whisper_device=%s kokoro_provider=%s",
        vp.normalize_speech_acceleration(vp._voice_settings_with_shared_speech().get("VOICE_ACCELERATION")),
        vp._effective_speech_acceleration(),
        vp._cuda_runtime_available(),
        vp._faster_whisper_device(),
        vp._kokoro_provider(),
    )
    vp.logger.info(
        "[native-voice] tts backend selected=%s effective=%s openai_compatible=%s kokoro=%s pocket_tts=%s piper=%s wyoming=%s",
        selected_tts_backend,
        effective_tts_backend,
        "configured" if vp._text(((vp._tts_config_snapshot().get("openai_compatible") or {}).get("base_url"))) else "missing",
        "available" if vp.build_kokoro_pipeline is not None else "missing",
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
                vp.logger.info("[native-voice] silero VAD model pre-loaded successfully")
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

    if "esphome_bootstrap" not in vp._background_tasks or vp._background_tasks["esphome_bootstrap"].done():
        vp._background_tasks["esphome_bootstrap"] = asyncio.create_task(vp._esphome_bootstrap_reconnect())
    if "discovery" not in vp._background_tasks or vp._background_tasks["discovery"].done():
        vp._background_tasks["discovery"] = asyncio.create_task(vp._discovery_loop())
    if "esphome" not in vp._background_tasks or vp._background_tasks["esphome"].done():
        vp._background_tasks["esphome"] = asyncio.create_task(vp._esphome_loop())


async def shutdown() -> None:
    vp = _vp()
    for key, task in list(vp._background_tasks.items()):
        if isinstance(task, asyncio.Task):
            task.cancel()
    for task in list(vp._background_tasks.values()):
        if isinstance(task, asyncio.Task):
            with contextlib.suppress(Exception):
                await task
    vp._background_tasks.clear()

    with contextlib.suppress(Exception):
        await vp._esphome_disconnect_all("portal_shutdown")


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
                "state": session.state if isinstance(session, VoiceSessionRuntime) else vp.VOICE_STATE_IDLE,
                "awaiting_announcement": bool(row.get("awaiting_announcement")),
            }
        )

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
        "vosk_available": vp.VOSK_IMPORT_ERROR is None,
        "vosk_error": vp._text(vp.VOSK_IMPORT_ERROR),
        "kokoro_available": vp.KOKORO_IMPORT_ERROR is None,
        "kokoro_error": vp._text(vp.KOKORO_IMPORT_ERROR),
        "pocket_tts_available": vp.POCKET_TTS_IMPORT_ERROR is None,
        "pocket_tts_error": vp._text(vp.POCKET_TTS_IMPORT_ERROR),
        "piper_available": vp.PIPER_IMPORT_ERROR is None,
        "piper_error": vp._text(vp.PIPER_IMPORT_ERROR),
        "wyoming_available": vp.WYOMING_IMPORT_ERROR is None,
        "wyoming_error": vp._text(vp.WYOMING_IMPORT_ERROR),
        "openai_compatible_available": bool(vp._text(((vp._tts_config_snapshot().get("openai_compatible") or {}).get("base_url")))),
        "selectors": selectors,
        "discovery": dict(vp._esphome_device_runtime.discovery_stats()),
        "esphome": vp._esphome_status(),
    }


@router.get("/tater-ha/v1/voice/esphome/status")
async def esphome_status(x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    return vp._esphome_status()


@router.post("/tater-ha/v1/voice/esphome/entities")
async def esphome_entities(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    selector = vp._text((payload or {}).get("selector"))
    if not selector:
        raise HTTPException(status_code=400, detail="selector is required")
    try:
        result = vp._esphome_entities_for_selector(selector)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, **result}


@router.post("/tater-ha/v1/voice/esphome/entities/command")
async def esphome_entities_command(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    selector = vp._text((payload or {}).get("selector"))
    entity_key = vp._text((payload or {}).get("entity_key") or (payload or {}).get("key"))
    command = vp._text((payload or {}).get("command"))
    value = (payload or {}).get("value")
    try:
        result = await vp._esphome_command_entity(
            selector,
            entity_key=entity_key,
            command=command,
            value=value,
            options=payload if isinstance(payload, dict) else {},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, **result}


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
    return {"ok": True, "selector": selector, "selected": selected, "status": vp._esphome_status()}


@router.post("/tater-ha/v1/voice/esphome/reconcile")
async def esphome_reconcile(payload: Optional[Dict[str, Any]] = None, x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    force = vp._as_bool((payload or {}).get("force"), True)
    return await vp._esphome_reconcile_once(force=force)


@router.post("/tater-ha/v1/voice/esphome/disconnect")
async def esphome_disconnect(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)
    selector = vp._text((payload or {}).get("selector"))
    if not selector:
        raise HTTPException(status_code=400, detail="selector is required")
    await vp._esphome_disconnect_selector(selector, reason="manual_endpoint")
    return {"ok": True, "selector": selector, "status": vp._esphome_status()}


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


@router.post("/tater-ha/v1/voice/esphome/play")
async def esphome_play(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    vp = _vp()
    vp._require_api_auth(x_tater_token)

    selector = vp._text(payload.get("selector"))
    source_url = vp._text(payload.get("source_url"))
    audio_b64 = vp._text(payload.get("audio_b64"))
    announce_text = vp._text(payload.get("text"))
    filename = vp._text(payload.get("filename")) or "audio.bin"
    requested_media_type = vp._text(payload.get("media_type")).split(";", 1)[0].strip().lower()
    timeout_s = vp._as_float(payload.get("timeout_s"), 180.0)

    if not selector:
        raise HTTPException(status_code=400, detail="selector is required")
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

    try:
        result = await vp._queue_selector_audio_url(
            selector,
            playback_url,
            text=announce_text,
            timeout_s=timeout_s,
        )
    except Exception as exc:
        raise HTTPException(status_code=409, detail=f"Failed to queue selector playback: {exc}") from exc

    return {
        "ok": True,
        "selector": selector,
        "source_url": source_url,
        "playback_url": playback_url,
        "media_type": media_type,
        **result,
    }


@router.get("/tater-ha/v1/voice/esphome/tts/{stream_id}.wav")
async def esphome_tts_stream(stream_id: str) -> Response:
    vp = _vp()
    row = vp._fetch_tts_url(stream_id)
    if not isinstance(row, dict):
        raise HTTPException(status_code=404, detail="TTS stream not found or expired")

    wav_bytes = row.get("wav_bytes") if isinstance(row.get("wav_bytes"), (bytes, bytearray)) else b""
    if not wav_bytes:
        raise HTTPException(status_code=404, detail="TTS stream has no audio data")

    vp._native_debug(
        f"esphome tts url fetch stream_id={vp._text(stream_id)} session_id={vp._text(row.get('session_id'))} "
        f"selector={vp._text(row.get('selector'))} bytes={len(wav_bytes)}"
    )

    headers = {
        "Cache-Control": "no-store, max-age=0",
        "Pragma": "no-cache",
    }
    return Response(content=bytes(wav_bytes), media_type="audio/wav", headers=headers)


@router.get("/tater-ha/v1/voice/esphome/media/{stream_id}")
async def esphome_media_stream(stream_id: str) -> Response:
    vp = _vp()
    row = vp._fetch_tts_url(stream_id)
    if not isinstance(row, dict):
        raise HTTPException(status_code=404, detail="Media stream not found or expired")

    body_bytes = row.get("body_bytes") if isinstance(row.get("body_bytes"), (bytes, bytearray)) else b""
    if not body_bytes:
        raise HTTPException(status_code=404, detail="Media stream has no audio data")

    media_type = vp._text(row.get("media_type")).split(";", 1)[0].strip().lower() or "application/octet-stream"
    vp._native_debug(
        f"esphome media url fetch stream_id={vp._text(stream_id)} session_id={vp._text(row.get('session_id'))} "
        f"selector={vp._text(row.get('selector'))} bytes={len(body_bytes)} media_type={media_type}"
    )

    headers = {
        "Cache-Control": "no-store, max-age=0",
        "Pragma": "no-cache",
    }
    return Response(content=bytes(body_bytes), media_type=media_type, headers=headers)
