from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from .conversation import VoiceSessionRuntime


def _vp():
    return sys.modules[__package__]


def _resolve_stt_backend() -> Tuple[str, str]:
    vp = _vp()
    selected = vp._selected_stt_backend()
    ok, reason = vp._stt_backend_available(selected)
    if ok:
        return selected, ""
    if selected != "wyoming":
        wyoming_ok, _wyoming_reason = vp._stt_backend_available("wyoming")
        if wyoming_ok:
            return "wyoming", f"{selected} unavailable: {reason}. Falling back to Wyoming."
    return selected, reason


def _tts_config_snapshot() -> Dict[str, Any]:
    vp = _vp()
    cfg = vp._voice_config_snapshot()
    tts = cfg.get("tts") if isinstance(cfg.get("tts"), dict) else {}
    return tts if isinstance(tts, dict) else {}


def _selected_tts_backend(source: Optional[Dict[str, Any]] = None) -> str:
    vp = _vp()
    snapshot = source if isinstance(source, dict) else _tts_config_snapshot()
    return vp._normalize_tts_backend(snapshot.get("backend"))


def _tts_selection_from_values(values: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    vp = _vp()
    values = values if isinstance(values, dict) else {}
    merged = vp._voice_settings_with_shared_speech(values)

    backend = vp._normalize_tts_backend(merged.get("VOICE_TTS_BACKEND"))
    model = vp._text(merged.get("VOICE_TTS_MODEL"))
    voice = vp._text(merged.get("VOICE_TTS_VOICE"))

    if backend == "kokoro":
        allowed_models = [row.get("value") for row in vp._kokoro_model_option_rows() if vp._text(row.get("value"))]
        if model not in allowed_models:
            model = vp.DEFAULT_KOKORO_MODEL if vp.DEFAULT_KOKORO_MODEL in allowed_models else vp._text(allowed_models[0] if allowed_models else "")
        allowed_voices = [vp._text(row.get("value")) for row in vp._kokoro_voice_option_rows(model_id=model) if vp._text(row.get("value"))]
        if voice not in allowed_voices:
            voice = vp.DEFAULT_KOKORO_VOICE if vp.DEFAULT_KOKORO_VOICE in allowed_voices else vp._text(allowed_voices[0] if allowed_voices else "")
    elif backend == "pocket_tts":
        model = model or vp.DEFAULT_POCKET_TTS_MODEL
        allowed_voices = set(vp.POCKET_TTS_PREDEFINED_VOICES.keys())
        voice = voice if voice in allowed_voices else vp.DEFAULT_POCKET_TTS_VOICE
    elif backend == "piper":
        model = model or vp.DEFAULT_PIPER_MODEL
        voice = ""
    else:
        model = ""
        voice = vp._text(merged.get("VOICE_WYOMING_TTS_VOICE")) or vp.DEFAULT_WYOMING_TTS_VOICE

    return {
        "backend": backend,
        "model": model,
        "voice": voice,
        "wyoming_host": vp._text(merged.get("VOICE_WYOMING_TTS_HOST")) or vp.DEFAULT_WYOMING_TTS_HOST,
        "wyoming_port": vp._as_int(merged.get("VOICE_WYOMING_TTS_PORT"), vp.DEFAULT_WYOMING_TTS_PORT, minimum=1, maximum=65535),
        "wyoming_voice": vp._text(merged.get("VOICE_WYOMING_TTS_VOICE")) or vp.DEFAULT_WYOMING_TTS_VOICE,
    }


def _tts_backend_available(backend: str) -> Tuple[bool, str]:
    vp = _vp()
    token = vp._normalize_tts_backend(backend)
    if token == "wyoming":
        ok = (
            vp.AsyncTcpClient is not None
            and vp.Synthesize is not None
            and vp.WyomingAudioStart is not None
            and vp.WyomingAudioChunk is not None
            and vp.WyomingAudioStop is not None
            and vp.WyomingError is not None
        )
        return ok, vp._text(vp.WYOMING_IMPORT_ERROR) or "wyoming dependency unavailable"
    if token == "kokoro":
        return (
            vp.build_kokoro_pipeline is not None and vp.KokoroPipelineConfig is not None,
            vp._text(vp.KOKORO_IMPORT_ERROR) or "kokoro dependency unavailable",
        )
    if token == "pocket_tts":
        return vp.PocketTTSModel is not None, vp._text(vp.POCKET_TTS_IMPORT_ERROR) or "pocket-tts dependency unavailable"
    if token == "piper":
        return (
            vp.PiperVoice is not None and vp.PiperSynthesisConfig is not None and vp.piper_download_voice is not None,
            vp._text(vp.PIPER_IMPORT_ERROR) or "piper dependency unavailable",
        )
    return False, f"unsupported TTS backend: {token}"


def _resolve_tts_backend(values: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    selected = _selected_tts_backend(_tts_selection_from_values(values))
    ok, reason = _tts_backend_available(selected)
    if ok:
        return selected, ""
    if selected != "wyoming":
        wyoming_ok, _wyoming_reason = _tts_backend_available("wyoming")
        if wyoming_ok:
            return "wyoming", f"{selected} unavailable: {reason}. Falling back to Wyoming."
    return selected, reason


def _load_faster_whisper_model() -> Any:
    vp = _vp()
    if vp.WhisperModel is None:
        raise RuntimeError(f"faster-whisper dependency unavailable: {vp.FASTER_WHISPER_IMPORT_ERROR or 'unknown import error'}")

    model_source = vp._resolve_faster_whisper_model_source()
    device = vp.DEFAULT_FASTER_WHISPER_DEVICE
    compute_type = vp.DEFAULT_FASTER_WHISPER_COMPUTE_TYPE
    key = (model_source, device, compute_type)

    with vp._faster_whisper_model_lock:
        model = vp._faster_whisper_model_cache.get(key)
        if model is None:
            kwargs: Dict[str, Any] = {"device": device, "compute_type": compute_type}
            if not os.path.isdir(model_source):
                kwargs["download_root"] = vp._ensure_stt_backend_model_root("faster_whisper")
            vp.logger.info(
                "[native-voice] faster-whisper model source=%s kind=%s",
                model_source,
                "local" if os.path.isdir(model_source) else "alias",
            )
            model = vp.WhisperModel(model_source, **kwargs)
            vp._faster_whisper_model_cache[key] = model
        return model


def _load_vosk_model() -> Any:
    vp = _vp()
    if vp.VoskModel is None:
        raise RuntimeError(f"vosk dependency unavailable: {vp.VOSK_IMPORT_ERROR or 'unknown import error'}")

    model_path = vp._resolve_vosk_model_path()
    if not os.path.isdir(model_path) or not vp._looks_like_vosk_model_dir(model_path):
        raise RuntimeError(f"Vosk STT selected but no extracted model was found under {vp._stt_backend_model_root('vosk')}")

    with vp._vosk_model_lock:
        model = vp._vosk_model_cache.get(model_path)
        if model is None:
            vp.logger.info("[native-voice] vosk model source=%s", model_path)
            model = vp.VoskModel(model_path)
            vp._vosk_model_cache[model_path] = model
        return model


def _transcribe_faster_whisper_sync(audio_bytes: bytes, audio_format: Dict[str, int], language: Optional[str]) -> str:
    vp = _vp()
    pcm16, _state = vp._pcm_to_pcm16_mono_16k(audio_bytes, audio_format)
    if not pcm16:
        return ""

    np_mod = importlib.import_module("numpy")
    audio_np = np_mod.frombuffer(pcm16, dtype=np_mod.int16).astype(np_mod.float32) / 32768.0
    model = _load_faster_whisper_model()
    segments, _info = model.transcribe(
        audio_np,
        language=vp._text(language) or None,
        beam_size=1,
        vad_filter=False,
        condition_on_previous_text=False,
    )
    parts = []
    for segment in segments:
        text = vp._text(getattr(segment, "text", ""))
        if text:
            parts.append(text)
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def _vosk_result_text(payload: Any) -> str:
    vp = _vp()
    raw = vp._text(payload)
    if not raw:
        return ""
    with contextlib.suppress(Exception):
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return vp._text(parsed.get("text"))
    return ""


def _transcribe_vosk_sync(audio_bytes: bytes, audio_format: Dict[str, int]) -> str:
    vp = _vp()
    pcm16, _state = vp._pcm_to_pcm16_mono_16k(audio_bytes, audio_format)
    if not pcm16:
        return ""

    model = _load_vosk_model()
    recognizer = vp.KaldiRecognizer(model, 16000.0)
    with contextlib.suppress(Exception):
        recognizer.SetWords(False)
    parts: List[str] = []
    chunk_size = 4000
    for offset in range(0, len(pcm16), chunk_size):
        chunk = pcm16[offset : offset + chunk_size]
        if not chunk:
            continue
        if recognizer.AcceptWaveform(chunk):
            text = _vosk_result_text(recognizer.Result())
            if text:
                parts.append(text)
    final_text = _vosk_result_text(recognizer.FinalResult())
    if final_text:
        parts.append(final_text)
    return re.sub(r"\s+", " ", " ".join(part for part in parts if part)).strip()


def _wyoming_timeout_s() -> float:
    vp = _vp()
    return vp._get_float_setting("VOICE_NATIVE_WYOMING_TIMEOUT_S", vp.DEFAULT_WYOMING_TIMEOUT_SECONDS, minimum=5.0, maximum=180.0)


def _wyoming_stt_endpoint() -> Tuple[str, int]:
    vp = _vp()
    cfg = vp._voice_config_snapshot()
    stt = cfg.get("wyoming_stt") if isinstance(cfg.get("wyoming_stt"), dict) else {}
    host = vp._text(stt.get("host")) or vp.DEFAULT_WYOMING_STT_HOST
    port = int(stt.get("port") or vp.DEFAULT_WYOMING_STT_PORT)
    return host, port


def _wyoming_tts_endpoint() -> Tuple[str, int]:
    vp = _vp()
    cfg = vp._voice_config_snapshot()
    tts = cfg.get("wyoming_tts") if isinstance(cfg.get("wyoming_tts"), dict) else {}
    host = vp._text(tts.get("host")) or vp.DEFAULT_WYOMING_TTS_HOST
    port = int(tts.get("port") or vp.DEFAULT_WYOMING_TTS_PORT)
    return host, port


async def _native_wyoming_refresh_tts_voices() -> Dict[str, Any]:
    vp = _vp()
    if vp.AsyncTcpClient is None or vp.Describe is None or vp.Info is None or vp.WyomingError is None:
        raise RuntimeError(f"Wyoming describe dependency unavailable: {vp.WYOMING_IMPORT_ERROR or 'unknown import error'}")

    host, port = _wyoming_tts_endpoint()
    timeout = _wyoming_timeout_s()

    info_obj = None
    async with vp.AsyncTcpClient(host, port) as client:
        await asyncio.wait_for(client.write_event(vp.Describe().event()), timeout=timeout)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            left = max(0.1, deadline - time.monotonic())
            event = await asyncio.wait_for(client.read_event(), timeout=left)
            if event is None:
                break
            if vp.WyomingError.is_type(event.type):
                err = vp.WyomingError.from_event(event)
                raise RuntimeError(f"Wyoming TTS describe error: {err.text} ({err.code or 'unknown'})")
            if vp.Info.is_type(event.type):
                info_obj = vp.Info.from_event(event)
                break

    if info_obj is None:
        raise RuntimeError("Wyoming TTS did not return info after describe.")

    voices: List[Dict[str, str]] = []
    seen = set()
    tts_programs = getattr(info_obj, "tts", None)
    if not isinstance(tts_programs, list):
        tts_programs = []

    for program in tts_programs:
        program_name = vp._text(getattr(program, "name", None))
        voice_rows = getattr(program, "voices", None)
        if not isinstance(voice_rows, list):
            continue
        for voice in voice_rows:
            voice_name = vp._text(getattr(voice, "name", None))
            languages = [vp._text(item) for item in (getattr(voice, "languages", None) or []) if vp._text(item)]
            speakers = getattr(voice, "speakers", None)
            speaker_rows = speakers if isinstance(speakers, list) else []

            if speaker_rows:
                for speaker in speaker_rows:
                    selection = {
                        "name": voice_name,
                        "language": vp._text(languages[0]) if languages else "",
                        "speaker": vp._text(getattr(speaker, "name", None)),
                    }
                    value = vp._voice_selection_to_value(selection)
                    if not value or value in seen:
                        continue
                    seen.add(value)
                    label = vp._voice_selection_label(selection)
                    if program_name:
                        label = f"{label} • {program_name}"
                    voices.append({"value": value, "label": label})
                continue

            selection = {"name": voice_name, "language": vp._text(languages[0]) if languages else "", "speaker": ""}
            value = vp._voice_selection_to_value(selection)
            if not value or value in seen:
                continue
            seen.add(value)
            label = vp._voice_selection_label(selection)
            if program_name:
                label = f"{label} • {program_name}"
            voices.append({"value": value, "label": label})

    voices = sorted(voices, key=lambda row: vp._lower(row.get("label")))
    vp._save_wyoming_tts_voice_catalog(voices, host=host, port=port, error="")
    return {"host": host, "port": port, "voices": voices, "count": len(voices)}


async def _native_wyoming_stream_stt_task(
    token: str,
    session_id: str,
    queue: asyncio.Queue,
    audio_format: Dict[str, int],
    language: Optional[str],
    session_ref: Optional[VoiceSessionRuntime] = None,
) -> None:
    vp = _vp()
    if (
        vp.AsyncTcpClient is None
        or vp.Transcribe is None
        or vp.Transcript is None
        or vp.WyomingAudioStart is None
        or vp.WyomingAudioChunk is None
        or vp.WyomingAudioStop is None
        or vp.WyomingError is None
    ):
        return

    host, port = _wyoming_stt_endpoint()
    timeout = _wyoming_timeout_s()
    rate = int(audio_format.get("rate") or vp.DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(audio_format.get("width") or vp.DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(audio_format.get("channels") or vp.DEFAULT_VOICE_CHANNELS)

    vp._native_debug(f"STT (stream) connect {host}:{port} rate={rate} width={width} ch={channels}")

    try:
        async with vp.AsyncTcpClient(host, port) as client:
            await asyncio.wait_for(client.write_event(vp.Transcribe(language=vp._text(language) or None).event()), timeout=timeout)
            await asyncio.wait_for(client.write_event(vp.WyomingAudioStart(rate=rate, width=width, channels=channels).event()), timeout=timeout)
            stop_sent = asyncio.Event()
            result_future: asyncio.Future[str] = asyncio.get_running_loop().create_future()

            async def _update_session_transcript(*, transcript: str, final: bool) -> None:
                text_value = vp._text(transcript)
                runtime = vp._selector_runtime(token)
                if runtime and "lock" in runtime:
                    async with runtime.get("lock"):
                        sess = runtime.get("session")
                        if isinstance(sess, VoiceSessionRuntime) and sess.session_id == session_id:
                            if final:
                                sess.stt_transcript = text_value
                            else:
                                sess.partial_transcript = text_value
                                sess.partial_transcript_updates += 1
                                sess.partial_transcript_updated_ts = vp._now()
                            return
                if isinstance(session_ref, VoiceSessionRuntime):
                    with contextlib.suppress(Exception):
                        if vp._text(session_ref.session_id) == vp._text(session_id):
                            if final:
                                session_ref.stt_transcript = text_value
                            else:
                                session_ref.partial_transcript = text_value
                                session_ref.partial_transcript_updates += 1
                                session_ref.partial_transcript_updated_ts = vp._now()

            async def _reader() -> None:
                try:
                    while True:
                        event = await asyncio.wait_for(client.read_event(), timeout=timeout)
                        if event is None:
                            break
                        if vp.Transcript.is_type(event.type):
                            transcript = vp._text(vp.Transcript.from_event(event).text)
                            if stop_sent.is_set():
                                vp._native_debug(f"STT stream transcript={transcript!r}")
                                await _update_session_transcript(transcript=transcript, final=True)
                                if not result_future.done():
                                    result_future.set_result(transcript)
                                return
                            if transcript and vp._experimental_partial_stt_enabled():
                                await _update_session_transcript(transcript=transcript, final=False)
                                vp._native_debug(f"STT partial transcript selector={token} session_id={session_id} transcript={transcript!r}")
                            continue
                        if vp.WyomingError.is_type(event.type):
                            err = vp.WyomingError.from_event(event)
                            vp._native_debug(f"Wyoming STT error: {err.text}")
                            if not result_future.done():
                                result_future.set_result("")
                            return
                except Exception as exc:
                    vp._native_debug(f"STT stream reader failed: {exc}")
                    if not result_future.done():
                        result_future.set_result("")

            reader_task = asyncio.create_task(_reader())
            try:
                while True:
                    chunk = await queue.get()
                    if chunk is None:
                        break
                    await asyncio.wait_for(
                        client.write_event(vp.WyomingAudioChunk(rate=rate, width=width, channels=channels, audio=chunk).event()),
                        timeout=timeout,
                    )

                await asyncio.wait_for(client.write_event(vp.WyomingAudioStop().event()), timeout=timeout)
                stop_sent.set()
                transcript = await asyncio.wait_for(result_future, timeout=timeout)
                if transcript:
                    return
                if isinstance(session_ref, VoiceSessionRuntime):
                    fallback = vp._text(session_ref.partial_transcript)
                    if fallback:
                        session_ref.stt_transcript = fallback
                        return
            finally:
                reader_task.cancel()
                with contextlib.suppress(Exception):
                    await reader_task
    except Exception as exc:
        vp._native_debug(f"STT stream task failed: {exc}")


async def _native_transcribe_session_audio(session: VoiceSessionRuntime) -> str:
    vp = _vp()
    backend = vp._normalize_stt_backend(vp._text(session.stt_backend_effective) or vp._text(session.stt_backend))
    if backend == "wyoming":
        if session.stt_task is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(session.stt_task, timeout=15.0)
        final_text = vp._text(session.stt_transcript)
        if final_text:
            return final_text
        fallback = vp._text(session.partial_transcript)
        session.stt_transcript = fallback
        return fallback

    if session.partial_stt_task is not None:
        session.partial_stt_task.cancel()
        with contextlib.suppress(Exception):
            await session.partial_stt_task
        session.partial_stt_task = None

    audio_bytes = bytes(session.audio_buffer or b"")
    if not audio_bytes:
        session.stt_transcript = ""
        return ""

    transcript = await _native_transcribe_local_audio_bytes(
        backend=backend,
        audio_bytes=audio_bytes,
        audio_format=session.audio_format,
        language=session.language,
        selector=session.selector,
        session_id=session.session_id,
        partial=False,
    )

    session.stt_transcript = vp._text(transcript)
    vp._native_debug(f"STT {backend} transcript={session.stt_transcript!r}")
    return session.stt_transcript


async def _native_transcribe_local_audio_bytes(
    *,
    backend: str,
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    language: Optional[str],
    selector: str,
    session_id: str,
    partial: bool,
) -> str:
    vp = _vp()
    token = vp._normalize_stt_backend(backend)
    data = bytes(audio_bytes or b"")
    if not data:
        return ""

    mode_label = "partial" if partial else "final"
    if token == "faster_whisper":
        vp._native_debug(f"STT ({mode_label} faster-whisper) local selector={selector} session_id={session_id} bytes={len(data)}")
        transcript = await asyncio.to_thread(_transcribe_faster_whisper_sync, data, audio_format, language)
    elif token == "vosk":
        vp._native_debug(f"STT ({mode_label} vosk) local selector={selector} session_id={session_id} bytes={len(data)}")
        transcript = await asyncio.to_thread(_transcribe_vosk_sync, data, audio_format)
    else:
        raise RuntimeError(f"Unsupported local STT backend: {token}")

    return vp._text(transcript)


async def _native_local_partial_stt_task(
    token: str,
    session_id: str,
    *,
    session_ref: Optional[VoiceSessionRuntime] = None,
) -> None:
    vp = _vp()
    last_audio_bytes = 0
    last_partial = ""
    while True:
        try:
            await asyncio.sleep(float(vp.DEFAULT_EXPERIMENTAL_PARTIAL_STT_INTERVAL_S))
            if not vp._experimental_partial_stt_enabled():
                return

            runtime = vp._selector_runtime(token)
            lock = runtime.get("lock")
            if lock is None or not hasattr(lock, "acquire"):
                return

            async with lock:
                session = runtime.get("session")
                if not isinstance(session, VoiceSessionRuntime) or vp._text(session.session_id) != vp._text(session_id):
                    return
                if bool(session.processing):
                    return
                backend = vp._normalize_stt_backend(vp._text(session.stt_backend_effective) or vp._text(session.stt_backend))
                if backend == "wyoming":
                    return
                audio_bytes = bytes(session.audio_buffer or b"")
                audio_format = dict(session.audio_format or {})
                language = session.language
                speech_s = float(session.speech_duration_s or 0.0)
                partial_updates = int(session.partial_transcript_updates or 0)

            if speech_s < float(vp.DEFAULT_EXPERIMENTAL_PARTIAL_STT_MIN_AUDIO_S):
                continue
            if not audio_bytes:
                continue

            rate = int(audio_format.get("rate") or vp.DEFAULT_VOICE_SAMPLE_RATE_HZ)
            width = int(audio_format.get("width") or vp.DEFAULT_VOICE_SAMPLE_WIDTH)
            channels = int(audio_format.get("channels") or vp.DEFAULT_VOICE_CHANNELS)
            bytes_per_second = max(1, rate * width * channels)
            min_new_bytes = int(bytes_per_second * float(vp.DEFAULT_EXPERIMENTAL_PARTIAL_STT_MIN_NEW_AUDIO_S))
            if last_audio_bytes > 0 and (len(audio_bytes) - last_audio_bytes) < min_new_bytes and partial_updates > 0:
                continue

            transcript = await _native_transcribe_local_audio_bytes(
                backend=backend,
                audio_bytes=audio_bytes,
                audio_format=audio_format,
                language=language,
                selector=token,
                session_id=session_id,
                partial=True,
            )
            text_value = vp._text(transcript)
            if not text_value or text_value == last_partial:
                if text_value:
                    last_audio_bytes = len(audio_bytes)
                continue

            async with lock:
                session = runtime.get("session")
                if not isinstance(session, VoiceSessionRuntime) or vp._text(session.session_id) != vp._text(session_id):
                    return
                if bool(session.processing):
                    return
                session.partial_transcript = text_value
                session.partial_transcript_updates += 1
                session.partial_transcript_updated_ts = vp._now()

            if isinstance(session_ref, VoiceSessionRuntime) and session_ref is not session and vp._text(session_ref.session_id) == vp._text(session_id):
                with contextlib.suppress(Exception):
                    session_ref.partial_transcript = text_value
                    session_ref.partial_transcript_updates += 1
                    session_ref.partial_transcript_updated_ts = vp._now()

            last_partial = text_value
            last_audio_bytes = len(audio_bytes)
            vp._native_debug(f"STT partial transcript selector={token} session_id={session_id} transcript={text_value!r}")
        except asyncio.CancelledError:
            return
        except Exception as exc:
            vp._native_debug(f"local partial STT task failed selector={token} session_id={session_id} error={exc}")
            return


async def _native_wyoming_synthesize(
    text: str,
    *,
    host: Optional[str] = None,
    port: Optional[int] = None,
    voice_value: Any = None,
) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    if (
        vp.AsyncTcpClient is None
        or vp.Synthesize is None
        or vp.WyomingAudioStart is None
        or vp.WyomingAudioChunk is None
        or vp.WyomingAudioStop is None
        or vp.WyomingError is None
    ):
        raise RuntimeError(f"Wyoming client dependency unavailable: {vp.WYOMING_IMPORT_ERROR or 'unknown import error'}")

    prompt = vp._text(text)
    if not prompt:
        return b"", {}

    cfg = vp._voice_config_snapshot()
    tts = cfg.get("wyoming_tts") if isinstance(cfg.get("wyoming_tts"), dict) else {}
    host = vp._text(host) or vp._text(tts.get("host")) or vp.DEFAULT_WYOMING_TTS_HOST
    port = vp._as_int(port, int(tts.get("port") or vp.DEFAULT_WYOMING_TTS_PORT), minimum=1, maximum=65535)
    selected = vp._voice_selection_from_string(voice_value if voice_value is not None else tts.get("voice"))
    selected_label = vp._voice_selection_label(selected) if selected else "default"
    timeout = _wyoming_timeout_s()

    vp._native_debug(f"TTS connect {host}:{port} text_len={len(prompt)} voice={selected_label}")

    synth_event = None
    if selected and vp.SynthesizeVoice is not None:
        voice_obj = vp.SynthesizeVoice(
            name=vp._text(selected.get("name")) or None,
            language=vp._text(selected.get("language")) or None,
            speaker=vp._text(selected.get("speaker")) or None,
        )
        synth_event = vp.Synthesize(text=prompt, voice=voice_obj).event()
    elif selected:
        with contextlib.suppress(Exception):
            synth_event = vp.Synthesize(text=prompt, voice=selected).event()
    if synth_event is None:
        synth_event = vp.Synthesize(text=prompt).event()

    audio_out = bytearray()
    audio_format: Dict[str, Any] = {}
    saw_start = False

    async with vp.AsyncTcpClient(host, port) as client:
        await asyncio.wait_for(client.write_event(synth_event), timeout=timeout)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            left = max(0.1, deadline - time.monotonic())
            event = await asyncio.wait_for(client.read_event(), timeout=left)
            if event is None:
                break
            if vp.WyomingAudioStart.is_type(event.type):
                start = vp.WyomingAudioStart.from_event(event)
                saw_start = True
                audio_format = {"rate": start.rate, "width": start.width, "channels": start.channels}
                continue
            if vp.WyomingAudioChunk.is_type(event.type):
                chunk = vp.WyomingAudioChunk.from_event(event)
                audio_out.extend(chunk.audio or b"")
                continue
            if vp.WyomingAudioStop.is_type(event.type):
                break
            if vp.WyomingError.is_type(event.type):
                err = vp.WyomingError.from_event(event)
                raise RuntimeError(f"Wyoming TTS error: {err.text} ({err.code or 'unknown'})")

    if not saw_start:
        raise RuntimeError("Wyoming TTS did not emit audio-start")
    return bytes(audio_out), audio_format


def _float_audio_to_pcm16_bytes(audio: Any) -> bytes:
    np_mod = importlib.import_module("numpy")
    array = np_mod.asarray(audio, dtype=np_mod.float32)
    if array.ndim > 1:
        array = np_mod.squeeze(array)
    if array.ndim > 1:
        array = array.reshape(-1)
    if not array.size:
        return b""
    array = np_mod.clip(array, -1.0, 1.0)
    return (array * 32767.0).astype(np_mod.int16).tobytes()


@contextlib.contextmanager
def _temporary_env(overrides: Dict[str, Any]):
    vp = _vp()
    previous: Dict[str, Optional[str]] = {}
    try:
        for key, value in overrides.items():
            previous[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = vp._text(value)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _load_kokoro_pipeline(model_id: str) -> Any:
    vp = _vp()
    if vp.build_kokoro_pipeline is None or vp.KokoroPipelineConfig is None:
        raise RuntimeError(f"kokoro dependency unavailable: {vp.KOKORO_IMPORT_ERROR or 'unknown import error'}")

    spec = vp._kokoro_model_spec(model_id)
    variant = vp._text(spec.get("variant")) or "v1.0"
    quality = vp._text(spec.get("quality")) or "q8"
    key = (variant, quality)

    with vp._kokoro_pipeline_lock:
        pipeline = vp._kokoro_pipeline_cache.get(key)
        if pipeline is None:
            root = vp._ensure_tts_backend_model_root("kokoro")
            onnx_backend_mod = importlib.import_module("pykokoro.onnx_backend")
            vp._patch_kokoro_ssmd_parser()

            def _kokoro_cache_path(folder: Optional[str] = None):
                base = root
                if folder:
                    base = os.path.join(base, folder)
                os.makedirs(base, exist_ok=True)
                from pathlib import Path
                return Path(base)

            setattr(onnx_backend_mod, "get_user_cache_path", _kokoro_cache_path)
            cfg = vp.KokoroPipelineConfig(
                voice=vp.DEFAULT_KOKORO_VOICE,
                model_source="huggingface",
                model_variant=variant,
                model_quality=quality,
                provider=vp.DEFAULT_KOKORO_PROVIDER,
                tokenizer_config=(vp.KokoroTokenizerConfig(use_spacy=False) if vp.KokoroTokenizerConfig is not None else None),
            )
            vp.logger.info("[native-voice] kokoro model source=%s model=%s", root, model_id)
            pipeline = vp.build_kokoro_pipeline(config=cfg, eager=True)
            vp._kokoro_pipeline_cache[key] = pipeline
        return pipeline


def _load_pocket_tts_model(model_id: str) -> Any:
    vp = _vp()
    if vp.PocketTTSModel is None:
        raise RuntimeError(f"pocket-tts dependency unavailable: {vp.POCKET_TTS_IMPORT_ERROR or 'unknown import error'}")

    token = vp._text(model_id) or vp.DEFAULT_POCKET_TTS_MODEL
    with vp._pocket_tts_model_lock:
        model = vp._pocket_tts_model_cache.get(token)
        if model is None:
            root = vp._ensure_tts_backend_model_root("pocket_tts")
            hf_root = os.path.join(root, "hf")
            os.makedirs(hf_root, exist_ok=True)
            vp.logger.info("[native-voice] pocket-tts model source=%s model=%s", hf_root, token)
            with _temporary_env({"HF_HOME": hf_root, "HF_HUB_CACHE": os.path.join(hf_root, "hub"), "HUGGINGFACE_HUB_CACHE": os.path.join(hf_root, "hub")}):
                model = vp.PocketTTSModel.load_model(config=token)
            vp._pocket_tts_model_cache[token] = model
        return model


def _piper_model_paths(model_id: str) -> Tuple[str, str]:
    vp = _vp()
    root = vp._ensure_tts_backend_model_root("piper")
    token = vp._text(model_id) or vp.DEFAULT_PIPER_MODEL
    return os.path.join(root, f"{token}.onnx"), os.path.join(root, f"{token}.onnx.json")


def _load_piper_voice_model(model_id: str) -> Any:
    vp = _vp()
    if vp.PiperVoice is None or vp.PiperSynthesisConfig is None or vp.piper_download_voice is None:
        raise RuntimeError(f"piper dependency unavailable: {vp.PIPER_IMPORT_ERROR or 'unknown import error'}")

    model_path, config_path = _piper_model_paths(model_id)
    backend_root = vp._ensure_tts_backend_model_root("piper")
    if not (os.path.isfile(model_path) and os.path.isfile(config_path)):
        vp.logger.info("[native-voice] piper model missing; downloading model=%s target_root=%s", model_id, backend_root)
        vp.piper_download_voice(vp._text(model_id) or vp.DEFAULT_PIPER_MODEL, download_dir=importlib.import_module("pathlib").Path(backend_root))

    cache_key = vp._text(model_path)
    with vp._piper_voice_lock:
        voice = vp._piper_voice_cache.get(cache_key)
        if voice is None:
            vp.logger.info("[native-voice] piper model source=%s", model_path)
            voice = vp.PiperVoice.load(model_path=model_path, config_path=config_path, download_dir=backend_root)
            vp._piper_voice_cache[cache_key] = voice
        return voice


def _synthesize_kokoro_sync(text: str, model_id: str, voice: str) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    pipeline = _load_kokoro_pipeline(model_id)
    result = pipeline.run(vp._text(text), voice=vp._text(voice) or vp.DEFAULT_KOKORO_VOICE)
    audio_bytes = _float_audio_to_pcm16_bytes(getattr(result, "audio", None))
    sample_rate = int(getattr(result, "sample_rate", 24000) or 24000)
    return audio_bytes, {"rate": sample_rate, "width": 2, "channels": 1}


def _synthesize_pocket_tts_sync(text: str, model_id: str, voice: str) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return b"", {}
    model = _load_pocket_tts_model(model_id)
    root = vp._ensure_tts_backend_model_root("pocket_tts")
    hf_root = os.path.join(root, "hf")
    os.makedirs(hf_root, exist_ok=True)
    with _temporary_env({"HF_HOME": hf_root, "HF_HUB_CACHE": os.path.join(hf_root, "hub"), "HUGGINGFACE_HUB_CACHE": os.path.join(hf_root, "hub")}):
        model_state = model.get_state_for_audio_prompt(vp._text(voice) or vp.DEFAULT_POCKET_TTS_VOICE)
        audio_tensor = model.generate_audio(model_state, prompt)
    tensor = audio_tensor.detach().cpu().squeeze()
    audio_bytes = _float_audio_to_pcm16_bytes(tensor.numpy())
    return audio_bytes, {"rate": int(getattr(model, "sample_rate", 24000) or 24000), "width": 2, "channels": 1}


def _split_piper_sentences(text: str) -> List[str]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return []
    parts: List[str] = []
    start = 0
    length = len(prompt)
    i = 0
    while i < length:
        ch = prompt[i]
        if ch not in ".!?":
            i += 1
            continue
        if i + 1 < length and prompt[i + 1] in ".!?":
            i += 1
            continue
        if ch == "." and i > 0 and i + 1 < length and prompt[i - 1].isdigit() and prompt[i + 1].isdigit():
            i += 1
            continue
        j = i - 1
        while j >= start and (prompt[j].isalnum() or prompt[j] in "_-"):
            j -= 1
        token = prompt[j + 1 : i].lower()
        if ch == "." and (token in vp._PIPER_ABBREVIATIONS or (len(token) == 1 and token.isalpha())):
            i += 1
            continue
        k = i + 1
        while k < length and prompt[k] in "\"'”’)]}":
            k += 1
        if k < length and not prompt[k].isspace():
            i += 1
            continue
        segment = prompt[start:k].strip()
        if segment:
            parts.append(segment)
        while k < length and prompt[k].isspace():
            k += 1
        start = k
        i = k
    tail = prompt[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _build_piper_segment_plan(text: str) -> List[Tuple[str, float]]:
    vp = _vp()
    normalized = re.sub(r"\r\n?", "\n", vp._text(text))
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", normalized) if vp._text(part)]
    if not paragraphs:
        return []
    plan: List[Tuple[str, float]] = []
    last_paragraph_index = len(paragraphs) - 1
    for paragraph_index, paragraph in enumerate(paragraphs):
        sentences = _split_piper_sentences(paragraph)
        if not sentences:
            continue
        last_sentence_index = len(sentences) - 1
        for sentence_index, sentence in enumerate(sentences):
            pause_seconds = 0.0
            if sentence_index < last_sentence_index:
                pause_seconds = vp.DEFAULT_PIPER_SENTENCE_PAUSE_SECONDS
            elif paragraph_index < last_paragraph_index:
                pause_seconds = vp.DEFAULT_PIPER_PARAGRAPH_PAUSE_SECONDS
            plan.append((sentence, pause_seconds))
    return plan


def _build_experimental_tts_chunks(text: str) -> List[str]:
    vp = _vp()
    prompt = vp._sanitize_spoken_response_text(text)
    if len(prompt) < int(vp.DEFAULT_EXPERIMENTAL_TTS_EARLY_START_MIN_CHARS):
        return []
    sentence_plan = _build_piper_segment_plan(prompt)
    sentences = [vp._text(sentence) for sentence, _pause_s in sentence_plan if vp._text(sentence)]
    if len(sentences) < 2:
        return []
    first_chunk = sentences[0]
    remaining = list(sentences[1:])
    if len(first_chunk) < int(vp.DEFAULT_EXPERIMENTAL_TTS_EARLY_START_MIN_FIRST_CHARS) and remaining:
        first_chunk = f"{first_chunk} {remaining.pop(0)}".strip()
    chunks = [first_chunk]
    if remaining:
        remainder = " ".join(part for part in remaining if part).strip()
        if remainder:
            chunks.append(remainder)
    return [chunk for chunk in chunks if vp._text(chunk)]


def _synthesize_piper_segment_sync(voice: Any, prompt: str) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    audio_out = bytearray()
    sample_rate = 22050
    sample_width = 2
    sample_channels = 1
    syn_config = vp.PiperSynthesisConfig()
    for chunk in voice.synthesize(prompt, syn_config=syn_config):
        audio_out.extend(chunk.audio_int16_bytes)
        sample_rate = int(getattr(chunk, "sample_rate", sample_rate) or sample_rate)
        sample_width = int(getattr(chunk, "sample_width", sample_width) or sample_width)
        sample_channels = int(getattr(chunk, "sample_channels", sample_channels) or sample_channels)
    return bytes(audio_out), {"rate": sample_rate, "width": sample_width, "channels": sample_channels}


def _synthesize_piper_sync(text: str, model_id: str) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return b"", {}
    voice = _load_piper_voice_model(model_id)
    segment_plan = _build_piper_segment_plan(prompt) or [(prompt, 0.0)]
    audio_parts: List[bytes] = []
    audio_format: Dict[str, Any] = {"rate": 22050, "width": 2, "channels": 1}
    for segment_text, pause_seconds in segment_plan:
        segment_audio, segment_format = _synthesize_piper_segment_sync(voice, segment_text)
        if segment_audio:
            audio_parts.append(segment_audio)
            audio_format = dict(segment_format)
        if pause_seconds > 0:
            audio_parts.append(vp._append_pcm_silence(b"", audio_format, seconds=pause_seconds))
    padded = vp._append_pcm_silence(b"".join(audio_parts), audio_format, seconds=vp.DEFAULT_PIPER_TAIL_PAD_SECONDS)
    return padded, audio_format


async def _native_synthesize_text(
    text: str,
    *,
    session: Optional[VoiceSessionRuntime] = None,
    values: Optional[Dict[str, Any]] = None,
) -> Tuple[bytes, Dict[str, Any], str, str]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return b"", {}, "", ""

    selection = _tts_selection_from_values(values)
    selected_backend = vp._normalize_tts_backend(vp._text(session.tts_backend if isinstance(session, VoiceSessionRuntime) else "") or selection.get("backend"))
    effective_backend = vp._normalize_tts_backend(vp._text(session.tts_backend_effective if isinstance(session, VoiceSessionRuntime) else ""))
    backend_note = ""
    if not effective_backend:
        effective_backend, backend_note = _resolve_tts_backend(values)

    try:
        if effective_backend == "kokoro":
            vp._native_debug(f"TTS (kokoro) local model={selection.get('model')} voice={selection.get('voice') or vp.DEFAULT_KOKORO_VOICE}")
            audio_bytes, audio_format = await asyncio.to_thread(
                _synthesize_kokoro_sync,
                prompt,
                vp._text(selection.get("model")) or vp.DEFAULT_KOKORO_MODEL,
                vp._text(selection.get("voice")) or vp.DEFAULT_KOKORO_VOICE,
            )
            return audio_bytes, audio_format, effective_backend, backend_note
        if effective_backend == "pocket_tts":
            vp._native_debug(f"TTS (pocket-tts) local model={selection.get('model')} voice={selection.get('voice') or vp.DEFAULT_POCKET_TTS_VOICE}")
            audio_bytes, audio_format = await asyncio.to_thread(
                _synthesize_pocket_tts_sync,
                prompt,
                vp._text(selection.get("model")) or vp.DEFAULT_POCKET_TTS_MODEL,
                vp._text(selection.get("voice")) or vp.DEFAULT_POCKET_TTS_VOICE,
            )
            return audio_bytes, audio_format, effective_backend, backend_note
        if effective_backend == "piper":
            vp._native_debug(f"TTS (piper) local model={selection.get('model') or vp.DEFAULT_PIPER_MODEL}")
            audio_bytes, audio_format = await asyncio.to_thread(_synthesize_piper_sync, prompt, vp._text(selection.get("model")) or vp.DEFAULT_PIPER_MODEL)
            return audio_bytes, audio_format, effective_backend, backend_note
        audio_bytes, audio_format = await _native_wyoming_synthesize(
            prompt,
            host=vp._text(selection.get("wyoming_host")) or None,
            port=selection.get("wyoming_port"),
            voice_value=selection.get("wyoming_voice"),
        )
        return audio_bytes, audio_format, "wyoming", backend_note
    except Exception as exc:
        if effective_backend != "wyoming":
            wyoming_ok, _wyoming_reason = _tts_backend_available("wyoming")
            if wyoming_ok:
                vp.logger.warning("[native-voice] TTS backend fallback selected=%s effective=%s reason=%s", selected_backend, effective_backend, vp._text(exc))
                audio_bytes, audio_format = await _native_wyoming_synthesize(
                    prompt,
                    host=vp._text(selection.get("wyoming_host")) or None,
                    port=selection.get("wyoming_port"),
                    voice_value=selection.get("wyoming_voice"),
                )
                fallback_note = (f"{backend_note} " if backend_note else "") + f"{effective_backend} synthesis failed: {vp._text(exc)}. Falling back to Wyoming."
                return audio_bytes, audio_format, "wyoming", fallback_note.strip()
        raise


def _normalized_audio_format(audio_format: Dict[str, Any]) -> Dict[str, int]:
    vp = _vp()
    return {
        "rate": int(audio_format.get("rate") or vp.DEFAULT_VOICE_SAMPLE_RATE_HZ),
        "width": int(audio_format.get("width") or vp.DEFAULT_VOICE_SAMPLE_WIDTH),
        "channels": int(audio_format.get("channels") or vp.DEFAULT_VOICE_CHANNELS),
    }


def _trim_pcm_for_playback(audio_bytes: bytes, audio_format: Dict[str, Any]) -> Tuple[bytes, Dict[str, int]]:
    vp = _vp()
    data = bytes(audio_bytes or b"")
    fmt = _normalized_audio_format(audio_format or {})
    width = int(fmt.get("width") or vp.DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(fmt.get("channels") or vp.DEFAULT_VOICE_CHANNELS)
    if width not in {1, 2, 3, 4}:
        width = vp.DEFAULT_VOICE_SAMPLE_WIDTH
    if channels < 1 or channels > 8:
        channels = vp.DEFAULT_VOICE_CHANNELS
    fmt = {"rate": int(fmt.get("rate") or vp.DEFAULT_VOICE_SAMPLE_RATE_HZ), "width": width, "channels": channels}
    if not data:
        return b"", fmt
    block_align = max(1, width * channels)
    usable = len(data) - (len(data) % block_align)
    if usable <= 0:
        return b"", fmt
    return data[:usable], fmt


def _stitch_pcm_playback_segments(parts: List[Tuple[bytes, Dict[str, Any], float]]) -> Tuple[bytes, Dict[str, int]]:
    vp = _vp()
    segments: List[Tuple[bytes, Dict[str, int], float]] = []
    for audio_bytes, audio_format, pause_s in parts:
        data, fmt = _trim_pcm_for_playback(audio_bytes, audio_format)
        if not data:
            continue
        segments.append((data, fmt, max(0.0, float(pause_s or 0.0))))
    if not segments:
        return b"", {}

    target_fmt = dict(segments[0][1])
    if all(fmt == target_fmt for _, fmt, _ in segments):
        out = bytearray()
        for data, fmt, pause_s in segments:
            out.extend(data)
            if pause_s > 0:
                out.extend(vp._append_pcm_silence(b"", fmt, seconds=pause_s))
        return bytes(out), target_fmt

    normalized_fmt = {"rate": 16000, "width": 2, "channels": 1}
    out = bytearray()
    for data, fmt, pause_s in segments:
        normalized, _state = vp._pcm_to_pcm16_mono_16k(data, fmt)
        if not normalized:
            return b"", {}
        out.extend(normalized)
        if pause_s > 0:
            out.extend(vp._append_pcm_silence(b"", normalized_fmt, seconds=pause_s))
    return bytes(out), normalized_fmt


async def _synthesize_spoken_response_audio(
    response_text: str,
    *,
    session: VoiceSessionRuntime,
    continue_conversation: bool,
    followup_cue: str = "",
) -> Tuple[bytes, Dict[str, Any], str, str]:
    vp = _vp()
    reply = vp._sanitize_spoken_response_text(response_text)
    cue = vp._sanitize_followup_cue_text(followup_cue)
    if not continue_conversation:
        return await _native_synthesize_text(reply, session=session)

    if not cue:
        combined = vp._continued_chat_spoken_reply_text(reply, continue_conversation=True, followup_cue=cue)
        audio_bytes, audio_format, backend_used, backend_note = await _native_synthesize_text(combined, session=session)
        if audio_bytes:
            audio_bytes = vp._append_pcm_silence(audio_bytes, audio_format, seconds=vp.DEFAULT_CONTINUED_CHAT_CUE_TO_REOPEN_PAUSE_S)
        return audio_bytes, audio_format, backend_used, backend_note

    split_error = ""
    try:
        reply_audio = b""
        reply_format: Dict[str, Any] = {}
        reply_backend = ""
        reply_note = ""
        if reply:
            reply_audio, reply_format, reply_backend, reply_note = await _native_synthesize_text(reply, session=session)

        cue_audio, cue_format, cue_backend, cue_note = await _native_synthesize_text(cue, session=session)
        stitched_audio, stitched_format = _stitch_pcm_playback_segments(
            [
                (reply_audio, reply_format, vp.DEFAULT_CONTINUED_CHAT_REPLY_TO_CUE_PAUSE_S if cue_audio else 0.0),
                (cue_audio, cue_format, vp.DEFAULT_CONTINUED_CHAT_CUE_TO_REOPEN_PAUSE_S),
            ]
        )
        if stitched_audio:
            backend_used = reply_backend or cue_backend or vp._text(session.tts_backend_effective)
            backend_note = vp._merge_text_notes(
                reply_note,
                cue_note,
                (f"reply/cue TTS backend mismatch: {reply_backend}->{cue_backend}" if reply_backend and cue_backend and reply_backend != cue_backend else ""),
            )
            return stitched_audio, stitched_format, backend_used, backend_note
    except Exception as exc:
        split_error = vp._text(exc)

    combined = vp._continued_chat_spoken_reply_text(reply, continue_conversation=True, followup_cue=cue)
    audio_bytes, audio_format, backend_used, backend_note = await _native_synthesize_text(combined, session=session)
    if audio_bytes:
        audio_bytes = vp._append_pcm_silence(audio_bytes, audio_format, seconds=vp.DEFAULT_CONTINUED_CHAT_CUE_TO_REOPEN_PAUSE_S)
    backend_note = vp._merge_text_notes(
        backend_note,
        (f"followup split playback fallback: {split_error}" if split_error else ""),
        "followup cue playback used single-pass fallback",
    )
    return audio_bytes, audio_format, backend_used, backend_note
