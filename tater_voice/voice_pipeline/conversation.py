from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


def _vp():
    return sys.modules[__package__]


CONVERSATION_DISPLAY_MIN_INTERVAL_S = 0.09


async def _dispatch_conversation_display(
    session: "VoiceSessionRuntime",
    payload: Dict[str, Any],
) -> None:
    callback = session.conversation_display_callback
    if not callable(callback):
        return
    try:
        result = callback(dict(payload))
        if inspect.isawaitable(result):
            await result
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        with contextlib.suppress(Exception):
            _vp()._native_debug(
                f"conversation display update failed selector={session.selector} "
                f"session_id={session.session_id} error={exc}"
            )


def _conversation_display_payload(session: "VoiceSessionRuntime") -> Dict[str, Any]:
    return {
        "turn_id": str(session.session_id or ""),
        "phase": str(session.conversation_display_phase or "listening"),
        "user_text": str(session.conversation_user_text or ""),
        "user_final": bool(session.conversation_user_final),
        "assistant_text": str(session.conversation_assistant_text or ""),
        "assistant_final": bool(session.conversation_assistant_final),
        "revision": int(session.conversation_display_revision or 0),
    }


async def _flush_conversation_display_after(
    session: "VoiceSessionRuntime",
    delay_s: float,
) -> None:
    try:
        await asyncio.sleep(max(0.0, float(delay_s or 0.0)))
        payload = dict(session.conversation_display_pending or {})
        session.conversation_display_pending = None
        if payload:
            await _dispatch_conversation_display(session, payload)
            session.conversation_display_last_sent_ts = time.monotonic()
    finally:
        if session.conversation_display_flush_task is asyncio.current_task():
            session.conversation_display_flush_task = None


async def _publish_conversation_display(
    session: "VoiceSessionRuntime",
    *,
    phase: str = "",
    user_text: Optional[str] = None,
    user_final: Optional[bool] = None,
    assistant_text: Optional[str] = None,
    assistant_final: Optional[bool] = None,
    force: bool = False,
) -> None:
    """Publish one engine-independent screen transcript snapshot.

    Partial STT hypotheses replace ``user_text`` rather than append to it,
    while LLM callbacks accumulate ``assistant_text`` before calling here.
    Complete snapshots keep reconnects and dropped/coalesced updates safe.
    """
    if not callable(session.conversation_display_callback):
        return
    normalized_phase = str(phase or "").strip().lower()
    if normalized_phase in {"listening", "thinking", "replying"}:
        session.conversation_display_phase = normalized_phase
    if user_text is not None:
        session.conversation_user_text = str(user_text or "").strip()
    if user_final is not None:
        session.conversation_user_final = bool(user_final)
    if assistant_text is not None:
        session.conversation_assistant_text = str(assistant_text or "").strip()
    if assistant_final is not None:
        session.conversation_assistant_final = bool(assistant_final)
    session.conversation_display_revision += 1
    payload = _conversation_display_payload(session)
    now = time.monotonic()
    elapsed = now - float(session.conversation_display_last_sent_ts or 0.0)
    if force or session.conversation_display_last_sent_ts <= 0.0 or elapsed >= CONVERSATION_DISPLAY_MIN_INTERVAL_S:
        pending_task = session.conversation_display_flush_task
        if isinstance(pending_task, asyncio.Task) and not pending_task.done():
            pending_task.cancel()
        session.conversation_display_flush_task = None
        session.conversation_display_pending = None
        await _dispatch_conversation_display(session, payload)
        session.conversation_display_last_sent_ts = time.monotonic()
        return

    session.conversation_display_pending = payload
    pending_task = session.conversation_display_flush_task
    if not isinstance(pending_task, asyncio.Task) or pending_task.done():
        session.conversation_display_flush_task = asyncio.create_task(
            _flush_conversation_display_after(
                session,
                CONVERSATION_DISPLAY_MIN_INTERVAL_S - elapsed,
            )
        )


@dataclass
class VoiceSessionRuntime:
    selector: str
    session_id: str
    conversation_id: str
    wake_word: str
    audio_format: Dict[str, int]
    started_ts: float
    startup_gate_until_ts: float
    language: str = ""
    context: Dict[str, Any] = field(default_factory=dict)
    stt_backend: str = field(default_factory=lambda: _vp().DEFAULT_STT_BACKEND)
    stt_backend_effective: str = field(default_factory=lambda: _vp().DEFAULT_STT_BACKEND)
    tts_backend: str = field(default_factory=lambda: _vp().DEFAULT_TTS_BACKEND)
    tts_backend_effective: str = field(default_factory=lambda: _vp().DEFAULT_TTS_BACKEND)

    audio_chunks: int = 0
    audio_bytes: int = 0
    secondary_audio_chunks: int = 0
    secondary_audio_bytes: int = 0
    secondary_audio_peak_dbfs: float = -120.0
    secondary_audio_level_chunks: int = 0
    dropped_startup_chunks: int = 0
    vad_startup_ignored_chunks: int = 0
    capture_started: bool = False
    first_audio_ts: float = 0.0
    first_vad_speech_ts: float = 0.0
    last_audio_ts: float = 0.0
    state: str = field(default_factory=lambda: _vp().VOICE_STATE_LISTENING)
    processing: bool = False
    continued_chat_reopen: bool = False
    max_audio_bytes: int = field(default_factory=lambda: _vp().DEFAULT_MAX_AUDIO_BYTES)
    startup_gate_s: float = 0.0
    vad_startup_ignore_s: float = 0.0
    vad_startup_ignore_until_ts: float = 0.0
    wake_preroll_boundary_expected: bool = False
    wake_preroll_boundary_received: bool = False
    wake_preroll_boundary_deadline_ts: float = 0.0
    wake_preroll_chunks: int = 0
    wake_preroll_bytes: int = 0
    startup_preroll_s: float = 0.0
    startup_preroll_max_bytes: int = 0
    startup_preroll_buffer: bytearray = field(default_factory=bytearray)
    startup_preroll_applied: bool = False
    audio_stall_timeout_s: float = field(default_factory=lambda: _vp().DEFAULT_AUDIO_STALL_TIMEOUT_S)
    audio_stall_no_speech_timeout_s: float = field(default_factory=lambda: _vp().DEFAULT_AUDIO_STALL_NO_SPEECH_TIMEOUT_S)
    blank_wake_timeout_s: float = field(default_factory=lambda: _vp().DEFAULT_BLANK_WAKE_TIMEOUT_S)
    audio_input_gain: float = field(default_factory=lambda: _vp().DEFAULT_AUDIO_INPUT_GAIN)
    noise_highpass_prev_input: float = 0.0
    noise_highpass_prev_output: float = 0.0
    noise_suppression_chunks: int = 0
    noise_suppression_last_log_ts: float = 0.0

    audio_buffer: bytearray = field(default_factory=bytearray)
    secondary_audio_buffer: bytearray = field(default_factory=bytearray)
    eou_engine: Any = None
    spud_link_stt_stream: Any = None
    spud_link_endpointing_requested: bool = False
    spud_link_endpointing_active: bool = False
    spud_link_endpointing_reuse_stt: bool = False
    spud_link_endpointing_speech_seen: bool = False
    spud_link_endpointing_fallback: bool = False
    spud_link_endpointing_error: str = ""
    spud_link_stream_audio_started: bool = False

    stt_task: Optional[asyncio.Task] = None
    stt_queue: Optional[asyncio.Queue] = None
    stt_queue_max_chunks: int = 0
    stt_queue_dropped_chunks: int = 0
    stt_stream_unhealthy: bool = False
    stt_stream_stop_requested: bool = False
    stt_stream_fallback_reason: str = ""
    stt_stream_fallback_used: bool = False
    partial_stt_task: Optional[asyncio.Task] = None
    stt_transcript: str = ""
    partial_transcript: str = ""
    partial_transcript_updates: int = 0
    partial_transcript_updated_ts: float = 0.0
    stt_metric_recorded: bool = False
    completeness_hold_until_ts: float = 0.0
    completeness_hold_reason: str = ""
    completeness_hold_count: int = 0
    completeness_hold_partial: str = ""
    stt_latency_ms: float = 0.0
    hydra_latency_ms: float = 0.0
    tts_latency_ms: float = 0.0
    speech_duration_s: float = 0.0
    silence_duration_s: float = 0.0
    turn_outcome: str = ""
    finalize_reason_detail: str = ""
    max_probability: float = 0.0
    audio_peak_dbfs: float = -120.0
    audio_level_chunks: int = 0
    vad_audio_peak_dbfs: float = -120.0
    vad_audio_level_chunks: int = 0
    no_speech_recovered_transcript: bool = False
    no_speech_recovered_partial: bool = False
    followup_retry_prompt: bool = False
    followup_retry_reason: str = ""
    stt_end_sent: bool = False
    intent_active: bool = False
    live_tool_progress_played: bool = False
    tool_visual_sent: bool = False
    last_tool_progress_text: str = ""
    live_tool_progress_callback: Optional[Callable[[str, Optional[Dict[str, Any]]], Any]] = None
    conversation_display_callback: Optional[Callable[[Dict[str, Any]], Any]] = None
    conversation_display_phase: str = "listening"
    conversation_user_text: str = ""
    conversation_user_final: bool = False
    conversation_assistant_text: str = ""
    conversation_assistant_final: bool = False
    conversation_display_revision: int = 0
    conversation_display_last_sent_ts: float = 0.0
    conversation_display_pending: Optional[Dict[str, Any]] = None
    conversation_display_flush_task: Optional[asyncio.Task] = None
    speaker_id: str = ""
    speaker_name: str = ""
    speaker_score: float = 0.0
    speaker_match_reason: str = ""
    voice_emotion: str = ""
    voice_emotion_score: float = 0.0
    voice_emotion_prompt_hint: str = ""
    wake_arbitration_group_id: str = ""
    wake_arbitration_candidate: bool = False
    wake_arbitration_winner: bool = False
    wake_arbitration_room_key: str = ""
    wake_arbitration_room_claimed: bool = False
    wake_arbitration_peak_dbfs: float = -120.0
    wake_arbitration_audio_chunks: int = 0


def _history_key(conv_id: str) -> str:
    return f"tater:voice:conv:{conv_id}:history"


def _history_ctx_key(conv_id: str) -> str:
    return f"tater:voice:conv:{conv_id}:ctx"


def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    vp = _vp()
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        if vp._text(content.get("phase") or "final") != "final":
            return None
        payload = content.get("content")
        if isinstance(payload, str):
            return {"role": "assistant", "content": payload[:4000]}
        try:
            return {"role": "assistant", "content": json.dumps(payload, ensure_ascii=False)[:2000]}
        except Exception:
            return None
    if isinstance(content, str):
        return {"role": role, "content": content}
    return {"role": role, "content": str(content)}


async def _load_history(conv_id: str, limit: int) -> List[Dict[str, Any]]:
    vp = _vp()
    raw = vp.redis_client.lrange(_history_key(conv_id), -limit, -1) or []
    out: List[Dict[str, Any]] = []
    for line in raw:
        with contextlib.suppress(Exception):
            item = json.loads(line)
            if not isinstance(item, dict):
                continue
            role = vp._text(item.get("role") or "assistant")
            if role not in {"user", "assistant"}:
                role = "assistant"
            templ = _to_template_msg(role, item.get("content"))
            if templ:
                out.append(templ)
    return out


async def _save_history_message(conv_id: str, role: str, content: Any) -> None:
    vp = _vp()
    cfg = vp._voice_config_snapshot()
    limits = cfg.get("limits") if isinstance(cfg.get("limits"), dict) else {}
    max_store = int(limits.get("history_store") or vp.DEFAULT_HISTORY_MAX_STORE)
    ttl = int(limits.get("session_ttl_s") or vp.DEFAULT_SESSION_TTL_SECONDS)

    key = _history_key(conv_id)
    payload = json.dumps({"role": role, "content": content}, ensure_ascii=False)
    pipe = vp.redis_client.pipeline()
    pipe.rpush(key, payload)
    pipe.ltrim(key, -max_store, -1)
    pipe.expire(key, ttl)
    pipe.execute()


async def _load_context(conv_id: str) -> Dict[str, Any]:
    vp = _vp()
    raw = vp.redis_client.get(_history_ctx_key(conv_id))
    if not raw:
        return {}
    with contextlib.suppress(Exception):
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    return {}


async def _save_context(conv_id: str, ctx: Dict[str, Any]) -> None:
    vp = _vp()
    if not isinstance(ctx, dict):
        return
    cfg = vp._voice_config_snapshot()
    limits = cfg.get("limits") if isinstance(cfg.get("limits"), dict) else {}
    ttl = int(limits.get("session_ttl_s") or vp.DEFAULT_SESSION_TTL_SECONDS)
    with contextlib.suppress(Exception):
        vp.redis_client.setex(_history_ctx_key(conv_id), ttl, json.dumps(ctx, ensure_ascii=False))


async def _run_hydra_turn_for_voice(*, transcript: str, conv_id: str, session: VoiceSessionRuntime) -> str:
    vp = _vp()
    user_text = vp._text(transcript)
    if not user_text:
        return ""

    cfg = vp._voice_config_snapshot()
    limits = cfg.get("limits") if isinstance(cfg.get("limits"), dict) else {}
    max_llm = int(limits.get("history_llm") or vp.DEFAULT_HISTORY_MAX_LLM)

    await _save_history_message(conv_id, "user", user_text)
    history = await _load_history(conv_id, max_llm)
    if not history or vp._text(history[-1].get("role")) != "user":
        history.append({"role": "user", "content": user_text})

    context = await _load_context(conv_id)
    if not isinstance(context, dict):
        context = {}
    session_context = session.context if isinstance(session.context, dict) else {}
    session_has_speaker = bool(vp._text(session_context.get("speaker_id")) or vp._text(session_context.get("speaker_name")))
    session_has_emotion = bool(vp._text(session_context.get("voice_emotion")))
    if session_context:
        context.update(session_context)
    if not session_has_speaker:
        prior_speaker_id = vp._text(context.get("speaker_id"))
        prior_speaker_name = vp._text(context.get("speaker_name"))
        reuse_prior_speaker = (
            bool(getattr(session, "continued_chat_reopen", False))
            and vp._text(getattr(session, "speaker_match_reason", "")) == "too_short"
            and bool(prior_speaker_id or prior_speaker_name)
        )
        if reuse_prior_speaker:
            session.speaker_id = prior_speaker_id
            session.speaker_name = prior_speaker_name
            with contextlib.suppress(Exception):
                session.speaker_score = float(context.get("speaker_score") or 0.0)
            if isinstance(session.context, dict):
                if prior_speaker_id:
                    session.context["speaker_id"] = prior_speaker_id
                if prior_speaker_name:
                    session.context["speaker_name"] = prior_speaker_name
                if context.get("speaker_score") not in (None, ""):
                    session.context["speaker_score"] = context.get("speaker_score")
            vp._native_debug(
                f"speaker id reused for short continued-chat reply selector={session.selector} "
                f"session_id={session.session_id} speaker={prior_speaker_name or prior_speaker_id!r}"
            )
        else:
            for key in ("speaker_id", "speaker_name", "speaker_score"):
                context.pop(key, None)
    if not session_has_emotion:
        for key in ("voice_emotion", "voice_emotion_score", "voice_emotion_prompt_hint"):
            context.pop(key, None)

    registry = dict(vp.verba_registry.get_verba_registry() or {})
    max_rounds, max_tool_calls = vp.resolve_agent_limits(vp.redis_client)

    origin = {
        "platform": "homeassistant",
        "entrypoint": "voice_core",
        "session_id": session.session_id,
        "device_id": session.selector,
    }
    device_name = vp._text(context.get("device_name"))
    area_name = vp._text(context.get("area_name")) or vp._text(context.get("room_name"))
    speaker_id = vp._text(context.get("speaker_id"))
    speaker_name = vp._text(context.get("speaker_name"))
    voice_emotion_hint = vp._text(context.get("voice_emotion_prompt_hint"))
    if device_name:
        origin["device_name"] = device_name
    if area_name:
        origin["area_name"] = area_name
    if speaker_id:
        origin["speaker_id"] = speaker_id
    if speaker_name:
        origin["speaker_name"] = speaker_name
    with contextlib.suppress(Exception):
        import people as people_identity

        people_identity.apply_resolution_to_origin(
            platform="voice_core",
            origin=origin,
            redis_client=vp.redis_client,
        )

    platform_preamble = ""
    tool_platform_preamble = ""
    person_name = vp._text(origin.get("person_name"))
    has_voice_context = bool(device_name or area_name or speaker_name or person_name)
    if has_voice_context or voice_emotion_hint:
        speaker_line = f"- Speaker: {speaker_name}\n" if speaker_name else ""
        person_line = f"- Person: {person_name}\n" if person_name else ""
        emotion_line = f"- Voice tone hint: {voice_emotion_hint}\n" if voice_emotion_hint else ""
        platform_preamble = (
            "VOICE CONTEXT:\n"
            f"- Device: {device_name or '(unknown)'}\n"
            f"- Area/Room: {area_name or '(unknown)'}\n"
            f"{speaker_line}"
            f"{person_line}\n"
            f"{emotion_line}"
            "DEFAULT ROOM RULE:\n"
            "If the user asks to control lights, switches, fans, speakers, or similar devices and does not specify a room, "
            "assume they mean the Area/Room shown above.\n\n"
            "Use this as voice context only. Do not claim the user explicitly said the room. "
            "Emotion ID is an acoustic guess; if you mention it, phrase it as how the user's voice seems, not as certainty.\n"
        )
        if has_voice_context:
            tool_platform_preamble = (
                "VOICE CONTEXT:\n"
                f"- Device: {device_name or '(unknown)'}\n"
                f"- Area/Room: {area_name or '(unknown)'}\n"
                f"{speaker_line}"
                f"{person_line}\n"
                "DEFAULT ROOM RULE:\n"
                "If the user asks to control lights, switches, fans, speakers, or similar devices and does not specify a room, "
                "assume they mean the Area/Room shown above.\n\n"
                "Use this as voice context only. Do not claim the user explicitly said the room.\n"
            )

    streamed_response_parts: List[str] = []

    async def _response_chunk(chunk: str) -> None:
        chunk_text = str(chunk or "")
        if not chunk_text:
            return
        streamed_response_parts.append(chunk_text)
        await _publish_conversation_display(
            session,
            phase="replying",
            assistant_text="".join(streamed_response_parts),
            assistant_final=False,
        )

    async with vp.get_llm_client_from_env(redis_conn=vp.redis_client) as llm_client:
        async def _wait(
            func_name: str,
            plugin_obj: Any,
            wait_text: str = "",
            wait_payload: Optional[Dict[str, Any]] = None,
        ) -> None:
            progress_payload = dict(wait_payload) if isinstance(wait_payload, dict) else {}
            text = vp._text(wait_text) or vp._text(progress_payload.get("text")) or "I'm working on that now."
            progress_payload["text"] = text
            await _save_history_message(
                conv_id,
                "assistant",
                {"marker": "plugin_wait", "content": text, "payload": progress_payload},
            )
            callback = session.live_tool_progress_callback if callable(session.live_tool_progress_callback) else None
            if callback is not None:
                result = callback(text, progress_payload)
                if inspect.isawaitable(result):
                    await result

        result = await vp.run_hydra_turn(
            llm_client=llm_client,
            platform="voice_core",
            history_messages=history,
            registry=registry,
            enabled_predicate=vp.get_verba_enabled,
            context=context,
            user_text=user_text,
            scope=conv_id,
            origin=origin,
            redis_client=vp.redis_client,
            max_rounds=max_rounds,
            max_tool_calls=max_tool_calls,
            platform_preamble=platform_preamble,
            tool_platform_preamble=tool_platform_preamble,
            wait_callback=_wait,
            response_callback=_response_chunk,
        )

    response_text = vp._text((result or {}).get("text"))
    if not response_text:
        response_text = "I couldn't generate a response right now."

    await _publish_conversation_display(
        session,
        phase="replying",
        assistant_text=response_text,
        assistant_final=True,
        force=True,
    )

    await _save_history_message(
        conv_id,
        "assistant",
        {"marker": "plugin_response", "phase": "final", "content": response_text},
    )
    await _save_context(conv_id, context)
    return response_text
