from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


def _vp():
    return sys.modules[__package__]


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
    dropped_startup_chunks: int = 0
    capture_started: bool = False
    last_audio_ts: float = 0.0
    state: str = field(default_factory=lambda: _vp().VOICE_STATE_LISTENING)
    processing: bool = False

    audio_buffer: bytearray = field(default_factory=bytearray)
    eou_engine: Any = None

    stt_task: Optional[asyncio.Task] = None
    stt_queue: Optional[asyncio.Queue] = None
    partial_stt_task: Optional[asyncio.Task] = None
    stt_transcript: str = ""
    partial_transcript: str = ""
    partial_transcript_updates: int = 0
    partial_transcript_updated_ts: float = 0.0
    completeness_hold_until_ts: float = 0.0
    completeness_hold_reason: str = ""
    completeness_hold_count: int = 0
    completeness_hold_partial: str = ""
    stt_latency_ms: float = 0.0
    tts_latency_ms: float = 0.0
    speech_duration_s: float = 0.0
    silence_duration_s: float = 0.0
    turn_outcome: str = ""
    finalize_reason_detail: str = ""
    max_probability: float = 0.0
    stt_end_sent: bool = False
    intent_active: bool = False
    live_tool_progress_played: bool = False
    last_tool_progress_text: str = ""
    live_tool_progress_callback: Optional[Callable[[str, Optional[Dict[str, Any]]], Any]] = None
    speaker_id: str = ""
    speaker_name: str = ""
    speaker_score: float = 0.0


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
    if session_context:
        context.update(session_context)
    if not session_has_speaker:
        for key in ("speaker_id", "speaker_name", "speaker_score"):
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
    person_name = vp._text(origin.get("person_name"))
    if device_name or area_name or speaker_name or person_name:
        speaker_line = f"- Speaker: {speaker_name}\n" if speaker_name else ""
        person_line = f"- Person: {person_name}\n" if person_name else ""
        platform_preamble = (
            "VOICE CONTEXT:\n"
            f"- Device: {device_name or '(unknown)'}\n"
            f"- Area/Room: {area_name or '(unknown)'}\n"
            f"{speaker_line}"
            f"{person_line}\n"
            "DEFAULT ROOM RULE:\n"
            "If the user asks to control lights, switches, fans, speakers, or similar devices and does not specify a room, "
            "assume they mean the Area/Room shown above.\n\n"
            "Use this as voice context only. Do not claim the user explicitly said the room unless they actually did.\n"
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
            if vp._experimental_live_tool_progress_enabled() and callback is not None:
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
            wait_callback=_wait,
        )

    response_text = vp._text((result or {}).get("text"))
    if not response_text:
        response_text = "I couldn't generate a response right now."

    await _save_history_message(
        conv_id,
        "assistant",
        {"marker": "plugin_response", "phase": "final", "content": response_text},
    )
    await _save_context(conv_id, context)
    return response_text
