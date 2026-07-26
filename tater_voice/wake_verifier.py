from __future__ import annotations

import asyncio
import difflib
import hashlib
import re
import struct
import time
from typing import Any, Dict

PACKET_MAGIC = b"TWV1"
PACKET_VERSION = 1
CODEC_PCM16_LE = 1
FLAG_ENFORCE = 0x01
PACKET_HEADER = struct.Struct("<4sBBHIII")
MAX_PACKET_SAMPLES = 16000 * 2
DEFAULT_MATCH_THRESHOLD = 0.85
DEFAULT_TIMEOUT_MS = 500


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_float(value: Any, default: float, *, minimum: float, maximum: float) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    return max(minimum, min(maximum, out))


def _as_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = int(default)
    return max(minimum, min(maximum, out))


def is_wake_verifier_packet(data: bytes) -> bool:
    return len(data or b"") >= PACKET_HEADER.size and bytes(data[:4]) == PACKET_MAGIC


def parse_packet(data: bytes) -> Dict[str, Any]:
    raw = bytes(data or b"")
    if len(raw) < PACKET_HEADER.size:
        raise ValueError("wake verifier packet is shorter than its header")
    magic, version, codec, flags, request_id, sample_rate, sample_count = PACKET_HEADER.unpack_from(raw)
    if magic != PACKET_MAGIC:
        raise ValueError("wake verifier packet magic does not match")
    if int(version) != PACKET_VERSION:
        raise ValueError(f"unsupported wake verifier packet version: {version}")
    if int(codec) != CODEC_PCM16_LE:
        raise ValueError(f"unsupported wake verifier codec: {codec}")
    if int(sample_rate) != 16000:
        raise ValueError(f"unsupported wake verifier sample rate: {sample_rate}")
    if int(sample_count) < 1 or int(sample_count) > MAX_PACKET_SAMPLES:
        raise ValueError(f"invalid wake verifier sample count: {sample_count}")
    expected_size = PACKET_HEADER.size + (int(sample_count) * 2)
    if len(raw) != expected_size:
        raise ValueError(f"wake verifier packet size mismatch: expected {expected_size}, received {len(raw)}")
    return {
        "request_id": int(request_id),
        "sample_rate": int(sample_rate),
        "sample_count": int(sample_count),
        "enforce": bool(int(flags) & FLAG_ENFORCE),
        "pcm": raw[PACKET_HEADER.size:],
    }


def build_packet(
    pcm: bytes,
    *,
    request_id: int,
    sample_rate: int = 16000,
    enforce: bool = False,
) -> bytes:
    raw = bytes(pcm or b"")
    if len(raw) % 2:
        raise ValueError("PCM16 wake verifier audio must contain complete samples")
    sample_count = len(raw) // 2
    if sample_count < 1 or sample_count > MAX_PACKET_SAMPLES:
        raise ValueError(f"invalid wake verifier sample count: {sample_count}")
    flags = FLAG_ENFORCE if enforce else 0
    return PACKET_HEADER.pack(
        PACKET_MAGIC,
        PACKET_VERSION,
        CODEC_PCM16_LE,
        flags,
        int(request_id) & 0xFFFFFFFF,
        int(sample_rate),
        sample_count,
    ) + raw


def normalize_phrase(value: Any) -> str:
    token = _text(value).replace("_", " ").replace("-", " ").lower()
    return " ".join(re.findall(r"[a-z0-9]+", token))


def transcript_match_score(transcript: Any, phrase: Any) -> float:
    target = normalize_phrase(phrase)
    words = normalize_phrase(transcript).split()
    target_words = target.split()
    if not target or not words:
        return 0.0
    # Compare short neighboring word groups so unrelated speech before or
    # after the wake phrase does not hide a valid match.
    max_words = max(1, len(target_words) + 1)
    candidates = [
        " ".join(words[start : start + size])
        for size in range(1, max_words + 1)
        for start in range(0, len(words) - size + 1)
    ]
    return max(difflib.SequenceMatcher(None, candidate, target).ratio() for candidate in candidates)


def _target_phrase(settings: Dict[str, Any]) -> str:
    explicit = normalize_phrase(settings.get("wake_verifier_phrase"))
    if explicit:
        return explicit
    profile = normalize_phrase(settings.get("wake_profile_name"))
    if profile:
        return profile
    wake_word = normalize_phrase(settings.get("wake_word"))
    return wake_word or "hey tater"


async def _verify_pcm(
    pcm: bytes,
    phrase: str,
    threshold: float,
    *,
    stt_engine: str,
    selector: str,
) -> Dict[str, Any]:
    from . import voice_pipeline as vp

    started = time.perf_counter()
    transcript = await vp._native_transcribe_wake_audio_bytes(
        backend=stt_engine,
        audio_bytes=pcm,
        audio_format={"rate": 16000, "width": 2, "channels": 1},
        language="en",
        selector=selector,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    score = transcript_match_score(transcript, phrase)
    return {
        "accepted": bool(score >= threshold),
        "transcript": transcript,
        "score": round(float(score), 4),
        "stt_ms": round(elapsed_ms, 1),
    }


async def verify_packet(data: bytes, *, selector: str = "") -> Dict[str, Any]:
    from . import native_live_settings
    from . import voice_pipeline as vp

    started = time.perf_counter()
    packet = parse_packet(data)
    settings = native_live_settings.settings_snapshot(selector)
    phrase = _target_phrase(settings)
    threshold = _as_float(
        settings.get("wake_verifier_threshold"),
        DEFAULT_MATCH_THRESHOLD,
        minimum=0.5,
        maximum=1.0,
    )
    timeout_ms = _as_int(
        settings.get("wake_verifier_timeout_ms"),
        DEFAULT_TIMEOUT_MS,
        minimum=100,
        maximum=2000,
    )
    selected_stt_engine = vp._selected_stt_backend()
    effective_stt_engine, stt_fallback_reason = vp._resolve_stt_backend()
    result: Dict[str, Any]
    try:
        result = await asyncio.wait_for(
            _verify_pcm(
                packet["pcm"],
                phrase,
                threshold,
                stt_engine=effective_stt_engine,
                selector=selector,
            ),
            timeout=float(timeout_ms) / 1000.0,
        )
        result["available"] = True
        result["reason"] = "matched" if result.get("accepted") else "transcript_mismatch"
    except asyncio.TimeoutError:
        result = {
            "accepted": True,
            "available": False,
            "transcript": "",
            "score": 0.0,
            "stt_ms": float(timeout_ms),
            "reason": "server_timeout_fail_open",
        }
    except Exception as exc:
        result = {
            "accepted": True,
            "available": False,
            "transcript": "",
            "score": 0.0,
            "stt_ms": 0.0,
            "reason": f"verifier_error_fail_open: {_text(exc) or type(exc).__name__}",
        }
    result.update(
        {
            "request_id": packet["request_id"],
            "enforce": bool(packet["enforce"]),
            "phrase": phrase,
            "threshold": threshold,
            "sample_count": packet["sample_count"],
            "audio_sha256": hashlib.sha256(packet["pcm"]).hexdigest(),
            "stt_engine": effective_stt_engine,
            "stt_engine_selected": selected_stt_engine,
            "stt_fallback_reason": stt_fallback_reason,
            "total_ms": round((time.perf_counter() - started) * 1000.0, 1),
        }
    )
    return result
