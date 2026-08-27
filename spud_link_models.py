from __future__ import annotations

import asyncio
import base64
import io
import json
import urllib.error
import urllib.request
import wave
from typing import Any, Dict, Optional
from urllib.parse import urlparse, urlunparse

from tater_runtime_profile import remote_only_enabled


SPUD_LINK_SETTINGS_KEY = "tater:spudlink:settings:v1"
MODEL_KINDS = (
    "llm",
    "stt",
    "tts",
    "vision",
    "audio",
    "video",
    "speaker_id",
    "emotion_id",
    "face_id",
)
ROUTE_CHOICES = frozenset({"auto", "hub", "local"})


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore").strip()
    return str(value or "").strip()


def _bool(value: Any, *, default: bool = False) -> bool:
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def normalize_route(value: Any, *, default: str = "auto") -> str:
    token = _text(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"spud_hub", "remote"}:
        token = "hub"
    elif token in {"this_tater", "on_device", "device"}:
        token = "local"
    return token if token in ROUTE_CHOICES else default


def _redis_client(redis_conn: Any = None) -> Any:
    if redis_conn is not None:
        return redis_conn
    from helpers import redis_client

    return redis_client


def load_model_routing_settings(*, redis_conn: Any = None, include_secret: bool = False) -> Dict[str, Any]:
    try:
        raw = _redis_client(redis_conn).hgetall(SPUD_LINK_SETTINGS_KEY) or {}
    except Exception:
        raw = {}
    mode = _text(raw.get("mode")).lower().replace("-", "_").replace(" ", "_")
    edge_default = remote_only_enabled()
    enabled = _bool(raw.get("model_routing_enabled"), default=edge_default)
    routes: Dict[str, str] = {}
    for kind in MODEL_KINDS:
        default = "hub" if kind == "llm" else "auto"
        routes[kind] = normalize_route(raw.get(f"model_route_{kind}"), default=default)
    token = _text(raw.get("node_token"))
    result: Dict[str, Any] = {
        "available": bool(mode == "spudlet" and _text(raw.get("hub_url")) and token),
        "mode": mode,
        "enabled": enabled,
        "edge_default": edge_default,
        "hub_url": _text(raw.get("hub_url")).rstrip("/"),
        "node_token_set": bool(token),
        "routes": routes,
    }
    if include_secret:
        result["node_token"] = token
    return result


def route_for(kind: str, *, redis_conn: Any = None) -> str:
    token = _text(kind).lower()
    settings = load_model_routing_settings(redis_conn=redis_conn)
    return normalize_route((settings.get("routes") or {}).get(token), default="hub" if token == "llm" else "auto")


def should_use_hub(kind: str, *, redis_conn: Any = None) -> bool:
    token = _text(kind).lower()
    settings = load_model_routing_settings(redis_conn=redis_conn)
    if not bool(settings.get("available")):
        return False
    route = normalize_route((settings.get("routes") or {}).get(token), default="hub" if token == "llm" else "auto")
    if route == "local":
        return False
    if route == "hub":
        return True
    if token == "llm":
        return True
    return bool(settings.get("enabled"))


def allow_local_fallback(kind: str, *, redis_conn: Any = None) -> bool:
    return route_for(kind, redis_conn=redis_conn) == "auto" and not remote_only_enabled()


def _connection(*, redis_conn: Any = None) -> tuple[str, str]:
    settings = load_model_routing_settings(redis_conn=redis_conn, include_secret=True)
    hub_url = _text(settings.get("hub_url")).rstrip("/")
    if hub_url and "://" not in hub_url:
        hub_url = f"http://{hub_url}"
    parsed = urlparse(hub_url)
    path = str(parsed.path or "").rstrip("/")
    for suffix in ("/api/spudlink/v1", "/api/spudlink"):
        if path.endswith(suffix):
            path = path[: -len(suffix)].rstrip("/")
            break
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        hub_url = urlunparse(parsed._replace(path=path, params="", query="", fragment="")).rstrip("/")
    token = _text(settings.get("node_token"))
    if settings.get("mode") != "spudlet" or not hub_url or not token:
        raise RuntimeError("Spud Link model routing is not paired. Connect this Spudlet to a Spud Hub first.")
    return hub_url, token


def _request(
    path: str,
    *,
    payload: Optional[Dict[str, Any]] = None,
    redis_conn: Any = None,
    timeout: float = 180.0,
) -> tuple[bytes, str]:
    hub_url, token = _connection(redis_conn=redis_conn)
    endpoint = f"{hub_url}/api/spudlink/v1/{path.lstrip('/')}"
    body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode("utf-8")
    headers = {
        "Accept": "application/json, audio/wav",
        "Authorization": f"Bearer {token}",
        "X-SpudLink-Client": "tater-spudlet",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(endpoint, data=body, headers=headers, method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(request, timeout=max(1.0, float(timeout))) as response:
            return response.read(), _text(response.headers.get("Content-Type")).lower()
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            parsed = json.loads(exc.read().decode("utf-8", errors="ignore"))
            if isinstance(parsed, dict):
                detail = _text(parsed.get("detail") or parsed.get("error"))
        except Exception:
            detail = ""
        raise RuntimeError(detail or f"Spud Hub returned HTTP {exc.code}.") from exc
    except Exception as exc:
        raise RuntimeError(f"Could not reach the Spud Hub model gateway: {exc}") from exc


def request_json(
    path: str,
    *,
    payload: Optional[Dict[str, Any]] = None,
    redis_conn: Any = None,
    timeout: float = 180.0,
) -> Dict[str, Any]:
    raw, _content_type = _request(path, payload=payload, redis_conn=redis_conn, timeout=timeout)
    try:
        parsed = json.loads(raw.decode("utf-8", errors="ignore"))
    except Exception as exc:
        raise RuntimeError("Spud Hub returned an invalid model response.") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Spud Hub returned an invalid model response.")
    if parsed.get("ok") is False:
        raise RuntimeError(_text(parsed.get("error") or parsed.get("detail")) or "Spud Hub model request failed.")
    return parsed


async def request_json_async(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await asyncio.to_thread(request_json, *args, **kwargs)


def request_media(
    kind: str,
    *,
    data: bytes,
    filename: str,
    mimetype: str,
    prompt: str,
    redis_conn: Any = None,
    timeout: float = 180.0,
) -> Dict[str, Any]:
    return request_json(
        f"models/{_text(kind).lower()}",
        payload={
            "data_base64": base64.b64encode(bytes(data or b"")).decode("ascii"),
            "filename": _text(filename),
            "mimetype": _text(mimetype),
            "prompt": _text(prompt),
        },
        redis_conn=redis_conn,
        timeout=timeout,
    )


def pcm_to_wav(audio_bytes: bytes, audio_format: Dict[str, Any]) -> bytes:
    with io.BytesIO() as buffer:
        with wave.open(buffer, "wb") as wav_file:
            wav_file.setnchannels(max(1, int(audio_format.get("channels") or 1)))
            wav_file.setsampwidth(max(1, int(audio_format.get("width") or 2)))
            wav_file.setframerate(max(1, int(audio_format.get("rate") or 16000)))
            wav_file.writeframes(bytes(audio_bytes or b""))
        return buffer.getvalue()


def request_stt(
    *,
    audio_bytes: bytes,
    audio_format: Dict[str, Any],
    language: str = "",
    redis_conn: Any = None,
) -> Dict[str, Any]:
    wav_bytes = pcm_to_wav(audio_bytes, audio_format)
    return request_json(
        "stt/transcribe",
        payload={
            "audio_base64": base64.b64encode(wav_bytes).decode("ascii"),
            "content_type": "audio/wav",
            "language": _text(language),
        },
        redis_conn=redis_conn,
        timeout=120.0,
    )


async def request_stt_async(**kwargs: Any) -> Dict[str, Any]:
    return await asyncio.to_thread(request_stt, **kwargs)


def request_tts_wav(*, text: str, redis_conn: Any = None) -> bytes:
    raw, content_type = _request(
        "tts/speech",
        payload={"text": _text(text)},
        redis_conn=redis_conn,
        timeout=180.0,
    )
    if "audio" not in content_type and not raw.startswith(b"RIFF"):
        raise RuntimeError("Spud Hub TTS returned an invalid audio response.")
    return raw


async def request_tts_wav_async(**kwargs: Any) -> bytes:
    return await asyncio.to_thread(request_tts_wav, **kwargs)
