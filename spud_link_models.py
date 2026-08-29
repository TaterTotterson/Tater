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

_RUNTIME_KIND_LABELS = {
    "llm": "LLM",
    "stt": "STT",
    "tts": "TTS",
    "vision": "Vision",
    "audio": "Audio Understanding",
    "video": "Video Understanding",
    "speaker_id": "Speaker ID",
    "emotion_id": "Emotion ID",
    "face_id": "Face ID",
}
_RUNTIME_ROLE_KINDS = {
    "base": "llm",
    "base llm": "llm",
    "llm": "llm",
    "image": "vision",
    "vision": "vision",
    "audio": "audio",
    "audio understanding": "audio",
    "video": "video",
    "video understanding": "video",
    "stt": "stt",
    "tts": "tts",
    "speaker id": "speaker_id",
    "speakerid": "speaker_id",
    "emotion id": "emotion_id",
    "emotionid": "emotion_id",
    "face id": "face_id",
    "faceid": "face_id",
}


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


def _runtime_row_kinds(row: Dict[str, Any]) -> list[str]:
    kinds: list[str] = []

    def _add(value: Any) -> None:
        token = _text(value).lower().replace("_", " ").replace("-", " ")
        kind = _RUNTIME_ROLE_KINDS.get(token)
        if kind and kind not in kinds:
            kinds.append(kind)

    _add(row.get("category"))
    _add(row.get("kind_label"))
    for role in row.get("roles") if isinstance(row.get("roles"), list) else []:
        _add(role)
    return kinds


def build_remote_runtime_model_rows(
    response: Dict[str, Any],
    *,
    routed_kinds: list[str],
) -> Optional[list[Dict[str, Any]]]:
    """Turn a Hub runtime inventory into concise Spudlet-side model rows.

    ``None`` means the Hub predates runtime inventory support, allowing callers
    to retain the older capability-only fallback. An empty list means a current
    Hub explicitly reported that none of its routed models are loaded.
    """

    loaded_models = response.get("loaded_models")
    if not isinstance(loaded_models, dict) or not isinstance(loaded_models.get("models"), list):
        return None

    routed = [kind for kind in MODEL_KINDS if kind in {_text(value).lower() for value in routed_kinds}]
    routed_set = set(routed)
    rows: list[Dict[str, Any]] = []
    covered: set[str] = set()
    for raw_row in loaded_models.get("models") or []:
        if not isinstance(raw_row, dict):
            continue
        source_kinds = [kind for kind in _runtime_row_kinds(raw_row) if kind in routed_set]
        if not source_kinds:
            continue
        model = _text(raw_row.get("model"))
        if not model:
            continue
        covered.update(source_kinds)
        provider = _text(raw_row.get("provider")) or "hub_model"
        provider_label = _text(raw_row.get("provider_label")) or "Spud Hub"
        raw_details = raw_row.get("details") if isinstance(raw_row.get("details"), list) else []
        source_details = [
            _text(value)
            for value in raw_details
            if _text(value) and not _text(value).lower().startswith("roles ")
        ]
        role_labels = [_RUNTIME_KIND_LABELS.get(kind, kind.replace("_", " ").title()) for kind in source_kinds]
        details = [f"Used for {', '.join(role_labels)}", "Loaded on Spud Hub", *source_details]
        source_device = _text(raw_row.get("device"))
        if source_device and source_device.lower() != "spud hub":
            details.append(f"Hub device {source_device}")
        rows.append(
            {
                "cache_key": "",
                "category": _text(raw_row.get("category")) or source_kinds[0],
                "kind_label": _text(raw_row.get("kind_label")) or _RUNTIME_KIND_LABELS.get(source_kinds[0], "Model"),
                "provider": f"spud_link_{provider}",
                "provider_label": provider_label,
                "model": model,
                "device": "Spud Hub",
                "memory_kind": "remote",
                "estimated_bytes": 0,
                "loaded_ts": float(raw_row.get("loaded_ts") or 0.0),
                "warning": _text(raw_row.get("warning")),
                "managed": True,
                "unloadable": False,
                "managed_by": "Spud Hub controls this model",
                "details": details,
                "roles": role_labels,
                "routed_kinds": source_kinds,
                "remote": True,
                "loaded": True,
            }
        )

    capabilities = response.get("models") if isinstance(response.get("models"), dict) else {}
    for kind in routed:
        if kind in covered:
            continue
        capability = capabilities.get(kind) if isinstance(capabilities.get(kind), dict) else {}
        if capability.get("loaded") is not True:
            continue
        label = _RUNTIME_KIND_LABELS.get(kind, kind.replace("_", " ").title())
        model = _text(capability.get("model"))
        if not model:
            continue
        provider_label = _text(capability.get("provider_label") or capability.get("provider")) or "Spud Hub"
        rows.append(
            {
                "cache_key": "",
                "category": kind,
                "kind_label": label,
                "provider": f"spud_link_{kind}",
                "provider_label": provider_label,
                "model": model,
                "device": "Spud Hub",
                "memory_kind": "remote",
                "estimated_bytes": 0,
                "managed": True,
                "unloadable": False,
                "managed_by": "Spud Hub controls this model",
                "details": [f"Used for {label}", "Loaded on Spud Hub"],
                "roles": [label],
                "routed_kinds": [kind],
                "remote": True,
                "loaded": True,
            }
        )

    rows.sort(key=lambda row: (_text(row.get("kind_label")), _text(row.get("provider_label")), _text(row.get("model"))))
    return rows


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
