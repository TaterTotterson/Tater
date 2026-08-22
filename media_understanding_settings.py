from typing import Any, Dict

from helpers import redis_client


MEDIA_UNDERSTANDING_MODE_CHOICES = {"api", "auto", "base", "dedicated"}
DEFAULT_MEDIA_UNDERSTANDING_MODE = "base"
DEFAULT_MEDIA_UNDERSTANDING_PROVIDER = "llama_cpp"
DEFAULT_MEDIA_UNDERSTANDING_API_BASE = "http://127.0.0.1:1234"
DEFAULT_AUDIO_MAX_SECONDS = 60
DEFAULT_VIDEO_MAX_SECONDS = 15


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _settings_key(kind: str) -> str:
    token = _clean(kind).lower()
    if token not in {"audio", "video"}:
        raise ValueError("Media understanding kind must be audio or video.")
    return f"tater:media_understanding:{token}"


def _default_max_seconds(kind: str) -> int:
    return DEFAULT_AUDIO_MAX_SECONDS if _clean(kind).lower() == "audio" else DEFAULT_VIDEO_MAX_SECONDS


def get_media_understanding_settings(kind: str) -> Dict[str, Any]:
    token = _clean(kind).lower()
    shared = redis_client.hgetall(_settings_key(token)) or {}
    mode = _clean(shared.get("mode")).lower() or DEFAULT_MEDIA_UNDERSTANDING_MODE
    if mode not in MEDIA_UNDERSTANDING_MODE_CHOICES:
        mode = DEFAULT_MEDIA_UNDERSTANDING_MODE
    provider = _clean(shared.get("provider")).lower() or DEFAULT_MEDIA_UNDERSTANDING_PROVIDER
    api_base = _clean(shared.get("api_base")).rstrip("/") or DEFAULT_MEDIA_UNDERSTANDING_API_BASE
    try:
        max_seconds = int(shared.get("max_seconds") or _default_max_seconds(token))
    except (TypeError, ValueError):
        max_seconds = _default_max_seconds(token)
    return {
        "mode": mode,
        "provider": provider,
        "api_base": api_base,
        "model": _clean(shared.get("model")),
        "api_key": _clean(shared.get("api_key")) or None,
        "max_seconds": max(1, min(3600, max_seconds)),
    }


def save_media_understanding_settings(
    kind: str,
    *,
    mode: str,
    provider: str,
    api_base: str,
    model: str,
    api_key: str,
    max_seconds: Any,
) -> None:
    token = _clean(kind).lower()
    key = _settings_key(token)
    normalized_mode = _clean(mode).lower() or DEFAULT_MEDIA_UNDERSTANDING_MODE
    if normalized_mode not in MEDIA_UNDERSTANDING_MODE_CHOICES:
        normalized_mode = DEFAULT_MEDIA_UNDERSTANDING_MODE
    try:
        normalized_max_seconds = int(max_seconds or _default_max_seconds(token))
    except (TypeError, ValueError):
        normalized_max_seconds = _default_max_seconds(token)
    redis_client.hset(
        key,
        mapping={
            "mode": normalized_mode,
            "provider": _clean(provider).lower() or DEFAULT_MEDIA_UNDERSTANDING_PROVIDER,
            "api_base": _clean(api_base).rstrip("/") or DEFAULT_MEDIA_UNDERSTANDING_API_BASE,
            "model": _clean(model),
            "api_key": _clean(api_key),
            "max_seconds": str(max(1, min(3600, normalized_max_seconds))),
        },
    )


def get_audio_understanding_settings() -> Dict[str, Any]:
    return get_media_understanding_settings("audio")


def get_video_understanding_settings() -> Dict[str, Any]:
    return get_media_understanding_settings("video")
