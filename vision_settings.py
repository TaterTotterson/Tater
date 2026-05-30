from typing import Any, Dict, Optional

from helpers import redis_client

VISION_SETTINGS_KEY = "verba_settings:Vision"
DEFAULT_VISION_API_BASE = "http://127.0.0.1:1234"
DEFAULT_VISION_MODEL = "qwen2.5-vl-7b-instruct"
DEFAULT_VISION_MODE = "api"
DEFAULT_VISION_PROVIDER = "openai_compatible"
VISION_MODE_CHOICES = {"api", "auto", "base", "dedicated"}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _first_non_empty(*values: Any) -> str:
    for value in values:
        cleaned = _clean(value)
        if cleaned:
            return cleaned
    return ""


def get_vision_settings(
    *,
    default_api_base: str = DEFAULT_VISION_API_BASE,
    default_model: str = DEFAULT_VISION_MODEL,
) -> Dict[str, Optional[str]]:
    shared = redis_client.hgetall(VISION_SETTINGS_KEY) or {}

    api_base = _first_non_empty(
        shared.get("api_base"),
        default_api_base,
        DEFAULT_VISION_API_BASE,
    ).rstrip("/")
    if not api_base:
        api_base = DEFAULT_VISION_API_BASE

    model = _first_non_empty(
        shared.get("model"),
        default_model,
        DEFAULT_VISION_MODEL,
    )
    if not model:
        model = DEFAULT_VISION_MODEL

    api_key = _first_non_empty(
        shared.get("api_key"),
    )

    mode = _clean(shared.get("mode") or shared.get("vision_mode")).lower() or DEFAULT_VISION_MODE
    if mode not in VISION_MODE_CHOICES:
        mode = DEFAULT_VISION_MODE

    provider = _clean(shared.get("provider") or shared.get("vision_provider")).lower() or DEFAULT_VISION_PROVIDER

    return {
        "mode": mode,
        "provider": provider,
        "api_base": api_base,
        "model": model,
        "api_key": api_key or None,
    }


def save_vision_settings(
    api_base: str,
    model: str,
    api_key: str,
    *,
    mode: str = DEFAULT_VISION_MODE,
    provider: str = DEFAULT_VISION_PROVIDER,
) -> None:
    normalized_mode = _clean(mode).lower() or DEFAULT_VISION_MODE
    if normalized_mode not in VISION_MODE_CHOICES:
        normalized_mode = DEFAULT_VISION_MODE
    normalized_provider = _clean(provider).lower() or DEFAULT_VISION_PROVIDER
    normalized_base = _clean(api_base).rstrip("/") or DEFAULT_VISION_API_BASE
    normalized_model = _clean(model) or DEFAULT_VISION_MODEL
    normalized_key = _clean(api_key)
    redis_client.hset(
        VISION_SETTINGS_KEY,
        mapping={
            "mode": normalized_mode,
            "provider": normalized_provider,
            "api_base": normalized_base,
            "model": normalized_model,
            "api_key": normalized_key,
        },
    )
