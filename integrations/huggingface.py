from __future__ import annotations

import os
from typing import Any, Dict, Optional

import requests

from helpers import redis_client

HUGGINGFACE_SETTINGS_KEY = "huggingface_settings"
HUGGINGFACE_API_BASE = "https://huggingface.co"
HUGGINGFACE_DEFAULT_TIMEOUT_SECONDS = 12

INTEGRATION = {
    "id": "huggingface",
    "name": "Hugging Face",
    "description": "Optional access token used when Tater auto-downloads Hugging Face models.",
    "badge": "HF",
    "order": 68,
    "fields": [
        {
            "key": "huggingface_token",
            "label": "Hugging Face Access Token",
            "type": "password",
            "default": "",
            "placeholder": "hf_...",
            "description": "Used for private/gated model downloads and higher Hub rate limits.",
        },
    ],
    "actions": [
        {
            "id": "test",
            "label": "Test Hugging Face",
            "status": "Checks the saved Hugging Face token with the Hub API.",
        },
    ],
}


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="ignore").strip()
        except Exception:
            return str(value or "").strip()
    return str(value or "").strip()


def _normalize_raw_settings(raw: Dict[str, Any]) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    for key, value in (raw or {}).items():
        key_text = _text(key)
        if key_text:
            normalized[key_text] = _text(value)
            normalized[key_text.lower()] = _text(value)
    return normalized


def _read_raw(client: Any = None) -> Dict[str, Any]:
    store = client or redis_client
    try:
        raw = store.hgetall(HUGGINGFACE_SETTINGS_KEY) or {}
    except Exception:
        raw = {}
    return raw if isinstance(raw, dict) else {}


def read_huggingface_settings(client: Any = None) -> Dict[str, Any]:
    raw = _normalize_raw_settings(_read_raw(client))
    token = (
        raw.get("HUGGINGFACE_TOKEN")
        or raw.get("huggingface_token")
        or raw.get("HF_TOKEN")
        or raw.get("hf_token")
        or ""
    )
    return {"HUGGINGFACE_TOKEN": _text(token)}


def huggingface_token(client: Any = None) -> str:
    saved = _text(read_huggingface_settings(client).get("HUGGINGFACE_TOKEN"))
    if saved:
        return saved
    for key in ("HF_TOKEN", "HUGGINGFACE_HUB_TOKEN", "HUGGING_FACE_HUB_TOKEN", "HUGGINGFACE_TOKEN"):
        token = _text(os.getenv(key))
        if token:
            return token
    return ""


def huggingface_environment(overrides: Optional[Dict[str, Any]] = None, client: Any = None) -> Dict[str, Any]:
    env = dict(overrides or {})
    token = _text(
        env.get("HF_TOKEN")
        or env.get("HUGGINGFACE_HUB_TOKEN")
        or env.get("HUGGING_FACE_HUB_TOKEN")
        or env.get("HUGGINGFACE_TOKEN")
        or huggingface_token(client)
    )
    if token:
        env["HF_TOKEN"] = token
        env["HUGGINGFACE_HUB_TOKEN"] = token
        env["HUGGING_FACE_HUB_TOKEN"] = token
        env["HUGGINGFACE_TOKEN"] = token
    return env


def save_huggingface_settings(*, token: Any = None, client: Any = None) -> Dict[str, Any]:
    store = client or redis_client
    current = read_huggingface_settings(store)
    next_token = _text(current.get("HUGGINGFACE_TOKEN") if token is None else token)
    store.hset(HUGGINGFACE_SETTINGS_KEY, mapping={"HUGGINGFACE_TOKEN": next_token})
    return read_huggingface_settings(store)


def read_integration_settings() -> Dict[str, Any]:
    settings = read_huggingface_settings()
    return {"huggingface_token": settings.get("HUGGINGFACE_TOKEN", "")}


def save_integration_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    saved = save_huggingface_settings(token=(payload or {}).get("huggingface_token"))
    return {"huggingface_token": saved.get("HUGGINGFACE_TOKEN", "")}


def integration_status() -> Dict[str, Any]:
    configured = bool(huggingface_token())
    return {
        "configured": configured,
        "message": "Hugging Face token is saved." if configured else "No Hugging Face token is saved.",
    }


def run_integration_action(action_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if _text(action_id) != "test":
        raise KeyError(f"Unsupported Hugging Face action: {action_id}")
    token = _text((payload or {}).get("huggingface_token")) or huggingface_token()
    if not token:
        raise ValueError("Hugging Face access token is required.")
    response = requests.get(
        f"{HUGGINGFACE_API_BASE}/api/whoami-v2",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=HUGGINGFACE_DEFAULT_TIMEOUT_SECONDS,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Hugging Face token test failed: HTTP {response.status_code}: {response.text[:200]}")
    try:
        data = response.json()
    except Exception:
        data = {}
    name = _text(data.get("name") or data.get("fullname") or data.get("preferred_username"))
    return {"ok": True, "message": f"Hugging Face token worked{f' for {name}' if name else ''}."}
