import os
import asyncio
import threading
import inspect
from openai import AsyncOpenAI
import requests
import nest_asyncio
from dotenv import load_dotenv
import re
import json
import base64
import uuid
import time
import websocket
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse, urlunparse
try:
    import httpx
except Exception:  # pragma: no cover - optional dependency guard
    httpx = None
from redis_runtime import (
    decrypt_current_redis_snapshot,
    encrypt_current_redis_snapshot,
    ensure_redis_encryption_key,
    get_redis_client,
    get_redis_encryption_status,
    get_redis_connection_config,
    get_redis_connection_status,
    redis_blob_client,
    redis_client,
    save_redis_connection_settings,
    test_redis_connection_settings,
)

load_dotenv()
nest_asyncio.apply()

_INTERNAL_PORTAL_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1", "0.0.0.0"}
_INTERNAL_PORTAL_AUTH_TARGETS = (
    ("homeassistant_portal_settings", 8787, "/tater-ha/v1", None),
    ("homekit_portal_settings", 8789, "/tater-homekit/v1", "AUTH_TOKEN"),
    ("xbmc_portal_settings", 8790, "/tater-xbmc/v1", None),
    ("macos_portal_settings", 8791, "/macos", "AUTH_TOKEN"),
)
_INTERNAL_PORTAL_AUTH_CACHE_TTL_SECONDS = max(
    1.0,
    float(os.getenv("TATER_INTERNAL_PORTAL_AUTH_CACHE_TTL_SECONDS", "5")),
)
_INTERNAL_PORTAL_AUTH_CACHE: Dict[str, Any] = {"expires_at": 0.0, "rows": []}
_INTERNAL_PORTAL_AUTH_LOCK = threading.RLock()
_INTERNAL_PORTAL_HTTP_PATCHED = False
_ORIG_REQUESTS_SESSION_REQUEST = None
_ORIG_HTTPX_CLIENT_REQUEST = None
_ORIG_HTTPX_ASYNC_CLIENT_REQUEST = None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _boolish(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = _text(value).strip().lower()
    if token in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def _port_or_default(value: Any, default: int) -> int:
    try:
        port = int(_text(value).strip())
    except Exception:
        port = int(default)
    if port < 1 or port > 65535:
        return int(default)
    return port


def _default_port_for_scheme(scheme: str) -> int:
    token = _text(scheme).strip().lower()
    if token == "https":
        return 443
    return 80


def _effective_api_auth_enabled(settings: Dict[str, Any], key_value: str) -> bool:
    raw_enabled = settings.get("API_AUTH_ENABLED")
    if raw_enabled is None or _text(raw_enabled).strip() == "":
        return bool(_text(key_value).strip())
    return _boolish(raw_enabled, False)


def _load_internal_portal_auth_rows() -> List[Dict[str, Any]]:
    now = time.time()
    with _INTERNAL_PORTAL_AUTH_LOCK:
        cached_rows = _INTERNAL_PORTAL_AUTH_CACHE.get("rows")
        cached_until = float(_INTERNAL_PORTAL_AUTH_CACHE.get("expires_at") or 0.0)
        if now < cached_until and isinstance(cached_rows, list):
            return [dict(row) for row in cached_rows if isinstance(row, dict)]

    rows: List[Dict[str, Any]] = []
    try:
        for settings_key, default_port, path_prefix, legacy_key in _INTERNAL_PORTAL_AUTH_TARGETS:
            settings = redis_client.hgetall(settings_key) or {}
            if not isinstance(settings, dict):
                settings = {}

            port = _port_or_default(settings.get("bind_port"), default_port)
            key_value = _text(settings.get("API_AUTH_KEY")).strip()
            if not key_value and legacy_key:
                key_value = _text(settings.get(legacy_key)).strip()
            enabled = _effective_api_auth_enabled(settings, key_value)
            rows.append(
                {
                    "settings_key": settings_key,
                    "port": int(port),
                    "path_prefix": _text(path_prefix).strip() or "/",
                    "key": key_value,
                    "enabled": bool(enabled),
                }
            )
    except Exception:
        with _INTERNAL_PORTAL_AUTH_LOCK:
            _INTERNAL_PORTAL_AUTH_CACHE["rows"] = []
            _INTERNAL_PORTAL_AUTH_CACHE["expires_at"] = now + 2.0
        return []

    with _INTERNAL_PORTAL_AUTH_LOCK:
        _INTERNAL_PORTAL_AUTH_CACHE["rows"] = [dict(row) for row in rows]
        _INTERNAL_PORTAL_AUTH_CACHE["expires_at"] = now + _INTERNAL_PORTAL_AUTH_CACHE_TTL_SECONDS
    return rows


def _maybe_portal_token_for_url(url: Any) -> str:
    parsed = urlparse(_text(url).strip())
    host = _text(parsed.hostname).strip().lower().strip("[]")
    if not host or host not in _INTERNAL_PORTAL_LOCAL_HOSTS:
        return ""

    path = _text(parsed.path).strip() or "/"
    port = int(parsed.port or _default_port_for_scheme(parsed.scheme))
    rows = _load_internal_portal_auth_rows()
    for row in rows:
        try:
            row_port = int(row.get("port") or 0)
            row_path = _text(row.get("path_prefix")).strip() or "/"
            key_value = _text(row.get("key")).strip()
            enabled = bool(row.get("enabled"))
        except Exception:
            continue
        if row_port != port:
            continue
        if not path.startswith(row_path):
            continue
        if not enabled or not key_value:
            return ""
        return key_value
    return ""


def _headers_have_tater_token(headers: Dict[str, Any]) -> bool:
    for key in (headers or {}).keys():
        if _text(key).strip().lower() == "x-tater-token":
            return True
    return False


def _inject_tater_token_header(url: Any, headers: Any) -> Any:
    token = _maybe_portal_token_for_url(url)
    if not token:
        return headers

    if isinstance(headers, dict):
        merged_headers = dict(headers)
    elif headers is None:
        merged_headers = {}
    else:
        try:
            merged_headers = dict(headers)
        except Exception:
            merged_headers = {}
            for key, value in getattr(headers, "items", lambda: [])():
                merged_headers[key] = value

    if _headers_have_tater_token(merged_headers):
        return headers

    merged_headers["X-Tater-Token"] = token
    return merged_headers


def _httpx_effective_url(client: Any, url: Any) -> str:
    raw = _text(url).strip()
    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        return raw
    if httpx is None:
        return raw
    try:
        base_url = getattr(client, "base_url", None)
        if base_url is None:
            return raw
        return str(base_url.join(raw))
    except Exception:
        return raw


def _json_like_payload(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="ignore")
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            return None
    return None


def _vision_payload_has_image_url(payload: Any) -> bool:
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_token = str(key or "").strip().lower()
            if key_token == "type" and str(value or "").strip().lower() == "image_url":
                return True
            if key_token == "image_url":
                return True
            if _vision_payload_has_image_url(value):
                return True
        return False
    if isinstance(payload, list):
        for item in payload:
            if _vision_payload_has_image_url(item):
                return True
    return False


def _looks_like_tracked_vision_call(url: Any, *, json_payload: Any = None, data_payload: Any = None, content_payload: Any = None) -> Dict[str, str]:
    raw_url = _text(url).strip()
    if not raw_url:
        return {}
    parsed = urlparse(raw_url)
    path = _text(parsed.path).strip()
    suffix = "/v1/chat/completions"
    if not path.endswith(suffix):
        return {}

    payload = _json_like_payload(json_payload)
    if payload is None:
        payload = _json_like_payload(data_payload)
    if payload is None:
        payload = _json_like_payload(content_payload)
    if payload is None or not _vision_payload_has_image_url(payload):
        return {}

    model = ""
    if isinstance(payload, dict):
        model = _text(payload.get("model")).strip()
    base_path = path[: -len(suffix)].rstrip("/")
    api_base = urlunparse(parsed._replace(path=base_path, params="", query="", fragment="")).rstrip("/")
    if not api_base:
        api_base = raw_url
    return {
        "api_base": api_base,
        "model": model,
    }


def _patch_internal_portal_http_auth() -> None:
    global _INTERNAL_PORTAL_HTTP_PATCHED
    global _ORIG_REQUESTS_SESSION_REQUEST
    global _ORIG_HTTPX_CLIENT_REQUEST
    global _ORIG_HTTPX_ASYNC_CLIENT_REQUEST

    if _INTERNAL_PORTAL_HTTP_PATCHED:
        return

    _ORIG_REQUESTS_SESSION_REQUEST = requests.sessions.Session.request

    def _requests_session_request_with_portal_auth(self, method, url, **kwargs):
        vision_call_id = ""
        vision_model = ""
        try:
            kwargs["headers"] = _inject_tater_token_header(url, kwargs.get("headers"))
        except Exception:
            pass
        try:
            vision_meta = _looks_like_tracked_vision_call(
                url,
                json_payload=kwargs.get("json"),
                data_payload=kwargs.get("data"),
                content_payload=kwargs.get("content"),
            )
            if vision_meta:
                vision_model = str(vision_meta.get("model") or "").strip()
                vision_call_id = register_active_vision_call(
                    api_base=str(vision_meta.get("api_base") or "").strip(),
                    model=vision_model,
                    source="vision_http",
                )
        except Exception:
            vision_call_id = ""
            vision_model = ""
        try:
            response = _ORIG_REQUESTS_SESSION_REQUEST(self, method, url, **kwargs)
        except Exception as exc:
            if vision_call_id:
                finish_active_vision_call(vision_call_id, error=str(exc), response_model=vision_model)
            raise
        if vision_call_id:
            status_code = int(getattr(response, "status_code", 0) or 0)
            vision_error = f"HTTP {status_code}" if status_code >= 300 else None
            finish_active_vision_call(vision_call_id, error=vision_error, response_model=vision_model)
        return response

    requests.sessions.Session.request = _requests_session_request_with_portal_auth

    if httpx is not None:
        _ORIG_HTTPX_CLIENT_REQUEST = httpx.Client.request
        _ORIG_HTTPX_ASYNC_CLIENT_REQUEST = httpx.AsyncClient.request

        def _httpx_client_request_with_portal_auth(self, method, url, *args, **kwargs):
            vision_call_id = ""
            vision_model = ""
            effective_url = ""
            try:
                effective_url = _httpx_effective_url(self, url)
                kwargs["headers"] = _inject_tater_token_header(effective_url, kwargs.get("headers"))
            except Exception:
                pass
            try:
                vision_meta = _looks_like_tracked_vision_call(
                    effective_url or url,
                    json_payload=kwargs.get("json"),
                    data_payload=kwargs.get("data"),
                    content_payload=kwargs.get("content"),
                )
                if vision_meta:
                    vision_model = str(vision_meta.get("model") or "").strip()
                    vision_call_id = register_active_vision_call(
                        api_base=str(vision_meta.get("api_base") or "").strip(),
                        model=vision_model,
                        source="vision_http",
                    )
            except Exception:
                vision_call_id = ""
                vision_model = ""
            try:
                response = _ORIG_HTTPX_CLIENT_REQUEST(self, method, url, *args, **kwargs)
            except Exception as exc:
                if vision_call_id:
                    finish_active_vision_call(vision_call_id, error=str(exc), response_model=vision_model)
                raise
            if vision_call_id:
                status_code = int(getattr(response, "status_code", 0) or 0)
                vision_error = f"HTTP {status_code}" if status_code >= 300 else None
                finish_active_vision_call(vision_call_id, error=vision_error, response_model=vision_model)
            return response

        async def _httpx_async_client_request_with_portal_auth(self, method, url, *args, **kwargs):
            vision_call_id = ""
            vision_model = ""
            effective_url = ""
            try:
                effective_url = _httpx_effective_url(self, url)
                kwargs["headers"] = _inject_tater_token_header(effective_url, kwargs.get("headers"))
            except Exception:
                pass
            try:
                vision_meta = _looks_like_tracked_vision_call(
                    effective_url or url,
                    json_payload=kwargs.get("json"),
                    data_payload=kwargs.get("data"),
                    content_payload=kwargs.get("content"),
                )
                if vision_meta:
                    vision_model = str(vision_meta.get("model") or "").strip()
                    vision_call_id = register_active_vision_call(
                        api_base=str(vision_meta.get("api_base") or "").strip(),
                        model=vision_model,
                        source="vision_http",
                    )
            except Exception:
                vision_call_id = ""
                vision_model = ""
            try:
                response = await _ORIG_HTTPX_ASYNC_CLIENT_REQUEST(self, method, url, *args, **kwargs)
            except Exception as exc:
                if vision_call_id:
                    finish_active_vision_call(vision_call_id, error=str(exc), response_model=vision_model)
                raise
            if vision_call_id:
                status_code = int(getattr(response, "status_code", 0) or 0)
                vision_error = f"HTTP {status_code}" if status_code >= 300 else None
                finish_active_vision_call(vision_call_id, error=vision_error, response_model=vision_model)
            return response

        httpx.Client.request = _httpx_client_request_with_portal_auth
        httpx.AsyncClient.request = _httpx_async_client_request_with_portal_auth

    _INTERNAL_PORTAL_HTTP_PATCHED = True


_patch_internal_portal_http_auth()


def get_tater_name():
    """Return the assistant's first and last name from Redis."""
    first = redis_client.get("tater:first_name")
    if not first:
        first = "Tater"
        redis_client.set("tater:first_name", first)

    last = redis_client.get("tater:last_name")
    if not last:
        last = "Totterson"
        redis_client.set("tater:last_name", last)

    return first, last

def get_tater_personality():
    """
    Return the assistant's personality / style description from Redis.
    Empty string means 'no forced personality'.
    """
    personality = redis_client.get("tater:personality")
    if not personality:
        personality = ""
        redis_client.set("tater:personality", personality)

    return personality

# ---------------------------------------------------------
# Main event loop reference + run_async helper
# ---------------------------------------------------------
_main_loop = None

def set_main_loop(loop):
    global _main_loop
    _main_loop = loop

def run_async(coro):
    loop = _main_loop or asyncio.get_event_loop_policy().get_event_loop()
    return loop.run_until_complete(coro)

# ---------------------------------------------------------
# LLM client wrapper (OpenAI-compatible /v1/chat/completions)
# ---------------------------------------------------------
HYDRA_LLM_HOST_KEY = "tater:hydra:llm_host"
HYDRA_LLM_PORT_KEY = "tater:hydra:llm_port"
HYDRA_LLM_MODEL_KEY = "tater:hydra:llm_model"
HYDRA_LLM_BASE_SERVERS_KEY = "tater:hydra:llm_base_servers"
HYDRA_LLM_SETUP_ERROR = (
    "Hydra LLM is not configured. Open Settings > Hydra and set Hydra LLM Host/IP, Port, and Model."
)
_HYDRA_BASE_RR_LOCK = threading.Lock()
_HYDRA_BASE_RR_INDEX: Dict[str, int] = {}
_ACTIVE_LLM_CALLS_LOCK = threading.RLock()
_ACTIVE_LLM_CALLS: Dict[str, Dict[str, Any]] = {}
_LLM_CALL_HISTORY: List[Dict[str, Any]] = []
_LLM_CALL_HISTORY_MAX = 5000
_LLM_CALL_COUNTERS: Dict[str, int] = {"started": 0, "completed": 0, "failed": 0}
_LLM_CALL_COUNTERS_REDIS_KEY = "tater:llm:runtime:counters"
_LLM_CALL_HISTORY_REDIS_KEY = "tater:llm:runtime:history"
_ACTIVE_VISION_CALLS_LOCK = threading.RLock()
_ACTIVE_VISION_CALLS: Dict[str, Dict[str, Any]] = {}
_VISION_CALL_HISTORY: List[Dict[str, Any]] = []
_VISION_CALL_HISTORY_MAX = 5000
_VISION_CALL_COUNTERS: Dict[str, int] = {"started": 0, "completed": 0, "failed": 0}
_VISION_CALL_COUNTERS_REDIS_KEY = "tater:vision:runtime:counters"
_VISION_CALL_HISTORY_REDIS_KEY = "tater:vision:runtime:history"

_LLM_ORIGIN_KIND_LABELS = {
    "hydra": "Hydra",
    "webui": "WebUI",
    "verba": "Verba",
    "portal": "Portal",
    "core": "Core",
    "other": "Other",
}
_GENERIC_LLM_CALL_FUNCTIONS = {
    "",
    "__call__",
    "_run",
    "_run_async",
    "_runner",
    "_thread_main",
    "_worker",
    "main",
    "run",
    "runner",
    "worker",
}
_LLM_ACTIVITY_KEYWORDS: List[Tuple[str, Tuple[str, ...]]] = [
    ("verification", ("verify", "verification", "validate", "validation", "checker", "check")),
    ("discovery", ("discover", "discovery", "scan", "crawl", "inspect", "search", "lookup")),
    ("cleanup", ("cleanup", "clean up", "prune", "trim", "purge", "gc", "garbage collect")),
    ("memory", ("memory", "ledger", "context window", "context", "recall")),
    ("planning", ("plan", "planning", "astraeus")),
    ("execution", ("execute", "execution", "run tool", "thanatos")),
    ("summary", ("summarize", "summary", "brief", "digest")),
    ("rewrite", ("rewrite", "rephrase", "edit text")),
]


def _llm_runtime_as_int(value: Any, default: int = 0, *, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    return max(int(minimum), int(parsed))


def _llm_runtime_as_float(value: Any, default: float = 0.0, *, minimum: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    return max(float(minimum), float(parsed))


def _normalize_llm_runtime_history_row(row: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    kind = str(row.get("kind") or "other").strip().lower() or "other"
    source = str(row.get("source") or "unknown").strip() or "unknown"
    return {
        "id": str(row.get("id") or "").strip(),
        "finished_at": _llm_runtime_as_float(row.get("finished_at"), 0.0, minimum=0.0),
        "started_at": _llm_runtime_as_float(row.get("started_at"), 0.0, minimum=0.0),
        "duration_ms": _llm_runtime_as_float(row.get("duration_ms"), 0.0, minimum=0.0),
        "ok": _boolish(row.get("ok"), default=False),
        "error": str(row.get("error") or "").strip(),
        "kind": kind,
        "source": source,
        "module": str(row.get("module") or "").strip(),
        "path": str(row.get("path") or "").strip(),
        "function": str(row.get("function") or "").strip(),
        "activity": str(row.get("activity") or "").strip(),
        "host": str(row.get("host") or "").strip(),
        "model": str(row.get("model") or "").strip(),
        "stream": _boolish(row.get("stream"), default=False),
        "message_count": _llm_runtime_as_int(row.get("message_count"), 0, minimum=0),
    }


def _persist_llm_runtime_counter_delta(*, started: int = 0, completed: int = 0, failed: int = 0) -> None:
    started_i = _llm_runtime_as_int(started, 0, minimum=0)
    completed_i = _llm_runtime_as_int(completed, 0, minimum=0)
    failed_i = _llm_runtime_as_int(failed, 0, minimum=0)
    if started_i <= 0 and completed_i <= 0 and failed_i <= 0:
        return
    try:
        client = get_redis_client() or redis_client
        if client is None:
            return
        pipe = client.pipeline()
        if started_i > 0:
            pipe.hincrby(_LLM_CALL_COUNTERS_REDIS_KEY, "started", started_i)
        if completed_i > 0:
            pipe.hincrby(_LLM_CALL_COUNTERS_REDIS_KEY, "completed", completed_i)
        if failed_i > 0:
            pipe.hincrby(_LLM_CALL_COUNTERS_REDIS_KEY, "failed", failed_i)
        pipe.execute()
    except Exception:
        return


def _persist_llm_runtime_history_row(row: Dict[str, Any]) -> None:
    normalized = _normalize_llm_runtime_history_row(row)
    if not isinstance(normalized, dict):
        return
    try:
        client = get_redis_client() or redis_client
        if client is None:
            return
        payload = json.dumps(normalized, separators=(",", ":"), ensure_ascii=False)
        history_limit = max(100, int(_LLM_CALL_HISTORY_MAX))
        pipe = client.pipeline()
        pipe.rpush(_LLM_CALL_HISTORY_REDIS_KEY, payload)
        pipe.ltrim(_LLM_CALL_HISTORY_REDIS_KEY, -history_limit, -1)
        pipe.execute()
    except Exception:
        return


def _load_persisted_llm_runtime_state() -> None:
    try:
        client = get_redis_client() or redis_client
    except Exception:
        client = redis_client
    if client is None:
        return

    counters = {"started": 0, "completed": 0, "failed": 0}
    history_rows: List[Dict[str, Any]] = []
    history_limit = max(100, int(_LLM_CALL_HISTORY_MAX))

    try:
        raw_counters = client.hgetall(_LLM_CALL_COUNTERS_REDIS_KEY) or {}
    except Exception:
        raw_counters = {}

    if isinstance(raw_counters, dict):
        counters["started"] = _llm_runtime_as_int(raw_counters.get("started"), 0, minimum=0)
        counters["completed"] = _llm_runtime_as_int(raw_counters.get("completed"), 0, minimum=0)
        counters["failed"] = _llm_runtime_as_int(raw_counters.get("failed"), 0, minimum=0)

    try:
        raw_history = client.lrange(_LLM_CALL_HISTORY_REDIS_KEY, -history_limit, -1) or []
    except Exception:
        raw_history = []

    if isinstance(raw_history, list):
        for item in raw_history:
            try:
                text = item.decode("utf-8", errors="ignore") if isinstance(item, (bytes, bytearray)) else str(item or "")
                parsed = json.loads(text) if text else None
                normalized = _normalize_llm_runtime_history_row(parsed)
                if isinstance(normalized, dict):
                    history_rows.append(normalized)
            except Exception:
                continue

    with _ACTIVE_LLM_CALLS_LOCK:
        _LLM_CALL_COUNTERS["started"] = counters["started"]
        _LLM_CALL_COUNTERS["completed"] = counters["completed"]
        _LLM_CALL_COUNTERS["failed"] = counters["failed"]
        _LLM_CALL_HISTORY[:] = history_rows[-history_limit:]


_load_persisted_llm_runtime_state()


def _normalize_vision_runtime_history_row(row: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    kind = str(row.get("kind") or "other").strip().lower() or "other"
    source = str(row.get("source") or "unknown").strip() or "unknown"
    return {
        "id": str(row.get("id") or "").strip(),
        "finished_at": _llm_runtime_as_float(row.get("finished_at"), 0.0, minimum=0.0),
        "started_at": _llm_runtime_as_float(row.get("started_at"), 0.0, minimum=0.0),
        "duration_ms": _llm_runtime_as_float(row.get("duration_ms"), 0.0, minimum=0.0),
        "ok": _boolish(row.get("ok"), default=False),
        "error": str(row.get("error") or "").strip(),
        "kind": kind,
        "source": source,
        "module": str(row.get("module") or "").strip(),
        "path": str(row.get("path") or "").strip(),
        "function": str(row.get("function") or "").strip(),
        "api_base": str(row.get("api_base") or "").strip(),
        "model": str(row.get("model") or "").strip(),
    }


def _persist_vision_runtime_counter_delta(*, started: int = 0, completed: int = 0, failed: int = 0) -> None:
    started_i = _llm_runtime_as_int(started, 0, minimum=0)
    completed_i = _llm_runtime_as_int(completed, 0, minimum=0)
    failed_i = _llm_runtime_as_int(failed, 0, minimum=0)
    if started_i <= 0 and completed_i <= 0 and failed_i <= 0:
        return
    try:
        client = get_redis_client() or redis_client
        if client is None:
            return
        pipe = client.pipeline()
        if started_i > 0:
            pipe.hincrby(_VISION_CALL_COUNTERS_REDIS_KEY, "started", started_i)
        if completed_i > 0:
            pipe.hincrby(_VISION_CALL_COUNTERS_REDIS_KEY, "completed", completed_i)
        if failed_i > 0:
            pipe.hincrby(_VISION_CALL_COUNTERS_REDIS_KEY, "failed", failed_i)
        pipe.execute()
    except Exception:
        return


def _persist_vision_runtime_history_row(row: Dict[str, Any]) -> None:
    normalized = _normalize_vision_runtime_history_row(row)
    if not isinstance(normalized, dict):
        return
    try:
        client = get_redis_client() or redis_client
        if client is None:
            return
        payload = json.dumps(normalized, separators=(",", ":"), ensure_ascii=False)
        history_limit = max(100, int(_VISION_CALL_HISTORY_MAX))
        pipe = client.pipeline()
        pipe.rpush(_VISION_CALL_HISTORY_REDIS_KEY, payload)
        pipe.ltrim(_VISION_CALL_HISTORY_REDIS_KEY, -history_limit, -1)
        pipe.execute()
    except Exception:
        return


def _load_persisted_vision_runtime_state() -> None:
    try:
        client = get_redis_client() or redis_client
    except Exception:
        client = redis_client
    if client is None:
        return

    counters = {"started": 0, "completed": 0, "failed": 0}
    history_rows: List[Dict[str, Any]] = []
    history_limit = max(100, int(_VISION_CALL_HISTORY_MAX))

    try:
        raw_counters = client.hgetall(_VISION_CALL_COUNTERS_REDIS_KEY) or {}
    except Exception:
        raw_counters = {}

    if isinstance(raw_counters, dict):
        counters["started"] = _llm_runtime_as_int(raw_counters.get("started"), 0, minimum=0)
        counters["completed"] = _llm_runtime_as_int(raw_counters.get("completed"), 0, minimum=0)
        counters["failed"] = _llm_runtime_as_int(raw_counters.get("failed"), 0, minimum=0)

    try:
        raw_history = client.lrange(_VISION_CALL_HISTORY_REDIS_KEY, -history_limit, -1) or []
    except Exception:
        raw_history = []

    if isinstance(raw_history, list):
        for item in raw_history:
            try:
                text = item.decode("utf-8", errors="ignore") if isinstance(item, (bytes, bytearray)) else str(item or "")
                parsed = json.loads(text) if text else None
                normalized = _normalize_vision_runtime_history_row(parsed)
                if isinstance(normalized, dict):
                    history_rows.append(normalized)
            except Exception:
                continue

    with _ACTIVE_VISION_CALLS_LOCK:
        _VISION_CALL_COUNTERS["started"] = counters["started"]
        _VISION_CALL_COUNTERS["completed"] = counters["completed"]
        _VISION_CALL_COUNTERS["failed"] = counters["failed"]
        _VISION_CALL_HISTORY[:] = history_rows[-history_limit:]


_load_persisted_vision_runtime_state()


def _normalize_base_url(host: str) -> str:
    """
    Ensure base_url ends with /v1 and includes scheme.
    Accepts: 127.0.0.1:11434  -> http://127.0.0.1:11434/v1
             http://host:port -> http://host:port/v1 (if missing)
             https://api.foo/v1 -> unchanged
    """
    if not host.startswith("http://") and not host.startswith("https://"):
        host = f"http://{host}"
    parsed = urlparse(host.rstrip("/"))
    path = parsed.path
    if not path.endswith("/v1"):
        path = (path + "/v1").replace("//", "/")
    return urlunparse(parsed._replace(path=path))

def _sanitize_chat_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Defensive sanitizer for OpenAI-compatible chat endpoints.

    - Drops empty user turns (content == "" after coercion)
      These can cause some backends (LM Studio / some Qwen templates) to return empty completions.
    - Coerces non-string message content (lists/dicts) into plain text,
      so we don't send multimodal structures to backends that don't support them.
    - Drops messages with missing role/content.
    """
    if not isinstance(messages, list):
        return []

    cleaned: List[Dict[str, Any]] = []
    for m in messages:
        if not isinstance(m, dict):
            continue

        role = (m.get("role") or "").strip()
        if role not in ("system", "user", "assistant", "tool"):
            # keep it strict; unknown roles can confuse some servers
            continue

        raw_content = m.get("content", None)

        # Convert any non-string (list/dict/etc) to text for maximum compatibility
        content_text = _coerce_content_to_text(raw_content).strip()

        # Drop empty user turns entirely (this is the big one)
        if role == "user" and content_text == "":
            continue

        # Also drop empty assistant/tool turns (optional but generally helpful)
        if role in ("assistant", "tool") and content_text == "":
            continue

        # System messages should not be empty either
        if role == "system" and content_text == "":
            continue

        cleaned.append({"role": role, "content": content_text})

    return cleaned

def _coerce_content_to_text(content) -> str:
    if isinstance(content, str):
        return content

    if isinstance(content, dict):
        # Common places providers hide text
        for k in ("text", "content", "value"):
            v = content.get(k)
            if isinstance(v, str):
                return v.strip()
        # Sometimes it's a list inside a dict
        for k in ("parts", "content", "messages"):
            v = content.get(k)
            if isinstance(v, list):
                return _coerce_content_to_text(v)
        return ""

    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, str):
                parts.append(p)
            elif isinstance(p, dict):
                # Prefer explicit text/content/value keys
                for k in ("text", "content", "value"):
                    v = p.get(k)
                    if isinstance(v, str):
                        parts.append(v)
                        break
        return "\n".join(s for s in parts if s).strip()

    return "" if content is None else str(content)


def _safe_redis_text_get(key: str, *, redis_conn: Any = None) -> str:
    client = redis_conn or redis_client
    try:
        return str(client.get(key) or "").strip()
    except Exception:
        return ""


def _build_hydra_llm_endpoint(host: Any, port: Any) -> str:
    raw_host = str(host or "").strip()
    raw_port = str(port or "").strip()
    if not raw_host:
        return ""

    candidate = raw_host if raw_host.startswith(("http://", "https://")) else f"http://{raw_host}"
    parsed = urlparse(candidate)
    hostname = str(parsed.hostname or "").strip()
    if not hostname:
        return ""

    resolved_port = raw_port or (str(parsed.port) if parsed.port is not None else "")
    if resolved_port:
        try:
            port_int = int(str(resolved_port).strip())
        except Exception:
            return ""
        if port_int < 1 or port_int > 65535:
            return ""
        netloc = f"{hostname}:{port_int}"
    else:
        netloc = hostname

    return urlunparse((parsed.scheme or "http", netloc, "", "", "", "")).rstrip("/")


def _resolve_hydra_llm_defaults(*, redis_conn: Any = None) -> tuple[str, str]:
    host = _safe_redis_text_get(HYDRA_LLM_HOST_KEY, redis_conn=redis_conn)
    port = _safe_redis_text_get(HYDRA_LLM_PORT_KEY, redis_conn=redis_conn)
    model = _safe_redis_text_get(HYDRA_LLM_MODEL_KEY, redis_conn=redis_conn)
    endpoint = _build_hydra_llm_endpoint(host, port)
    return endpoint, model


def _normalize_hydra_base_server_row(row: Any) -> Optional[Dict[str, str]]:
    if not isinstance(row, dict):
        return None

    raw_host = str(row.get("host") or "").strip()
    raw_port = str(row.get("port") or "").strip()
    raw_model = str(row.get("model") or "").strip()
    if not raw_host and not raw_port and not raw_model:
        return None

    endpoint = _build_hydra_llm_endpoint(raw_host, raw_port)
    if not endpoint or not raw_model:
        return None

    parsed = urlparse(endpoint)
    hostname = str(parsed.hostname or "").strip()
    if not hostname:
        return None

    host_with_scheme = raw_host.startswith(("http://", "https://"))
    canonical_host = f"{parsed.scheme}://{hostname}" if host_with_scheme else hostname
    canonical_port = str(parsed.port) if parsed.port is not None else ""

    return {
        "host": canonical_host,
        "port": canonical_port,
        "model": raw_model,
        "endpoint": endpoint,
    }


def resolve_hydra_base_servers(*, redis_conn: Any = None, include_legacy: bool = True) -> List[Dict[str, str]]:
    client = redis_conn or redis_client
    rows: List[Dict[str, str]] = []
    seen: set[Tuple[str, str]] = set()

    raw_payload = _safe_redis_text_get(HYDRA_LLM_BASE_SERVERS_KEY, redis_conn=client)
    parsed_payload: Any = []
    if raw_payload:
        try:
            parsed_payload = json.loads(raw_payload)
        except Exception:
            parsed_payload = []

    if isinstance(parsed_payload, list):
        for item in parsed_payload:
            normalized = _normalize_hydra_base_server_row(item)
            if not normalized:
                continue
            signature = (normalized["endpoint"], normalized["model"])
            if signature in seen:
                continue
            seen.add(signature)
            rows.append(normalized)

    if rows or not include_legacy:
        return rows

    legacy_host = _safe_redis_text_get(HYDRA_LLM_HOST_KEY, redis_conn=client)
    legacy_port = _safe_redis_text_get(HYDRA_LLM_PORT_KEY, redis_conn=client)
    legacy_model = _safe_redis_text_get(HYDRA_LLM_MODEL_KEY, redis_conn=client)
    legacy_row = _normalize_hydra_base_server_row(
        {"host": legacy_host, "port": legacy_port, "model": legacy_model}
    )
    if legacy_row:
        signature = (legacy_row["endpoint"], legacy_row["model"])
        if signature not in seen:
            rows.append(legacy_row)
    return rows


def _next_hydra_base_rr_index(pool_key: str, size: int) -> int:
    if size <= 1:
        return 0
    key = str(pool_key or "").strip() or "__default__"
    with _HYDRA_BASE_RR_LOCK:
        next_index = int(_HYDRA_BASE_RR_INDEX.get(key, 0))
        selected = next_index % int(size)
        _HYDRA_BASE_RR_INDEX[key] = (selected + 1) % int(size)
    return selected


def _llm_origin_kind_label(kind: Any) -> str:
    token = str(kind or "").strip().lower() or "other"
    return _LLM_ORIGIN_KIND_LABELS.get(token, token.capitalize() or "Other")


def _classify_llm_call_origin(filename: str, module_name: str) -> Dict[str, str]:
    raw_path = str(filename or "").strip()
    normalized = raw_path.replace("\\", "/")
    lowered = normalized.lower()
    stem = os.path.splitext(os.path.basename(normalized))[0].strip().lower()
    module_token = str(module_name or "").strip()
    source = stem or module_token or "unknown"
    kind = "other"

    if "/hydra/" in lowered:
        kind = "hydra"
        source = "hydra"
    elif "/verba/" in lowered or "/verbas/" in lowered:
        kind = "verba"
        source = source.removesuffix("_verba")
    elif "/portals/" in lowered:
        kind = "portal"
        source = source.removesuffix("_portal")
    elif "/cores/" in lowered:
        kind = "core"
        source = source.removesuffix("_core")
    elif stem == "tateros_app":
        kind = "webui"
        source = "webui"
    elif stem.endswith("_core"):
        kind = "core"
        source = stem.removesuffix("_core")
    elif stem.endswith("_portal"):
        kind = "portal"
        source = stem.removesuffix("_portal")
    elif stem.endswith("_verba"):
        kind = "verba"
        source = stem.removesuffix("_verba")

    source = str(source or "unknown").strip() or "unknown"
    rel_path = normalized
    try:
        cwd = os.getcwd()
        rel_candidate = os.path.relpath(raw_path, cwd)
        if rel_candidate and not rel_candidate.startswith(".."):
            rel_path = rel_candidate.replace("\\", "/")
    except Exception:
        pass

    return {
        "kind": kind,
        "source": source,
        "module": module_token or source,
        "path": rel_path,
    }


def _infer_llm_call_origin(max_depth: int = 48) -> Dict[str, str]:
    frame = inspect.currentframe()
    fallback_info: Optional[Dict[str, str]] = None
    try:
        if frame is not None:
            frame = frame.f_back
        depth = 0
        while frame is not None and depth < int(max_depth):
            code = frame.f_code
            filename = str(getattr(code, "co_filename", "") or "")
            function_name = str(getattr(code, "co_name", "") or "").strip()
            module_name = str(frame.f_globals.get("__name__") or "").strip()
            depth += 1
            frame = frame.f_back

            if not filename:
                continue
            normalized = filename.replace("\\", "/").lower()
            module_lower = module_name.lower()
            if normalized.endswith("/helpers.py"):
                continue

            info = _classify_llm_call_origin(filename, module_name)
            info["function"] = function_name
            # Skip asyncio/threadpool plumbing frames when we can so runtime labels
            # reflect the actual caller module (portal/core/verba/hydra).
            if (
                "/asyncio/" in normalized
                or "/concurrent/futures/" in normalized
                or normalized.endswith("/threading.py")
                or module_lower.startswith("asyncio.")
                or module_lower.startswith("concurrent.futures")
            ):
                if fallback_info is None:
                    fallback_info = dict(info)
                continue

            if str(info.get("kind") or "other") != "other":
                return info
            if fallback_info is None:
                fallback_info = dict(info)
    finally:
        # Explicitly clear frame references.
        del frame

    if isinstance(fallback_info, dict):
        return fallback_info

    return {
        "kind": "other",
        "source": "unknown",
        "module": "",
        "path": "",
        "function": "",
    }


def _llm_activity_from_text(text: str) -> str:
    lowered = _text(text).strip().lower()
    if not lowered:
        return ""
    for label, keywords in _LLM_ACTIVITY_KEYWORDS:
        if any(keyword in lowered for keyword in keywords):
            return label
    return ""


def _normalize_llm_activity_hint(value: Any) -> str:
    raw = _text(value).strip()
    if not raw:
        return ""
    by_keyword = _llm_activity_from_text(raw)
    if by_keyword:
        return by_keyword
    cleaned = re.sub(r"[\s_\-]+", " ", raw).strip().lower()
    cleaned = re.sub(r"[^a-z0-9 ]+", "", cleaned).strip()
    return cleaned[:48] if cleaned else ""


def _llm_activity_from_origin(origin: Dict[str, str]) -> str:
    function_name = _text((origin or {}).get("function")).strip()
    normalized_function = function_name.lstrip("_").strip().lower()
    if normalized_function and normalized_function not in _GENERIC_LLM_CALL_FUNCTIONS:
        inferred = _llm_activity_from_text(normalized_function.replace("_", " "))
        if inferred:
            return inferred
        cleaned = re.sub(r"[^a-z0-9_]+", "_", normalized_function)
        cleaned = re.sub(r"_+", "_", cleaned).strip("_")
        if cleaned:
            return cleaned.replace("_", " ")

    origin_blob = " ".join(
        part
        for part in [
            _text((origin or {}).get("source")),
            _text((origin or {}).get("module")),
            _text((origin or {}).get("path")),
        ]
        if _text(part).strip()
    )
    return _llm_activity_from_text(origin_blob)


def _llm_activity_from_messages(messages: List[Dict[str, Any]]) -> str:
    if not isinstance(messages, list) or not messages:
        return ""
    selected: List[str] = []
    try:
        for idx, item in enumerate(messages):
            if not isinstance(item, dict):
                continue
            role = _text(item.get("role")).strip().lower()
            if role not in {"system", "user"}:
                continue
            if idx < 2 or idx >= max(0, len(messages) - 2):
                content = _coerce_content_to_text(item.get("content")).strip()
                if content:
                    selected.append(content[:420])
    except Exception:
        selected = []
    if not selected:
        return ""
    return _llm_activity_from_text("\n".join(selected))


def _infer_llm_call_activity(*, origin: Dict[str, str], messages: List[Dict[str, Any]]) -> str:
    by_messages = _llm_activity_from_messages(messages)
    if by_messages:
        return by_messages
    return _llm_activity_from_origin(origin)


def _register_active_llm_call(
    *,
    host: str,
    model: str,
    stream: bool,
    message_count: int,
    messages: List[Dict[str, Any]],
    activity_hint: str = "",
) -> str:
    origin = _infer_llm_call_origin()
    activity = _normalize_llm_activity_hint(activity_hint) or _infer_llm_call_activity(
        origin=origin,
        messages=(messages if isinstance(messages, list) else []),
    )
    call_id = str(uuid.uuid4())
    started_at = time.time()
    row = {
        "id": call_id,
        "host": str(host or "").strip(),
        "model": str(model or "").strip(),
        "stream": bool(stream),
        "message_count": max(0, int(message_count)),
        "started_at": started_at,
        "kind": str(origin.get("kind") or "other"),
        "source": str(origin.get("source") or "unknown"),
        "module": str(origin.get("module") or ""),
        "path": str(origin.get("path") or ""),
        "function": str(origin.get("function") or ""),
        "activity": str(activity or "").strip(),
    }
    with _ACTIVE_LLM_CALLS_LOCK:
        _ACTIVE_LLM_CALLS[call_id] = row
        _LLM_CALL_COUNTERS["started"] = int(_LLM_CALL_COUNTERS.get("started") or 0) + 1
    _persist_llm_runtime_counter_delta(started=1)
    return call_id


def _finish_active_llm_call(
    call_id: str,
    *,
    error: Optional[Exception] = None,
    response_model: str = "",
) -> None:
    call_token = str(call_id or "").strip()
    if not call_token:
        return

    finished_at = time.time()
    with _ACTIVE_LLM_CALLS_LOCK:
        row = _ACTIVE_LLM_CALLS.pop(call_token, None)
        if not isinstance(row, dict):
            return

        started_at = float(row.get("started_at") or 0.0)
        duration_ms = max(0.0, (finished_at - started_at) * 1000.0) if started_at > 0.0 else 0.0
        ok = error is None

        if ok:
            _LLM_CALL_COUNTERS["completed"] = int(_LLM_CALL_COUNTERS.get("completed") or 0) + 1
        else:
            _LLM_CALL_COUNTERS["failed"] = int(_LLM_CALL_COUNTERS.get("failed") or 0) + 1

        history_row = {
            "id": call_token,
            "finished_at": finished_at,
            "started_at": started_at,
            "duration_ms": duration_ms,
            "ok": bool(ok),
            "error": str(error) if error is not None else "",
            "kind": str(row.get("kind") or "other"),
            "source": str(row.get("source") or "unknown"),
            "module": str(row.get("module") or ""),
            "path": str(row.get("path") or ""),
            "function": str(row.get("function") or ""),
            "activity": str(row.get("activity") or "").strip(),
            "host": str(row.get("host") or ""),
            "model": str(response_model or row.get("model") or "").strip(),
            "stream": bool(row.get("stream")),
            "message_count": max(0, int(row.get("message_count") or 0)),
        }
        _LLM_CALL_HISTORY.append(history_row)
        overflow = len(_LLM_CALL_HISTORY) - int(_LLM_CALL_HISTORY_MAX)
        if overflow > 0:
            del _LLM_CALL_HISTORY[:overflow]

    if ok:
        _persist_llm_runtime_counter_delta(completed=1)
    else:
        _persist_llm_runtime_counter_delta(failed=1)
    _persist_llm_runtime_history_row(history_row)


def get_active_llm_calls_snapshot(*, limit: int = 100) -> List[Dict[str, Any]]:
    max_items = max(1, min(int(limit or 0), 500))
    now = time.time()

    with _ACTIVE_LLM_CALLS_LOCK:
        rows = [dict(item) for item in _ACTIVE_LLM_CALLS.values() if isinstance(item, dict)]

    rows.sort(key=lambda row: float(row.get("started_at") or 0.0))
    out: List[Dict[str, Any]] = []
    for row in rows[-max_items:]:
        started_at = float(row.get("started_at") or 0.0)
        age_seconds = max(0, int(now - started_at)) if started_at > 0 else 0
        kind = str(row.get("kind") or "other")
        source = str(row.get("source") or "unknown")
        out.append(
            {
                "id": str(row.get("id") or ""),
                "kind": kind,
                "kind_label": _llm_origin_kind_label(kind),
                "source": source,
                "source_label": f"{_llm_origin_kind_label(kind)} - {source}",
                "module": str(row.get("module") or ""),
                "path": str(row.get("path") or ""),
                "function": str(row.get("function") or ""),
                "activity": str(row.get("activity") or "").strip(),
                "host": str(row.get("host") or ""),
                "model": str(row.get("model") or ""),
                "stream": bool(row.get("stream")),
                "message_count": max(0, int(row.get("message_count") or 0)),
                "started_at": started_at,
                "age_seconds": age_seconds,
            }
        )
    return out


def _llm_call_history_windows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    now = time.time()
    windows = [
        ("24h", "Last 24 hours", 24 * 60 * 60),
        ("7d", "Last 7 days", 7 * 24 * 60 * 60),
        ("30d", "Last 30 days", 30 * 24 * 60 * 60),
    ]
    buckets: Dict[str, Dict[str, Any]] = {
        key: {
            "key": key,
            "label": label,
            "calls": 0,
            "completed": 0,
            "failed": 0,
            "duration_ms_total": 0.0,
            "source_counts": {},
        }
        for key, label, _ in windows
    }

    for row in rows:
        finished_at = float(row.get("finished_at") or 0.0)
        if finished_at <= 0.0:
            continue
        age = max(0.0, now - finished_at)
        ok = bool(row.get("ok"))
        duration_ms = max(0.0, float(row.get("duration_ms") or 0.0))
        kind = str(row.get("kind") or "other")
        source = str(row.get("source") or "unknown")
        source_key = f"{kind}:{source}"

        for key, _label, seconds in windows:
            if age > float(seconds):
                continue
            bucket = buckets[key]
            bucket["calls"] = int(bucket.get("calls") or 0) + 1
            if ok:
                bucket["completed"] = int(bucket.get("completed") or 0) + 1
            else:
                bucket["failed"] = int(bucket.get("failed") or 0) + 1
            bucket["duration_ms_total"] = float(bucket.get("duration_ms_total") or 0.0) + duration_ms
            source_counts = bucket.get("source_counts")
            if isinstance(source_counts, dict):
                source_counts[source_key] = int(source_counts.get(source_key) or 0) + 1

    window_rows: List[Dict[str, Any]] = []
    for key, label, _seconds in windows:
        bucket = buckets.get(key) or {"key": key, "label": label}
        calls = int(bucket.get("calls") or 0)
        duration_ms_total = float(bucket.get("duration_ms_total") or 0.0)
        avg_ms = (duration_ms_total / calls) if calls > 0 else 0.0

        source_rows: List[Dict[str, Any]] = []
        raw_source_counts = bucket.get("source_counts") if isinstance(bucket.get("source_counts"), dict) else {}
        for source_key, count in raw_source_counts.items():
            kind, _sep, source = str(source_key or "").partition(":")
            source_rows.append(
                {
                    "kind": kind or "other",
                    "source": source or "unknown",
                    "label": f"{_llm_origin_kind_label(kind or 'other')} - {source or 'unknown'}",
                    "calls": int(count),
                }
            )
        source_rows.sort(key=lambda row: (-int(row.get("calls") or 0), str(row.get("label") or "")))

        window_rows.append(
            {
                "key": key,
                "label": label,
                "calls": calls,
                "completed": int(bucket.get("completed") or 0),
                "failed": int(bucket.get("failed") or 0),
                "avg_ms": round(avg_ms, 2),
                "top_sources": source_rows[:4],
            }
        )

    return {
        "windows": window_rows,
        "sample_size": int(len(rows)),
    }


def get_llm_call_runtime_summary(*, include_history: bool = False) -> Dict[str, Any]:
    active_calls = get_active_llm_calls_snapshot(limit=120)
    by_kind_counts: Dict[str, int] = {}
    by_source_counts: Dict[str, int] = {}

    for row in active_calls:
        kind = str(row.get("kind") or "other")
        source = str(row.get("source") or "unknown")
        by_kind_counts[kind] = int(by_kind_counts.get(kind) or 0) + 1
        source_key = f"{kind}:{source}"
        by_source_counts[source_key] = int(by_source_counts.get(source_key) or 0) + 1

    by_kind = [
        {
            "kind": kind,
            "label": _llm_origin_kind_label(kind),
            "calls": int(count),
        }
        for kind, count in by_kind_counts.items()
    ]
    by_kind.sort(key=lambda row: (-int(row.get("calls") or 0), str(row.get("label") or "")))

    by_source = []
    for source_key, count in by_source_counts.items():
        kind, _sep, source = str(source_key or "").partition(":")
        by_source.append(
            {
                "kind": kind or "other",
                "source": source or "unknown",
                "label": f"{_llm_origin_kind_label(kind or 'other')} - {source or 'unknown'}",
                "calls": int(count),
            }
        )
    by_source.sort(key=lambda row: (-int(row.get("calls") or 0), str(row.get("label") or "")))

    with _ACTIVE_LLM_CALLS_LOCK:
        totals = {
            "started": int(_LLM_CALL_COUNTERS.get("started") or 0),
            "completed": int(_LLM_CALL_COUNTERS.get("completed") or 0),
            "failed": int(_LLM_CALL_COUNTERS.get("failed") or 0),
        }
        history_rows = [dict(item) for item in _LLM_CALL_HISTORY] if include_history else []

    out = {
        "active_total": int(len(active_calls)),
        "totals": totals,
        "active_by_kind": by_kind,
        "active_by_source": by_source,
        "active_calls": active_calls,
    }
    if include_history:
        out["history"] = _llm_call_history_windows(history_rows)
    return out


def register_active_vision_call(
    *,
    api_base: str,
    model: str,
    source: str = "",
) -> str:
    origin = _infer_llm_call_origin()
    call_id = str(uuid.uuid4())
    started_at = time.time()
    row = {
        "id": call_id,
        "api_base": str(api_base or "").strip(),
        "model": str(model or "").strip(),
        "started_at": started_at,
        "kind": str(origin.get("kind") or "other"),
        "source": str(source or origin.get("source") or "unknown").strip() or "unknown",
        "module": str(origin.get("module") or ""),
        "path": str(origin.get("path") or ""),
        "function": str(origin.get("function") or ""),
    }
    with _ACTIVE_VISION_CALLS_LOCK:
        _ACTIVE_VISION_CALLS[call_id] = row
        _VISION_CALL_COUNTERS["started"] = int(_VISION_CALL_COUNTERS.get("started") or 0) + 1
    _persist_vision_runtime_counter_delta(started=1)
    return call_id


def finish_active_vision_call(
    call_id: str,
    *,
    error: Any = None,
    response_model: str = "",
) -> None:
    call_token = str(call_id or "").strip()
    if not call_token:
        return

    finished_at = time.time()
    with _ACTIVE_VISION_CALLS_LOCK:
        row = _ACTIVE_VISION_CALLS.pop(call_token, None)
        if not isinstance(row, dict):
            return
        started_at = float(row.get("started_at") or 0.0)
        duration_ms = max(0.0, (finished_at - started_at) * 1000.0) if started_at > 0.0 else 0.0
        ok = not bool(error)
        if ok:
            _VISION_CALL_COUNTERS["completed"] = int(_VISION_CALL_COUNTERS.get("completed") or 0) + 1
        else:
            _VISION_CALL_COUNTERS["failed"] = int(_VISION_CALL_COUNTERS.get("failed") or 0) + 1
        final_model = str(response_model or row.get("model") or "").strip()
        if final_model:
            row["model"] = final_model
        history_row = {
            "id": call_token,
            "finished_at": finished_at,
            "started_at": started_at,
            "duration_ms": duration_ms,
            "ok": bool(ok),
            "error": (str(error).strip() if error else ""),
            "kind": str(row.get("kind") or "other"),
            "source": str(row.get("source") or "unknown"),
            "module": str(row.get("module") or ""),
            "path": str(row.get("path") or ""),
            "function": str(row.get("function") or ""),
            "api_base": str(row.get("api_base") or ""),
            "model": final_model,
        }
        _VISION_CALL_HISTORY.append(history_row)
        overflow = len(_VISION_CALL_HISTORY) - int(_VISION_CALL_HISTORY_MAX)
        if overflow > 0:
            del _VISION_CALL_HISTORY[:overflow]

    if ok:
        _persist_vision_runtime_counter_delta(completed=1)
    else:
        _persist_vision_runtime_counter_delta(failed=1)
    _persist_vision_runtime_history_row(history_row)


def get_active_vision_calls_snapshot(*, limit: int = 100) -> List[Dict[str, Any]]:
    max_items = max(1, min(int(limit or 0), 500))
    now = time.time()

    with _ACTIVE_VISION_CALLS_LOCK:
        rows = [dict(item) for item in _ACTIVE_VISION_CALLS.values() if isinstance(item, dict)]

    rows.sort(key=lambda row: float(row.get("started_at") or 0.0))
    out: List[Dict[str, Any]] = []
    for row in rows[-max_items:]:
        started_at = float(row.get("started_at") or 0.0)
        age_seconds = max(0, int(now - started_at)) if started_at > 0 else 0
        kind = str(row.get("kind") or "other")
        source = str(row.get("source") or "unknown")
        out.append(
            {
                "id": str(row.get("id") or ""),
                "kind": kind,
                "kind_label": _llm_origin_kind_label(kind),
                "source": source,
                "source_label": f"{_llm_origin_kind_label(kind)} - {source}",
                "module": str(row.get("module") or ""),
                "path": str(row.get("path") or ""),
                "function": str(row.get("function") or ""),
                "api_base": str(row.get("api_base") or ""),
                "model": str(row.get("model") or ""),
                "started_at": started_at,
                "age_seconds": age_seconds,
            }
        )
    return out


def get_vision_call_runtime_summary(*, include_history: bool = False) -> Dict[str, Any]:
    active_calls = get_active_vision_calls_snapshot(limit=120)
    by_kind_counts: Dict[str, int] = {}
    by_source_counts: Dict[str, int] = {}

    for row in active_calls:
        kind = str(row.get("kind") or "other")
        source = str(row.get("source") or "unknown")
        by_kind_counts[kind] = int(by_kind_counts.get(kind) or 0) + 1
        source_key = f"{kind}:{source}"
        by_source_counts[source_key] = int(by_source_counts.get(source_key) or 0) + 1

    by_kind = [
        {
            "kind": kind,
            "label": _llm_origin_kind_label(kind),
            "calls": int(count),
        }
        for kind, count in by_kind_counts.items()
    ]
    by_kind.sort(key=lambda row: (-int(row.get("calls") or 0), str(row.get("label") or "")))

    by_source = []
    for source_key, count in by_source_counts.items():
        kind, _sep, source = str(source_key or "").partition(":")
        by_source.append(
            {
                "kind": kind or "other",
                "source": source or "unknown",
                "label": f"{_llm_origin_kind_label(kind or 'other')} - {source or 'unknown'}",
                "calls": int(count),
            }
        )
    by_source.sort(key=lambda row: (-int(row.get("calls") or 0), str(row.get("label") or "")))

    with _ACTIVE_VISION_CALLS_LOCK:
        totals = {
            "started": int(_VISION_CALL_COUNTERS.get("started") or 0),
            "completed": int(_VISION_CALL_COUNTERS.get("completed") or 0),
            "failed": int(_VISION_CALL_COUNTERS.get("failed") or 0),
        }
        history_rows = [dict(item) for item in _VISION_CALL_HISTORY] if include_history else []

    out = {
        "active_total": int(len(active_calls)),
        "totals": totals,
        "active_by_kind": by_kind,
        "active_by_source": by_source,
        "active_calls": active_calls,
    }
    if include_history:
        out["history"] = _llm_call_history_windows(history_rows)
    return out


def build_llm_host_from_env(default_host="127.0.0.1", default_port="11434") -> str:
    """
    Legacy helper name kept for compatibility.
    Returns Hydra Base LLM endpoint from Redis settings.
    """
    endpoint, _ = _resolve_hydra_llm_defaults()
    return endpoint

def get_llm_client_from_env(host: Optional[str] = None, model: Optional[str] = None, **kwargs) -> "LLMClientWrapper":
    """
    Construct an LLMClientWrapper using explicit host/model overrides,
    with Hydra Base LLM settings fallback from Redis.
    No .env host/model fallback is used.
    """
    redis_conn = kwargs.pop("redis_conn", None)
    explicit_host = str(host or "").strip()
    explicit_model = str(model or "").strip()

    base_servers = resolve_hydra_base_servers(redis_conn=redis_conn, include_legacy=True)
    default_host = str(base_servers[0]["endpoint"]).strip() if base_servers else ""
    default_model = str(base_servers[0]["model"]).strip() if base_servers else ""
    if not default_host or not default_model:
        fallback_host, fallback_model = _resolve_hydra_llm_defaults(redis_conn=redis_conn)
        if not default_host:
            default_host = fallback_host
        if not default_model:
            default_model = fallback_model

    resolved_host = explicit_host or default_host
    resolved_model = explicit_model or default_model
    if not resolved_host or not resolved_model:
        raise RuntimeError(HYDRA_LLM_SETUP_ERROR)

    if not explicit_host and not explicit_model and len(base_servers) > 1:
        clients: List[LLMClientWrapper] = []
        signature_parts: List[str] = []
        for row in base_servers:
            endpoint = str(row.get("endpoint") or "").strip()
            row_model = str(row.get("model") or "").strip()
            if not endpoint or not row_model:
                continue
            clients.append(LLMClientWrapper(host=endpoint, model=row_model, **kwargs))
            signature_parts.append(f"{endpoint}|{row_model}")
        if len(clients) > 1:
            pool_key = "||".join(signature_parts)
            return RoundRobinLLMClientWrapper(clients=clients, pool_key=pool_key)
        if len(clients) == 1:
            return clients[0]

    return LLMClientWrapper(host=resolved_host, model=resolved_model, **kwargs)

class LLMClientWrapper:
    def __init__(self, host, model=None, **kwargs):
        resolved_host = str(host or "").strip()
        resolved_model = str(model or "").strip()
        if not resolved_host or not resolved_model:
            raise RuntimeError(HYDRA_LLM_SETUP_ERROR)

        base_url = _normalize_base_url(resolved_host)

        self.client = AsyncOpenAI(
            base_url=base_url,
            api_key=os.getenv("LLM_API_KEY", "not-needed"),
            **kwargs
        )

        self.host = base_url.rstrip("/")
        self.model = resolved_model

        # Common generation defaults (caller can override per-call)
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.7"))

        # Per-instance perf aggregation for one chat turn.
        self._llm_calls = 0
        self._llm_elapsed_sec = 0.0
        self._llm_prompt_tokens = 0
        self._llm_completion_tokens = 0
        self._llm_total_tokens = 0
        self._llm_model_last = str(resolved_model or "").strip() or "LLM"

    async def aclose(self):
        client = getattr(self, "client", None)
        if client is None:
            return

        close_async = getattr(client, "aclose", None)
        if callable(close_async):
            try:
                await close_async()
            except RuntimeError as exc:
                # Defensive shutdown guard: can happen if app is tearing down.
                if "Event loop is closed" not in str(exc):
                    raise
            return

        close_sync = getattr(client, "close", None)
        if callable(close_sync):
            result = close_sync()
            if asyncio.iscoroutine(result):
                try:
                    await result
                except RuntimeError as exc:
                    if "Event loop is closed" not in str(exc):
                        raise

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    def get_perf_stats(self, *, reset: bool = False) -> Dict[str, Any]:
        elapsed = max(0.0, float(self._llm_elapsed_sec))
        prompt_tokens = max(0, int(self._llm_prompt_tokens))
        completion_tokens = max(0, int(self._llm_completion_tokens))
        total_tokens = max(0, int(self._llm_total_tokens))
        tps_total = (float(total_tokens) / elapsed) if elapsed > 0.0 and total_tokens > 0 else 0.0
        tps_comp = (float(completion_tokens) / elapsed) if elapsed > 0.0 and completion_tokens > 0 else 0.0

        out: Dict[str, Any] = {
            "model": str(self._llm_model_last or self.model or "LLM"),
            "elapsed": round(elapsed, 6),
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "tps_total": round(tps_total, 4),
            "tps_comp": round(tps_comp, 4),
            "calls": max(0, int(self._llm_calls)),
        }

        if reset:
            self._llm_calls = 0
            self._llm_elapsed_sec = 0.0
            self._llm_prompt_tokens = 0
            self._llm_completion_tokens = 0
            self._llm_total_tokens = 0
        return out

    async def chat(self, messages, **kwargs):
        """
        Thin wrapper around OpenAI-compatible /v1/chat/completions.
        Accepts either timeout (seconds) or timeout_ms (milliseconds).
        Returns: {"model": str, "message": {"role": "assistant", "content": "..."}}
        """
        # Normalize timeout variants
        timeout = kwargs.pop("timeout", None)
        timeout_ms = kwargs.pop("timeout_ms", None)
        if timeout is None and timeout_ms is not None:
            try:
                timeout = float(timeout_ms) / 1000.0
            except Exception:
                timeout = None

        stream = kwargs.pop("stream", False)
        activity_hint = kwargs.pop("activity", "")
        model = kwargs.pop("model", self.model)

        # Provide sensible defaults if not supplied. A caller can pass
        # max_tokens=None to explicitly disable token capping for this call.
        if "max_tokens" not in kwargs:
            kwargs["max_tokens"] = self.max_tokens
        elif kwargs.get("max_tokens") is None:
            kwargs.pop("max_tokens", None)
        if "temperature" not in kwargs:
            kwargs["temperature"] = self.temperature

        # sanitize messages (prevents empty-user poison + normalizes non-string content)
        try:
            messages = _sanitize_chat_messages(messages if isinstance(messages, list) else [])
        except Exception:
            # fail open: at least avoid crashing
            messages = messages if isinstance(messages, list) else []

        call_id = _register_active_llm_call(
            host=str(self.host or "").strip(),
            model=str(model or "").strip(),
            stream=bool(stream),
            message_count=(len(messages) if isinstance(messages, list) else 0),
            messages=(messages if isinstance(messages, list) else []),
            activity_hint=str(activity_hint or ""),
        )
        call_error: Optional[Exception] = None
        final_model = str(model or "").strip()

        try:
            started_at = asyncio.get_running_loop().time()
            response = await self.client.chat.completions.create(
                model=model,
                messages=messages,
                stream=stream,
                timeout=timeout,
                **kwargs,
            )
            elapsed = max(0.0, float(asyncio.get_running_loop().time() - started_at))
            self._llm_calls += 1
            self._llm_elapsed_sec += elapsed

            response_model = getattr(response, "model", model)
            if response_model:
                self._llm_model_last = str(response_model)
            final_model = str(response_model or final_model or "").strip()

            usage = getattr(response, "usage", None)
            prompt_tokens = 0
            completion_tokens = 0
            total_tokens = 0
            try:
                if isinstance(usage, dict):
                    prompt_tokens = int(usage.get("prompt_tokens") or 0)
                    completion_tokens = int(usage.get("completion_tokens") or 0)
                    total_tokens = int(usage.get("total_tokens") or 0)
                elif usage is not None:
                    prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
                    completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
                    total_tokens = int(getattr(usage, "total_tokens", 0) or 0)
            except Exception:
                prompt_tokens = 0
                completion_tokens = 0
                total_tokens = 0
            if total_tokens <= 0:
                total_tokens = max(0, prompt_tokens + completion_tokens)

            self._llm_prompt_tokens += max(0, prompt_tokens)
            self._llm_completion_tokens += max(0, completion_tokens)
            self._llm_total_tokens += max(0, total_tokens)

            if stream:
                return response

            # Defensive: choices can be empty in edge cases / errors
            if not getattr(response, "choices", None):
                return {
                    "model": getattr(response, "model", model),
                    "message": {"role": "assistant", "content": ""},
                }

            choice = response.choices[0].message or {}
            raw_content = getattr(choice, "content", "") if hasattr(choice, "content") else choice.get("content", "")
            content_text = _coerce_content_to_text(raw_content)

            return {
                "model": getattr(response, "model", model),
                "message": {"role": getattr(choice, "role", "assistant"), "content": content_text},
            }
        except Exception as exc:
            call_error = exc
            raise
        finally:
            _finish_active_llm_call(
                call_id,
                error=call_error,
                response_model=final_model,
            )


class RoundRobinLLMClientWrapper:
    def __init__(self, *, clients: List[LLMClientWrapper], pool_key: str = ""):
        self._clients = [client for client in (clients or []) if client is not None]
        if not self._clients:
            raise RuntimeError(HYDRA_LLM_SETUP_ERROR)
        self._pool_key = str(pool_key or "").strip()
        self.host = str(getattr(self._clients[0], "host", "") or "")
        self.model = str(getattr(self._clients[0], "model", "") or "")

    def _select_client(self) -> LLMClientWrapper:
        if len(self._clients) == 1:
            return self._clients[0]
        idx = _next_hydra_base_rr_index(self._pool_key, len(self._clients))
        return self._clients[idx]

    async def chat(self, messages, **kwargs):
        client = self._select_client()
        return await client.chat(messages, **kwargs)

    async def aclose(self):
        seen: set[int] = set()
        for client in self._clients:
            ident = id(client)
            if ident in seen:
                continue
            seen.add(ident)
            close_async = getattr(client, "aclose", None)
            if callable(close_async):
                try:
                    await close_async()
                except RuntimeError as exc:
                    if "Event loop is closed" not in str(exc):
                        raise

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    def get_perf_stats(self, *, reset: bool = False) -> Dict[str, Any]:
        elapsed_total = 0.0
        prompt_tokens_total = 0
        completion_tokens_total = 0
        total_tokens_total = 0
        calls_total = 0
        model_names: List[str] = []

        for client in self._clients:
            getter = getattr(client, "get_perf_stats", None)
            if not callable(getter):
                continue
            stats = getter(reset=reset)
            if not isinstance(stats, dict):
                continue
            elapsed_total += max(0.0, float(stats.get("elapsed") or 0.0))
            prompt_tokens_total += max(0, int(stats.get("prompt_tokens") or 0))
            completion_tokens_total += max(0, int(stats.get("completion_tokens") or 0))
            total_tokens_total += max(0, int(stats.get("total_tokens") or 0))
            calls_total += max(0, int(stats.get("calls") or 0))
            model_name = str(stats.get("model") or "").strip()
            if model_name and model_name not in model_names:
                model_names.append(model_name)

        if total_tokens_total <= 0:
            total_tokens_total = max(0, prompt_tokens_total + completion_tokens_total)

        tps_total = (
            float(total_tokens_total) / float(elapsed_total)
            if elapsed_total > 0.0 and total_tokens_total > 0
            else 0.0
        )
        tps_comp = (
            float(completion_tokens_total) / float(elapsed_total)
            if elapsed_total > 0.0 and completion_tokens_total > 0
            else 0.0
        )

        model_label = ", ".join(model_names[:4]).strip()
        if len(model_names) > 4:
            model_label = f"{model_label}, +{len(model_names) - 4}"
        if model_label:
            model_label = f"round_robin({model_label})"
        else:
            model_label = "round_robin"

        return {
            "model": model_label,
            "elapsed": round(elapsed_total, 6),
            "prompt_tokens": prompt_tokens_total,
            "completion_tokens": completion_tokens_total,
            "total_tokens": total_tokens_total,
            "tps_total": round(tps_total, 4),
            "tps_comp": round(tps_comp, 4),
            "calls": calls_total,
        }

# ---------------------------------------------------------
# Function JSON parsing helpers (unchanged)
# ---------------------------------------------------------
def extract_json(text: str):
    """
    Extract the first valid JSON object or array from text.
    Strips code fences and tolerates extra prose around it.
    Works for both { ... } and [ ... ] blocks.
    """
    if not text:
        return None

    s = text.strip()

    # Remove ```json fences
    if s.startswith("```") and s.endswith("```"):
        s = re.sub(r"^```(?:json)?\s*|\s*```$", "", s, flags=re.MULTILINE).strip()

    # Try whole text first
    try:
        json.loads(s)
        return s
    except Exception:
        pass

    # Bracket scanning for either {...} or [...]
    stack = []
    start_idx = None
    for i, char in enumerate(s):
        if char in "{[":
            if not stack:
                start_idx = i
            stack.append(char)
        elif char in "}]":
            if stack:
                opening = stack.pop()
                if not stack and start_idx is not None:
                    candidate = s[start_idx:i+1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except json.JSONDecodeError:
                        continue
    return None

TOOL_MARKUP_REPAIR_PROMPT = (
    "Formatting error: do not emit tool channel markup like <|channel|> or to=... . "
    "If you need to call a tool, respond with exactly ONE strict JSON object and nothing else: "
    "{\"function\":\"name\",\"arguments\":{...}}. "
    "Do not include extra prose, and do not output multiple tool calls. "
    "Otherwise respond with NO_TOOL."
)
TOOL_MARKUP_FAILURE_TEXT = "Sorry, I had trouble formatting a tool call. Please try again."

def looks_like_tool_markup(text: str) -> bool:
    if not text:
        return False
    s = str(text)
    if "<|" in s and "|>" in s:
        return True
    if re.search(r"\bto=[A-Za-z0-9_.-]+\b", s) and ("commentary" in s or "message" in s):
        return True
    return False

def parse_function_json(response_text: str):
    _DECISION_PREFIX_RE = re.compile(
        r"^\s*(CONTINUE|RETRY|ASK[\s_-]*USER|FAIL|FINAL|FINAL[\s_-]*ANSWER|RETRY[\s_-]*TOOL|NEED[\s_-]*USER[\s_-]*INFO|ANSWER|NO_TOOL)\s*:\s*(.*)$",
        re.IGNORECASE | re.DOTALL,
    )
    _NON_TOOL_PREFIXES = {
        "continue",
        "retry",
        "ask_user",
        "fail",
        "final",
        "retry_tool",
        "final_answer",
        "need_user_info",
        "answer",
        "no_tool",
    }

    def _pick(obj):
        if isinstance(obj, dict):
            if "function" in obj and isinstance(obj["function"], str):
                return {"function": obj["function"], "arguments": obj.get("arguments", {}) or {}}
            if "tool" in obj and isinstance(obj["tool"], str):
                return {"function": obj["tool"], "arguments": obj.get("arguments", {}) or {}}
        return None

    if not response_text:
        return None

    s = str(response_text).strip()

    # Remove code fence markers anywhere to avoid treating ```json as a tool name.
    if "```" in s:
        s = re.sub(r"```(?:json)?", "", s, flags=re.IGNORECASE).strip()

    # ---------------------------------------------------------
    # Tool-call markup (Codex/OpenAI style) support:
    #   <|channel|>commentary to=repo_browser.get_verba_help <|message|>{"verba_id":"weather_forecast"}
    # ---------------------------------------------------------
    m_tool = re.search(r"to=([a-zA-Z0-9_.-]+).*?<\|message\|>(\{.*\})", s, re.DOTALL)
    if m_tool:
        tool_name = m_tool.group(1).strip()
        tool_name = tool_name.split(".")[-1] if tool_name else tool_name
        blob = m_tool.group(2).strip()
        try:
            args = json.loads(blob)
            if isinstance(args, dict):
                if tool_name == "get_verba_help":
                    if "verba_id" not in args and "name" in args:
                        args["verba_id"] = args.get("name")
                return {"function": tool_name, "arguments": args}
        except Exception:
            return {"function": tool_name, "arguments": {}}

    # strip code fences early so shorthand/prefix parsing still works
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.MULTILINE).strip()
    if s.endswith("```"):
        s = re.sub(r"\s*```$", "", s, flags=re.MULTILINE).strip()

    # Handle channel wrappers:
    #   <|channel|>final ANSWER<|message|>RETRY_TOOL: {...}
    if "<|message|>" in s:
        s = s.rsplit("<|message|>", 1)[-1].strip()

    # Handle Minos decision wrappers:
    #   RETRY_TOOL: {...}
    #   FINAL_ANSWER: ...
    decision_match = _DECISION_PREFIX_RE.match(s)
    if decision_match:
        decision_kind = str(decision_match.group(1) or "").strip().lower().replace(" ", "_").replace("-", "_")
        decision_body = str(decision_match.group(2) or "").strip()
        if decision_kind.startswith("retry"):
            s = decision_body
        else:
            # FINAL_ANSWER / NEED_USER_INFO / NO_TOOL are not tool calls.
            return None

    # Accept shorthand ONLY when it's the whole message: plugin_id{"arg":"..."}
    m = re.match(r'^([a-zA-Z0-9_]+)\s*(\{.*\})\s*$', s, re.DOTALL)
    if m:
        func = m.group(1)
        blob = m.group(2)
        try:
            args = json.loads(blob)
            if isinstance(args, dict):
                return {"function": func, "arguments": args}
        except Exception:
            pass

    try:
        response_json = json.loads(s)
    except json.JSONDecodeError:
        json_str = extract_json(s)
        if not json_str:
            json_str = None

        if json_str:
            prefix = s.split(json_str, 1)[0].strip()
        else:
            prefix = ""

        # Slightly stricter "possible func" match
        m2 = re.search(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*[:(]*\s*$', prefix)
        if json_str:
            try:
                args = json.loads(json_str)
                picked = _pick(args)
                if picked:
                    return picked
            except Exception:
                args = None

            if m2:
                possible_func = str(m2.group(1) or "").strip()
                if possible_func and possible_func.lower() not in _NON_TOOL_PREFIXES:
                    try:
                        args = json.loads(json_str)
                        if isinstance(args, dict):
                            return {"function": possible_func, "arguments": args}
                    except Exception:
                        pass

        if json_str:
            try:
                response_json = json.loads(json_str)
            except Exception:
                response_json = None
        else:
            response_json = None

    picked = _pick(response_json)
    if picked:
        return picked

    if isinstance(response_json, list):
        for item in response_json:
            picked = _pick(item)
            if picked:
                return picked

    # ---------------------------------------------------------
    # Relaxed fallback: tolerate invalid JSON for file/code tools
    # ---------------------------------------------------------
    def _extract_relaxed_string(text: str, key: str) -> str | None:
        pat = rf'"{re.escape(key)}"\s*:\s*"(.*)"\s*\}}\s*\}}\s*$'
        m = re.search(pat, text, flags=re.DOTALL)
        if m:
            return m.group(1)
        pat = rf"'{re.escape(key)}'\s*:\s*'(.*)'\s*\}}\s*\}}\s*$"
        m = re.search(pat, text, flags=re.DOTALL)
        if m:
            return m.group(1)
        return None

    def _extract_relaxed_scalar(text: str, key: str) -> str | None:
        pat = rf'"{re.escape(key)}"\s*:\s*"([^"]+)"'
        m = re.search(pat, text)
        if m:
            return m.group(1)
        pat = rf"'{re.escape(key)}'\s*:\s*'([^']+)'"
        m = re.search(pat, text)
        if m:
            return m.group(1)
        return None

    def _relaxed_tool_call(text: str) -> dict | None:
        fm = re.search(r'"function"\s*:\s*"([^"]+)"', text)
        if not fm:
            fm = re.search(r"'function'\s*:\s*'([^']+)'", text)
        if not fm:
            return None
        func = fm.group(1).strip()
        if not func:
            return None

        if func in {"write_file"}:
            path = _extract_relaxed_scalar(text, "path")
            content = _extract_relaxed_string(text, "content")
            if path and content is not None:
                return {"function": func, "arguments": {"path": path, "content": content}}
        return None

    relaxed = _relaxed_tool_call(s)
    if relaxed:
        return relaxed

    return None

# ---------------------------------------------------------
# Get latest image from redis
# ---------------------------------------------------------
def get_latest_image_from_history(key: str, allowed_mimetypes=None):
    if allowed_mimetypes is None:
        allowed_mimetypes = ["image/png", "image/jpeg"]

    history = redis_client.lrange(key, 0, -1)
    for entry in reversed(history):
        try:
            msg = json.loads(entry)
            content = msg.get("content")

            # 🔥 Unwrap plugin_response wrapper
            if isinstance(content, dict) and content.get("marker") == "plugin_response":
                content = content.get("content", {})

            if isinstance(content, dict):
                mimetype = content.get("mimetype", "")
                filename = content.get("name", "").lower()

                if (
                    content.get("type") == "image"
                    and content.get("data")
                    and mimetype in allowed_mimetypes
                    and not filename.endswith(".webp")
                ):
                    image_bytes = base64.b64decode(content["data"])
                    return image_bytes, filename or "input.png"
        except Exception:
            continue

    return None, None

# ---------------------------------------------------------
# Get latest file from redis
# ---------------------------------------------------------
def get_latest_file_from_history(channel_id, filetype="file", extensions=None):
    history_key = f"tater:channel:{channel_id}:history"
    raw_history = redis_client.lrange(history_key, 0, -1)

    for entry in reversed(raw_history):
        try:
            data = json.loads(entry)
            content = data.get("content")

            # 🔥 Unwrap plugin_response wrapper
            if isinstance(content, dict) and content.get("marker") == "plugin_response":
                content = content.get("content", {})

            if isinstance(content, dict) and content.get("type") == filetype:
                filename = content.get("name", "").lower()
                if not extensions or any(filename.endswith(ext) for ext in extensions):
                    return content
        except Exception:
            continue

    return None

# ---------------------------------------------------------
# ComfyUI websocket (no timeouts, Ctrl-C friendly)
# ---------------------------------------------------------
def run_comfy_prompt(base_http: str, base_ws: str, prompt: dict):
    client_id = str(uuid.uuid4())

    # 1) Open dedicated WS for this job (no timeout)
    ws = websocket.create_connection(f"{base_ws}/ws?clientId={client_id}")

    try:
        # 2) POST the prompt, include client_id (no timeout)
        resp = requests.post(
            f"{base_http}/prompt",
            json={"prompt": prompt, "client_id": client_id}
        )
        resp.raise_for_status()
        data = resp.json()
        prompt_id = data.get("prompt_id") or data.get("promptId")
        if not prompt_id:
            raise RuntimeError(f"ComfyUI /prompt did not return prompt_id: {data}")

        # 3) Listen until our prompt is finished
        while True:
            try:
                raw = ws.recv()  # blocks; KeyboardInterrupt will break out cleanly
            except KeyboardInterrupt:
                # Graceful cancel: close socket and bubble up so caller can handle it
                try:
                    ws.close()
                finally:
                    raise
            except Exception as e:
                # Other WS errors bubble as runtime errors
                raise RuntimeError(f"ComfyUI WS error for prompt {prompt_id}: {e}")

            if not raw:
                continue

            try:
                evt = json.loads(raw)
            except Exception:
                continue

            etype = evt.get("type")
            edata = evt.get("data") or {}
            evt_prompt_id = edata.get("prompt_id") or evt.get("prompt_id")

            # Only react to our own prompt
            if evt_prompt_id != prompt_id:
                continue

            # Finished: 'executing' with node == None indicates completion
            if etype == "executing" and edata.get("node") is None:
                return prompt_id, evt

            # (Optional: handle other terminal frames here if your setup emits them.)

    finally:
        try:
            ws.close()
        except Exception:
            pass
