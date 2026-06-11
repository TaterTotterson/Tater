import os
import asyncio
import threading
import inspect
import logging
import contextlib
import gc
import io
import functools
import queue
import shutil
import signal
import subprocess
import sys
import tempfile
import hashlib
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
import platform
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor
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
    migrate_current_redis_to_internal,
    save_redis_connection_settings,
    shutdown_internal_redis,
    test_redis_connection_settings,
)
from tater_paths import agent_lab_path, runtime_dir

load_dotenv()
nest_asyncio.apply()

logger = logging.getLogger("tateros.helpers")

_INTERNAL_PORTAL_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1", "0.0.0.0"}
_INTERNAL_PORTAL_AUTH_TARGETS = (
    ("homekit_portal_settings", 8501, "/api/portals/homekit_portal/api/tater-homekit/v1", "AUTH_TOKEN"),
    ("xbmc_portal_settings", 8501, "/api/portals/xbmc_portal/api/tater-xbmc/v1", None),
    ("macos_portal_settings", 8501, "/api/portals/macos_portal/api/macos", "AUTH_TOKEN"),
    ("voice_core_settings", 8501, "/tater-ha/v1/voice", None),
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


def _main_app_port(default: int = 8501) -> int:
    return _port_or_default(os.getenv("HTMLUI_PORT"), default)


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

            if str(path_prefix or "").startswith("/api/portals/") or settings_key == "voice_core_settings":
                port = _main_app_port(default_port)
            else:
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
HYDRA_LLM_PROVIDER_KEY = "tater:hydra:llm_provider"
HYDRA_LLM_BASE_SERVERS_KEY = "tater:hydra:llm_base_servers"
SPUD_LINK_SETTINGS_KEY = "tater:spudlink:settings:v1"
SPUD_LINK_MODE_SPUDLET = "spudlet"
HYDRA_HF_TRANSFORMERS_CONTEXT_TOKENS_KEY = "tater:hydra:llm:hf_transformers_context_tokens"
HYDRA_HF_TRANSFORMERS_DEVICE_KEY = "tater:hydra:llm:hf_transformers_device"
HYDRA_HF_TRANSFORMERS_DTYPE_KEY = "tater:hydra:llm:hf_transformers_dtype"
HYDRA_HF_TRANSFORMERS_DEVICE_MAP_KEY = "tater:hydra:llm:hf_transformers_device_map"
HYDRA_HF_TRANSFORMERS_ATTN_IMPLEMENTATION_KEY = "tater:hydra:llm:hf_transformers_attn_implementation"
HYDRA_HF_TRANSFORMERS_TRUST_REMOTE_CODE_KEY = "tater:hydra:llm:hf_transformers_trust_remote_code"
HYDRA_LLAMA_CPP_CONTEXT_TOKENS_KEY = "tater:hydra:llm:llama_cpp_context_tokens"
HYDRA_LLAMA_CPP_VISION_CONTEXT_TOKENS_KEY = "tater:hydra:llm:llama_cpp_vision_context_tokens"
HYDRA_LLAMA_CPP_MTP_ENABLED_KEY = "tater:hydra:llm:llama_cpp_mtp_enabled"
HYDRA_LLAMA_CPP_MTP_DRAFT_TOKENS_KEY = "tater:hydra:llm:llama_cpp_mtp_draft_tokens"
HYDRA_LLAMA_CPP_N_BATCH_KEY = "tater:hydra:llm:llama_cpp_n_batch"
HYDRA_LLAMA_CPP_N_UBATCH_KEY = "tater:hydra:llm:llama_cpp_n_ubatch"
HYDRA_LLAMA_CPP_FLASH_ATTN_KEY = "tater:hydra:llm:llama_cpp_flash_attn"
HYDRA_LLAMA_CPP_OFFLOAD_KQV_KEY = "tater:hydra:llm:llama_cpp_offload_kqv"
HYDRA_HF_TRANSFORMERS_CHAT_TEMPLATE_OVERRIDES_KEY = "tater:hydra:llm:hf_transformers_chat_template_overrides"
HYDRA_LLAMA_CPP_CHAT_TEMPLATE_OVERRIDES_KEY = "tater:hydra:llm:llama_cpp_chat_template_overrides"
HYDRA_MLX_LM_CONTEXT_TOKENS_KEY = "tater:hydra:llm:mlx_lm_context_tokens"
HYDRA_MLX_LM_TRUST_REMOTE_CODE_KEY = "tater:hydra:llm:mlx_lm_trust_remote_code"
HYDRA_MLX_LM_LAZY_LOAD_KEY = "tater:hydra:llm:mlx_lm_lazy_load"
HYDRA_MLX_ENGINE_PREFILL_STEP_SIZE_KEY = "tater:hydra:llm:mlx_engine_prefill_step_size"
HYDRA_MLX_ENGINE_KV_BITS_KEY = "tater:hydra:llm:mlx_engine_kv_bits"
HYDRA_MLX_ENGINE_KV_GROUP_SIZE_KEY = "tater:hydra:llm:mlx_engine_kv_group_size"
HYDRA_MLX_ENGINE_QUANTIZED_KV_START_KEY = "tater:hydra:llm:mlx_engine_quantized_kv_start"
HYDRA_MLX_LM_CHAT_TEMPLATE_OVERRIDES_KEY = "tater:hydra:llm:mlx_lm_chat_template_overrides"
DEFAULT_HF_TRANSFORMERS_CONTEXT_TOKENS = 4096
DEFAULT_HF_TRANSFORMERS_DEVICE = "auto"
DEFAULT_HF_TRANSFORMERS_DTYPE = "auto"
DEFAULT_HF_TRANSFORMERS_DEVICE_MAP = "default"
DEFAULT_HF_TRANSFORMERS_ATTN_IMPLEMENTATION = ""
DEFAULT_HF_TRANSFORMERS_TRUST_REMOTE_CODE = False
DEFAULT_LLAMA_CPP_CONTEXT_TOKENS = 4096
DEFAULT_LLAMA_CPP_VISION_CONTEXT_TOKENS = 4096
DEFAULT_LLAMA_CPP_MTP_ENABLED = False
DEFAULT_LLAMA_CPP_MTP_DRAFT_TOKENS = 3
DEFAULT_LLAMA_CPP_N_BATCH = 512
DEFAULT_LLAMA_CPP_N_UBATCH = 0
DEFAULT_LLAMA_CPP_FLASH_ATTN = False
DEFAULT_LLAMA_CPP_OFFLOAD_KQV = True
DEFAULT_MLX_LM_TRUST_REMOTE_CODE = False
DEFAULT_MLX_LM_LAZY_LOAD = False
DEFAULT_MLX_ENGINE_PREFILL_STEP_SIZE = 2048
MEDIUM_MLX_ENGINE_PREFILL_STEP_SIZE = 4096
FAST_MLX_ENGINE_PREFILL_STEP_SIZE = 8192
MAX_MLX_ENGINE_PREFILL_STEP_SIZE = 32768
HYDRA_LLM_SETUP_ERROR = (
    "Hydra LLM is not configured. Open Settings > Models and set a base provider, endpoint if required, and model."
)
HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE = "openai_compatible"
HYDRA_LLM_PROVIDER_HF_TRANSFORMERS = "hf_transformers"
HYDRA_LLM_PROVIDER_LLAMA_CPP = "llama_cpp"
HYDRA_LLM_PROVIDER_MLX_LM = "mlx_lm"
HYDRA_LLM_PROVIDER_SPUD_LINK = "spud_link"
HYDRA_LLM_PROVIDER_CHOICES = {
    HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE,
    HYDRA_LLM_PROVIDER_HF_TRANSFORMERS,
    HYDRA_LLM_PROVIDER_LLAMA_CPP,
    HYDRA_LLM_PROVIDER_MLX_LM,
    HYDRA_LLM_PROVIDER_SPUD_LINK,
}
_HYDRA_BASE_RR_LOCK = threading.Lock()
_HYDRA_BASE_RR_INDEX: Dict[str, int] = {}
_ACTIVE_LLM_CALLS_LOCK = threading.RLock()
_ACTIVE_LLM_CALLS: Dict[str, Dict[str, Any]] = {}
_LLM_CALL_HISTORY: List[Dict[str, Any]] = []
_LLM_CALL_HISTORY_MAX = 5000
_LLM_CALL_COUNTERS: Dict[str, int] = {"started": 0, "completed": 0, "failed": 0}
_LLM_CALL_COUNTERS_REDIS_KEY = "tater:llm:runtime:counters"
_LLM_CALL_HISTORY_REDIS_KEY = "tater:llm:runtime:history"
_LLM_DEBUG_EVENTS_LOCK = threading.RLock()
_LLM_DEBUG_EVENTS: List[Dict[str, Any]] = []
_LLM_DEBUG_EVENTS_MAX = 1200
_LLM_DEBUG_EVENT_SEQ = 0
_ACTIVE_VISION_CALLS_LOCK = threading.RLock()
_ACTIVE_VISION_CALLS: Dict[str, Dict[str, Any]] = {}
_VISION_CALL_HISTORY: List[Dict[str, Any]] = []
_VISION_CALL_HISTORY_MAX = 5000
_VISION_CALL_COUNTERS: Dict[str, int] = {"started": 0, "completed": 0, "failed": 0}
_VISION_CALL_COUNTERS_REDIS_KEY = "tater:vision:runtime:counters"
_VISION_CALL_HISTORY_REDIS_KEY = "tater:vision:runtime:history"
_HF_LLM_MODEL_CACHE: Dict[Tuple[str, str, str, bool], Dict[str, Any]] = {}
_HF_LLM_MODEL_CACHE_LOCK = threading.RLock()
_HF_LLM_GENERATION_LOCKS: Dict[Tuple[str, str, str, bool], threading.RLock] = {}
_LLAMA_CPP_MODEL_CACHE: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
_LLAMA_CPP_MODEL_CACHE_LOCK = threading.RLock()
_LLAMA_CPP_GENERATION_LOCKS: Dict[Tuple[Any, ...], threading.RLock] = {}
_LLAMA_CPP_PARTIAL_MODEL_DEL_PATCHED = False
_LLAMA_CPP_VISION_FAILURE_COOLDOWNS: Dict[str, Dict[str, Any]] = {}
_LLAMA_CPP_VISION_FAILURE_COOLDOWN_LOCK = threading.RLock()
_MLX_LM_MODEL_CACHE: Dict[Tuple[str, str, bool, bool], Dict[str, Any]] = {}
_MLX_LM_MODEL_CACHE_LOCK = threading.RLock()
_MLX_LM_GENERATION_LOCKS: Dict[Tuple[str, str, bool, bool], threading.RLock] = {}
_MLX_VLM_MODEL_CACHE: Dict[Tuple[str, bool, bool], Dict[str, Any]] = {}
_MLX_VLM_MODEL_CACHE_LOCK = threading.RLock()
_MLX_VLM_GENERATION_LOCKS: Dict[Tuple[str, bool, bool], threading.RLock] = {}
_MLX_ENGINE_MODEL_CACHE: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
_MLX_ENGINE_MODEL_CACHE_LOCK = threading.RLock()
_MLX_ENGINE_GENERATION_LOCKS: Dict[Tuple[Any, ...], threading.RLock] = {}
_MLX_RUNTIME_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tater-mlx-runtime")
_MLX_RUNTIME_LOCAL = threading.local()
_LOCAL_VISION_GENERATION_LOCK = threading.RLock()
HFProgressCallback = Callable[[Dict[str, Any]], None]


class HfLlmDownloadCancelled(RuntimeError):
    """Raised by progress callbacks to cooperatively stop local LLM downloads."""

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

    system_parts = [str(m.get("content") or "").strip() for m in cleaned if m.get("role") == "system"]
    if not system_parts:
        return cleaned

    # Some OpenAI-compatible local servers use strict Jinja chat templates that
    # only allow system content at message index 0. Keep all Tater context, but
    # coalesce it into one leading system message for maximum backend compatibility.
    merged_system = "\n\n".join(part for part in system_parts if part)
    non_system = [m for m in cleaned if m.get("role") != "system"]
    return ([{"role": "system", "content": merged_system}] if merged_system else []) + non_system

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


def _image_data_url(image_bytes: bytes, filename: str = "") -> str:
    import mimetypes

    mime = mimetypes.guess_type(str(filename or ""))[0] or "image/png"
    encoded = base64.b64encode(bytes(image_bytes or b"")).decode("utf-8")
    return f"data:{mime};base64,{encoded}"


def _image_url_value_from_block(block: Any) -> str:
    if not isinstance(block, dict):
        return ""
    image_url = block.get("image_url")
    if isinstance(image_url, dict):
        return str(image_url.get("url") or "").strip()
    if isinstance(image_url, str):
        return image_url.strip()
    for key in ("url", "src", "data_url", "image"):
        value = block.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _pil_image_from_url_value(value: Any) -> Any:
    url = str(value or "").strip()
    if not url:
        return None
    try:
        from PIL import Image  # type: ignore
    except Exception as exc:
        raise RuntimeError("Local vision needs Pillow installed to decode image inputs.") from exc

    raw: bytes
    if url.startswith("data:"):
        try:
            _, encoded = url.split(",", 1)
            raw = base64.b64decode(encoded)
        except Exception as exc:
            raise RuntimeError("Could not decode image data URL for local vision.") from exc
    elif url.startswith(("http://", "https://")):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            raw = bytes(response.content or b"")
        except Exception as exc:
            raise RuntimeError(f"Could not fetch image URL for local vision: {exc}") from exc
    else:
        path = os.path.abspath(os.path.expanduser(url[len("file://") :] if url.startswith("file://") else url))
        try:
            with open(path, "rb") as handle:
                raw = handle.read()
        except Exception as exc:
            raise RuntimeError(f"Could not read image file for local vision: {exc}") from exc

    try:
        image = Image.open(io.BytesIO(raw))
        return image.convert("RGB")
    except Exception as exc:
        raise RuntimeError("Could not decode image bytes for local vision.") from exc


def _extract_pil_images_from_messages(messages: List[Dict[str, Any]]) -> List[Any]:
    images: List[Any] = []
    for item in messages if isinstance(messages, list) else []:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = str(block.get("type") or "").strip().lower()
            if block_type not in {"image", "image_url", "input_image"} and "image_url" not in block:
                continue
            image = _pil_image_from_url_value(_image_url_value_from_block(block))
            if image is not None:
                images.append(image)
    return images


def _messages_with_hf_image_blocks(messages: List[Dict[str, Any]], images: List[Any]) -> List[Dict[str, Any]]:
    image_iter = iter(images)
    out: List[Dict[str, Any]] = []
    for item in messages if isinstance(messages, list) else []:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "user").strip().lower() or "user"
        content = item.get("content")
        if not isinstance(content, list):
            out.append({"role": role, "content": [{"type": "text", "text": _coerce_content_to_text(content).strip()}]})
            continue
        blocks: List[Dict[str, Any]] = []
        for block in content:
            if not isinstance(block, dict):
                text = _coerce_content_to_text(block).strip()
                if text:
                    blocks.append({"type": "text", "text": text})
                continue
            block_type = str(block.get("type") or "").strip().lower()
            if block_type in {"image", "image_url", "input_image"} or "image_url" in block:
                try:
                    blocks.append({"type": "image", "image": next(image_iter)})
                except StopIteration:
                    blocks.append({"type": "image"})
                continue
            text = _coerce_content_to_text(block.get("text") or block.get("content") or block.get("value")).strip()
            if text:
                blocks.append({"type": "text", "text": text})
        out.append({"role": role, "content": blocks})
    return out


def _safe_redis_text_get(key: str, *, redis_conn: Any = None) -> str:
    client = redis_conn or redis_client
    try:
        return str(client.get(key) or "").strip()
    except Exception:
        return ""


def _setting_text(redis_key: str, env_key: str, default: str = "") -> str:
    raw = _safe_redis_text_get(redis_key)
    if not raw and env_key:
        raw = str(os.getenv(env_key) or "").strip()
    return raw if raw else str(default or "")


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


def _normalize_hydra_llm_provider(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if token in {"hf", "huggingface", "hugging_face", "transformers", "hf_transformers", "local_transformers"}:
        return HYDRA_LLM_PROVIDER_HF_TRANSFORMERS
    if token in {"llama", "llamacpp", "llama_cpp", "llama.cpp", "gguf", "llama_cpp_python", "llama-cpp-python"}:
        return HYDRA_LLM_PROVIDER_LLAMA_CPP
    if token in {"mlx", "mlx_lm", "mlx-lm", "apple_mlx", "apple_silicon", "mlxlm"}:
        return HYDRA_LLM_PROVIDER_MLX_LM
    if token in {"spud", "spudlink", "spud_link", "spud_hub", "spudlet", "tater_native", "tater_spud_link"}:
        return HYDRA_LLM_PROVIDER_SPUD_LINK
    return HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE


def _is_local_hydra_llm_provider(provider: Any) -> bool:
    return _normalize_hydra_llm_provider(provider) in {
        HYDRA_LLM_PROVIDER_HF_TRANSFORMERS,
        HYDRA_LLM_PROVIDER_LLAMA_CPP,
        HYDRA_LLM_PROVIDER_MLX_LM,
    }


def _hf_llm_model_root() -> str:
    raw = str(os.getenv("TATER_HF_TRANSFORMERS_MODEL_ROOT") or "").strip()
    if raw:
        return os.path.abspath(os.path.expanduser(raw))
    return str(agent_lab_path("models", "llm", "huggingface"))


def _llama_cpp_model_root() -> str:
    raw = str(os.getenv("TATER_LLAMA_CPP_MODEL_ROOT") or "").strip()
    if raw:
        return os.path.abspath(os.path.expanduser(raw))
    return str(agent_lab_path("models", "llm", "llama-cpp"))


def _mlx_lm_model_root() -> str:
    raw = str(os.getenv("TATER_MLX_LM_MODEL_ROOT") or "").strip()
    if raw:
        return os.path.abspath(os.path.expanduser(raw))
    return str(agent_lab_path("models", "llm", "mlx"))


def _hf_llm_device_pref() -> str:
    token = _setting_text(
        HYDRA_HF_TRANSFORMERS_DEVICE_KEY,
        "TATER_HF_TRANSFORMERS_DEVICE",
        DEFAULT_HF_TRANSFORMERS_DEVICE,
    ).strip().lower()
    if token in {"", "default"}:
        return DEFAULT_HF_TRANSFORMERS_DEVICE
    if token in {"auto", "cuda", "mps", "cpu"}:
        return token
    return token or DEFAULT_HF_TRANSFORMERS_DEVICE


def _hf_llm_dtype_pref() -> str:
    token = _setting_text(
        HYDRA_HF_TRANSFORMERS_DTYPE_KEY,
        "TATER_HF_TRANSFORMERS_DTYPE",
        DEFAULT_HF_TRANSFORMERS_DTYPE,
    ).strip().lower()
    if token in {"", "default"}:
        return DEFAULT_HF_TRANSFORMERS_DTYPE
    aliases = {
        "fp16": "float16",
        "half": "float16",
        "bf16": "bfloat16",
        "fp32": "float32",
    }
    return aliases.get(token, token or DEFAULT_HF_TRANSFORMERS_DTYPE)


def _hf_llm_trust_remote_code() -> bool:
    raw = _safe_redis_text_get(HYDRA_HF_TRANSFORMERS_TRUST_REMOTE_CODE_KEY)
    if raw:
        return _boolish(raw, default=DEFAULT_HF_TRANSFORMERS_TRUST_REMOTE_CODE)
    return _boolish(os.getenv("TATER_HF_TRANSFORMERS_TRUST_REMOTE_CODE"), default=DEFAULT_HF_TRANSFORMERS_TRUST_REMOTE_CODE)


def _local_llm_context_tokens(
    redis_key: str,
    env_keys: Tuple[str, ...],
    default: Optional[int],
    *,
    minimum: int = 256,
    maximum: int = 1_048_576,
) -> Optional[int]:
    raw = _safe_redis_text_get(redis_key)
    if not raw:
        for env_key in env_keys:
            raw = str(os.getenv(env_key) or "").strip()
            if raw:
                break
    if not raw:
        return default
    try:
        value = int(str(raw).strip())
    except Exception:
        return default
    return max(int(minimum), min(int(maximum), int(value)))


def _hf_llm_max_input_tokens() -> int:
    value = _local_llm_context_tokens(
        HYDRA_HF_TRANSFORMERS_CONTEXT_TOKENS_KEY,
        ("TATER_HF_TRANSFORMERS_MAX_INPUT_TOKENS",),
        DEFAULT_HF_TRANSFORMERS_CONTEXT_TOKENS,
    )
    return int(value or DEFAULT_HF_TRANSFORMERS_CONTEXT_TOKENS)


def _hf_llm_resolve_device(torch_module: Any, preference: str) -> str:
    token = str(preference or "auto").strip().lower()
    if token and token != "auto":
        return token
    try:
        if bool(getattr(getattr(torch_module, "cuda", None), "is_available", lambda: False)()):
            return "cuda"
    except Exception:
        pass
    try:
        mps_backend = getattr(getattr(torch_module, "backends", None), "mps", None)
        if bool(getattr(mps_backend, "is_available", lambda: False)()):
            return "mps"
    except Exception:
        pass
    return "cpu"


def _hf_llm_torch_dtype(torch_module: Any, dtype_pref: str, device: str) -> Any:
    token = str(dtype_pref or "auto").strip().lower()
    if token in {"", "auto"}:
        return "auto"
    mapping = {
        "float16": "float16",
        "fp16": "float16",
        "half": "float16",
        "bfloat16": "bfloat16",
        "bf16": "bfloat16",
        "float32": "float32",
        "fp32": "float32",
    }
    attr = mapping.get(token, token)
    return getattr(torch_module, attr, "auto")


def _hf_llm_device_map_pref(device: str) -> str:
    raw = _setting_text(
        HYDRA_HF_TRANSFORMERS_DEVICE_MAP_KEY,
        "TATER_HF_TRANSFORMERS_DEVICE_MAP",
        DEFAULT_HF_TRANSFORMERS_DEVICE_MAP,
    ).strip().lower()
    if raw in {"", "default"}:
        raw = ""
    if raw in {"none", "off", "false", "0", "disabled"}:
        return ""
    if raw:
        return raw
    if str(device or "").startswith("cuda"):
        return "auto"
    return ""


def _hf_llm_attn_implementation() -> str:
    token = _setting_text(
        HYDRA_HF_TRANSFORMERS_ATTN_IMPLEMENTATION_KEY,
        "TATER_HF_TRANSFORMERS_ATTN_IMPLEMENTATION",
        DEFAULT_HF_TRANSFORMERS_ATTN_IMPLEMENTATION,
    ).strip().lower()
    if token in {"", "auto", "default", "none", "off", "false", "0"}:
        return ""
    allowed = {"sdpa", "flash_attention_2", "eager"}
    return token if token in allowed else ""


def _hf_llm_cache_key(model_id: str) -> Tuple[Any, ...]:
    return (
        str(model_id or "").strip(),
        _hf_llm_device_pref(),
        _hf_llm_dtype_pref(),
        _hf_llm_trust_remote_code(),
        _hf_llm_device_map_pref(_hf_llm_device_pref()),
        _hf_llm_attn_implementation(),
    )


def _huggingface_integration_token() -> str:
    try:
        from tateros import integration_store as integration_store_module

        env = integration_store_module.huggingface_environment(client=redis_client)
    except Exception:
        env = {}
    if not isinstance(env, dict):
        return ""
    for key in (
        "HF_TOKEN",
        "HUGGINGFACE_HUB_TOKEN",
        "HUGGING_FACE_HUB_TOKEN",
        "HUGGINGFACE_TOKEN",
        "HF_HUB_TOKEN",
        "HUGGINGFACE_API_TOKEN",
    ):
        token = _text(env.get(key)).strip()
        if token:
            return token
    return ""


def _hf_llm_hub_token() -> Optional[str]:
    token = _text(os.getenv("TATER_HF_TRANSFORMERS_TOKEN")).strip()
    if not token:
        token = _huggingface_integration_token()
    if not token:
        token = _text(
            os.getenv("HF_TOKEN")
            or os.getenv("HUGGINGFACE_HUB_TOKEN")
            or os.getenv("HUGGING_FACE_HUB_TOKEN")
            or os.getenv("HUGGINGFACE_TOKEN")
            or os.getenv("HF_HUB_TOKEN")
            or os.getenv("HUGGINGFACE_API_TOKEN")
        ).strip()
    return token or None


def _hf_llm_snapshot_max_workers() -> int:
    try:
        value = int(str(os.getenv("TATER_HF_TRANSFORMERS_DOWNLOAD_WORKERS") or "4").strip())
    except Exception:
        value = 4
    return max(1, min(16, value))


def _llama_cpp_n_ctx(*, vision: bool = False) -> int:
    if vision:
        value = _local_llm_context_tokens(
            HYDRA_LLAMA_CPP_VISION_CONTEXT_TOKENS_KEY,
            ("TATER_LLAMA_CPP_VISION_N_CTX", "TATER_LLAMA_CPP_VISION_CONTEXT_TOKENS"),
            DEFAULT_LLAMA_CPP_VISION_CONTEXT_TOKENS,
        )
        return int(value or DEFAULT_LLAMA_CPP_VISION_CONTEXT_TOKENS)
    value = _local_llm_context_tokens(
        HYDRA_LLAMA_CPP_CONTEXT_TOKENS_KEY,
        ("TATER_LLAMA_CPP_N_CTX", "LLM_CONTEXT_SIZE"),
        DEFAULT_LLAMA_CPP_CONTEXT_TOKENS,
    )
    return int(value or DEFAULT_LLAMA_CPP_CONTEXT_TOKENS)


def _llama_cpp_n_batch(*, vision: bool = False) -> int:
    if vision:
        raw = str(os.getenv("TATER_LLAMA_CPP_VISION_N_BATCH") or "128").strip()
        default = 128
    else:
        raw = str(_safe_redis_text_get(HYDRA_LLAMA_CPP_N_BATCH_KEY) or os.getenv("TATER_LLAMA_CPP_N_BATCH") or DEFAULT_LLAMA_CPP_N_BATCH).strip()
        default = DEFAULT_LLAMA_CPP_N_BATCH
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(16 if vision else 32, min(8192, value))


def _llama_cpp_n_ubatch(*, vision: bool = False) -> int:
    if vision:
        raw = str(os.getenv("TATER_LLAMA_CPP_VISION_N_UBATCH") or os.getenv("TATER_LLAMA_CPP_VISION_N_BATCH") or "128").strip()
        default = 128
    else:
        raw = str(_safe_redis_text_get(HYDRA_LLAMA_CPP_N_UBATCH_KEY) or os.getenv("TATER_LLAMA_CPP_N_UBATCH") or DEFAULT_LLAMA_CPP_N_UBATCH).strip()
        default = DEFAULT_LLAMA_CPP_N_UBATCH
    try:
        value = int(float(raw))
    except Exception:
        value = int(default)
    if value <= 0:
        return 0
    return max(16 if vision else 32, min(8192, value))


def _llama_cpp_n_threads() -> int:
    raw = str(os.getenv("TATER_LLAMA_CPP_N_THREADS") or "").strip()
    if raw:
        try:
            value = int(raw)
        except Exception:
            value = 0
    else:
        try:
            value = max(1, int((os.cpu_count() or 4) * 0.75))
        except Exception:
            value = 4
    return max(1, min(256, value))


def _llama_cpp_n_gpu_layers() -> int:
    raw = str(os.getenv("TATER_LLAMA_CPP_N_GPU_LAYERS") or "auto").strip().lower()
    if raw in {"", "auto", "all", "gpu"}:
        return -1
    if raw in {"none", "off", "false", "cpu"}:
        return 0
    try:
        return int(raw)
    except Exception:
        return -1


def _llama_cpp_flash_attn_enabled() -> bool:
    raw = _safe_redis_text_get(HYDRA_LLAMA_CPP_FLASH_ATTN_KEY)
    if raw:
        return _boolish(raw, default=DEFAULT_LLAMA_CPP_FLASH_ATTN)
    return _boolish(os.getenv("TATER_LLAMA_CPP_FLASH_ATTN"), default=DEFAULT_LLAMA_CPP_FLASH_ATTN)


def _llama_cpp_offload_kqv_enabled() -> bool:
    raw = _safe_redis_text_get(HYDRA_LLAMA_CPP_OFFLOAD_KQV_KEY)
    if raw:
        return _boolish(raw, default=DEFAULT_LLAMA_CPP_OFFLOAD_KQV)
    return _boolish(os.getenv("TATER_LLAMA_CPP_OFFLOAD_KQV"), default=DEFAULT_LLAMA_CPP_OFFLOAD_KQV)


def _llama_cpp_mtp_enabled() -> bool:
    raw = _safe_redis_text_get(HYDRA_LLAMA_CPP_MTP_ENABLED_KEY)
    if raw:
        return _boolish(raw, default=DEFAULT_LLAMA_CPP_MTP_ENABLED)
    return _boolish(os.getenv("TATER_LLAMA_CPP_MTP_ENABLED"), default=DEFAULT_LLAMA_CPP_MTP_ENABLED)


def _llama_cpp_mtp_draft_tokens() -> int:
    raw = _safe_redis_text_get(HYDRA_LLAMA_CPP_MTP_DRAFT_TOKENS_KEY)
    if not raw:
        raw = str(os.getenv("TATER_LLAMA_CPP_MTP_DRAFT_TOKENS") or DEFAULT_LLAMA_CPP_MTP_DRAFT_TOKENS).strip()
    try:
        value = int(float(raw))
    except Exception:
        value = DEFAULT_LLAMA_CPP_MTP_DRAFT_TOKENS
    return max(1, min(16, int(value)))


def _llama_cpp_mtp_spec_type() -> str:
    token = str(os.getenv("TATER_LLAMA_CPP_MTP_SPEC_TYPE") or "draft-mtp").strip().lower()
    return token or "draft-mtp"


def _llama_cpp_mtp_load_arg_support(Llama: Any) -> Dict[str, Any]:
    try:
        params = inspect.signature(Llama.__init__).parameters
    except Exception:
        params = {}
    explicit = set(params.keys())
    candidates = (
        ("spec_type", "spec_draft_n_max"),
        ("spec_type", "draft_n_max"),
        ("speculative_type", "speculative_draft_n_max"),
        ("speculative_type", "draft_n_max"),
    )
    for type_key, draft_key in candidates:
        if type_key in explicit and draft_key in explicit:
            return {"supported": True, "type_key": type_key, "draft_key": draft_key}
    return {
        "supported": False,
        "type_key": "spec_type",
        "draft_key": "spec_draft_n_max",
        "has_var_kwargs": any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()),
    }


def _llama_cpp_mtp_load_kwargs(Llama: Any, *, enabled: bool, spec_type: str, draft_tokens: int) -> Tuple[Dict[str, Any], str]:
    if not enabled:
        return {}, ""
    support = _llama_cpp_mtp_load_arg_support(Llama)
    if support.get("supported"):
        return {
            str(support["type_key"]): str(spec_type or "draft-mtp"),
            str(support["draft_key"]): int(draft_tokens),
        }, ""
    if _boolish(os.getenv("TATER_LLAMA_CPP_MTP_FORCE_KWARGS"), default=False):
        return {
            "spec_type": str(spec_type or "draft-mtp"),
            "spec_draft_n_max": int(draft_tokens),
        }, ""
    return {}, (
        "MTP is enabled in settings, but this llama-cpp-python build does not expose native "
        "draft-mtp load arguments yet. Running llama.cpp without MTP."
    )


def _llama_cpp_warning_text(*parts: Any) -> str:
    rows: List[str] = []
    seen: set[str] = set()
    for part in parts:
        text = str(part or "").strip()
        if text and text not in seen:
            seen.add(text)
            rows.append(text)
    return "; ".join(rows)


def _llama_cpp_context_load_error_message(
    exc: BaseException,
    *,
    model_id: str,
    n_ctx: Any,
    n_gpu_layers: Any,
    n_batch: Any,
    vision: bool = False,
    mmproj_path: str = "",
) -> str:
    raw = str(exc or "").strip() or exc.__class__.__name__
    lowered = raw.lower()
    if "llama_context" not in lowered and "context" not in lowered:
        return raw

    model_label = str(model_id or "").strip() or "the selected GGUF model"
    message = (
        f"{raw}. llama.cpp could not create a runtime context for {model_label}. "
        f"Current load settings: n_ctx={n_ctx}, n_gpu_layers={n_gpu_layers}, n_batch={n_batch}. "
        "This usually means the requested context length or GPU/RAM use is too high for the selected "
        "model/quantization/build. "
    )
    if vision:
        message = (
            f"{message}Lower the llama.cpp Vision Context Length in Settings first "
            "(try 4096 or 8192), then try the image request again."
        )
        message = (
            f"{message} If this happens during boot or while other GPU models are loading, "
            "lower TATER_LLAMA_CPP_VISION_N_BATCH first (try 128, 64, or 32)."
        )
        projector_note = (
            f" Vision projector: {os.path.basename(str(mmproj_path or '').strip())}."
            if str(mmproj_path or "").strip()
            else " No matching mmproj vision projector was found for this GGUF."
        )
        message = (
            f"{message}{projector_note} llama.cpp vision GGUF models need both the model GGUF "
            "and a matching mmproj projector; otherwise use a dedicated vision model/provider."
        )
    else:
        message = (
            f"{message}Lower the llama.cpp Context Length in Settings first "
            "(try 4096, 8192, or 16384), then save and load the model again."
        )
    return message


def _llama_cpp_patch_partial_model_destructor() -> None:
    global _LLAMA_CPP_PARTIAL_MODEL_DEL_PATCHED
    if _LLAMA_CPP_PARTIAL_MODEL_DEL_PATCHED:
        return
    try:
        import llama_cpp._internals as internals  # type: ignore
    except Exception:
        return
    model_cls = getattr(internals, "LlamaModel", None)
    if model_cls is None:
        return
    if bool(getattr(model_cls, "_tater_partial_model_del_patch", False)):
        _LLAMA_CPP_PARTIAL_MODEL_DEL_PATCHED = True
        return
    original_del = getattr(model_cls, "__del__", None)
    if not callable(original_del):
        return

    def _tater_safe_llama_model_del(self):
        try:
            if not hasattr(self, "sampler"):
                return None
            return original_del(self)
        except AttributeError as exc:
            if "sampler" in str(exc):
                return None
            raise

    try:
        setattr(model_cls, "_tater_original_del", original_del)
        setattr(model_cls, "__del__", _tater_safe_llama_model_del)
        setattr(model_cls, "_tater_partial_model_del_patch", True)
        _LLAMA_CPP_PARTIAL_MODEL_DEL_PATCHED = True
    except Exception:
        return


def _safe_path_size_bytes(path: Any, *, file_limit: int = 20000) -> int:
    raw = str(path or "").strip()
    if not raw:
        return 0
    try:
        target = os.path.abspath(os.path.expanduser(raw))
    except Exception:
        target = raw
    try:
        if os.path.isfile(target):
            return max(0, int(os.path.getsize(target)))
        if not os.path.isdir(target):
            return 0
        total = 0
        count = 0
        for root, _dirs, files in os.walk(target):
            for filename in files:
                count += 1
                if count > int(file_limit):
                    return total
                try:
                    total += max(0, int(os.path.getsize(os.path.join(root, filename))))
                except Exception:
                    continue
        return total
    except Exception:
        return 0


def _model_memory_footprint_bytes(model: Any) -> int:
    if model is None:
        return 0
    try:
        getter = getattr(model, "get_memory_footprint", None)
        if callable(getter):
            value = getter()
            return max(0, int(value or 0))
    except Exception:
        pass
    total = 0
    for attr in ("parameters", "buffers"):
        try:
            getter = getattr(model, attr, None)
            if not callable(getter):
                continue
            for item in getter():
                try:
                    total += int(item.nelement()) * int(item.element_size())
                except Exception:
                    continue
        except Exception:
            continue
    return max(0, int(total))


def runtime_object_memory_footprint_bytes(model: Any) -> int:
    return _model_memory_footprint_bytes(model)


def runtime_path_size_bytes(path: Any, *, file_limit: int = 20000) -> int:
    return _safe_path_size_bytes(path, file_limit=file_limit)


def _local_llm_cache_token(provider: str, cache_key: Tuple[Any, ...]) -> str:
    try:
        payload = json.dumps(list(cache_key), separators=(",", ":"), ensure_ascii=False)
        encoded = base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")
        return f"{_normalize_hydra_llm_provider(provider)}:{encoded}"
    except Exception:
        return ""


def _decode_local_llm_cache_token(value: Any) -> Tuple[str, Tuple[Any, ...]]:
    token = str(value or "").strip()
    if not token or ":" not in token:
        return "", tuple()
    provider, encoded = token.split(":", 1)
    provider_token = _normalize_hydra_llm_provider(provider)
    try:
        padding = "=" * ((4 - (len(encoded) % 4)) % 4)
        raw = base64.urlsafe_b64decode(f"{encoded}{padding}".encode("ascii")).decode("utf-8", "replace")
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return provider_token, tuple(parsed)
    except Exception:
        pass
    return provider_token, tuple()


def _system_ram_snapshot() -> Dict[str, Any]:
    try:
        import psutil  # type: ignore

        mem = psutil.virtual_memory()
        total = max(0, int(getattr(mem, "total", 0) or 0))
        available = max(0, int(getattr(mem, "available", 0) or 0))
        used = max(0, int(getattr(mem, "used", 0) or max(0, total - available)))
        return {
            "available": True,
            "total_bytes": total,
            "available_bytes": available,
            "used_bytes": used,
            "percent": round((used / total) * 100.0, 2) if total > 0 else 0.0,
        }
    except Exception:
        pass
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        pages = int(os.sysconf("SC_PHYS_PAGES"))
        total = max(0, page_size * pages)
        return {
            "available": total > 0,
            "total_bytes": total,
            "available_bytes": 0,
            "used_bytes": 0,
            "percent": 0.0,
        }
    except Exception:
        return {
            "available": False,
            "total_bytes": 0,
            "available_bytes": 0,
            "used_bytes": 0,
            "percent": 0.0,
        }


def _system_cpu_snapshot() -> Dict[str, Any]:
    try:
        import psutil  # type: ignore

        percent = float(psutil.cpu_percent(interval=None))
        load_avg: List[float] = []
        try:
            load_avg = [round(float(item), 2) for item in os.getloadavg()]
        except Exception:
            load_avg = []
        return {
            "available": True,
            "percent": round(max(0.0, min(100.0, percent)), 2),
            "logical_count": int(psutil.cpu_count(logical=True) or 0),
            "physical_count": int(psutil.cpu_count(logical=False) or 0),
            "load_average": load_avg,
        }
    except Exception:
        pass
    try:
        load_avg = [round(float(item), 2) for item in os.getloadavg()]
    except Exception:
        load_avg = []
    return {
        "available": False,
        "percent": 0.0,
        "logical_count": int(os.cpu_count() or 0),
        "physical_count": 0,
        "load_average": load_avg,
    }


def _system_gpu_float(value: Any) -> Optional[float]:
    token = str(value or "").strip()
    if not token or token.lower() in {"n/a", "[not supported]", "not supported", "none"}:
        return None
    token = token.replace("%", "").replace("MiB", "").replace("W", "").strip()
    try:
        parsed = float(token)
    except Exception:
        return None
    return parsed if parsed >= 0 else None


def _system_gpu_mib_to_bytes(value: Any) -> int:
    parsed = _system_gpu_float(value)
    if parsed is None:
        return 0
    return max(0, int(parsed * 1024 * 1024))


def _system_gpu_bytes(value: Any) -> int:
    token = str(value or "").strip().replace(",", "")
    if not token or token.lower() in {"n/a", "[not supported]", "not supported", "none"}:
        return 0
    match = re.search(r"[-+]?\d+(?:\.\d+)?", token)
    if not match:
        return 0
    try:
        parsed = float(match.group(0))
    except Exception:
        return 0
    if parsed <= 0:
        return 0
    lower = token.lower()
    if "gib" in lower or re.search(r"\bgb\b", lower):
        return max(0, int(parsed * 1024 * 1024 * 1024))
    if "mib" in lower or re.search(r"\bmb\b", lower):
        return max(0, int(parsed * 1024 * 1024))
    if "kib" in lower or re.search(r"\bkb\b", lower):
        return max(0, int(parsed * 1024))
    return max(0, int(parsed))


def _system_gpu_clamped_percent(value: Any) -> Optional[float]:
    parsed = _system_gpu_float(value)
    if parsed is None:
        return None
    return round(max(0.0, min(100.0, parsed)), 2)


def _system_gpu_json_payload(text: Any) -> Optional[Any]:
    raw = str(text or "").strip()
    if not raw:
        return None
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]
    try:
        return json.loads(raw)
    except Exception:
        return None


def _system_gpu_row_value(
    row: Dict[str, Any],
    options: List[Tuple[str, ...]],
    *,
    excluded_fragments: Tuple[str, ...] = tuple(),
) -> Any:
    normalized_items = [(re.sub(r"[^a-z0-9%]+", " ", str(key or "").lower()).strip(), value) for key, value in row.items()]
    excluded = [re.sub(r"[^a-z0-9%]+", " ", str(fragment or "").lower()).strip() for fragment in excluded_fragments]
    excluded = [fragment for fragment in excluded if fragment]
    for fragments in options:
        wanted = [re.sub(r"[^a-z0-9%]+", " ", str(fragment or "").lower()).strip() for fragment in fragments]
        wanted = [fragment for fragment in wanted if fragment]
        if not wanted:
            continue
        for key, value in normalized_items:
            if any(fragment in key for fragment in excluded):
                continue
            if all(fragment in key for fragment in wanted):
                return value
    return None


def _nvidia_smi_vram_snapshot() -> Optional[Dict[str, Any]]:
    exe = shutil.which("nvidia-smi")
    if not exe:
        return None
    query = (
        "index,name,utilization.gpu,memory.total,memory.used,memory.free,"
        "temperature.gpu,power.draw,power.limit"
    )
    try:
        proc = subprocess.run(
            [exe, f"--query-gpu={query}", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=1.2,
            check=False,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None

    devices: List[Dict[str, Any]] = []
    total = 0
    used = 0
    free = 0
    utilization_values: List[float] = []
    for line in str(proc.stdout or "").splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 9:
            continue
        index_raw, name, util_raw, total_raw, used_raw, free_raw, temp_raw, power_raw, power_limit_raw = parts[:9]
        device_total = _system_gpu_mib_to_bytes(total_raw)
        device_used = _system_gpu_mib_to_bytes(used_raw)
        device_free = _system_gpu_mib_to_bytes(free_raw)
        utilization = _system_gpu_float(util_raw)
        temperature = _system_gpu_float(temp_raw)
        power_draw = _system_gpu_float(power_raw)
        power_limit = _system_gpu_float(power_limit_raw)
        if utilization is not None:
            utilization_values.append(max(0.0, min(100.0, utilization)))
        total += device_total
        used += device_used
        free += device_free
        try:
            index = int(float(index_raw))
        except Exception:
            index = len(devices)
        devices.append(
            {
                "index": index,
                "name": name or f"cuda:{index}",
                "total_bytes": device_total,
                "free_bytes": device_free,
                "used_bytes": device_used,
                "percent": round((device_used / device_total) * 100.0, 2) if device_total > 0 else 0.0,
                "utilization_percent": round(max(0.0, min(100.0, utilization)), 2) if utilization is not None else None,
                "temperature_c": round(temperature, 1) if temperature is not None else None,
                "power_draw_w": round(power_draw, 1) if power_draw is not None else None,
                "power_limit_w": round(power_limit, 1) if power_limit is not None else None,
            }
        )

    if not devices:
        return None
    utilization_percent: Optional[float] = None
    if utilization_values:
        utilization_percent = round(sum(utilization_values) / len(utilization_values), 2)
    return {
        "available": total > 0,
        "backend": "nvidia",
        "total_bytes": total,
        "free_bytes": free,
        "used_bytes": used,
        "percent": round((used / total) * 100.0, 2) if total > 0 else 0.0,
        "utilization_percent": utilization_percent,
        "devices": devices,
    }


def _rocm_smi_vram_snapshot() -> Optional[Dict[str, Any]]:
    exe = shutil.which("rocm-smi") or shutil.which("rocm-smi.py")
    if not exe:
        return None
    commands = [
        [exe, "--showproductname", "--showuse", "--showmemuse", "--showmeminfo", "vram", "--showtemp", "--showpower", "--json"],
        [exe, "--showuse", "--showmemuse", "--showmeminfo", "vram", "--showtemp", "--showpower", "--json"],
        [exe, "--json"],
    ]
    payload: Optional[Any] = None
    for command in commands:
        try:
            proc = subprocess.run(command, capture_output=True, text=True, timeout=1.8, check=False)
        except Exception:
            continue
        if proc.returncode != 0:
            continue
        payload = _system_gpu_json_payload(proc.stdout)
        if payload is not None:
            break
    if not isinstance(payload, dict):
        return None

    devices: List[Dict[str, Any]] = []
    total = 0
    used = 0
    free = 0
    utilization_values: List[float] = []
    for key, raw_row in payload.items():
        if not isinstance(raw_row, dict):
            continue
        row = dict(raw_row)
        key_text = str(key or "")
        if not key_text.lower().startswith(("card", "gpu")) and not any("vram" in str(k).lower() or "gpu use" in str(k).lower() for k in row.keys()):
            continue
        try:
            index = int(float(re.sub(r"[^0-9.]+", "", key_text) or len(devices)))
        except Exception:
            index = len(devices)
        name = str(
            _system_gpu_row_value(
                row,
                [
                    ("card", "series"),
                    ("card", "model"),
                    ("product", "name"),
                    ("gpu", "name"),
                    ("device", "name"),
                ],
            )
            or f"ROCm GPU {index}"
        ).strip()
        utilization = _system_gpu_clamped_percent(
            _system_gpu_row_value(
                row,
                [
                    ("gpu", "use"),
                    ("gpu", "busy"),
                    ("activity",),
                    ("use", "%"),
                ],
            )
        )
        device_total = _system_gpu_bytes(
            _system_gpu_row_value(
                row,
                [
                    ("vram", "total", "memory"),
                    ("vram", "total"),
                    ("memory", "total", "vram"),
                ],
                excluded_fragments=("used", "free", "allocated", "%"),
            )
        )
        device_used = _system_gpu_bytes(
            _system_gpu_row_value(
                row,
                [
                    ("vram", "used", "memory"),
                    ("vram", "used"),
                    ("memory", "used", "vram"),
                ],
                excluded_fragments=("%",),
            )
        )
        device_free = _system_gpu_bytes(
            _system_gpu_row_value(
                row,
                [
                    ("vram", "free", "memory"),
                    ("vram", "free"),
                    ("memory", "free", "vram"),
                ],
            )
        )
        memory_percent = _system_gpu_clamped_percent(
            _system_gpu_row_value(
                row,
                [
                    ("vram%",),
                    ("memory", "allocated", "vram"),
                    ("memory", "use"),
                ],
            )
        )
        if device_used <= 0 and device_total > 0 and memory_percent is not None:
            device_used = max(0, int(device_total * (memory_percent / 100.0)))
        if device_free <= 0 and device_total > 0:
            device_free = max(0, device_total - device_used)
        temperature = _system_gpu_float(
            _system_gpu_row_value(row, [("temperature", "edge"), ("temperature", "junction"), ("temperature",), ("temp",)])
        )
        power_draw = _system_gpu_float(
            _system_gpu_row_value(row, [("average", "power"), ("power", "draw"), ("power", "w")])
        )
        if utilization is not None:
            utilization_values.append(utilization)
        total += device_total
        used += device_used
        free += device_free
        devices.append(
            {
                "index": index,
                "name": name or f"ROCm GPU {index}",
                "total_bytes": device_total,
                "free_bytes": device_free,
                "used_bytes": device_used,
                "percent": round((device_used / device_total) * 100.0, 2) if device_total > 0 else 0.0,
                "utilization_percent": utilization,
                "temperature_c": round(temperature, 1) if temperature is not None else None,
                "power_draw_w": round(power_draw, 1) if power_draw is not None else None,
            }
        )

    if not devices:
        return None
    utilization_percent: Optional[float] = None
    if utilization_values:
        utilization_percent = round(sum(utilization_values) / len(utilization_values), 2)
    return {
        "available": True,
        "backend": "rocm",
        "total_bytes": total,
        "free_bytes": free,
        "used_bytes": used,
        "percent": round((used / total) * 100.0, 2) if total > 0 else 0.0,
        "utilization_percent": utilization_percent,
        "devices": devices,
    }


def _jetson_tegrastats_snapshot() -> Optional[Dict[str, Any]]:
    exe = shutil.which("tegrastats")
    if not exe:
        return None
    output = ""
    commands = ([exe, "--interval", "200", "--count", "1"], [exe, "--interval", "200"])
    for command in commands:
        try:
            proc = subprocess.run(command, capture_output=True, text=True, timeout=1.2, check=False)
            output = str(proc.stdout or proc.stderr or "").strip()
        except subprocess.TimeoutExpired as exc:
            raw_output = exc.stdout or exc.stderr or ""
            output = raw_output.decode("utf-8", "replace") if isinstance(raw_output, bytes) else str(raw_output or "")
            output = output.strip()
        except Exception:
            output = ""
        if output:
            break
    if not output:
        return None
    line = output.splitlines()[0]
    util_match = re.search(r"\bGR3D_FREQ\s+(\d+(?:\.\d+)?)%", line)
    utilization = _system_gpu_clamped_percent(util_match.group(1) if util_match else None)
    ram_match = re.search(r"\bRAM\s+(\d+(?:\.\d+)?)/(\d+(?:\.\d+)?)MB\b", line, flags=re.IGNORECASE)
    shared_used = _system_gpu_mib_to_bytes(ram_match.group(1)) if ram_match else 0
    shared_total = _system_gpu_mib_to_bytes(ram_match.group(2)) if ram_match else 0
    temp_match = re.search(r"\bGPU@(\d+(?:\.\d+)?)C\b", line)
    temperature = _system_gpu_float(temp_match.group(1) if temp_match else None)
    device = {
        "index": 0,
        "name": "Jetson GPU",
        "total_bytes": 0,
        "free_bytes": 0,
        "used_bytes": 0,
        "percent": 0.0,
        "utilization_percent": utilization,
        "temperature_c": round(temperature, 1) if temperature is not None else None,
        "shared_memory_total_bytes": shared_total,
        "shared_memory_used_bytes": shared_used,
        "unified": True,
    }
    if utilization is None and shared_total <= 0:
        return None
    return {
        "available": True,
        "backend": "jetson",
        "total_bytes": 0,
        "free_bytes": 0,
        "used_bytes": 0,
        "percent": 0.0,
        "utilization_percent": utilization,
        "unified": True,
        "devices": [device],
    }


def _apple_ioreg_float(text: str, key: str) -> Optional[float]:
    match = re.search(rf'"{re.escape(key)}"\s*=\s*([-+]?\d+(?:\.\d+)?)', text)
    if not match:
        return None
    return _system_gpu_float(match.group(1))


def _apple_ioreg_int(text: str, key: str) -> int:
    parsed = _apple_ioreg_float(text, key)
    if parsed is None:
        return 0
    return max(0, int(parsed))


def _apple_ioreg_metal_snapshot() -> Optional[Dict[str, Any]]:
    exe = shutil.which("ioreg")
    if not exe:
        return None
    output = ""
    for class_name in ("IOAccelerator", "AGXAccelerator"):
        try:
            proc = subprocess.run(
                [exe, "-r", "-d", "1", "-w", "0", "-c", class_name],
                capture_output=True,
                text=True,
                timeout=1.0,
                check=False,
            )
        except Exception:
            continue
        if proc.returncode == 0 and str(proc.stdout or "").strip():
            output = str(proc.stdout or "")
            break
    if not output or "PerformanceStatistics" not in output:
        return None

    perf_match = re.search(r'"PerformanceStatistics"\s*=\s*\{([^}]*)\}', output, flags=re.DOTALL)
    perf_text = perf_match.group(1) if perf_match else output
    utilization_values = [
        value
        for value in (
            _apple_ioreg_float(perf_text, "Device Utilization %"),
            _apple_ioreg_float(perf_text, "Renderer Utilization %"),
            _apple_ioreg_float(perf_text, "Tiler Utilization %"),
        )
        if value is not None
    ]
    utilization = max(utilization_values) if utilization_values else None

    used_candidates = [
        _apple_ioreg_int(perf_text, "In use system memory"),
        _apple_ioreg_int(perf_text, "In use system memory (driver)"),
        _apple_ioreg_int(perf_text, "In use video memory"),
        _apple_ioreg_int(perf_text, "In use vid memory"),
        _apple_ioreg_int(perf_text, "vramUsedBytes"),
    ]
    allocated = _apple_ioreg_int(perf_text, "Alloc system memory")
    used = max([value for value in used_candidates if value > 0] or [0])
    if used <= 0 and allocated > 0:
        used = allocated

    model_match = re.search(r'"model"\s*=\s*"([^"]+)"', output)
    model = str(model_match.group(1) if model_match else "").strip()
    if model and "gpu" not in model.lower():
        model = f"{model} GPU"
    core_count = _apple_ioreg_int(output, "gpu-core-count")

    return {
        "name": model or "Apple Metal GPU",
        "used_bytes": used,
        "allocated_bytes": allocated,
        "utilization_percent": round(max(0.0, min(100.0, utilization)), 2) if utilization is not None else None,
        "core_count": core_count,
    }


def _apple_metal_vram_snapshot() -> Optional[Dict[str, Any]]:
    if platform.system().lower() != "darwin":
        return None
    apple_silicon = platform.machine().lower() in {"arm64", "aarch64"}
    used_candidates: List[int] = []
    total_candidates: List[int] = []
    detail_parts: List[str] = []
    ioreg_snapshot = _apple_ioreg_metal_snapshot()
    if ioreg_snapshot:
        detail_parts.append("IORegistry")
        used_candidates.append(max(0, int(ioreg_snapshot.get("used_bytes") or 0)))
    if not ioreg_snapshot:
        try:
            import torch  # type: ignore

            mps_backend = getattr(getattr(torch, "backends", None), "mps", None)
            mps_runtime = getattr(torch, "mps", None)
            if mps_backend is not None and bool(getattr(mps_backend, "is_available", lambda: False)()):
                detail_parts.append("MPS")
                for attr in ("current_allocated_memory", "driver_allocated_memory"):
                    getter = getattr(mps_runtime, attr, None) if mps_runtime is not None else None
                    if callable(getter):
                        with contextlib.suppress(Exception):
                            used_candidates.append(max(0, int(getter() or 0)))
                getter = getattr(mps_runtime, "recommended_max_memory", None) if mps_runtime is not None else None
                if callable(getter):
                    with contextlib.suppress(Exception):
                        total_candidates.append(max(0, int(getter() or 0)))
        except Exception:
            pass
        try:
            import mlx.core as mx  # type: ignore

            detail_parts.append("MLX")
            for attr in ("get_active_memory", "get_cache_memory"):
                getter = getattr(mx, attr, None)
                if callable(getter):
                    with contextlib.suppress(Exception):
                        used_candidates.append(max(0, int(getter() or 0)))
            getter = getattr(mx, "get_peak_memory", None)
            if callable(getter):
                with contextlib.suppress(Exception):
                    total_candidates.append(max(0, int(getter() or 0)))
        except Exception:
            pass
    ram_snapshot = _system_ram_snapshot()
    unified_total = max(0, int(ram_snapshot.get("total_bytes") or 0))
    unified_memory = bool(apple_silicon)
    if unified_total > 0 and unified_memory:
        total_candidates.append(unified_total)
    if not detail_parts and not apple_silicon:
        return None
    used = max(used_candidates) if used_candidates else 0
    total = max(total_candidates) if total_candidates else 0
    if total > 0:
        used = min(used, total)
    free = max(0, total - used) if total > 0 else 0
    utilization_percent = None
    if ioreg_snapshot:
        utilization_percent = ioreg_snapshot.get("utilization_percent")
    utilization_percent = _system_gpu_clamped_percent(utilization_percent)
    device_name = str((ioreg_snapshot or {}).get("name") or "Apple Metal GPU").strip() or "Apple Metal GPU"
    core_count = max(0, int((ioreg_snapshot or {}).get("core_count") or 0))
    device_detail = " / ".join(dict.fromkeys(detail_parts)) if detail_parts else "Unified memory"
    return {
        "available": True,
        "backend": "metal",
        "total_bytes": total,
        "free_bytes": free,
        "used_bytes": used,
        "percent": round((used / total) * 100.0, 2) if total > 0 else 0.0,
        "utilization_percent": utilization_percent,
        "unified": unified_memory,
        "devices": [
            {
                "index": 0,
                "name": device_name,
                "total_bytes": total,
                "free_bytes": free,
                "used_bytes": used,
                "percent": round((used / total) * 100.0, 2) if total > 0 else 0.0,
                "utilization_percent": utilization_percent,
                "unified": unified_memory,
                "shared_memory_total_bytes": total if unified_memory else 0,
                "shared_memory_used_bytes": used if unified_memory else 0,
                "core_count": core_count,
                "detail": device_detail,
            }
        ],
    }


def _torch_cuda_vram_snapshot() -> Optional[Dict[str, Any]]:
    try:
        import torch  # type: ignore

        cuda = getattr(torch, "cuda", None)
        if cuda is None or not bool(cuda.is_available()):
            return None
        total = 0
        free = 0
        devices: List[Dict[str, Any]] = []
        for index in range(int(cuda.device_count())):
            props = cuda.get_device_properties(index)
            device_total = max(0, int(getattr(props, "total_memory", 0) or 0))
            device_free = 0
            try:
                free_pair = cuda.mem_get_info(index)
                device_free = max(0, int(free_pair[0] or 0))
                if free_pair and len(free_pair) > 1:
                    device_total = max(device_total, int(free_pair[1] or 0))
            except Exception:
                pass
            device_used = max(0, device_total - device_free) if device_free else 0
            total += device_total
            free += device_free
            devices.append(
                {
                    "index": index,
                    "name": str(getattr(props, "name", "") or f"cuda:{index}"),
                    "total_bytes": device_total,
                    "free_bytes": device_free,
                    "used_bytes": device_used,
                    "percent": round((device_used / device_total) * 100.0, 2) if device_total > 0 and device_used > 0 else 0.0,
                    "utilization_percent": None,
                }
            )
        used = max(0, total - free) if free else 0
        backend = "cuda"
        try:
            if str(getattr(getattr(torch, "version", None), "hip", "") or "").strip():
                backend = "rocm"
        except Exception:
            pass
        return {
            "available": total > 0,
            "backend": backend,
            "total_bytes": total,
            "free_bytes": free,
            "used_bytes": used,
            "percent": round((used / total) * 100.0, 2) if total > 0 and used > 0 else 0.0,
            "utilization_percent": None,
            "devices": devices,
        }
    except Exception:
        return None


def _merge_gpu_usage_snapshot(memory_snapshot: Dict[str, Any], usage_snapshot: Dict[str, Any], *, backend: str) -> Dict[str, Any]:
    merged = dict(memory_snapshot or {})
    merged["backend"] = backend
    usage_percent = _system_gpu_clamped_percent((usage_snapshot or {}).get("utilization_percent"))
    if usage_percent is not None:
        merged["utilization_percent"] = usage_percent
    usage_devices = [row for row in list((usage_snapshot or {}).get("devices") or []) if isinstance(row, dict)]
    memory_devices = [dict(row) for row in list(merged.get("devices") or []) if isinstance(row, dict)]
    if usage_devices and memory_devices:
        for index, row in enumerate(memory_devices):
            usage_row = usage_devices[min(index, len(usage_devices) - 1)]
            device_usage = _system_gpu_clamped_percent(usage_row.get("utilization_percent"))
            if device_usage is not None:
                row["utilization_percent"] = device_usage
            for key in ("temperature_c", "power_draw_w", "power_limit_w", "shared_memory_total_bytes", "shared_memory_used_bytes", "unified"):
                if usage_row.get(key) is not None:
                    row[key] = usage_row.get(key)
        merged["devices"] = memory_devices
    return merged


def _system_vram_snapshot() -> Dict[str, Any]:
    nvidia_snapshot = _nvidia_smi_vram_snapshot()
    if nvidia_snapshot:
        return nvidia_snapshot

    rocm_snapshot = _rocm_smi_vram_snapshot()
    if rocm_snapshot:
        return rocm_snapshot

    jetson_snapshot = _jetson_tegrastats_snapshot()
    if jetson_snapshot:
        torch_snapshot = _torch_cuda_vram_snapshot()
        if torch_snapshot and int(torch_snapshot.get("total_bytes") or 0) > 0:
            return _merge_gpu_usage_snapshot(torch_snapshot, jetson_snapshot, backend="jetson-cuda")
        return jetson_snapshot

    apple_snapshot = _apple_metal_vram_snapshot()
    if apple_snapshot:
        return apple_snapshot

    torch_snapshot = _torch_cuda_vram_snapshot()
    if torch_snapshot:
        return torch_snapshot

    return {
        "available": False,
        "backend": "",
        "total_bytes": 0,
        "free_bytes": 0,
        "used_bytes": 0,
        "percent": 0.0,
        "utilization_percent": None,
        "devices": [],
    }


def _local_llm_memory_kind(provider: str, bundle: Dict[str, Any]) -> str:
    provider_token = _normalize_hydra_llm_provider(provider)
    device = str((bundle or {}).get("device") or "").strip().lower()
    if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP:
        if str((bundle or {}).get("gpu_backend") or "").strip() and int((bundle or {}).get("n_gpu_layers") or 0) != 0:
            return "vram"
        return "ram"
    if "cuda" in device or device == "gpu":
        return "vram"
    if "mps" in device or "metal" in device or "apple" in device:
        return "unified"
    return "ram"


def _local_llm_provider_label(provider: str) -> str:
    provider_token = _normalize_hydra_llm_provider(provider)
    if provider_token == HYDRA_LLM_PROVIDER_HF_TRANSFORMERS:
        return "Transformers"
    if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP:
        return "llama.cpp"
    if provider_token == HYDRA_LLM_PROVIDER_MLX_LM:
        return "MLX LM"
    return "Local LLM"


def _local_llm_loaded_model_row(provider: str, cache_key: Tuple[Any, ...], bundle: Dict[str, Any]) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(provider)
    model = str(cache_key[0] if cache_key else "").strip()
    device = str((bundle or {}).get("device") or "").strip()
    memory_bytes = max(0, int((bundle or {}).get("memory_estimate_bytes") or 0))
    if memory_bytes <= 0:
        if provider_token == HYDRA_LLM_PROVIDER_HF_TRANSFORMERS:
            memory_bytes = _model_memory_footprint_bytes((bundle or {}).get("model"))
        else:
            memory_bytes = _safe_path_size_bytes((bundle or {}).get("model_path"))
    memory_kind = _local_llm_memory_kind(provider_token, bundle)
    return {
        "cache_key": _local_llm_cache_token(provider_token, cache_key),
        "provider": provider_token,
        "provider_label": _local_llm_provider_label(provider_token),
        "model": model,
        "device": device,
        "memory_kind": memory_kind,
        "estimated_bytes": memory_bytes,
        "model_path": str((bundle or {}).get("model_path") or ""),
        "model_root": str((bundle or {}).get("model_root") or ""),
        "loaded_ts": float((bundle or {}).get("loaded_ts") or 0.0),
        "gpu_backend": str((bundle or {}).get("gpu_backend") or ""),
        "warning": _llama_cpp_warning_text(
            (bundle or {}).get("gpu_warning"),
            (bundle or {}).get("mtp_warning"),
            (bundle or {}).get("chat_template_warning"),
        ),
        "chat_template_override": bool((bundle or {}).get("chat_template_override")),
        "chat_template_handler": str((bundle or {}).get("chat_template_handler") or ""),
        "supports_vision": bool((bundle or {}).get("supports_vision")),
        "mmproj_path": str((bundle or {}).get("mmproj_path") or (bundle or {}).get("vision_projector_path") or ""),
        "vision_chat_handler": str((bundle or {}).get("vision_chat_handler") or ""),
    }


def get_local_llm_loaded_models_snapshot(*, include_models: bool = True) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    cache_specs = (
        (HYDRA_LLM_PROVIDER_HF_TRANSFORMERS, _HF_LLM_MODEL_CACHE, _HF_LLM_MODEL_CACHE_LOCK),
        (HYDRA_LLM_PROVIDER_LLAMA_CPP, _LLAMA_CPP_MODEL_CACHE, _LLAMA_CPP_MODEL_CACHE_LOCK),
        (HYDRA_LLM_PROVIDER_MLX_LM, _MLX_ENGINE_MODEL_CACHE, _MLX_ENGINE_MODEL_CACHE_LOCK),
        (HYDRA_LLM_PROVIDER_MLX_LM, _MLX_LM_MODEL_CACHE, _MLX_LM_MODEL_CACHE_LOCK),
        (HYDRA_LLM_PROVIDER_MLX_LM, _MLX_VLM_MODEL_CACHE, _MLX_VLM_MODEL_CACHE_LOCK),
    )
    for provider, cache, lock in cache_specs:
        with lock:
            items = list(cache.items())
        for cache_key, bundle in items:
            if not isinstance(bundle, dict):
                continue
            try:
                rows.append(_local_llm_loaded_model_row(provider, tuple(cache_key), bundle))
            except Exception:
                logger.debug("[local-llm] failed to summarize loaded model provider=%s key=%s", provider, cache_key, exc_info=True)

    rows.sort(key=lambda row: (str(row.get("provider_label") or ""), str(row.get("model") or "")))
    totals = {
        "estimated_ram_bytes": 0,
        "estimated_vram_bytes": 0,
        "estimated_unified_bytes": 0,
        "estimated_total_bytes": 0,
    }
    by_provider: Dict[str, int] = {}
    for row in rows:
        provider = str(row.get("provider") or "").strip()
        by_provider[provider] = by_provider.get(provider, 0) + 1
        estimated = max(0, int(row.get("estimated_bytes") or 0))
        totals["estimated_total_bytes"] += estimated
        kind = str(row.get("memory_kind") or "ram").strip().lower()
        if kind == "vram":
            totals["estimated_vram_bytes"] += estimated
        elif kind == "unified":
            totals["estimated_unified_bytes"] += estimated
        else:
            totals["estimated_ram_bytes"] += estimated

    system = {
        "cpu": _system_cpu_snapshot(),
        "ram": _system_ram_snapshot(),
        "vram": _system_vram_snapshot(),
        "unified_memory": bool(_mlx_lm_is_apple_silicon()),
    }
    payload = {
        "loaded_count": len(rows),
        "by_provider": by_provider,
        "totals": totals,
        "system": system,
    }
    if include_models:
        payload["models"] = rows
    return payload


def _release_local_llm_runtime_memory() -> None:
    try:
        gc.collect()
    except Exception:
        pass
    try:
        import torch  # type: ignore

        cuda = getattr(torch, "cuda", None)
        if cuda is not None and bool(cuda.is_available()):
            try:
                cuda.empty_cache()
            except Exception:
                pass
        mps = getattr(torch, "mps", None)
        if mps is not None and hasattr(mps, "empty_cache"):
            try:
                mps.empty_cache()
            except Exception:
                pass
    except Exception:
        pass
    try:
        import mlx.core as mx  # type: ignore

        clear_cache = getattr(mx, "clear_cache", None)
        if callable(clear_cache):
            clear_cache()
    except Exception:
        pass


def unload_local_llm_models(
    *,
    provider: Any = "",
    model: Any = "",
    cache_key: Any = "",
    all_models: bool = False,
) -> Dict[str, Any]:
    requested_provider = _normalize_hydra_llm_provider(provider) if str(provider or "").strip() else ""
    requested_model = str(model or "").strip()
    token_provider, decoded_key = _decode_local_llm_cache_token(cache_key)
    if token_provider:
        requested_provider = token_provider

    removed: List[Dict[str, Any]] = []
    removed_bundles: List[Dict[str, Any]] = []
    cache_specs = (
        (HYDRA_LLM_PROVIDER_HF_TRANSFORMERS, _HF_LLM_MODEL_CACHE, _HF_LLM_GENERATION_LOCKS, _HF_LLM_MODEL_CACHE_LOCK),
        (HYDRA_LLM_PROVIDER_LLAMA_CPP, _LLAMA_CPP_MODEL_CACHE, _LLAMA_CPP_GENERATION_LOCKS, _LLAMA_CPP_MODEL_CACHE_LOCK),
        (HYDRA_LLM_PROVIDER_MLX_LM, _MLX_ENGINE_MODEL_CACHE, _MLX_ENGINE_GENERATION_LOCKS, _MLX_ENGINE_MODEL_CACHE_LOCK),
        (HYDRA_LLM_PROVIDER_MLX_LM, _MLX_LM_MODEL_CACHE, _MLX_LM_GENERATION_LOCKS, _MLX_LM_MODEL_CACHE_LOCK),
        (HYDRA_LLM_PROVIDER_MLX_LM, _MLX_VLM_MODEL_CACHE, _MLX_VLM_GENERATION_LOCKS, _MLX_VLM_MODEL_CACHE_LOCK),
    )

    for provider_token, cache, locks, lock in cache_specs:
        if requested_provider and provider_token != requested_provider:
            continue
        with lock:
            for raw_key, bundle in list(cache.items()):
                key = tuple(raw_key)
                key_model = str(key[0] if key else "").strip()
                if not all_models:
                    if decoded_key and key != decoded_key:
                        continue
                    if not decoded_key and requested_model and key_model != requested_model:
                        continue
                    if not decoded_key and not requested_model:
                        continue
                row = _local_llm_loaded_model_row(provider_token, key, bundle if isinstance(bundle, dict) else {})
                removed.append(row)
                if isinstance(bundle, dict):
                    shutdown = getattr(bundle.get("model_kit"), "shutdown", None)
                    if callable(shutdown):
                        try:
                            shutdown()
                        except Exception:
                            pass
                    removed_bundles.append(bundle)
                    bundle.clear()
                cache.pop(raw_key, None)
                locks.pop(raw_key, None)

    removed_bundles.clear()
    if removed:
        _release_local_llm_runtime_memory()
    return {
        "ok": True,
        "unloaded_count": len(removed),
        "models": removed,
        "loaded": get_local_llm_loaded_models_snapshot(include_models=True),
    }


def _llama_cpp_system_info(llama_cpp_module: Any = None) -> str:
    try:
        module = llama_cpp_module
        if module is None:
            import llama_cpp as imported_llama_cpp  # type: ignore

            module = imported_llama_cpp
        info_fn = getattr(module, "llama_print_system_info", None)
        if not callable(info_fn):
            return ""
        info = info_fn()
        if isinstance(info, bytes):
            return info.decode("utf-8", "replace").strip()
        return str(info or "").strip()
    except Exception:
        return ""


def _llama_cpp_gpu_backend(system_info: str) -> str:
    text = re.sub(r"\s+", " ", str(system_info or "").lower())
    if not text:
        return ""
    checks = (
        ("cuda", ("cuda", "cublas")),
        ("metal", ("metal", "mtl")),
        ("vulkan", ("vulkan",)),
        ("rocm", ("hip", "hipblas", "rocblas")),
        ("sycl", ("sycl",)),
        ("opencl", ("opencl", "clblast")),
    )
    for label, names in checks:
        for name in names:
            if re.search(rf"\b{re.escape(name)}\s*:", text):
                return label
            if re.search(rf"\bggml[_-]{re.escape(name)}\s*:", text):
                return label
            if re.search(rf"\b{re.escape(name)}\s*[=:]\s*(1|on|true|yes)\b", text):
                return label
            if re.search(rf"\bggml[_-]{re.escape(name)}\s*[=:]\s*(1|on|true|yes)\b", text):
                return label
    return ""


def get_llama_cpp_runtime_diagnostics() -> Dict[str, Any]:
    diagnostics: Dict[str, Any] = {
        "available": False,
        "n_gpu_layers": _llama_cpp_n_gpu_layers(),
        "n_ctx": _llama_cpp_n_ctx(),
        "vision_n_ctx": _llama_cpp_n_ctx(vision=True),
        "n_batch": _llama_cpp_n_batch(),
        "n_ubatch": _llama_cpp_n_ubatch(),
        "flash_attn": _llama_cpp_flash_attn_enabled(),
        "offload_kqv": _llama_cpp_offload_kqv_enabled(),
        "mtp_enabled": _llama_cpp_mtp_enabled(),
        "mtp_spec_type": _llama_cpp_mtp_spec_type(),
        "mtp_draft_tokens": _llama_cpp_mtp_draft_tokens(),
        "env": {
            "TATER_LLAMA_CPP_N_GPU_LAYERS": str(os.getenv("TATER_LLAMA_CPP_N_GPU_LAYERS") or ""),
            "TATER_LLAMA_CPP_N_BATCH": str(os.getenv("TATER_LLAMA_CPP_N_BATCH") or ""),
            "TATER_LLAMA_CPP_N_UBATCH": str(os.getenv("TATER_LLAMA_CPP_N_UBATCH") or ""),
            "TATER_LLAMA_CPP_FLASH_ATTN": str(os.getenv("TATER_LLAMA_CPP_FLASH_ATTN") or ""),
            "TATER_LLAMA_CPP_OFFLOAD_KQV": str(os.getenv("TATER_LLAMA_CPP_OFFLOAD_KQV") or ""),
            "TATER_LLAMA_CPP_VISION_N_CTX": str(os.getenv("TATER_LLAMA_CPP_VISION_N_CTX") or ""),
            "TATER_LLAMA_CPP_VISION_CONTEXT_TOKENS": str(os.getenv("TATER_LLAMA_CPP_VISION_CONTEXT_TOKENS") or ""),
            "TATER_LLAMA_CPP_MTP_ENABLED": str(os.getenv("TATER_LLAMA_CPP_MTP_ENABLED") or ""),
            "TATER_LLAMA_CPP_MTP_DRAFT_TOKENS": str(os.getenv("TATER_LLAMA_CPP_MTP_DRAFT_TOKENS") or ""),
            "TATER_LLAMA_CPP_MTP_SPEC_TYPE": str(os.getenv("TATER_LLAMA_CPP_MTP_SPEC_TYPE") or ""),
            "CUDA_VISIBLE_DEVICES": str(os.getenv("CUDA_VISIBLE_DEVICES") or ""),
            "NVIDIA_VISIBLE_DEVICES": str(os.getenv("NVIDIA_VISIBLE_DEVICES") or ""),
        },
        "ld_library_path": str(os.getenv("LD_LIBRARY_PATH") or ""),
    }
    try:
        try:
            from importlib import metadata

            diagnostics["installed_version"] = metadata.version("llama-cpp-python")
        except Exception:
            pass
        import llama_cpp as llama_cpp_module  # type: ignore
        from llama_cpp import Llama  # type: ignore

        system_info = _llama_cpp_system_info(llama_cpp_module)
        gpu_backend = _llama_cpp_gpu_backend(system_info)
        mtp_support = _llama_cpp_mtp_load_arg_support(Llama)
        mtp_warning = ""
        if diagnostics["mtp_enabled"] and not mtp_support.get("supported"):
            mtp_warning = (
                "MTP is enabled, but the installed llama-cpp-python high-level API does not expose "
                "native draft-mtp load arguments yet."
            )
        diagnostics.update(
            {
                "available": True,
                "module_path": str(getattr(llama_cpp_module, "__file__", "") or ""),
                "version": str(getattr(llama_cpp_module, "__version__", "") or ""),
                "system_info": system_info,
                "gpu_backend": gpu_backend,
                "gpu_enabled": bool(gpu_backend),
                "mtp_supported": bool(mtp_support.get("supported")),
                "mtp_load_args": {
                    "type_key": str(mtp_support.get("type_key") or ""),
                    "draft_key": str(mtp_support.get("draft_key") or ""),
                },
            }
        )
        if diagnostics["n_gpu_layers"] == 0:
            diagnostics["warning"] = "TATER_LLAMA_CPP_N_GPU_LAYERS is set to CPU-only."
        elif not gpu_backend:
            diagnostics["warning"] = "llama-cpp-python is installed, but it does not report a GPU backend."
        if mtp_warning:
            diagnostics["mtp_warning"] = mtp_warning
            diagnostics["warning"] = _llama_cpp_warning_text(diagnostics.get("warning"), mtp_warning)
    except Exception as exc:
        diagnostics["error"] = str(exc) or type(exc).__name__
        if "libcuda.so.1" in str(exc):
            diagnostics["warning"] = (
                "The CUDA llama.cpp build is installed, but libcuda.so.1 is not visible. "
                "Start the container with the NVIDIA runtime/GPU access so the host driver library is mounted."
            )

    try:
        import torch  # type: ignore

        diagnostics["torch_cuda_available"] = bool(getattr(torch, "cuda", None) and torch.cuda.is_available())
        diagnostics["torch_cuda_device_count"] = int(torch.cuda.device_count()) if getattr(torch, "cuda", None) else 0
    except Exception as exc:
        diagnostics["torch_error"] = str(exc) or type(exc).__name__
    return diagnostics


def _llama_cpp_disable_thinking_enabled() -> bool:
    return _boolish(os.getenv("TATER_LLAMA_CPP_DISABLE_THINKING"), default=True)


def _hf_transformers_disable_thinking_enabled() -> bool:
    return _boolish(os.getenv("TATER_HF_TRANSFORMERS_DISABLE_THINKING"), default=True)


def _llama_cpp_preferred_quants() -> List[str]:
    raw = str(os.getenv("TATER_LLAMA_CPP_PREFERRED_QUANTS") or "Q4_K_M,Q5_K_M,Q4_K_S,Q5_0,Q4_0,Q8_0").strip()
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def _mlx_lm_is_apple_silicon() -> bool:
    return platform.system().lower() == "darwin" and platform.machine().lower() in {"arm64", "aarch64"}


def _mlx_lm_trust_remote_code() -> bool:
    raw = _safe_redis_text_get(HYDRA_MLX_LM_TRUST_REMOTE_CODE_KEY)
    if raw:
        return _boolish(raw, default=DEFAULT_MLX_LM_TRUST_REMOTE_CODE)
    return _boolish(os.getenv("TATER_MLX_LM_TRUST_REMOTE_CODE"), default=DEFAULT_MLX_LM_TRUST_REMOTE_CODE)


def _mlx_lm_lazy_load() -> bool:
    raw = _safe_redis_text_get(HYDRA_MLX_LM_LAZY_LOAD_KEY)
    if raw:
        return _boolish(raw, default=DEFAULT_MLX_LM_LAZY_LOAD)
    return _boolish(os.getenv("TATER_MLX_LM_LAZY"), default=DEFAULT_MLX_LM_LAZY_LOAD)


def _mlx_lm_disable_thinking_enabled() -> bool:
    return _boolish(os.getenv("TATER_MLX_LM_DISABLE_THINKING"), default=True)


def _local_no_thinking_template_variants(enabled: bool = True) -> Tuple[Dict[str, Any], ...]:
    if not enabled:
        return ({},)
    return (
        {"enable_thinking": False, "reasoning_budget": 0},
        {"enable_thinking": False},
        {"reasoning_budget": 0},
        {},
    )


def _merge_template_kwargs(*items: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for item in items:
        if isinstance(item, dict):
            merged.update(item)
    return merged


def _llama_cpp_chat_template_max_chars() -> int:
    try:
        raw = int(float(os.getenv("TATER_LLAMA_CPP_CHAT_TEMPLATE_MAX_CHARS") or "200000"))
    except Exception:
        raw = 200000
    return max(1024, min(1_000_000, raw))


def _normalize_llama_cpp_chat_template(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if len(text) > _llama_cpp_chat_template_max_chars():
        raise ValueError(f"Chat template is too large; max {_llama_cpp_chat_template_max_chars()} characters.")
    return text


def _llama_cpp_chat_template_field(model_id: Any) -> str:
    token = str(model_id or "").strip()
    if not token:
        return ""
    return hashlib.sha256(token.encode("utf-8", errors="ignore")).hexdigest()


def _local_llm_chat_template_store_key(provider: Any) -> str:
    provider_token = _normalize_hydra_llm_provider(provider)
    if provider_token == HYDRA_LLM_PROVIDER_HF_TRANSFORMERS:
        return HYDRA_HF_TRANSFORMERS_CHAT_TEMPLATE_OVERRIDES_KEY
    if provider_token == HYDRA_LLM_PROVIDER_MLX_LM:
        return HYDRA_MLX_LM_CHAT_TEMPLATE_OVERRIDES_KEY
    return HYDRA_LLAMA_CPP_CHAT_TEMPLATE_OVERRIDES_KEY


def _llama_cpp_chat_template_capabilities(template: Any) -> Dict[str, bool]:
    text = str(template or "")
    lower = text.lower()
    return {
        "has_template": bool(text.strip()),
        "enable_thinking": "enable_thinking" in text,
        "reasoning_budget": "reasoning_budget" in text,
        "no_think_marker": "/no_think" in text or "no_think" in text,
        "think_tags": "<think" in lower or "</think" in lower,
        "uses_enable_thinking": "enable_thinking" in text,
        "uses_reasoning_budget": "reasoning_budget" in text,
        "mentions_thinking": "thinking" in lower or "<think" in lower,
    }


def get_local_llm_chat_template_override(provider: Any, model_id: Any) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model_id or "").strip()
    field = _llama_cpp_chat_template_field(model_token)
    if not field:
        return {}
    try:
        raw = redis_client.hget(_local_llm_chat_template_store_key(provider_token), field)
    except Exception:
        raw = None
    if not raw:
        return {}
    try:
        payload = json.loads(str(raw))
    except Exception:
        payload = {"template": str(raw)}
    template = _normalize_llama_cpp_chat_template(payload.get("template") if isinstance(payload, dict) else "")
    if not template:
        return {}
    return {
        "provider": provider_token,
        "model": model_token,
        "template": template,
        "updated_ts": float((payload or {}).get("updated_ts") or 0.0) if isinstance(payload, dict) else 0.0,
        "source": "override",
    }


def get_llama_cpp_chat_template_override(model_id: Any) -> Dict[str, Any]:
    return get_local_llm_chat_template_override(HYDRA_LLM_PROVIDER_LLAMA_CPP, model_id)


def set_local_llm_chat_template_override(provider: Any, model_id: Any, template: Any) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model_id or "").strip()
    if not model_token:
        raise ValueError("Local model id is required.")
    normalized = _normalize_llama_cpp_chat_template(template)
    if not normalized.strip():
        raise ValueError("Chat template cannot be blank. Reset the override to use the embedded template.")
    field = _llama_cpp_chat_template_field(model_token)
    payload = {
        "provider": provider_token,
        "model": model_token,
        "template": normalized,
        "updated_ts": time.time(),
    }
    redis_client.hset(_local_llm_chat_template_store_key(provider_token), field, json.dumps(payload, sort_keys=True))
    return {**payload, "source": "override"}


def set_llama_cpp_chat_template_override(model_id: Any, template: Any) -> Dict[str, Any]:
    return set_local_llm_chat_template_override(HYDRA_LLM_PROVIDER_LLAMA_CPP, model_id, template)


def clear_local_llm_chat_template_override(provider: Any, model_id: Any) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model_id or "").strip()
    field = _llama_cpp_chat_template_field(model_token)
    if field:
        try:
            redis_client.hdel(_local_llm_chat_template_store_key(provider_token), field)
        except Exception:
            pass
    return {"provider": provider_token, "model": model_token, "template": "", "source": "embedded"}


def clear_llama_cpp_chat_template_override(model_id: Any) -> Dict[str, Any]:
    return clear_local_llm_chat_template_override(HYDRA_LLM_PROVIDER_LLAMA_CPP, model_id)


def _local_llm_chat_template_override_text(provider: Any, model_id: Any) -> str:
    return str(get_local_llm_chat_template_override(provider, model_id).get("template") or "")


def _llama_cpp_chat_template_override_text(model_id: Any) -> str:
    return _local_llm_chat_template_override_text(HYDRA_LLM_PROVIDER_LLAMA_CPP, model_id)


def _llama_cpp_chat_template_cache_token(model_id: Any) -> str:
    template = _llama_cpp_chat_template_override_text(model_id)
    return hashlib.sha256(template.encode("utf-8", errors="ignore")).hexdigest() if template else ""


def _mlx_lm_adapter_path() -> str:
    return os.path.abspath(os.path.expanduser(str(os.getenv("TATER_MLX_LM_ADAPTER_PATH") or "").strip())) if str(os.getenv("TATER_MLX_LM_ADAPTER_PATH") or "").strip() else ""


def _mlx_lm_revision() -> str:
    return str(os.getenv("TATER_MLX_LM_REVISION") or "").strip()


def _mlx_lm_hub_token() -> Optional[str]:
    token = _text(os.getenv("TATER_MLX_LM_TOKEN") or os.getenv("TATER_HF_TRANSFORMERS_TOKEN")).strip()
    if not token:
        token = _huggingface_integration_token()
    if not token:
        token = _text(
            os.getenv("HF_TOKEN")
            or os.getenv("HUGGINGFACE_HUB_TOKEN")
            or os.getenv("HUGGING_FACE_HUB_TOKEN")
            or os.getenv("HUGGINGFACE_TOKEN")
            or os.getenv("HF_HUB_TOKEN")
            or os.getenv("HUGGINGFACE_API_TOKEN")
        ).strip()
    return token or None


def _mlx_lm_max_kv_size() -> Optional[int]:
    return _local_llm_context_tokens(
        HYDRA_MLX_LM_CONTEXT_TOKENS_KEY,
        ("TATER_MLX_LM_MAX_KV_SIZE",),
        None,
        minimum=128,
    )


def _mlx_runtime_call(func: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
    previous = bool(getattr(_MLX_RUNTIME_LOCAL, "active", False))
    _MLX_RUNTIME_LOCAL.active = True
    try:
        return func(*args, **kwargs)
    finally:
        _MLX_RUNTIME_LOCAL.active = previous


def _run_mlx_runtime_sync(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if bool(getattr(_MLX_RUNTIME_LOCAL, "active", False)):
        return func(*args, **kwargs)
    future = _MLX_RUNTIME_EXECUTOR.submit(_mlx_runtime_call, func, tuple(args), dict(kwargs))
    return future.result()


async def _run_mlx_runtime_async(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    loop = asyncio.get_running_loop()
    call = functools.partial(_mlx_runtime_call, func, tuple(args), dict(kwargs))
    return await loop.run_in_executor(_MLX_RUNTIME_EXECUTOR, call)


def _mlx_engine_checkout_path() -> str:
    raw = str(os.getenv("TATER_MLX_ENGINE_PATH") or "").strip()
    if raw:
        return os.path.abspath(os.path.expanduser(raw))
    return str(runtime_dir() / "mlx-engine")


def _mlx_engine_max_seq_nums() -> int:
    try:
        raw = int(float(os.getenv("TATER_MLX_ENGINE_MAX_SEQ_NUMS") or "1"))
    except Exception:
        raw = 1
    return max(1, min(64, raw))


def _mlx_engine_prefill_step_size_raw() -> str:
    raw = _safe_redis_text_get(HYDRA_MLX_ENGINE_PREFILL_STEP_SIZE_KEY)
    if not raw:
        raw = str(os.getenv("TATER_MLX_ENGINE_PREFILL_STEP_SIZE") or "").strip()
    return raw


def _mlx_engine_parse_prefill_step_size(raw: Any) -> Optional[int]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        value = int(float(text))
    except Exception:
        return None
    if value < 1:
        return None
    return max(1, min(MAX_MLX_ENGINE_PREFILL_STEP_SIZE, value))


def _mlx_engine_auto_prefill_step_size() -> int:
    if not _mlx_lm_is_apple_silicon():
        return DEFAULT_MLX_ENGINE_PREFILL_STEP_SIZE
    try:
        total_bytes = max(0, int(_system_ram_snapshot().get("total_bytes") or 0))
    except Exception:
        total_bytes = 0
    total_gib = total_bytes / float(1024**3) if total_bytes > 0 else 0.0
    if total_gib >= 64:
        return FAST_MLX_ENGINE_PREFILL_STEP_SIZE
    if total_gib >= 32:
        return MEDIUM_MLX_ENGINE_PREFILL_STEP_SIZE
    return DEFAULT_MLX_ENGINE_PREFILL_STEP_SIZE


def _mlx_engine_resolve_prefill_step_size() -> Tuple[Optional[int], str]:
    raw = _mlx_engine_prefill_step_size_raw()
    token = raw.strip().lower()
    if token in {"runtime", "engine", "mlx-default"}:
        return None, "runtime_default"
    manual = _mlx_engine_parse_prefill_step_size(raw)
    if manual is not None:
        return manual, "manual"
    return _mlx_engine_auto_prefill_step_size(), "auto"


def _mlx_engine_prefill_step_size() -> Optional[int]:
    return _mlx_engine_resolve_prefill_step_size()[0]


def _mlx_engine_optional_int_setting(
    redis_key: str,
    env_name: str,
    *,
    minimum: int = 0,
    maximum: int = 1_048_576,
    allow_zero: bool = False,
    allowed: Optional[Tuple[int, ...]] = None,
) -> Optional[int]:
    raw = _safe_redis_text_get(redis_key)
    if not raw:
        raw = str(os.getenv(env_name) or "").strip()
    if not raw:
        return None
    try:
        value = int(float(raw))
    except Exception:
        return None
    if value < 0 or (value == 0 and not allow_zero):
        return None
    if allowed is not None and value not in set(int(item) for item in allowed):
        return None
    return max(int(minimum), min(int(maximum), int(value)))


def _mlx_engine_optional_int_env(name: str) -> Optional[int]:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return None
    try:
        return int(float(raw))
    except Exception:
        return None


def _mlx_engine_max_image_size() -> Optional[Tuple[int, int]]:
    value = _mlx_engine_optional_int_env("TATER_MLX_ENGINE_MAX_IMAGE_SIZE")
    if value is None or value <= 0:
        return None
    side = max(64, min(4096, int(value)))
    return (side, side)


def _mlx_engine_prepare_import_path() -> str:
    checkout = _mlx_engine_checkout_path()
    package_dir = os.path.join(checkout, "mlx_engine")
    if os.path.isdir(package_dir) and checkout not in sys.path:
        sys.path.insert(0, checkout)
    return checkout


def _mlx_engine_patch_gemma4_blockwise_overlay() -> None:
    try:
        from mlx_vlm.models.gemma4 import language as gemma4_language  # type: ignore
    except Exception:
        return

    model_cls = getattr(gemma4_language, "Gemma4TextModel", None)
    if model_cls is None:
        return
    current = getattr(model_cls, "_apply_blockwise_bidirectional_overlay", None)
    if getattr(current, "_tater_mlx_gemma4_overlay_compat", False):
        return
    mx_module = getattr(gemma4_language, "mx", None)
    if mx_module is None or not callable(current):
        return

    original = current

    def _apply_blockwise_bidirectional_overlay_compat(self: Any, base_mask: Any, mm_token_type_ids: Any) -> Any:
        if mm_token_type_ids is None:
            return base_mask
        try:
            key_len = int(base_mask.shape[-1])
            query_len = int(base_mask.shape[-2])
            token_len = int(mm_token_type_ids.shape[1])
        except Exception:
            return original(self, base_mask, mm_token_type_ids)
        if token_len != key_len or query_len <= 0:
            return base_mask

        try:
            block_sequence_ids = self._block_sequence_ids_for_mask(mm_token_type_ids)
            if query_len < token_len:
                start = token_len - query_len
                query_blocks = block_sequence_ids[:, start : start + query_len]
            else:
                query_blocks = block_sequence_ids
            q_blocks = mx_module.expand_dims(query_blocks, -1)
            k_blocks = mx_module.expand_dims(block_sequence_ids, -2)
            same_block = (q_blocks != -1) & (q_blocks == k_blocks)
            return base_mask | mx_module.expand_dims(same_block, 1)
        except Exception:
            return original(self, base_mask, mm_token_type_ids)

    setattr(_apply_blockwise_bidirectional_overlay_compat, "_tater_mlx_gemma4_overlay_compat", True)
    setattr(model_cls, "_apply_blockwise_bidirectional_overlay", _apply_blockwise_bidirectional_overlay_compat)


def _mlx_engine_patch_batched_vision_loader() -> None:
    try:
        from mlx_engine.model_kit.batched_vision import model_kit as batched_vision_model_kit  # type: ignore
    except Exception:
        return

    kit_cls = getattr(batched_vision_model_kit, "BatchedVisionModelKit", None)
    if kit_cls is None:
        return
    current = getattr(kit_cls, "_load_model", None)
    if getattr(current, "_tater_mlx_vlm_load_compat", False):
        return

    mlx_vlm_module = getattr(batched_vision_model_kit, "mlx_vlm", None)
    mx_module = getattr(batched_vision_model_kit, "mx", None)
    if mlx_vlm_module is None or mx_module is None:
        return

    def _load_model_compat(self: Any) -> None:
        loaded = mlx_vlm_module.utils.load_model(
            self._model_path,
            lazy=False,
            trust_remote_code=self._trust_remote_code,
        )
        if isinstance(loaded, (tuple, list)):
            if not loaded:
                raise RuntimeError("mlx-vlm returned no model object.")
            self.model = loaded[0]
        else:
            self.model = loaded
        mx_module.synchronize()
        mx_module.clear_cache()

    setattr(_load_model_compat, "_tater_mlx_vlm_load_compat", True)
    setattr(kit_cls, "_load_model", _load_model_compat)


def _mlx_vlm_token_from_id(tokenizer: Any, token_id: Any, fallback: str) -> str:
    if token_id is None:
        return fallback
    convert = getattr(tokenizer, "convert_ids_to_tokens", None)
    if callable(convert):
        try:
            token = convert(int(token_id))
            if isinstance(token, str) and token:
                return token
        except Exception:
            pass
    decode = getattr(tokenizer, "decode", None)
    if callable(decode):
        try:
            token = decode([int(token_id)])
            if isinstance(token, str) and token:
                return token
        except Exception:
            pass
    return fallback


def _mlx_vlm_first_token_id(value: Any) -> Optional[int]:
    if isinstance(value, (list, tuple)):
        for item in value:
            parsed = _mlx_vlm_first_token_id(item)
            if parsed is not None:
                return parsed
        return None
    try:
        return int(value)
    except Exception:
        return None


def _mlx_vlm_config_token_id(config: Dict[str, Any], key: str) -> Optional[int]:
    for source in (config, config.get("text_config", {}), config.get("vision_config", {})):
        if isinstance(source, dict) and key in source:
            parsed = _mlx_vlm_first_token_id(source.get(key))
            if parsed is not None:
                return parsed
    return None


def _mlx_vlm_set_missing_attr(target: Any, name: str, value: Any) -> None:
    if target is None or value is None:
        return
    try:
        current = getattr(target, name, None)
        if current in (None, ""):
            setattr(target, name, value)
    except Exception:
        pass


def _mlx_vlm_normalize_special_tokens(target: Any, config: Dict[str, Any], token_source: Any) -> None:
    if target is None:
        return
    eos_token_id = _mlx_vlm_config_token_id(config, "eos_token_id")
    bos_token_id = _mlx_vlm_config_token_id(config, "bos_token_id")
    pad_token_id = _mlx_vlm_config_token_id(config, "pad_token_id")
    if pad_token_id is None:
        pad_token_id = eos_token_id

    eos_token = _mlx_vlm_token_from_id(token_source, eos_token_id, "<eos>") if eos_token_id is not None else None
    bos_token = _mlx_vlm_token_from_id(token_source, bos_token_id, "<bos>") if bos_token_id is not None else None
    pad_token = _mlx_vlm_token_from_id(token_source, pad_token_id, eos_token or "<pad>") if pad_token_id is not None else None

    _mlx_vlm_set_missing_attr(target, "eos_token_id", eos_token_id)
    _mlx_vlm_set_missing_attr(target, "bos_token_id", bos_token_id)
    _mlx_vlm_set_missing_attr(target, "pad_token_id", pad_token_id)
    _mlx_vlm_set_missing_attr(target, "eos_token", eos_token)
    _mlx_vlm_set_missing_attr(target, "bos_token", bos_token)
    _mlx_vlm_set_missing_attr(target, "pad_token", pad_token)


def _mlx_vlm_normalize_vision_tokens(processor: Any, tokenizer: Any, config: Any) -> None:
    if not isinstance(config, dict) or "vision_config" not in config:
        return
    nested_tokenizer = getattr(processor, "tokenizer", None)
    token_source = nested_tokenizer or tokenizer or processor

    image_token_id = config.get("image_token_id") or config.get("vision_config", {}).get("image_token_id")
    boi_token_id = config.get("boi_token_id") or config.get("vision_config", {}).get("boi_token_id")
    eoi_token_id = config.get("eoi_token_id") or config.get("vision_config", {}).get("eoi_token_id")
    image_token = _mlx_vlm_token_from_id(token_source, image_token_id, "<|image|>")
    boi_token = _mlx_vlm_token_from_id(token_source, boi_token_id, "<|image>")
    eoi_token = _mlx_vlm_token_from_id(token_source, eoi_token_id, "<image|>")

    targets = [processor, tokenizer, nested_tokenizer]
    seen: set[int] = set()
    for target in targets:
        if target is None:
            continue
        ident = id(target)
        if ident in seen:
            continue
        seen.add(ident)
        _mlx_vlm_set_missing_attr(target, "image_token_id", image_token_id)
        _mlx_vlm_set_missing_attr(target, "boi_token_id", boi_token_id)
        _mlx_vlm_set_missing_attr(target, "eoi_token_id", eoi_token_id)
        _mlx_vlm_set_missing_attr(target, "image_token", image_token)
        _mlx_vlm_set_missing_attr(target, "boi_token", boi_token)
        _mlx_vlm_set_missing_attr(target, "eoi_token", eoi_token)
        _mlx_vlm_normalize_special_tokens(target, config, token_source)

    image_seq_length = (
        config.get("image_seq_length")
        or config.get("vision_soft_tokens_per_image")
        or config.get("vision_config", {}).get("image_seq_length")
        or config.get("vision_config", {}).get("max_soft_tokens")
        or getattr(processor, "image_seq_length", None)
    )
    try:
        image_seq_length_int = int(image_seq_length)
    except Exception:
        image_seq_length_int = 0
    if processor is not None and image_seq_length_int > 0 and image_token:
        try:
            processor.image_seq_length = image_seq_length_int
            processor.full_image_sequence = f"{boi_token}{image_token * image_seq_length_int}{eoi_token}"
        except Exception:
            pass


def _mlx_engine_normalize_bundle_vision_tokens(bundle: Dict[str, Any]) -> None:
    config = bundle.get("config")
    if not isinstance(config, dict) or "vision_config" not in config:
        return
    model_kit = bundle.get("model_kit")
    _mlx_vlm_normalize_vision_tokens(
        getattr(model_kit, "processor", None),
        getattr(model_kit, "tokenizer", None),
        config,
    )
    _mlx_vlm_normalize_vision_tokens(
        bundle.get("processor"),
        bundle.get("tokenizer"),
        config,
    )


def _mlx_engine_import_helpers() -> Dict[str, Any]:
    checkout = _mlx_engine_prepare_import_path()
    try:
        from mlx_engine.generate import create_generator, load_model, tokenize  # type: ignore
        _mlx_engine_patch_gemma4_blockwise_overlay()
        _mlx_engine_patch_batched_vision_loader()
    except Exception as exc:
        hint = (
            "Install/update the MLX engine dependencies with setup_tater.sh so the MLX provider can run."
        )
        if checkout and not os.path.isdir(os.path.join(checkout, "mlx_engine")):
            hint = f"MLX engine checkout was not found at {checkout}. {hint}"
        elif "outlines_core" in str(exc):
            hint = f"MLX engine needs outlines-core==0.1.26. {hint}"
        raise RuntimeError(hint) from exc
    return {
        "load_model": load_model,
        "create_generator": create_generator,
        "tokenize": tokenize,
    }


def _mlx_engine_config_json(model_path: str) -> Dict[str, Any]:
    try:
        path = Path(str(model_path or "")) / "config.json"
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _mlx_engine_cache_key(model_id: str) -> Tuple[Any, ...]:
    return (
        str(model_id or "").strip(),
        _mlx_lm_trust_remote_code(),
        _mlx_lm_max_kv_size() or 0,
        _mlx_engine_max_seq_nums(),
        _mlx_engine_prefill_step_size() or 0,
        _mlx_engine_optional_int_setting(
            HYDRA_MLX_ENGINE_KV_BITS_KEY,
            "TATER_MLX_ENGINE_KV_BITS",
            minimum=2,
            maximum=8,
            allowed=(2, 3, 4, 6, 8),
        )
        or 0,
        _mlx_engine_optional_int_setting(
            HYDRA_MLX_ENGINE_KV_GROUP_SIZE_KEY,
            "TATER_MLX_ENGINE_KV_GROUP_SIZE",
            minimum=32,
            maximum=128,
            allowed=(32, 64, 128),
        )
        or 0,
        _mlx_engine_optional_int_setting(
            HYDRA_MLX_ENGINE_QUANTIZED_KV_START_KEY,
            "TATER_MLX_ENGINE_QUANTIZED_KV_START",
            minimum=0,
            maximum=1_048_576,
            allow_zero=True,
        )
        or 0,
    )


def _mlx_engine_bundle_backend_exception(bundle: Any) -> Optional[Exception]:
    if not isinstance(bundle, dict):
        return None
    model_kit = bundle.get("model_kit")
    backend_exception = getattr(model_kit, "_backend_exception", None)
    return backend_exception if isinstance(backend_exception, Exception) else None


def _mlx_engine_bundle_is_poisoned(bundle: Any) -> bool:
    if _mlx_engine_bundle_backend_exception(bundle) is not None:
        return True
    model_kit = bundle.get("model_kit") if isinstance(bundle, dict) else None
    is_shutdown = getattr(model_kit, "is_shutdown", None)
    if callable(is_shutdown):
        try:
            return bool(is_shutdown())
        except Exception:
            return False
    return False


def _mlx_engine_shutdown_bundle(bundle: Any) -> None:
    if not isinstance(bundle, dict):
        return
    shutdown = getattr(bundle.get("model_kit"), "shutdown", None)
    if callable(shutdown):
        try:
            shutdown()
        except Exception:
            logger.debug("[mlx-engine] Failed to shut down poisoned model kit.", exc_info=True)


def _mlx_engine_evict_bundle(bundle: Any, *, reason: str) -> None:
    if not isinstance(bundle, dict):
        return
    cache_key = bundle.get("cache_key")
    with _MLX_ENGINE_MODEL_CACHE_LOCK:
        if cache_key is not None and _MLX_ENGINE_MODEL_CACHE.get(cache_key) is bundle:
            _MLX_ENGINE_MODEL_CACHE.pop(cache_key, None)
            _MLX_ENGINE_GENERATION_LOCKS.pop(cache_key, None)
    _mlx_engine_shutdown_bundle(bundle)
    logger.warning("[mlx-engine] Evicted cached model after backend failure: %s", reason)


def _mlx_engine_generation_exception_is_fatal(bundle: Dict[str, Any], exc: Exception) -> bool:
    if _mlx_engine_bundle_backend_exception(bundle) is not None:
        return True
    detail = str(exc or "")
    return "Encountered fatal exception in the backend generation thread" in detail


def _load_mlx_engine_bundle(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> Dict[str, Any]:
    model_token = str(model_id or "").strip()
    if not model_token:
        raise RuntimeError("MLX model id or local path is required.")
    if not _mlx_lm_is_apple_silicon():
        raise RuntimeError("MLX engine runs on Apple Silicon Macs only.")
    if _mlx_lm_adapter_path():
        raise RuntimeError("MLX engine adapter loading is not wired in Tater yet.")

    cache_key = _mlx_engine_cache_key(model_token)
    with _MLX_ENGINE_MODEL_CACHE_LOCK:
        cached = _MLX_ENGINE_MODEL_CACHE.get(cache_key)
        if isinstance(cached, dict):
            if _mlx_engine_bundle_is_poisoned(cached):
                _mlx_engine_evict_bundle(cached, reason="cached model kit has a backend exception")
            else:
                _mlx_engine_normalize_bundle_vision_tokens(cached)
                _emit_hf_llm_progress(
                    progress_callback,
                    {
                        "event": "complete",
                        "stage": "load",
                        "description": "MLX engine model is already loaded",
                        "progress": 100.0,
                        "device": "apple_silicon",
                    },
                )
                return cached

        helpers = _mlx_engine_import_helpers()
        model_path = _download_mlx_lm_model(model_token, progress_callback=progress_callback)
        config = _mlx_engine_config_json(model_path)
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "start",
                "stage": "load",
                "description": "Loading MLX engine model into memory",
                "progress": 0.0,
            },
        )

        load_kwargs: Dict[str, Any] = {
            "trust_remote_code": _mlx_lm_trust_remote_code(),
            "max_seq_nums": _mlx_engine_max_seq_nums(),
        }
        max_kv_size = _mlx_lm_max_kv_size()
        if max_kv_size is not None:
            load_kwargs["max_kv_size"] = max(128, int(max_kv_size))
        prefill_step_size, prefill_step_size_source = _mlx_engine_resolve_prefill_step_size()
        if prefill_step_size is not None:
            load_kwargs["prefill_step_size"] = prefill_step_size
        if "vision_config" not in config:
            for redis_key, env_name, arg_name, minimum, maximum, allow_zero, allowed in (
                (HYDRA_MLX_ENGINE_KV_BITS_KEY, "TATER_MLX_ENGINE_KV_BITS", "kv_bits", 2, 8, False, (2, 3, 4, 6, 8)),
                (HYDRA_MLX_ENGINE_KV_GROUP_SIZE_KEY, "TATER_MLX_ENGINE_KV_GROUP_SIZE", "kv_group_size", 32, 128, False, (32, 64, 128)),
                (
                    HYDRA_MLX_ENGINE_QUANTIZED_KV_START_KEY,
                    "TATER_MLX_ENGINE_QUANTIZED_KV_START",
                    "quantized_kv_start",
                    0,
                    1_048_576,
                    True,
                    None,
                ),
            ):
                value = _mlx_engine_optional_int_setting(
                    redis_key,
                    env_name,
                    minimum=minimum,
                    maximum=maximum,
                    allow_zero=allow_zero,
                    allowed=allowed,
                )
                if value is not None:
                    load_kwargs[arg_name] = value
        elif any(
            _mlx_engine_optional_int_setting(redis_key, env_name, minimum=minimum, maximum=maximum, allow_zero=allow_zero, allowed=allowed)
            is not None
            for redis_key, env_name, minimum, maximum, allow_zero, allowed in (
                (HYDRA_MLX_ENGINE_KV_BITS_KEY, "TATER_MLX_ENGINE_KV_BITS", 2, 8, False, (2, 3, 4, 6, 8)),
                (HYDRA_MLX_ENGINE_KV_GROUP_SIZE_KEY, "TATER_MLX_ENGINE_KV_GROUP_SIZE", 32, 128, False, (32, 64, 128)),
                (HYDRA_MLX_ENGINE_QUANTIZED_KV_START_KEY, "TATER_MLX_ENGINE_QUANTIZED_KV_START", 0, 1_048_576, True, None),
            )
        ):
            logger.info("[mlx-engine] KV cache quantization settings are ignored for vision-capable model %s.", model_token)

        model_kit = helpers["load_model"](model_path, **load_kwargs)
        _mlx_vlm_normalize_vision_tokens(
            getattr(model_kit, "processor", None),
            getattr(model_kit, "tokenizer", None),
            config,
        )
        trust_kwargs: Dict[str, Any] = {}
        if _mlx_lm_trust_remote_code():
            trust_kwargs["trust_remote_code"] = True

        tokenizer = getattr(model_kit, "tokenizer", None)
        processor = getattr(model_kit, "processor", None) if "vision_config" in config else None
        try:
            from transformers import AutoProcessor, AutoTokenizer  # type: ignore

            try:
                tokenizer = AutoTokenizer.from_pretrained(model_path, **trust_kwargs)
            except Exception:
                pass
            if "vision_config" in config:
                try:
                    processor = AutoProcessor.from_pretrained(model_path, **trust_kwargs)
                except Exception:
                    pass
        except Exception:
            pass
        if processor is None:
            processor = tokenizer
        _mlx_vlm_normalize_vision_tokens(processor, tokenizer, config)

        chat_template_override, chat_template_handler, chat_template_warning = _install_local_llm_chat_template_override(
            HYDRA_LLM_PROVIDER_MLX_LM,
            model_token,
            tokenizer=tokenizer,
            processor=processor,
            config=config,
            model=model_kit,
        )
        if chat_template_warning:
            logger.warning("[mlx-engine] %s", chat_template_warning)

        generation_lock = _MLX_ENGINE_GENERATION_LOCKS.get(cache_key)
        if generation_lock is None:
            generation_lock = threading.RLock()
            _MLX_ENGINE_GENERATION_LOCKS[cache_key] = generation_lock

        bundle = {
            "runtime": "mlx-engine",
            "cache_key": cache_key,
            "model_kit": model_kit,
            "tokenizer": tokenizer,
            "processor": processor,
            "config": config,
            "lock": generation_lock,
            "model_path": model_path,
            "model_root": _mlx_lm_model_root(),
            "device": "apple_silicon",
            "memory_estimate_bytes": _safe_path_size_bytes(model_path),
            "loaded_ts": time.time(),
            "chat_template_override": bool(chat_template_override),
            "chat_template_handler": chat_template_handler,
            "chat_template_warning": chat_template_warning,
            "trust_remote_code": _mlx_lm_trust_remote_code(),
            "max_kv_size": max_kv_size,
            "prefill_step_size": prefill_step_size,
            "prefill_step_size_source": prefill_step_size_source,
            "kv_bits": load_kwargs.get("kv_bits"),
            "kv_group_size": load_kwargs.get("kv_group_size"),
            "quantized_kv_start": load_kwargs.get("quantized_kv_start"),
            "supports_vision": bool("vision_config" in config),
            "vision_chat_handler": "mlx-engine" if "vision_config" in config else "",
            "create_generator": helpers["create_generator"],
            "tokenize": helpers["tokenize"],
        }
        _mlx_engine_normalize_bundle_vision_tokens(bundle)
        _MLX_ENGINE_MODEL_CACHE[cache_key] = bundle
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "complete",
                "stage": "load",
                "description": "MLX engine model loaded",
                "progress": 100.0,
                "device": "apple_silicon",
            },
        )
        return bundle


def _mlx_engine_tokenize_prompt(bundle: Dict[str, Any], prompt: str) -> List[int]:
    tokenize = bundle.get("tokenize")
    model_kit = bundle.get("model_kit")
    if callable(tokenize) and model_kit is not None:
        tokens = tokenize(model_kit, prompt)
        return list(tokens or [])
    tokenizer = bundle.get("tokenizer") or bundle.get("processor")
    encode = getattr(tokenizer, "encode", None)
    if callable(encode):
        tokens = encode(prompt)
        return list(tokens or [])
    return []


def _mlx_engine_stop_strings(stop: Any) -> List[str]:
    if not stop:
        return []
    items = stop if isinstance(stop, list) else [stop]
    return [str(item) for item in items if str(item or "")]


def _mlx_engine_run_generation(
    bundle: Dict[str, Any],
    prompt: str,
    *,
    max_tokens: int,
    temp: float,
    stop: Any = None,
    images_b64: Optional[List[str]] = None,
    max_image_size: Optional[Tuple[int, int]] = None,
    extra_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    create_generator = bundle.get("create_generator")
    model_kit = bundle.get("model_kit")
    generation_lock = bundle.get("lock")
    if not callable(create_generator) or model_kit is None:
        raise RuntimeError("MLX engine runtime is missing generation helpers.")

    prompt_tokens = _mlx_engine_tokenize_prompt(bundle, prompt)
    generation_kwargs: Dict[str, Any] = {
        "max_tokens": max(1, int(max_tokens or 1)),
        "temp": max(0.0, float(temp)),
        "request_id": f"tater-{uuid.uuid4().hex}",
    }
    stop_strings = _mlx_engine_stop_strings(stop)
    if stop_strings:
        generation_kwargs["stop_strings"] = stop_strings
    if images_b64:
        generation_kwargs["images_b64"] = [str(item) for item in images_b64 if str(item or "")]
    if max_image_size is not None:
        generation_kwargs["max_image_size"] = max_image_size
    for key in (
        "top_p",
        "top_k",
        "min_p",
        "min_tokens_to_keep",
        "seed",
        "json_schema",
        "repetition_penalty",
        "repetition_context_size",
        "num_draft_tokens",
    ):
        if extra_kwargs and key in extra_kwargs and extra_kwargs.get(key) is not None:
            generation_kwargs[key] = extra_kwargs[key]

    content = ""
    completion_tokens = 0
    generation_started = time.perf_counter()
    lock = generation_lock if hasattr(generation_lock, "__enter__") else threading.RLock()
    try:
        with lock:
            for generation_result in create_generator(model_kit, prompt_tokens, **generation_kwargs):
                text_part = _coerce_content_to_text(getattr(generation_result, "text", ""))
                content += text_part
                tokens = getattr(generation_result, "tokens", None)
                try:
                    completion_tokens += len(tokens or [])
                except Exception:
                    pass
                if getattr(generation_result, "stop_condition", None) is not None:
                    break
    except Exception as exc:
        if _mlx_engine_generation_exception_is_fatal(bundle, exc):
            reason = (str(exc or type(exc).__name__) or type(exc).__name__).replace("\n", " ")[:240]
            _mlx_engine_evict_bundle(bundle, reason=reason)
        raise
    generation_elapsed = max(0.0, time.perf_counter() - generation_started)
    content = _strip_local_thinking_blocks(_apply_local_stop_sequences(content, stop)).strip()
    if completion_tokens <= 0:
        completion_tokens = len(_mlx_engine_tokenize_prompt(bundle, content))
    return {
        "content": content,
        "prompt_tokens": len(prompt_tokens),
        "completion_tokens": max(0, int(completion_tokens)),
        "elapsed": generation_elapsed,
    }


def _emit_hf_llm_progress(progress_callback: Optional[HFProgressCallback], payload: Dict[str, Any]) -> None:
    if not callable(progress_callback):
        return
    try:
        progress_callback(dict(payload or {}))
    except HfLlmDownloadCancelled:
        raise
    except Exception:
        pass


def _hf_cache_repo_dir(cache_dir: str, repo_id: str) -> Path:
    repo_token = str(repo_id or "").strip().replace("/", "--")
    return Path(str(cache_dir or "")).expanduser() / f"models--{repo_token}"


def _latest_hf_snapshot_path(cache_dir: str, repo_id: str) -> str:
    repo_dir = _hf_cache_repo_dir(cache_dir, repo_id)
    snapshots_dir = repo_dir / "snapshots"
    try:
        snapshots = [path for path in snapshots_dir.iterdir() if path.is_dir()]
    except Exception:
        snapshots = []
    if not snapshots:
        return ""
    latest = max(snapshots, key=lambda path: path.stat().st_mtime if path.exists() else 0.0)
    return str(latest)


def _hf_file_size_and_blob_ids(repo_id: str, filename: str) -> Tuple[int, List[str]]:
    repo = str(repo_id or "").strip()
    target = str(filename or "").strip()
    if not repo or not target:
        return 0, []
    try:
        from huggingface_hub import HfApi  # type: ignore
    except Exception:
        return 0, []
    try:
        api = HfApi(token=_hf_llm_hub_token())
        try:
            info = api.model_info(repo_id=repo, files_metadata=True)
        except TypeError:
            info = api.model_info(repo_id=repo)
    except Exception:
        return 0, []

    for sibling in list(getattr(info, "siblings", []) or []):
        path = str(getattr(sibling, "rfilename", "") or getattr(sibling, "path", "") or "").strip()
        if path != target:
            continue
        size = 0
        for value in (getattr(sibling, "size", None),):
            try:
                size = max(size, int(value or 0))
            except Exception:
                pass
        blob_ids: List[str] = []
        for value in (
            getattr(sibling, "blob_id", None),
            getattr(sibling, "oid", None),
        ):
            token = str(value or "").strip()
            if token and token not in blob_ids:
                blob_ids.append(token)
        lfs = getattr(sibling, "lfs", None)
        if isinstance(lfs, dict):
            for key in ("size",):
                try:
                    size = max(size, int(lfs.get(key) or 0))
                except Exception:
                    pass
            for key in ("sha256", "oid"):
                token = str(lfs.get(key) or "").strip()
                if token and token not in blob_ids:
                    blob_ids.append(token)
        return max(0, size), blob_ids
    return 0, []


def _download_stat_size(path: Path, *, total_bytes: int = 0) -> int:
    try:
        stat = path.stat()
    except Exception:
        return 0
    logical_size = max(0, int(getattr(stat, "st_size", 0) or 0))
    allocated_size = max(0, int(getattr(stat, "st_blocks", 0) or 0) * 512)
    if total_bytes > 0 and logical_size >= int(total_bytes * 0.95) and 0 < allocated_size < int(logical_size * 0.8):
        return allocated_size
    return logical_size


class _HFHubFileDownloadMonitor:
    def __init__(
        self,
        *,
        repo_id: str,
        filename: str,
        cache_dir: str,
        total_bytes: int,
        blob_ids: List[str],
        description: str,
        progress_callback: Optional[HFProgressCallback],
    ):
        self.repo_id = str(repo_id or "").strip()
        self.filename = str(filename or "").strip()
        self.cache_dir = str(cache_dir or "").strip()
        self.total_bytes = max(0, int(total_bytes or 0))
        self.blob_ids = [str(item or "").strip() for item in (blob_ids or []) if str(item or "").strip()]
        self.description = str(description or self.filename or "Downloading model file").strip()
        self.progress_callback = progress_callback
        self.repo_dir = _hf_cache_repo_dir(self.cache_dir, self.repo_id)
        self.started_at = time.time()
        self.event_id = f"cache-monitor:{self.repo_id}:{self.filename}"
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.last_completed = 0
        self.last_ts = self.started_at
        self.last_rate = 0.0
        self.last_increase_ts = self.started_at

    def start(self) -> None:
        if not callable(self.progress_callback) or self.total_bytes <= 0:
            return
        self.thread = threading.Thread(target=self._run, name="hf-download-progress-monitor", daemon=True)
        self.thread.start()

    def stop(self, *, completed: bool = False) -> None:
        self.stop_event.set()
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=1.5)
        if completed and self.total_bytes > 0:
            try:
                self._emit(self.total_bytes, event="complete", rate=0.0)
            except HfLlmDownloadCancelled:
                pass

    def _candidate_paths(self) -> List[Path]:
        paths: List[Path] = []
        blobs_dir = self.repo_dir / "blobs"
        for blob_id in self.blob_ids:
            for suffix in ("", ".incomplete"):
                path = blobs_dir / f"{blob_id}{suffix}"
                if path not in paths:
                    paths.append(path)
        snapshots_dir = self.repo_dir / "snapshots"
        if self.filename and snapshots_dir.exists():
            try:
                target_suffix = "/" + self.filename.replace("\\", "/")
                for path in snapshots_dir.rglob(Path(self.filename).name):
                    try:
                        if path.as_posix().endswith(target_suffix) and path not in paths:
                            paths.append(path)
                    except Exception:
                        continue
            except Exception:
                pass
        return paths

    def _generic_recent_size(self) -> int:
        if not self.repo_dir.exists():
            return 0
        newest_size = 0
        cutoff = self.started_at - 5.0
        try:
            for path in self.repo_dir.rglob("*"):
                try:
                    if not path.is_file():
                        continue
                    stat = path.stat()
                    if float(getattr(stat, "st_mtime", 0.0) or 0.0) < cutoff:
                        continue
                    size = _download_stat_size(path, total_bytes=self.total_bytes)
                    if self.total_bytes > 0:
                        size = min(size, self.total_bytes)
                    newest_size = max(newest_size, size)
                except Exception:
                    continue
        except Exception:
            return newest_size
        return newest_size

    def _completed_bytes(self) -> int:
        sizes: List[int] = []
        for path in self._candidate_paths():
            size = _download_stat_size(path, total_bytes=self.total_bytes)
            if size > 0:
                sizes.append(size)
        completed = max(sizes) if sizes else self._generic_recent_size()
        if self.total_bytes > 0:
            completed = min(completed, self.total_bytes)
        return max(0, int(completed))

    def _emit(self, completed: int, *, event: str = "update", rate: Optional[float] = None) -> None:
        total = self.total_bytes
        completed = max(0, min(int(completed), total if total > 0 else int(completed)))
        rate_value = max(0.0, float(rate if rate is not None else self.last_rate))
        eta_seconds = ((total - completed) / rate_value) if total > completed and rate_value > 0.0 else 0.0
        progress = (completed / total * 100.0) if total > 0 else 0.0
        _emit_hf_llm_progress(
            self.progress_callback,
            {
                "event": event,
                "stage": "download",
                "id": self.event_id,
                "source": "cache_monitor",
                "description": self.description,
                "unit": "B",
                "completed": completed,
                "total": total,
                "rate": rate_value,
                "elapsed": max(0.0, time.time() - self.started_at),
                "eta_seconds": eta_seconds,
                "progress": progress,
            },
        )

    def _run(self) -> None:
        interval = max(0.25, min(2.0, float(os.getenv("TATER_HF_DOWNLOAD_MONITOR_INTERVAL_SEC", "0.5") or 0.5)))
        while not self.stop_event.wait(interval):
            now = time.time()
            completed = self._completed_bytes()
            if completed > self.last_completed:
                elapsed = max(0.001, now - self.last_ts)
                raw_rate = float(completed - self.last_completed) / elapsed
                self.last_rate = raw_rate if self.last_rate <= 0.0 else (self.last_rate * 0.65) + (raw_rate * 0.35)
                self.last_completed = completed
                self.last_increase_ts = now
            elif now - self.last_increase_ts > 30.0:
                self.last_rate = 0.0
            self.last_ts = now
            try:
                self._emit(self.last_completed)
            except HfLlmDownloadCancelled:
                self.stop_event.set()
                return


def _terminate_process(proc: Any, *, timeout: float = 3.0, process_group: bool = False) -> None:
    if proc is None:
        return
    try:
        if proc.poll() is not None:
            return
    except Exception:
        return
    if process_group and os.name != "nt":
        try:
            os.killpg(int(proc.pid), signal.SIGTERM)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass
    else:
        try:
            proc.terminate()
        except Exception:
            pass
    try:
        proc.wait(timeout=timeout)
        return
    except Exception:
        pass
    if process_group and os.name != "nt":
        try:
            os.killpg(int(proc.pid), signal.SIGKILL)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    else:
        try:
            proc.kill()
        except Exception:
            pass
    try:
        proc.wait(timeout=1.0)
    except Exception:
        pass


def _purge_hf_cached_file(cache_dir: str, repo_id: str, filename: str, blob_ids: List[str]) -> List[str]:
    repo_dir = _hf_cache_repo_dir(cache_dir, repo_id)
    removed: List[str] = []
    candidates: List[Path] = []
    blobs_dir = repo_dir / "blobs"
    for blob_id in [str(item or "").strip() for item in (blob_ids or []) if str(item or "").strip()]:
        candidates.append(blobs_dir / blob_id)
        candidates.append(blobs_dir / f"{blob_id}.incomplete")
        lock_dir = Path(str(cache_dir or "")).expanduser() / ".locks" / f"models--{str(repo_id or '').strip().replace('/', '--')}"
        candidates.append(lock_dir / f"{blob_id}.lock")

    target = str(filename or "").replace("\\", "/").strip()
    snapshots_dir = repo_dir / "snapshots"
    if target and snapshots_dir.exists():
        try:
            for snapshot_dir in snapshots_dir.iterdir():
                candidates.append(snapshot_dir / target)
        except Exception:
            pass

    if repo_dir.exists():
        try:
            for path in repo_dir.rglob("*.incomplete"):
                candidates.append(path)
        except Exception:
            pass

    seen: set[str] = set()
    for path in candidates:
        try:
            token = str(path)
            if token in seen:
                continue
            seen.add(token)
            if path.exists() or path.is_symlink():
                path.unlink()
                removed.append(token)
        except Exception:
            continue
    return removed


def _purge_hf_cached_snapshot(cache_dir: str, repo_id: str) -> List[str]:
    root = Path(str(cache_dir or "")).expanduser()
    repo_dir = _hf_cache_repo_dir(str(root), repo_id)
    lock_dir = root / ".locks" / f"models--{str(repo_id or '').strip().replace('/', '--')}"
    removed: List[str] = []

    try:
        root_resolved = root.resolve()
    except Exception:
        root_resolved = root

    for path in (repo_dir, lock_dir):
        try:
            target = path.resolve()
            if root_resolved not in [target, *target.parents]:
                continue
            if path.exists() or path.is_symlink():
                if path.is_dir() and not path.is_symlink():
                    shutil.rmtree(path)
                else:
                    path.unlink()
                removed.append(str(path))
        except Exception:
            continue
    return removed


def _read_worker_json_lines(stream: Any, out_queue: "queue.Queue[str]") -> None:
    if stream is None:
        return
    try:
        for line in stream:
            if line:
                out_queue.put(str(line))
    except Exception:
        return


def _drain_snapshot_worker_lines(
    line_queue: "queue.Queue[str]",
    progress_callback: Optional[HFProgressCallback],
) -> Tuple[str, str]:
    result_path = ""
    worker_error = ""
    while True:
        try:
            raw_line = line_queue.get_nowait()
        except queue.Empty:
            break
        raw_line = str(raw_line or "").strip()
        if not raw_line:
            continue
        try:
            payload = json.loads(raw_line)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        payload_type = str(payload.get("type") or "").strip().lower()
        if payload_type == "progress":
            event = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
            _emit_hf_llm_progress(progress_callback, event)
        elif payload_type == "result":
            result_path = str(payload.get("path") or "").strip()
        elif payload_type == "error":
            worker_error = str(payload.get("error") or "").strip()
    return result_path, worker_error


def _snapshot_download_with_progress_worker(
    *,
    kwargs: Dict[str, Any],
    progress_callback: Optional[HFProgressCallback],
    description: str,
) -> str:
    repo_id = str((kwargs or {}).get("repo_id") or "").strip()
    cache_dir = str((kwargs or {}).get("cache_dir") or "").strip()
    if not callable(progress_callback) or not _boolish(os.getenv("TATER_HF_DOWNLOAD_WORKER"), default=True):
        from huggingface_hub import snapshot_download  # type: ignore

        direct_kwargs = dict(kwargs or {})
        tqdm_class = _make_hf_snapshot_tqdm_class(progress_callback)
        if tqdm_class is not None:
            direct_kwargs["tqdm_class"] = tqdm_class
        try:
            return str(snapshot_download(**direct_kwargs))
        except TypeError as exc:
            if "tqdm_class" not in direct_kwargs:
                raise
            logger.debug("huggingface_hub snapshot_download rejected tqdm_class; retrying without progress hook: %s", exc)
            direct_kwargs.pop("tqdm_class", None)
            return str(snapshot_download(**direct_kwargs))

    worker_kwargs = dict(kwargs or {})
    token_value = str(worker_kwargs.pop("token", "") or "").strip()
    worker_payload = json.dumps({"kwargs": worker_kwargs}, separators=(",", ":"))
    worker_code = r'''
import json
import os
import sys
import uuid

from huggingface_hub import snapshot_download

try:
    from tqdm.auto import tqdm as base_tqdm
except Exception:
    base_tqdm = None


def emit(payload):
    print(json.dumps(payload, separators=(",", ":")), flush=True)


if base_tqdm is not None:
    class TaterSnapshotTqdm(base_tqdm):
        def __init__(self, *args, **kwargs):
            self._tater_progress_id = uuid.uuid4().hex
            super().__init__(*args, **kwargs)
            self._emit_tater_progress("start")

        def _emit_tater_progress(self, event):
            try:
                fmt = getattr(self, "format_dict", {}) or {}
            except Exception:
                fmt = {}
            try:
                total = float(getattr(self, "total", None) or fmt.get("total") or 0.0)
            except Exception:
                total = 0.0
            try:
                completed = float(getattr(self, "n", None) or fmt.get("n") or 0.0)
            except Exception:
                completed = 0.0
            try:
                rate = float(fmt.get("rate") or 0.0)
            except Exception:
                rate = 0.0
            try:
                elapsed = float(fmt.get("elapsed") or 0.0)
            except Exception:
                elapsed = 0.0
            eta_seconds = ((total - completed) / rate) if total > completed and rate > 0.0 else 0.0
            progress = (completed / total * 100.0) if total > 0.0 else 0.0
            emit({
                "type": "progress",
                "payload": {
                    "event": event,
                    "stage": "download",
                    "id": self._tater_progress_id,
                    "source": "worker_tqdm",
                    "description": str(getattr(self, "desc", "") or fmt.get("prefix") or "").strip(),
                    "unit": str(fmt.get("unit") or "").strip(),
                    "completed": completed,
                    "total": total,
                    "rate": rate,
                    "elapsed": elapsed,
                    "eta_seconds": eta_seconds,
                    "progress": progress,
                },
            })

        def update(self, n=1):
            result = super().update(n)
            self._emit_tater_progress("update")
            return result

        def close(self):
            self._emit_tater_progress("close")
            return super().close()


try:
    config = json.loads(sys.argv[1])
    kwargs = dict(config.get("kwargs") or {})
    token = os.environ.get("HF_TOKEN") or None
    if token and not kwargs.get("token"):
        kwargs["token"] = token
    if base_tqdm is not None:
        kwargs["tqdm_class"] = TaterSnapshotTqdm
    try:
        path = snapshot_download(**kwargs)
    except TypeError:
        if "tqdm_class" not in kwargs:
            raise
        kwargs.pop("tqdm_class", None)
        path = snapshot_download(**kwargs)
    emit({"type": "result", "path": path})
except Exception as exc:
    emit({"type": "error", "error": str(exc) or type(exc).__name__})
    raise
'''
    env = os.environ.copy()
    if token_value:
        env["HF_TOKEN"] = token_value
    else:
        env.pop("HF_TOKEN", None)
    proc = subprocess.Popen(
        [sys.executable, "-c", worker_code, worker_payload],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        env=env,
        start_new_session=(os.name != "nt"),
    )
    line_queue: "queue.Queue[str]" = queue.Queue()
    reader_thread = threading.Thread(
        target=_read_worker_json_lines,
        args=(proc.stdout, line_queue),
        name="hf-snapshot-worker-reader",
        daemon=True,
    )
    reader_thread.start()
    heartbeat_id = f"snapshot-worker:{repo_id}"
    result_path = ""
    worker_error = ""
    try:
        while True:
            next_path, next_error = _drain_snapshot_worker_lines(line_queue, progress_callback)
            if next_path:
                result_path = next_path
            if next_error:
                worker_error = next_error
            return_code = proc.poll()
            if return_code is not None:
                break
            _emit_hf_llm_progress(
                progress_callback,
                {
                    "event": "heartbeat",
                    "stage": "download",
                    "id": heartbeat_id,
                    "source": "worker",
                    "description": description,
                    "progress": 0.0,
                },
            )
            time.sleep(0.25)

        next_path, next_error = _drain_snapshot_worker_lines(line_queue, progress_callback)
        if next_path:
            result_path = next_path
        if next_error:
            worker_error = next_error

        if return_code != 0:
            _emit_hf_llm_progress(
                progress_callback,
                {
                    "event": "error",
                    "stage": "download",
                    "id": heartbeat_id,
                    "source": "worker",
                    "description": description,
                    "progress": 0.0,
                },
            )
            raise RuntimeError(worker_error or f"Hugging Face snapshot download failed for {repo_id} with exit code {return_code}.")

        if result_path:
            return result_path

        from huggingface_hub import snapshot_download  # type: ignore

        local_kwargs = dict(kwargs or {})
        local_kwargs["local_files_only"] = True
        return str(snapshot_download(**local_kwargs))
    except HfLlmDownloadCancelled:
        _terminate_process(proc, process_group=True)
        removed = _purge_hf_cached_snapshot(cache_dir, repo_id)
        logger.info("[hf-download] cancelled snapshot %s removed_paths=%s", repo_id, len(removed))
        raise
    finally:
        try:
            if proc.stdout is not None:
                proc.stdout.close()
        except Exception:
            pass
        if reader_thread.is_alive():
            reader_thread.join(timeout=1.0)


def _hf_hub_download_with_progress_worker(
    *,
    repo_id: str,
    filename: str,
    cache_dir: str,
    token: Optional[str],
    progress_callback: Optional[HFProgressCallback],
    description: str,
    total_bytes: int,
    blob_ids: List[str],
) -> str:
    if not callable(progress_callback) or not _boolish(os.getenv("TATER_HF_DOWNLOAD_WORKER"), default=True):
        try:
            from huggingface_hub import hf_hub_download  # type: ignore
        except Exception as exc:
            raise RuntimeError("Hugging Face downloads need huggingface_hub installed.") from exc
        kwargs: Dict[str, Any] = {
            "repo_id": repo_id,
            "filename": filename,
            "cache_dir": cache_dir,
        }
        if token:
            kwargs["token"] = token
        tqdm_class = _make_hf_snapshot_tqdm_class(progress_callback)
        if tqdm_class is not None:
            kwargs["tqdm_class"] = tqdm_class
        try:
            return str(hf_hub_download(**kwargs))
        except TypeError as exc:
            if "tqdm_class" not in kwargs:
                raise
            logger.debug("huggingface_hub hf_hub_download rejected tqdm_class; retrying without progress hook: %s", exc)
            kwargs.pop("tqdm_class", None)
            return str(hf_hub_download(**kwargs))

    monitor = _HFHubFileDownloadMonitor(
        repo_id=repo_id,
        filename=filename,
        cache_dir=cache_dir,
        total_bytes=total_bytes,
        blob_ids=blob_ids,
        description=description,
        progress_callback=progress_callback,
    )
    monitor.start()
    worker_code = (
        "import json, os, sys\n"
        "from huggingface_hub import hf_hub_download\n"
        "token = os.environ.get('HF_TOKEN') or None\n"
        "path = hf_hub_download(repo_id=sys.argv[1], filename=sys.argv[2], cache_dir=sys.argv[3], token=token)\n"
        "print(json.dumps({'path': path}), flush=True)\n"
    )
    env = os.environ.copy()
    if token:
        env["HF_TOKEN"] = str(token)
    else:
        env.pop("HF_TOKEN", None)
    proc = subprocess.Popen(
        [sys.executable, "-c", worker_code, repo_id, filename, cache_dir],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
        start_new_session=(os.name != "nt"),
    )
    path = ""
    heartbeat_id = f"worker:{repo_id}:{filename}"
    try:
        while True:
            return_code = proc.poll()
            if return_code is not None:
                break
            _emit_hf_llm_progress(
                progress_callback,
                {
                    "event": "heartbeat",
                    "stage": "download",
                    "id": heartbeat_id,
                    "source": "worker",
                    "description": description,
                    "progress": 0.0,
                },
            )
            time.sleep(0.25)

        if return_code != 0:
            _emit_hf_llm_progress(
                progress_callback,
                {
                    "event": "error",
                    "stage": "download",
                    "id": heartbeat_id,
                    "source": "worker",
                    "description": description,
                    "progress": 0.0,
                },
            )
            raise RuntimeError(f"Hugging Face download process failed for {repo_id}/{filename} with exit code {return_code}.")

        try:
            from huggingface_hub import hf_hub_download  # type: ignore
        except Exception as exc:
            raise RuntimeError("Hugging Face downloads need huggingface_hub installed.") from exc
        kwargs: Dict[str, Any] = {
            "repo_id": repo_id,
            "filename": filename,
            "cache_dir": cache_dir,
            "local_files_only": True,
        }
        if token:
            kwargs["token"] = token
        path = str(hf_hub_download(**kwargs))
        return path
    except HfLlmDownloadCancelled:
        _terminate_process(proc, process_group=True)
        removed = _purge_hf_cached_file(cache_dir, repo_id, filename, blob_ids)
        logger.info("[hf-download] cancelled %s/%s removed_files=%s", repo_id, filename, len(removed))
        raise
    finally:
        monitor.stop(completed=bool(path and os.path.exists(str(path))))


def _make_hf_snapshot_tqdm_class(progress_callback: Optional[HFProgressCallback]) -> Any:
    if not callable(progress_callback):
        return None
    try:
        from tqdm.auto import tqdm as base_tqdm  # type: ignore
    except Exception:
        return None

    class TaterHFSnapshotTqdm(base_tqdm):
        def __init__(self, *args, **kwargs):
            self._tater_progress_id = uuid.uuid4().hex
            super().__init__(*args, **kwargs)
            self._emit_tater_progress("start")

        def _emit_tater_progress(self, event: str) -> None:
            try:
                fmt = getattr(self, "format_dict", {}) or {}
            except Exception:
                fmt = {}
            try:
                total = float(getattr(self, "total", None) or fmt.get("total") or 0.0)
            except Exception:
                total = 0.0
            try:
                completed = float(getattr(self, "n", None) or fmt.get("n") or 0.0)
            except Exception:
                completed = 0.0
            try:
                rate = float(fmt.get("rate") or 0.0)
            except Exception:
                rate = 0.0
            try:
                elapsed = float(fmt.get("elapsed") or 0.0)
            except Exception:
                elapsed = 0.0
            eta_seconds = ((total - completed) / rate) if total > completed and rate > 0.0 else 0.0
            progress = (completed / total * 100.0) if total > 0.0 else 0.0
            _emit_hf_llm_progress(
                progress_callback,
                {
                    "event": event,
                    "stage": "download",
                    "id": self._tater_progress_id,
                    "source": "tqdm",
                    "description": str(getattr(self, "desc", "") or fmt.get("prefix") or "").strip(),
                    "unit": str(fmt.get("unit") or "").strip(),
                    "completed": completed,
                    "total": total,
                    "rate": rate,
                    "elapsed": elapsed,
                    "eta_seconds": eta_seconds,
                    "progress": progress,
                },
            )

        def update(self, n=1):
            result = super().update(n)
            self._emit_tater_progress("update")
            return result

        def close(self):
            self._emit_tater_progress("close")
            return super().close()

    return TaterHFSnapshotTqdm


def _download_hf_llm_snapshot(
    model_id: str,
    *,
    model_root: str,
    progress_callback: Optional[HFProgressCallback] = None,
) -> None:
    _emit_hf_llm_progress(
        progress_callback,
        {
            "event": "start",
            "stage": "download",
            "description": "Resolving model repository",
            "progress": 0.0,
        },
    )
    try:
        from huggingface_hub import snapshot_download  # type: ignore
    except Exception:
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "skip",
                "stage": "download",
                "description": "huggingface_hub is unavailable; Transformers will download files while loading",
                "progress": 0.0,
            },
        )
        return

    kwargs: Dict[str, Any] = {
        "repo_id": str(model_id or "").strip(),
        "cache_dir": model_root,
        "max_workers": _hf_llm_snapshot_max_workers(),
        "ignore_patterns": [
            "*.gguf",
            "*.onnx",
            "*.tflite",
            "*.h5",
            "*.ot",
            "*.msgpack",
            "*.mlmodel",
            "onnx/*",
            "*/onnx/*",
        ],
    }
    token = _hf_llm_hub_token()
    if token:
        kwargs["token"] = token
    _ = snapshot_download
    _snapshot_download_with_progress_worker(
        kwargs=kwargs,
        progress_callback=progress_callback,
        description="Downloading model files",
    )
    _emit_hf_llm_progress(
        progress_callback,
        {
            "event": "complete",
            "stage": "download",
            "description": "Model files are cached",
            "progress": 100.0,
        },
    )


def _hf_model_config_supports_vision(config: Any) -> bool:
    if config is None:
        return False
    for attr in ("vision_config", "image_token_id", "mm_vision_tower", "vision_tower"):
        try:
            if getattr(config, attr, None) is not None:
                return True
        except Exception:
            pass
    try:
        model_type = str(getattr(config, "model_type", "") or "").lower()
        architectures = " ".join(str(item or "") for item in getattr(config, "architectures", []) or []).lower()
    except Exception:
        model_type = ""
        architectures = ""
    return any(token in f"{model_type} {architectures}" for token in ("vision", "vl", "vla", "image", "multimodal"))


def _hf_processor_supports_vision(processor: Any) -> bool:
    if processor is None:
        return False
    for attr in ("image_processor", "image_processor_class", "vision_processor"):
        try:
            if getattr(processor, attr, None) is not None:
                return True
        except Exception:
            pass
    try:
        name = str(type(processor).__name__).lower()
        return any(token in name for token in ("image", "vision", "vl", "multimodal"))
    except Exception:
        return False


def _gguf_read_u32(handle) -> int:
    data = handle.read(4)
    if len(data) != 4:
        raise EOFError("short gguf u32")
    return int.from_bytes(data, "little", signed=False)


def _gguf_read_u64(handle) -> int:
    data = handle.read(8)
    if len(data) != 8:
        raise EOFError("short gguf u64")
    return int.from_bytes(data, "little", signed=False)


def _gguf_read_i64(handle) -> int:
    data = handle.read(8)
    if len(data) != 8:
        raise EOFError("short gguf i64")
    return int.from_bytes(data, "little", signed=True)


def _gguf_read_string(handle) -> str:
    length = _gguf_read_u64(handle)
    if length > _llama_cpp_chat_template_max_chars():
        raise ValueError("gguf string too large")
    return handle.read(int(length)).decode("utf-8", errors="replace")


def _gguf_skip_value(handle, value_type: int) -> None:
    scalar_sizes = {
        0: 1,
        1: 1,
        2: 2,
        3: 2,
        4: 4,
        5: 4,
        6: 4,
        7: 1,
        10: 8,
        11: 8,
        12: 8,
    }
    if value_type in scalar_sizes:
        handle.seek(scalar_sizes[value_type], os.SEEK_CUR)
    elif value_type == 8:
        length = _gguf_read_u64(handle)
        handle.seek(int(length), os.SEEK_CUR)
    elif value_type == 9:
        item_type = _gguf_read_u32(handle)
        count = _gguf_read_u64(handle)
        if count > 1_000_000:
            raise ValueError("gguf array too large")
        for _ in range(int(count)):
            _gguf_skip_value(handle, item_type)
    else:
        raise ValueError(f"unknown gguf metadata type {value_type}")


def _gguf_read_metadata_value(handle, value_type: int) -> Any:
    if value_type == 0:
        return int.from_bytes(handle.read(1), "little", signed=False)
    if value_type == 1:
        return int.from_bytes(handle.read(1), "little", signed=True)
    if value_type == 2:
        return int.from_bytes(handle.read(2), "little", signed=False)
    if value_type == 3:
        return int.from_bytes(handle.read(2), "little", signed=True)
    if value_type == 4:
        return _gguf_read_u32(handle)
    if value_type == 5:
        return int.from_bytes(handle.read(4), "little", signed=True)
    if value_type == 7:
        return bool(int.from_bytes(handle.read(1), "little", signed=False))
    if value_type == 8:
        return _gguf_read_string(handle)
    if value_type == 10:
        return _gguf_read_u64(handle)
    if value_type == 11:
        return _gguf_read_i64(handle)
    _gguf_skip_value(handle, value_type)
    return None


def _read_gguf_metadata(path: Any, *, key_prefixes: Tuple[str, ...] = (), keys: Tuple[str, ...] = ()) -> Dict[str, Any]:
    file_path = Path(str(path or "")).expanduser()
    if not file_path.exists() or not file_path.is_file() or file_path.suffix.lower() != ".gguf":
        return {}
    wanted_keys = {str(key or "") for key in keys if str(key or "")}
    wanted_prefixes = tuple(str(prefix or "") for prefix in key_prefixes if str(prefix or ""))
    out: Dict[str, Any] = {}
    try:
        with file_path.open("rb") as handle:
            if handle.read(4) != b"GGUF":
                return {}
            _version = _gguf_read_u32(handle)
            _tensor_count = _gguf_read_u64(handle)
            metadata_count = min(_gguf_read_u64(handle), 20000)
            for _ in range(int(metadata_count)):
                key = _gguf_read_string(handle)
                value_type = _gguf_read_u32(handle)
                wanted = (key in wanted_keys) or any(key.startswith(prefix) for prefix in wanted_prefixes)
                if wanted:
                    out[key] = _gguf_read_metadata_value(handle, value_type)
                else:
                    _gguf_skip_value(handle, value_type)
    except Exception as exc:
        logger.debug("[llama-cpp] failed reading GGUF metadata from %s: %s", file_path, exc)
        return out
    return out


def read_llama_cpp_gguf_chat_templates(path: Any) -> Dict[str, str]:
    metadata = _read_gguf_metadata(
        path,
        key_prefixes=("tokenizer.chat_template",),
        keys=("general.name",),
    )
    templates: Dict[str, str] = {}
    for key, value in metadata.items():
        if not key.startswith("tokenizer.chat_template"):
            continue
        template = str(value or "")
        if not template.strip():
            continue
        name = "chat_template.default" if key == "tokenizer.chat_template" else key[len("tokenizer.") :]
        templates[name] = template
    return templates


def _chat_template_entries_from_value(value: Any, name: str = "chat_template") -> Dict[str, str]:
    templates: Dict[str, str] = {}
    if isinstance(value, str):
        if value.strip():
            templates[name] = value
        return templates
    if isinstance(value, list):
        for index, item in enumerate(value):
            item_name = f"{name}.{index + 1}"
            if isinstance(item, dict):
                raw_name = str(item.get("name") or item.get("template_name") or "").strip()
                item_template = item.get("template") or item.get("chat_template")
                templates.update(_chat_template_entries_from_value(item_template, raw_name or item_name))
            else:
                templates.update(_chat_template_entries_from_value(item, item_name))
        return templates
    if isinstance(value, dict):
        direct = value.get("chat_template") if "chat_template" in value else value.get("template")
        if direct is not None:
            templates.update(_chat_template_entries_from_value(direct, name))
        for key, item in value.items():
            key_text = str(key or "").strip()
            if not key_text or key_text in {"chat_template", "template"}:
                continue
            if isinstance(item, str) and ("template" in key_text.lower() or key_text.lower() in {"default", "tool_use"}):
                templates.update(_chat_template_entries_from_value(item, f"{name}.{key_text}"))
            elif isinstance(item, (dict, list)) and ("template" in key_text.lower() or key_text.lower() in {"default", "tool_use", "templates"}):
                templates.update(_chat_template_entries_from_value(item, f"{name}.{key_text}"))
        return templates
    return templates


def read_local_llm_repo_chat_templates(path: Any) -> Dict[str, str]:
    raw_path = str(path or "").strip()
    if not raw_path:
        return {}
    root = Path(raw_path).expanduser()
    if root.is_file():
        root = root.parent
    if not root.is_dir():
        return {}
    text_template_files = (
        "chat_template.jinja",
        "chat_template.j2",
        "chat_template.tpl",
        "chat_template.txt",
    )
    files = (
        "tokenizer_config.json",
        "chat_template.json",
        "processor_config.json",
        "preprocessor_config.json",
        "config.json",
    )
    templates: Dict[str, str] = {}
    for filename in text_template_files:
        candidate = root / filename
        if not candidate.is_file():
            continue
        try:
            if candidate.stat().st_size > 2_000_000:
                continue
            template = candidate.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if template.strip():
            templates[filename] = template
    for filename in files:
        candidate = root / filename
        if not candidate.is_file():
            continue
        try:
            if candidate.stat().st_size > 2_000_000:
                continue
            payload = json.loads(candidate.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            continue
        file_templates = _chat_template_entries_from_value(payload, filename)
        for key, template in file_templates.items():
            clean = str(template or "")
            if clean.strip():
                templates[key] = clean
    return templates


def get_llama_cpp_chat_template_info(model_id: Any, *, model_path: Any = "") -> Dict[str, Any]:
    return get_local_llm_chat_template_info(
        HYDRA_LLM_PROVIDER_LLAMA_CPP,
        model_id,
        model_path=model_path,
    )


def get_local_llm_chat_template_info(provider: Any, model_id: Any, *, model_path: Any = "") -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model_id or "").strip()
    path = str(model_path or "").strip()
    if not path:
        if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP:
            try:
                ref = _parse_llama_cpp_model_ref(model_token)
                if ref.get("kind") == "local":
                    path = str(ref.get("path") or "").strip()
            except Exception:
                path = ""
        else:
            expanded = Path(model_token).expanduser()
            if expanded.exists():
                path = str(expanded)
    templates = (
        read_llama_cpp_gguf_chat_templates(path)
        if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP and path
        else read_local_llm_repo_chat_templates(path)
        if path
        else {}
    )
    embedded_template = str(
        templates.get("chat_template.default")
        or templates.get("tokenizer_config.json")
        or templates.get("chat_template.json")
        or ""
    )
    if not embedded_template and templates:
        embedded_template = str(next(iter(templates.values())) or "")
    override = get_local_llm_chat_template_override(provider_token, model_token)
    override_template = str(override.get("template") or "")
    effective_template = override_template or embedded_template
    chat_format = str(os.getenv("TATER_LLAMA_CPP_CHAT_FORMAT") or "").strip() if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP else ""
    embedded_source = "gguf" if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP else "embedded"
    source = "override" if override_template else (embedded_source if embedded_template else ("chat_format" if chat_format else "fallback"))
    capabilities = _llama_cpp_chat_template_capabilities(effective_template)
    return {
        "provider": provider_token,
        "model": model_token,
        "model_path": path,
        "chat_format": chat_format,
        "source": source,
        "has_override": bool(override_template),
        "override_active": bool(override_template),
        "override_template": override_template,
        "override_template_chars": len(override_template),
        "embedded_template": embedded_template,
        "embedded_template_chars": len(embedded_template),
        "effective_template": effective_template,
        "effective_template_chars": len(effective_template),
        "max_chars": _llama_cpp_chat_template_max_chars(),
        "template_names": sorted(templates.keys()),
        "updated_ts": float(override.get("updated_ts") or 0.0),
        "capabilities": capabilities,
        **capabilities,
        "no_thinking_supported": bool(
            capabilities.get("enable_thinking")
            or capabilities.get("reasoning_budget")
            or capabilities.get("no_think_marker")
        ),
    }


def _set_chat_template_on_target(target: Any, template: str) -> bool:
    if target is None or not str(template or "").strip():
        return False
    if isinstance(target, dict):
        target["chat_template"] = template
        return True
    applied = False
    try:
        setattr(target, "chat_template", template)
        applied = True
    except Exception:
        pass
    for attr in ("tokenizer", "_tokenizer"):
        try:
            nested = getattr(target, attr, None)
        except Exception:
            nested = None
        if nested is not None and nested is not target:
            applied = _set_chat_template_on_target(nested, template) or applied
    return applied


def _install_local_llm_chat_template_override(
    provider: Any,
    model_id: Any,
    *,
    tokenizer: Any = None,
    processor: Any = None,
    config: Any = None,
    model: Any = None,
) -> Tuple[bool, str, str]:
    provider_token = _normalize_hydra_llm_provider(provider)
    template = _local_llm_chat_template_override_text(provider_token, model_id)
    if not template:
        return False, "", ""
    targets: List[Tuple[str, Any]] = [
        ("tokenizer", tokenizer),
        ("processor", processor),
        ("config", config),
    ]
    try:
        model_config = getattr(model, "config", None)
    except Exception:
        model_config = None
    if model_config is not None:
        targets.append(("model.config", model_config))
    applied_names: List[str] = []
    seen: set[int] = set()
    for name, target in targets:
        if target is None:
            continue
        ident = id(target)
        if ident in seen:
            continue
        seen.add(ident)
        if _set_chat_template_on_target(target, template):
            applied_names.append(name)
    if not applied_names:
        return True, "", "Chat template override is saved, but no runtime tokenizer/processor target accepted it."
    return True, ",".join(applied_names), ""


def _parse_llama_cpp_model_ref(model_id: str) -> Dict[str, str]:
    raw = str(model_id or "").strip()
    if not raw:
        raise RuntimeError("llama.cpp GGUF model id or path is required.")
    expanded = os.path.abspath(os.path.expanduser(raw))
    if raw.endswith(".gguf") and (os.path.exists(expanded) or raw.startswith(("/", "./", "../", "~"))):
        return {"kind": "local", "path": expanded}
    if raw.startswith("file://"):
        path = os.path.abspath(os.path.expanduser(raw[len("file://") :]))
        return {"kind": "local", "path": path}
    if raw.startswith("hf://"):
        raw = raw[len("hf://") :]
    filename = ""
    repo_id = raw
    for sep in ("::", "#"):
        if sep in raw:
            left, right = raw.split(sep, 1)
            repo_id = left.strip()
            filename = right.strip()
            if filename.startswith("filename="):
                filename = filename.split("=", 1)[1].strip()
            break
    if not filename and raw.lower().endswith(".gguf"):
        parts = raw.split("/")
        if len(parts) >= 3:
            repo_id = "/".join(parts[:2])
            filename = "/".join(parts[2:])
    return {
        "kind": "hf",
        "repo_id": repo_id.strip(),
        "filename": filename.strip(),
    }


def _llama_cpp_select_gguf_filename(repo_id: str) -> str:
    preferred_filename = str(os.getenv("TATER_LLAMA_CPP_GGUF_FILENAME") or "").strip()
    try:
        from huggingface_hub import HfApi  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "llama.cpp GGUF auto-download needs huggingface_hub installed, or use repo_id::filename.gguf."
        ) from exc

    token = _hf_llm_hub_token()
    try:
        files = HfApi(token=token).list_repo_files(repo_id=str(repo_id or "").strip(), repo_type="model")
    except Exception as exc:
        raise RuntimeError(f"Could not list Hugging Face files for {repo_id}: {exc}") from exc
    gguf_files = [
        str(path or "")
        for path in files
        if str(path or "").lower().endswith(".gguf") and "mmproj" not in str(path or "").lower()
    ]
    if not gguf_files:
        raise RuntimeError(f"No GGUF files found in Hugging Face repo {repo_id}.")
    if preferred_filename:
        for path in gguf_files:
            if path == preferred_filename or path.endswith(f"/{preferred_filename}"):
                return path
        raise RuntimeError(f"Preferred GGUF file {preferred_filename} was not found in {repo_id}.")
    if len(gguf_files) == 1:
        return gguf_files[0]

    def _score(path: str) -> Tuple[int, int, str]:
        upper = os.path.basename(path).upper()
        for index, quant in enumerate(_llama_cpp_preferred_quants()):
            if quant and quant in upper:
                return (index, len(path), path)
        return (999, len(path), path)

    return sorted(gguf_files, key=_score)[0]


def _llama_cpp_mmproj_candidates(files: List[str]) -> List[str]:
    rows = [
        str(path or "").strip()
        for path in files
        if str(path or "").strip().lower().endswith(".gguf") and "mmproj" in str(path or "").strip().lower()
    ]
    return sorted(rows, key=lambda path: (len(path), path.lower()))


def _local_gguf_quant_label(path: str) -> str:
    name = os.path.basename(str(path or "")).upper()
    for quant in ("Q2_K", "Q3_K_M", "Q4_K_M", "Q4_K_S", "Q5_K_M", "Q5_K_S", "Q6_K", "Q8_0", "F16", "BF16"):
        if quant in name:
            return quant
    return ""


def _llama_cpp_select_mmproj_filename(repo_id: str, model_filename: str = "") -> str:
    preferred_filename = str(os.getenv("TATER_LLAMA_CPP_MMPROJ_FILENAME") or "").strip()
    try:
        from huggingface_hub import HfApi  # type: ignore
    except Exception as exc:
        raise RuntimeError("llama.cpp vision auto-download needs huggingface_hub installed.") from exc

    token = _hf_llm_hub_token()
    try:
        files = HfApi(token=token).list_repo_files(repo_id=str(repo_id or "").strip(), repo_type="model")
    except Exception as exc:
        raise RuntimeError(f"Could not list Hugging Face files for {repo_id}: {exc}") from exc
    mmproj_files = _llama_cpp_mmproj_candidates([str(item or "") for item in files])
    if not mmproj_files:
        return ""
    if preferred_filename:
        for path in mmproj_files:
            if path == preferred_filename or path.endswith(f"/{preferred_filename}"):
                return path
        raise RuntimeError(f"Preferred mmproj file {preferred_filename} was not found in {repo_id}.")

    model_base = os.path.basename(str(model_filename or "")).lower()
    model_quant = _local_gguf_quant_label(model_base)

    def _score(path: str) -> Tuple[int, int, str]:
        lower = os.path.basename(path).lower()
        quant = _local_gguf_quant_label(path)
        score = 50
        if model_quant and quant == model_quant:
            score = 0
        elif quant in {"F16", "BF16"}:
            score = 5
        elif "f16" in lower or "bf16" in lower:
            score = 5
        elif "q8_0" in lower:
            score = 10
        if model_base:
            stem = re.sub(r"(?i)(?:^mmproj[-_]?|\.gguf$)", "", lower)
            model_stem = re.sub(r"(?i)\.gguf$", "", model_base)
            if stem and (stem in model_stem or model_stem in stem):
                score -= 2
        return (score, len(path), path)

    return sorted(mmproj_files, key=_score)[0]


def _download_llama_cpp_mmproj(
    model_id: str,
    *,
    model_path: str = "",
    progress_callback: Optional[HFProgressCallback] = None,
) -> str:
    ref = _parse_llama_cpp_model_ref(model_id)
    if ref.get("kind") == "local":
        local_path = Path(str(ref.get("path") or model_path or "")).expanduser()
        if not local_path.exists():
            return ""
        candidates = sorted(local_path.parent.glob("*mmproj*.gguf"), key=lambda path: (len(path.name), path.name.lower()))
        return str(candidates[0]) if candidates else ""

    repo_id = str(ref.get("repo_id") or "").strip()
    filename = str(ref.get("filename") or "").strip()
    if not repo_id or "/" not in repo_id:
        return ""
    try:
        mmproj_filename = _llama_cpp_select_mmproj_filename(repo_id, filename)
    except RuntimeError as exc:
        logger.debug("[llama-cpp] no mmproj selected for %s: %s", model_id, exc)
        return ""
    if not mmproj_filename:
        return ""

    try:
        from huggingface_hub import hf_hub_download  # type: ignore
    except Exception as exc:
        raise RuntimeError("llama.cpp vision auto-download needs huggingface_hub installed.") from exc

    model_root = _llama_cpp_model_root()
    os.makedirs(model_root, exist_ok=True)
    _emit_hf_llm_progress(
        progress_callback,
        {
            "event": "start",
            "stage": "download",
            "description": f"Downloading vision projector {mmproj_filename}",
            "progress": 0.0,
        },
    )
    kwargs: Dict[str, Any] = {
        "repo_id": repo_id,
        "filename": mmproj_filename,
        "cache_dir": model_root,
    }
    token = _hf_llm_hub_token()
    if token:
        kwargs["token"] = token
    expected_size, blob_ids = _hf_file_size_and_blob_ids(repo_id, mmproj_filename)
    path = _hf_hub_download_with_progress_worker(
        repo_id=repo_id,
        filename=mmproj_filename,
        cache_dir=model_root,
        token=token,
        progress_callback=progress_callback,
        description=f"Downloading vision projector {mmproj_filename}",
        total_bytes=expected_size,
        blob_ids=blob_ids,
    )
    _emit_hf_llm_progress(
        progress_callback,
        {
            "event": "complete",
            "stage": "download",
            "description": "Vision projector is cached",
            "progress": 100.0,
        },
    )
    return str(path)


def _llama_cpp_make_vision_chat_handler(model_id: str, mmproj_path: str) -> Tuple[Any, str, str]:
    path = str(mmproj_path or "").strip()
    if not path:
        return None, "", ""
    try:
        import llama_cpp.llama_chat_format as chat_format_module  # type: ignore
    except Exception as exc:
        return None, "", f"llama-cpp-python vision chat handlers are unavailable: {exc}"

    lowered = str(model_id or "").lower()
    preferred: List[str] = []
    if "qwen" in lowered and ("2.5" in lowered or "25" in lowered or "vl" in lowered):
        preferred.extend(["Qwen25VLChatHandler", "Qwen2VLChatHandler"])
    if "minicpm" in lowered:
        preferred.append("MiniCPMv26ChatHandler")
    if "moondream" in lowered:
        preferred.append("MoondreamChatHandler")
    if "nanollava" in lowered:
        preferred.append("NanollavaChatHandler")
    if "llava" in lowered and ("1.6" in lowered or "34b" in lowered):
        preferred.append("Llava16ChatHandler")
    if "llava" in lowered:
        preferred.append("Llava15ChatHandler")
    if "llama-3" in lowered and "vision" in lowered:
        preferred.append("Llama3VisionAlphaChatHandler")
    if "gemma" in lowered:
        preferred.extend(["Gemma3ChatHandler", "GemmaVisionChatHandler", "Llava15ChatHandler"])

    fallback = [
        "Qwen25VLChatHandler",
        "MiniCPMv26ChatHandler",
        "Llava16ChatHandler",
        "Llava15ChatHandler",
        "MoondreamChatHandler",
        "NanollavaChatHandler",
        "Llama3VisionAlphaChatHandler",
    ]
    names: List[str] = []
    for name in preferred + fallback:
        if name not in names:
            names.append(name)

    errors: List[str] = []
    for name in names:
        cls = getattr(chat_format_module, name, None)
        if cls is None:
            continue
        for args, kwargs in (
            ((), {"clip_model_path": path}),
            ((), {"mmproj_path": path}),
            ((path,), {}),
        ):
            try:
                return cls(*args, **kwargs), name, ""
            except Exception as exc:
                errors.append(f"{name}: {exc}")
                continue
    return None, "", "; ".join(errors[-3:]) or "No compatible llama-cpp-python vision chat handler was found."


def _download_llama_cpp_model(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> str:
    ref = _parse_llama_cpp_model_ref(model_id)
    if ref.get("kind") == "local":
        path = str(ref.get("path") or "").strip()
        if not os.path.exists(path):
            raise RuntimeError(f"llama.cpp GGUF file does not exist: {path}")
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "complete",
                "stage": "download",
                "description": "Using local GGUF file",
                "progress": 100.0,
            },
        )
        return path

    repo_id = str(ref.get("repo_id") or "").strip()
    filename = str(ref.get("filename") or "").strip()
    if not repo_id or "/" not in repo_id:
        raise RuntimeError("llama.cpp provider needs a Hugging Face repo id, like owner/repo or owner/repo::model.gguf.")
    if not filename:
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "start",
                "stage": "download",
                "description": "Selecting GGUF file",
                "progress": 0.0,
            },
        )
        filename = _llama_cpp_select_gguf_filename(repo_id)

    try:
        from huggingface_hub import hf_hub_download  # type: ignore
    except Exception as exc:
        raise RuntimeError("llama.cpp GGUF auto-download needs huggingface_hub installed.") from exc

    model_root = _llama_cpp_model_root()
    os.makedirs(model_root, exist_ok=True)
    _emit_hf_llm_progress(
        progress_callback,
        {
            "event": "start",
            "stage": "download",
            "description": f"Downloading {filename}",
            "progress": 0.0,
        },
    )
    kwargs: Dict[str, Any] = {
        "repo_id": repo_id,
        "filename": filename,
        "cache_dir": model_root,
    }
    token = _hf_llm_hub_token()
    if token:
        kwargs["token"] = token
    expected_size, blob_ids = _hf_file_size_and_blob_ids(repo_id, filename)
    path = _hf_hub_download_with_progress_worker(
        repo_id=repo_id,
        filename=filename,
        cache_dir=model_root,
        token=token,
        progress_callback=progress_callback,
        description=f"Downloading {filename}",
        total_bytes=expected_size,
        blob_ids=blob_ids,
    )
    _emit_hf_llm_progress(
        progress_callback,
        {
            "event": "complete",
            "stage": "download",
            "description": "GGUF file is cached",
            "progress": 100.0,
        },
    )
    return str(path)


def _download_mlx_lm_model(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> str:
    raw = str(model_id or "").strip()
    if not raw:
        raise RuntimeError("MLX LM model id or local path is required.")

    expanded = os.path.abspath(os.path.expanduser(raw))
    if os.path.exists(expanded) or raw.startswith(("/", "./", "../", "~")):
        if not os.path.exists(expanded):
            raise RuntimeError(f"MLX LM local model path does not exist: {expanded}")
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "complete",
                "stage": "download",
                "description": "Using local MLX model files",
                "progress": 100.0,
            },
        )
        return expanded
    if raw.startswith("file://"):
        path = os.path.abspath(os.path.expanduser(raw[len("file://") :]))
        if not os.path.exists(path):
            raise RuntimeError(f"MLX LM local model path does not exist: {path}")
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "complete",
                "stage": "download",
                "description": "Using local MLX model files",
                "progress": 100.0,
            },
        )
        return path
    if raw.startswith("hf://"):
        raw = raw[len("hf://") :]

    try:
        from huggingface_hub import snapshot_download  # type: ignore
    except Exception as exc:
        raise RuntimeError("MLX LM auto-download needs huggingface_hub installed.") from exc

    model_root = _mlx_lm_model_root()
    os.makedirs(model_root, exist_ok=True)
    _emit_hf_llm_progress(
        progress_callback,
        {
            "event": "start",
            "stage": "download",
            "description": "Downloading MLX model files",
            "progress": 0.0,
        },
    )
    kwargs: Dict[str, Any] = {
        "repo_id": raw,
        "cache_dir": model_root,
        "allow_patterns": [
            "*.json",
            "*.safetensors",
            "*.py",
            "tokenizer.model",
            "*.tiktoken",
            "tiktoken.model",
            "*.txt",
            "*.jsonl",
            "*.jinja",
        ],
    }
    revision = _mlx_lm_revision()
    if revision:
        kwargs["revision"] = revision
    token = _mlx_lm_hub_token()
    if token:
        kwargs["token"] = token
    _ = snapshot_download
    path = _snapshot_download_with_progress_worker(
        kwargs=kwargs,
        progress_callback=progress_callback,
        description="Downloading MLX model files",
    )
    _emit_hf_llm_progress(
        progress_callback,
        {
            "event": "complete",
            "stage": "download",
            "description": "MLX model files are cached",
            "progress": 100.0,
        },
    )
    return str(path)


def _load_hf_llm_bundle(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> Dict[str, Any]:
    model_token = str(model_id or "").strip()
    if not model_token:
        raise RuntimeError("Hugging Face Transformers model id is required.")

    cache_key = _hf_llm_cache_key(model_token)
    with _HF_LLM_MODEL_CACHE_LOCK:
        cached = _HF_LLM_MODEL_CACHE.get(cache_key)
        if isinstance(cached, dict):
            _emit_hf_llm_progress(
                progress_callback,
                {
                    "event": "complete",
                    "stage": "load",
                    "description": "Model is already loaded",
                    "progress": 100.0,
                },
            )
            return cached

        try:
            import torch  # type: ignore
            import transformers as transformers_module  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "Hugging Face Transformers LLM backend needs torch and transformers installed."
            ) from exc

        AutoProcessor = getattr(transformers_module, "AutoProcessor", None)
        AutoTokenizer = getattr(transformers_module, "AutoTokenizer", None)
        if AutoTokenizer is None:
            raise RuntimeError("Hugging Face Transformers LLM backend could not find AutoTokenizer.")

        model_class_names = (
            "AutoModelForImageTextToText",
            "AutoModelForVision2Seq",
            "AutoModelForCausalLM",
            "AutoModelForSeq2SeqLM",
        )
        model_classes = [
            (name, getattr(transformers_module, name, None))
            for name in model_class_names
            if getattr(transformers_module, name, None) is not None
        ]
        if not model_classes:
            raise RuntimeError("Hugging Face Transformers LLM backend could not find a supported AutoModel class.")

        model_root = _hf_llm_model_root()
        os.makedirs(model_root, exist_ok=True)
        device = _hf_llm_resolve_device(torch, cache_key[1])
        torch_dtype = _hf_llm_torch_dtype(torch, cache_key[2], device)
        trust_remote_code = bool(cache_key[3])
        device_map = _hf_llm_device_map_pref(device)
        attn_implementation = _hf_llm_attn_implementation()
        hub_token = _hf_llm_hub_token()

        _download_hf_llm_snapshot(
            model_token,
            model_root=model_root,
            progress_callback=progress_callback,
        )
        model_path = _latest_hf_snapshot_path(model_root, model_token)
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "start",
                "stage": "load",
                "description": "Loading tokenizer and model into memory",
                "progress": 0.0,
            },
        )

        load_kwargs: Dict[str, Any] = {
            "cache_dir": model_root,
            "trust_remote_code": trust_remote_code,
        }
        if hub_token:
            load_kwargs["token"] = hub_token
        if torch_dtype != "auto" or device != "cpu":
            load_kwargs["torch_dtype"] = torch_dtype
        else:
            load_kwargs["torch_dtype"] = "auto"
        if device_map:
            load_kwargs["device_map"] = device_map
        if attn_implementation:
            load_kwargs["attn_implementation"] = attn_implementation

        pretrained_kwargs: Dict[str, Any] = {
            "cache_dir": model_root,
            "trust_remote_code": trust_remote_code,
        }
        if hub_token:
            pretrained_kwargs["token"] = hub_token

        processor = None
        tokenizer = None
        tokenizer_error: Optional[Exception] = None
        processor_error: Optional[Exception] = None
        try:
            tokenizer = AutoTokenizer.from_pretrained(model_token, **pretrained_kwargs)
        except Exception as exc:
            tokenizer_error = exc
        if AutoProcessor is not None:
            try:
                processor = AutoProcessor.from_pretrained(model_token, **pretrained_kwargs)
            except Exception as exc:
                processor_error = exc
                processor = None
        if tokenizer is None and processor is not None:
            tokenizer = getattr(processor, "tokenizer", None)
        if tokenizer is None:
            detail_exc = tokenizer_error or processor_error
            detail = str(detail_exc) if detail_exc is not None else "unknown tokenizer error"
            raise RuntimeError(f"Could not load tokenizer or processor for {model_token}: {detail}") from detail_exc

        model = None
        loaded_device_map = device_map
        load_errors: List[str] = []
        for class_name, model_class in model_classes:
            try:
                model = model_class.from_pretrained(model_token, **load_kwargs)
                loaded_device_map = device_map
                break
            except TypeError as exc:
                message = str(exc)
                if "attn_implementation" in message and "attn_implementation" in load_kwargs:
                    retry_kwargs = dict(load_kwargs)
                    retry_kwargs.pop("attn_implementation", None)
                    try:
                        model = model_class.from_pretrained(model_token, **retry_kwargs)
                        loaded_device_map = device_map
                        break
                    except Exception as retry_exc:
                        load_errors.append(f"{class_name}: {str(retry_exc)}")
                        continue
                if "torch_dtype" not in message:
                    load_errors.append(f"{class_name}: {message}")
                    continue
                retry_kwargs = dict(load_kwargs)
                retry_kwargs["dtype"] = retry_kwargs.pop("torch_dtype")
                try:
                    model = model_class.from_pretrained(model_token, **retry_kwargs)
                    loaded_device_map = device_map
                    break
                except Exception as retry_exc:
                    load_errors.append(f"{class_name}: {str(retry_exc)}")
            except Exception as exc:
                message = str(exc)
                if not device_map or ("accelerate" not in message.lower() and "device_map" not in message.lower()):
                    load_errors.append(f"{class_name}: {message}")
                    continue
                retry_kwargs = dict(load_kwargs)
                retry_kwargs.pop("device_map", None)
                try:
                    model = model_class.from_pretrained(model_token, **retry_kwargs)
                    loaded_device_map = ""
                    break
                except Exception as retry_exc:
                    load_errors.append(f"{class_name}: {str(retry_exc)}")
        if model is None:
            detail = "; ".join(error for error in load_errors if error) or "no compatible model class loaded"
            raise RuntimeError(f"Could not load Hugging Face model {model_token}. {detail}")
        device_map = loaded_device_map
        if getattr(tokenizer, "pad_token_id", None) is None and getattr(tokenizer, "eos_token", None) is not None:
            try:
                tokenizer.pad_token = tokenizer.eos_token
            except Exception:
                pass
        if not device_map:
            try:
                model.to(device)
            except Exception:
                device = "cpu"
        else:
            try:
                first_param = next(model.parameters())
                device = str(getattr(first_param, "device", None) or device)
            except Exception:
                device = str(getattr(model, "device", None) or device)
        try:
            model.eval()
        except Exception:
            pass
        chat_template_override, chat_template_handler, chat_template_warning = _install_local_llm_chat_template_override(
            HYDRA_LLM_PROVIDER_HF_TRANSFORMERS,
            model_token,
            tokenizer=tokenizer,
            processor=processor,
            model=model,
        )
        if chat_template_warning:
            logger.warning("[transformers] %s", chat_template_warning)

        generation_lock = _HF_LLM_GENERATION_LOCKS.get(cache_key)
        if generation_lock is None:
            generation_lock = threading.RLock()
            _HF_LLM_GENERATION_LOCKS[cache_key] = generation_lock

        bundle = {
            "model": model,
            "tokenizer": tokenizer,
            "processor": processor,
            "device": device,
            "torch": torch,
            "lock": generation_lock,
            "model_path": str(model_path or ""),
            "model_root": model_root,
            "memory_estimate_bytes": _model_memory_footprint_bytes(model),
            "loaded_ts": time.time(),
            "chat_template_override": bool(chat_template_override),
            "chat_template_handler": chat_template_handler,
            "chat_template_warning": chat_template_warning,
            "torch_dtype": str(torch_dtype),
            "device_map": str(device_map or ""),
            "attn_implementation": str(attn_implementation or ""),
            "trust_remote_code": trust_remote_code,
            "supports_vision": bool(_hf_processor_supports_vision(processor) or _hf_model_config_supports_vision(getattr(model, "config", None))),
        }
        _HF_LLM_MODEL_CACHE[cache_key] = bundle
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "complete",
                "stage": "load",
                "description": "Model loaded",
                "progress": 100.0,
                "device": device,
            },
        )
        return bundle


def preload_hf_transformers_llm_model(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> Dict[str, Any]:
    bundle = _load_hf_llm_bundle(model_id, progress_callback=progress_callback)
    return {
        "ok": True,
        "model": str(model_id or "").strip(),
        "device": str(bundle.get("device") or ""),
        "model_root": str(bundle.get("model_root") or _hf_llm_model_root()),
        "model_path": str(bundle.get("model_path") or ""),
        "warning": str(bundle.get("chat_template_warning") or ""),
        "supports_vision": bool(bundle.get("supports_vision")),
    }


def download_hf_transformers_llm_model(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> Dict[str, Any]:
    model_token = str(model_id or "").strip()
    if not model_token:
        raise RuntimeError("Hugging Face Transformers model id is required.")
    model_root = _hf_llm_model_root()
    os.makedirs(model_root, exist_ok=True)
    _download_hf_llm_snapshot(model_token, model_root=model_root, progress_callback=progress_callback)
    return {
        "ok": True,
        "model": model_token,
        "model_root": model_root,
        "model_path": _latest_hf_snapshot_path(model_root, model_token),
    }


def _llama_cpp_cache_key(model_id: str, *, vision: bool = False) -> Tuple[Any, ...]:
    return (
        str(model_id or "").strip(),
        str(os.getenv("TATER_LLAMA_CPP_CHAT_FORMAT") or "").strip(),
        _llama_cpp_n_ctx(vision=vision),
        _llama_cpp_n_gpu_layers(),
        _llama_cpp_n_batch(vision=vision),
        _llama_cpp_n_ubatch(vision=vision),
        _llama_cpp_flash_attn_enabled(),
        _llama_cpp_offload_kqv_enabled(),
        _llama_cpp_mtp_enabled(),
        _llama_cpp_mtp_draft_tokens(),
        _llama_cpp_mtp_spec_type(),
        "" if vision else _llama_cpp_chat_template_cache_token(model_id),
        bool(vision),
    )


def _llama_cpp_install_chat_template_override(model: Any, template: str) -> Tuple[str, str]:
    if not str(template or "").strip():
        return "", ""
    try:
        import llama_cpp.llama_chat_format as chat_format_module  # type: ignore
    except Exception as exc:
        return "", f"llama-cpp-python chat template override support is unavailable: {exc}"
    try:
        eos_token_id = model.token_eos() if callable(getattr(model, "token_eos", None)) else -1
        bos_token_id = model.token_bos() if callable(getattr(model, "token_bos", None)) else -1
        internals_model = getattr(model, "_model", None)
        eos_token = internals_model.token_get_text(eos_token_id) if internals_model is not None and eos_token_id != -1 else ""
        bos_token = internals_model.token_get_text(bos_token_id) if internals_model is not None and bos_token_id != -1 else ""
        if isinstance(eos_token, bytes):
            eos_token = eos_token.decode("utf-8", errors="replace")
        if isinstance(bos_token, bytes):
            bos_token = bos_token.decode("utf-8", errors="replace")
        handler = chat_format_module.Jinja2ChatFormatter(
            template=template,
            eos_token=eos_token,
            bos_token=bos_token,
            stop_token_ids=[eos_token_id] if eos_token_id != -1 else [],
        ).to_chat_handler()
        handler_name = "tater.chat_template_override"
        chat_handlers = getattr(model, "_chat_handlers", None)
        if isinstance(chat_handlers, dict):
            chat_handlers[handler_name] = handler
            model.chat_handler = None
            model.chat_format = handler_name
        else:
            model.chat_handler = handler
        return handler_name, ""
    except Exception as exc:
        return "", f"Could not install llama.cpp chat template override: {exc}"


def _load_llama_cpp_bundle(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
    vision: bool = False,
) -> Dict[str, Any]:
    model_token = str(model_id or "").strip()
    if not model_token:
        raise RuntimeError("llama.cpp GGUF model id or path is required.")

    cache_key = _llama_cpp_cache_key(model_token, vision=vision)
    with _LLAMA_CPP_MODEL_CACHE_LOCK:
        cached = _LLAMA_CPP_MODEL_CACHE.get(cache_key)
        if isinstance(cached, dict):
            _emit_hf_llm_progress(
                progress_callback,
                {
                    "event": "complete",
                    "stage": "load",
                    "description": "GGUF model is already loaded",
                    "progress": 100.0,
                    "device": str(cached.get("device") or ""),
                    "warning": _llama_cpp_warning_text(
                        cached.get("gpu_warning"),
                        cached.get("mtp_warning"),
                        cached.get("vision_warning"),
                        cached.get("chat_template_warning"),
                    ),
                },
            )
            return cached

        try:
            import llama_cpp as llama_cpp_module  # type: ignore
            from llama_cpp import Llama  # type: ignore
        except Exception as exc:
            raise RuntimeError("llama.cpp provider needs llama-cpp-python installed.") from exc
        _llama_cpp_patch_partial_model_destructor()

        system_info = _llama_cpp_system_info(llama_cpp_module)
        gpu_backend = _llama_cpp_gpu_backend(system_info)
        gpu_requested = cache_key[3] != 0
        n_ubatch = int(cache_key[5] or 0)
        flash_attn_enabled = bool(cache_key[6])
        offload_kqv_enabled = bool(cache_key[7])
        mtp_requested = bool(cache_key[8])
        mtp_draft_tokens = int(cache_key[9])
        mtp_spec_type = str(cache_key[10] or "draft-mtp")
        chat_template_override_hash = str(cache_key[11] or "")
        vision_requested = bool(cache_key[12])
        gpu_warning = ""
        if gpu_requested and system_info and not gpu_backend:
            gpu_warning = (
                "llama.cpp GPU offload was requested, but the installed llama-cpp-python build "
                "does not report a GPU backend. Reinstall the NVIDIA profile or rebuild "
                "llama-cpp-python with CUDA support."
            )
            logger.warning("[llama-cpp] %s system_info=%s", gpu_warning, system_info)

        model_path = _download_llama_cpp_model(model_token, progress_callback=progress_callback)
        mmproj_path = ""
        if vision_requested:
            mmproj_path = _download_llama_cpp_mmproj(
                model_token,
                model_path=model_path,
                progress_callback=progress_callback,
            )
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "start",
                "stage": "load",
                "description": "Loading GGUF model into llama.cpp",
                "progress": 0.0,
            },
        )

        load_kwargs: Dict[str, Any] = {
            "model_path": model_path,
            "n_ctx": cache_key[2],
            "n_gpu_layers": cache_key[3],
            "n_batch": cache_key[4],
            "n_threads": _llama_cpp_n_threads(),
            "verbose": _boolish(os.getenv("TATER_LLAMA_CPP_VERBOSE"), default=False),
        }
        if n_ubatch > 0:
            load_kwargs["n_ubatch"] = n_ubatch
        load_kwargs["offload_kqv"] = offload_kqv_enabled
        if flash_attn_enabled:
            load_kwargs["flash_attn"] = True
        chat_format = cache_key[1]
        if chat_format:
            load_kwargs["chat_format"] = chat_format
        if _boolish(os.getenv("TATER_LLAMA_CPP_USE_MLOCK"), default=False):
            load_kwargs["use_mlock"] = True

        vision_warning = ""
        chat_handler_name = ""
        if vision_requested and mmproj_path:
            chat_handler, chat_handler_name, vision_warning = _llama_cpp_make_vision_chat_handler(
                model_token,
                mmproj_path,
            )
            if chat_handler is not None:
                load_kwargs["chat_handler"] = chat_handler
            elif vision_warning:
                logger.warning("[llama-cpp] %s", vision_warning)

        mtp_kwargs, mtp_warning = _llama_cpp_mtp_load_kwargs(
            Llama,
            enabled=mtp_requested,
            spec_type=mtp_spec_type,
            draft_tokens=mtp_draft_tokens,
        )
        if mtp_kwargs:
            load_kwargs.update(mtp_kwargs)
        elif mtp_warning:
            logger.warning("[llama-cpp] %s", mtp_warning)

        try:
            model = Llama(**load_kwargs)
        except TypeError as exc:
            if not mtp_kwargs or not any(token in str(exc) for token in mtp_kwargs.keys()):
                raise
            for key in mtp_kwargs:
                load_kwargs.pop(key, None)
            mtp_warning = _llama_cpp_warning_text(
                mtp_warning,
                f"llama-cpp-python rejected MTP load arguments ({exc}); running without MTP.",
            )
            logger.warning("[llama-cpp] %s", mtp_warning)
            try:
                model = Llama(**load_kwargs)
            except Exception as retry_exc:
                raise RuntimeError(
                    _llama_cpp_context_load_error_message(
                        retry_exc,
                        model_id=model_token,
                        n_ctx=cache_key[2],
                        n_gpu_layers=cache_key[3],
                        n_batch=cache_key[4],
                        vision=vision_requested,
                        mmproj_path=mmproj_path,
                    )
                ) from retry_exc
        except Exception as exc:
            raise RuntimeError(
                _llama_cpp_context_load_error_message(
                    exc,
                    model_id=model_token,
                    n_ctx=cache_key[2],
                    n_gpu_layers=cache_key[3],
                    n_batch=cache_key[4],
                    vision=vision_requested,
                    mmproj_path=mmproj_path,
                )
            ) from exc
        chat_template_override = "" if vision_requested else _llama_cpp_chat_template_override_text(model_token)
        chat_template_handler_name = ""
        chat_template_warning = ""
        if chat_template_override:
            chat_template_handler_name, chat_template_warning = _llama_cpp_install_chat_template_override(
                model,
                chat_template_override,
            )
            if chat_template_warning:
                logger.warning("[llama-cpp] %s", chat_template_warning)
        device = gpu_backend if gpu_requested and gpu_backend else ("gpu" if gpu_requested and not system_info else "cpu")
        generation_lock = _LLAMA_CPP_GENERATION_LOCKS.get(cache_key)
        if generation_lock is None:
            generation_lock = threading.RLock()
            _LLAMA_CPP_GENERATION_LOCKS[cache_key] = generation_lock
        bundle = {
            "model": model,
            "model_path": model_path,
            "mmproj_path": mmproj_path,
            "model_root": _llama_cpp_model_root(),
            "lock": generation_lock,
            "n_gpu_layers": cache_key[3],
            "n_ctx": cache_key[2],
            "n_batch": cache_key[4],
            "n_ubatch": n_ubatch,
            "flash_attn": flash_attn_enabled,
            "offload_kqv": offload_kqv_enabled,
            "device": device,
            "gpu_backend": gpu_backend,
            "gpu_warning": gpu_warning,
            "memory_estimate_bytes": _safe_path_size_bytes(model_path),
            "loaded_ts": time.time(),
            "mtp_requested": mtp_requested,
            "mtp_enabled": bool(mtp_requested and mtp_kwargs and not mtp_warning),
            "mtp_spec_type": mtp_spec_type,
            "mtp_draft_tokens": mtp_draft_tokens,
            "mtp_warning": mtp_warning,
            "chat_template_override": bool(chat_template_override),
            "chat_template_override_hash": chat_template_override_hash,
            "chat_template_handler": chat_template_handler_name,
            "chat_template_warning": chat_template_warning,
            "vision_requested": bool(vision_requested),
            "supports_vision": bool(vision_requested and mmproj_path and chat_handler_name),
            "vision_projector_path": mmproj_path,
            "vision_chat_handler": chat_handler_name,
            "vision_warning": vision_warning,
            "llama_cpp_system_info": system_info,
        }
        _LLAMA_CPP_MODEL_CACHE[cache_key] = bundle
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "complete",
                "stage": "load",
                "description": "GGUF model loaded",
                "progress": 100.0,
                "device": device,
                "warning": _llama_cpp_warning_text(gpu_warning, mtp_warning, chat_template_warning),
            },
        )
        return bundle


def preload_llama_cpp_llm_model(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
    vision: bool = False,
) -> Dict[str, Any]:
    bundle = _load_llama_cpp_bundle(model_id, progress_callback=progress_callback, vision=vision)
    return {
        "ok": True,
        "model": str(model_id or "").strip(),
        "device": str(bundle.get("device") or ("gpu" if int(bundle.get("n_gpu_layers") or 0) != 0 else "cpu")),
        "model_root": str(bundle.get("model_root") or _llama_cpp_model_root()),
        "model_path": str(bundle.get("model_path") or ""),
        "gpu_backend": str(bundle.get("gpu_backend") or ""),
        "n_ctx": int(bundle.get("n_ctx") or 0),
        "n_gpu_layers": int(bundle.get("n_gpu_layers") or 0),
        "n_batch": int(bundle.get("n_batch") or 0),
        "n_ubatch": int(bundle.get("n_ubatch") or 0),
        "flash_attn": bool(bundle.get("flash_attn")),
        "offload_kqv": bool(bundle.get("offload_kqv")),
        "mtp_requested": bool(bundle.get("mtp_requested")),
        "mtp_enabled": bool(bundle.get("mtp_enabled")),
        "mtp_spec_type": str(bundle.get("mtp_spec_type") or ""),
        "mtp_draft_tokens": int(bundle.get("mtp_draft_tokens") or 0),
        "mmproj_path": str(bundle.get("mmproj_path") or ""),
        "supports_vision": bool(bundle.get("supports_vision")),
        "vision_chat_handler": str(bundle.get("vision_chat_handler") or ""),
        "warning": _llama_cpp_warning_text(
            bundle.get("gpu_warning"),
            bundle.get("mtp_warning"),
            bundle.get("vision_warning"),
            bundle.get("chat_template_warning"),
        ),
    }


def download_llama_cpp_llm_model(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> Dict[str, Any]:
    model_token = str(model_id or "").strip()
    if not model_token:
        raise RuntimeError("llama.cpp GGUF model id or path is required.")
    model_path = _download_llama_cpp_model(model_token, progress_callback=progress_callback)
    mmproj_path = _download_llama_cpp_mmproj(
        model_token,
        model_path=model_path,
        progress_callback=progress_callback,
    )
    return {
        "ok": True,
        "model": model_token,
        "model_root": _llama_cpp_model_root(),
        "model_path": model_path,
        "mmproj_path": mmproj_path,
        "supports_vision": bool(mmproj_path),
    }


def _mlx_lm_cache_key(model_id: str) -> Tuple[str, str, bool, bool]:
    return (
        str(model_id or "").strip(),
        _mlx_lm_adapter_path(),
        _mlx_lm_trust_remote_code(),
        _mlx_lm_lazy_load(),
    )


def _mlx_lm_loaded_bundle_for_model(model_id: str) -> Optional[Dict[str, Any]]:
    model_token = str(model_id or "").strip()
    if not model_token:
        return None
    cache_key = _mlx_lm_cache_key(model_token)
    with _MLX_LM_MODEL_CACHE_LOCK:
        cached = _MLX_LM_MODEL_CACHE.get(cache_key)
        return cached if isinstance(cached, dict) else None


def _load_mlx_lm_bundle(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> Dict[str, Any]:
    model_token = str(model_id or "").strip()
    if not model_token:
        raise RuntimeError("MLX LM model id or local path is required.")
    if not _mlx_lm_is_apple_silicon():
        raise RuntimeError("MLX LM provider runs on Apple Silicon Macs only. Use llama.cpp GGUF on this device.")

    cache_key = _mlx_lm_cache_key(model_token)
    with _MLX_LM_MODEL_CACHE_LOCK:
        cached = _MLX_LM_MODEL_CACHE.get(cache_key)
        if isinstance(cached, dict):
            _emit_hf_llm_progress(
                progress_callback,
                {
                    "event": "complete",
                    "stage": "load",
                    "description": "MLX model is already loaded",
                    "progress": 100.0,
                    "device": "apple_silicon",
                },
            )
            return cached

        try:
            from mlx_lm import generate as mlx_generate  # type: ignore
            from mlx_lm import load as mlx_load  # type: ignore
            from mlx_lm import stream_generate as mlx_stream_generate  # type: ignore
            from mlx_lm.sample_utils import make_logits_processors, make_sampler  # type: ignore
        except Exception as exc:
            raise RuntimeError("MLX LM provider needs mlx-lm installed on an Apple Silicon Mac.") from exc

        model_path = _download_mlx_lm_model(model_token, progress_callback=progress_callback)
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "start",
                "stage": "load",
                "description": "Loading MLX model into memory",
                "progress": 0.0,
            },
        )

        tokenizer_config: Dict[str, Any] = {}
        if cache_key[2]:
            tokenizer_config["trust_remote_code"] = True
        load_kwargs: Dict[str, Any] = {
            "tokenizer_config": tokenizer_config,
            "lazy": cache_key[3],
            "return_config": True,
        }
        adapter_path = cache_key[1]
        if adapter_path:
            load_kwargs["adapter_path"] = adapter_path

        try:
            loaded = mlx_load(model_path, **load_kwargs)
        except TypeError:
            retry_kwargs = dict(load_kwargs)
            retry_kwargs.pop("return_config", None)
            loaded = mlx_load(model_path, **retry_kwargs)
        if not isinstance(loaded, tuple) or len(loaded) < 2:
            raise RuntimeError(f"Could not load MLX model {model_token}.")
        model = loaded[0]
        tokenizer = loaded[1]
        config = loaded[2] if len(loaded) >= 3 and isinstance(loaded[2], dict) else {}
        chat_template_override, chat_template_handler, chat_template_warning = _install_local_llm_chat_template_override(
            HYDRA_LLM_PROVIDER_MLX_LM,
            model_token,
            tokenizer=tokenizer,
            config=config,
            model=model,
        )
        if chat_template_warning:
            logger.warning("[mlx-lm] %s", chat_template_warning)

        generation_lock = _MLX_LM_GENERATION_LOCKS.get(cache_key)
        if generation_lock is None:
            generation_lock = threading.RLock()
            _MLX_LM_GENERATION_LOCKS[cache_key] = generation_lock

        bundle = {
            "model": model,
            "tokenizer": tokenizer,
            "config": config,
            "lock": generation_lock,
            "model_path": model_path,
            "model_root": _mlx_lm_model_root(),
            "device": "apple_silicon",
            "memory_estimate_bytes": _safe_path_size_bytes(model_path),
            "loaded_ts": time.time(),
            "chat_template_override": bool(chat_template_override),
            "chat_template_handler": chat_template_handler,
            "chat_template_warning": chat_template_warning,
            "trust_remote_code": bool(cache_key[2]),
            "lazy_load": bool(cache_key[3]),
            "supports_vision": False,
            "vision_chat_handler": "",
            "generate": mlx_generate,
            "stream_generate": mlx_stream_generate,
            "make_sampler": make_sampler,
            "make_logits_processors": make_logits_processors,
        }
        _MLX_LM_MODEL_CACHE[cache_key] = bundle
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "complete",
                "stage": "load",
                "description": "MLX model loaded",
                "progress": 100.0,
                "device": "apple_silicon",
            },
        )
        return bundle


def _preload_mlx_lm_llm_model_sync(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> Dict[str, Any]:
    bundle = _load_mlx_engine_bundle(model_id, progress_callback=progress_callback)
    return {
        "ok": True,
        "model": str(model_id or "").strip(),
        "device": str(bundle.get("device") or "apple_silicon"),
        "model_root": str(bundle.get("model_root") or _mlx_lm_model_root()),
        "model_path": str(bundle.get("model_path") or ""),
        "supports_vision": bool(bundle.get("supports_vision")),
        "vision_chat_handler": str(bundle.get("vision_chat_handler") or ""),
        "warning": str(bundle.get("chat_template_warning") or ""),
        "runtime": "mlx-engine",
    }


def preload_mlx_lm_llm_model(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> Dict[str, Any]:
    return _run_mlx_runtime_sync(
        _preload_mlx_lm_llm_model_sync,
        model_id,
        progress_callback=progress_callback,
    )


def download_mlx_lm_llm_model(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> Dict[str, Any]:
    model_token = str(model_id or "").strip()
    if not model_token:
        raise RuntimeError("MLX LM model id or local path is required.")
    model_path = _download_mlx_lm_model(model_token, progress_callback=progress_callback)
    return {
        "ok": True,
        "model": model_token,
        "model_root": _mlx_lm_model_root(),
        "model_path": model_path,
        "supports_vision": False,
        "vision_chat_handler": "",
    }


def _mlx_vlm_cache_key(model_id: str) -> Tuple[str, bool, bool]:
    return (
        str(model_id or "").strip(),
        _mlx_lm_trust_remote_code(),
        _mlx_lm_lazy_load(),
    )


def _load_mlx_vlm_bundle(
    model_id: str,
    *,
    progress_callback: Optional[HFProgressCallback] = None,
) -> Dict[str, Any]:
    model_token = str(model_id or "").strip()
    if not model_token:
        raise RuntimeError("MLX VLM model id or local path is required.")
    if not _mlx_lm_is_apple_silicon():
        raise RuntimeError("MLX vision runs on Apple Silicon Macs only. Use Transformers or llama.cpp on this device.")

    cache_key = _mlx_vlm_cache_key(model_token)
    with _MLX_VLM_MODEL_CACHE_LOCK:
        cached = _MLX_VLM_MODEL_CACHE.get(cache_key)
        if isinstance(cached, dict):
            _emit_hf_llm_progress(
                progress_callback,
                {
                    "event": "complete",
                    "stage": "load",
                    "description": "MLX vision model is already loaded",
                    "progress": 100.0,
                    "device": "apple_silicon",
                },
            )
            return cached

        try:
            from mlx_vlm import generate as mlx_vlm_generate  # type: ignore
            from mlx_vlm import load as mlx_vlm_load  # type: ignore
            from mlx_vlm.prompt_utils import apply_chat_template as mlx_vlm_apply_chat_template  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "MLX vision needs mlx-vlm installed on an Apple Silicon Mac. "
                "Install/update requirements, then load a vision-capable MLX model."
            ) from exc

        try:
            from mlx_vlm.utils import load_config as mlx_vlm_load_config  # type: ignore
        except Exception:
            mlx_vlm_load_config = None

        model_path = _download_mlx_lm_model(model_token, progress_callback=progress_callback)
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "start",
                "stage": "load",
                "description": "Loading MLX vision model into memory",
                "progress": 0.0,
            },
        )

        load_kwargs: Dict[str, Any] = {}
        if cache_key[1]:
            load_kwargs["trust_remote_code"] = True
        if cache_key[2]:
            load_kwargs["lazy"] = True
        try:
            loaded = mlx_vlm_load(model_path, **load_kwargs)
        except TypeError:
            retry_kwargs = dict(load_kwargs)
            retry_kwargs.pop("lazy", None)
            try:
                loaded = mlx_vlm_load(model_path, **retry_kwargs)
            except TypeError:
                retry_kwargs.pop("trust_remote_code", None)
                loaded = mlx_vlm_load(model_path, **retry_kwargs)
        if not isinstance(loaded, tuple) or len(loaded) < 2:
            raise RuntimeError(f"Could not load MLX vision model {model_token}.")
        model_obj = loaded[0]
        processor = loaded[1]

        config = getattr(model_obj, "config", None)
        if not isinstance(config, dict) and callable(mlx_vlm_load_config):
            try:
                config_kwargs: Dict[str, Any] = {}
                if cache_key[1]:
                    config_kwargs["trust_remote_code"] = True
                try:
                    config = mlx_vlm_load_config(model_path, **config_kwargs)
                except TypeError:
                    config = mlx_vlm_load_config(model_path)
            except Exception:
                config = None
        _mlx_vlm_normalize_vision_tokens(processor, getattr(processor, "tokenizer", None), config)
        chat_template_override, chat_template_handler, chat_template_warning = _install_local_llm_chat_template_override(
            HYDRA_LLM_PROVIDER_MLX_LM,
            model_token,
            processor=processor,
            config=config,
            model=model_obj,
        )
        if chat_template_warning:
            logger.warning("[mlx-vlm] %s", chat_template_warning)

        generation_lock = _MLX_VLM_GENERATION_LOCKS.get(cache_key)
        if generation_lock is None:
            generation_lock = threading.RLock()
            _MLX_VLM_GENERATION_LOCKS[cache_key] = generation_lock

        bundle = {
            "model": model_obj,
            "processor": processor,
            "config": config,
            "lock": generation_lock,
            "model_path": model_path,
            "model_root": _mlx_lm_model_root(),
            "device": "apple_silicon",
            "memory_estimate_bytes": _safe_path_size_bytes(model_path),
            "loaded_ts": time.time(),
            "chat_template_override": bool(chat_template_override),
            "chat_template_handler": chat_template_handler,
            "chat_template_warning": chat_template_warning,
            "generate": mlx_vlm_generate,
            "apply_chat_template": mlx_vlm_apply_chat_template,
            "supports_vision": True,
            "vision_chat_handler": "mlx-vlm",
        }
        _MLX_VLM_MODEL_CACHE[cache_key] = bundle
        _emit_hf_llm_progress(
            progress_callback,
            {
                "event": "complete",
                "stage": "load",
                "description": "MLX vision model loaded",
                "progress": 100.0,
                "device": "apple_silicon",
            },
        )
        return bundle


def _mlx_vlm_processor_tokenizer(bundle: Dict[str, Any]) -> Any:
    processor = bundle.get("processor")
    for attr in ("tokenizer", "_tokenizer"):
        try:
            tokenizer = getattr(processor, attr, None)
            if tokenizer is not None:
                return tokenizer
        except Exception:
            pass
    return processor


def _mlx_vlm_generate_with_fallback(
    generate: Any,
    model_obj: Any,
    processor: Any,
    formatted_prompt: str,
    images: Optional[List[str]],
    generation_kwargs: Dict[str, Any],
    *,
    allow_no_images: bool = False,
) -> Any:
    if not callable(generate):
        raise RuntimeError("MLX vision runtime is missing generate helper.")
    image_values: List[Tuple[str, Any]] = []
    if images is not None:
        image_list = list(images)
        image_values.append(("positional", image_list))
        image_values.append(("named", image_list))
        if len(image_list) == 1:
            image_values.append(("named", image_list[0]))
    if allow_no_images:
        image_values.append(("none", None))

    kw_variants: List[Dict[str, Any]] = [dict(generation_kwargs)]
    no_temperature = dict(generation_kwargs)
    no_temperature.pop("temperature", None)
    if no_temperature not in kw_variants:
        kw_variants.append(no_temperature)
    no_max_tokens = dict(no_temperature)
    no_max_tokens.pop("max_tokens", None)
    if no_max_tokens not in kw_variants:
        kw_variants.append(no_max_tokens)

    last_exc: Optional[TypeError] = None
    for kwargs_variant in kw_variants:
        for mode, image_value in image_values:
            try:
                if mode == "positional":
                    return generate(model_obj, processor, formatted_prompt, image_value, **kwargs_variant)
                if mode == "named":
                    return generate(model_obj, processor, formatted_prompt, image=image_value, **kwargs_variant)
                return generate(model_obj, processor, formatted_prompt, **kwargs_variant)
            except TypeError as exc:
                last_exc = exc
                continue
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("MLX vision generation failed.")


def _resolve_hydra_llm_defaults(*, redis_conn: Any = None) -> tuple[str, str]:
    host = _safe_redis_text_get(HYDRA_LLM_HOST_KEY, redis_conn=redis_conn)
    port = _safe_redis_text_get(HYDRA_LLM_PORT_KEY, redis_conn=redis_conn)
    model = _safe_redis_text_get(HYDRA_LLM_MODEL_KEY, redis_conn=redis_conn)
    endpoint = _build_hydra_llm_endpoint(host, port)
    return endpoint, model


def _normalize_hydra_base_server_row(row: Any) -> Optional[Dict[str, str]]:
    if not isinstance(row, dict):
        return None

    provider = _normalize_hydra_llm_provider(row.get("provider"))
    raw_host = str(row.get("host") or "").strip()
    raw_port = str(row.get("port") or "").strip()
    raw_model = str(row.get("model") or "").strip()
    raw_api_key = str(row.get("api_key") or "").strip()
    if not raw_host and not raw_port and not raw_model:
        return None

    if _is_local_hydra_llm_provider(provider):
        if not raw_model:
            return None
        return {
            "provider": provider,
            "host": "",
            "port": "",
            "model": raw_model,
            "api_key": "",
            "endpoint": (
                "hf://transformers"
                if provider == HYDRA_LLM_PROVIDER_HF_TRANSFORMERS
                else "llama-cpp://local"
                if provider == HYDRA_LLM_PROVIDER_LLAMA_CPP
                else "mlx-lm://local"
            ),
        }

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
        "provider": provider,
        "host": canonical_host,
        "port": canonical_port,
        "model": raw_model,
        "api_key": raw_api_key,
        "endpoint": endpoint,
    }


def _normalize_spud_link_hub_url(value: Any) -> str:
    raw = str(value or "").strip().rstrip("/")
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"http://{raw}"
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    path = str(parsed.path or "").rstrip("/")
    for suffix in ("/api/spudlink/v1/chat/completions", "/api/spudlink/v1", "/api/spudlink"):
        if path.endswith(suffix):
            path = path[: -len(suffix)].rstrip("/")
            break
    return urlunparse(parsed._replace(path=path, params="", query="", fragment="")).rstrip("/")


def _resolve_spud_link_base_server(*, redis_conn: Any = None) -> Optional[Dict[str, str]]:
    client = redis_conn or redis_client
    try:
        raw = client.hgetall(SPUD_LINK_SETTINGS_KEY) or {}
    except Exception:
        return None
    mode = str(raw.get("mode") or "").strip().lower().replace("-", "_").replace(" ", "_")
    if mode != SPUD_LINK_MODE_SPUDLET:
        return None
    hub_url = _normalize_spud_link_hub_url(raw.get("hub_url"))
    node_token = str(raw.get("node_token") or "").strip()
    if not hub_url or not node_token:
        return None
    endpoint = f"{hub_url}/api/spudlink/v1"
    return {
        "provider": HYDRA_LLM_PROVIDER_SPUD_LINK,
        "host": hub_url,
        "port": "",
        "model": "tater/base",
        "api_key": node_token,
        "endpoint": endpoint,
    }


def resolve_hydra_base_servers(*, redis_conn: Any = None, include_legacy: bool = True) -> List[Dict[str, str]]:
    client = redis_conn or redis_client
    rows: List[Dict[str, str]] = []
    seen: set[Tuple[str, str, str, str]] = set()

    spud_link_row = _resolve_spud_link_base_server(redis_conn=client)
    if spud_link_row:
        signature = (
            spud_link_row.get("provider", HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE),
            spud_link_row["endpoint"],
            spud_link_row["model"],
            spud_link_row.get("api_key", ""),
        )
        seen.add(signature)
        rows.append(spud_link_row)
        return rows

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
            signature = (
                normalized.get("provider", HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE),
                normalized["endpoint"],
                normalized["model"],
                normalized.get("api_key", ""),
            )
            if signature in seen:
                continue
            seen.add(signature)
            rows.append(normalized)

    if rows or not include_legacy:
        return rows

    legacy_host = _safe_redis_text_get(HYDRA_LLM_HOST_KEY, redis_conn=client)
    legacy_port = _safe_redis_text_get(HYDRA_LLM_PORT_KEY, redis_conn=client)
    legacy_model = _safe_redis_text_get(HYDRA_LLM_MODEL_KEY, redis_conn=client)
    legacy_provider = _safe_redis_text_get(HYDRA_LLM_PROVIDER_KEY, redis_conn=client)
    legacy_row = _normalize_hydra_base_server_row(
        {"provider": legacy_provider, "host": legacy_host, "port": legacy_port, "model": legacy_model}
    )
    if legacy_row:
        signature = (
            legacy_row.get("provider", HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE),
            legacy_row["endpoint"],
            legacy_row["model"],
            legacy_row.get("api_key", ""),
        )
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


def _llm_debug_text(value: Any, *, limit: int = 900) -> str:
    text = _coerce_content_to_text(value).strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    max_len = max(80, int(limit or 900))
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def _llm_debug_row_for_call(call_id: Any) -> Dict[str, Any]:
    token = str(call_id or "").strip()
    if not token:
        return {}
    with _ACTIVE_LLM_CALLS_LOCK:
        row = _ACTIVE_LLM_CALLS.get(token)
        return dict(row) if isinstance(row, dict) else {}


def _append_llm_debug_event(
    *,
    phase: str,
    message: str,
    call_id: Any = "",
    level: str = "info",
    provider: str = "",
    host: str = "",
    model: str = "",
    activity: str = "",
    kind: str = "",
    source: str = "",
    detail: Any = None,
    output: Any = None,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    total_tokens: int = 0,
    duration_ms: float = 0.0,
) -> None:
    global _LLM_DEBUG_EVENT_SEQ
    call_row = _llm_debug_row_for_call(call_id)
    if call_row:
        host = host or str(call_row.get("host") or "")
        model = model or str(call_row.get("model") or "")
        activity = activity or str(call_row.get("activity") or "")
        kind = kind or str(call_row.get("kind") or "")
        source = source or str(call_row.get("source") or "")

    phase_token = str(phase or "event").strip().lower().replace(" ", "_") or "event"
    level_token = str(level or "info").strip().lower() or "info"
    if level_token not in {"debug", "info", "success", "warning", "error"}:
        level_token = "info"

    event = {
        "id": 0,
        "ts": time.time(),
        "level": level_token,
        "phase": phase_token,
        "message": _llm_debug_text(message, limit=360),
        "call_id": str(call_id or "").strip(),
        "provider": str(provider or "").strip(),
        "host": str(host or "").strip(),
        "model": str(model or "").strip(),
        "activity": str(activity or "").strip(),
        "kind": str(kind or "").strip(),
        "source": str(source or "").strip(),
        "source_label": (
            f"{_llm_origin_kind_label(kind)} - {source}"
            if str(kind or "").strip() or str(source or "").strip()
            else ""
        ),
        "detail": _llm_debug_text(detail, limit=900),
        "output": _llm_debug_text(output, limit=1200),
        "prompt_tokens": max(0, int(prompt_tokens or 0)),
        "completion_tokens": max(0, int(completion_tokens or 0)),
        "total_tokens": max(0, int(total_tokens or 0)),
        "duration_ms": round(max(0.0, float(duration_ms or 0.0)), 2),
    }

    with _LLM_DEBUG_EVENTS_LOCK:
        _LLM_DEBUG_EVENT_SEQ += 1
        event["id"] = int(_LLM_DEBUG_EVENT_SEQ)
        _LLM_DEBUG_EVENTS.append(event)
        overflow = len(_LLM_DEBUG_EVENTS) - int(_LLM_DEBUG_EVENTS_MAX)
        if overflow > 0:
            del _LLM_DEBUG_EVENTS[:overflow]


def _append_llm_debug_result(
    *,
    call_id: Any,
    provider: str = "",
    host: str = "",
    model: str = "",
    result: Any = None,
    usage: Any = None,
    timing: Any = None,
    elapsed: float = 0.0,
) -> None:
    response_text = ""
    if isinstance(result, dict):
        message = result.get("message")
        if isinstance(message, dict):
            response_text = _coerce_content_to_text(message.get("content"))
    prompt_tokens = _llm_runtime_as_int((usage or {}).get("prompt_tokens"), 0, minimum=0) if isinstance(usage, dict) else 0
    completion_tokens = _llm_runtime_as_int((usage or {}).get("completion_tokens"), 0, minimum=0) if isinstance(usage, dict) else 0
    total_tokens = (
        _llm_runtime_as_int((usage or {}).get("total_tokens"), prompt_tokens + completion_tokens, minimum=0)
        if isinstance(usage, dict)
        else 0
    )
    timing_map = timing if isinstance(timing, dict) else {}
    duration_ms = max(0.0, float(elapsed or 0.0)) * 1000.0
    prompt_elapsed = _perf_nonnegative_float(timing_map.get("prompt_elapsed"))
    completion_elapsed = _perf_nonnegative_float(timing_map.get("completion_elapsed"))
    speed_basis = str(timing_map.get("speed_basis") or "").strip()
    detail_parts: List[str] = []
    if prompt_tokens:
        detail_parts.append(f"prompt_tokens={prompt_tokens}")
    if completion_tokens:
        detail_parts.append(f"completion_tokens={completion_tokens}")
    if total_tokens:
        detail_parts.append(f"total_tokens={total_tokens}")
    if prompt_elapsed:
        detail_parts.append(f"prompt_ms={round(prompt_elapsed * 1000.0, 1)}")
    if completion_elapsed:
        detail_parts.append(f"generation_ms={round(completion_elapsed * 1000.0, 1)}")
    if speed_basis:
        detail_parts.append(f"timing={speed_basis}")
    _append_llm_debug_event(
        phase="output",
        level="success",
        message="Model output ready",
        call_id=call_id,
        provider=provider,
        host=host,
        model=model,
        detail=" • ".join(detail_parts),
        output=response_text,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        duration_ms=duration_ms,
    )


def get_llm_debug_runtime_snapshot(*, since_id: int = 0, limit: int = 200) -> Dict[str, Any]:
    try:
        since = max(0, int(since_id or 0))
    except Exception:
        since = 0
    try:
        max_items = max(20, min(int(limit or 200), 500))
    except Exception:
        max_items = 200

    with _LLM_DEBUG_EVENTS_LOCK:
        current_seq = int(_LLM_DEBUG_EVENT_SEQ)
        if since > 0:
            rows = [dict(row) for row in _LLM_DEBUG_EVENTS if int(row.get("id") or 0) > since]
        else:
            rows = [dict(row) for row in _LLM_DEBUG_EVENTS[-max_items:]]
    if len(rows) > max_items:
        rows = rows[-max_items:]

    return {
        "ok": True,
        "generated_at": time.time(),
        "next_id": current_seq,
        "events": rows,
        "active_calls": get_active_llm_calls_snapshot(limit=50),
        "summary": get_llm_call_runtime_summary(include_history=False),
    }


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
    _append_llm_debug_event(
        phase="start",
        level="info",
        message="LLM call started",
        call_id=call_id,
        host=row["host"],
        model=row["model"],
        activity=row["activity"],
        kind=row["kind"],
        source=row["source"],
        detail=f"messages={row['message_count']} stream={str(bool(row['stream'])).lower()}",
    )
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
    _append_llm_debug_event(
        phase="finish",
        level=("success" if ok else "error"),
        message=("LLM call completed" if ok else "LLM call failed"),
        call_id=call_token,
        host=history_row["host"],
        model=history_row["model"],
        activity=history_row["activity"],
        kind=history_row["kind"],
        source=history_row["source"],
        detail=(history_row["error"] if not ok else f"duration_ms={round(duration_ms, 2)}"),
        duration_ms=duration_ms,
    )


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
    try:
        rows = resolve_hydra_base_servers(include_legacy=True)
    except Exception:
        rows = []
    primary = rows[0] if rows and isinstance(rows[0], dict) else {}
    endpoint = str(primary.get("endpoint") or "").strip() if isinstance(primary, dict) else ""
    if endpoint:
        return endpoint
    endpoint, _ = _resolve_hydra_llm_defaults()
    return endpoint

def _make_llm_client_for_provider(
    *,
    provider: str,
    host: str,
    model: str,
    api_key: str = "",
    **kwargs,
) -> Any:
    selected_provider = _normalize_hydra_llm_provider(provider)
    if selected_provider == HYDRA_LLM_PROVIDER_HF_TRANSFORMERS:
        return TransformersLLMClientWrapper(model=model, **kwargs)
    if selected_provider == HYDRA_LLM_PROVIDER_LLAMA_CPP:
        return LlamaCppLLMClientWrapper(model=model, **kwargs)
    if selected_provider == HYDRA_LLM_PROVIDER_MLX_LM:
        return MlxLmLLMClientWrapper(model=model, **kwargs)
    if selected_provider == HYDRA_LLM_PROVIDER_SPUD_LINK:
        return SpudLinkLLMClientWrapper(host=host, model=model, api_key=api_key, **kwargs)
    return LLMClientWrapper(host=host, model=model, api_key=api_key, **kwargs)


def get_llm_client_from_env(host: Optional[str] = None, model: Optional[str] = None, **kwargs) -> Any:
    """
    Construct an LLMClientWrapper using explicit host/model overrides,
    with Hydra Base LLM settings fallback from Redis.
    No .env host/model fallback is used.
    """
    redis_conn = kwargs.pop("redis_conn", None)
    api_key_arg_provided = "api_key" in kwargs
    provider_arg_provided = "provider" in kwargs
    explicit_api_key = str(kwargs.pop("api_key", "") or "").strip()
    explicit_provider = _normalize_hydra_llm_provider(kwargs.pop("provider", "")) if provider_arg_provided else ""
    explicit_host = str(host or "").strip()
    explicit_model = str(model or "").strip()

    base_servers = resolve_hydra_base_servers(redis_conn=redis_conn, include_legacy=True)
    default_provider = (
        str(base_servers[0].get("provider") or HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE).strip()
        if base_servers
        else ""
    )
    default_host = str(base_servers[0]["endpoint"]).strip() if base_servers else ""
    default_model = str(base_servers[0]["model"]).strip() if base_servers else ""
    default_api_key = str(base_servers[0].get("api_key") or "").strip() if base_servers else ""
    if not default_host or not default_model:
        fallback_host, fallback_model = _resolve_hydra_llm_defaults(redis_conn=redis_conn)
        if not default_provider:
            default_provider = _safe_redis_text_get(HYDRA_LLM_PROVIDER_KEY, redis_conn=redis_conn)
        if not default_host:
            default_host = fallback_host
        if not default_model:
            default_model = fallback_model

    default_provider = _normalize_hydra_llm_provider(default_provider)
    resolved_provider = explicit_provider or (
        HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE if explicit_host else default_provider
    )
    resolved_host = explicit_host or default_host
    resolved_model = explicit_model or default_model
    if _is_local_hydra_llm_provider(resolved_provider):
        if not resolved_model:
            raise RuntimeError(HYDRA_LLM_SETUP_ERROR)
    elif not resolved_host or not resolved_model:
        raise RuntimeError(HYDRA_LLM_SETUP_ERROR)

    if not explicit_host and not explicit_model and len(base_servers) > 1:
        clients: List[Any] = []
        signature_parts: List[str] = []
        for row in base_servers:
            row_provider = _normalize_hydra_llm_provider(row.get("provider"))
            endpoint = str(row.get("endpoint") or "").strip()
            row_model = str(row.get("model") or "").strip()
            row_api_key = str(row.get("api_key") or "").strip()
            if not row_model:
                continue
            if not _is_local_hydra_llm_provider(row_provider) and not endpoint:
                continue
            clients.append(
                _make_llm_client_for_provider(
                    provider=row_provider,
                    host=endpoint,
                    model=row_model,
                    api_key=row_api_key,
                    **kwargs,
                )
            )
            signature_parts.append(f"{row_provider}|{endpoint}|{row_model}|{row_api_key}")
        if len(clients) > 1:
            pool_key = "||".join(signature_parts)
            return RoundRobinLLMClientWrapper(clients=clients, pool_key=pool_key)
        if len(clients) == 1:
            return clients[0]

    resolved_api_key = explicit_api_key
    if not resolved_api_key and not api_key_arg_provided and not explicit_host and not explicit_model:
        resolved_api_key = default_api_key

    return _make_llm_client_for_provider(
        provider=resolved_provider,
        host=resolved_host,
        model=resolved_model,
        api_key=resolved_api_key,
        **kwargs,
    )

def primary_hydra_llm_client_kwargs(*, redis_conn: Any = None) -> Dict[str, Any]:
    """Return explicit kwargs for the first configured Hydra Base LLM row."""
    out: Dict[str, Any] = {"redis_conn": redis_conn}
    try:
        rows = resolve_hydra_base_servers(redis_conn=redis_conn, include_legacy=True)
    except Exception:
        rows = []
    primary = rows[0] if rows and isinstance(rows[0], dict) else {}
    provider = _normalize_hydra_llm_provider(primary.get("provider") if isinstance(primary, dict) else "")
    model = str(primary.get("model") or "").strip() if isinstance(primary, dict) else ""
    if not model:
        return out
    out["provider"] = provider
    out["model"] = model
    if not _is_local_hydra_llm_provider(provider):
        out["host"] = str(primary.get("endpoint") or "").strip()
        api_key = str(primary.get("api_key") or "").strip()
        if api_key:
            out["api_key"] = api_key
    return out

def get_primary_llm_client_from_env(host: Optional[str] = None, model: Optional[str] = None, **kwargs) -> Any:
    """Construct an LLM client pinned to the first configured Hydra Base LLM row."""
    redis_conn = kwargs.pop("redis_conn", None)
    has_explicit_provider = "provider" in kwargs
    has_explicit_api_key = "api_key" in kwargs
    host_hint = str(host or "").strip().lower()
    if host_hint and not has_explicit_provider:
        if host_hint.startswith("hf://"):
            kwargs["provider"] = HYDRA_LLM_PROVIDER_HF_TRANSFORMERS
            has_explicit_provider = True
        elif host_hint.startswith("llama-cpp://"):
            kwargs["provider"] = HYDRA_LLM_PROVIDER_LLAMA_CPP
            has_explicit_provider = True
        elif host_hint.startswith("mlx-lm://"):
            kwargs["provider"] = HYDRA_LLM_PROVIDER_MLX_LM
            has_explicit_provider = True
    if host or model or has_explicit_provider or has_explicit_api_key:
        return get_llm_client_from_env(host=host, model=model, redis_conn=redis_conn, **kwargs)
    client_kwargs = primary_hydra_llm_client_kwargs(redis_conn=redis_conn)
    client_kwargs.update(kwargs)
    return get_llm_client_from_env(**client_kwargs)

def _perf_nonnegative_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except Exception:
        return float(default)
    if result < 0.0:
        return 0.0
    return result


def _perf_nonnegative_int(value: Any, default: int = 0) -> int:
    try:
        result = int(float(value))
    except Exception:
        return int(default)
    if result < 0:
        return 0
    return result


def _build_llm_perf_stats(
    *,
    model: str,
    elapsed: Any,
    prompt_tokens: Any,
    completion_tokens: Any,
    total_tokens: Any,
    calls: Any,
    prompt_elapsed: Any = 0.0,
    completion_elapsed: Any = 0.0,
    speed_basis: str = "wall_time",
) -> Dict[str, Any]:
    elapsed_sec = _perf_nonnegative_float(elapsed)
    prompt_elapsed_sec = _perf_nonnegative_float(prompt_elapsed)
    completion_elapsed_sec = _perf_nonnegative_float(completion_elapsed)
    prompt_count = _perf_nonnegative_int(prompt_tokens)
    completion_count = _perf_nonnegative_int(completion_tokens)
    total_count = _perf_nonnegative_int(total_tokens)
    if total_count <= 0:
        total_count = max(0, prompt_count + completion_count)

    total_basis_sec = elapsed_sec
    completion_basis_sec = completion_elapsed_sec if completion_elapsed_sec > 0.0 else elapsed_sec
    prompt_basis_sec = prompt_elapsed_sec
    tps_total = (float(total_count) / total_basis_sec) if total_basis_sec > 0.0 and total_count > 0 else 0.0
    tps_comp = (
        float(completion_count) / completion_basis_sec
        if completion_basis_sec > 0.0 and completion_count > 0
        else 0.0
    )
    tps_prompt = (
        float(prompt_count) / prompt_basis_sec
        if prompt_basis_sec > 0.0 and prompt_count > 0
        else 0.0
    )

    return {
        "model": str(model or "LLM"),
        "elapsed": round(elapsed_sec, 6),
        "prompt_elapsed": round(prompt_elapsed_sec, 6),
        "completion_elapsed": round(completion_elapsed_sec, 6),
        "prompt_tokens": prompt_count,
        "completion_tokens": completion_count,
        "total_tokens": total_count,
        "tps_total": round(tps_total, 4),
        "tps_prompt": round(tps_prompt, 4),
        "tps_comp": round(tps_comp, 4),
        "calls": _perf_nonnegative_int(calls),
        "speed_basis": str(speed_basis or "wall_time"),
    }


def _timing_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    out: Dict[str, Any] = {}
    for name in (
        "prompt_ms",
        "predicted_ms",
        "completion_ms",
        "generation_ms",
        "prompt_per_second",
        "predicted_per_second",
        "completion_per_second",
        "generation_per_second",
        "prompt_tps",
        "generation_tps",
    ):
        try:
            attr = getattr(value, name)
        except Exception:
            continue
        if attr is not None:
            out[name] = attr
    return out


def _seconds_from_timing_ms(timings: Dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = timings.get(key)
        seconds = _perf_nonnegative_float(value)
        if seconds > 0.0:
            return seconds / 1000.0
    return 0.0


def _seconds_from_tps(tokens: int, timings: Dict[str, Any], *keys: str) -> float:
    if tokens <= 0:
        return 0.0
    for key in keys:
        tps = _perf_nonnegative_float(timings.get(key))
        if tps > 0.0:
            return float(tokens) / tps
    return 0.0


def _llama_cpp_response_timing(
    response: Any,
    *,
    fallback_elapsed: float,
    prompt_tokens: int,
    completion_tokens: int,
) -> Dict[str, Any]:
    timings: Dict[str, Any] = {}
    if isinstance(response, dict):
        for key in ("timings", "timing", "performance", "perf"):
            candidate = _timing_mapping(response.get(key))
            if candidate:
                timings.update(candidate)
                break
    prompt_elapsed = _seconds_from_timing_ms(timings, "prompt_ms", "prompt_eval_ms", "prefill_ms")
    completion_elapsed = _seconds_from_timing_ms(
        timings,
        "predicted_ms",
        "completion_ms",
        "generation_ms",
        "decode_ms",
    )
    if prompt_elapsed <= 0.0:
        prompt_elapsed = _seconds_from_tps(prompt_tokens, timings, "prompt_per_second", "prompt_tps")
    if completion_elapsed <= 0.0:
        completion_elapsed = _seconds_from_tps(
            completion_tokens,
            timings,
            "predicted_per_second",
            "completion_per_second",
            "generation_per_second",
            "generation_tps",
        )
    source = "llama_cpp_timing" if prompt_elapsed > 0.0 or completion_elapsed > 0.0 else "local_generate"
    if completion_elapsed <= 0.0:
        completion_elapsed = _perf_nonnegative_float(fallback_elapsed)
    return {
        "prompt_elapsed": prompt_elapsed,
        "completion_elapsed": completion_elapsed,
        "speed_basis": source,
    }


def _llama_cpp_count_text_tokens(model: Any, text: Any) -> int:
    content = _coerce_content_to_text(text)
    if not content:
        return 0
    tokenizer = getattr(model, "tokenize", None)
    if not callable(tokenizer):
        return 0
    raw = content.encode("utf-8", "ignore")
    for args, kwargs in (
        ((raw,), {"add_bos": False}),
        ((raw,), {}),
        ((content,), {"add_bos": False}),
        ((content,), {}),
    ):
        try:
            tokens = tokenizer(*args, **kwargs)
            return int(len(tokens or []))
        except Exception:
            continue
    return 0


class LLMClientWrapper:
    def __init__(self, host, model=None, **kwargs):
        explicit_api_key = str(kwargs.pop("api_key", "") or "").strip()
        resolved_host = str(host or "").strip()
        resolved_model = str(model or "").strip()
        if not resolved_host or not resolved_model:
            raise RuntimeError(HYDRA_LLM_SETUP_ERROR)

        base_url = _normalize_base_url(resolved_host)

        resolved_api_key = explicit_api_key or os.getenv("LLM_API_KEY") or "not-needed"
        self.client = AsyncOpenAI(
            base_url=base_url,
            api_key=resolved_api_key,
            **kwargs
        )

        self.host = base_url.rstrip("/")
        self.model = resolved_model
        self.api_key = resolved_api_key

        # Common generation defaults (caller can override per-call)
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.7"))

        # Per-instance perf aggregation for one chat turn.
        self._llm_calls = 0
        self._llm_elapsed_sec = 0.0
        self._llm_prompt_elapsed_sec = 0.0
        self._llm_completion_elapsed_sec = 0.0
        self._llm_prompt_tokens = 0
        self._llm_completion_tokens = 0
        self._llm_total_tokens = 0
        self._llm_model_last = str(resolved_model or "").strip() or "LLM"
        self._llm_speed_basis = "api_round_trip"

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
        out = _build_llm_perf_stats(
            model=str(self._llm_model_last or self.model or "LLM"),
            elapsed=self._llm_elapsed_sec,
            prompt_elapsed=getattr(self, "_llm_prompt_elapsed_sec", 0.0),
            completion_elapsed=getattr(self, "_llm_completion_elapsed_sec", 0.0),
            prompt_tokens=self._llm_prompt_tokens,
            completion_tokens=self._llm_completion_tokens,
            total_tokens=self._llm_total_tokens,
            calls=self._llm_calls,
            speed_basis=getattr(self, "_llm_speed_basis", "api_round_trip"),
        )

        if reset:
            self._llm_calls = 0
            self._llm_elapsed_sec = 0.0
            self._llm_prompt_elapsed_sec = 0.0
            self._llm_completion_elapsed_sec = 0.0
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
            _append_llm_debug_event(
                phase="prompt",
                level="info",
                message="Prompt submitted",
                call_id=call_id,
                provider=HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE,
                host=str(self.host or "").strip(),
                model=str(model or "").strip(),
                detail=f"messages={len(messages) if isinstance(messages, list) else 0}",
            )
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
                result = {
                    "model": getattr(response, "model", model),
                    "message": {"role": "assistant", "content": ""},
                }
                _append_llm_debug_result(
                    call_id=call_id,
                    provider=HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE,
                    host=str(self.host or "").strip(),
                    model=final_model,
                    result=result,
                    usage={
                        "prompt_tokens": prompt_tokens,
                        "completion_tokens": completion_tokens,
                        "total_tokens": total_tokens,
                    },
                    timing={"speed_basis": "api_round_trip"},
                    elapsed=elapsed,
                )
                return result

            choice = response.choices[0].message or {}
            raw_content = getattr(choice, "content", "") if hasattr(choice, "content") else choice.get("content", "")
            content_text = _strip_local_thinking_blocks(raw_content).strip()

            result = {
                "model": getattr(response, "model", model),
                "message": {"role": getattr(choice, "role", "assistant"), "content": content_text},
            }
            _append_llm_debug_result(
                call_id=call_id,
                provider=HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE,
                host=str(self.host or "").strip(),
                model=final_model,
                result=result,
                usage={
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                },
                timing={"speed_basis": "api_round_trip"},
                elapsed=elapsed,
            )
            return result
        except Exception as exc:
            call_error = exc
            raise
        finally:
            _finish_active_llm_call(
                call_id,
                error=call_error,
                response_model=final_model,
            )


class SpudLinkLLMClientWrapper:
    def __init__(self, host, model=None, **kwargs):
        explicit_api_key = str(kwargs.pop("api_key", "") or "").strip()
        resolved_host = str(host or "").strip().rstrip("/")
        resolved_model = str(model or "").strip() or "tater/base"
        if not resolved_host or not explicit_api_key:
            raise RuntimeError("Spud Link model routing is not paired. Connect this Spudlet to a Spud Hub first.")

        if resolved_host.endswith("/api/spudlink/v1/tater/llm"):
            endpoint = resolved_host
        elif resolved_host.endswith("/api/spudlink/v1"):
            endpoint = f"{resolved_host}/tater/llm"
        else:
            endpoint = f"{resolved_host}/api/spudlink/v1/tater/llm"

        self.host = endpoint
        self.model = resolved_model
        self.api_key = explicit_api_key
        self.provider = HYDRA_LLM_PROVIDER_SPUD_LINK
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.7"))
        self._client = httpx.AsyncClient() if httpx is not None else None

        self._llm_calls = 0
        self._llm_elapsed_sec = 0.0
        self._llm_prompt_elapsed_sec = 0.0
        self._llm_completion_elapsed_sec = 0.0
        self._llm_prompt_tokens = 0
        self._llm_completion_tokens = 0
        self._llm_total_tokens = 0
        self._llm_model_last = resolved_model
        self._llm_speed_basis = "spud_link_native"

    async def aclose(self):
        client = getattr(self, "_client", None)
        if client is not None:
            await client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    def get_perf_stats(self, *, reset: bool = False) -> Dict[str, Any]:
        out = _build_llm_perf_stats(
            model=str(self._llm_model_last or self.model or "Spud Link"),
            elapsed=self._llm_elapsed_sec,
            prompt_elapsed=getattr(self, "_llm_prompt_elapsed_sec", 0.0),
            completion_elapsed=getattr(self, "_llm_completion_elapsed_sec", 0.0),
            prompt_tokens=self._llm_prompt_tokens,
            completion_tokens=self._llm_completion_tokens,
            total_tokens=self._llm_total_tokens,
            calls=self._llm_calls,
            speed_basis=getattr(self, "_llm_speed_basis", "spud_link_native"),
        )
        if reset:
            self._llm_calls = 0
            self._llm_elapsed_sec = 0.0
            self._llm_prompt_elapsed_sec = 0.0
            self._llm_completion_elapsed_sec = 0.0
            self._llm_prompt_tokens = 0
            self._llm_completion_tokens = 0
            self._llm_total_tokens = 0
        return out

    async def chat(self, messages, **kwargs):
        timeout = kwargs.pop("timeout", None)
        timeout_ms = kwargs.pop("timeout_ms", None)
        if timeout is None and timeout_ms is not None:
            try:
                timeout = float(timeout_ms) / 1000.0
            except Exception:
                timeout = None
        timeout = float(timeout or 120.0)

        stream = bool(kwargs.pop("stream", False))
        if stream:
            raise RuntimeError("Spud Link native LLM routing does not support streaming model calls yet.")
        activity_hint = kwargs.pop("activity", "")
        model = str(kwargs.pop("model", self.model) or self.model).strip() or "tater/base"

        if "max_tokens" not in kwargs:
            kwargs["max_tokens"] = self.max_tokens
        elif kwargs.get("max_tokens") is None:
            kwargs.pop("max_tokens", None)
        if "temperature" not in kwargs:
            kwargs["temperature"] = self.temperature

        try:
            messages = _sanitize_chat_messages(messages if isinstance(messages, list) else [])
        except Exception:
            messages = messages if isinstance(messages, list) else []

        call_id = _register_active_llm_call(
            host=str(self.host or "").strip(),
            model=model,
            stream=False,
            message_count=(len(messages) if isinstance(messages, list) else 0),
            messages=(messages if isinstance(messages, list) else []),
            activity_hint=str(activity_hint or "spud_link_native"),
        )
        call_error: Optional[Exception] = None
        final_model = model

        try:
            _append_llm_debug_event(
                phase="prompt",
                level="info",
                message="Native Spud Link prompt submitted",
                call_id=call_id,
                provider=HYDRA_LLM_PROVIDER_SPUD_LINK,
                host=str(self.host or "").strip(),
                model=model,
                detail=f"messages={len(messages) if isinstance(messages, list) else 0}",
            )
            body = {
                "model": model,
                "messages": messages,
                "stream": False,
            }
            for key in ("max_tokens", "temperature", "top_p", "stop"):
                if key in kwargs:
                    body[key] = kwargs[key]

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "X-Spudlink-Token": self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            started_at = asyncio.get_running_loop().time()
            if self._client is not None:
                response = await self._client.post(self.host, json=body, headers=headers, timeout=timeout)
                response.raise_for_status()
                payload = response.json()
            else:
                def _post_sync() -> Any:
                    response_sync = requests.post(self.host, json=body, headers=headers, timeout=timeout)
                    response_sync.raise_for_status()
                    return response_sync.json()

                payload = await asyncio.to_thread(_post_sync)

            elapsed = max(0.0, float(asyncio.get_running_loop().time() - started_at))
            self._llm_calls += 1
            self._llm_elapsed_sec += elapsed

            if not isinstance(payload, dict):
                raise RuntimeError("Spud Link Hub returned an invalid native LLM response.")
            response_model = str(payload.get("model") or model or "").strip()
            if response_model:
                self._llm_model_last = response_model
                final_model = response_model

            usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
            prompt_tokens = 0
            completion_tokens = 0
            total_tokens = 0
            try:
                prompt_tokens = int(usage.get("prompt_tokens") or 0)
                completion_tokens = int(usage.get("completion_tokens") or 0)
                total_tokens = int(usage.get("total_tokens") or 0)
            except Exception:
                prompt_tokens = completion_tokens = total_tokens = 0
            if total_tokens <= 0:
                total_tokens = max(0, prompt_tokens + completion_tokens)
            self._llm_prompt_tokens += max(0, prompt_tokens)
            self._llm_completion_tokens += max(0, completion_tokens)
            self._llm_total_tokens += max(0, total_tokens)

            message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
            content_text = _strip_local_thinking_blocks(str(message.get("content") or "")).strip()
            result = {
                "model": response_model or model,
                "message": {
                    "role": str(message.get("role") or "assistant"),
                    "content": content_text,
                },
            }
            _append_llm_debug_result(
                call_id=call_id,
                provider=HYDRA_LLM_PROVIDER_SPUD_LINK,
                host=str(self.host or "").strip(),
                model=final_model,
                result=result,
                usage={
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                },
                timing={"speed_basis": "spud_link_native"},
                elapsed=elapsed,
            )
            return result
        except Exception as exc:
            call_error = exc
            raise
        finally:
            _finish_active_llm_call(call_id, error=call_error, response_model=final_model)


class TransformersLLMClientWrapper:
    def __init__(self, model=None, **kwargs):
        _ = kwargs
        resolved_model = str(model or "").strip()
        if not resolved_model:
            raise RuntimeError(HYDRA_LLM_SETUP_ERROR)

        self.host = "hf://transformers"
        self.model = resolved_model
        self.provider = HYDRA_LLM_PROVIDER_HF_TRANSFORMERS
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.7"))

        self._llm_calls = 0
        self._llm_elapsed_sec = 0.0
        self._llm_prompt_elapsed_sec = 0.0
        self._llm_completion_elapsed_sec = 0.0
        self._llm_prompt_tokens = 0
        self._llm_completion_tokens = 0
        self._llm_total_tokens = 0
        self._llm_model_last = resolved_model
        self._llm_speed_basis = "local_generate"

    async def aclose(self):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    def get_perf_stats(self, *, reset: bool = False) -> Dict[str, Any]:
        out = _build_llm_perf_stats(
            model=str(self._llm_model_last or self.model or "HF Transformers"),
            elapsed=self._llm_elapsed_sec,
            prompt_elapsed=getattr(self, "_llm_prompt_elapsed_sec", 0.0),
            completion_elapsed=getattr(self, "_llm_completion_elapsed_sec", 0.0),
            prompt_tokens=self._llm_prompt_tokens,
            completion_tokens=self._llm_completion_tokens,
            total_tokens=self._llm_total_tokens,
            calls=self._llm_calls,
            speed_basis=getattr(self, "_llm_speed_basis", "local_generate"),
        )

        if reset:
            self._llm_calls = 0
            self._llm_elapsed_sec = 0.0
            self._llm_prompt_elapsed_sec = 0.0
            self._llm_completion_elapsed_sec = 0.0
            self._llm_prompt_tokens = 0
            self._llm_completion_tokens = 0
            self._llm_total_tokens = 0
        return out

    def _format_messages_without_template(self, messages: List[Dict[str, Any]]) -> str:
        parts: List[str] = []
        for item in (messages if isinstance(messages, list) else []):
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "user").strip().lower()
            content = _coerce_content_to_text(item.get("content")).strip()
            if not content:
                continue
            if role == "system":
                parts.append(f"System: {content}")
            elif role == "assistant":
                parts.append(f"Assistant: {content}")
            else:
                parts.append(f"User: {content}")
        parts.append("Assistant:")
        return "\n\n".join(parts).strip()

    def _messages_with_text_blocks(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for item in (messages if isinstance(messages, list) else []):
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "user").strip().lower() or "user"
            content = item.get("content")
            if isinstance(content, list):
                normalized.append({"role": role, "content": content})
                continue
            text = _coerce_content_to_text(content).strip()
            normalized.append({"role": role, "content": [{"type": "text", "text": text}]})
        return normalized

    def _model_input_device(self, bundle: Dict[str, Any]) -> str:
        model = bundle.get("model")
        device = str(bundle.get("device") or "cpu")
        try:
            first_param = next(model.parameters())
            return str(getattr(first_param, "device", None) or device)
        except Exception:
            return device

    def _move_encoded_to_device(self, encoded: Any, device: str) -> Any:
        if hasattr(encoded, "to"):
            try:
                return encoded.to(device)
            except Exception:
                pass
        if isinstance(encoded, dict):
            return {key: value.to(device) if hasattr(value, "to") else value for key, value in encoded.items()}
        return encoded

    def _chat_template_inputs(
        self,
        formatter: Any,
        messages: List[Dict[str, Any]],
        *,
        tokenize: bool,
        return_dict: bool = False,
    ) -> Any:
        base_kwargs: Dict[str, Any] = {
            "add_generation_prompt": True,
            "tokenize": bool(tokenize),
        }
        if tokenize:
            base_kwargs.update({"return_tensors": "pt"})
        if return_dict:
            base_kwargs["return_dict"] = True
        variants = [messages]
        block_messages = self._messages_with_text_blocks(messages)
        if block_messages != messages:
            variants.append(block_messages)
        last_exc: Optional[Exception] = None
        thinking_variants = _local_no_thinking_template_variants(_hf_transformers_disable_thinking_enabled())
        for variant in variants:
            for extra in thinking_variants:
                try:
                    return formatter.apply_chat_template(variant, **base_kwargs, **extra)
                except TypeError as exc:
                    last_exc = exc
                    continue
                except Exception as exc:
                    last_exc = exc
                    break
        if last_exc is not None:
            raise last_exc
        return formatter.apply_chat_template(messages, **base_kwargs)

    def _encode_messages(self, bundle: Dict[str, Any], messages: List[Dict[str, Any]]) -> Tuple[Any, int]:
        tokenizer = bundle["tokenizer"]
        processor = bundle.get("processor")
        formatter = processor if processor is not None and hasattr(processor, "apply_chat_template") else tokenizer
        input_builder = processor if processor is not None and callable(processor) else tokenizer
        torch = bundle["torch"]
        device = self._model_input_device(bundle)
        max_input_tokens = _hf_llm_max_input_tokens()
        images = _extract_pil_images_from_messages(messages)

        if images and processor is not None:
            prompt_text = ""
            vision_messages = _messages_with_hf_image_blocks(messages, images)
            try:
                prompt_text = self._chat_template_inputs(
                    formatter,
                    vision_messages,
                    tokenize=False,
                    return_dict=False,
                )
            except Exception:
                prompt_text = self._format_messages_without_template(messages)
            prompt_text = _coerce_content_to_text(prompt_text).strip()
            if not prompt_text:
                prompt_text = self._format_messages_without_template(messages)
            processor_variants = (
                {"text": [prompt_text], "images": images, "return_tensors": "pt"},
                {"text": prompt_text, "images": images[0] if len(images) == 1 else images, "return_tensors": "pt"},
                {"text": [prompt_text], "images": images[0] if len(images) == 1 else images, "return_tensors": "pt"},
            )
            last_exc: Optional[Exception] = None
            for call_kwargs in processor_variants:
                try:
                    encoded = processor(**call_kwargs)
                    encoded = self._move_encoded_to_device(encoded, device)
                    encoded_fields = dict(encoded)
                    input_ids = encoded_fields.get("input_ids")
                    attention_mask = encoded_fields.get("attention_mask")
                    if input_ids is not None and hasattr(input_ids, "dim") and input_ids.dim() == 1:
                        input_ids = input_ids.unsqueeze(0)
                        encoded_fields["input_ids"] = input_ids
                    if attention_mask is not None and hasattr(attention_mask, "dim") and attention_mask.dim() == 1:
                        attention_mask = attention_mask.unsqueeze(0)
                        encoded_fields["attention_mask"] = attention_mask
                    input_len = int(getattr(input_ids, "shape", [0])[-1] or 0) if input_ids is not None else 0
                    return encoded_fields, input_len
                except Exception as exc:
                    last_exc = exc
                    continue
            raise RuntimeError(f"Could not encode image input for Hugging Face vision model: {last_exc}")

        input_ids = None
        attention_mask = None
        encoded_fields: Dict[str, Any] = {}
        try:
            input_ids = self._chat_template_inputs(formatter, messages, tokenize=True, return_dict=True)
        except Exception:
            input_ids = None

        if input_ids is None:
            try:
                input_ids = self._chat_template_inputs(formatter, messages, tokenize=True, return_dict=False)
            except Exception:
                input_ids = None

        if input_ids is None and formatter is not tokenizer:
            try:
                input_ids = self._chat_template_inputs(tokenizer, messages, tokenize=True, return_dict=False)
            except Exception:
                input_ids = None

        if input_ids is None:
            try:
                prompt_text = self._chat_template_inputs(formatter, messages, tokenize=False, return_dict=False)
                input_ids = input_builder(text=prompt_text, return_tensors="pt")
            except Exception:
                input_ids = None

        if input_ids is None and formatter is not tokenizer:
            try:
                prompt_text = self._chat_template_inputs(tokenizer, messages, tokenize=False, return_dict=False)
                input_ids = tokenizer(prompt_text, return_tensors="pt")
            except Exception:
                input_ids = None

        if input_ids is not None:
            try:
                if hasattr(input_ids, "to") and not isinstance(input_ids, dict):
                    input_ids = input_ids.to(device)
                elif isinstance(input_ids, dict):
                    input_ids = self._move_encoded_to_device(input_ids, device)
            except Exception:
                pass

        if input_ids is not None:
            try:
                input_ids = dict(input_ids)
            except Exception:
                pass

        if input_ids is not None:
            try:
                if isinstance(input_ids, dict):
                    encoded_fields = dict(input_ids)
                    attention_mask = encoded_fields.get("attention_mask")
                    input_ids = encoded_fields.get("input_ids")
            except Exception:
                pass
            try:
                if input_ids is not None and hasattr(input_ids, "dim") and input_ids.dim() == 1:
                    input_ids = input_ids.unsqueeze(0)
                    if attention_mask is not None and hasattr(attention_mask, "dim") and attention_mask.dim() == 1:
                        attention_mask = attention_mask.unsqueeze(0)
                if input_ids is not None and max_input_tokens > 0 and int(input_ids.shape[-1]) > max_input_tokens:
                    input_ids = input_ids[:, -max_input_tokens:]
                    if attention_mask is not None:
                        attention_mask = attention_mask[:, -max_input_tokens:]
                if input_ids is not None:
                    if attention_mask is None:
                        attention_mask = torch.ones_like(input_ids)
                    input_ids = input_ids.to(device)
                    attention_mask = attention_mask.to(device)
                    encoded_fields["input_ids"] = input_ids
                    encoded_fields["attention_mask"] = attention_mask
                    encoded_fields = self._move_encoded_to_device(encoded_fields, device)
                    return encoded_fields, int(input_ids.shape[-1])
            except Exception:
                pass

        prompt = self._format_messages_without_template(messages)
        try:
            encoded = input_builder(
                text=prompt,
                return_tensors="pt",
                truncation=True,
                max_length=max_input_tokens,
            )
        except Exception:
            encoded = tokenizer(
                prompt,
                return_tensors="pt",
                truncation=True,
                max_length=max_input_tokens,
            )
        encoded = self._move_encoded_to_device(encoded, device)
        input_len = int(encoded["input_ids"].shape[-1])
        return encoded, input_len

    def _decode_completion(self, bundle: Dict[str, Any], completion_ids: Any) -> str:
        processor = bundle.get("processor")
        tokenizer = bundle.get("tokenizer")
        decoder = processor if processor is not None and hasattr(processor, "decode") else tokenizer
        try:
            raw = decoder.decode(completion_ids, skip_special_tokens=False)
        except Exception:
            raw = tokenizer.decode(completion_ids, skip_special_tokens=True)
        parser = getattr(processor, "parse_response", None) if processor is not None else None
        if callable(parser):
            try:
                parsed = parser(raw)
                if isinstance(parsed, str):
                    return parsed
                if isinstance(parsed, dict):
                    for key in ("response", "text", "content"):
                        value = parsed.get(key)
                        if isinstance(value, str) and value.strip():
                            return value
                if parsed is not None:
                    return _coerce_content_to_text(parsed)
            except Exception:
                pass
        try:
            return tokenizer.decode(completion_ids, skip_special_tokens=True)
        except Exception:
            return _coerce_content_to_text(raw)

    def _build_generation_kwargs(self, tokenizer: Any, timeout: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        generation_kwargs: Dict[str, Any] = {}
        max_new_tokens = kwargs.pop("max_tokens", self.max_tokens)
        try:
            generation_kwargs["max_new_tokens"] = max(1, int(max_new_tokens))
        except Exception:
            generation_kwargs["max_new_tokens"] = self.max_tokens

        temperature = kwargs.pop("temperature", self.temperature)
        try:
            temperature_value = float(temperature)
        except Exception:
            temperature_value = self.temperature
        if temperature_value > 0:
            generation_kwargs["temperature"] = temperature_value
            generation_kwargs["do_sample"] = bool(kwargs.pop("do_sample", True))
        else:
            generation_kwargs["do_sample"] = bool(kwargs.pop("do_sample", False))

        for key in ("top_p", "top_k", "repetition_penalty", "num_beams", "min_new_tokens"):
            if key in kwargs and kwargs.get(key) is not None:
                generation_kwargs[key] = kwargs.pop(key)

        try:
            timeout_value = float(timeout) if timeout is not None else 0.0
        except Exception:
            timeout_value = 0.0
        if timeout_value > 0:
            generation_kwargs["max_time"] = timeout_value

        eos_token_id = getattr(tokenizer, "eos_token_id", None)
        pad_token_id = getattr(tokenizer, "pad_token_id", None)
        if eos_token_id is not None:
            generation_kwargs["eos_token_id"] = eos_token_id
        if pad_token_id is not None:
            generation_kwargs["pad_token_id"] = pad_token_id
        elif eos_token_id is not None:
            generation_kwargs["pad_token_id"] = eos_token_id

        return generation_kwargs

    def _apply_stop_sequences(self, text: str, stop: Any) -> str:
        if not stop:
            return text
        stops = stop if isinstance(stop, list) else [stop]
        cut_at: Optional[int] = None
        for item in stops:
            token = str(item or "")
            if not token:
                continue
            idx = text.find(token)
            if idx >= 0:
                cut_at = idx if cut_at is None else min(cut_at, idx)
        if cut_at is None:
            return text
        return text[:cut_at]

    def _chat_sync(self, messages: List[Dict[str, Any]], *, timeout: Any = None, **kwargs) -> Dict[str, Any]:
        stop = kwargs.pop("stop", None)
        bundle = _load_hf_llm_bundle(self.model)
        model = bundle["model"]
        tokenizer = bundle["tokenizer"]
        torch = bundle["torch"]
        generation_lock = bundle["lock"]

        encoded, prompt_tokens = self._encode_messages(bundle, messages)
        generation_kwargs = self._build_generation_kwargs(tokenizer, timeout, kwargs)

        with generation_lock:
            generation_started = time.perf_counter()
            with torch.inference_mode():
                output_ids = model.generate(**encoded, **generation_kwargs)
        generation_elapsed = max(0.0, time.perf_counter() - generation_started)

        input_ids = encoded["input_ids"]
        is_encoder_decoder = bool(getattr(getattr(model, "config", None), "is_encoder_decoder", False))
        completion_ids = output_ids[0] if is_encoder_decoder else output_ids[0][int(input_ids.shape[-1]):]
        content = self._decode_completion(bundle, completion_ids)
        content = self._apply_stop_sequences(_coerce_content_to_text(content).strip(), stop).strip()
        completion_tokens = int(getattr(completion_ids, "shape", [0])[-1] or 0)
        total_tokens = max(0, int(prompt_tokens) + int(completion_tokens))

        return {
            "model": self.model,
            "message": {"role": "assistant", "content": content},
            "_usage": {
                "prompt_tokens": int(prompt_tokens),
                "completion_tokens": int(completion_tokens),
                "total_tokens": int(total_tokens),
            },
            "_timing": {
                "completion_elapsed": generation_elapsed,
                "speed_basis": "local_generate",
            },
        }

    async def chat(self, messages, **kwargs):
        timeout = kwargs.pop("timeout", None)
        timeout_ms = kwargs.pop("timeout_ms", None)
        if timeout is None and timeout_ms is not None:
            try:
                timeout = float(timeout_ms) / 1000.0
            except Exception:
                timeout = None

        stream = kwargs.pop("stream", False)
        if stream:
            raise RuntimeError("Hugging Face Transformers LLM backend does not support streaming yet.")

        activity_hint = kwargs.pop("activity", "")
        model = kwargs.pop("model", self.model)
        if model and str(model).strip() != self.model:
            self.model = str(model).strip()

        if "max_tokens" not in kwargs:
            kwargs["max_tokens"] = self.max_tokens
        elif kwargs.get("max_tokens") is None:
            kwargs.pop("max_tokens", None)
        if "temperature" not in kwargs:
            kwargs["temperature"] = self.temperature

        try:
            if _vision_payload_has_image_url(messages):
                messages = messages if isinstance(messages, list) else []
            else:
                messages = _sanitize_chat_messages(messages if isinstance(messages, list) else [])
        except Exception:
            messages = messages if isinstance(messages, list) else []

        call_id = _register_active_llm_call(
            host=self.host,
            model=str(self.model or "").strip(),
            stream=False,
            message_count=(len(messages) if isinstance(messages, list) else 0),
            messages=(messages if isinstance(messages, list) else []),
            activity_hint=str(activity_hint or ""),
        )
        call_error: Optional[Exception] = None
        final_model = str(self.model or "").strip()
        started_at = asyncio.get_running_loop().time()

        try:
            _append_llm_debug_event(
                phase="prompt",
                level="info",
                message="Local prompt submitted",
                call_id=call_id,
                provider=HYDRA_LLM_PROVIDER_HF_TRANSFORMERS,
                host=self.host,
                model=str(self.model or "").strip(),
                detail=f"messages={len(messages) if isinstance(messages, list) else 0}",
            )
            result = await asyncio.to_thread(self._chat_sync, messages, timeout=timeout, **kwargs)
            elapsed = max(0.0, float(asyncio.get_running_loop().time() - started_at))
            usage = result.pop("_usage", {}) if isinstance(result, dict) else {}
            timing = result.pop("_timing", {}) if isinstance(result, dict) else {}
            prompt_tokens = int((usage or {}).get("prompt_tokens") or 0)
            completion_tokens = int((usage or {}).get("completion_tokens") or 0)
            total_tokens = int((usage or {}).get("total_tokens") or (prompt_tokens + completion_tokens))
            completion_elapsed = _perf_nonnegative_float((timing or {}).get("completion_elapsed"), elapsed)

            self._llm_calls += 1
            self._llm_elapsed_sec += elapsed
            self._llm_prompt_elapsed_sec += _perf_nonnegative_float((timing or {}).get("prompt_elapsed"))
            self._llm_completion_elapsed_sec += completion_elapsed
            self._llm_prompt_tokens += max(0, prompt_tokens)
            self._llm_completion_tokens += max(0, completion_tokens)
            self._llm_total_tokens += max(0, total_tokens)
            self._llm_model_last = str((result or {}).get("model") or self.model)
            self._llm_speed_basis = str((timing or {}).get("speed_basis") or "local_generate")
            final_model = self._llm_model_last
            _append_llm_debug_result(
                call_id=call_id,
                provider=HYDRA_LLM_PROVIDER_HF_TRANSFORMERS,
                host=self.host,
                model=final_model,
                result=result,
                usage={
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                },
                timing=timing,
                elapsed=elapsed,
            )
            return result
        except Exception as exc:
            call_error = exc
            raise
        finally:
            _finish_active_llm_call(
                call_id,
                error=call_error,
                response_model=final_model,
            )


def _strip_local_thinking_blocks(text: Any) -> str:
    content = _coerce_content_to_text(text)
    if not content:
        return ""
    content = re.sub(r"(?is)<think>.*?</think>", "", content).strip()
    if re.search(r"(?is)</think>", content) and not re.search(r"(?is)<think>", content):
        content = re.sub(r"(?is)^.*?</think>", "", content).strip()
    content = re.sub(r"(?is)^<think>.*", "", content).strip()
    content = re.sub(r"(?is)<\|channel\>thought\s*.*?<channel\|>", "", content).strip()
    if re.search(r"(?is)<channel\|>", content) and not re.search(r"(?is)<\|channel\>thought", content):
        content = re.sub(r"(?is)^.*?<channel\|>", "", content).strip()
    content = re.sub(r"(?is)^<\|channel\>thought\s*.*", "", content).strip()
    return content


def _llama_cpp_disable_thinking_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(item) for item in (messages if isinstance(messages, list) else []) if isinstance(item, dict)]


class LlamaCppLLMClientWrapper:
    def __init__(self, model=None, **kwargs):
        vision = kwargs.pop("vision", False)
        _ = kwargs
        resolved_model = str(model or "").strip()
        if not resolved_model:
            raise RuntimeError(HYDRA_LLM_SETUP_ERROR)

        self.host = "llama-cpp://local"
        self.model = resolved_model
        self.provider = HYDRA_LLM_PROVIDER_LLAMA_CPP
        self.vision = _boolish(vision, default=False)
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.7"))

        self._llm_calls = 0
        self._llm_elapsed_sec = 0.0
        self._llm_prompt_elapsed_sec = 0.0
        self._llm_completion_elapsed_sec = 0.0
        self._llm_prompt_tokens = 0
        self._llm_completion_tokens = 0
        self._llm_total_tokens = 0
        self._llm_model_last = resolved_model
        self._llm_speed_basis = "local_generate"

    async def aclose(self):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    def get_perf_stats(self, *, reset: bool = False) -> Dict[str, Any]:
        out = _build_llm_perf_stats(
            model=str(self._llm_model_last or self.model or "llama.cpp"),
            elapsed=self._llm_elapsed_sec,
            prompt_elapsed=getattr(self, "_llm_prompt_elapsed_sec", 0.0),
            completion_elapsed=getattr(self, "_llm_completion_elapsed_sec", 0.0),
            prompt_tokens=self._llm_prompt_tokens,
            completion_tokens=self._llm_completion_tokens,
            total_tokens=self._llm_total_tokens,
            calls=self._llm_calls,
            speed_basis=getattr(self, "_llm_speed_basis", "local_generate"),
        )
        if reset:
            self._llm_calls = 0
            self._llm_elapsed_sec = 0.0
            self._llm_prompt_elapsed_sec = 0.0
            self._llm_completion_elapsed_sec = 0.0
            self._llm_prompt_tokens = 0
            self._llm_completion_tokens = 0
            self._llm_total_tokens = 0
        return out

    def _build_chat_kwargs(self, timeout: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        chat_kwargs: Dict[str, Any] = {}
        chat_template_kwargs = kwargs.pop("chat_template_kwargs", None)
        if isinstance(chat_template_kwargs, dict):
            chat_template_kwargs = dict(chat_template_kwargs)
        else:
            chat_template_kwargs = {}
        if _llama_cpp_disable_thinking_enabled():
            chat_template_kwargs = _merge_template_kwargs(
                {"enable_thinking": False, "reasoning_budget": 0},
                chat_template_kwargs,
            )
            chat_kwargs["reasoning_budget"] = 0
        if chat_template_kwargs:
            chat_kwargs["chat_template_kwargs"] = chat_template_kwargs
        max_tokens = kwargs.pop("max_tokens", self.max_tokens)
        try:
            chat_kwargs["max_tokens"] = max(1, int(max_tokens))
        except Exception:
            chat_kwargs["max_tokens"] = self.max_tokens
        temperature = kwargs.pop("temperature", self.temperature)
        try:
            chat_kwargs["temperature"] = float(temperature)
        except Exception:
            chat_kwargs["temperature"] = self.temperature
        for key in (
            "top_p",
            "top_k",
            "min_p",
            "typical_p",
            "repeat_penalty",
            "presence_penalty",
            "frequency_penalty",
            "stop",
            "seed",
        ):
            if key in kwargs and kwargs.get(key) is not None:
                chat_kwargs[key] = kwargs.pop(key)
        try:
            timeout_value = float(timeout) if timeout is not None else 0.0
        except Exception:
            timeout_value = 0.0
        if timeout_value > 0 and "max_tokens" not in chat_kwargs:
            chat_kwargs["max_tokens"] = self.max_tokens
        chat_kwargs["stream"] = False
        return chat_kwargs

    def _create_chat_completion_with_fallback(
        self,
        model: Any,
        messages: List[Dict[str, Any]],
        chat_kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        attempt_kwargs = dict(chat_kwargs)
        last_exc: Optional[TypeError] = None
        for _attempt in range(6):
            try:
                return model.create_chat_completion(messages=messages, **attempt_kwargs)
            except TypeError as exc:
                last_exc = exc
                retry_kwargs = dict(attempt_kwargs)
                template_kwargs = retry_kwargs.get("chat_template_kwargs")
                template_kwargs = dict(template_kwargs) if isinstance(template_kwargs, dict) else None
                changed = False

                if "reasoning_budget" in retry_kwargs:
                    retry_kwargs.pop("reasoning_budget", None)
                    changed = True
                elif template_kwargs is not None and "reasoning_budget" in template_kwargs:
                    template_kwargs.pop("reasoning_budget", None)
                    retry_kwargs["chat_template_kwargs"] = template_kwargs
                    changed = True
                elif template_kwargs is not None and "enable_thinking" in template_kwargs:
                    template_kwargs.pop("enable_thinking", None)
                    if template_kwargs:
                        retry_kwargs["chat_template_kwargs"] = template_kwargs
                    else:
                        retry_kwargs.pop("chat_template_kwargs", None)
                    changed = True
                elif "chat_template_kwargs" in retry_kwargs:
                    retry_kwargs.pop("chat_template_kwargs", None)
                    changed = True

                if not changed or retry_kwargs == attempt_kwargs:
                    raise
                attempt_kwargs = retry_kwargs
        if last_exc is not None:
            raise last_exc
        return model.create_chat_completion(messages=messages, **attempt_kwargs)

    def _chat_sync(self, messages: List[Dict[str, Any]], *, timeout: Any = None, **kwargs) -> Dict[str, Any]:
        vision_requested = bool(self.vision or _vision_payload_has_image_url(messages))
        bundle = _load_llama_cpp_bundle(self.model, vision=vision_requested)
        model = bundle["model"]
        generation_lock = bundle["lock"]
        chat_kwargs = self._build_chat_kwargs(timeout, kwargs)
        local_messages = _llama_cpp_disable_thinking_messages(messages)

        with generation_lock:
            generation_started = time.perf_counter()
            response = self._create_chat_completion_with_fallback(model, local_messages, chat_kwargs)
        generation_elapsed = max(0.0, time.perf_counter() - generation_started)

        choices = response.get("choices") if isinstance(response, dict) else []
        first_choice = choices[0] if isinstance(choices, list) and choices else {}
        message = first_choice.get("message") if isinstance(first_choice, dict) else {}
        content = ""
        if isinstance(message, dict):
            content = _coerce_content_to_text(message.get("content"))
        if not content and isinstance(first_choice, dict):
            content = _coerce_content_to_text(first_choice.get("text"))
        content = _strip_local_thinking_blocks(content).strip()
        usage = response.get("usage", {}) if isinstance(response, dict) else {}
        prompt_tokens = int((usage or {}).get("prompt_tokens") or 0)
        completion_tokens = int((usage or {}).get("completion_tokens") or 0)
        if completion_tokens <= 0:
            completion_tokens = _llama_cpp_count_text_tokens(model, content)
        total_tokens = int((usage or {}).get("total_tokens") or (prompt_tokens + completion_tokens))
        timing = _llama_cpp_response_timing(
            response,
            fallback_elapsed=generation_elapsed,
            prompt_tokens=max(0, prompt_tokens),
            completion_tokens=max(0, completion_tokens),
        )
        return {
            "model": self.model,
            "message": {"role": "assistant", "content": content},
            "_usage": {
                "prompt_tokens": max(0, prompt_tokens),
                "completion_tokens": max(0, completion_tokens),
                "total_tokens": max(0, total_tokens),
            },
            "_timing": timing,
        }

    async def chat(self, messages, **kwargs):
        timeout = kwargs.pop("timeout", None)
        timeout_ms = kwargs.pop("timeout_ms", None)
        if timeout is None and timeout_ms is not None:
            try:
                timeout = float(timeout_ms) / 1000.0
            except Exception:
                timeout = None
        stream = kwargs.pop("stream", False)
        if stream:
            raise RuntimeError("llama.cpp provider does not support streaming yet.")
        activity_hint = kwargs.pop("activity", "")
        model = kwargs.pop("model", self.model)
        if model and str(model).strip() != self.model:
            self.model = str(model).strip()
        if "max_tokens" not in kwargs:
            kwargs["max_tokens"] = self.max_tokens
        elif kwargs.get("max_tokens") is None:
            kwargs.pop("max_tokens", None)
        if "temperature" not in kwargs:
            kwargs["temperature"] = self.temperature
        try:
            if _vision_payload_has_image_url(messages):
                messages = messages if isinstance(messages, list) else []
            else:
                messages = _sanitize_chat_messages(messages if isinstance(messages, list) else [])
        except Exception:
            messages = messages if isinstance(messages, list) else []

        call_id = _register_active_llm_call(
            host=self.host,
            model=str(self.model or "").strip(),
            stream=False,
            message_count=(len(messages) if isinstance(messages, list) else 0),
            messages=(messages if isinstance(messages, list) else []),
            activity_hint=str(activity_hint or ""),
        )
        call_error: Optional[Exception] = None
        final_model = str(self.model or "").strip()
        started_at = asyncio.get_running_loop().time()
        try:
            _append_llm_debug_event(
                phase="prompt",
                level="info",
                message="Local prompt submitted",
                call_id=call_id,
                provider=HYDRA_LLM_PROVIDER_LLAMA_CPP,
                host=self.host,
                model=str(self.model or "").strip(),
                detail=f"messages={len(messages) if isinstance(messages, list) else 0}",
            )
            result = await asyncio.to_thread(self._chat_sync, messages, timeout=timeout, **kwargs)
            elapsed = max(0.0, float(asyncio.get_running_loop().time() - started_at))
            usage = result.pop("_usage", {}) if isinstance(result, dict) else {}
            timing = result.pop("_timing", {}) if isinstance(result, dict) else {}
            prompt_tokens = int((usage or {}).get("prompt_tokens") or 0)
            completion_tokens = int((usage or {}).get("completion_tokens") or 0)
            total_tokens = int((usage or {}).get("total_tokens") or (prompt_tokens + completion_tokens))
            completion_elapsed = _perf_nonnegative_float((timing or {}).get("completion_elapsed"), elapsed)
            self._llm_calls += 1
            self._llm_elapsed_sec += elapsed
            self._llm_prompt_elapsed_sec += _perf_nonnegative_float((timing or {}).get("prompt_elapsed"))
            self._llm_completion_elapsed_sec += completion_elapsed
            self._llm_prompt_tokens += max(0, prompt_tokens)
            self._llm_completion_tokens += max(0, completion_tokens)
            self._llm_total_tokens += max(0, total_tokens)
            self._llm_model_last = str((result or {}).get("model") or self.model)
            self._llm_speed_basis = str((timing or {}).get("speed_basis") or "local_generate")
            final_model = self._llm_model_last
            _append_llm_debug_result(
                call_id=call_id,
                provider=HYDRA_LLM_PROVIDER_LLAMA_CPP,
                host=self.host,
                model=final_model,
                result=result,
                usage={
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                },
                timing=timing,
                elapsed=elapsed,
            )
            return result
        except Exception as exc:
            call_error = exc
            raise
        finally:
            _finish_active_llm_call(
                call_id,
                error=call_error,
                response_model=final_model,
            )


def _mlx_lm_disable_thinking_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(item) for item in (messages if isinstance(messages, list) else []) if isinstance(item, dict)]


def _apply_local_stop_sequences(text: Any, stop: Any) -> str:
    content = _coerce_content_to_text(text)
    if not content or not stop:
        return content
    stops = stop if isinstance(stop, list) else [stop]
    cut_at: Optional[int] = None
    for item in stops:
        token = str(item or "")
        if not token:
            continue
        idx = content.find(token)
        if idx >= 0:
            cut_at = idx if cut_at is None else min(cut_at, idx)
    return content if cut_at is None else content[:cut_at]


class MlxLmLLMClientWrapper:
    def __init__(self, model=None, **kwargs):
        _ = kwargs
        resolved_model = str(model or "").strip()
        if not resolved_model:
            raise RuntimeError(HYDRA_LLM_SETUP_ERROR)

        self.host = "mlx-lm://local"
        self.model = resolved_model
        self.provider = HYDRA_LLM_PROVIDER_MLX_LM
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.7"))

        self._llm_calls = 0
        self._llm_elapsed_sec = 0.0
        self._llm_prompt_elapsed_sec = 0.0
        self._llm_completion_elapsed_sec = 0.0
        self._llm_prompt_tokens = 0
        self._llm_completion_tokens = 0
        self._llm_total_tokens = 0
        self._llm_model_last = resolved_model
        self._llm_speed_basis = "local_generate"

    async def aclose(self):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    def get_perf_stats(self, *, reset: bool = False) -> Dict[str, Any]:
        out = _build_llm_perf_stats(
            model=str(self._llm_model_last or self.model or "MLX LM"),
            elapsed=self._llm_elapsed_sec,
            prompt_elapsed=getattr(self, "_llm_prompt_elapsed_sec", 0.0),
            completion_elapsed=getattr(self, "_llm_completion_elapsed_sec", 0.0),
            prompt_tokens=self._llm_prompt_tokens,
            completion_tokens=self._llm_completion_tokens,
            total_tokens=self._llm_total_tokens,
            calls=self._llm_calls,
            speed_basis=getattr(self, "_llm_speed_basis", "local_generate"),
        )
        if reset:
            self._llm_calls = 0
            self._llm_elapsed_sec = 0.0
            self._llm_prompt_elapsed_sec = 0.0
            self._llm_completion_elapsed_sec = 0.0
            self._llm_prompt_tokens = 0
            self._llm_completion_tokens = 0
            self._llm_total_tokens = 0
        return out

    def _format_messages_without_template(self, messages: List[Dict[str, Any]]) -> str:
        parts: List[str] = []
        for item in (messages if isinstance(messages, list) else []):
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "user").strip().lower()
            content = _coerce_content_to_text(item.get("content")).strip()
            if not content:
                continue
            if role == "system":
                parts.append(f"System: {content}")
            elif role == "assistant":
                parts.append(f"Assistant: {content}")
            else:
                parts.append(f"User: {content}")
        parts.append("Assistant:")
        return "\n\n".join(parts).strip()

    def _chat_template_prompt(self, tokenizer: Any, messages: List[Dict[str, Any]]) -> str:
        local_messages = _mlx_lm_disable_thinking_messages(messages)
        template = getattr(tokenizer, "apply_chat_template", None)
        if callable(template):
            last_exc: Optional[Exception] = None
            thinking_variants = _local_no_thinking_template_variants(_mlx_lm_disable_thinking_enabled())
            for extra in thinking_variants:
                try:
                    prompt = template(local_messages, tokenize=False, add_generation_prompt=True, **extra)
                    return _coerce_content_to_text(prompt)
                except TypeError as exc:
                    last_exc = exc
                    continue
                except Exception as exc:
                    last_exc = exc
                    break
            if last_exc is not None:
                logger.debug("MLX LM chat template failed; using plain prompt: %s", last_exc)
        return self._format_messages_without_template(local_messages)

    def _estimate_tokens(self, tokenizer: Any, text: str) -> int:
        try:
            encoded = tokenizer.encode(text)
            return int(len(encoded))
        except Exception:
            return 0

    def _build_generation_kwargs(self, bundle: Dict[str, Any], kwargs: Dict[str, Any]) -> Dict[str, Any]:
        generation_kwargs: Dict[str, Any] = {}
        max_tokens = kwargs.pop("max_tokens", self.max_tokens)
        try:
            generation_kwargs["max_tokens"] = max(1, int(max_tokens))
        except Exception:
            generation_kwargs["max_tokens"] = self.max_tokens

        temperature = kwargs.pop("temperature", self.temperature)
        try:
            temp_value = max(0.0, float(temperature))
        except Exception:
            temp_value = self.temperature
        try:
            top_p = float(kwargs.pop("top_p", 1.0))
        except Exception:
            top_p = 1.0
        try:
            min_p = float(kwargs.pop("min_p", 0.0))
        except Exception:
            min_p = 0.0
        try:
            top_k = int(kwargs.pop("top_k", 0))
        except Exception:
            top_k = 0
        try:
            min_tokens_to_keep = int(kwargs.pop("min_tokens_to_keep", 1))
        except Exception:
            min_tokens_to_keep = 1

        make_sampler = bundle.get("make_sampler")
        if callable(make_sampler):
            generation_kwargs["sampler"] = make_sampler(
                temp=temp_value,
                top_p=top_p,
                min_p=min_p,
                top_k=top_k,
                min_tokens_to_keep=max(1, min_tokens_to_keep),
            )

        make_logits_processors = bundle.get("make_logits_processors")
        if callable(make_logits_processors):
            processors = make_logits_processors(
                repetition_penalty=kwargs.pop("repeat_penalty", kwargs.pop("repetition_penalty", None)),
                presence_penalty=kwargs.pop("presence_penalty", None),
                frequency_penalty=kwargs.pop("frequency_penalty", None),
            )
            if processors:
                generation_kwargs["logits_processors"] = processors

        max_kv_size = kwargs.pop("max_kv_size", _mlx_lm_max_kv_size())
        if max_kv_size is not None:
            try:
                generation_kwargs["max_kv_size"] = max(128, int(max_kv_size))
            except Exception:
                pass
        for key in ("prefill_step_size", "kv_bits", "kv_group_size", "quantized_kv_start"):
            if key in kwargs and kwargs.get(key) is not None:
                generation_kwargs[key] = kwargs.pop(key)
        return generation_kwargs

    def _build_engine_generation_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        generation_kwargs: Dict[str, Any] = {}
        for key in ("top_p", "top_k", "min_p", "min_tokens_to_keep", "seed", "json_schema", "num_draft_tokens"):
            if key in kwargs and kwargs.get(key) is not None:
                generation_kwargs[key] = kwargs.pop(key)
        repetition_penalty = kwargs.pop("repeat_penalty", kwargs.pop("repetition_penalty", None))
        if repetition_penalty is not None:
            generation_kwargs["repetition_penalty"] = repetition_penalty
        repetition_context_size = kwargs.pop("repetition_context_size", None)
        if repetition_context_size is not None:
            generation_kwargs["repetition_context_size"] = repetition_context_size
        return generation_kwargs

    def _chat_sync_mlx_engine(
        self,
        bundle: Dict[str, Any],
        messages: List[Dict[str, Any]],
        *,
        stop: Any = None,
        **kwargs,
    ) -> Dict[str, Any]:
        tokenizer = bundle.get("tokenizer") or bundle.get("processor")
        prompt = self._chat_template_prompt(tokenizer, messages)
        max_tokens = kwargs.pop("max_tokens", self.max_tokens)
        try:
            max_tokens_i = max(1, int(max_tokens))
        except Exception:
            max_tokens_i = self.max_tokens
        temperature = kwargs.pop("temperature", self.temperature)
        try:
            temp_value = max(0.0, float(temperature))
        except Exception:
            temp_value = self.temperature

        output = _mlx_engine_run_generation(
            bundle,
            prompt,
            max_tokens=max_tokens_i,
            temp=temp_value,
            stop=stop,
            extra_kwargs=self._build_engine_generation_kwargs(kwargs),
        )
        content = str(output.get("content") or "").strip()
        prompt_tokens = max(0, int(output.get("prompt_tokens") or 0))
        completion_tokens = max(0, int(output.get("completion_tokens") or 0))
        total_tokens = prompt_tokens + completion_tokens
        generation_elapsed = _perf_nonnegative_float(output.get("elapsed"))
        return {
            "model": self.model,
            "message": {"role": "assistant", "content": content},
            "_usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
            },
            "_timing": {
                "prompt_elapsed": 0.0,
                "completion_elapsed": generation_elapsed,
                "speed_basis": "mlx_engine",
            },
        }

    def _chat_sync(self, messages: List[Dict[str, Any]], *, timeout: Any = None, **kwargs) -> Dict[str, Any]:
        _ = timeout
        stop = kwargs.pop("stop", None)
        seed = kwargs.pop("seed", None)
        if seed is not None and "seed" not in kwargs:
            kwargs["seed"] = seed
        engine_bundle = _load_mlx_engine_bundle(self.model)
        return self._chat_sync_mlx_engine(engine_bundle, messages, stop=stop, **dict(kwargs))

    async def chat(self, messages, **kwargs):
        timeout = kwargs.pop("timeout", None)
        timeout_ms = kwargs.pop("timeout_ms", None)
        if timeout is None and timeout_ms is not None:
            try:
                timeout = float(timeout_ms) / 1000.0
            except Exception:
                timeout = None
        stream = kwargs.pop("stream", False)
        if stream:
            raise RuntimeError("MLX LM provider does not support streaming yet.")
        activity_hint = kwargs.pop("activity", "")
        model = kwargs.pop("model", self.model)
        if model and str(model).strip() != self.model:
            self.model = str(model).strip()
        if "max_tokens" not in kwargs:
            kwargs["max_tokens"] = self.max_tokens
        elif kwargs.get("max_tokens") is None:
            kwargs.pop("max_tokens", None)
        if "temperature" not in kwargs:
            kwargs["temperature"] = self.temperature
        try:
            messages = _sanitize_chat_messages(messages if isinstance(messages, list) else [])
        except Exception:
            messages = messages if isinstance(messages, list) else []

        call_id = _register_active_llm_call(
            host=self.host,
            model=str(self.model or "").strip(),
            stream=False,
            message_count=(len(messages) if isinstance(messages, list) else 0),
            messages=(messages if isinstance(messages, list) else []),
            activity_hint=str(activity_hint or ""),
        )
        call_error: Optional[Exception] = None
        final_model = str(self.model or "").strip()
        started_at = asyncio.get_running_loop().time()
        try:
            _append_llm_debug_event(
                phase="prompt",
                level="info",
                message="Local prompt submitted",
                call_id=call_id,
                provider=HYDRA_LLM_PROVIDER_MLX_LM,
                host=self.host,
                model=str(self.model or "").strip(),
                detail=f"messages={len(messages) if isinstance(messages, list) else 0}",
            )
            result = await _run_mlx_runtime_async(self._chat_sync, messages, timeout=timeout, **kwargs)
            elapsed = max(0.0, float(asyncio.get_running_loop().time() - started_at))
            usage = result.pop("_usage", {}) if isinstance(result, dict) else {}
            timing = result.pop("_timing", {}) if isinstance(result, dict) else {}
            prompt_tokens = int((usage or {}).get("prompt_tokens") or 0)
            completion_tokens = int((usage or {}).get("completion_tokens") or 0)
            total_tokens = int((usage or {}).get("total_tokens") or (prompt_tokens + completion_tokens))
            completion_elapsed = _perf_nonnegative_float((timing or {}).get("completion_elapsed"), elapsed)
            self._llm_calls += 1
            self._llm_elapsed_sec += elapsed
            self._llm_prompt_elapsed_sec += _perf_nonnegative_float((timing or {}).get("prompt_elapsed"))
            self._llm_completion_elapsed_sec += completion_elapsed
            self._llm_prompt_tokens += max(0, prompt_tokens)
            self._llm_completion_tokens += max(0, completion_tokens)
            self._llm_total_tokens += max(0, total_tokens)
            self._llm_model_last = str((result or {}).get("model") or self.model)
            self._llm_speed_basis = str((timing or {}).get("speed_basis") or "local_generate")
            final_model = self._llm_model_last
            _append_llm_debug_result(
                call_id=call_id,
                provider=HYDRA_LLM_PROVIDER_MLX_LM,
                host=self.host,
                model=final_model,
                result=result,
                usage={
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                },
                timing=timing,
                elapsed=elapsed,
            )
            return result
        except Exception as exc:
            call_error = exc
            raise
        finally:
            _finish_active_llm_call(
                call_id,
                error=call_error,
                response_model=final_model,
            )


def _local_vision_messages(image_bytes: bytes, filename: str, prompt: str) -> List[Dict[str, Any]]:
    image_bytes, filename = _local_vision_prepare_image_bytes(image_bytes, filename)
    return [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": str(prompt or "").strip() or "Describe this image."},
                {"type": "image_url", "image_url": {"url": _image_data_url(image_bytes, filename)}},
            ],
        }
    ]


def _local_vision_prepare_image_bytes(image_bytes: bytes, filename: str) -> Tuple[bytes, str]:
    name = str(filename or "image.png").strip() or "image.png"
    lower_name = name.lower()
    if not lower_name.endswith(".gif"):
        return bytes(image_bytes or b""), name
    try:
        from PIL import Image  # type: ignore
    except Exception:
        return bytes(image_bytes or b""), name
    try:
        image = Image.open(io.BytesIO(bytes(image_bytes or b"")))
        image.seek(0)
        frame = image.convert("RGBA")
        background = Image.new("RGBA", frame.size, (255, 255, 255, 255))
        background.alpha_composite(frame)
        out = io.BytesIO()
        background.convert("RGB").save(out, format="PNG")
        stem = Path(name).stem or "image"
        return out.getvalue(), f"{stem}.png"
    except Exception as exc:
        logger.debug("[local-vision] failed to normalize GIF %s to PNG: %s", name, exc)
        return bytes(image_bytes or b""), name


_LLAMA_CPP_VISION_WORKER_ARG = "--tater-llama-cpp-vision-worker"
_LLAMA_CPP_VISION_WORKER_ENV = "TATER_LLAMA_CPP_VISION_WORKER"
_LLAMA_CPP_VISION_WORKER_RESULT_PREFIX = "TATER_LLAMA_CPP_VISION_RESULT:"


def _llama_cpp_vision_worker_enabled() -> bool:
    if str(os.getenv(_LLAMA_CPP_VISION_WORKER_ENV) or "").strip() == "1":
        return False
    return _boolish(os.getenv("TATER_LLAMA_CPP_VISION_SUBPROCESS"), default=True)


def _llama_cpp_vision_unload_matching_cache_enabled() -> bool:
    return _boolish(os.getenv("TATER_LLAMA_CPP_VISION_UNLOAD_MATCHING_CACHE"), default=False)


def _local_vision_serialize_enabled() -> bool:
    return _boolish(os.getenv("TATER_LOCAL_VISION_SERIALIZE"), default=True)


def _local_vision_lock_timeout_seconds() -> float:
    raw = str(os.getenv("TATER_LOCAL_VISION_LOCK_TIMEOUT_SECONDS") or "300").strip()
    try:
        value = float(raw)
    except Exception:
        value = 300.0
    return max(5.0, min(3600.0, value))


def _llama_cpp_vision_worker_retry_count() -> int:
    raw = str(os.getenv("TATER_LLAMA_CPP_VISION_WORKER_RETRIES") or "1").strip()
    try:
        value = int(raw)
    except Exception:
        value = 1
    return max(0, min(3, value))


def _llama_cpp_vision_worker_retry_delay_seconds() -> float:
    raw = str(os.getenv("TATER_LLAMA_CPP_VISION_WORKER_RETRY_DELAY_SECONDS") or "2").strip()
    try:
        value = float(raw)
    except Exception:
        value = 2.0
    return max(0.25, min(30.0, value))


def _llama_cpp_vision_failure_cooldown_seconds() -> float:
    raw = str(os.getenv("TATER_LLAMA_CPP_VISION_FAILURE_COOLDOWN_SECONDS") or "300").strip()
    try:
        value = float(raw)
    except Exception:
        value = 300.0
    return max(0.0, min(3600.0, value))


def _llama_cpp_vision_failure_key(model_token: Any) -> str:
    return str(model_token or "").strip() or "llama-cpp-vision"


def _llama_cpp_vision_projector_native_crash_text(text: Any) -> bool:
    lowered = str(text or "").lower()
    if "ggml_assert(buffer)" not in lowered and "ggml_assert" not in lowered:
        return False
    return any(token in lowered for token in ("libmtmd", "clip_model_loader", "projector:", "mmproj", "clip_init"))


def _llama_cpp_vision_error_summary(text: Any, limit: int = 1400) -> str:
    wanted = (
        "ggml_assert",
        "projector:",
        "clip_ctx:",
        "clip_model_loader",
        "load_hparams:",
        "load_tensors:",
        "cuda",
        "out of memory",
        "failed",
        "error",
    )
    rows: List[str] = []
    seen: set[str] = set()
    for line in str(text or "").splitlines():
        clean = line.strip()
        if not clean:
            continue
        lowered = clean.lower()
        if not any(token in lowered for token in wanted):
            continue
        if clean in seen:
            continue
        seen.add(clean)
        rows.append(clean)
        if len(rows) >= 14:
            break
    summary = "\n".join(rows).strip() or _tail_text(text, limit)
    return _tail_text(summary, limit)


def _llama_cpp_vision_raise_if_failure_cooling(model_token: Any) -> None:
    cooldown = _llama_cpp_vision_failure_cooldown_seconds()
    if cooldown <= 0:
        return
    key = _llama_cpp_vision_failure_key(model_token)
    now = time.time()
    with _LLAMA_CPP_VISION_FAILURE_COOLDOWN_LOCK:
        row = _LLAMA_CPP_VISION_FAILURE_COOLDOWNS.get(key)
        until = float((row or {}).get("until") or 0.0)
        if until <= now:
            _LLAMA_CPP_VISION_FAILURE_COOLDOWNS.pop(key, None)
            return
        remaining = max(1, int(round(until - now)))
        reason = str((row or {}).get("reason") or "previous llama.cpp vision projector crash").strip()
    raise RuntimeError(
        f"llama.cpp vision is paused for {remaining}s after a native projector crash. "
        f"Last failure: {reason}"
    )


def _llama_cpp_vision_record_failure(model_token: Any, exc: BaseException) -> None:
    text = str(exc or "")
    if not _llama_cpp_vision_projector_native_crash_text(text):
        return
    cooldown = _llama_cpp_vision_failure_cooldown_seconds()
    if cooldown <= 0:
        return
    key = _llama_cpp_vision_failure_key(model_token)
    summary = _llama_cpp_vision_error_summary(text, 900)
    until = time.time() + cooldown
    with _LLAMA_CPP_VISION_FAILURE_COOLDOWN_LOCK:
        _LLAMA_CPP_VISION_FAILURE_COOLDOWNS[key] = {"until": until, "reason": summary}


def _llama_cpp_vision_clear_failure(model_token: Any) -> None:
    key = _llama_cpp_vision_failure_key(model_token)
    with _LLAMA_CPP_VISION_FAILURE_COOLDOWN_LOCK:
        _LLAMA_CPP_VISION_FAILURE_COOLDOWNS.pop(key, None)


def _llama_cpp_vision_worker_retryable_error(exc: BaseException) -> bool:
    text = str(exc or "").lower()
    if _llama_cpp_vision_projector_native_crash_text(text):
        return False
    return any(
        token in text
        for token in (
            "exit signal 6",
            "exit signal 11",
            "ggml_assert(buffer)",
            "ggml_assert",
            "cuda error",
            "cuda out of memory",
            "out of memory",
            "failed to create llama_context",
            "could not create a runtime context",
            "failed to allocate",
            "allocation",
            "worker timed out",
        )
    )


def _llama_cpp_vision_worker_batch_retry_values() -> List[int]:
    current = _llama_cpp_n_batch(vision=True)
    values: List[int] = [current]
    for candidate in (128, 64, 32, 16):
        if candidate < current and candidate not in values:
            values.append(candidate)
    return values


def _tail_text(value: Any, limit: int = 5000) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text.strip()
    return text[-limit:].strip()


def _subprocess_returncode_label(code: Any) -> str:
    try:
        value = int(code)
    except Exception:
        return str(code or "unknown")
    if value < 0:
        return f"signal {-value}"
    return str(value)


def _describe_image_with_llama_cpp_direct(
    *,
    model_token: str,
    messages: List[Dict[str, Any]],
    timeout: float,
) -> Dict[str, Any]:
    bundle = _load_llama_cpp_bundle(model_token, vision=True)
    if not bool(bundle.get("supports_vision")):
        detail = str(bundle.get("vision_warning") or "").strip()
        if not detail and not str(bundle.get("mmproj_path") or "").strip():
            detail = "No matching mmproj vision projector was found for this GGUF."
        raise RuntimeError(detail or f"{model_token} is not loaded as a llama.cpp vision model.")
    client = LlamaCppLLMClientWrapper(model=model_token, vision=True)
    return client._chat_sync(messages, timeout=timeout, max_tokens=768, temperature=0.2)


def _describe_image_with_llama_cpp_subprocess(
    *,
    model_token: str,
    image_bytes: bytes,
    filename: str,
    prompt: str,
    timeout: float,
) -> Dict[str, Any]:
    _llama_cpp_vision_raise_if_failure_cooling(model_token)

    if _llama_cpp_vision_unload_matching_cache_enabled():
        try:
            unload_local_llm_models(provider=HYDRA_LLM_PROVIDER_LLAMA_CPP, model=model_token)
        except Exception as exc:
            logger.warning("[llama-cpp-vision] failed unloading matching in-process cache before worker: %s", exc)

    payload = {
        "model": str(model_token or "").strip(),
        "image_b64": base64.b64encode(bytes(image_bytes or b"")).decode("ascii"),
        "filename": str(filename or "image.png"),
        "prompt": str(prompt or ""),
        "timeout": float(timeout or 90.0),
    }
    env = dict(os.environ)
    env[_LLAMA_CPP_VISION_WORKER_ENV] = "1"
    env.setdefault("PYTHONUNBUFFERED", "1")
    command = [sys.executable, os.path.abspath(__file__), _LLAMA_CPP_VISION_WORKER_ARG]
    worker_timeout = max(45.0, float(timeout or 90.0) + 60.0)

    def _run_once(run_env: Dict[str, str]) -> Dict[str, Any]:
        try:
            completed = subprocess.run(
                command,
                input=json.dumps(payload),
                text=True,
                capture_output=True,
                timeout=worker_timeout,
                cwd=os.path.dirname(os.path.abspath(__file__)),
                env=run_env,
            )
        except subprocess.TimeoutExpired as exc:
            combined = "\n".join([_tail_text(exc.stdout), _tail_text(exc.stderr)]).strip()
            detail = f" Logs: {combined}" if combined else ""
            raise RuntimeError(f"llama.cpp vision worker timed out after {int(worker_timeout)} seconds.{detail}") from exc

        combined_output = "\n".join([str(completed.stdout or ""), str(completed.stderr or "")])
        matches = re.findall(rf"{re.escape(_LLAMA_CPP_VISION_WORKER_RESULT_PREFIX)}([A-Za-z0-9+/=]+)", combined_output)
        if not matches:
            detail = _tail_text(combined_output)
            if completed.returncode != 0:
                if _llama_cpp_vision_projector_native_crash_text(detail):
                    summary = _llama_cpp_vision_error_summary(detail)
                    raise RuntimeError(
                        "llama.cpp vision worker crashed while loading the vision projector. "
                        "The selected GGUF/mmproj pair or llama-cpp-python build appears incompatible "
                        "with the current GPU path, or the projector could not allocate GPU buffers. "
                        "Changing n_batch will not fix this projector-load failure. "
                        f"Details: {summary}"
                    )
                raise RuntimeError(
                    "llama.cpp vision worker crashed "
                    f"(exit {_subprocess_returncode_label(completed.returncode)}). Tater stayed online."
                    f"{' Logs: ' + detail if detail else ''}"
                )
            raise RuntimeError(f"llama.cpp vision worker finished without a result.{' Logs: ' + detail if detail else ''}")

        try:
            worker_payload = json.loads(base64.b64decode(matches[-1]).decode("utf-8"))
        except Exception as exc:
            raise RuntimeError("llama.cpp vision worker returned an unreadable result.") from exc
        if completed.returncode != 0 and not bool(worker_payload.get("ok")):
            detail = str(worker_payload.get("error") or "").strip() or _tail_text(combined_output)
            raise RuntimeError(
                "llama.cpp vision worker failed "
                f"(exit {_subprocess_returncode_label(completed.returncode)}).{f' {detail}' if detail else ''}"
            )
        if not bool(worker_payload.get("ok")):
            raise RuntimeError(str(worker_payload.get("error") or "llama.cpp vision worker failed."))
        result = worker_payload.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("llama.cpp vision worker returned an invalid result.")
        return result

    retry_count = _llama_cpp_vision_worker_retry_count()
    attempts = max(1, retry_count + 1)
    batch_values = _llama_cpp_vision_worker_batch_retry_values()
    last_exc: Optional[RuntimeError] = None
    total_profiles = max(1, len(batch_values) * attempts)
    profile_index = 0
    try:
        for batch_value in batch_values:
            run_env = dict(env)
            run_env["TATER_LLAMA_CPP_VISION_N_BATCH"] = str(batch_value)
            for attempt in range(attempts):
                profile_index += 1
                try:
                    result = _run_once(run_env)
                    _llama_cpp_vision_clear_failure(model_token)
                    return result
                except RuntimeError as exc:
                    last_exc = exc
                    if not _llama_cpp_vision_worker_retryable_error(exc):
                        raise
                    if profile_index >= total_profiles:
                        raise
                    delay = _llama_cpp_vision_worker_retry_delay_seconds() * float(attempt + 1)
                    logger.warning(
                        "[llama-cpp-vision] worker failed with n_batch=%s; retrying in %.1fs (%s/%s): %s",
                        batch_value,
                        delay,
                        profile_index + 1,
                        total_profiles,
                        _tail_text(exc, 700),
                    )
                    time.sleep(delay)
        raise last_exc or RuntimeError("llama.cpp vision worker failed.")
    except RuntimeError as exc:
        _llama_cpp_vision_record_failure(model_token, exc)
        raise


def _mlx_vlm_result_text(output: Any) -> str:
    for attr in ("text", "content", "response"):
        value = getattr(output, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    text = _coerce_content_to_text(output).strip()
    if text:
        return text
    if isinstance(output, (list, tuple)):
        for item in output:
            text = _mlx_vlm_result_text(item)
            if text:
                return text
    return str(output or "").strip()


def _mlx_engine_vision_prompt(bundle: Dict[str, Any], prompt_text: str, image_b64: str) -> str:
    conversation = [
        {
            "role": "user",
            "content": [
                {"type": "image", "base64": image_b64},
                {"type": "text", "text": prompt_text},
            ],
        }
    ]
    targets = [bundle.get("processor"), bundle.get("tokenizer")]
    seen: set[int] = set()
    last_exc: Optional[Exception] = None
    for target in targets:
        if target is None:
            continue
        ident = id(target)
        if ident in seen:
            continue
        seen.add(ident)
        template = getattr(target, "apply_chat_template", None)
        if not callable(template):
            continue
        for extra in _local_no_thinking_template_variants(_mlx_lm_disable_thinking_enabled()):
            try:
                prompt = template(conversation, tokenize=False, add_generation_prompt=True, **extra)
                return _coerce_content_to_text(prompt)
            except TypeError as exc:
                last_exc = exc
                continue
            except Exception as exc:
                last_exc = exc
                break
    if last_exc is not None:
        logger.debug("MLX engine vision chat template failed; using plain prompt: %s", last_exc)
    return prompt_text


def _describe_image_with_mlx_engine(
    *,
    model_token: str,
    image_bytes: bytes,
    filename: str,
    prompt: str,
    timeout: float,
) -> Dict[str, Any]:
    _ = timeout
    prepared_bytes, _prepared_name = _local_vision_prepare_image_bytes(image_bytes, filename)
    image_b64 = base64.b64encode(bytes(prepared_bytes or b"")).decode("utf-8")
    if not image_b64:
        raise RuntimeError("No image bytes were provided for MLX vision.")

    bundle = _load_mlx_engine_bundle(model_token)
    _mlx_engine_normalize_bundle_vision_tokens(bundle)
    prompt_text = str(prompt or "").strip() or "Describe this image."
    formatted_prompt = _mlx_engine_vision_prompt(bundle, prompt_text, image_b64)
    output = _mlx_engine_run_generation(
        bundle,
        formatted_prompt,
        max_tokens=768,
        temp=0.2,
        images_b64=[image_b64],
        max_image_size=_mlx_engine_max_image_size(),
    )
    content = str(output.get("content") or "").strip()
    return {
        "model": model_token,
        "message": {"role": "assistant", "content": content},
    }


def _describe_image_with_mlx_vlm(
    *,
    model_token: str,
    image_bytes: bytes,
    filename: str,
    prompt: str,
    timeout: float,
) -> Dict[str, Any]:
    _ = timeout
    prepared_bytes, prepared_name = _local_vision_prepare_image_bytes(image_bytes, filename)
    suffix = Path(str(prepared_name or "image.png")).suffix.lower()
    if suffix not in {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".gif"}:
        suffix = ".png"
    temp_path = ""
    try:
        with tempfile.NamedTemporaryFile(prefix="tater-mlx-vlm-", suffix=suffix, delete=False) as handle:
            handle.write(bytes(prepared_bytes or b""))
            temp_path = str(handle.name)

        bundle = _load_mlx_vlm_bundle(model_token)
        model_obj = bundle["model"]
        processor = bundle["processor"]
        config = bundle.get("config") or getattr(model_obj, "config", None)
        apply_chat_template = bundle.get("apply_chat_template")
        generate = bundle.get("generate")
        generation_lock = bundle["lock"]
        if not callable(apply_chat_template) or not callable(generate):
            raise RuntimeError("MLX vision runtime is missing generate/template helpers.")

        prompt_text = str(prompt or "").strip() or "Describe this image."
        formatted_prompt = ""
        template_variants = [
            _merge_template_kwargs(thinking, image)
            for thinking in _local_no_thinking_template_variants(_mlx_lm_disable_thinking_enabled())
            for image in ({"num_images": 1}, {})
        ]
        seen_variants: set[Tuple[Tuple[str, str], ...]] = set()
        last_template_exc: Optional[Exception] = None
        for extra in template_variants:
            signature = tuple(sorted((str(key), repr(value)) for key, value in extra.items()))
            if signature in seen_variants:
                continue
            seen_variants.add(signature)
            try:
                formatted_prompt = _coerce_content_to_text(apply_chat_template(processor, config, prompt_text, **extra))
                break
            except TypeError as exc:
                last_template_exc = exc
                continue
            except Exception as exc:
                last_template_exc = exc
                break
        if not formatted_prompt:
            if last_template_exc is not None:
                logger.debug("MLX VLM image chat template failed; using plain prompt: %s", last_template_exc)
            formatted_prompt = prompt_text

        generation_kwargs: Dict[str, Any] = {
            "max_tokens": 768,
            "temperature": 0.2,
            "verbose": False,
        }
        with generation_lock:
            output = _mlx_vlm_generate_with_fallback(
                generate,
                model_obj,
                processor,
                formatted_prompt,
                [temp_path],
                generation_kwargs,
                allow_no_images=False,
            )

        content = _strip_local_thinking_blocks(_mlx_vlm_result_text(output)).strip()
        return {
            "model": model_token,
            "message": {"role": "assistant", "content": content},
        }
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except Exception:
                pass


def describe_image_with_local_llm(
    *,
    provider: str,
    model: str,
    image_bytes: bytes,
    filename: str = "image.png",
    prompt: str = "",
    timeout: float = 90.0,
) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model or "").strip()
    if not _is_local_hydra_llm_provider(provider_token) or not model_token:
        raise RuntimeError("Choose a local vision provider and model.")

    messages = _local_vision_messages(image_bytes, filename, prompt)
    call_id = register_active_vision_call(
        api_base={
            HYDRA_LLM_PROVIDER_HF_TRANSFORMERS: "hf://transformers",
            HYDRA_LLM_PROVIDER_LLAMA_CPP: "llama-cpp://local",
            HYDRA_LLM_PROVIDER_MLX_LM: "mlx-vlm://local",
        }.get(provider_token, "local://vision"),
        model=model_token,
        source="local_vision",
    )
    call_error: Optional[Exception] = None
    response_model = model_token
    lock_acquired = False
    try:
        if _local_vision_serialize_enabled():
            lock_timeout = _local_vision_lock_timeout_seconds()
            lock_acquired = _LOCAL_VISION_GENERATION_LOCK.acquire(timeout=lock_timeout)
            if not lock_acquired:
                raise RuntimeError(
                    f"Local vision is busy and did not become available within {int(lock_timeout)} seconds."
                )

        if provider_token == HYDRA_LLM_PROVIDER_HF_TRANSFORMERS:
            bundle = _load_hf_llm_bundle(model_token)
            if not bool(bundle.get("supports_vision")):
                raise RuntimeError(f"{model_token} does not look like a Hugging Face vision-capable model.")
            client = TransformersLLMClientWrapper(model=model_token)
            result = client._chat_sync(messages, timeout=timeout, max_tokens=768, temperature=0.2)
        elif provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP:
            if _llama_cpp_vision_worker_enabled():
                result = _describe_image_with_llama_cpp_subprocess(
                    model_token=model_token,
                    image_bytes=image_bytes,
                    filename=filename,
                    prompt=prompt,
                    timeout=timeout,
                )
            else:
                result = _describe_image_with_llama_cpp_direct(
                    model_token=model_token,
                    messages=messages,
                    timeout=timeout,
                )
        elif provider_token == HYDRA_LLM_PROVIDER_MLX_LM:
            def _describe_mlx_local() -> Dict[str, Any]:
                return _describe_image_with_mlx_engine(
                    model_token=model_token,
                    image_bytes=image_bytes,
                    filename=filename,
                    prompt=prompt,
                    timeout=timeout,
                )

            result = _run_mlx_runtime_sync(_describe_mlx_local)
        else:
            raise RuntimeError("OpenAI-compatible vision should use the API vision path.")

        content = _coerce_content_to_text(((result or {}).get("message") or {}).get("content")).strip()
        response_model = str((result or {}).get("model") or model_token)
        if not content:
            raise RuntimeError("Local vision model returned an empty description.")
        return {
            "ok": True,
            "description": content,
            "model": response_model,
            "provider": provider_token,
            "provider_label": _local_llm_provider_label(provider_token),
        }
    except Exception as exc:
        call_error = exc
        raise
    finally:
        if lock_acquired:
            try:
                _LOCAL_VISION_GENERATION_LOCK.release()
            except RuntimeError:
                pass
        finish_active_vision_call(call_id, error=call_error, response_model=response_model)


class RoundRobinLLMClientWrapper:
    def __init__(self, *, clients: List[Any], pool_key: str = ""):
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
        prompt_elapsed_total = 0.0
        completion_elapsed_total = 0.0
        prompt_tokens_total = 0
        completion_tokens_total = 0
        total_tokens_total = 0
        calls_total = 0
        model_names: List[str] = []
        speed_bases: List[str] = []

        for client in self._clients:
            getter = getattr(client, "get_perf_stats", None)
            if not callable(getter):
                continue
            stats = getter(reset=reset)
            if not isinstance(stats, dict):
                continue
            elapsed_total += max(0.0, float(stats.get("elapsed") or 0.0))
            prompt_elapsed_total += max(0.0, float(stats.get("prompt_elapsed") or 0.0))
            completion_elapsed_total += max(0.0, float(stats.get("completion_elapsed") or 0.0))
            prompt_tokens_total += max(0, int(stats.get("prompt_tokens") or 0))
            completion_tokens_total += max(0, int(stats.get("completion_tokens") or 0))
            total_tokens_total += max(0, int(stats.get("total_tokens") or 0))
            calls_total += max(0, int(stats.get("calls") or 0))
            model_name = str(stats.get("model") or "").strip()
            if model_name and model_name not in model_names:
                model_names.append(model_name)
            speed_basis = str(stats.get("speed_basis") or "").strip()
            if speed_basis and speed_basis not in speed_bases:
                speed_bases.append(speed_basis)

        if total_tokens_total <= 0:
            total_tokens_total = max(0, prompt_tokens_total + completion_tokens_total)

        tps_total = (
            float(total_tokens_total) / float(elapsed_total)
            if elapsed_total > 0.0 and total_tokens_total > 0
            else 0.0
        )
        tps_comp = (
            float(completion_tokens_total) / float(completion_elapsed_total or elapsed_total)
            if (completion_elapsed_total > 0.0 or elapsed_total > 0.0) and completion_tokens_total > 0
            else 0.0
        )
        tps_prompt = (
            float(prompt_tokens_total) / float(prompt_elapsed_total)
            if prompt_elapsed_total > 0.0 and prompt_tokens_total > 0
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
            "prompt_elapsed": round(prompt_elapsed_total, 6),
            "completion_elapsed": round(completion_elapsed_total, 6),
            "prompt_tokens": prompt_tokens_total,
            "completion_tokens": completion_tokens_total,
            "total_tokens": total_tokens_total,
            "tps_total": round(tps_total, 4),
            "tps_prompt": round(tps_prompt, 4),
            "tps_comp": round(tps_comp, 4),
            "calls": calls_total,
            "speed_basis": speed_bases[0] if len(speed_bases) == 1 else ("mixed" if speed_bases else "wall_time"),
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


def _llama_cpp_vision_worker_main() -> int:
    try:
        raw_payload = sys.stdin.read()
        payload = json.loads(raw_payload or "{}")
        model_token = str(payload.get("model") or "").strip()
        image_b64 = str(payload.get("image_b64") or "")
        image_bytes = base64.b64decode(image_b64, validate=True)
        filename = str(payload.get("filename") or "image.png")
        prompt = str(payload.get("prompt") or "")
        timeout = float(payload.get("timeout") or 90.0)
        messages = _local_vision_messages(image_bytes, filename, prompt)
        result = _describe_image_with_llama_cpp_direct(
            model_token=model_token,
            messages=messages,
            timeout=timeout,
        )
        output = {"ok": True, "result": result}
        return_code = 0
    except Exception as exc:
        output = {"ok": False, "error": str(exc) or exc.__class__.__name__}
        return_code = 1
    encoded = base64.b64encode(json.dumps(output, separators=(",", ":")).encode("utf-8")).decode("ascii")
    print(f"{_LLAMA_CPP_VISION_WORKER_RESULT_PREFIX}{encoded}", flush=True)
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    # macOS Metal can abort during llama.cpp/ggml finalizers after the worker has already
    # produced its result. Exit directly so setup users do not get a Python crash dialog.
    os._exit(return_code)
    return return_code


if __name__ == "__main__" and _LLAMA_CPP_VISION_WORKER_ARG in sys.argv[1:]:
    _llama_cpp_vision_worker_main()
    os._exit(1)
