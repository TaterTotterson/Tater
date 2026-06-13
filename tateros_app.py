import asyncio
import base64
import contextlib
import hashlib
import hmac
import importlib
import inspect
import io
import json
import logging
import mimetypes
import os
import queue
import re
import secrets
import shutil
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
import wave
from collections import Counter, deque
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

import dotenv

dotenv.load_dotenv()

from tateros import integration_store as integration_store_module

from fastapi import Cookie, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from redis.exceptions import RedisError

import core_registry as core_registry_module
import people as people_module
import verba_registry as verba_registry_module
import portal_registry as portal_registry_module
from tater_paths import agent_lab_path
from tater_voice import firmware as esphome_firmware_module
from tater_voice import home as esphome_home_module
from tater_voice import nanowakeword_engine as esphome_nanowakeword_module
from tater_voice import openwakeword_engine as esphome_openwakeword_module
from admin_gate import DEFAULT_ADMIN_ONLY_PLUGINS, REDIS_KEY as ADMIN_GATE_KEY, get_admin_only_plugins
from hydra import estimate_hydra_chat_context_window, get_active_chat_jobs_snapshot, run_hydra_turn
from hydra import (
    HYDRA_ASTRAEUS_PLAN_REVIEW_ENABLED_KEY,
    HYDRA_AUTO_CONTINUE_INCOMPLETE_FINAL_ENABLED_KEY,
    HYDRA_BEAST_CONFIG_ROLE_IDS,
    HYDRA_BEAST_MODE_ENABLED_KEY,
    HYDRA_LLM_HOST_KEY,
    HYDRA_LLM_MODEL_KEY,
    HYDRA_LLM_PORT_KEY,
    HYDRA_MAX_LEDGER_ITEMS_KEY,
    HYDRA_ROLE_LLM_KEY_PREFIX,
    DEFAULT_ASTRAEUS_PLAN_REVIEW_ENABLED,
    DEFAULT_AUTO_CONTINUE_INCOMPLETE_FINAL_ENABLED,
    DEFAULT_MAX_LEDGER_ITEMS,
)
from emoji_responder import get_emoji_settings as get_core_emoji_settings, save_emoji_settings as save_core_emoji_settings
from notify import notifier_destination_catalog
from helpers import (
    DEFAULT_HF_TRANSFORMERS_ATTN_IMPLEMENTATION,
    DEFAULT_HF_TRANSFORMERS_CONTEXT_TOKENS,
    DEFAULT_HF_TRANSFORMERS_DEVICE,
    DEFAULT_HF_TRANSFORMERS_DEVICE_MAP,
    DEFAULT_HF_TRANSFORMERS_DTYPE,
    DEFAULT_HF_TRANSFORMERS_TRUST_REMOTE_CODE,
    DEFAULT_LLAMA_CPP_FLASH_ATTN,
    DEFAULT_LLAMA_CPP_CONTEXT_TOKENS,
    DEFAULT_LLAMA_CPP_MTP_DRAFT_MODEL,
    DEFAULT_LLAMA_CPP_MTP_DRAFT_TOKENS,
    DEFAULT_LLAMA_CPP_MTP_ENABLED,
    DEFAULT_LLAMA_CPP_N_BATCH,
    DEFAULT_LLAMA_CPP_N_UBATCH,
    DEFAULT_LLAMA_CPP_OFFLOAD_KQV,
    DEFAULT_LLAMA_CPP_VISION_CONTEXT_TOKENS,
    DEFAULT_MLX_LM_LAZY_LOAD,
    DEFAULT_MLX_LM_TRUST_REMOTE_CODE,
    HYDRA_HF_TRANSFORMERS_ATTN_IMPLEMENTATION_KEY,
    HYDRA_HF_TRANSFORMERS_CONTEXT_TOKENS_KEY,
    HYDRA_HF_TRANSFORMERS_DEVICE_KEY,
    HYDRA_HF_TRANSFORMERS_DEVICE_MAP_KEY,
    HYDRA_HF_TRANSFORMERS_DTYPE_KEY,
    HYDRA_HF_TRANSFORMERS_TRUST_REMOTE_CODE_KEY,
    HYDRA_LLM_BASE_SERVERS_KEY,
    HYDRA_LLM_PROVIDER_HF_TRANSFORMERS,
    HYDRA_LLM_PROVIDER_KEY,
    HYDRA_LLM_PROVIDER_LLAMA_CPP,
    HYDRA_LLM_PROVIDER_MLX_LM,
    HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE,
    HYDRA_LLM_PROVIDER_SPUD_LINK,
    HYDRA_LLAMA_CPP_CONTEXT_TOKENS_KEY,
    HYDRA_LLAMA_CPP_FLASH_ATTN_KEY,
    HYDRA_LLAMA_CPP_MTP_DRAFT_MODEL_KEY,
    HYDRA_LLAMA_CPP_MTP_DRAFT_TOKENS_KEY,
    HYDRA_LLAMA_CPP_MTP_ENABLED_KEY,
    HYDRA_LLAMA_CPP_N_BATCH_KEY,
    HYDRA_LLAMA_CPP_N_UBATCH_KEY,
    HYDRA_LLAMA_CPP_OFFLOAD_KQV_KEY,
    HYDRA_LLAMA_CPP_VISION_CONTEXT_TOKENS_KEY,
    HYDRA_MLX_ENGINE_KV_BITS_KEY,
    HYDRA_MLX_ENGINE_KV_GROUP_SIZE_KEY,
    HYDRA_MLX_ENGINE_PREFILL_STEP_SIZE_KEY,
    HYDRA_MLX_ENGINE_QUANTIZED_KV_START_KEY,
    HYDRA_MLX_LM_CONTEXT_TOKENS_KEY,
    HYDRA_MLX_LM_LAZY_LOAD_KEY,
    HYDRA_MLX_LM_TRUST_REMOTE_CODE_KEY,
    HfLlmDownloadCancelled,
    decrypt_current_redis_snapshot,
    download_hf_transformers_llm_model,
    download_llama_cpp_llm_model,
    download_mlx_lm_llm_model,
    encrypt_current_redis_snapshot,
    ensure_redis_encryption_key,
    get_llama_cpp_runtime_diagnostics,
    get_llama_cpp_chat_template_info,
    get_local_llm_chat_template_info,
    get_local_llm_loaded_models_snapshot,
    get_redis_connection_config,
    get_redis_encryption_status,
    get_redis_connection_status,
    get_llm_call_runtime_summary,
    get_llm_debug_runtime_snapshot,
    get_vision_call_runtime_summary,
    get_llm_client_from_env,
    preload_hf_transformers_llm_model,
    preload_llama_cpp_llm_model,
    preload_mlx_lm_llm_model,
    migrate_current_redis_to_internal,
    redis_blob_client as shared_redis_blob_client,
    redis_client as shared_redis_client,
    resolve_hydra_base_servers,
    runtime_object_memory_footprint_bytes,
    runtime_path_size_bytes,
    save_redis_connection_settings,
    set_main_loop,
    shutdown_internal_redis,
    test_redis_connection_settings,
    set_llama_cpp_chat_template_override,
    clear_llama_cpp_chat_template_override,
    set_local_llm_chat_template_override,
    clear_local_llm_chat_template_override,
    unload_local_llm_models,
)
from runtime_executors import configure_runtime_executors, run_dashboard, run_wake, shutdown_runtime_executors
from verba_settings import (
    get_verba_enabled,
    get_verba_settings,
    save_verba_settings as save_verba_settings_values,
    set_verba_enabled as set_verba_enabled_flag,
)
from verba_kernel import normalize_platform
from speech_settings import (
    get_announcement_tts_ui_payload,
    get_speech_settings as get_shared_speech_settings,
    get_speech_ui_payload,
    save_speech_settings as save_shared_speech_settings,
)
from speech_tts import (
    clear_tts_model_caches as clear_announcement_tts_model_caches,
    fetch_openai_compatible_tts_model_options,
    fetch_openai_compatible_tts_voice_options,
    fetch_wyoming_tts_voice_options,
    get_runtime_tts_wav,
    synthesize_preview_wav,
)
from integration_registry import (
    get_integration_catalog,
    get_integration_device_group,
    get_integration_devices,
    get_integration_devices_by_capability,
    run_integration_action as run_registered_integration_action,
    save_integration_settings as save_registered_integration_settings,
)
from integration_runtime import (
    bind_integration_runtime_loop,
    clear_integration_runtime_provider,
    ensure_integration_runtime_started,
    integration_runtime_events,
    integration_runtime_states,
    integration_runtime_status,
    restart_integration_runtime,
    start_integration_runtime,
    stop_integration_runtime,
)
from vision_settings import get_vision_settings as get_shared_vision_settings, save_vision_settings as save_shared_vision_settings
from tateros import core_store as core_store_module
from tateros import verba_store as verba_store_module
from tateros import portal_store as portal_store_module


logger = logging.getLogger("tateros")
HOMEASSISTANT_DEFAULT_BASE_URL = "http://homeassistant.local:8123"
HUE_DEFAULT_BRIDGE_HOST = "http://philips-hue.local"
HUE_DEFAULT_DEVICE_TYPE = "tater_shop#tater"
HUE_DEFAULT_TIMEOUT_SECONDS = 10
ALADDIN_DEFAULT_TIMEOUT_SECONDS = 5
SONOS_DEFAULT_ENABLED = True
SONOS_DEFAULT_DISCOVERY_TIMEOUT_SECONDS = 2
UNIFI_NETWORK_DEFAULT_BASE_URL = "https://10.4.20.1"
UNIFI_PROTECT_DEFAULT_BASE_URL = "https://10.4.20.127"
OPENWAKEWORD_DETECT_LOG_EVERY = 120
OPENWAKEWORD_DETECT_SLOW_LOG_S = 1.0
OPENWAKEWORD_STREAM_QUEUE_MAX = 12
openwakeword_detect_stats_lock = threading.Lock()
openwakeword_detect_stats: Dict[str, Dict[str, Any]] = {}
nanowakeword_detect_stats_lock = threading.Lock()
nanowakeword_detect_stats: Dict[str, Dict[str, Any]] = {}
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)-20s %(message)s",
)

redis_client = shared_redis_client
redis_blob_client = shared_redis_blob_client
HYDRA_LLM_RECOVERY_NOTICE_KEY = "tater:hydra:llm:recovery_notice"
APP_LOG_BUFFER_LIMIT = 5000
app_log_lock = threading.Lock()
app_log_entries: deque[Dict[str, Any]] = deque(maxlen=APP_LOG_BUFFER_LIMIT)
app_log_next_seq = 1


class _TaterAppLogBufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        global app_log_next_seq
        try:
            message = record.getMessage()
            display = self.format(record)
            exc_text = ""
            if record.exc_info:
                formatter = self.formatter or logging.Formatter()
                exc_text = formatter.formatException(record.exc_info)
            entry = {
                "seq": 0,
                "ts": float(getattr(record, "created", time.time()) or time.time()),
                "level": str(record.levelname or "").lower(),
                "levelno": int(record.levelno or 0),
                "logger": str(record.name or "root"),
                "module": str(getattr(record, "module", "") or ""),
                "function": str(getattr(record, "funcName", "") or ""),
                "line": int(getattr(record, "lineno", 0) or 0),
                "message": str(message or ""),
                "display": str(display or message or ""),
                "exception": str(exc_text or ""),
            }
            with app_log_lock:
                entry["seq"] = app_log_next_seq
                app_log_next_seq += 1
                app_log_entries.append(entry)
        except Exception:
            self.handleError(record)


def _install_app_log_buffer_handler() -> None:
    root_logger = logging.getLogger()
    if any(isinstance(handler, _TaterAppLogBufferHandler) for handler in root_logger.handlers):
        return
    handler = _TaterAppLogBufferHandler(level=logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)-20s %(message)s"))
    root_logger.addHandler(handler)


_install_app_log_buffer_handler()


def _integration_module(integration_id: str, *, auto_restore: bool = True):
    return integration_store_module.integration_module(integration_id, auto_restore=auto_restore)


def _integration_module_or_400(integration_id: str, display_name: str | None = None):
    module = _integration_module(integration_id)
    if module is not None:
        return module
    name = str(display_name or integration_id or "integration").strip()
    raise HTTPException(
        status_code=400,
        detail=f"{name} integration is not enabled. Enable it in Integration Manager first.",
    )


CHAT_HISTORY_KEY = "webui:chat_history"
DEFAULT_MAX_STORE = 20
DEFAULT_MAX_DISPLAY = 8
DEFAULT_MAX_LLM = 8
DEFAULT_TATER_AVATAR_PATH = Path(__file__).resolve().parent / "images" / "tater.png"
WEBUI_ATTACH_MAX_MB_EACH = int(os.getenv("WEBUI_ATTACH_MAX_MB_EACH", "0"))
WEBUI_ATTACH_MAX_MB_TOTAL = int(os.getenv("WEBUI_ATTACH_MAX_MB_TOTAL", "0"))
WEBUI_ATTACH_TTL_SECONDS = int(os.getenv("WEBUI_ATTACH_TTL_SECONDS", "0"))
WEBUI_ATTACH_INDEX_MAX = int(os.getenv("WEBUI_ATTACH_INDEX_MAX", "500"))
FILE_BLOB_KEY_PREFIX = "webui:file:"
FILE_INDEX_KEY = "webui:file_index"
LAST_LLM_STATS_KEY = "webui:last_llm_stats"
TATER_API_SETTINGS_KEY = "tater:openai_api:settings"
TATER_API_MODE_DIRECT = "direct"
TATER_API_MODE_HYDRA = "hydra"
TATER_API_MODE_CHOICES = {TATER_API_MODE_DIRECT, TATER_API_MODE_HYDRA}
SPUD_LINK_SETTINGS_KEY = "tater:spudlink:settings:v1"
SPUD_LINK_NODES_KEY = "tater:spudlink:nodes:v1"
SPUD_LINK_MODE_DISABLED = "disabled"
SPUD_LINK_MODE_HUB = "hub"
SPUD_LINK_MODE_SPUDLET = "spudlet"
SPUD_LINK_MODE_LITTLE_SPUD = "little_spud"
SPUD_LINK_MODE_CHOICES = {
    SPUD_LINK_MODE_DISABLED,
    SPUD_LINK_MODE_HUB,
    SPUD_LINK_MODE_SPUDLET,
    SPUD_LINK_MODE_LITTLE_SPUD,
}
SPUD_LINK_TATER_MODE_CHOICES = {
    SPUD_LINK_MODE_DISABLED,
    SPUD_LINK_MODE_HUB,
    SPUD_LINK_MODE_SPUDLET,
}
SPUD_LINK_SERVER_MODE_CHOICES = {
    SPUD_LINK_MODE_HUB,
    SPUD_LINK_MODE_SPUDLET,
}
SPUD_LINK_PAIRING_TTL_SECONDS = 10 * 60
SPUD_LINK_ACTIVE_RUN_TTL_SECONDS = 24 * 60 * 60
WEBUI_POPUP_EFFECT_STYLE_KEY = "tater:webui:popup_effect_style"
DEFAULT_WEBUI_POPUP_EFFECT_STYLE = "flame"
WEBUI_POPUP_EFFECT_STYLE_CHOICES = {"disabled", "flame", "dust", "glitch", "portal", "melt"}
WEBUI_AUTH_PASSWORD_HASH_KEY = "tater:webui_auth:password_hash"
WEBUI_AUTH_SESSIONS_KEY = "tater:webui_auth:sessions"
WEBUI_AUTH_COOKIE_NAME = "tater_webui_session"
WEBUI_AUTH_PASSWORD_MIN_LENGTH = 4
WEBUI_AUTH_PBKDF2_ITERATIONS = 260_000
WEBUI_AUTH_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365 * 5
RUNTIME_CONTEXT_ESTIMATE_TTL_SECONDS = 20
DASHBOARD_BRIEFS_KEY = "tater:dashboard:briefs:v1"
DASHBOARD_SNAPSHOT_KEY = "tater:dashboard:snapshot:v1"
DASHBOARD_SETTINGS_KEY = "tater:dashboard:settings:v1"
DASHBOARD_BRIEF_SCHEMA_VERSION = 10
DASHBOARD_SNAPSHOT_SCHEMA_VERSION = 8
DASHBOARD_SNAPSHOT_STALE_SECONDS = 60 * 5
DASHBOARD_BRIEF_TTL_SECONDS = 60 * 60
DASHBOARD_BRIEF_CHECK_INTERVAL_SECONDS = DASHBOARD_BRIEF_TTL_SECONDS
DASHBOARD_REFRESH_INTERVAL_OPTIONS_SECONDS = (0, 30, 60, 300, 900, 1800, 3600, 7200, 14400)
DASHBOARD_BRIEF_INTERVAL_OPTIONS_SECONDS = (0, 300, 900, 1800, 3600, 7200, 14400, 21600, 43200)
DASHBOARD_BRIEF_STARTUP_DELAY_SECONDS = 20
DASHBOARD_BRIEF_RETRY_SECONDS = 60 * 5
DASHBOARD_AWARENESS_EVENTS_PER_SOURCE = 1000
DASHBOARD_AWARENESS_EVENT_DETAIL_LIMIT = 320
DASHBOARD_AWARENESS_EVENT_HIGHLIGHT_LIMIT = 18
DASHBOARD_PERSONAL_PERSON_KEY = "tater:dashboard:personal_person_id"
DASHBOARD_AWARENESS_CAMERA_SUMMARY_REQUEST = (
    "Summarize what the cameras saw across all awareness event sources in the last 12 hours. "
    "Focus on camera image descriptions and notable visual activity for the homeowner. "
    "Ignore simple sensor-only events unless they help explain the camera activity."
)
runtime_context_estimate_cache: Dict[str, Any] = {"updated_at": 0.0, "payload": {}}
dashboard_brief_refresh_lock = threading.RLock()
dashboard_brief_refresh_state: Dict[str, Any] = {
    "running": False,
    "started_at": 0.0,
    "finished_at": 0.0,
    "last_error": "",
    "last_reason": "",
    "last_ids": [],
}
dashboard_snapshot_refresh_lock = threading.RLock()
dashboard_snapshot_refresh_state: Dict[str, Any] = {
    "running": False,
    "started_at": 0.0,
    "finished_at": 0.0,
    "last_error": "",
    "last_reason": "",
}
dashboard_brief_scheduler_task: Optional[asyncio.Task] = None
speech_model_warmup_lock = threading.RLock()
speech_model_warmup_state: Dict[str, Any] = {
    "running": False,
    "started_ts": 0.0,
    "finished_ts": 0.0,
    "reason": "",
    "items": [],
    "errors": [],
}
hf_llm_warmup_lock = threading.RLock()
hf_llm_warmup_state: Dict[str, Any] = {
    "running": False,
    "started_ts": 0.0,
    "finished_ts": 0.0,
    "reason": "",
    "items": [],
    "errors": [],
    "unload_before": [],
    "unload_result": {},
    "runtime_restart": {},
    "progress": 0.0,
    "active_key": "",
    "cancel_requested": False,
    "cancelled": False,
}
hf_llm_warmup_cancel_keys: set[str] = set()


def _trim(value: Any) -> str:
    return str(value or "").strip()


bootstrap_state: Dict[str, Any] = {
    "restore_enabled": True,
    "restore_in_progress": False,
    "restore_complete": False,
    "restore_error": "",
    "restore_summary": {},
    "autostart_enabled": True,
}
redis_maintenance_lock = threading.RLock()


class SurfaceRuntimeManager:
    """Thread runtime manager for core/portal modules exposing run(stop_event=...)."""

    def __init__(self, *, kind: str, package_name: str, env_name: str, default_subdir: str):
        self.kind = str(kind).strip().lower() or "surface"
        self.package_name = package_name
        self.surface_dir = self._resolve_module_dir(env_name, default_subdir)
        self.lock = threading.RLock()
        self.threads: Dict[str, threading.Thread] = {}
        self.stop_flags: Dict[str, threading.Event] = {}

    def _resolve_module_dir(self, env_name: str, default_subdir: str) -> Path:
        app_root = Path(__file__).resolve().parent
        raw = str(os.getenv(env_name, "") or "").strip()
        if not raw:
            return (app_root / default_subdir).resolve()
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = (app_root / candidate).resolve()
        return candidate.resolve()

    def _ensure_import_context(self) -> None:
        parent = str(self.surface_dir.parent)
        if parent and parent not in sys.path:
            sys.path.insert(0, parent)

        package = sys.modules.get(self.package_name)
        if package is not None and not isinstance(package, ModuleType):
            sys.modules.pop(self.package_name, None)
            package = None

        importlib.invalidate_caches()
        if package is None:
            package = importlib.import_module(self.package_name)

        package_paths = getattr(package, "__path__", None)
        if package_paths is not None:
            expected = str(self.surface_dir)
            normalized = {str(Path(path).resolve()) for path in package_paths}
            if expected not in normalized:
                package_paths.append(expected)

    def _import_module(self, module_key: str, *, reload_module: bool = True):
        key = str(module_key or "").strip()
        if not key:
            raise ImportError(f"Missing {self.kind} module key")

        self._ensure_import_context()
        module_name = f"{self.package_name}.{key}"
        errors: List[str] = []
        last_exc: Optional[Exception] = None

        for _ in range(2):
            try:
                importlib.invalidate_caches()

                existing = sys.modules.get(module_name)
                if isinstance(existing, ModuleType):
                    if reload_module:
                        return importlib.reload(existing)
                    return existing

                if module_name in sys.modules:
                    sys.modules.pop(module_name, None)
                return importlib.import_module(module_name)
            except Exception as exc:  # pragma: no cover - defensive import guard
                last_exc = exc
                errors.append(f"{module_name}: {type(exc).__name__}: {exc}")
                sys.modules.pop(module_name, None)

        raise ImportError("; ".join(errors)) from last_exc

    def is_running(self, module_key: str) -> bool:
        with self.lock:
            thread = self.threads.get(module_key)
            return bool(thread and thread.is_alive())

    def start(self, module_key: str) -> Dict[str, Any]:
        key = str(module_key or "").strip()
        if not key:
            raise ValueError(f"Missing {self.kind} module key")

        with self.lock:
            thread = self.threads.get(key)
            stop_flag = self.stop_flags.get(key)

            if thread and thread.is_alive():
                return {"started": False, "running": True, "reason": "already-running"}

            if not isinstance(stop_flag, threading.Event):
                stop_flag = threading.Event()

        def runner():
            try:
                module = self._import_module(key)
                run_fn = getattr(module, "run", None)
                if callable(run_fn):
                    run_fn(stop_event=stop_flag)
                else:
                    logger.warning("[%s] %s missing run(stop_event=...)", self.kind, key)
            except Exception as exc:
                logger.error("[%s] %s crashed: %s", self.kind, key, exc, exc_info=True)
            finally:
                with self.lock:
                    current = self.threads.get(key)
                    if current is threading.current_thread():
                        self.threads.pop(key, None)
                        self.stop_flags.pop(key, None)

        thread = threading.Thread(
            target=runner,
            daemon=True,
            name=f"{self.kind}-{key}",
        )
        with self.lock:
            self.threads[key] = thread
            self.stop_flags[key] = stop_flag

        logger.info("[%s] starting %s", self.kind, key)
        thread.start()
        logger.info("[%s] started %s", self.kind, key)
        return {"started": True, "running": True, "reason": "started"}

    def stop(self, module_key: str, *, timeout: float = 3.0) -> Dict[str, Any]:
        key = str(module_key or "").strip()
        if not key:
            raise ValueError(f"Missing {self.kind} module key")

        with self.lock:
            thread = self.threads.get(key)
            stop_flag = self.stop_flags.get(key)

        logger.info("[%s] stopping %s", self.kind, key)
        if stop_flag:
            stop_flag.set()

        if thread and thread.is_alive():
            thread.join(timeout=timeout)

        running = bool(thread and thread.is_alive())
        with self.lock:
            if not running:
                self.threads.pop(key, None)
                self.stop_flags.pop(key, None)
                logger.info("[%s] stopped %s", self.kind, key)
            else:
                logger.warning("[%s] stop timed out for %s", self.kind, key)

        return {
            "stopped": not running,
            "running": running,
            "reason": "stop-timeout" if running else "stopped",
        }

    def stop_all(self, *, timeout: float = 8.0) -> None:
        with self.lock:
            keys = list(self.threads.keys())
            stop_flags = {key: self.stop_flags.get(key) for key in keys}

        for key in keys:
            logger.info("[%s] stopping %s", self.kind, key)
            stop_flag = stop_flags.get(key)
            if stop_flag:
                stop_flag.set()

        deadline = time.monotonic() + max(0.1, float(timeout or 0))
        pending = set(keys)
        while pending and time.monotonic() < deadline:
            for key in list(pending):
                with self.lock:
                    thread = self.threads.get(key)
                if not thread or not thread.is_alive():
                    with self.lock:
                        self.threads.pop(key, None)
                        self.stop_flags.pop(key, None)
                    logger.info("[%s] stopped %s", self.kind, key)
                    pending.discard(key)
            if pending:
                time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))

        for key in list(pending):
            with self.lock:
                thread = self.threads.get(key)
            if thread and thread.is_alive():
                logger.warning("[%s] stop timed out for %s", self.kind, key)
            else:
                with self.lock:
                    self.threads.pop(key, None)
                    self.stop_flags.pop(key, None)
                logger.info("[%s] stopped %s", self.kind, key)


core_runtime = SurfaceRuntimeManager(
    kind="core",
    package_name="cores",
    env_name="TATER_CORE_DIR",
    default_subdir="cores",
)
portal_runtime = SurfaceRuntimeManager(
    kind="portal",
    package_name="portals",
    env_name="TATER_PORTAL_DIR",
    default_subdir="portals",
)


def _read_non_negative_int(key: str, default: int) -> int:
    raw = redis_client.get(key)
    try:
        value = int(str(raw).strip()) if raw is not None else int(default)
    except Exception:
        value = int(default)
    return max(0, value)


def _read_positive_int(key: str, default: int) -> int:
    return max(1, _read_non_negative_int(key, default))


def _read_local_llm_context_setting(
    redis_key: str,
    env_keys: Tuple[str, ...],
    default: Optional[int],
    *,
    minimum: int = 256,
    maximum: int = 1_048_576,
) -> str:
    raw = str(redis_client.get(redis_key) or "").strip()
    if not raw:
        for env_key in env_keys:
            raw = str(os.getenv(env_key) or "").strip()
            if raw:
                break
    if not raw:
        return "" if default is None else str(int(default))
    try:
        value = int(raw)
    except Exception:
        return "" if default is None else str(int(default))
    return str(max(int(minimum), min(int(maximum), int(value))))


def _read_bool_setting(redis_key: str, env_keys: Tuple[str, ...], default: bool) -> bool:
    raw = str(redis_client.get(redis_key) or "").strip()
    if not raw:
        for env_key in env_keys:
            raw = str(os.getenv(env_key) or "").strip()
            if raw:
                break
    if not raw:
        return bool(default)
    return _as_bool_flag(raw, default=default)


def _read_text_setting(redis_key: str, env_keys: Tuple[str, ...], default: str = "") -> str:
    raw = str(redis_client.get(redis_key) or "").strip()
    if not raw:
        for env_key in env_keys:
            raw = str(os.getenv(env_key) or "").strip()
            if raw:
                break
    return str(raw or default or "").strip()


def _read_text_choice_setting(
    redis_key: str,
    env_keys: Tuple[str, ...],
    default: str,
    *,
    allowed: Tuple[str, ...],
) -> str:
    raw = str(redis_client.get(redis_key) or "").strip()
    if not raw:
        for env_key in env_keys:
            raw = str(os.getenv(env_key) or "").strip()
            if raw:
                break
    token = str(raw or default or "").strip().lower()
    aliases = {
        "fp16": "float16",
        "half": "float16",
        "bf16": "bfloat16",
        "fp32": "float32",
        "none": "disabled",
        "off": "disabled",
        "false": "disabled",
        "0": "disabled",
        "default": "default",
        "auto": "auto",
    }
    token = aliases.get(token, token)
    allowed_set = {str(item).strip().lower() for item in allowed}
    default_token = str(default or "").strip().lower()
    return token if token in allowed_set else default_token


def _read_optional_int_setting(
    redis_key: str,
    env_keys: Tuple[str, ...],
    *,
    minimum: int = 0,
    maximum: int = 1_048_576,
    allow_zero: bool = False,
) -> str:
    raw = str(redis_client.get(redis_key) or "").strip()
    if not raw:
        for env_key in env_keys:
            raw = str(os.getenv(env_key) or "").strip()
            if raw:
                break
    if not raw:
        return ""
    try:
        value = int(float(raw))
    except Exception:
        return ""
    if value < 0 or (value == 0 and not allow_zero):
        return ""
    return str(max(int(minimum), min(int(maximum), int(value))))


def _read_bounded_int_setting(
    redis_key: str,
    env_keys: Tuple[str, ...],
    default: int,
    *,
    minimum: int = 1,
    maximum: int = 16,
) -> str:
    raw = str(redis_client.get(redis_key) or "").strip()
    if not raw:
        for env_key in env_keys:
            raw = str(os.getenv(env_key) or "").strip()
            if raw:
                break
    try:
        value = int(float(raw)) if raw else int(default)
    except Exception:
        value = int(default)
    return str(max(int(minimum), min(int(maximum), int(value))))


def _setting_fields(required: Dict[str, Any], current: Dict[str, str]) -> List[Dict[str, Any]]:
    fields: List[Dict[str, Any]] = []
    for setting_key, setting_meta in (required or {}).items():
        if not isinstance(setting_meta, dict):
            continue

        default_value = setting_meta.get("default", "")
        raw_value = current.get(setting_key, default_value)
        fields.append(
            {
                "key": setting_key,
                "label": setting_meta.get("label", setting_key),
                "type": setting_meta.get("type", "text"),
                "description": setting_meta.get("description", ""),
                "options": setting_meta.get("options", []),
                "accept": setting_meta.get("accept", ""),
                "value": raw_value,
                "default": default_value,
            }
        )
    return fields


def _portal_settings_module(portal_key: str) -> Optional[ModuleType]:
    key = str(portal_key or "").strip()
    if not key:
        return None
    try:
        return portal_runtime._import_module(key, reload_module=False)
    except Exception:
        return None


def _portal_setting_fields(portal_key: str, required: Dict[str, Any], current: Dict[str, str]) -> List[Dict[str, Any]]:
    fields = _setting_fields(required, current)
    module = _portal_settings_module(portal_key)
    if module is None:
        return fields
    hook = getattr(module, "webui_settings_fields", None)
    if not callable(hook):
        return fields
    try:
        updated = hook(
            fields=[dict(field) if isinstance(field, dict) else field for field in fields],
            current_settings=dict(current or {}),
            redis_client=redis_client,
            notifier_destination_catalog=notifier_destination_catalog,
        )
    except Exception:
        logger.exception("[portals] settings field hook failed for %s", portal_key)
        return fields
    return updated if isinstance(updated, list) else fields


def _esphome_settings_fields() -> List[Dict[str, Any]]:
    try:
        rows = esphome_home_module.settings_fields()
        return rows if isinstance(rows, list) else []
    except Exception:
        logger.exception("[esphome] failed building ESPHome settings fields")
        return []


def _save_esphome_settings_values(values: Dict[str, Any]) -> Dict[str, Any]:
    try:
        result = esphome_home_module.save_settings_values(values or {})
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Failed to save ESPHome settings.") from exc
    return result if isinstance(result, dict) else {"ok": True}


def _portal_prepare_settings_values(portal_key: str, values: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(values or {})
    module = _portal_settings_module(portal_key)
    if module is None:
        return out
    hook = getattr(module, "webui_prepare_settings_values", None)
    if not callable(hook):
        return out
    try:
        prepared = hook(values=dict(out), redis_client=redis_client)
    except Exception:
        logger.exception("[portals] settings save hook failed for %s", portal_key)
        return out
    return prepared if isinstance(prepared, dict) else out


def _verba_setting_fields(plugin: Any, required: Dict[str, Any], current: Dict[str, str]) -> List[Dict[str, Any]]:
    fields = _setting_fields(required, current)
    hook = getattr(plugin, "webui_settings_fields", None)
    if not callable(hook):
        return fields
    plugin_id = str(getattr(plugin, "name", "") or getattr(plugin, "verba_name", "") or "").strip() or "unknown"
    try:
        updated = hook(
            fields=[dict(field) if isinstance(field, dict) else field for field in fields],
            current_settings=dict(current or {}),
            redis_client=redis_client,
            notifier_destination_catalog=notifier_destination_catalog,
        )
    except Exception:
        logger.exception("[verbas] settings field hook failed for %s", plugin_id)
        return fields
    return updated if isinstance(updated, list) else fields


def _verba_prepare_settings_values(plugin: Any, values: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(values or {})
    hook = getattr(plugin, "webui_prepare_settings_values", None)
    if not callable(hook):
        return out
    plugin_id = str(getattr(plugin, "name", "") or getattr(plugin, "verba_name", "") or "").strip() or "unknown"
    try:
        prepared = hook(values=dict(out), redis_client=redis_client)
    except Exception:
        logger.exception("[verbas] settings save hook failed for %s", plugin_id)
        return out
    return prepared if isinstance(prepared, dict) else out


def _as_bool_flag(value: Any, default: bool = True) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _normalize_popup_effect_style(value: Any, default: str = DEFAULT_WEBUI_POPUP_EFFECT_STYLE) -> str:
    token = str(value or "").strip().lower()
    if token in WEBUI_POPUP_EFFECT_STYLE_CHOICES:
        return token
    fallback = str(default or DEFAULT_WEBUI_POPUP_EFFECT_STYLE).strip().lower()
    return fallback if fallback in WEBUI_POPUP_EFFECT_STYLE_CHOICES else DEFAULT_WEBUI_POPUP_EFFECT_STYLE


def _speech_model_warmup_snapshot() -> Dict[str, Any]:
    with speech_model_warmup_lock:
        return {
            "running": bool(speech_model_warmup_state.get("running")),
            "started_ts": float(speech_model_warmup_state.get("started_ts") or 0.0),
            "finished_ts": float(speech_model_warmup_state.get("finished_ts") or 0.0),
            "reason": str(speech_model_warmup_state.get("reason") or ""),
            "items": list(speech_model_warmup_state.get("items") or []),
            "errors": list(speech_model_warmup_state.get("errors") or []),
        }


def _hf_llm_warmup_snapshot() -> Dict[str, Any]:
    with hf_llm_warmup_lock:
        running = bool(hf_llm_warmup_state.get("running"))
        active_key = str(hf_llm_warmup_state.get("active_key") or "").strip()
        cancel_requested = bool(hf_llm_warmup_state.get("cancel_requested"))
        cancel_keys = set(hf_llm_warmup_cancel_keys)
        items: List[Dict[str, Any]] = []
        for raw_item in list(hf_llm_warmup_state.get("items") or []):
            item = dict(raw_item) if isinstance(raw_item, dict) else {}
            key = str(item.get("key") or _hf_llm_warmup_item_key(item.get("provider", ""), item.get("model", ""))).strip()
            status = str(item.get("status") or "").strip().lower()
            item["key"] = key
            item_cancel_requested = cancel_requested or key in cancel_keys
            item["cancel_requested"] = bool(item_cancel_requested)
            item["active"] = bool(running and key and key == active_key)
            item["cancelable"] = bool(
                running
                and status not in {"loaded", "downloaded", "error", "cancelled", "canceled"}
                and not item_cancel_requested
                and key
            )
            items.append(item)
        if items:
            progress_values = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                try:
                    progress_values.append(float(item.get("progress") or 0.0))
                except Exception:
                    progress_values.append(0.0)
            progress = sum(progress_values) / max(1, len(progress_values))
        else:
            try:
                progress = float(hf_llm_warmup_state.get("progress") or 0.0)
            except Exception:
                progress = 0.0
        return {
            "running": running,
            "started_ts": float(hf_llm_warmup_state.get("started_ts") or 0.0),
            "finished_ts": float(hf_llm_warmup_state.get("finished_ts") or 0.0),
            "reason": str(hf_llm_warmup_state.get("reason") or ""),
            "items": items,
            "errors": list(hf_llm_warmup_state.get("errors") or []),
            "unload_before": list(hf_llm_warmup_state.get("unload_before") or []),
            "unload_result": dict(hf_llm_warmup_state.get("unload_result") or {}),
            "runtime_restart": dict(hf_llm_warmup_state.get("runtime_restart") or {}),
            "progress": max(0.0, min(100.0, progress)),
            "active_key": active_key,
            "cancel_requested": cancel_requested,
            "cancelled": bool(hf_llm_warmup_state.get("cancelled")),
            "load_models": bool(hf_llm_warmup_state.get("load_models", True)),
        }


def _hf_llm_warmup_on_save_enabled() -> bool:
    token = os.getenv("TATER_LOCAL_LLM_WARMUP_ON_SAVE")
    if token is None:
        token = os.getenv("TATER_HF_TRANSFORMERS_WARMUP_ON_SAVE")
    if token is None:
        return False
    token = str(token or "true").strip().lower()
    return token in {"1", "true", "yes", "on", "enabled"}


def _local_llm_warmup_on_startup_enabled() -> bool:
    token = os.getenv("TATER_LOCAL_LLM_WARMUP_ON_STARTUP")
    if token is None:
        return True
    return str(token or "").strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _local_llm_warmup_startup_wait_seconds() -> float:
    raw = str(os.getenv("TATER_LOCAL_LLM_WARMUP_STARTUP_WAIT_SECONDS") or "75").strip()
    try:
        value = float(raw)
    except Exception:
        value = 75.0
    return max(0.0, min(1800.0, value))


def _hf_llm_warmup_item_key(provider: str, model: str) -> str:
    return f"{_normalize_hydra_llm_provider(provider)}:{str(model or '').strip()}"


def _hf_llm_warmup_base_item(provider: str, model: str) -> Dict[str, str]:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model or "").strip()
    return {
        "key": _hf_llm_warmup_item_key(provider_token, model_token),
        "provider": provider_token,
        "provider_label": _hydra_llm_provider_label(provider_token),
        "model": model_token,
    }


def _hf_llm_warmup_target(value: Any) -> Dict[str, str]:
    if isinstance(value, dict):
        provider = _normalize_hydra_llm_provider(value.get("provider"))
        model = str(value.get("model") or "").strip()
    else:
        provider = HYDRA_LLM_PROVIDER_HF_TRANSFORMERS
        model = str(value or "").strip()
    if not model or not _is_local_hydra_llm_provider(provider):
        return {}
    return _hf_llm_warmup_base_item(provider, model)


def _dedupe_hf_llm_warmup_targets(values: List[Any]) -> List[Dict[str, str]]:
    targets: List[Dict[str, str]] = []
    seen: set[str] = set()
    for value in values or []:
        target = _hf_llm_warmup_target(value)
        if not target:
            continue
        key = str(target.get("key") or _hf_llm_warmup_item_key(target.get("provider", ""), target.get("model", ""))).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        targets.append(target)
    return targets


def _hf_llm_warmup_models(base_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    models: List[Dict[str, str]] = []
    for row in base_rows or []:
        if not isinstance(row, dict):
            continue
        provider = _normalize_hydra_llm_provider(row.get("provider"))
        if not _is_local_hydra_llm_provider(provider):
            continue
        model = str(row.get("model") or "").strip()
        if not model:
            continue
        models.append(_hf_llm_warmup_base_item(provider, model))
    return _dedupe_hf_llm_warmup_targets(models)


def _sanitize_hydra_base_rows_for_startup(base_rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    rows = [dict(row) for row in (base_rows or []) if isinstance(row, dict)]
    local_rows: List[Dict[str, str]] = []
    for row in rows:
        provider = _normalize_hydra_llm_provider(row.get("provider"))
        model = str(row.get("model") or "").strip()
        if model and _is_local_hydra_llm_provider(provider):
            local_rows.append({"provider": provider, "model": model})
    if not local_rows:
        return rows, {}

    try:
        installed_models = _local_llm_models_payload().get("models", [])
        installed_keys = {
            (_normalize_hydra_llm_provider(item.get("provider")), str(item.get("model") or "").strip())
            for item in installed_models
            if isinstance(item, dict) and str(item.get("model") or "").strip()
        }
    except Exception as exc:
        logger.warning("[local-llm-warmup] startup could not verify installed local models: %s", exc)
        return rows, {}

    missing_rows = [
        row
        for row in local_rows
        if (_normalize_hydra_llm_provider(row.get("provider")), str(row.get("model") or "").strip()) not in installed_keys
    ]
    if not missing_rows:
        return rows, {}

    labels = [
        f"{_hydra_llm_provider_label(row.get('provider'))} {str(row.get('model') or '').strip()}"
        for row in missing_rows
    ]
    preview = "; ".join(labels[:3])
    if len(labels) > 3:
        preview = f"{preview}; +{len(labels) - 3} more"
    notice = (
        "Tater reset the Base model to blank because the selected local model is no longer installed: "
        f"{preview}. Open Settings > Models and choose a downloaded model to load it again."
    )

    try:
        redis_client.delete(HYDRA_LLM_BASE_SERVERS_KEY)
        _set_hydra_legacy_base_keys([])
        redis_client.set(HYDRA_LLM_RECOVERY_NOTICE_KEY, notice)
    except Exception as exc:
        logger.warning("[local-llm-warmup] startup found missing local base model but could not reset settings: %s", exc)

    logger.warning("[local-llm-warmup] startup reset stale local base model selection: %s", preview)
    return [], {
        "reset": True,
        "reason": "missing_local_base_model",
        "missing_models": labels,
        "notice": notice,
    }


def _hf_llm_warmup_update_item(provider: str, model: str, updates: Dict[str, Any]) -> None:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model or "").strip()
    if not model_token:
        return
    target_key = _hf_llm_warmup_item_key(provider_token, model_token)
    with hf_llm_warmup_lock:
        items = list(hf_llm_warmup_state.get("items") or [])
        if not items:
            items = [dict(_hf_llm_warmup_base_item(provider_token, model_token), status="pending", progress=0.0)]
        found = False
        next_items: List[Dict[str, Any]] = []
        for item in items:
            row = dict(item) if isinstance(item, dict) else {}
            row_model = str(row.get("model") or "").strip()
            row_provider = _normalize_hydra_llm_provider(row.get("provider") or provider_token)
            row_key = str(row.get("key") or _hf_llm_warmup_item_key(row_provider, row_model)).strip()
            if row_key == target_key or (row_model == model_token and not row.get("provider")):
                row.update(_hf_llm_warmup_base_item(provider_token, model_token))
                row.update(updates or {})
                found = True
            next_items.append(row)
        if not found:
            next_items.append({**_hf_llm_warmup_base_item(provider_token, model_token), **dict(updates or {})})
        hf_llm_warmup_state["items"] = next_items


def _hf_llm_warmup_item_snapshot(provider: str, model: str) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model or "").strip()
    if not model_token:
        return {}
    target_key = _hf_llm_warmup_item_key(provider_token, model_token)
    with hf_llm_warmup_lock:
        for item in list(hf_llm_warmup_state.get("items") or []):
            if not isinstance(item, dict):
                continue
            row_model = str(item.get("model") or "").strip()
            row_provider = _normalize_hydra_llm_provider(item.get("provider") or provider_token)
            row_key = str(item.get("key") or _hf_llm_warmup_item_key(row_provider, row_model)).strip()
            if row_key == target_key or (row_model == model_token and not item.get("provider")):
                return dict(item)
    return {}


def _hf_llm_warmup_cancel_requested(provider: str, model: str) -> bool:
    key = _hf_llm_warmup_item_key(provider, model)
    with hf_llm_warmup_lock:
        return bool(hf_llm_warmup_state.get("cancel_requested")) or key in hf_llm_warmup_cancel_keys


def _hf_llm_warmup_download_bar_updates(
    provider: str,
    model: str,
    *,
    event_id: str,
    event_name: str,
    description: str,
    unit: str,
    source: str,
    completed: float,
    total: float,
    rate: float,
    eta_seconds: float,
    event_progress: float,
) -> Dict[str, Any]:
    if not event_id or total <= 0:
        return {}

    now = time.time()
    snapshot = _hf_llm_warmup_item_snapshot(provider, model)
    raw_bars = snapshot.get("download_progress_bars") if isinstance(snapshot, dict) else {}
    bars: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw_bars, dict):
        for key, value in raw_bars.items():
            if isinstance(value, dict):
                bars[str(key)] = dict(value)

    unit_token = str(unit or "").strip().lower()
    bars[event_id] = {
        "event": str(event_name or "").strip().lower(),
        "source": str(source or "").strip().lower(),
        "description": str(description or "").strip(),
        "unit": unit_token,
        "completed": max(0.0, float(completed)),
        "total": max(0.0, float(total)),
        "rate": max(0.0, float(rate)),
        "eta_seconds": max(0.0, float(eta_seconds)),
        "progress": max(0.0, min(100.0, float(event_progress))),
        "updated_ts": now,
    }

    byte_units = {"b", "byte", "bytes"}
    byte_bars = [bar for bar in bars.values() if str(bar.get("unit") or "").lower() in byte_units and float(bar.get("total") or 0.0) > 0.0]
    monitor_byte_bars = [bar for bar in byte_bars if str(bar.get("source") or "").lower() == "cache_monitor"]
    if monitor_byte_bars:
        byte_bars = monitor_byte_bars
    if byte_bars:
        bytes_done = sum(min(float(bar.get("completed") or 0.0), float(bar.get("total") or 0.0)) for bar in byte_bars)
        bytes_total = sum(float(bar.get("total") or 0.0) for bar in byte_bars)
        active_byte_bars = [
            bar
            for bar in byte_bars
            if str(bar.get("event") or "") != "close"
            and float(bar.get("completed") or 0.0) < float(bar.get("total") or 0.0)
            and now - float(bar.get("updated_ts") or 0.0) < 15.0
        ]
        speed = sum(float(bar.get("rate") or 0.0) for bar in active_byte_bars)
        if speed <= 0.0:
            speed = max(float(bar.get("rate") or 0.0) for bar in byte_bars)
        progress = (bytes_done / bytes_total * 100.0) if bytes_total > 0.0 else 0.0
        eta = ((bytes_total - bytes_done) / speed) if bytes_total > bytes_done and speed > 0.0 else 0.0
        scaled_progress = 4.0 + (max(0.0, min(100.0, progress)) * 0.72)
        return {
            "download_progress_bars": bars,
            "download_bytes": int(max(0.0, bytes_done)),
            "download_total_bytes": int(max(0.0, bytes_total)),
            "download_speed_bytes_per_sec": int(max(0.0, speed)),
            "download_eta_seconds": max(0.0, eta),
            "download_progress": max(0.0, min(100.0, progress)),
            "current_bytes": int(max(0.0, bytes_done)),
            "current_total_bytes": int(max(0.0, bytes_total)),
            "current_progress": max(0.0, min(100.0, progress)),
            "current_speed_bytes_per_sec": int(max(0.0, speed)),
            "current_eta_seconds": max(0.0, eta),
            "progress": max(4.0, min(78.0, scaled_progress)),
        }

    file_bars = [bar for bar in bars.values() if str(bar.get("unit") or "").lower() not in byte_units and float(bar.get("total") or 0.0) > 0.0]
    if file_bars:
        files_done = sum(min(float(bar.get("completed") or 0.0), float(bar.get("total") or 0.0)) for bar in file_bars)
        files_total = sum(float(bar.get("total") or 0.0) for bar in file_bars)
        file_progress = (files_done / files_total * 100.0) if files_total > 0.0 else 0.0
        file_rate = sum(float(bar.get("rate") or 0.0) for bar in file_bars if str(bar.get("event") or "") != "close")
        file_eta = ((files_total - files_done) / file_rate) if files_total > files_done and file_rate > 0.0 else 0.0
        return {
            "download_progress_bars": bars,
            "files_completed": int(max(0.0, files_done)),
            "files_total": int(max(0.0, files_total)),
            "files_progress": max(0.0, min(100.0, file_progress)),
            "files_rate_per_sec": max(0.0, file_rate),
            "files_eta_seconds": max(0.0, file_eta),
        }

    return {"download_progress_bars": bars}


def _hf_llm_warmup_progress_callback(provider: str, model: str):
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model or "").strip()

    def _callback(event: Dict[str, Any]) -> None:
        if _hf_llm_warmup_cancel_requested(provider_token, model_token):
            raise HfLlmDownloadCancelled("Local model download cancelled.")
        if not isinstance(event, dict):
            return
        stage = str(event.get("stage") or "").strip().lower()
        event_name = str(event.get("event") or "").strip().lower()
        event_id = str(event.get("id") or "").strip()
        desc = str(event.get("description") or "").strip()
        unit = str(event.get("unit") or "").strip()
        source = str(event.get("source") or "").strip()
        try:
            event_progress = float(event.get("progress") or 0.0)
        except Exception:
            event_progress = 0.0
        try:
            completed = float(event.get("completed") or 0.0)
        except Exception:
            completed = 0.0
        try:
            total = float(event.get("total") or 0.0)
        except Exception:
            total = 0.0
        try:
            rate = float(event.get("rate") or 0.0)
        except Exception:
            rate = 0.0
        try:
            eta_seconds = float(event.get("eta_seconds") or 0.0)
        except Exception:
            eta_seconds = 0.0

        updates: Dict[str, Any] = {
            "updated_ts": time.time(),
        }
        if stage == "download":
            base_progress = 4.0
            scaled_progress = base_progress + (max(0.0, min(100.0, event_progress)) * 0.72)
            updates.update(
                {
                    "status": "downloading",
                    "stage": "download",
                    "message": desc or "Downloading model files",
                    "progress": max(4.0, min(78.0, scaled_progress)),
                }
            )
            if total > 0:
                aggregate_updates = _hf_llm_warmup_download_bar_updates(
                    provider_token,
                    model_token,
                    event_id=event_id,
                    event_name=event_name,
                    description=desc,
                    unit=unit,
                    source=source,
                    completed=completed,
                    total=total,
                    rate=rate,
                    eta_seconds=eta_seconds,
                    event_progress=event_progress,
                )
                if aggregate_updates:
                    updates.update(aggregate_updates)
                elif unit.lower() in {"b", "byte", "bytes"}:
                    updates.update(
                        {
                            "current_bytes": int(max(0.0, completed)),
                            "current_total_bytes": int(max(0.0, total)),
                            "current_progress": max(0.0, min(100.0, event_progress)),
                            "current_speed_bytes_per_sec": int(max(0.0, rate)),
                            "current_eta_seconds": max(0.0, eta_seconds),
                        }
                    )
                else:
                    updates.update(
                        {
                            "files_completed": int(max(0.0, completed)),
                            "files_total": int(max(0.0, total)),
                            "files_progress": max(0.0, min(100.0, event_progress)),
                            "files_rate_per_sec": max(0.0, rate),
                            "files_eta_seconds": max(0.0, eta_seconds),
                        }
                    )
        elif stage == "load":
            progress = 82.0 + (max(0.0, min(100.0, event_progress)) * 0.16)
            updates.update(
                {
                    "status": "loading" if str(event.get("event") or "") != "complete" else "loaded",
                    "stage": "load",
                    "message": desc or "Loading model",
                    "progress": max(80.0, min(98.0, progress)),
                }
            )
            if event.get("device"):
                updates["device"] = str(event.get("device") or "")
        else:
            updates.update(
                {
                    "status": "working",
                    "message": desc or "Preparing model",
                    "progress": max(2.0, min(98.0, event_progress)),
                }
            )
        _hf_llm_warmup_update_item(provider_token, model_token, updates)

    return _callback


def _unload_local_llm_warmup_targets(targets: List[Any]) -> Dict[str, Any]:
    clean_targets = _dedupe_hf_llm_warmup_targets(targets or [])
    removed: List[Dict[str, Any]] = []
    errors: List[str] = []
    for target in clean_targets:
        provider = _normalize_hydra_llm_provider(target.get("provider"))
        model = str(target.get("model") or "").strip()
        if not model or not _is_local_hydra_llm_provider(provider):
            continue
        try:
            result = unload_local_llm_models(provider=provider, model=model)
            removed.extend([dict(item) for item in result.get("models", []) if isinstance(item, dict)])
        except Exception as exc:
            message = str(exc) or type(exc).__name__
            errors.append(f"{_hydra_llm_provider_label(provider)} {model}: {message}")
            logger.warning("[local-llm-warmup] unload previous model failed provider=%s model=%s: %s", provider, model, message)
    return {
        "ok": not errors,
        "requested": clean_targets,
        "unloaded_count": len(removed),
        "models": removed,
        "errors": errors,
    }


def _run_hf_llm_warmup(
    models: List[Any],
    *,
    reason: str,
    load_models: bool = True,
    unload_before: Optional[List[Any]] = None,
) -> None:
    items = [dict(target, status="pending", progress=0.0) for target in _dedupe_hf_llm_warmup_targets(models or [])]
    unload_targets = _dedupe_hf_llm_warmup_targets(unload_before or [])
    with hf_llm_warmup_lock:
        cancel_requested = bool(hf_llm_warmup_state.get("cancel_requested"))
        hf_llm_warmup_state.update(
            {
                "running": True,
                "started_ts": time.time(),
                "finished_ts": 0.0,
                "reason": reason,
                "items": list(items),
                "errors": [],
                "unload_before": list(unload_targets),
                "unload_result": {},
                "runtime_restart": {},
                "progress": 0.0,
                "active_key": "",
                "cancel_requested": cancel_requested,
                "cancelled": False,
                "load_models": bool(load_models),
            }
        )

    logger.info(
        "[local-llm-warmup] starting reason=%s load_models=%s models=%s",
        reason,
        bool(load_models),
        [f"{item.get('provider')}:{item.get('model')}" for item in items],
    )
    item_states: List[Dict[str, Any]] = []
    errors: List[str] = []
    if load_models and unload_targets:
        with hf_llm_warmup_lock:
            hf_llm_warmup_state["active_key"] = "__unload_previous__"
            hf_llm_warmup_state["progress"] = 1.0
        unload_result = _unload_local_llm_warmup_targets(unload_targets)
        with hf_llm_warmup_lock:
            hf_llm_warmup_state["active_key"] = ""
            hf_llm_warmup_state["unload_result"] = dict(unload_result)
        if unload_result.get("errors"):
            errors.extend(str(error) for error in unload_result.get("errors") or [] if str(error or "").strip())
        logger.info(
            "[local-llm-warmup] unloaded previous models count=%s errors=%s",
            int(unload_result.get("unloaded_count") or 0),
            len(unload_result.get("errors") or []),
        )
    for item in items:
        row = dict(item)
        provider = _normalize_hydra_llm_provider(row.get("provider"))
        provider_label = _hydra_llm_provider_label(provider)
        model = str(row.get("model") or "").strip()
        key = _hf_llm_warmup_item_key(provider, model)
        row["key"] = key
        if _hf_llm_warmup_cancel_requested(provider, model):
            row["started_ts"] = time.time()
            row["finished_ts"] = time.time()
            row["status"] = "cancelled"
            row["progress"] = 0.0
            row["provider"] = provider
            row["provider_label"] = provider_label
            row["message"] = "Cancelled before download started."
            item_states.append(row)
            with hf_llm_warmup_lock:
                hf_llm_warmup_state["items"] = list(item_states) + [
                    dict(pending, status="pending", progress=0.0) for pending in items[len(item_states) :]
                ]
            continue
        row["started_ts"] = time.time()
        row["status"] = "preparing"
        row["progress"] = 2.0
        with hf_llm_warmup_lock:
            hf_llm_warmup_state["active_key"] = key
            hf_llm_warmup_state["items"] = list(item_states) + [
                row,
                *[dict(pending, status="pending", progress=0.0) for pending in items[len(item_states) + 1 :]],
            ]
        try:
            if _hf_llm_warmup_cancel_requested(provider, model):
                raise HfLlmDownloadCancelled("Local model download cancelled.")
            if load_models:
                if provider == HYDRA_LLM_PROVIDER_LLAMA_CPP:
                    loaded = preload_llama_cpp_llm_model(
                        model,
                        progress_callback=_hf_llm_warmup_progress_callback(provider, model),
                    )
                elif provider == HYDRA_LLM_PROVIDER_MLX_LM:
                    loaded = preload_mlx_lm_llm_model(
                        model,
                        progress_callback=_hf_llm_warmup_progress_callback(provider, model),
                    )
                else:
                    loaded = preload_hf_transformers_llm_model(
                        model,
                        progress_callback=_hf_llm_warmup_progress_callback(provider, model),
                    )
            else:
                if provider == HYDRA_LLM_PROVIDER_LLAMA_CPP:
                    loaded = download_llama_cpp_llm_model(
                        model,
                        progress_callback=_hf_llm_warmup_progress_callback(provider, model),
                    )
                elif provider == HYDRA_LLM_PROVIDER_MLX_LM:
                    loaded = download_mlx_lm_llm_model(
                        model,
                        progress_callback=_hf_llm_warmup_progress_callback(provider, model),
                    )
                else:
                    loaded = download_hf_transformers_llm_model(
                        model,
                        progress_callback=_hf_llm_warmup_progress_callback(provider, model),
                    )
            row.update(_hf_llm_warmup_item_snapshot(provider, model))
            row["status"] = "loaded" if load_models else "downloaded"
            row["provider"] = provider
            row["provider_label"] = provider_label
            row["device"] = str(loaded.get("device") or "")
            row["model_root"] = str(loaded.get("model_root") or "")
            if loaded.get("model_path"):
                row["model_path"] = str(loaded.get("model_path") or "")
            if loaded.get("mmproj_path"):
                row["mmproj_path"] = str(loaded.get("mmproj_path") or "")
                row["mmproj_filename"] = Path(str(loaded.get("mmproj_path") or "")).name
            if "supports_vision" in loaded:
                row["supports_vision"] = bool(loaded.get("supports_vision"))
            provider_warning = str(loaded.get("warning") or "").strip()
            if provider_warning:
                row["warning"] = provider_warning
                row["message"] = provider_warning
            else:
                row["message"] = (
                    f"Loaded on {row['device'] or 'device'}"
                    if load_models
                    else "Downloaded into Tater. Select it from Settings to use it."
                )
            row["progress"] = 100.0
            _record_downloaded_local_llm_model(
                provider=provider,
                model=model,
                model_root=str(row.get("model_root") or ""),
                model_path=str(row.get("model_path") or ""),
                source=str(reason or "local-warmup"),
                supports_vision=bool(row.get("supports_vision")),
                mmproj_path=str(row.get("mmproj_path") or ""),
            )
            logger.info(
                "[local-llm-warmup] %s %s model %s%s",
                "loaded" if load_models else "downloaded",
                provider_label,
                model,
                f" on {row['device'] or 'unknown device'}" if load_models else "",
            )
        except HfLlmDownloadCancelled:
            row.update(_hf_llm_warmup_item_snapshot(provider, model))
            row["status"] = "cancelled"
            row["provider"] = provider
            row["provider_label"] = provider_label
            row["message"] = "Download cancelled."
            row["progress"] = max(0.0, min(100.0, float(row.get("progress") or 0.0)))
            logger.info("[local-llm-warmup] %s model %s cancelled", provider_label, model)
        except Exception as exc:
            row.update(_hf_llm_warmup_item_snapshot(provider, model))
            message = str(exc) or type(exc).__name__
            row["status"] = "error"
            row["provider"] = provider
            row["provider_label"] = provider_label
            row["error"] = message
            row["message"] = message
            errors.append(f"{provider_label} {model}: {message}")
            logger.warning("[local-llm-warmup] %s model %s failed: %s", provider_label, model, message)
        row["finished_ts"] = time.time()
        item_states.append(row)
        with hf_llm_warmup_lock:
            if str(hf_llm_warmup_state.get("active_key") or "") == key:
                hf_llm_warmup_state["active_key"] = ""
            hf_llm_warmup_state["items"] = list(item_states) + [
                dict(pending, status="pending", progress=0.0) for pending in items[len(item_states) :]
            ]
            hf_llm_warmup_state["errors"] = list(errors)

    runtime_restart: Dict[str, Any] = {}
    settings_triggered = str(reason or "").strip().lower().startswith("settings-save")
    loaded_count = sum(1 for item in item_states if str(item.get("status") or "").strip().lower() == "loaded")
    if load_models and settings_triggered and loaded_count > 0:
        with hf_llm_warmup_lock:
            hf_llm_warmup_state["active_key"] = "__runtime_restart__"
            hf_llm_warmup_state["progress"] = 99.0
            hf_llm_warmup_state["runtime_restart"] = {
                "running": True,
                "reason": reason,
                "started_ts": time.time(),
                "active_before": {},
                "stopped": {},
                "resumed": {},
            }
        try:
            runtime_restart = _restart_running_surfaces_for_local_llm_reload(reason=reason)
        except Exception as exc:
            runtime_restart = {
                "reason": reason,
                "started_ts": time.time(),
                "finished_ts": time.time(),
                "error": str(exc) or type(exc).__name__,
                "active_before": {},
                "stopped": {},
                "resumed": {},
            }
            logger.warning("[local-llm-warmup] runtime restart after model load failed: %s", runtime_restart["error"], exc_info=True)
        runtime_restart["running"] = False
        with hf_llm_warmup_lock:
            hf_llm_warmup_state["runtime_restart"] = dict(runtime_restart)
            if str(hf_llm_warmup_state.get("active_key") or "") == "__runtime_restart__":
                hf_llm_warmup_state["active_key"] = ""

    with hf_llm_warmup_lock:
        cancelled = any(str(item.get("status") or "").lower() in {"cancelled", "canceled"} for item in item_states)
        hf_llm_warmup_state.update(
            {
                "running": False,
                "finished_ts": time.time(),
                "items": item_states,
                "errors": errors,
                "progress": 100.0 if not errors else _hf_llm_warmup_snapshot().get("progress", 0.0),
                "active_key": "",
                "runtime_restart": dict(runtime_restart),
                "cancelled": cancelled,
                "load_models": bool(load_models),
            }
        )
    logger.info("[local-llm-warmup] finished errors=%s", len(errors))


def _start_hf_llm_warmup(
    models: List[Any],
    *,
    reason: str = "settings-save",
    load_models: bool = True,
    unload_before: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    clean_items = _dedupe_hf_llm_warmup_targets(models or [])
    clean_unload_items = _dedupe_hf_llm_warmup_targets(unload_before or [])
    if not clean_items:
        snapshot = _hf_llm_warmup_snapshot()
        snapshot["started"] = False
        snapshot["already_running"] = False
        return snapshot

    with hf_llm_warmup_lock:
        if bool(hf_llm_warmup_state.get("running")):
            snapshot = _hf_llm_warmup_snapshot()
            snapshot["started"] = False
            snapshot["already_running"] = True
            return snapshot
        hf_llm_warmup_cancel_keys.clear()
        hf_llm_warmup_state.update(
            {
                "running": True,
                "started_ts": time.time(),
                "finished_ts": 0.0,
                "reason": reason,
                "items": [dict(item, status="pending") for item in clean_items],
                "errors": [],
                "unload_before": list(clean_unload_items),
                "unload_result": {},
                "runtime_restart": {},
                "progress": 0.0,
                "active_key": "",
                "cancel_requested": False,
                "cancelled": False,
                "load_models": bool(load_models),
            }
        )

    thread = threading.Thread(
        target=_run_hf_llm_warmup,
        kwargs={
            "models": clean_items,
            "reason": reason,
            "load_models": bool(load_models),
            "unload_before": clean_unload_items,
        },
        daemon=True,
        name="hf-llm-warmup",
    )
    thread.start()
    snapshot = _hf_llm_warmup_snapshot()
    snapshot["started"] = True
    snapshot["already_running"] = False
    return snapshot


def _start_local_llm_warmup_for_startup(*, reason: str) -> Dict[str, Any]:
    if not _local_llm_warmup_on_startup_enabled():
        snapshot = _hf_llm_warmup_snapshot()
        snapshot["started"] = False
        snapshot["skipped"] = True
        snapshot["skip_reason"] = "TATER_LOCAL_LLM_WARMUP_ON_STARTUP=false"
        logger.info("[local-llm-warmup] startup warmup skipped (TATER_LOCAL_LLM_WARMUP_ON_STARTUP=false)")
        return snapshot

    try:
        rows = resolve_hydra_base_servers(redis_conn=redis_client, include_legacy=True)
    except Exception as exc:
        snapshot = _hf_llm_warmup_snapshot()
        snapshot["started"] = False
        snapshot["error"] = str(exc) or type(exc).__name__
        logger.warning("[local-llm-warmup] startup failed to read base models: %s", exc)
        return snapshot

    rows, recovery = _sanitize_hydra_base_rows_for_startup(rows)
    models = _hf_llm_warmup_models(rows)
    if not models:
        snapshot = _hf_llm_warmup_snapshot()
        snapshot["started"] = False
        snapshot["skipped"] = True
        snapshot["skip_reason"] = "no local base models configured"
        if recovery:
            snapshot["recovery"] = recovery
        logger.info("[local-llm-warmup] startup warmup skipped (no local base models configured)")
        return snapshot

    snapshot = _start_hf_llm_warmup(models, reason=reason, load_models=True)
    if recovery:
        snapshot["recovery"] = recovery
    wait_seconds = _local_llm_warmup_startup_wait_seconds()
    if wait_seconds <= 0:
        snapshot["waited_seconds"] = 0.0
        snapshot["startup_wait_timeout"] = False
        return snapshot

    started_wait = time.time()
    deadline = started_wait + wait_seconds
    while time.time() < deadline:
        current = _hf_llm_warmup_snapshot()
        if not bool(current.get("running")):
            snapshot = current
            break
        time.sleep(0.25)
    else:
        snapshot = _hf_llm_warmup_snapshot()

    snapshot["waited_seconds"] = round(max(0.0, time.time() - started_wait), 2)
    snapshot["startup_wait_timeout"] = bool(snapshot.get("running"))
    if snapshot["startup_wait_timeout"]:
        logger.info(
            "[local-llm-warmup] startup wait timed out after %.1fs; continuing boot while warmup finishes",
            wait_seconds,
        )
    else:
        logger.info("[local-llm-warmup] startup warmup ready after %.1fs", snapshot["waited_seconds"])
    return snapshot


def _hf_browser_token() -> Optional[str]:
    for key in ("TATER_HF_MODEL_BROWSER_TOKEN", "TATER_MLX_LM_TOKEN", "TATER_HF_TRANSFORMERS_TOKEN"):
        token = str(os.getenv(key) or "").strip()
        if token:
            return token
    try:
        env = integration_store_module.huggingface_environment(client=redis_client)
    except Exception:
        env = {}
    if isinstance(env, dict):
        for key in (
            "HF_TOKEN",
            "HUGGINGFACE_HUB_TOKEN",
            "HUGGING_FACE_HUB_TOKEN",
            "HUGGINGFACE_TOKEN",
            "HF_HUB_TOKEN",
            "HUGGINGFACE_API_TOKEN",
        ):
            token = str(env.get(key) or "").strip()
            if token:
                return token
    for key in ("HF_TOKEN", "HUGGINGFACE_HUB_TOKEN", "HUGGING_FACE_HUB_TOKEN", "HUGGINGFACE_TOKEN", "HF_HUB_TOKEN"):
        token = str(os.getenv(key) or "").strip()
        if token:
            return token
    return None


def _hf_browser_integration_status() -> Dict[str, Any]:
    env_override = False
    for key in (
        "TATER_HF_MODEL_BROWSER_TOKEN",
        "TATER_MLX_LM_TOKEN",
        "TATER_HF_TRANSFORMERS_TOKEN",
        "HF_TOKEN",
        "HUGGINGFACE_HUB_TOKEN",
        "HUGGING_FACE_HUB_TOKEN",
        "HUGGINGFACE_TOKEN",
        "HF_HUB_TOKEN",
        "HUGGINGFACE_API_TOKEN",
    ):
        if str(os.getenv(key) or "").strip():
            env_override = True
            break
    try:
        installed = bool(integration_store_module.is_integration_installed("huggingface"))
    except Exception:
        installed = False
    try:
        enabled = bool(integration_store_module.get_integration_enabled("huggingface"))
    except Exception:
        enabled = False
    token = _hf_browser_token()
    has_token = bool(str(token or "").strip())
    if not installed:
        message = "Hugging Face integration is not installed yet."
    elif not enabled:
        message = "Hugging Face integration is installed but not enabled."
    elif not has_token:
        message = "Hugging Face integration is enabled, but no access token is saved."
    else:
        message = "Hugging Face integration is ready."
    return {
        "id": "huggingface",
        "installed": installed,
        "enabled": enabled,
        "has_token": has_token,
        "env_token": env_override,
        "ready": bool(installed and enabled and has_token),
        "setup_needed": bool((not installed) or (not enabled) or (not has_token)),
        "message": message,
    }


def _hf_browser_error_detail(exc: Exception, fallback: str) -> Dict[str, Any]:
    raw_message = str(exc).strip() or fallback
    lowered = raw_message.lower()
    auth_or_setup_error = any(
        token in lowered
        for token in (
            "401",
            "403",
            "unauthorized",
            "forbidden",
            "gated",
            "private",
            "token",
            "rate limit",
            "too many requests",
        )
    )
    integration = _hf_browser_integration_status()
    if auth_or_setup_error or integration.get("setup_needed"):
        return {
            "code": "huggingface_setup_required",
            "message": (
                "Hugging Face setup is needed for this request. Open Integrations, install/enable Hugging Face, "
                "and save an access token for gated/private models and higher Hub rate limits."
            ),
            "raw_error": raw_message,
            "integration": integration,
        }
    return {
        "code": "huggingface_browser_error",
        "message": raw_message,
        "integration": integration,
    }


def _hf_browser_cursor_encode(url: str) -> str:
    clean_url = str(url or "").strip()
    if not clean_url:
        return ""
    return base64.urlsafe_b64encode(clean_url.encode("utf-8")).decode("ascii").rstrip("=")


def _hf_browser_cursor_decode(cursor: str) -> str:
    token = str(cursor or "").strip()
    if not token:
        return ""
    try:
        padded = token + ("=" * (-len(token) % 4))
        url = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8", "replace").strip()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid Hugging Face page cursor.") from exc
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "huggingface.co" or parsed.path != "/api/models":
        raise HTTPException(status_code=400, detail="Invalid Hugging Face page cursor.")
    return url


def _hf_browser_next_url_from_link_header(link_header: Any) -> str:
    raw = str(link_header or "").strip()
    if not raw:
        return ""
    for part in raw.split(","):
        section = part.strip()
        if 'rel="next"' not in section and "rel=next" not in section:
            continue
        match = re.search(r"<([^>]+)>", section)
        if match:
            url = match.group(1).strip()
            if url.startswith("/api/models"):
                return f"https://huggingface.co{url}"
            return url
    return ""


def _hf_browser_models_api_url(
    *,
    provider: str,
    search: str,
    sort: str,
    limit: int,
    task: str = "text-generation",
) -> str:
    library = _hf_browser_provider_library(provider)
    app_filter = _hf_browser_provider_app_filter(provider, task)
    pipeline_tag = _hf_browser_provider_pipeline_filter(provider, task)
    params: Dict[str, Any] = {
        "sort": sort,
        "direction": "-1",
        "limit": str(max(1, min(100, int(limit or 24)))),
        "full": "true",
        "cardData": "true",
    }
    if pipeline_tag:
        params["pipeline_tag"] = pipeline_tag
    if library:
        params["library"] = library
    if app_filter:
        params["apps"] = app_filter
    if search:
        params["search"] = search
    return f"https://huggingface.co/api/models?{urlencode(params)}"


def _hf_browser_fetch_models_page(url: str) -> Tuple[List[Any], str]:
    request_url = str(url or "").strip()
    if not request_url:
        raise RuntimeError("Hugging Face models URL is missing.")
    token = _hf_browser_token()
    headers = {
        "Accept": "application/json",
        "User-Agent": "Tater-HuggingFace-Model-Browser",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(request_url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw_body = response.read()
            link_header = response.headers.get("Link", "")
    except urllib.error.HTTPError as exc:
        try:
            body_text = exc.read().decode("utf-8", "replace")
        except Exception:
            body_text = ""
        message = body_text.strip() or str(exc)
        raise RuntimeError(message) from exc
    except Exception as exc:
        raise RuntimeError(str(exc) or "Failed to fetch Hugging Face models.") from exc
    try:
        payload = json.loads(raw_body.decode("utf-8", "replace"))
    except Exception as exc:
        raise RuntimeError("Hugging Face returned invalid model data.") from exc
    rows = payload if isinstance(payload, list) else []
    next_url = _hf_browser_next_url_from_link_header(link_header)
    return rows, next_url


def _hf_hub_call_compat(fn: Callable[..., Any], **kwargs: Any) -> Any:
    call_kwargs = dict(kwargs)
    removed: set[str] = set()
    fallback_order = ("direction", "pipeline_tag", "task", "library", "tags", "filter", "cardData", "full", "fetch_config")
    for _ in range(16):
        try:
            return fn(**call_kwargs)
        except TypeError as exc:
            message = str(exc)
            match = re.search(r"unexpected keyword argument ['\"]([^'\"]+)['\"]", message)
            if match:
                key = str(match.group(1) or "").strip()
                if key in call_kwargs:
                    call_kwargs.pop(key, None)
                    removed.add(key)
                    continue
            fallback_key = next((key for key in fallback_order if key in call_kwargs and key not in removed), "")
            if fallback_key:
                call_kwargs.pop(fallback_key, None)
                removed.add(fallback_key)
                continue
            raise
    return fn(**call_kwargs)


def _hf_browser_object_value(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _hf_browser_datetime(value: Any) -> str:
    if value is None:
        return ""
    try:
        return value.isoformat()
    except Exception:
        return str(value or "").strip()


def _hf_browser_size(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except Exception:
        return 0


def _hf_browser_card_license(card_data: Any) -> str:
    if not card_data:
        return ""
    if isinstance(card_data, dict):
        value = card_data.get("license") or card_data.get("licenses") or ""
    else:
        value = getattr(card_data, "license", "") or getattr(card_data, "licenses", "")
    if isinstance(value, list):
        return ", ".join(str(item or "").strip() for item in value if str(item or "").strip())
    return str(value or "").strip()


def _hf_browser_param_size_label(value: Any) -> str:
    try:
        count = int(float(value))
    except Exception:
        return ""
    if count <= 0:
        return ""
    if count >= 1_000_000_000:
        number = count / 1_000_000_000.0
        suffix = "B"
    elif count >= 1_000_000:
        number = count / 1_000_000.0
        suffix = "M"
    else:
        return ""
    text = f"{number:.1f}".rstrip("0").rstrip(".")
    return f"{text}{suffix}"


def _hf_browser_safetensors_param_count(safetensors: Any) -> int:
    if not safetensors:
        return 0
    total = _hf_browser_object_value(safetensors, "total", 0)
    try:
        parsed_total = int(float(total or 0))
    except Exception:
        parsed_total = 0
    if parsed_total > 0:
        return parsed_total
    parameters = _hf_browser_object_value(safetensors, "parameters", {}) or {}
    if isinstance(parameters, dict):
        total_params = 0
        for value in parameters.values():
            try:
                total_params += int(float(value or 0))
            except Exception:
                continue
        return total_params
    return 0


def _hf_browser_model_size_label(model_id: str, tags: List[str], files: List[str], model: Any, card_data: Any) -> str:
    structured = _hf_browser_param_size_label(
        _hf_browser_safetensors_param_count(_hf_browser_object_value(model, "safetensors", None))
    )
    if structured:
        return structured

    parts = [model_id, *tags[:40]]
    for filename in files[:20]:
        if filename.lower().endswith((".gguf", ".safetensors", "config.json")):
            parts.append(filename)
    if isinstance(card_data, dict):
        for key in ("base_model", "model_name", "model_name_or_path"):
            value = card_data.get(key)
            if isinstance(value, str):
                parts.append(value)
            elif isinstance(value, list):
                parts.extend(str(item or "") for item in value[:5])

    haystack = " ".join(str(part or "") for part in parts if str(part or "").strip())
    mixture = re.search(r"(?<![A-Za-z0-9])(\d+)\s*x\s*(\d+(?:\.\d+)?)\s*([bBmM])(?=$|[^A-Za-z0-9])", haystack)
    if mixture:
        left = mixture.group(1)
        right = mixture.group(2).rstrip("0").rstrip(".")
        return f"{left}x{right}{mixture.group(3).upper()}"

    matches = list(re.finditer(r"(?<![A-Za-z0-9])(\d+(?:\.\d+)?)\s*([bBmM])(?=$|[^A-Za-z0-9])", haystack))
    for match in matches:
        prefix = haystack[max(0, match.start() - 3) : match.start()].lower()
        suffix = haystack[match.end() : match.end() + 4].lower()
        if prefix.endswith(("q", "v")) or suffix.startswith(("it/", "it-", "bit")):
            continue
        number = match.group(1).rstrip("0").rstrip(".")
        return f"{number}{match.group(2).upper()}"
    return ""


def _hf_browser_provider_search(provider: str, query: str, task: str = "text-generation") -> str:
    q = str(query or "").strip()
    lowered = q.lower()
    task_token = _normalize_hf_browser_task(task)
    if task_token == "image-text-to-text" and not any(token in lowered for token in ("vision", "vl", "image")):
        q = f"{q} vision".strip()
    return q


HF_BROWSER_TATER_PICKS: Tuple[Dict[str, Any], ...] = (
    {
        "id": "TaterTotterson/gemma-4-26B-A4B-it-GGUF-Tater-NoThink",
        "provider": HYDRA_LLM_PROVIDER_LLAMA_CPP,
        "author": "TaterTotterson",
        "model_size": "26B",
        "library_name": "llama.cpp",
        "pipeline_tag": "image-text-to-text",
        "license": "apache-2.0",
        "supports_vision": True,
        "tasks": ("text-generation", "image-text-to-text"),
        "tags": (
            "tater",
            "nothink",
            "gguf",
            "llama.cpp",
            "gemma4",
            "vision",
            "UD-Q4_K_M",
        ),
        "tater_pick_label": "Tater Pick",
        "tater_pick_note": "NoThink UD-Q4_K_M with matching mmproj-F16.",
        "preferred_gguf": "gemma-4-26B-A4B-it-UD-Q4_K_M.gguf",
        "preferred_mmproj": "mmproj-F16.gguf",
    },
)


def _hf_browser_tater_pick_owner() -> str:
    return str(os.getenv("TATER_HF_TATER_PICK_OWNER") or "TaterTotterson").strip()


def _hf_browser_tater_pick_tag_tokens() -> Tuple[str, ...]:
    raw = str(os.getenv("TATER_HF_TATER_PICK_TAGS") or "").strip()
    if raw:
        tags = [item.strip().lower() for item in re.split(r"[,;\s]+", raw) if item.strip()]
    else:
        tags = ["tater-pick", "tater-recommended", "tater-nothink"]
    return tuple(dict.fromkeys(tags))


def _hf_browser_card_tags(card_data: Any) -> List[str]:
    if not card_data:
        return []
    if isinstance(card_data, dict):
        raw = card_data.get("tags") or []
    else:
        raw = getattr(card_data, "tags", []) or []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return [str(item or "").strip() for item in raw if str(item or "").strip()]
    return []


def _hf_browser_paths_to_file_rows(paths: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw_path in paths:
        path = str(raw_path or "").strip()
        if not path or path in seen:
            continue
        seen.add(path)
        lower = path.lower()
        rows.append(
            {
                "path": path,
                "size": 0,
                "is_gguf": lower.endswith(".gguf"),
                "is_mmproj": lower.endswith(".gguf") and "mmproj" in lower,
                "is_safetensors": lower.endswith(".safetensors"),
                "is_config": os.path.basename(lower) in {"config.json", "tokenizer.json", "tokenizer.model"},
                "quant": _hf_browser_file_quant(path),
            }
        )
    return rows


def _hf_browser_tater_pick_metadata(
    model_id: Any,
    *,
    tags: Optional[List[str]] = None,
    card_data: Any = None,
    files: Optional[List[str]] = None,
) -> Dict[str, Any]:
    repo = str(model_id or "").strip()
    if not repo:
        return {}

    static_row = _hf_browser_tater_pick_by_id(repo)
    if static_row:
        return static_row

    owner = _hf_browser_tater_pick_owner().lower()
    if not owner or "/" not in repo or repo.split("/", 1)[0].lower() != owner:
        return {}

    tag_set = {
        str(item or "").strip().lower()
        for item in [*(tags or []), *_hf_browser_card_tags(card_data)]
        if str(item or "").strip()
    }
    pick_tags = set(_hf_browser_tater_pick_tag_tokens())
    has_pick_tag = bool(tag_set.intersection(pick_tags))
    has_nothink_marker = any(
        token in tag_set
        for token in (
            "nothink",
            "no-think",
            "no_think",
            "thinking-disabled",
            "disable-thinking",
            "non-thinking",
        )
    )
    if not has_pick_tag:
        return {}

    file_rows = _hf_browser_paths_to_file_rows(files or [])
    preferred_gguf = _hf_browser_preferred_gguf(file_rows) if file_rows else ""
    preferred_mmproj = _hf_browser_preferred_mmproj(file_rows, preferred_gguf) if file_rows else ""
    quant = _hf_browser_file_quant(preferred_gguf)
    note_bits: List[str] = []
    if has_nothink_marker:
        note_bits.append("NoThink")
    if quant:
        note_bits.append(quant)
    if preferred_mmproj:
        note_bits.append("with matching projector")
    note = " ".join(note_bits).strip()
    return {
        "id": repo,
        "tater_pick_label": "Tater Pick",
        "tater_pick_note": note or "Curated Tater model.",
        "preferred_gguf": preferred_gguf,
        "preferred_mmproj": preferred_mmproj,
    }


def _hf_browser_tater_pick_by_id(model_id: Any) -> Dict[str, Any]:
    needle = str(model_id or "").strip().lower()
    if not needle:
        return {}
    for row in HF_BROWSER_TATER_PICKS:
        if str(row.get("id") or "").strip().lower() == needle:
            return dict(row)
    return {}


def _hf_browser_tater_pick_matches_query(row: Dict[str, Any], query: str) -> bool:
    q = str(query or "").strip().lower()
    if not q:
        return True
    haystack = " ".join(
        [
            str(row.get("id") or ""),
            str(row.get("provider") or ""),
            str(row.get("library_name") or ""),
            str(row.get("model_size") or ""),
            str(row.get("tater_pick_note") or ""),
            " ".join(str(item or "") for item in (row.get("tags") or [])),
        ]
    ).lower()
    return all(token in haystack for token in q.split())


def _hf_browser_tater_pick_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(row.get("provider"))
    tags = [str(item or "").strip() for item in (row.get("tags") or []) if str(item or "").strip()]
    return {
        "id": str(row.get("id") or "").strip(),
        "author": str(row.get("author") or "").strip(),
        "model_size": str(row.get("model_size") or "").strip(),
        "downloads": 0,
        "likes": 0,
        "last_modified": "",
        "pipeline_tag": str(row.get("pipeline_tag") or "").strip(),
        "library_name": str(row.get("library_name") or "").strip(),
        "tags": tags[:30],
        "license": str(row.get("license") or "").strip(),
        "private": False,
        "gated": "",
        "compatible": True,
        "supports_vision": bool(row.get("supports_vision")),
        "provider": provider_token,
        "provider_label": _hydra_llm_provider_label(provider_token),
        "tater_pick": True,
        "tater_pick_label": str(row.get("tater_pick_label") or "Tater Pick").strip(),
        "tater_pick_note": str(row.get("tater_pick_note") or "").strip(),
        "preferred_gguf": str(row.get("preferred_gguf") or "").strip(),
        "preferred_mmproj": str(row.get("preferred_mmproj") or "").strip(),
    }


def _hf_browser_provider_for_model(model: Any, fallback: str = HYDRA_LLM_PROVIDER_HF_TRANSFORMERS) -> str:
    for provider_token in (HYDRA_LLM_PROVIDER_LLAMA_CPP, HYDRA_LLM_PROVIDER_MLX_LM, HYDRA_LLM_PROVIDER_HF_TRANSFORMERS):
        try:
            if _hf_browser_provider_matches(model, provider_token):
                return provider_token
        except Exception:
            continue
    return _normalize_hydra_llm_provider(fallback)


def _hf_browser_tater_pick_models_from_hub(*, query: str, task: str, limit: int, provider: str) -> List[Dict[str, Any]]:
    owner = _hf_browser_tater_pick_owner()
    if not owner:
        return []
    try:
        from huggingface_hub import HfApi  # type: ignore
    except Exception:
        return []

    api = HfApi(token=_hf_browser_token())
    try:
        iterator = _hf_hub_call_compat(
            api.list_models,
            author=owner,
            sort="lastModified",
            direction=-1,
            limit=max(50, min(200, int(limit or 24) * 4)),
            full=True,
            cardData=True,
            fetch_config=True,
        )
        raw_models = list(iterator or [])
    except Exception as exc:
        logger.warning("[huggingface-browser] Tater Picks live lookup failed: %s", exc)
        return []

    task_token = _normalize_hf_browser_task(task)
    provider_filter = _normalize_hydra_llm_provider(provider)
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for model in raw_models:
        model_id = str(_hf_browser_object_value(model, "modelId") or _hf_browser_object_value(model, "id") or "").strip()
        if not model_id or model_id.lower() in seen:
            continue
        seen.add(model_id.lower())
        tags = [
            str(item or "").strip()
            for item in (_hf_browser_object_value(model, "tags", []) or [])
            if str(item or "").strip()
        ]
        siblings = _hf_browser_object_value(model, "siblings", []) or []
        files = [
            str(_hf_browser_object_value(item, "rfilename") or _hf_browser_object_value(item, "path") or "").strip()
            for item in siblings
            if str(_hf_browser_object_value(item, "rfilename") or _hf_browser_object_value(item, "path") or "").strip()
        ]
        card_data = _hf_browser_object_value(model, "cardData", None) or _hf_browser_object_value(model, "card_data", None)
        if not _hf_browser_tater_pick_metadata(model_id, tags=tags, card_data=card_data, files=files):
            continue
        provider_token = _hf_browser_provider_for_model(model, fallback=HYDRA_LLM_PROVIDER_LLAMA_CPP)
        if provider_token != provider_filter:
            continue
        summary = _hf_browser_model_summary(model, provider=provider_token)
        if task_token == "image-text-to-text" and not bool(summary.get("supports_vision")):
            continue
        if not _hf_browser_tater_pick_matches_query(summary, query):
            continue
        rows.append(summary)
    return rows[: max(1, min(100, int(limit or 24)))]


def _hf_browser_tater_pick_models(*, query: str, task: str, limit: int, provider: str) -> List[Dict[str, Any]]:
    task_token = _normalize_hf_browser_task(task)
    provider_filter = _normalize_hydra_llm_provider(provider)
    live_rows = _hf_browser_tater_pick_models_from_hub(query=query, task=task_token, limit=limit, provider=provider_filter)
    rows: List[Dict[str, Any]] = list(live_rows)
    seen = {str(row.get("id") or "").strip().lower() for row in rows if str(row.get("id") or "").strip()}
    for row in HF_BROWSER_TATER_PICKS:
        if str(row.get("id") or "").strip().lower() in seen:
            continue
        if _normalize_hydra_llm_provider(row.get("provider")) != provider_filter:
            continue
        tasks = {str(item or "").strip() for item in (row.get("tasks") or []) if str(item or "").strip()}
        if tasks and task_token not in tasks:
            continue
        if not _hf_browser_tater_pick_matches_query(row, query):
            continue
        rows.append(_hf_browser_tater_pick_summary(row))
    return rows[: max(1, min(100, int(limit or 24)))]


def _hf_browser_provider_app_filter(provider: str, task: str = "text-generation") -> str:
    provider_token = _normalize_hydra_llm_provider(provider)
    task_token = _normalize_hf_browser_task(task)
    if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP:
        return "llama.cpp"
    if provider_token == HYDRA_LLM_PROVIDER_MLX_LM:
        if task_token == "image-text-to-text":
            return ""
        return "mlx-lm"
    return ""


def _hf_browser_provider_library(provider: str) -> str:
    provider_token = _normalize_hydra_llm_provider(provider)
    if provider_token in {HYDRA_LLM_PROVIDER_LLAMA_CPP, HYDRA_LLM_PROVIDER_MLX_LM}:
        return ""
    return "transformers"


def _hf_browser_provider_pipeline_filter(provider: str, task: str = "text-generation") -> str:
    provider_token = _normalize_hydra_llm_provider(provider)
    task_token = _normalize_hf_browser_task(task)
    if task_token == "text-generation" and provider_token in {HYDRA_LLM_PROVIDER_LLAMA_CPP, HYDRA_LLM_PROVIDER_MLX_LM}:
        return ""
    return task_token


def _hf_browser_text_generation_tokens() -> Tuple[str, ...]:
    return ("text-generation", "text2text-generation", "conversational", "image-text-to-text")


def _hf_browser_vision_tokens() -> Tuple[str, ...]:
    return ("image-text-to-text", "image-to-text", "visual-question-answering")


def _normalize_hf_browser_task(value: Any) -> str:
    token = str(value or "text-generation").strip().lower().replace("_", "-")
    if token in {"vision", "image", "image-text", "image-text-to-text", "vl", "vllm"}:
        return "image-text-to-text"
    return "text-generation"


def _hf_browser_model_has_any_task(model: Any, tokens: set[str]) -> bool:
    pipeline_tag = str(_hf_browser_object_value(model, "pipeline_tag") or "").strip().lower()
    if pipeline_tag in tokens:
        return True
    tags = [
        str(item or "").strip().lower()
        for item in (_hf_browser_object_value(model, "tags", []) or [])
        if str(item or "").strip()
    ]
    if any(tag in tokens for tag in tags):
        return True
    card_data = _hf_browser_object_value(model, "cardData", None) or _hf_browser_object_value(model, "card_data", None)
    card_pipeline = ""
    card_tags: List[str] = []
    if isinstance(card_data, dict):
        card_pipeline = str(card_data.get("pipeline_tag") or "").strip().lower()
        raw_tags = card_data.get("tags") or []
        if isinstance(raw_tags, list):
            card_tags = [str(item or "").strip().lower() for item in raw_tags if str(item or "").strip()]
    else:
        card_pipeline = str(getattr(card_data, "pipeline_tag", "") or "").strip().lower()
        raw_tags = getattr(card_data, "tags", []) or []
        if isinstance(raw_tags, list):
            card_tags = [str(item or "").strip().lower() for item in raw_tags if str(item or "").strip()]
    return card_pipeline in tokens or any(tag in tokens for tag in card_tags)


def _hf_browser_is_text_generation_model(model: Any) -> bool:
    return _hf_browser_model_has_any_task(model, set(_hf_browser_text_generation_tokens()))


def _hf_browser_is_vision_model(model: Any) -> bool:
    if _hf_browser_model_has_any_task(model, set(_hf_browser_vision_tokens())):
        return True
    model_id = str(_hf_browser_object_value(model, "modelId") or _hf_browser_object_value(model, "id") or "").strip().lower()
    tags = " ".join(
        str(item or "").strip().lower()
        for item in (_hf_browser_object_value(model, "tags", []) or [])
        if str(item or "").strip()
    )
    siblings = _hf_browser_object_value(model, "siblings", []) or []
    files = " ".join(
        str(_hf_browser_object_value(item, "rfilename") or _hf_browser_object_value(item, "path") or "").strip().lower()
        for item in siblings
        if str(_hf_browser_object_value(item, "rfilename") or _hf_browser_object_value(item, "path") or "").strip()
    )
    return any(token in f"{model_id} {tags} {files}" for token in ("vision", "-vl", "_vl", "vl-", "mmproj", "multimodal"))


def _hf_browser_provider_matches(model: Any, provider: str) -> bool:
    provider_token = _normalize_hydra_llm_provider(provider)
    library = str(_hf_browser_object_value(model, "library_name") or "").strip().lower()
    tags = " ".join(
        str(item or "").strip().lower()
        for item in (_hf_browser_object_value(model, "tags", []) or [])
        if str(item or "").strip()
    )
    siblings = _hf_browser_object_value(model, "siblings", []) or []
    files = " ".join(
        str(_hf_browser_object_value(item, "rfilename") or _hf_browser_object_value(item, "path") or "").strip().lower()
        for item in siblings
        if str(_hf_browser_object_value(item, "rfilename") or _hf_browser_object_value(item, "path") or "").strip()
    )
    model_id = str(_hf_browser_object_value(model, "modelId") or _hf_browser_object_value(model, "id") or "").strip().lower()
    provider_haystack = f"{model_id} {library} {tags} {files}"
    if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP:
        return (
            "llama.cpp" in provider_haystack
            or "llama-cpp" in provider_haystack
            or "llamacpp" in provider_haystack
            or library == "gguf"
            or "gguf" in tags
            or ".gguf" in files
            or "gguf" in model_id
        )
    if provider_token == HYDRA_LLM_PROVIDER_MLX_LM:
        return (
            "mlx-lm" in provider_haystack
            or "mlx_lm" in provider_haystack
            or "mlx-vlm" in provider_haystack
            or "mlx_vlm" in provider_haystack
            or library == "mlx"
            or "mlx" in tags
            or "mlx" in files
            or "mlx" in model_id
        )
    return library in {"", "transformers"} and ".gguf" not in files


def _hf_browser_file_quant(path: str) -> str:
    name = os.path.basename(str(path or "")).upper()
    for quant in (
        "UD-Q4_K_M",
        "UD_Q4_K_M",
        "UD-Q4_K_S",
        "UD_Q4_K_S",
        "UD-Q5_K_M",
        "UD_Q5_K_M",
        "Q2_K",
        "Q3_K_M",
        "Q4_K_M",
        "Q4_K_S",
        "Q5_K_M",
        "Q5_K_S",
        "Q6_K",
        "Q8_0",
        "F16",
        "BF16",
    ):
        if quant in name:
            return quant.replace("_Q", "-Q", 1) if quant.startswith("UD_") else quant
    return ""


def _hf_browser_preferred_gguf(files: List[Dict[str, Any]]) -> str:
    ggufs = [row for row in files if bool(row.get("is_gguf")) and "mmproj" not in str(row.get("path") or "").lower()]
    if not ggufs:
        return ""
    preferred = ("UD-Q4_K_M", "Q4_K_M", "UD-Q5_K_M", "Q5_K_M", "UD-Q4_K_S", "Q4_K_S", "Q5_0", "Q4_0", "Q8_0")

    def _score(row: Dict[str, Any]) -> Tuple[int, int, str]:
        path = str(row.get("path") or "")
        upper = os.path.basename(path).upper()
        for index, quant in enumerate(preferred):
            if quant in upper:
                return (index, len(path), path)
        return (999, len(path), path)

    return str(sorted(ggufs, key=_score)[0].get("path") or "")


def _hf_browser_preferred_mmproj(files: List[Dict[str, Any]], preferred_gguf: str = "") -> str:
    mmprojs = [row for row in files if bool(row.get("is_mmproj"))]
    if not mmprojs:
        return ""
    main_quant = _hf_browser_file_quant(preferred_gguf)

    def _score(row: Dict[str, Any]) -> Tuple[int, int, str]:
        path = str(row.get("path") or "")
        quant = _hf_browser_file_quant(path)
        score = 50
        if main_quant and quant == main_quant:
            score = 0
        elif quant in {"F16", "BF16"}:
            score = 5
        elif quant == "Q8_0":
            score = 10
        return (score, len(path), path)

    return str(sorted(mmprojs, key=_score)[0].get("path") or "")


def _hf_browser_model_summary(model: Any, *, provider: str) -> Dict[str, Any]:
    model_id = str(_hf_browser_object_value(model, "modelId") or _hf_browser_object_value(model, "id") or "").strip()
    tags = [
        str(item or "").strip()
        for item in (_hf_browser_object_value(model, "tags", []) or [])
        if str(item or "").strip()
    ]
    siblings = _hf_browser_object_value(model, "siblings", []) or []
    files = [
        str(_hf_browser_object_value(item, "rfilename") or _hf_browser_object_value(item, "path") or "").strip()
        for item in siblings
        if str(_hf_browser_object_value(item, "rfilename") or _hf_browser_object_value(item, "path") or "").strip()
    ]
    provider_token = _normalize_hydra_llm_provider(provider)
    lower_id = model_id.lower()
    lower_tags = " ".join(tags).lower()
    lower_files = " ".join(files).lower()
    lower_library = str(_hf_browser_object_value(model, "library_name") or "").strip().lower()
    provider_haystack = f"{lower_id} {lower_library} {lower_tags} {lower_files}"
    card_data = _hf_browser_object_value(model, "cardData", None) or _hf_browser_object_value(model, "card_data", None)
    tater_pick = _hf_browser_tater_pick_metadata(model_id, tags=tags, card_data=card_data, files=files)
    supports_vision = _hf_browser_is_vision_model(model) or "mmproj" in lower_files
    if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP:
        compatible = (
            "llama.cpp" in provider_haystack
            or "llama-cpp" in provider_haystack
            or "llamacpp" in provider_haystack
            or "gguf" in lower_id
            or "gguf" in lower_tags
            or ".gguf" in lower_files
        )
    elif provider_token == HYDRA_LLM_PROVIDER_MLX_LM:
        compatible = (
            "mlx-lm" in provider_haystack
            or "mlx_lm" in provider_haystack
            or "mlx-vlm" in provider_haystack
            or "mlx_vlm" in provider_haystack
            or "mlx" in provider_haystack
        )
    else:
        compatible = ".gguf" not in lower_files
    model_size_label = _hf_browser_model_size_label(model_id, tags, files, model, card_data)
    summary = {
        "id": model_id,
        "author": str(_hf_browser_object_value(model, "author") or "").strip(),
        "model_size": model_size_label,
        "downloads": _hf_browser_size(_hf_browser_object_value(model, "downloads", 0)),
        "likes": _hf_browser_size(_hf_browser_object_value(model, "likes", 0)),
        "last_modified": _hf_browser_datetime(
            _hf_browser_object_value(model, "lastModified")
            or _hf_browser_object_value(model, "last_modified")
        ),
        "pipeline_tag": str(_hf_browser_object_value(model, "pipeline_tag") or "").strip(),
        "library_name": str(_hf_browser_object_value(model, "library_name") or "").strip(),
        "tags": tags[:30],
        "license": _hf_browser_card_license(card_data),
        "private": bool(_hf_browser_object_value(model, "private", False)),
        "gated": str(_hf_browser_object_value(model, "gated", "") or "").strip(),
        "compatible": compatible,
        "supports_vision": supports_vision,
        "provider": provider_token,
        "provider_label": _hydra_llm_provider_label(provider_token),
    }
    if tater_pick:
        summary.update(
            {
                "tater_pick": True,
                "tater_pick_label": str(tater_pick.get("tater_pick_label") or "Tater Pick").strip(),
                "tater_pick_note": str(tater_pick.get("tater_pick_note") or "").strip(),
                "preferred_gguf": str(tater_pick.get("preferred_gguf") or "").strip(),
                "preferred_mmproj": str(tater_pick.get("preferred_mmproj") or "").strip(),
            }
        )
    return summary


def _hf_browser_file_rows(info: Any, fallback_files: List[str]) -> List[Dict[str, Any]]:
    siblings = _hf_browser_object_value(info, "siblings", []) or []
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in siblings:
        path = str(_hf_browser_object_value(item, "rfilename") or _hf_browser_object_value(item, "path") or "").strip()
        if not path:
            continue
        seen.add(path)
        lower = path.lower()
        size = _hf_browser_size(_hf_browser_object_value(item, "size", 0))
        rows.append(
            {
                "path": path,
                "size": size,
                "is_gguf": lower.endswith(".gguf"),
                "is_mmproj": lower.endswith(".gguf") and "mmproj" in lower,
                "is_safetensors": lower.endswith(".safetensors"),
                "is_config": os.path.basename(lower) in {"config.json", "tokenizer.json", "tokenizer.model"},
                "quant": _hf_browser_file_quant(path),
            }
        )
    for path in fallback_files:
        path = str(path or "").strip()
        if not path or path in seen:
            continue
        lower = path.lower()
        rows.append(
            {
                "path": path,
                "size": 0,
                "is_gguf": lower.endswith(".gguf"),
                "is_mmproj": lower.endswith(".gguf") and "mmproj" in lower,
                "is_safetensors": lower.endswith(".safetensors"),
                "is_config": os.path.basename(lower) in {"config.json", "tokenizer.json", "tokenizer.model"},
                "quant": _hf_browser_file_quant(path),
            }
        )
    return sorted(rows, key=lambda row: (not bool(row.get("is_gguf")), bool(row.get("is_mmproj")), str(row.get("path") or "").lower()))


def _hf_browser_effective_model_id(provider: str, repo_id: str, filename: str = "") -> str:
    provider_token = _normalize_hydra_llm_provider(provider)
    repo = str(repo_id or "").strip()
    file_token = str(filename or "").strip()
    if not repo:
        return ""
    if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP and file_token:
        return f"{repo}::{file_token}"
    return repo


def _local_llm_model_registry_path() -> Path:
    raw = str(os.getenv("TATER_LOCAL_LLM_MODEL_REGISTRY") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return agent_lab_path("models", "llm", "downloaded_models.json")


def _local_llm_model_root(provider: str) -> Path:
    provider_token = _normalize_hydra_llm_provider(provider)
    env_key = {
        HYDRA_LLM_PROVIDER_HF_TRANSFORMERS: "TATER_HF_TRANSFORMERS_MODEL_ROOT",
        HYDRA_LLM_PROVIDER_LLAMA_CPP: "TATER_LLAMA_CPP_MODEL_ROOT",
        HYDRA_LLM_PROVIDER_MLX_LM: "TATER_MLX_LM_MODEL_ROOT",
    }.get(provider_token, "")
    raw = str(os.getenv(env_key) or "").strip() if env_key else ""
    if raw:
        return Path(raw).expanduser().resolve()
    folder = {
        HYDRA_LLM_PROVIDER_HF_TRANSFORMERS: "huggingface",
        HYDRA_LLM_PROVIDER_LLAMA_CPP: "llama-cpp",
        HYDRA_LLM_PROVIDER_MLX_LM: "mlx",
    }.get(provider_token, "huggingface")
    return agent_lab_path("models", "llm", folder)


def _local_llm_context_value(value: Any, *, maximum: int = 1_048_576) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        return 0
    if parsed < 128 or parsed > 10_000_000:
        return 0
    return max(128, min(int(maximum), int(parsed)))


def _local_llm_context_from_json(payload: Any) -> int:
    if not isinstance(payload, dict):
        return 0
    direct_keys = (
        "max_position_embeddings",
        "model_max_length",
        "max_seq_len",
        "max_sequence_length",
        "seq_length",
        "sequence_length",
        "n_positions",
        "n_ctx",
        "context_length",
        "sliding_window",
    )
    candidates: List[int] = []
    for key in direct_keys:
        value = _local_llm_context_value(payload.get(key))
        if value:
            candidates.append(value)

    rope_scaling = payload.get("rope_scaling") if isinstance(payload.get("rope_scaling"), dict) else {}
    original = _local_llm_context_value(
        rope_scaling.get("original_max_position_embeddings")
        or rope_scaling.get("original_max_position")
        or payload.get("original_max_position_embeddings")
    )
    try:
        factor = float(rope_scaling.get("factor") or 0)
    except Exception:
        factor = 0.0
    if original and factor > 1.0:
        candidates.append(_local_llm_context_value(int(original * factor)))

    for nested_key in ("text_config", "llm_config", "language_config", "model_config"):
        nested = _local_llm_context_from_json(payload.get(nested_key))
        if nested:
            candidates.append(nested)
    return max(candidates) if candidates else 0


def _local_llm_context_from_json_files(path: Path) -> int:
    folder = path if path.is_dir() else path.parent
    if not folder.exists():
        return 0
    candidates: List[int] = []
    for filename in ("config.json", "tokenizer_config.json", "generation_config.json", "params.json"):
        candidate = folder / filename
        if not candidate.exists() or not candidate.is_file():
            continue
        try:
            parsed = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        value = _local_llm_context_from_json(parsed)
        if value:
            candidates.append(value)
    return max(candidates) if candidates else 0


def _local_llm_json_files_support_vision(path: Path) -> bool:
    folder = path if path.is_dir() else path.parent
    if not folder.exists():
        return False
    for filename in ("config.json", "preprocessor_config.json", "processor_config.json", "image_processor_config.json"):
        candidate = folder / filename
        if candidate.exists() and candidate.is_file():
            try:
                parsed = json.loads(candidate.read_text(encoding="utf-8"))
            except Exception:
                parsed = {}
            text = json.dumps(parsed).lower() if isinstance(parsed, dict) else ""
            if filename != "config.json" or any(token in text for token in ("vision", "image", "multimodal", "vl")):
                return True
    return False


def _local_llm_mmproj_for_gguf(path: Path) -> Path:
    if not path.exists() or not path.is_file():
        return Path()
    candidates = sorted(path.parent.glob("*mmproj*.gguf"), key=lambda item: (len(item.name), item.name.lower()))
    return candidates[0] if candidates else Path()


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


def _gguf_read_string(handle) -> str:
    length = _gguf_read_u64(handle)
    if length > 1_000_000:
        raise ValueError("gguf string too large")
    return handle.read(int(length)).decode("utf-8", errors="ignore")


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


def _gguf_read_context_value(handle, value_type: int) -> int:
    if value_type in {0, 2, 4, 10}:
        sizes = {0: 1, 2: 2, 4: 4, 10: 8}
        return int.from_bytes(handle.read(sizes[value_type]), "little", signed=False)
    if value_type in {1, 3, 5, 11}:
        sizes = {1: 1, 3: 2, 5: 4, 11: 8}
        return int.from_bytes(handle.read(sizes[value_type]), "little", signed=True)
    if value_type == 8:
        return _local_llm_context_value(_gguf_read_string(handle))
    _gguf_skip_value(handle, value_type)
    return 0


def _local_llm_context_from_gguf(path: Path) -> int:
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".gguf":
        return 0
    try:
        with path.open("rb") as handle:
            if handle.read(4) != b"GGUF":
                return 0
            _version = _gguf_read_u32(handle)
            _tensor_count = _gguf_read_u64(handle)
            metadata_count = min(_gguf_read_u64(handle), 10000)
            for _ in range(int(metadata_count)):
                key = _gguf_read_string(handle)
                value_type = _gguf_read_u32(handle)
                if key.endswith(".context_length") or key in {"context_length", "general.context_length"}:
                    return _local_llm_context_value(_gguf_read_context_value(handle))
                _gguf_skip_value(handle, value_type)
    except Exception:
        return 0
    return 0


def _local_llm_repo_snapshot_path(model_root: str, repo_id: str) -> Path:
    root = Path(str(model_root or "")).expanduser()
    repo = str(repo_id or "").strip()
    if not root.exists() or "/" not in repo:
        return Path()
    repo_dir = root / f"models--{repo.replace('/', '--')}"
    snapshots_dir = repo_dir / "snapshots"
    if not snapshots_dir.exists():
        return Path()
    snapshots = [path for path in snapshots_dir.iterdir() if path.is_dir()]
    if not snapshots:
        return Path()
    return max(snapshots, key=lambda path: path.stat().st_mtime if path.exists() else 0.0)


def _local_llm_detect_max_context(provider: str, model_path: str = "", model_root: str = "", repo_id: str = "") -> tuple[int, str]:
    provider_token = _normalize_hydra_llm_provider(provider)
    path = Path(str(model_path or "")).expanduser() if str(model_path or "").strip() else Path()
    if not path.exists():
        path = _local_llm_repo_snapshot_path(model_root, repo_id)
    if not path.exists():
        return 0, ""
    if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP:
        gguf_value = _local_llm_context_from_gguf(path)
        if gguf_value:
            return gguf_value, "gguf"
    json_value = _local_llm_context_from_json_files(path)
    if json_value:
        return json_value, "config"
    return 0, ""


def _local_llm_provider_cache_rows(provider: str) -> List[Dict[str, Any]]:
    provider_token = _normalize_hydra_llm_provider(provider)
    root = _local_llm_model_root(provider_token)
    if not root.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for repo_dir in root.glob("models--*--*"):
        if not repo_dir.is_dir():
            continue
        repo_bits = repo_dir.name.split("--")
        if len(repo_bits) < 3:
            continue
        repo_id = f"{repo_bits[1]}/{'/'.join(repo_bits[2:])}"
        snapshots_dir = repo_dir / "snapshots"
        if not snapshots_dir.exists():
            continue
        snapshots = [path for path in snapshots_dir.iterdir() if path.is_dir()]
        if not snapshots:
            continue
        latest_snapshot = max(snapshots, key=lambda path: path.stat().st_mtime if path.exists() else 0.0)
        if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP:
            gguf_files = sorted(
                path for path in latest_snapshot.rglob("*.gguf") if "mmproj" not in path.name.lower()
            )
            for gguf in gguf_files:
                try:
                    rel = gguf.relative_to(latest_snapshot).as_posix()
                except Exception:
                    rel = gguf.name
                model_id = f"{repo_id}::{rel}"
                mmproj_path = _local_llm_mmproj_for_gguf(gguf)
                try:
                    mmproj_rel = mmproj_path.relative_to(latest_snapshot).as_posix() if mmproj_path else ""
                except Exception:
                    mmproj_rel = mmproj_path.name if mmproj_path else ""
                max_context, context_source = _local_llm_detect_max_context(
                    provider_token,
                    model_path=str(gguf),
                    model_root=str(root),
                    repo_id=repo_id,
                )
                rows.append(
                    {
                        "provider": provider_token,
                        "provider_label": _hydra_llm_provider_label(provider_token),
                        "model": model_id,
                        "repo_id": repo_id,
                        "filename": rel,
                        "model_path": str(gguf),
                        "model_root": str(root),
                        "status": "ready",
                        "source": "cache-scan",
                        "downloaded_ts": float(gguf.stat().st_mtime if gguf.exists() else time.time()),
                        "max_context_tokens": max_context,
                        "context_source": context_source,
                        "supports_vision": bool(mmproj_path),
                        "mmproj_filename": mmproj_rel,
                        "mmproj_path": str(mmproj_path) if mmproj_path else "",
                    }
                )
        else:
            max_context, context_source = _local_llm_detect_max_context(
                provider_token,
                model_path=str(latest_snapshot),
                model_root=str(root),
                repo_id=repo_id,
            )
            rows.append(
                {
                    "provider": provider_token,
                    "provider_label": _hydra_llm_provider_label(provider_token),
                    "model": repo_id,
                    "repo_id": repo_id,
                    "filename": "",
                    "model_path": str(latest_snapshot),
                    "model_root": str(root),
                    "status": "ready",
                    "source": "cache-scan",
                    "downloaded_ts": float(latest_snapshot.stat().st_mtime if latest_snapshot.exists() else time.time()),
                    "max_context_tokens": max_context,
                    "context_source": context_source,
                    "supports_vision": bool(
                        provider_token != HYDRA_LLM_PROVIDER_MLX_LM
                        and _local_llm_json_files_support_vision(latest_snapshot)
                    ),
                }
            )
    return rows


def _read_local_llm_model_registry() -> List[Dict[str, Any]]:
    path = _local_llm_model_registry_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    except Exception:
        return []
    rows = payload.get("models") if isinstance(payload, dict) else payload
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _write_local_llm_model_registry(rows: List[Dict[str, Any]]) -> None:
    path = _local_llm_model_registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"models": rows, "updated_ts": time.time()}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _normalize_local_llm_model_row(row: Dict[str, Any]) -> Dict[str, Any]:
    provider = _normalize_hydra_llm_provider(row.get("provider"))
    model = str(row.get("model") or row.get("model_id") or "").strip()
    repo_id = str(row.get("repo_id") or "").strip()
    filename = str(row.get("filename") or "").strip()
    model_path = str(row.get("model_path") or "").strip()
    model_root = str(row.get("model_root") or "").strip()
    if provider == HYDRA_LLM_PROVIDER_LLAMA_CPP:
        if "::" in model:
            model_repo, model_filename = model.split("::", 1)
            repo_id = repo_id or model_repo.strip()
            filename = filename or model_filename.strip()
        elif model and "/" in model and not repo_id:
            repo_id = model
        path_obj = Path(model_path).expanduser() if model_path else Path()
        if repo_id and not filename and path_obj.is_file() and path_obj.suffix.lower() == ".gguf" and "mmproj" not in path_obj.name.lower():
            snapshot_path = _local_llm_repo_snapshot_path(model_root, repo_id)
            try:
                filename = path_obj.relative_to(snapshot_path).as_posix() if snapshot_path.exists() else path_obj.name
            except Exception:
                filename = path_obj.name
        if repo_id and filename:
            model = f"{repo_id}::{filename}"
    if not model and repo_id:
        model = _hf_browser_effective_model_id(provider, repo_id, filename)
    max_context = _local_llm_context_value(row.get("max_context_tokens"))
    context_source = str(row.get("context_source") or "").strip()
    if not max_context:
        max_context, context_source = _local_llm_detect_max_context(
            provider,
            model_path=model_path,
            model_root=model_root,
            repo_id=repo_id or (model.split("::", 1)[0] if provider == HYDRA_LLM_PROVIDER_LLAMA_CPP else model),
        )
    supports_vision = bool(row.get("supports_vision"))
    mmproj_filename = str(row.get("mmproj_filename") or "").strip()
    mmproj_path = str(row.get("mmproj_path") or "").strip()
    if provider == HYDRA_LLM_PROVIDER_MLX_LM:
        supports_vision = False
    elif provider == HYDRA_LLM_PROVIDER_LLAMA_CPP:
        path_obj = Path(str(row.get("model_path") or "")).expanduser()
        if path_obj.exists():
            detected_mmproj = _local_llm_mmproj_for_gguf(path_obj)
            if detected_mmproj:
                supports_vision = True
                mmproj_path = str(detected_mmproj)
                if not mmproj_filename:
                    mmproj_filename = detected_mmproj.name
    elif not supports_vision:
        path_obj = Path(str(row.get("model_path") or "")).expanduser()
        supports_vision = bool(path_obj.exists() and _local_llm_json_files_support_vision(path_obj))
    return {
        "provider": provider,
        "provider_label": _hydra_llm_provider_label(provider),
        "model": model,
        "repo_id": repo_id,
        "filename": filename,
        "model_path": model_path,
        "model_root": model_root,
        "status": str(row.get("status") or "ready").strip() or "ready",
        "source": str(row.get("source") or "").strip(),
        "downloaded_ts": float(row.get("downloaded_ts") or row.get("updated_ts") or 0.0),
        "max_context_tokens": int(max_context or 0),
        "context_source": context_source,
        "supports_vision": supports_vision,
        "mmproj_filename": mmproj_filename,
        "mmproj_path": mmproj_path,
    }


def _local_llm_model_dedupe_key(row: Dict[str, Any]) -> Tuple[str, str]:
    provider = _normalize_hydra_llm_provider(row.get("provider"))
    model = str(row.get("model") or "").strip()
    if provider != HYDRA_LLM_PROVIDER_LLAMA_CPP:
        return (provider, model)
    repo_id = str(row.get("repo_id") or "").strip()
    filename = str(row.get("filename") or "").strip()
    if "::" in model:
        model_repo, model_filename = model.split("::", 1)
        repo_id = repo_id or model_repo.strip()
        filename = filename or model_filename.strip()
    if repo_id and filename:
        return (provider, f"{repo_id}::{filename}")
    if repo_id:
        return (provider, repo_id)
    return (provider, model)


def _local_llm_model_row_score(row: Dict[str, Any]) -> Tuple[int, int, int, float]:
    provider = _normalize_hydra_llm_provider(row.get("provider"))
    model = str(row.get("model") or "").strip()
    filename = str(row.get("filename") or "").strip()
    explicit_file = provider == HYDRA_LLM_PROVIDER_LLAMA_CPP and bool(filename or "::" in model)
    cache_scan = str(row.get("source") or "").strip() == "cache-scan"
    model_path = str(row.get("model_path") or "").strip()
    path_exists = bool(model_path and Path(model_path).expanduser().exists())
    return (
        1 if explicit_file else 0,
        1 if cache_scan else 0,
        1 if path_exists else 0,
        float(row.get("downloaded_ts") or 0.0),
    )


def _local_llm_models_payload(provider: str = "") -> Dict[str, Any]:
    provider_filter = _normalize_hydra_llm_provider(provider) if str(provider or "").strip() else ""
    rows = [_normalize_local_llm_model_row(row) for row in _read_local_llm_model_registry()]
    rows.extend(_local_llm_provider_cache_rows(HYDRA_LLM_PROVIDER_HF_TRANSFORMERS))
    rows.extend(_local_llm_provider_cache_rows(HYDRA_LLM_PROVIDER_LLAMA_CPP))
    rows.extend(_local_llm_provider_cache_rows(HYDRA_LLM_PROVIDER_MLX_LM))
    explicit_llama_repos = {
        str(row.get("repo_id") or "").strip()
        for row in rows
        if _normalize_hydra_llm_provider(row.get("provider")) == HYDRA_LLM_PROVIDER_LLAMA_CPP
        and str(row.get("repo_id") or "").strip()
        and str(row.get("filename") or "").strip()
    }
    deduped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in rows:
        provider_token = _normalize_hydra_llm_provider(row.get("provider"))
        model = str(row.get("model") or "").strip()
        if not model or not _is_local_hydra_llm_provider(provider_token):
            continue
        if provider_filter and provider_token != provider_filter:
            continue
        if (
            provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP
            and not str(row.get("filename") or "").strip()
            and str(row.get("repo_id") or "").strip() in explicit_llama_repos
        ):
            continue
        key = _local_llm_model_dedupe_key(row)
        current = deduped.get(key)
        if not current or _local_llm_model_row_score(row) >= _local_llm_model_row_score(current):
            row["provider"] = provider_token
            row["provider_label"] = _hydra_llm_provider_label(provider_token)
            deduped[key] = row
    models = sorted(
        deduped.values(),
        key=lambda row: (
            str(row.get("provider_label") or ""),
            -float(row.get("downloaded_ts") or 0.0),
            str(row.get("model") or "").lower(),
        ),
    )
    by_provider: Dict[str, List[Dict[str, Any]]] = {}
    for row in models:
        by_provider.setdefault(str(row.get("provider") or ""), []).append(row)
    return {"models": models, "by_provider": by_provider, "updated_ts": time.time()}


def _local_llm_chat_template_model_info(provider: str, model: str) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(provider)
    if not _is_local_hydra_llm_provider(provider_token):
        raise HTTPException(status_code=400, detail="Local LLM provider is required.")
    model_token = str(model or "").strip()
    if not model_token:
        raise HTTPException(status_code=400, detail="Local model is required.")
    payload = _local_llm_models_payload(provider=provider_token)
    row = next(
        (
            dict(item)
            for item in payload.get("models", [])
            if _normalize_hydra_llm_provider(item.get("provider")) == provider_token
            and str(item.get("model") or "").strip() == model_token
        ),
        {},
    )
    model_path = str(row.get("model_path") or "").strip()
    info = get_local_llm_chat_template_info(provider_token, model_token, model_path=model_path)
    info["installed"] = bool(row)
    info["provider"] = provider_token
    info["provider_label"] = _hydra_llm_provider_label(provider_token)
    info["model_path"] = model_path or str(info.get("model_path") or "")
    return info


def _llama_cpp_chat_template_model_info(model: str) -> Dict[str, Any]:
    return _local_llm_chat_template_model_info(HYDRA_LLM_PROVIDER_LLAMA_CPP, model)


def _record_downloaded_local_llm_model(
    *,
    provider: str,
    model: str,
    model_root: str = "",
    model_path: str = "",
    source: str = "",
    supports_vision: bool = False,
    mmproj_path: str = "",
) -> None:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model or "").strip()
    if not model_token or not _is_local_hydra_llm_provider(provider_token):
        return
    rows = [_normalize_local_llm_model_row(row) for row in _read_local_llm_model_registry()]
    next_row = {
        "provider": provider_token,
        "provider_label": _hydra_llm_provider_label(provider_token),
        "model": model_token,
        "repo_id": model_token.split("::", 1)[0] if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP else model_token,
        "filename": model_token.split("::", 1)[1] if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP and "::" in model_token else "",
        "model_path": str(model_path or "").strip(),
        "model_root": str(model_root or "").strip(),
        "status": "ready",
        "source": str(source or "huggingface-browser").strip(),
        "downloaded_ts": time.time(),
        "supports_vision": bool(supports_vision),
        "mmproj_path": str(mmproj_path or "").strip(),
    }
    if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP and str(mmproj_path or "").strip():
        next_row["mmproj_filename"] = Path(str(mmproj_path or "")).name
    max_context, context_source = _local_llm_detect_max_context(
        provider_token,
        model_path=next_row["model_path"],
        model_root=next_row["model_root"],
        repo_id=next_row["repo_id"],
    )
    if max_context:
        next_row["max_context_tokens"] = max_context
        next_row["context_source"] = context_source
    next_key = _local_llm_model_dedupe_key(next_row)
    replaced = False
    out: List[Dict[str, Any]] = []
    for row in rows:
        row_provider = _normalize_hydra_llm_provider(row.get("provider"))
        row_repo = str(row.get("repo_id") or "").strip()
        row_filename = str(row.get("filename") or "").strip()
        stale_llama_repo_row = (
            provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP
            and row_provider == provider_token
            and bool(str(next_row.get("repo_id") or "").strip())
            and bool(str(next_row.get("filename") or "").strip())
            and row_repo == str(next_row.get("repo_id") or "").strip()
            and not row_filename
        )
        replace_row = (
            row_provider == provider_token
            and (
                str(row.get("model") or "").strip() == model_token
                or _local_llm_model_dedupe_key(row) == next_key
                or stale_llama_repo_row
            )
        )
        if replace_row:
            if replaced:
                continue
            merged_row = dict(next_row)
            if not bool(merged_row.get("supports_vision")) and bool(row.get("supports_vision")):
                merged_row["supports_vision"] = True
            if not str(merged_row.get("mmproj_path") or "").strip() and str(row.get("mmproj_path") or "").strip():
                merged_row["mmproj_path"] = str(row.get("mmproj_path") or "").strip()
            if not str(merged_row.get("mmproj_filename") or "").strip() and str(row.get("mmproj_filename") or "").strip():
                merged_row["mmproj_filename"] = str(row.get("mmproj_filename") or "").strip()
            out.append(merged_row)
            replaced = True
        else:
            out.append(row)
    if not replaced:
        out.append(next_row)
    try:
        _write_local_llm_model_registry(out)
    except Exception as exc:
        logger.warning("[local-llm-models] failed recording downloaded model %s:%s: %s", provider_token, model_token, exc)


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def _local_llm_repo_cache_dir(provider: str, repo_id: str) -> Path:
    root = _local_llm_model_root(provider)
    repo = str(repo_id or "").strip()
    if "/" not in repo:
        return Path()
    return root / f"models--{repo.replace('/', '--')}"


def _prune_empty_local_model_dirs(start: Path, stop: Path) -> None:
    try:
        current = start.resolve()
        stop_path = stop.resolve()
    except Exception:
        return
    while current != stop_path and _path_within(current, stop_path):
        try:
            current.rmdir()
        except Exception:
            break
        current = current.parent


def _delete_local_model_path(path: Path, root: Path, deleted_paths: List[str]) -> None:
    if not str(path or "").strip() or (not path.exists() and not path.is_symlink()):
        return
    if not _path_within(path, root):
        raise HTTPException(status_code=400, detail="Refusing to delete a model path outside Tater's local model folder.")

    blob_target: Optional[Path] = None
    try:
        if path.is_symlink():
            resolved = path.resolve()
            if resolved.exists() and _path_within(resolved, root):
                blob_target = resolved
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            path.unlink()
        deleted_paths.append(str(path))
        if blob_target is not None and blob_target.exists() and _path_within(blob_target, root):
            try:
                blob_target.unlink()
                deleted_paths.append(str(blob_target))
            except IsADirectoryError:
                pass
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not delete local model file: {exc}") from exc


def _local_llm_model_usage(provider: str, model: str) -> List[str]:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model or "").strip()
    usage: List[str] = []
    if not model_token:
        return usage

    try:
        base_rows = resolve_hydra_base_servers(redis_conn=redis_client, include_legacy=True)
    except Exception:
        base_rows = []
    for idx, row in enumerate(base_rows or []):
        if (
            _normalize_hydra_llm_provider((row or {}).get("provider")) == provider_token
            and str((row or {}).get("model") or "").strip() == model_token
        ):
            usage.append("Base Model" if idx == 0 else f"Additional Base Server {idx}")

    try:
        from spudex.settings import get_spudex_settings

        spudex_settings = get_spudex_settings(redis_client)
        if (
            _normalize_hydra_llm_provider(spudex_settings.get("llm_provider")) == provider_token
            and str(spudex_settings.get("llm_model") or "").strip() == model_token
        ):
            usage.append("Spudex LLM")
    except Exception:
        pass

    try:
        vision_settings = get_shared_vision_settings(
            default_api_base="http://127.0.0.1:1234",
            default_model="qwen2.5-vl-7b-instruct",
        )
        if (
            str(vision_settings.get("mode") or "").strip().lower() == "dedicated"
            and _normalize_hydra_llm_provider(vision_settings.get("provider")) == provider_token
            and str(vision_settings.get("model") or "").strip() == model_token
        ):
            usage.append("Vision Model")
    except Exception:
        pass

    for role in HYDRA_BEAST_CONFIG_ROLE_IDS:
        try:
            role_provider = _normalize_hydra_llm_provider(redis_client.get(_hydra_role_llm_key(role, "provider")))
            role_model = str(redis_client.get(_hydra_role_llm_key(role, "model")) or "").strip()
        except Exception:
            continue
        if role_provider == provider_token and role_model == model_token:
            usage.append(f"Beast {role.title()}")
    return usage


def _delete_local_llm_model(provider: str, model: str) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(provider)
    model_token = str(model or "").strip()
    if not model_token or not _is_local_hydra_llm_provider(provider_token):
        raise HTTPException(status_code=400, detail="Choose an installed local model to delete.")

    key = _hf_llm_warmup_item_key(provider_token, model_token)
    with hf_llm_warmup_lock:
        if bool(hf_llm_warmup_state.get("running")):
            for raw_item in list(hf_llm_warmup_state.get("items") or []):
                item = dict(raw_item) if isinstance(raw_item, dict) else {}
                item_key = str(
                    item.get("key") or _hf_llm_warmup_item_key(item.get("provider", ""), item.get("model", ""))
                ).strip()
                status = str(item.get("status") or "").strip().lower()
                if item_key == key and status not in {"loaded", "downloaded", "error", "cancelled", "canceled"}:
                    raise HTTPException(status_code=409, detail="This model is downloading right now. Cancel or wait for it to finish before deleting.")

    usage = _local_llm_model_usage(provider_token, model_token)
    if usage:
        raise HTTPException(
            status_code=409,
            detail=f"This model is selected in {', '.join(usage)}. Change that setting before deleting it.",
        )

    payload = _local_llm_models_payload(provider=provider_token)
    row = next(
        (
            dict(item)
            for item in payload.get("models", [])
            if isinstance(item, dict) and str(item.get("model") or "").strip() == model_token
        ),
        {},
    )
    if not row:
        raise HTTPException(status_code=404, detail="Local model is not installed.")

    root = Path(str(row.get("model_root") or _local_llm_model_root(provider_token))).expanduser().resolve()
    repo_id = str(row.get("repo_id") or "").strip()
    filename = str(row.get("filename") or "").strip()
    if not repo_id and provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP and "::" in model_token:
        repo_id, filename = model_token.split("::", 1)
    elif not repo_id:
        repo_id = model_token
    if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP and not filename and "::" in model_token:
        filename = model_token.split("::", 1)[1]

    deleted_paths: List[str] = []
    if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP and filename:
        raw_model_path = str(row.get("model_path") or "").strip()
        model_path = Path(raw_model_path).expanduser() if raw_model_path else Path()
        if raw_model_path:
            _delete_local_model_path(model_path, root, deleted_paths)
            _prune_empty_local_model_dirs(model_path.parent, root)
        repo_dir = _local_llm_repo_cache_dir(provider_token, repo_id)
        if repo_dir.exists() and _path_within(repo_dir, root) and not list(repo_dir.rglob("*.gguf")):
            _delete_local_model_path(repo_dir, root, deleted_paths)
    else:
        repo_dir = _local_llm_repo_cache_dir(provider_token, repo_id)
        if repo_dir.exists() and _path_within(repo_dir, root):
            _delete_local_model_path(repo_dir, root, deleted_paths)
        else:
            raw_model_path = str(row.get("model_path") or "").strip()
            model_path = Path(raw_model_path).expanduser() if raw_model_path else Path()
            if raw_model_path:
                if model_path.is_dir():
                    candidate = model_path
                    while candidate.parent != candidate and candidate.name != f"models--{repo_id.replace('/', '--')}":
                        candidate = candidate.parent
                    if candidate.name == f"models--{repo_id.replace('/', '--')}":
                        model_path = candidate
                _delete_local_model_path(model_path, root, deleted_paths)

    rows = [_normalize_local_llm_model_row(item) for item in _read_local_llm_model_registry()]
    rows = [
        item
        for item in rows
        if not (
            _normalize_hydra_llm_provider(item.get("provider")) == provider_token
            and str(item.get("model") or "").strip() == model_token
        )
    ]
    _write_local_llm_model_registry(rows)

    return {
        "provider": provider_token,
        "provider_label": _hydra_llm_provider_label(provider_token),
        "model": model_token,
        "deleted_paths": deleted_paths,
    }


def _speech_model_warmup_on_startup_enabled() -> bool:
    token = str(os.getenv("TATER_SPEECH_WARMUP_ON_STARTUP", "true") or "true").strip().lower()
    return token in {"1", "true", "yes", "on", "enabled"}


def _speech_model_warmup_tts_items(settings: Dict[str, Any]) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    seen: set[Tuple[str, str, str, str]] = set()
    for prefix in ("", "announcement_"):
        backend = str(settings.get(f"{prefix}tts_backend") or "").strip()
        model = str(settings.get(f"{prefix}tts_model") or "").strip()
        voice = str(settings.get(f"{prefix}tts_voice") or "").strip()
        key = (backend, model, voice, str(settings.get("acceleration") or "").strip())
        if not backend or key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "kind": "tts",
                "backend": backend,
                "model": model,
                "voice": voice,
                "acceleration": str(settings.get("acceleration") or "").strip(),
            }
        )
    return items


def _warm_speech_model_item(item: Dict[str, str]) -> str:
    kind = str(item.get("kind") or "").strip()
    backend = str(item.get("backend") or "").strip()
    if kind == "stt":
        from tater_voice import voice_pipeline as voice_pipeline

        token = voice_pipeline._normalize_stt_backend(backend)
        if token == "wyoming":
            return "skipped remote Wyoming STT"
        ok, reason = voice_pipeline._stt_backend_available(token)
        if not ok:
            raise RuntimeError(reason or f"{token} unavailable")
        if token == "faster_whisper":
            voice_pipeline._load_faster_whisper_model()
            if voice_pipeline._faster_whisper_device() == "cuda":
                silence = b"\x00\x00" * int(16000 * 0.5)
                voice_pipeline._transcribe_faster_whisper_sync(
                    silence,
                    {"rate": 16000, "width": 2, "channels": 1},
                    None,
                )
                return f"loaded STT {token} and warmed CUDA decode"
        elif token == "vosk":
            voice_pipeline._load_vosk_model()
        else:
            raise RuntimeError(f"unsupported STT backend: {token}")
        return f"loaded STT {token}"

    if kind == "tts":
        from tater_voice import voice_pipeline

        token = voice_pipeline._normalize_tts_backend(backend)
        model = str(item.get("model") or "").strip()
        if token == "wyoming":
            return "skipped remote Wyoming TTS"
        if token == "openai_compatible":
            return "skipped remote OpenAI-compatible TTS"
        if token == "kokoro":
            voice_pipeline._load_kokoro_pipeline(model or voice_pipeline.DEFAULT_KOKORO_MODEL)
        elif token == "piper":
            voice_pipeline._load_piper_voice_model(model or voice_pipeline.DEFAULT_PIPER_MODEL)
        elif token == "pocket_tts":
            voice_pipeline._load_pocket_tts_model(model or voice_pipeline.DEFAULT_POCKET_TTS_MODEL)
        else:
            raise RuntimeError(f"unsupported TTS backend: {token}")
        return f"loaded TTS {token}"

    if kind == "speaker_id":
        from tater_voice import speaker_id as esphome_speaker_id

        token = backend.lower()
        if token != "speechbrain":
            raise RuntimeError(f"unsupported Speaker ID backend: {token or 'unknown'}")
        return esphome_speaker_id.warmup_model(enabled_only=True)

    if kind == "emotion_id":
        from tater_voice import emotion_id as esphome_emotion_id

        token = backend.lower()
        if token != "speechbrain":
            raise RuntimeError(f"unsupported Emotion ID backend: {token or 'unknown'}")
        return esphome_emotion_id.warmup_model(enabled_only=True)

    if kind == "wake_word":
        from tater_voice import openwakeword_engine
        from tater_voice import nanowakeword_engine

        token = backend.lower()
        if token == "openwakeword":
            return openwakeword_engine.warmup_model(enabled_only=True)
        if token == "nanowakeword":
            return nanowakeword_engine.warmup_model(enabled_only=True)
        else:
            raise RuntimeError(f"unsupported wake-word backend: {token or 'unknown'}")

    raise RuntimeError(f"unsupported warmup item: {kind or 'unknown'}")


def _run_speech_model_warmup(settings: Dict[str, Any], *, reason: str) -> None:
    items = [
        {"kind": "stt", "backend": str(settings.get("stt_backend") or "").strip()},
        *_speech_model_warmup_tts_items(settings),
        {"kind": "wake_word", "backend": "openwakeword"},
        {"kind": "wake_word", "backend": "nanowakeword"},
        {"kind": "speaker_id", "backend": "speechbrain"},
        {"kind": "emotion_id", "backend": "speechbrain"},
    ]
    items = [item for item in items if str(item.get("backend") or "").strip()]
    with speech_model_warmup_lock:
        speech_model_warmup_state.update(
            {
                "running": True,
                "started_ts": time.time(),
                "finished_ts": 0.0,
                "reason": reason,
                "items": [dict(item, status="pending") for item in items],
                "errors": [],
            }
        )

    logger.info("[speech-warmup] starting reason=%s items=%s", reason, items)
    item_states: List[Dict[str, Any]] = []
    errors: List[str] = []
    for item in items:
        row = dict(item)
        label = f"{row.get('kind')}:{row.get('backend')}"
        row["started_ts"] = time.time()
        try:
            row["message"] = _warm_speech_model_item(row)
            row["status"] = "loaded" if not str(row["message"]).startswith("skipped") else "skipped"
            logger.info("[speech-warmup] %s %s", label, row["message"])
        except Exception as exc:
            message = str(exc) or type(exc).__name__
            row["status"] = "error"
            row["error"] = message
            errors.append(f"{label}: {message}")
            logger.warning("[speech-warmup] %s failed: %s", label, message)
        row["finished_ts"] = time.time()
        item_states.append(row)
        with speech_model_warmup_lock:
            speech_model_warmup_state["items"] = list(item_states) + [
                dict(pending, status="pending") for pending in items[len(item_states) :]
            ]
            speech_model_warmup_state["errors"] = list(errors)

    with speech_model_warmup_lock:
        speech_model_warmup_state.update(
            {
                "running": False,
                "finished_ts": time.time(),
                "items": item_states,
                "errors": errors,
            }
        )
    logger.info("[speech-warmup] finished errors=%s", len(errors))


def _start_speech_model_warmup(settings: Dict[str, Any], *, reason: str = "settings-save") -> Dict[str, Any]:
    with speech_model_warmup_lock:
        if bool(speech_model_warmup_state.get("running")):
            snapshot = _speech_model_warmup_snapshot()
            snapshot["started"] = False
            snapshot["already_running"] = True
            return snapshot
        speech_model_warmup_state.update(
            {
                "running": True,
                "started_ts": time.time(),
                "finished_ts": 0.0,
                "reason": reason,
                "items": [],
                "errors": [],
            }
        )

    thread = threading.Thread(
        target=_run_speech_model_warmup,
        kwargs={"settings": dict(settings or {}), "reason": reason},
        daemon=True,
        name="speech-model-warmup",
    )
    thread.start()
    snapshot = _speech_model_warmup_snapshot()
    snapshot["started"] = True
    snapshot["already_running"] = False
    return snapshot


def _speech_model_warmup_startup_wait_seconds() -> float:
    raw = str(os.getenv("TATER_SPEECH_WARMUP_STARTUP_WAIT_SECONDS") or "120").strip()
    try:
        value = float(raw)
    except Exception:
        value = 120.0
    return max(0.0, min(1800.0, value))


def _wait_for_speech_model_warmup(*, timeout: Optional[float] = None) -> Dict[str, Any]:
    wait_seconds = _speech_model_warmup_startup_wait_seconds() if timeout is None else float(timeout)
    if wait_seconds <= 0:
        snapshot = _speech_model_warmup_snapshot()
        snapshot["waited_seconds"] = 0.0
        snapshot["startup_wait_timeout"] = bool(snapshot.get("running"))
        return snapshot

    started_wait = time.time()
    deadline = started_wait + wait_seconds
    while time.time() < deadline:
        snapshot = _speech_model_warmup_snapshot()
        if not bool(snapshot.get("running")):
            snapshot["waited_seconds"] = round(max(0.0, time.time() - started_wait), 2)
            snapshot["startup_wait_timeout"] = False
            return snapshot
        time.sleep(0.25)

    snapshot = _speech_model_warmup_snapshot()
    snapshot["waited_seconds"] = round(max(0.0, time.time() - started_wait), 2)
    snapshot["startup_wait_timeout"] = bool(snapshot.get("running"))
    if snapshot["startup_wait_timeout"]:
        logger.info(
            "[speech-warmup] startup wait timed out after %.1fs; continuing boot while warmup finishes",
            wait_seconds,
        )
    return snapshot


def _reload_local_tts_model_caches(*, reason: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "ok": True,
        "reason": reason,
        "voice_pipeline": {},
        "announcement_tts": {},
        "errors": [],
    }
    try:
        from tater_voice import voice_pipeline

        result["voice_pipeline"] = voice_pipeline.clear_tts_model_caches()
    except Exception as exc:
        message = str(exc) or type(exc).__name__
        result["errors"].append(f"voice_pipeline: {message}")
        logger.warning("[speech-warmup] native TTS cache clear failed: %s", message)

    try:
        result["announcement_tts"] = clear_announcement_tts_model_caches()
    except Exception as exc:
        message = str(exc) or type(exc).__name__
        result["errors"].append(f"announcement_tts: {message}")
        logger.warning("[speech-warmup] announcement TTS cache clear failed: %s", message)

    result["ok"] = not bool(result["errors"])
    if result["ok"]:
        logger.info(
            "[speech-warmup] cleared TTS model caches reason=%s voice_pipeline=%s announcement_tts=%s",
            reason,
            result["voice_pipeline"],
            result["announcement_tts"],
        )
    return result


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
    if token in {"llama", "llamacpp", "llama_cpp", "llama.cpp", "gguf"}:
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


def _hydra_llm_provider_label(provider: Any) -> str:
    token = _normalize_hydra_llm_provider(provider)
    if token == HYDRA_LLM_PROVIDER_HF_TRANSFORMERS:
        return "Hugging Face Transformers"
    if token == HYDRA_LLM_PROVIDER_LLAMA_CPP:
        return "llama.cpp GGUF"
    if token == HYDRA_LLM_PROVIDER_MLX_LM:
        return "MLX LM"
    if token == HYDRA_LLM_PROVIDER_SPUD_LINK:
        return "Spud Link"
    return "OpenAI-Compatible API"


def _normalize_hydra_base_server_rows(rows: Any) -> List[Dict[str, str]]:
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="hydra_base_servers must be a list")

    normalized: List[Dict[str, str]] = []
    seen: set[Tuple[str, str, str, str]] = set()

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            raise HTTPException(status_code=400, detail=f"hydra_base_servers[{idx}] must be an object")

        provider = _normalize_hydra_llm_provider(row.get("provider"))
        host = str(row.get("host") or "").strip()
        port = str(row.get("port") or "").strip()
        model = str(row.get("model") or "").strip()
        api_key = str(row.get("api_key") or "").strip()

        if provider == HYDRA_LLM_PROVIDER_SPUD_LINK:
            continue
        if not host and not port and not model:
            continue
        if _is_local_hydra_llm_provider(provider):
            if not model:
                raise HTTPException(status_code=400, detail=f"hydra_base_servers[{idx}].model is required")
            signature = (provider, "", model, "")
            if signature in seen:
                continue
            seen.add(signature)
            normalized.append(
                {
                    "provider": provider,
                    "host": "",
                    "port": "",
                    "model": model,
                    "api_key": "",
                }
            )
            continue
        if not host:
            raise HTTPException(status_code=400, detail=f"hydra_base_servers[{idx}].host is required")
        if not model:
            raise HTTPException(status_code=400, detail=f"hydra_base_servers[{idx}].model is required")

        endpoint = _build_hydra_llm_endpoint(host, port)
        if not endpoint:
            raise HTTPException(status_code=400, detail=f"hydra_base_servers[{idx}] host/port is invalid")

        parsed = urlparse(endpoint)
        hostname = str(parsed.hostname or "").strip()
        if not hostname:
            raise HTTPException(status_code=400, detail=f"hydra_base_servers[{idx}] host is invalid")

        host_with_scheme = host.startswith(("http://", "https://"))
        canonical_host = f"{parsed.scheme}://{hostname}" if host_with_scheme else hostname
        canonical_port = str(parsed.port) if parsed.port is not None else ""
        signature = (provider, endpoint, model, api_key)
        if signature in seen:
            continue
        seen.add(signature)
        normalized.append(
            {
                "provider": provider,
                "host": canonical_host,
                "port": canonical_port,
                "model": model,
                "api_key": api_key,
            }
        )

    return normalized


def _set_hydra_legacy_base_keys(base_rows: List[Dict[str, str]]) -> None:
    rows = [row for row in (base_rows or []) if isinstance(row, dict)]
    if not rows:
        redis_client.delete(HYDRA_LLM_HOST_KEY)
        redis_client.delete(HYDRA_LLM_PORT_KEY)
        redis_client.delete(HYDRA_LLM_MODEL_KEY)
        redis_client.delete(HYDRA_LLM_PROVIDER_KEY)
        return

    first = rows[0]
    first_provider = _normalize_hydra_llm_provider(first.get("provider"))
    first_host = str(first.get("host") or "").strip()
    first_port = str(first.get("port") or "").strip()
    first_model = str(first.get("model") or "").strip()

    redis_client.set(HYDRA_LLM_PROVIDER_KEY, first_provider)

    if first_host:
        redis_client.set(HYDRA_LLM_HOST_KEY, first_host)
    else:
        redis_client.delete(HYDRA_LLM_HOST_KEY)

    if first_port:
        redis_client.set(HYDRA_LLM_PORT_KEY, first_port)
    else:
        redis_client.delete(HYDRA_LLM_PORT_KEY)

    if first_model:
        redis_client.set(HYDRA_LLM_MODEL_KEY, first_model)
    else:
        redis_client.delete(HYDRA_LLM_MODEL_KEY)


def _hydra_role_llm_key(role: str, field: str) -> str:
    return f"{HYDRA_ROLE_LLM_KEY_PREFIX}{str(role or '').strip()}:{str(field or '').strip()}"


def _discover_runtime_webui_tabs(
    surface_entries: List[Dict[str, Any]],
    *,
    runtime: SurfaceRuntimeManager,
    kind: str,
    require_desired_running: bool,
) -> List[Dict[str, Any]]:
    discovered: List[Dict[str, Any]] = []
    seen_labels = set()

    for entry in surface_entries or []:
        if not isinstance(entry, dict):
            continue

        key = str(entry.get("key") or "").strip()
        if not key:
            continue
        if not _as_bool_flag(entry.get("has_webui_tab_renderer"), False):
            continue

        tab_cfg = entry.get("webui_tab") if isinstance(entry.get("webui_tab"), dict) else {}
        label = str(tab_cfg.get("label") or "").strip()
        if not label:
            continue
        if label in seen_labels:
            continue

        requires_running = _as_bool_flag(tab_cfg.get("requires_running"), True)
        desired_running = str(redis_client.get(f"{key}_running") or "").strip().lower() == "true"
        if require_desired_running and requires_running and not desired_running:
            continue

        try:
            order = int(tab_cfg.get("order", 1000))
        except Exception:
            order = 1000

        discovered.append(
            {
                "label": label,
                "core_key": key,
                "surface_key": key,
                "surface_kind": kind,
                "order": order,
                "requires_running": requires_running,
                "running": bool(runtime.is_running(key)),
            }
        )
        seen_labels.add(label)

    discovered.sort(key=lambda row: (int(row.get("order", 1000)), str(row.get("label") or "").lower()))
    return discovered


def _discover_core_webui_tabs(core_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _discover_runtime_webui_tabs(
        core_entries,
        runtime=core_runtime,
        kind="core",
        require_desired_running=True,
    )


def _discover_platform_webui_tabs(platform_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return []


def _load_surface_htmlui_tab_payload(tab_spec: Dict[str, Any]) -> Dict[str, Any]:
    key = str((tab_spec or {}).get("surface_key") or (tab_spec or {}).get("core_key") or "").strip()
    if not key:
        return {"error": "Missing surface key for tab."}

    runtime = core_runtime

    try:
        module = runtime._import_module(key, reload_module=False)
    except Exception as exc:
        return {"error": f"Import failed: {exc}"}

    provider = getattr(module, "get_htmlui_tab_data", None)
    if not callable(provider):
        return {
            "summary": "This core does not expose HTMLUI tab data yet.",
            "stats": [],
            "items": [],
            "empty_message": "No HTMLUI panel payload is available for this core.",
        }

    try:
        raw = provider(
            redis_client=redis_client,
            core_key=key,
            core_tab=tab_spec,
        )
    except Exception as exc:
        return {"error": f"Failed to build core tab data: {exc}"}

    if not isinstance(raw, dict):
        return {"error": "Core HTMLUI tab payload is invalid (expected object)."}

    stats: List[Dict[str, Any]] = []
    for item in raw.get("stats") or []:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "").strip()
        value = item.get("value")
        if not label:
            continue
        stats.append({"label": label, "value": value})

    rows: List[Dict[str, Any]] = []
    for item in raw.get("items") or []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "title": str(item.get("title") or "").strip(),
                "subtitle": str(item.get("subtitle") or "").strip(),
                "detail": str(item.get("detail") or "").strip(),
            }
        )

    ui = raw.get("ui")
    if not isinstance(ui, dict):
        ui = {}

    return {
        "summary": str(raw.get("summary") or "").strip(),
        "stats": stats,
        "items": rows,
        "empty_message": str(raw.get("empty_message") or "").strip(),
        "ui": ui,
        "updated_at": float(time.time()),
    }


def _run_surface_htmlui_tab_action(tab_spec: Dict[str, Any], payload: "CoreTabActionRequest") -> Dict[str, Any]:
    key = str((tab_spec or {}).get("surface_key") or (tab_spec or {}).get("core_key") or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="Missing surface key.")

    runtime = core_runtime
    try:
        module = runtime._import_module(key, reload_module=False)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load core module: {exc}")

    action_handler = getattr(module, "handle_htmlui_tab_action", None)
    if not callable(action_handler):
        raise HTTPException(status_code=404, detail=f"{key} does not expose handle_htmlui_tab_action().")

    action_name = str(payload.action or "").strip()
    if not action_name:
        raise HTTPException(status_code=400, detail="Missing action name.")

    try:
        result = action_handler(
            action=action_name,
            payload=payload.payload if isinstance(payload.payload, dict) else {},
            redis_client=redis_client,
            core_key=key,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"HTMLUI action failed: {exc}")

    if result is None:
        return {"ok": True}
    if not isinstance(result, dict):
        return {"ok": True, "result": result}
    return result


def _esphome_platform_tab_spec() -> Dict[str, Any]:
    return esphome_home_module.runtime_tab_spec()


def _verba_display_name(verba: Any) -> str:
    return (
        str(getattr(verba, "verba_name", "") or "").strip()
        or str(getattr(verba, "pretty_name", "") or "").strip()
        or str(getattr(verba, "name", "") or "").strip()
    )


def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        if str(content.get("phase") or "final") != "final":
            return None
        payload = content.get("content")
        if isinstance(payload, str):
            return {"role": "assistant", "content": payload[:4000]}
        if isinstance(payload, dict):
            for key in ("summary", "text", "message", "content"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return {"role": "assistant", "content": value[:4000]}
            return {"role": "assistant", "content": json.dumps(payload, ensure_ascii=False)[:2000]}

    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        rendered = json.dumps(
            {
                "function": content.get("plugin"),
                "arguments": content.get("arguments", {}),
            },
            indent=2,
        )
        return {"role": role, "content": rendered}

    if isinstance(content, dict) and content.get("type") in {"image", "audio", "video", "file"}:
        media_type = str(content.get("type") or "file")
        name = str(content.get("name") or "").strip()
        marker = f"[{media_type.capitalize()} attached]"
        if name:
            marker = f"{marker} {name}"
        return {"role": role, "content": marker}

    if isinstance(content, str):
        return {"role": role, "content": content}
    return {"role": role, "content": str(content)}


def _enforce_user_assistant_alternation(loop_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for item in loop_messages:
        if not item:
            continue
        if not merged:
            merged.append(item)
            continue
        if merged[-1].get("role") == item.get("role"):
            old = merged[-1].get("content", "")
            new = item.get("content", "")
            merged[-1]["content"] = (f"{old}\n\n{new}").strip()
        else:
            merged.append(item)

    if merged and merged[0].get("role") != "user":
        merged.insert(0, {"role": "user", "content": ""})

    return merged


def _compact_chat_history_row(row: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    if not isinstance(row, dict):
        return row, False
    content = row.get("content")
    if not isinstance(content, dict):
        return row, False
    content_type = str(content.get("type") or "").strip().lower()
    mimetype_value = str(content.get("mimetype") or content.get("mime_type") or "").strip().lower()
    if content_type not in {"image", "audio", "video", "file"} and not mimetype_value.startswith(("image/", "audio/", "video/")):
        return row, False
    has_inline_payload = any(
        isinstance(content.get(key), (bytes, bytearray)) or (isinstance(content.get(key), str) and content.get(key).strip())
        for key in ("data", "bytes", "data_b64")
    )
    has_materializable_ref = any(str(content.get(key) or "").strip() for key in ("blob_key", "path", "file_path", "artifact_path"))
    if not has_inline_payload and not has_materializable_ref:
        return row, False
    normalized = _normalize_plugin_response_item(content)
    if not isinstance(normalized, dict) or normalized == content:
        return row, False
    compacted = dict(row)
    compacted["content"] = normalized
    return compacted, True


def _load_chat_history_tail(count: int) -> List[Dict[str, Any]]:
    if count <= 0:
        return []
    try:
        list_len = int(redis_client.llen(CHAT_HISTORY_KEY) or 0)
    except Exception:
        list_len = 0
    start_index = max(0, list_len - int(count))
    raw = redis_client.lrange(CHAT_HISTORY_KEY, -count, -1)
    out: List[Dict[str, Any]] = []
    for offset, line in enumerate(raw):
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                compacted, changed = _compact_chat_history_row(parsed)
                out.append(compacted)
                if changed and list_len > 0:
                    try:
                        redis_client.lset(CHAT_HISTORY_KEY, start_index + offset, json.dumps(compacted))
                    except Exception:
                        pass
        except Exception:
            continue
    return out


def _load_chat_history() -> List[Dict[str, Any]]:
    raw = redis_client.lrange(CHAT_HISTORY_KEY, 0, -1)
    out: List[Dict[str, Any]] = []
    for idx, line in enumerate(raw):
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                compacted, changed = _compact_chat_history_row(parsed)
                out.append(compacted)
                if changed:
                    try:
                        redis_client.lset(CHAT_HISTORY_KEY, idx, json.dumps(compacted))
                    except Exception:
                        pass
        except Exception:
            continue
    return out


def _loop_messages_from_history_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    loop_messages: List[Dict[str, Any]] = []
    for msg in rows:
        role = str(msg.get("role") or "assistant")
        if role not in {"user", "assistant"}:
            role = "assistant"
        templ = _to_template_msg(role, msg.get("content"))
        if templ is not None:
            loop_messages.append(templ)
    return _enforce_user_assistant_alternation(loop_messages)


def _load_loop_messages_for_hydra(max_llm: int) -> List[Dict[str, Any]]:
    history_tail = _load_chat_history_tail(max_llm)
    return _loop_messages_from_history_rows(history_tail)


def _bytes_to_mb(byte_count: int) -> float:
    return float(byte_count) / (1024.0 * 1024.0)


def _show_speed_stats_enabled(*, default: bool = False) -> bool:
    fallback = "true" if bool(default) else "false"
    token = str(redis_client.get("tater:show_speed_stats") or fallback).strip().lower()
    return token in {"1", "true", "yes", "on", "enabled"}


def _save_last_llm_stats(stats: Any) -> None:
    if not isinstance(stats, dict):
        return

    mapping = {
        "model": str(stats.get("model") or "LLM"),
        "elapsed": str(float(stats.get("elapsed") or 0.0)),
        "prompt_elapsed": str(float(stats.get("prompt_elapsed") or 0.0)),
        "completion_elapsed": str(float(stats.get("completion_elapsed") or 0.0)),
        "prompt_tokens": str(int(stats.get("prompt_tokens") or 0)),
        "completion_tokens": str(int(stats.get("completion_tokens") or 0)),
        "total_tokens": str(int(stats.get("total_tokens") or 0)),
        "tps_total": str(float(stats.get("tps_total") or 0.0)),
        "tps_prompt": str(float(stats.get("tps_prompt") or 0.0)),
        "tps_comp": str(float(stats.get("tps_comp") or 0.0)),
        "calls": str(int(stats.get("calls") or 0)),
        "speed_basis": str(stats.get("speed_basis") or "wall_time"),
        "updated_at": str(float(stats.get("updated_at") or time.time())),
    }
    redis_client.hset(LAST_LLM_STATS_KEY, mapping=mapping)


def _load_last_llm_stats() -> Dict[str, Any]:
    raw = redis_client.hgetall(LAST_LLM_STATS_KEY) or {}
    if not isinstance(raw, dict) or not raw:
        return {}

    def _as_int(value: Any) -> int:
        try:
            return int(float(value))
        except Exception:
            return 0

    def _as_float(value: Any) -> float:
        try:
            return float(value)
        except Exception:
            return 0.0

    stats = {
        "model": str(raw.get("model") or "LLM"),
        "elapsed": max(0.0, _as_float(raw.get("elapsed"))),
        "prompt_elapsed": max(0.0, _as_float(raw.get("prompt_elapsed"))),
        "completion_elapsed": max(0.0, _as_float(raw.get("completion_elapsed"))),
        "prompt_tokens": max(0, _as_int(raw.get("prompt_tokens"))),
        "completion_tokens": max(0, _as_int(raw.get("completion_tokens"))),
        "total_tokens": max(0, _as_int(raw.get("total_tokens"))),
        "tps_total": max(0.0, _as_float(raw.get("tps_total"))),
        "tps_prompt": max(0.0, _as_float(raw.get("tps_prompt"))),
        "tps_comp": max(0.0, _as_float(raw.get("tps_comp"))),
        "calls": max(0, _as_int(raw.get("calls"))),
        "speed_basis": str(raw.get("speed_basis") or "wall_time"),
        "updated_at": max(0.0, _as_float(raw.get("updated_at"))),
    }
    return stats


def _media_type_from_mimetype(mimetype: Any) -> str:
    token = str(mimetype or "").strip().lower()
    if token.startswith("image/"):
        return "image"
    if token.startswith("audio/"):
        return "audio"
    if token.startswith("video/"):
        return "video"
    return "file"


def _store_file_blob_in_redis(file_id: str, data: bytes) -> None:
    key = f"{FILE_BLOB_KEY_PREFIX}{str(file_id or '').strip()}"
    if not key or key == FILE_BLOB_KEY_PREFIX:
        raise ValueError("Missing file id")
    redis_blob_client.set(key, data)
    if WEBUI_ATTACH_TTL_SECONDS > 0:
        redis_blob_client.expire(key, int(WEBUI_ATTACH_TTL_SECONDS))
    redis_client.rpush(FILE_INDEX_KEY, str(file_id))
    redis_client.ltrim(FILE_INDEX_KEY, -int(WEBUI_ATTACH_INDEX_MAX), -1)


def _load_file_blob_from_redis(file_id: str) -> Optional[bytes]:
    token = str(file_id or "").strip()
    if not token:
        return None
    raw = redis_blob_client.get(f"{FILE_BLOB_KEY_PREFIX}{token}")
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    return None


def _decode_attachment_data(raw_value: Any) -> bytes:
    text = str(raw_value or "").strip()
    if not text:
        raise ValueError("empty attachment payload")
    payload = text
    if text.lower().startswith("data:"):
        comma = text.find(",")
        if comma <= 0:
            raise ValueError("invalid data url")
        header = text[:comma].lower()
        if ";base64" not in header:
            raise ValueError("attachment data url must be base64")
        payload = text[comma + 1 :].strip()

    compact = "".join(payload.split())
    if not compact:
        raise ValueError("empty attachment payload")
    try:
        raw = base64.b64decode(compact, validate=True)
    except Exception as exc:
        raise ValueError("invalid base64 payload") from exc
    if not raw:
        raise ValueError("empty attachment bytes")
    return raw


def _normalize_chat_attachment_payloads(attachments_raw: Any) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not isinstance(attachments_raw, list):
        return [], []

    attachment_messages: List[Dict[str, Any]] = []
    input_artifacts: List[Dict[str, Any]] = []
    stored_ids: List[str] = []
    total_bytes = 0
    per_file_limit_mb = max(0, int(WEBUI_ATTACH_MAX_MB_EACH))
    total_limit_mb = max(0, int(WEBUI_ATTACH_MAX_MB_TOTAL))
    per_file_limit = per_file_limit_mb * 1024 * 1024
    total_limit = total_limit_mb * 1024 * 1024

    try:
        for idx, item in enumerate(attachments_raw):
            if not isinstance(item, dict):
                raise ValueError(f"attachment #{idx + 1} is invalid")

            payload_raw = item.get("data_url")
            if payload_raw is None:
                payload_raw = item.get("data_b64")
            raw = _decode_attachment_data(payload_raw)

            size = len(raw)
            if per_file_limit > 0 and size > per_file_limit:
                raise ValueError(
                    f"attachment '{str(item.get('name') or f'attachment-{idx + 1}').strip()}' "
                    f"exceeds {per_file_limit_mb}MB per-file limit"
                )
            total_bytes += size
            if total_limit > 0 and total_bytes > total_limit:
                raise ValueError(f"combined attachment size exceeds {total_limit_mb}MB limit")

            name = str(item.get("name") or f"attachment-{idx + 1}").strip() or f"attachment-{idx + 1}"
            mimetype = str(item.get("mimetype") or "application/octet-stream").strip() or "application/octet-stream"
            media_type = _media_type_from_mimetype(mimetype)

            file_id = str(uuid.uuid4())
            _store_file_blob_in_redis(file_id, raw)
            stored_ids.append(file_id)

            msg = {
                "type": media_type,
                "id": file_id,
                "name": name,
                "mimetype": mimetype,
                "size": size,
            }
            attachment_messages.append(msg)
            input_artifacts.append(
                {
                    "type": media_type,
                    "file_id": file_id,
                    "name": name,
                    "mimetype": mimetype,
                    "size": size,
                    "source": "webui_attachment",
                }
            )
    except Exception:
        for fid in stored_ids:
            try:
                redis_blob_client.delete(f"{FILE_BLOB_KEY_PREFIX}{fid}")
            except Exception:
                pass
        raise

    return attachment_messages, input_artifacts


def _guess_image_mimetype(raw: bytes) -> str:
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if raw.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if raw.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if raw.startswith(b"RIFF") and raw[8:12] == b"WEBP":
        return "image/webp"
    return "image/png"


def _normalize_avatar_b64(raw_value: Any) -> str:
    text = str(raw_value or "").strip()
    if not text:
        raise ValueError("empty avatar payload")

    payload = text
    if text.lower().startswith("data:"):
        comma = text.find(",")
        if comma <= 0:
            raise ValueError("invalid data url")
        header = text[:comma].lower()
        if ";base64" not in header:
            raise ValueError("avatar data url must be base64")
        payload = text[comma + 1 :].strip()

    compact = "".join(payload.split())
    if not compact:
        raise ValueError("empty avatar payload")

    try:
        raw = base64.b64decode(compact, validate=True)
    except Exception as exc:
        raise ValueError("invalid avatar base64") from exc
    if not raw:
        raise ValueError("empty avatar bytes")
    return base64.b64encode(raw).decode("utf-8")


def _avatar_data_url_from_b64(raw_value: Any) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""

    if text.lower().startswith("data:"):
        comma = text.find(",")
        if comma <= 0:
            return ""
        header = text[:comma].lower()
        if ";base64" not in header:
            return ""
        payload = "".join(text[comma + 1 :].split())
        if not payload:
            return ""
        return f"{text[:comma + 1]}{payload}"

    compact = "".join(text.split())
    if not compact:
        return ""
    try:
        raw = base64.b64decode(compact, validate=True)
    except Exception:
        return ""
    mimetype = _guess_image_mimetype(raw)
    normalized = base64.b64encode(raw).decode("utf-8")
    return f"data:{mimetype};base64,{normalized}"


def _read_user_avatar_data_url(chat_settings: Dict[str, Any]) -> str:
    avatar_raw = chat_settings.get("avatar")
    avatar = _avatar_data_url_from_b64(avatar_raw)
    if avatar:
        return avatar
    if str(avatar_raw or "").strip():
        try:
            redis_client.hdel("chat_settings", "avatar")
        except Exception:
            pass
    return ""


def _read_tater_avatar_data_url() -> str:
    raw_override = redis_client.get("tater:avatar")
    override_avatar = _avatar_data_url_from_b64(raw_override)
    if override_avatar:
        return override_avatar
    if str(raw_override or "").strip():
        try:
            redis_client.delete("tater:avatar")
        except Exception:
            pass

    try:
        raw_default = DEFAULT_TATER_AVATAR_PATH.read_bytes()
    except Exception:
        return ""
    mimetype = _guess_image_mimetype(raw_default)
    encoded = base64.b64encode(raw_default).decode("utf-8")
    return f"data:{mimetype};base64,{encoded}"


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(text: str) -> bytes:
    compact = str(text or "").strip()
    if not compact:
        return b""
    padding = "=" * ((4 - (len(compact) % 4)) % 4)
    return base64.urlsafe_b64decode((compact + padding).encode("utf-8"))


def _load_webui_password_hash() -> str:
    return str(redis_client.get(WEBUI_AUTH_PASSWORD_HASH_KEY) or "").strip()


def _webui_password_is_set() -> bool:
    return bool(_load_webui_password_hash())


def _hash_webui_password(password: str, *, salt: bytes, iterations: int = WEBUI_AUTH_PBKDF2_ITERATIONS) -> str:
    rounds = max(100_000, int(iterations or WEBUI_AUTH_PBKDF2_ITERATIONS))
    digest = hashlib.pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), salt, rounds)
    return f"pbkdf2_sha256${rounds}${_b64url_encode(salt)}${_b64url_encode(digest)}"


def _verify_webui_password(password: str, stored_hash: str) -> bool:
    text = str(stored_hash or "").strip()
    if not text:
        return False
    try:
        algo, rounds_raw, salt_b64, digest_b64 = text.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        rounds = max(1, int(rounds_raw))
        salt = _b64url_decode(salt_b64)
        expected = _b64url_decode(digest_b64)
        if not salt or not expected:
            return False
        computed = hashlib.pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), salt, rounds)
        return hmac.compare_digest(computed, expected)
    except Exception:
        return False


def _new_webui_session_token() -> str:
    return secrets.token_urlsafe(32)


def _webui_session_digest(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def _store_webui_session(token: str) -> None:
    digest = _webui_session_digest(token)
    if not digest:
        return
    redis_client.hset(WEBUI_AUTH_SESSIONS_KEY, digest, int(time.time()))


def _clear_webui_sessions() -> None:
    redis_client.delete(WEBUI_AUTH_SESSIONS_KEY)


def _webui_session_is_valid(token: Optional[str]) -> bool:
    digest = _webui_session_digest(str(token or "").strip())
    if not digest:
        return False
    return bool(redis_client.hexists(WEBUI_AUTH_SESSIONS_KEY, digest))


def _issue_webui_auth_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=WEBUI_AUTH_COOKIE_NAME,
        value=str(token or ""),
        max_age=WEBUI_AUTH_COOKIE_MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
        secure=False,
        path="/",
    )


def _clear_webui_auth_cookie(response: Response) -> None:
    response.delete_cookie(key=WEBUI_AUTH_COOKIE_NAME, path="/")


def _webui_auth_profile_payload(
    *,
    authenticated: bool,
) -> Dict[str, Any]:
    chat_settings = redis_client.hgetall("chat_settings") or {}
    password_set = _webui_password_is_set()
    mode = "ready" if bool(authenticated) or not password_set else "login"
    return {
        "password_set": bool(password_set),
        "authenticated": bool(authenticated),
        "mode": mode,
        "username": str(chat_settings.get("username") or "User"),
        "user_avatar": _read_user_avatar_data_url(chat_settings),
    }


def _normalize_tater_api_mode(value: Any, default: str = TATER_API_MODE_DIRECT) -> str:
    token = str(value or "").strip().lower()
    if token in {"hydra", "agent", "tools"}:
        return TATER_API_MODE_HYDRA
    if token in {"direct", "base", "llm", "chat"}:
        return TATER_API_MODE_DIRECT
    fallback = str(default or TATER_API_MODE_DIRECT).strip().lower()
    return fallback if fallback in TATER_API_MODE_CHOICES else TATER_API_MODE_DIRECT


def _load_tater_api_settings(*, include_secret: bool = False) -> Dict[str, Any]:
    raw = redis_client.hgetall(TATER_API_SETTINGS_KEY) or {}
    enabled_raw = raw.get("enabled")
    if enabled_raw is None:
        enabled_raw = os.getenv("TATER_OPENAI_API_ENABLED")
    key_value = str(raw.get("api_key") or os.getenv("TATER_OPENAI_API_KEY") or "").strip()
    mode = _normalize_tater_api_mode(raw.get("mode") or os.getenv("TATER_OPENAI_API_MODE"))
    hydra_tools_enabled = _as_bool_flag(
        raw.get("hydra_tools_enabled") or os.getenv("TATER_OPENAI_API_HYDRA_TOOLS_ENABLED"),
        default=False,
    )
    settings: Dict[str, Any] = {
        "enabled": _as_bool_flag(enabled_raw, default=False),
        "mode": mode,
        "hydra_tools_enabled": bool(hydra_tools_enabled),
        "api_key_set": bool(key_value),
    }
    if include_secret:
        settings["api_key"] = key_value
    return settings


def _save_tater_api_settings_from_updates(updates: Dict[str, Any]) -> None:
    keys = {
        "tater_api_enabled",
        "tater_api_key",
        "clear_tater_api_key",
        "tater_api_mode",
        "tater_api_hydra_tools_enabled",
    }
    if not any(key in updates for key in keys):
        return

    mapping: Dict[str, str] = {}
    if "tater_api_enabled" in updates:
        mapping["enabled"] = "true" if _as_bool_flag(updates.get("tater_api_enabled"), default=False) else "false"
    if "tater_api_mode" in updates:
        mapping["mode"] = _normalize_tater_api_mode(updates.get("tater_api_mode"))
    if "tater_api_hydra_tools_enabled" in updates:
        mapping["hydra_tools_enabled"] = (
            "true" if _as_bool_flag(updates.get("tater_api_hydra_tools_enabled"), default=False) else "false"
        )
    if bool(updates.get("clear_tater_api_key")):
        redis_client.hdel(TATER_API_SETTINGS_KEY, "api_key")
    elif "tater_api_key" in updates:
        key_value = str(updates.get("tater_api_key") or "").strip()
        if key_value:
            mapping["api_key"] = key_value

    if mapping:
        redis_client.hset(TATER_API_SETTINGS_KEY, mapping=mapping)


def _normalize_spud_link_mode(value: Any, default: str = SPUD_LINK_MODE_DISABLED) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "off": SPUD_LINK_MODE_DISABLED,
        "none": SPUD_LINK_MODE_DISABLED,
        "master": SPUD_LINK_MODE_HUB,
        "spud_hub": SPUD_LINK_MODE_HUB,
        "hub": SPUD_LINK_MODE_HUB,
        "mini": SPUD_LINK_MODE_SPUDLET,
        "mini_spud": SPUD_LINK_MODE_SPUDLET,
        "llm": SPUD_LINK_MODE_SPUDLET,
        "llm_only": SPUD_LINK_MODE_SPUDLET,
        "linked": SPUD_LINK_MODE_LITTLE_SPUD,
        "linked_spud": SPUD_LINK_MODE_LITTLE_SPUD,
        "little": SPUD_LINK_MODE_LITTLE_SPUD,
        "little_spud": SPUD_LINK_MODE_LITTLE_SPUD,
        "tools": SPUD_LINK_MODE_LITTLE_SPUD,
    }
    token = aliases.get(token, token)
    if token in SPUD_LINK_MODE_CHOICES:
        return token
    fallback = str(default or SPUD_LINK_MODE_DISABLED).strip().lower()
    return fallback if fallback in SPUD_LINK_MODE_CHOICES else SPUD_LINK_MODE_DISABLED


def _normalize_spud_link_tater_mode(value: Any, default: str = SPUD_LINK_MODE_DISABLED) -> str:
    mode = _normalize_spud_link_mode(value, default=default)
    if mode == SPUD_LINK_MODE_LITTLE_SPUD:
        return SPUD_LINK_MODE_SPUDLET
    if mode in SPUD_LINK_TATER_MODE_CHOICES:
        return mode
    fallback = _normalize_spud_link_mode(default, default=SPUD_LINK_MODE_DISABLED)
    return fallback if fallback in SPUD_LINK_TATER_MODE_CHOICES else SPUD_LINK_MODE_DISABLED


def _spud_link_first_header_value(value: Any) -> str:
    return str(value or "").split(",", 1)[0].strip()


def _spud_link_forwarded_header_parts(request: Request) -> Dict[str, str]:
    raw = str(request.headers.get("forwarded") or "").split(",", 1)[0].strip()
    parts: Dict[str, str] = {}
    if not raw:
        return parts
    for item in raw.split(";"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = key.strip().lower()
        value = value.strip().strip('"')
        if key and value:
            parts[key] = value
    return parts


def _spud_link_external_request_base_url(request: Optional[Request]) -> str:
    if request is None:
        return ""
    forwarded = _spud_link_forwarded_header_parts(request)
    scheme = (
        _spud_link_first_header_value(request.headers.get("x-forwarded-proto"))
        or forwarded.get("proto")
        or str(request.url.scheme or "")
    ).lower()
    if scheme not in {"http", "https"}:
        scheme = "https" if str(request.url.scheme or "").lower() == "https" else "http"
    host = (
        _spud_link_first_header_value(request.headers.get("x-forwarded-host"))
        or forwarded.get("host")
        or str(request.headers.get("host") or "")
        or str(request.url.netloc or "")
    ).strip()
    if not host:
        return ""
    port = _spud_link_first_header_value(request.headers.get("x-forwarded-port"))
    if port and ":" not in host and not ((scheme == "https" and port == "443") or (scheme == "http" and port == "80")):
        host = f"{host}:{port}"
    prefix = (
        _spud_link_first_header_value(request.headers.get("x-forwarded-prefix"))
        or _spud_link_first_header_value(request.headers.get("x-script-name"))
        or str(request.scope.get("root_path") or "")
    ).strip()
    if prefix:
        prefix = "/" + prefix.strip("/")
        if prefix == "/":
            prefix = ""
    return f"{scheme}://{host}{prefix}".rstrip("/")


def _spud_link_server_url(settings: Dict[str, Any], request: Optional[Request] = None) -> str:
    public_url = str(settings.get("public_url") or "").strip().rstrip("/")
    if public_url:
        return public_url
    request_url = _spud_link_external_request_base_url(request)
    if request_url:
        return request_url
    return ""


def _spud_link_pairing_payload(
    *,
    settings: Dict[str, Any],
    code: str,
    expires_at: float,
    request: Optional[Request] = None,
    default_role: str = SPUD_LINK_MODE_LITTLE_SPUD,
) -> Dict[str, Any]:
    role = _normalize_spud_link_mode(default_role, default=SPUD_LINK_MODE_LITTLE_SPUD)
    if role not in {SPUD_LINK_MODE_SPUDLET, SPUD_LINK_MODE_LITTLE_SPUD}:
        role = SPUD_LINK_MODE_LITTLE_SPUD
    supported_roles: List[str] = []
    if bool(settings.get("allow_little_spuds")):
        supported_roles.append(SPUD_LINK_MODE_LITTLE_SPUD)
    if bool(settings.get("allow_spudlets")):
        supported_roles.append(SPUD_LINK_MODE_SPUDLET)
    if supported_roles and role not in supported_roles:
        role = supported_roles[0]
    server_url = _spud_link_server_url(settings, request=request)
    payload = {
        "schema": "tater.spudlink.pair.v1",
        "hub_url": server_url,
        "pair_url": f"{server_url}/api/spudlink/pair" if server_url else "",
        "pairing_code": code,
        "role": role,
        "supported_roles": supported_roles,
        "expires_at": expires_at,
        "hub_name": str(settings.get("node_name") or _spud_link_local_node_name()),
        "server_mode": _normalize_spud_link_tater_mode(settings.get("mode")),
    }
    return payload


def _spud_link_pairing_uri(payload: Dict[str, Any]) -> str:
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).decode("ascii")
    return f"tater-spudlink://pair?data={encoded.rstrip('=')}"


def _spud_link_qr_svg_data_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        import qrcode  # type: ignore
        from qrcode.image.svg import SvgPathImage  # type: ignore
    except Exception:
        return ""
    try:
        image = qrcode.make(text, image_factory=SvgPathImage, border=2)
        buffer = io.BytesIO()
        image.save(buffer)
        encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
        return f"data:image/svg+xml;base64,{encoded}"
    except Exception:
        return ""


def _spud_link_token_digest(token: Any) -> str:
    text = str(token or "").strip()
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _spud_link_local_node_name() -> str:
    configured = str(redis_client.hget(SPUD_LINK_SETTINGS_KEY, "node_name") or "").strip()
    if configured:
        return configured
    try:
        node = str(os.uname().nodename or "").strip()
    except Exception:
        node = ""
    return node or "Tater"


def _load_spud_link_settings(*, include_secret: bool = False) -> Dict[str, Any]:
    raw = redis_client.hgetall(SPUD_LINK_SETTINGS_KEY) or {}
    mode = _normalize_spud_link_tater_mode(raw.get("mode"))
    token_value = str(raw.get("node_token") or "").strip()
    pairing_expires_at = 0.0
    try:
        pairing_expires_at = float(raw.get("pairing_expires_at") or 0)
    except Exception:
        pairing_expires_at = 0.0
    hub_connected_at = 0.0
    try:
        hub_connected_at = float(raw.get("hub_connected_at") or 0)
    except Exception:
        hub_connected_at = 0.0
    settings: Dict[str, Any] = {
        "mode": mode,
        "enabled": mode != SPUD_LINK_MODE_DISABLED,
        "node_name": str(raw.get("node_name") or _spud_link_local_node_name()).strip() or "Tater",
        "public_url": str(raw.get("public_url") or "").strip(),
        "pairing_enabled": _as_bool_flag(raw.get("pairing_enabled"), default=False),
        "allow_spudlets": _as_bool_flag(raw.get("allow_spudlets"), default=True),
        "allow_little_spuds": _as_bool_flag(raw.get("allow_little_spuds"), default=True),
        "little_spud_tools_enabled": _as_bool_flag(raw.get("little_spud_tools_enabled"), default=True),
        "telemetry_enabled": _as_bool_flag(raw.get("telemetry_enabled"), default=True),
        "request_previews_enabled": _as_bool_flag(raw.get("request_previews_enabled"), default=False),
        "hub_url": str(raw.get("hub_url") or "").strip(),
        "hub_name": str(raw.get("hub_name") or "").strip(),
        "hub_mode": _normalize_spud_link_tater_mode(raw.get("hub_mode"), default=SPUD_LINK_MODE_HUB),
        "hub_connected_at": hub_connected_at,
        "node_token_set": bool(token_value),
        "pairing_code_active": bool(str(raw.get("pairing_code_hash") or "").strip() and pairing_expires_at > time.time()),
        "pairing_expires_at": pairing_expires_at if pairing_expires_at > time.time() else 0.0,
    }
    if include_secret:
        settings["node_token"] = token_value
        settings["pairing_code_hash"] = str(raw.get("pairing_code_hash") or "").strip()
    return settings


def _save_spud_link_settings_from_updates(updates: Dict[str, Any]) -> None:
    keys = {
        "spud_link_mode",
        "spud_link_node_name",
        "spud_link_public_url",
        "spud_link_pairing_enabled",
        "spud_link_allow_spudlets",
        "spud_link_allow_little_spuds",
        "spud_link_little_spud_tools_enabled",
        "spud_link_telemetry_enabled",
        "spud_link_request_previews_enabled",
        "spud_link_hub_url",
        "spud_link_node_token",
        "clear_spud_link_node_token",
    }
    if not any(key in updates for key in keys):
        return
    mapping: Dict[str, str] = {}
    if "spud_link_mode" in updates:
        mapping["mode"] = _normalize_spud_link_tater_mode(updates.get("spud_link_mode"))
    if "spud_link_node_name" in updates:
        mapping["node_name"] = str(updates.get("spud_link_node_name") or "").strip()[:120]
    if "spud_link_public_url" in updates:
        mapping["public_url"] = str(updates.get("spud_link_public_url") or "").strip()[:500]
    if "spud_link_pairing_enabled" in updates:
        mapping["pairing_enabled"] = (
            "true" if _as_bool_flag(updates.get("spud_link_pairing_enabled"), default=False) else "false"
        )
    if "spud_link_allow_spudlets" in updates:
        mapping["allow_spudlets"] = (
            "true" if _as_bool_flag(updates.get("spud_link_allow_spudlets"), default=True) else "false"
        )
    if "spud_link_allow_little_spuds" in updates:
        mapping["allow_little_spuds"] = (
            "true" if _as_bool_flag(updates.get("spud_link_allow_little_spuds"), default=True) else "false"
        )
    if "spud_link_little_spud_tools_enabled" in updates:
        mapping["little_spud_tools_enabled"] = (
            "true" if _as_bool_flag(updates.get("spud_link_little_spud_tools_enabled"), default=True) else "false"
        )
    if "spud_link_telemetry_enabled" in updates:
        mapping["telemetry_enabled"] = (
            "true" if _as_bool_flag(updates.get("spud_link_telemetry_enabled"), default=True) else "false"
        )
    if "spud_link_request_previews_enabled" in updates:
        mapping["request_previews_enabled"] = (
            "true" if _as_bool_flag(updates.get("spud_link_request_previews_enabled"), default=False) else "false"
        )
    if "spud_link_hub_url" in updates:
        mapping["hub_url"] = str(updates.get("spud_link_hub_url") or "").strip()[:500]
    if bool(updates.get("clear_spud_link_node_token")):
        redis_client.hdel(SPUD_LINK_SETTINGS_KEY, "node_token")
    elif "spud_link_node_token" in updates:
        node_token = str(updates.get("spud_link_node_token") or "").strip()
        if node_token:
            mapping["node_token"] = node_token
    if mapping:
        redis_client.hset(SPUD_LINK_SETTINGS_KEY, mapping=mapping)


def _spud_link_load_nodes() -> List[Dict[str, Any]]:
    raw = redis_client.hgetall(SPUD_LINK_NODES_KEY) or {}
    nodes: List[Dict[str, Any]] = []
    for node_id, raw_value in raw.items():
        try:
            row = json.loads(str(raw_value or "{}"))
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        row["id"] = str(row.get("id") or node_id or "").strip()
        row["role"] = _normalize_spud_link_mode(row.get("role"), default=SPUD_LINK_MODE_SPUDLET)
        row["token_set"] = bool(str(row.get("token_hash") or "").strip())
        row.pop("token_hash", None)
        nodes.append(row)
    nodes.sort(key=lambda row: (-float(row.get("last_seen_at") or row.get("created_at") or 0), str(row.get("name") or row.get("id") or "")))
    return nodes


def _spud_link_store_node(node: Dict[str, Any]) -> Dict[str, Any]:
    node_id = str(node.get("id") or "").strip()
    if not node_id:
        node_id = f"spud_{uuid.uuid4().hex[:16]}"
        node["id"] = node_id
    redis_client.hset(SPUD_LINK_NODES_KEY, node_id, json.dumps(node, separators=(",", ":"), ensure_ascii=False))
    redacted = dict(node)
    redacted["token_set"] = bool(str(redacted.get("token_hash") or "").strip())
    redacted.pop("token_hash", None)
    return redacted


def _spud_link_request_network_info(request: Request) -> Dict[str, str]:
    forwarded_for = str(request.headers.get("x-forwarded-for") or "").split(",", 1)[0].strip()
    forwarded_host = str(request.headers.get("x-real-ip") or "").strip()
    client_host = str(getattr(request.client, "host", "") or "").strip()
    remote_addr = forwarded_for or forwarded_host or client_host
    user_agent = str(request.headers.get("user-agent") or "").strip()
    return {
        "remote_addr": remote_addr[:120],
        "user_agent": user_agent[:260],
    }


def _spud_link_touch_node_from_request(node: Dict[str, Any], request: Request) -> None:
    now = time.time()
    network_info = _spud_link_request_network_info(request)
    remote_addr = network_info.get("remote_addr") or ""
    user_agent = network_info.get("user_agent") or ""
    node["last_seen_at"] = now
    if remote_addr:
        node["last_remote_addr"] = remote_addr
        node.setdefault("first_remote_addr", remote_addr)
    if user_agent:
        node["last_user_agent"] = user_agent


def _spud_link_sanitize_activity(value: Any, *, allow_previews: bool = False) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    sensitive_markers = {
        "prompt",
        "message",
        "messages",
        "content",
        "text",
        "preview",
        "request",
        "response",
        "raw",
        "tool_result",
        "user_message",
    }
    clean: Dict[str, Any] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        key_lower = key.lower()
        if not allow_previews and any(marker in key_lower for marker in sensitive_markers):
            clean[key] = "[redacted]"
            continue
        if raw_value is None or isinstance(raw_value, (bool, int, float, str)):
            clean[key] = raw_value
        elif allow_previews:
            clean[key] = raw_value
        else:
            clean[key] = str(raw_value)[:240]
    return clean


def _spud_link_identity_text(value: Any, *, default: str, limit: int = 80) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[\x00-\x1f\x7f]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        text = default
    return text[: max(1, int(limit or 80))].strip() or default


def _spud_link_history_key(identity: Dict[str, str]) -> str:
    raw = f"{identity.get('user_name') or 'User'}:{identity.get('device_name') or 'Little Spud'}"
    token = re.sub(r"[^a-zA-Z0-9_.-]+", "_", raw).strip("_")[:120]
    if not token:
        token = hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"tater:little_spud:{token}:history"


def _spud_link_active_run_key(identity: Dict[str, str]) -> str:
    history_key = str(identity.get("history_key") or "").strip()
    if history_key:
        return f"{history_key}:active_runs"
    return f"{_spud_link_history_key(identity)}:active_runs"


def _spud_link_identity_from_request(
    payload: Any,
    request: Request,
    node: Dict[str, Any],
) -> Dict[str, str]:
    metadata = payload.metadata if isinstance(getattr(payload, "metadata", None), dict) else {}
    user_name = _spud_link_identity_text(
        getattr(payload, "user_name", None)
        or metadata.get("user_name")
        or metadata.get("username")
        or request.headers.get("x-spudlink-user")
        or request.headers.get("x-little-spud-user")
        or getattr(payload, "user", None),
        default="User",
    )
    device_name = _spud_link_identity_text(
        getattr(payload, "device_name", None)
        or metadata.get("device_name")
        or metadata.get("device")
        or request.headers.get("x-spudlink-device")
        or request.headers.get("x-little-spud-device")
        or node.get("name"),
        default="Little Spud",
    )
    alias_id = f"{user_name}:{device_name}"
    display_name = f"{user_name} on {device_name}"
    return {
        "user_name": user_name,
        "device_name": device_name,
        "alias_id": alias_id,
        "display_name": display_name,
        "scope": f"user:{alias_id}",
        "history_key": _spud_link_history_key({"user_name": user_name, "device_name": device_name}),
    }


def _save_little_spud_history(
    identity: Dict[str, str],
    *,
    role: str,
    content: Any,
    attachments: Optional[List[Dict[str, Any]]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    key = str(identity.get("history_key") or "").strip()
    if not key:
        return
    payload = {
        "id": f"lsmsg_{uuid.uuid4().hex[:16]}",
        "role": str(role or "assistant"),
        "platform": "little_spud",
        "username": str(identity.get("display_name") or identity.get("user_name") or "User"),
        "user": str(identity.get("user_name") or "User"),
        "user_id": str(identity.get("alias_id") or identity.get("user_name") or "User"),
        "device_name": str(identity.get("device_name") or "Little Spud"),
        "device_id": str(identity.get("device_name") or "Little Spud"),
        "content": content,
        "ts": time.time(),
    }
    if isinstance(attachments, list) and attachments:
        payload["attachments"] = [dict(item) for item in attachments if isinstance(item, dict)]
    if isinstance(meta, dict) and meta:
        payload["meta"] = dict(meta)
    redis_client.rpush(key, json.dumps(payload, ensure_ascii=False))
    max_store = _read_non_negative_int("tater:max_store", DEFAULT_MAX_STORE)
    if max_store > 0:
        redis_client.ltrim(key, -max_store, -1)


def _load_little_spud_history(identity: Dict[str, str], *, limit: int = 80) -> List[Dict[str, Any]]:
    key = str(identity.get("history_key") or "").strip()
    if not key:
        return []
    count = max(1, min(int(limit or 80), 200))
    rows: List[Dict[str, Any]] = []
    try:
        raw_items = redis_client.lrange(key, -count, -1) or []
    except Exception:
        return []
    for raw in raw_items:
        try:
            item = json.loads(raw)
        except Exception:
            continue
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        if role not in {"user", "assistant", "system"}:
            continue
        content = item.get("content")
        if not isinstance(content, str):
            content = json.dumps(content, ensure_ascii=False) if content is not None else ""
        content = str(content or "").strip()
        if not content:
            continue
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        attachments = item.get("attachments") if isinstance(item.get("attachments"), list) else []
        row = {
            "id": str(item.get("id") or f"lsmsg_{hashlib.sha256((role + content).encode('utf-8', errors='ignore')).hexdigest()[:16]}"),
            "role": role,
            "content": content,
            "createdAt": int(float(item.get("ts") or time.time()) * 1000),
            "meta": {
                **meta,
                "source": "spud_hub_history",
                "device_name": str(item.get("device_name") or identity.get("device_name") or "Little Spud"),
            },
        }
        if attachments:
            row["attachments"] = [dict(artifact) for artifact in attachments if isinstance(artifact, dict)]
        rows.append(
            row
        )
    return rows


def _save_little_spud_active_run(identity: Dict[str, str], payload: Dict[str, Any]) -> None:
    key = _spud_link_active_run_key(identity)
    if not key:
        return
    row = dict(payload or {})
    run_id = str(row.get("run_id") or "").strip() or f"lsrun_{uuid.uuid4().hex[:16]}"
    row["run_id"] = run_id
    row.setdefault("status", "running")
    row.setdefault("text", "Tater is thinking")
    row.setdefault("started_at", time.time())
    row["updated_at"] = time.time()
    row["identity"] = {
        "user_name": str(identity.get("user_name") or "User"),
        "device_name": str(identity.get("device_name") or "Little Spud"),
        "scope": str(identity.get("scope") or ""),
    }
    try:
        redis_client.hset(key, run_id, json.dumps(row, separators=(",", ":"), ensure_ascii=False))
        redis_client.expire(key, SPUD_LINK_ACTIVE_RUN_TTL_SECONDS)
    except Exception:
        logger.exception("[spudlink] failed saving Little Spud active run")


def _load_little_spud_active_runs(identity: Dict[str, str]) -> List[Dict[str, Any]]:
    key = _spud_link_active_run_key(identity)
    if not key:
        return []
    try:
        raw_items = redis_client.hgetall(key) or {}
    except Exception:
        return []
    rows: List[Dict[str, Any]] = []
    for run_id, raw in raw_items.items():
        try:
            row = json.loads(raw)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "").strip().lower()
        if status not in {"queued", "running"}:
            continue
        row["run_id"] = str(row.get("run_id") or run_id or "").strip()
        rows.append(row)
    rows.sort(key=lambda item: float(item.get("updated_at") or item.get("started_at") or 0), reverse=True)
    return rows


def _load_little_spud_active_run(identity: Dict[str, str]) -> Optional[Dict[str, Any]]:
    rows = _load_little_spud_active_runs(identity)
    return rows[0] if rows else None


def _clear_little_spud_active_run(identity: Dict[str, str], run_id: str = "") -> None:
    key = _spud_link_active_run_key(identity)
    if not key:
        return
    try:
        run_id = str(run_id or "").strip()
        if run_id:
            redis_client.hdel(key, run_id)
        else:
            redis_client.delete(key)
    except Exception:
        pass


def _spud_link_public_settings_payload() -> Dict[str, Any]:
    settings = _load_spud_link_settings(include_secret=False)
    server_mode = _normalize_spud_link_tater_mode(settings.get("mode"))
    is_server = server_mode in SPUD_LINK_SERVER_MODE_CHOICES
    little_spud_tools_enabled = is_server and bool(settings.get("little_spud_tools_enabled"))
    hub_url = str(settings.get("hub_url") or "").strip()
    node_token_set = bool(settings.get("node_token_set"))
    paired_hub = {
        "connected": bool(server_mode == SPUD_LINK_MODE_SPUDLET and hub_url and node_token_set),
        "hub_url": hub_url,
        "hub_name": str(settings.get("hub_name") or "").strip(),
        "hub_mode": _normalize_spud_link_tater_mode(settings.get("hub_mode"), default=SPUD_LINK_MODE_HUB),
        "connected_at": float(settings.get("hub_connected_at") or 0),
        "node_token_set": node_token_set,
        "node_name": str(settings.get("node_name") or _spud_link_local_node_name()).strip() or "Tater",
        "public_url": str(settings.get("public_url") or "").strip(),
        "model_provider": "spud_link" if server_mode == SPUD_LINK_MODE_SPUDLET else "",
        "model": "tater/base" if server_mode == SPUD_LINK_MODE_SPUDLET else "",
    }
    return {
        **settings,
        "roles": {
            "disabled": "Disabled",
            "hub": "Spud Hub",
            "spudlet": "Spudlet",
        },
        "client_roles": {
            "little_spud": "Little Spud",
            "spudlet": "Spudlet",
        },
        "local_status": {
            "node_name": settings.get("node_name") or _spud_link_local_node_name(),
            "mode": server_mode,
            "time": time.time(),
        },
        "capabilities": {
            "server": is_server,
            "llm": is_server,
            "hydra": is_server,
            "tools": little_spud_tools_enabled,
            "little_spud_clients": is_server and bool(settings.get("allow_little_spuds")),
            "spudlet_clients": is_server and bool(settings.get("allow_spudlets")),
        },
        "paired_hub": paired_hub,
        "linked_nodes": _spud_link_load_nodes(),
    }


def _extract_tater_api_token(request: Request) -> str:
    auth = str(request.headers.get("authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    api_key = str(request.headers.get("x-api-key") or "").strip()
    if api_key:
        return api_key
    return str(request.query_params.get("api_key") or "").strip()


def _require_tater_api_request(request: Request) -> Dict[str, Any]:
    settings = _load_tater_api_settings(include_secret=True)
    if not bool(settings.get("enabled")):
        raise HTTPException(status_code=404, detail="Tater OpenAI-compatible API is disabled.")
    expected = str(settings.get("api_key") or "").strip()
    if not expected:
        raise HTTPException(status_code=403, detail="Tater OpenAI-compatible API key is not configured.")
    supplied = _extract_tater_api_token(request)
    if not supplied or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid Tater API key.")
    return settings


def _openai_content_to_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, dict):
                item_type = str(item.get("type") or "").strip().lower()
                if item_type in {"text", "input_text"}:
                    parts.append(str(item.get("text") or ""))
                elif "text" in item and not item_type:
                    parts.append(str(item.get("text") or ""))
            elif item is not None:
                parts.append(str(item))
        return "\n".join(part for part in parts if part).strip()
    return str(content)


def _normalize_openai_chat_messages(messages: Any) -> List[Dict[str, str]]:
    if not isinstance(messages, list):
        return []
    normalized: List[Dict[str, str]] = []
    for raw_message in messages:
        if not isinstance(raw_message, dict):
            continue
        role = str(raw_message.get("role") or "user").strip().lower()
        if role not in {"system", "user", "assistant", "tool"}:
            role = "user"
        content = _openai_content_to_text(raw_message.get("content")).strip()
        if not content:
            continue
        normalized.append({"role": role, "content": content})
    return normalized


def _openai_user_text_and_history(messages: List[Dict[str, str]]) -> Tuple[str, List[Dict[str, str]]]:
    if not messages:
        raise HTTPException(status_code=400, detail="messages must include at least one text message.")
    last_user_idx = -1
    for idx in range(len(messages) - 1, -1, -1):
        if messages[idx].get("role") == "user":
            last_user_idx = idx
            break
    if last_user_idx < 0:
        last_user_idx = len(messages) - 1
    user_text = str(messages[last_user_idx].get("content") or "").strip()
    if not user_text:
        raise HTTPException(status_code=400, detail="The latest user message is empty.")
    max_llm = _read_positive_int("tater:max_llm", DEFAULT_MAX_LLM)
    history = [dict(item) for item in messages[:last_user_idx]]
    if max_llm > 0:
        history = history[-max_llm:]
    return user_text, history


def _tater_api_primary_model_id() -> str:
    try:
        rows = resolve_hydra_base_servers(redis_conn=redis_client, include_legacy=True)
    except Exception:
        rows = []
    row = rows[0] if rows else {}
    provider = _normalize_hydra_llm_provider(row.get("provider") if isinstance(row, dict) else "")
    model = str((row or {}).get("model") or "").strip() if isinstance(row, dict) else ""
    if model and provider and provider != HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE:
        return f"{provider}::{model}"
    return model or "tater/base"


def _tater_api_mode_for_model(model: Any, default_mode: str) -> str:
    token = str(model or "").strip().lower()
    if token in {"tater/hydra", "tater-hydra", "hydra", "agent"}:
        return TATER_API_MODE_HYDRA
    if token in {"tater/base", "tater/direct", "tater-direct", "base", "direct"}:
        return TATER_API_MODE_DIRECT
    return _normalize_tater_api_mode(default_mode)


def _openai_usage_from_perf(perf: Any) -> Dict[str, int]:
    def _safe_token_count(value: Any) -> int:
        try:
            return max(0, int(float(value or 0)))
        except Exception:
            return 0

    if not isinstance(perf, dict):
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    prompt_tokens = _safe_token_count(perf.get("prompt_tokens"))
    completion_tokens = _safe_token_count(perf.get("completion_tokens"))
    total_tokens = _safe_token_count(perf.get("total_tokens"))
    if total_tokens <= 0:
        total_tokens = prompt_tokens + completion_tokens
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
    }


def _openai_chat_completion_response(
    *,
    content: str,
    model: str,
    usage: Optional[Dict[str, int]] = None,
    completion_id: str = "",
    spud_link: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = {
        "id": completion_id or f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": str(model or _tater_api_primary_model_id()),
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": str(content or "")},
                "finish_reason": "stop",
            }
        ],
        "usage": usage or {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }
    if isinstance(spud_link, dict) and spud_link:
        payload["spud_link"] = spud_link
    return payload


async def _run_tater_api_direct_completion(
    payload: Any,
    messages: List[Dict[str, str]],
) -> Dict[str, Any]:
    generation_kwargs: Dict[str, Any] = {"activity": "external_api"}
    if payload.max_tokens is not None:
        try:
            generation_kwargs["max_tokens"] = max(1, int(payload.max_tokens))
        except Exception:
            raise HTTPException(status_code=400, detail="max_tokens must be a positive integer.")
    if payload.temperature is not None:
        generation_kwargs["temperature"] = payload.temperature
    if payload.top_p is not None:
        generation_kwargs["top_p"] = payload.top_p
    if payload.stop is not None:
        generation_kwargs["stop"] = payload.stop

    perf: Dict[str, Any] = {}
    async with get_llm_client_from_env(redis_conn=redis_client) as llm_client:
        result = await llm_client.chat(messages, stream=False, **generation_kwargs)
        try:
            perf = llm_client.get_perf_stats(reset=False) if hasattr(llm_client, "get_perf_stats") else {}
            if isinstance(perf, dict):
                perf["updated_at"] = time.time()
                _save_last_llm_stats(perf)
        except Exception:
            logger.exception("[tater-api] failed saving LLM speed stats")

    response_message = result.get("message") if isinstance(result, dict) else {}
    content = ""
    if isinstance(response_message, dict):
        content = str(response_message.get("content") or "")
    model = str((result or {}).get("model") or _tater_api_primary_model_id()) if isinstance(result, dict) else _tater_api_primary_model_id()
    return _openai_chat_completion_response(
        content=content,
        model=model,
        usage=_openai_usage_from_perf(perf),
    )


async def _run_tater_api_hydra_completion(
    payload: Any,
    messages: List[Dict[str, str]],
    *,
    tools_enabled: bool,
    request: Request,
    platform: str = "openai_api",
    origin_override: Optional[Dict[str, Any]] = None,
    scope_override: Optional[str] = None,
    platform_preamble: str = "External OpenAI-compatible API request.",
    context_extra: Optional[Dict[str, Any]] = None,
    wait_callback: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    user_text, history_messages = _openai_user_text_and_history(messages)
    session_id = str(request.headers.get("x-tater-session") or payload.user or "default").strip() or "default"
    platform_token = str(platform or "openai_api").strip() or "openai_api"
    if isinstance(origin_override, dict):
        origin = dict(origin_override)
        origin.setdefault("platform", platform_token)
        origin.setdefault("session_id", session_id)
    else:
        origin = {
            "platform": platform_token,
            "user": str(payload.user or "api").strip() or "api",
            "user_id": str(payload.user or "api").strip() or "api",
            "session_id": session_id,
        }
    context_payload = {
        "raw_message": user_text,
        "openai_api": platform_token == "openai_api",
        "spud_link": platform_token == "little_spud",
        "tools_enabled": bool(tools_enabled),
    }
    if isinstance(context_extra, dict):
        context_payload.update(context_extra)

    verba_registry_module.ensure_verbas_loaded()
    registry = dict(verba_registry_module.get_verba_registry() or {}) if tools_enabled else {}

    perf: Dict[str, Any] = {}
    async with get_llm_client_from_env(redis_conn=redis_client) as llm_client:
        result = await run_hydra_turn(
            llm_client=llm_client,
            platform=platform_token,
            history_messages=history_messages,
            registry=registry,
            enabled_predicate=(get_verba_enabled if tools_enabled else None),
            context=context_payload,
            user_text=user_text,
            scope=scope_override if scope_override is not None else f"session:{session_id}",
            origin=origin,
            redis_client=redis_client,
            platform_preamble=platform_preamble,
            wait_callback=wait_callback,
        )
        try:
            perf = llm_client.get_perf_stats(reset=False) if hasattr(llm_client, "get_perf_stats") else {}
            if isinstance(perf, dict):
                perf["updated_at"] = time.time()
                _save_last_llm_stats(perf)
        except Exception:
            logger.exception("[tater-api] failed saving Hydra LLM speed stats")

    content = ""
    spud_link_payload: Dict[str, Any] = {}
    if isinstance(result, dict):
        text = result.get("text")
        if isinstance(text, str):
            content = text
        artifacts = result.get("artifacts") if isinstance(result.get("artifacts"), list) else []
        response_artifacts: List[Dict[str, Any]] = []
        for item in artifacts:
            normalized_item = _normalize_plugin_response_item(item)
            if not isinstance(normalized_item, dict):
                continue
            media_type = str(normalized_item.get("type") or "").strip().lower()
            mimetype_value = str(normalized_item.get("mimetype") or normalized_item.get("mime_type") or "").strip()
            file_id = str(normalized_item.get("id") or normalized_item.get("file_id") or "").strip()
            response_item = {
                "type": media_type,
                "name": str(normalized_item.get("name") or normalized_item.get("filename") or "attachment").strip() or "attachment",
                "mimetype": mimetype_value,
                "size": normalized_item.get("size"),
            }
            if file_id:
                response_item["id"] = file_id
                response_item["url"] = f"/api/spudlink/v1/files/{file_id}?mimetype={quote(mimetype_value or 'application/octet-stream')}"
            for key in ("url", "previewUrl", "dataUrl"):
                value = str(normalized_item.get(key) or "").strip()
                if value and key not in response_item:
                    response_item[key] = value
            response_artifacts.append(response_item)
        if response_artifacts:
            spud_link_payload["artifacts"] = response_artifacts
            spud_link_payload["artifact_count"] = len(response_artifacts)
        if not content.strip():
            content_parts: List[str] = []
            for item in artifacts:
                if isinstance(item, str):
                    content_parts.append(item)
                elif isinstance(item, dict):
                    summary = item.get("summary") or item.get("text") or item.get("message")
                    if summary:
                        content_parts.append(str(summary))
            content = "\n".join(part for part in content_parts if part).strip()

    return _openai_chat_completion_response(
        content=content,
        model="tater/hydra",
        usage=_openai_usage_from_perf(perf),
        spud_link=spud_link_payload,
    )


async def _run_spud_link_native_llm_completion(
    payload: Any,
    messages: List[Dict[str, str]],
) -> Dict[str, Any]:
    generation_kwargs: Dict[str, Any] = {"activity": "spud_link_model"}
    if payload.max_tokens is not None:
        try:
            generation_kwargs["max_tokens"] = max(1, int(payload.max_tokens))
        except Exception:
            raise HTTPException(status_code=400, detail="max_tokens must be a positive integer.")
    if payload.temperature is not None:
        generation_kwargs["temperature"] = payload.temperature
    if payload.top_p is not None:
        generation_kwargs["top_p"] = payload.top_p
    if payload.stop is not None:
        generation_kwargs["stop"] = payload.stop

    perf: Dict[str, Any] = {}
    async with get_llm_client_from_env(redis_conn=redis_client) as llm_client:
        result = await llm_client.chat(messages, stream=False, **generation_kwargs)
        try:
            perf = llm_client.get_perf_stats(reset=False) if hasattr(llm_client, "get_perf_stats") else {}
            if isinstance(perf, dict):
                perf["updated_at"] = time.time()
                _save_last_llm_stats(perf)
        except Exception:
            logger.exception("[spudlink] failed saving native model LLM speed stats")

    response_message = result.get("message") if isinstance(result, dict) else {}
    content = ""
    role = "assistant"
    if isinstance(response_message, dict):
        content = str(response_message.get("content") or "")
        role = str(response_message.get("role") or "assistant").strip() or "assistant"
    model = str((result or {}).get("model") or _tater_api_primary_model_id()) if isinstance(result, dict) else _tater_api_primary_model_id()
    return {
        "ok": True,
        "model": model,
        "message": {"role": role, "content": content},
        "usage": _openai_usage_from_perf(perf),
        "stats": perf if isinstance(perf, dict) else {},
    }


def _spud_link_hydra_native_payload_from_result(result: Any, perf: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    content = ""
    artifacts = result.get("artifacts") if isinstance(result, dict) and isinstance(result.get("artifacts"), list) else []
    if isinstance(result, dict):
        for key in ("text", "answer", "message", "content", "response", "output"):
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                content = value.strip()
                break
        if not content:
            content_parts: List[str] = []
            for item in artifacts:
                if isinstance(item, str):
                    content_parts.append(item)
                elif isinstance(item, dict):
                    summary = item.get("summary_for_user") or item.get("summary") or item.get("text") or item.get("message")
                    if summary:
                        content_parts.append(str(summary))
            content = "\n".join(part for part in content_parts if part).strip()

    response_artifacts: List[Dict[str, Any]] = []
    for item in artifacts:
        normalized_item = _normalize_plugin_response_item(item)
        if not isinstance(normalized_item, dict):
            continue
        media_type = str(normalized_item.get("type") or "").strip().lower()
        mimetype_value = str(normalized_item.get("mimetype") or normalized_item.get("mime_type") or "").strip()
        file_id = str(normalized_item.get("id") or normalized_item.get("file_id") or "").strip()
        response_item = {
            "type": media_type,
            "name": str(normalized_item.get("name") or normalized_item.get("filename") or "attachment").strip() or "attachment",
            "mimetype": mimetype_value,
            "size": normalized_item.get("size"),
        }
        if file_id:
            response_item["id"] = file_id
            response_item["url"] = f"/api/spudlink/v1/files/{file_id}?mimetype={quote(mimetype_value or 'application/octet-stream')}"
        for key in ("url", "previewUrl", "dataUrl"):
            value = str(normalized_item.get(key) or "").strip()
            if value and key not in response_item:
                response_item[key] = value
        response_artifacts.append(response_item)

    return {
        "content": content,
        "artifacts": response_artifacts,
        "artifact_count": len(response_artifacts),
        "usage": _openai_usage_from_perf(perf),
    }


async def _run_spud_link_native_hydra_completion(
    payload: Any,
    messages: List[Dict[str, str]],
    *,
    tools_enabled: bool,
    request: Request,
    platform: str = "little_spud",
    origin_override: Optional[Dict[str, Any]] = None,
    scope_override: Optional[str] = None,
    platform_preamble: str = "Little Spud native Tater client request.",
    context_extra: Optional[Dict[str, Any]] = None,
    wait_callback: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    user_text, history_messages = _openai_user_text_and_history(messages)
    session_id = str(request.headers.get("x-tater-session") or getattr(payload, "user", None) or "default").strip() or "default"
    platform_token = str(platform or "little_spud").strip() or "little_spud"
    if isinstance(origin_override, dict):
        origin = dict(origin_override)
        origin.setdefault("platform", platform_token)
        origin.setdefault("session_id", session_id)
    else:
        origin = {
            "platform": platform_token,
            "user": str(getattr(payload, "user", None) or "little_spud").strip() or "little_spud",
            "user_id": str(getattr(payload, "user", None) or "little_spud").strip() or "little_spud",
            "session_id": session_id,
        }
    context_payload = {
        "raw_message": user_text,
        "spud_link": True,
        "native_spud_link": True,
        "tools_enabled": bool(tools_enabled),
    }
    if isinstance(context_extra, dict):
        context_payload.update(context_extra)

    verba_registry_module.ensure_verbas_loaded()
    registry = dict(verba_registry_module.get_verba_registry() or {}) if tools_enabled else {}

    perf: Dict[str, Any] = {}
    async with get_llm_client_from_env(redis_conn=redis_client) as llm_client:
        result = await run_hydra_turn(
            llm_client=llm_client,
            platform=platform_token,
            history_messages=history_messages,
            registry=registry,
            enabled_predicate=(get_verba_enabled if tools_enabled else None),
            context=context_payload,
            user_text=user_text,
            scope=scope_override if scope_override is not None else f"session:{session_id}",
            origin=origin,
            redis_client=redis_client,
            platform_preamble=platform_preamble,
            wait_callback=wait_callback,
        )
        try:
            perf = llm_client.get_perf_stats(reset=False) if hasattr(llm_client, "get_perf_stats") else {}
            if isinstance(perf, dict):
                perf["updated_at"] = time.time()
                _save_last_llm_stats(perf)
        except Exception:
            logger.exception("[spudlink] failed saving native Hydra LLM speed stats")

    native_payload = _spud_link_hydra_native_payload_from_result(result, perf)
    if not str(native_payload.get("content") or "").strip() and not native_payload.get("artifacts"):
        logger.warning(
            "[spudlink] native Hydra returned empty payload status=%s keys=%s",
            str(result.get("status") or "") if isinstance(result, dict) else type(result).__name__,
            sorted(result.keys()) if isinstance(result, dict) else [],
        )
    return native_payload


async def _run_tater_api_chat_completion(
    payload: Any,
    *,
    settings: Dict[str, Any],
    request: Request,
) -> Dict[str, Any]:
    messages = _normalize_openai_chat_messages(payload.messages)
    if not messages:
        raise HTTPException(status_code=400, detail="messages must include at least one text message.")
    mode = _tater_api_mode_for_model(payload.model, str(settings.get("mode") or TATER_API_MODE_DIRECT))
    if mode == TATER_API_MODE_HYDRA:
        return await _run_tater_api_hydra_completion(
            payload,
            messages,
            tools_enabled=bool(settings.get("hydra_tools_enabled")),
            request=request,
        )
    return await _run_tater_api_direct_completion(payload, messages)


async def _stream_openai_chat_completion(completion: Dict[str, Any], *, include_done: bool = True):
    completion_id = str(completion.get("id") or f"chatcmpl-{uuid.uuid4().hex}")
    created = int(completion.get("created") or time.time())
    model = str(completion.get("model") or "tater/base")
    choices = completion.get("choices") if isinstance(completion.get("choices"), list) else []
    message = choices[0].get("message") if choices and isinstance(choices[0], dict) else {}
    content = str((message or {}).get("content") or "")
    first = {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
    }
    yield f"data: {json.dumps(first, separators=(',', ':'))}\n\n"
    if content:
        chunk = {
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}],
        }
        yield f"data: {json.dumps(chunk, separators=(',', ':'))}\n\n"
    final = {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    }
    yield f"data: {json.dumps(final, separators=(',', ':'))}\n\n"
    if include_done:
        yield "data: [DONE]\n\n"


def _extract_spud_link_token(request: Request) -> str:
    auth = str(request.headers.get("authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return str(request.headers.get("x-spudlink-token") or request.query_params.get("token") or "").strip()


def _find_spud_link_node_by_token(token: str) -> Dict[str, Any]:
    digest = _spud_link_token_digest(token)
    if not digest:
        raise HTTPException(status_code=401, detail="Missing Spud Link token.")
    raw = redis_client.hgetall(SPUD_LINK_NODES_KEY) or {}
    for node_id, raw_value in raw.items():
        try:
            row = json.loads(str(raw_value or "{}"))
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        if hmac.compare_digest(str(row.get("token_hash") or ""), digest):
            row["id"] = str(row.get("id") or node_id or "").strip()
            row["role"] = _normalize_spud_link_mode(row.get("role"), default=SPUD_LINK_MODE_SPUDLET)
            return row
    raise HTTPException(status_code=401, detail="Invalid Spud Link token.")


def _require_spud_link_hub_enabled() -> Dict[str, Any]:
    settings = _load_spud_link_settings(include_secret=True)
    if _normalize_spud_link_mode(settings.get("mode")) != SPUD_LINK_MODE_HUB:
        raise HTTPException(status_code=404, detail="Spud Hub is not enabled.")
    return settings


def _require_spud_link_server_enabled() -> Dict[str, Any]:
    settings = _load_spud_link_settings(include_secret=True)
    if _normalize_spud_link_tater_mode(settings.get("mode")) not in SPUD_LINK_SERVER_MODE_CHOICES:
        raise HTTPException(status_code=404, detail="Spud Link server is not enabled.")
    return settings


def _require_spud_link_node_request(request: Request) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    settings = _require_spud_link_server_enabled()
    node = _find_spud_link_node_by_token(_extract_spud_link_token(request))
    return settings, node


def _spud_link_node_response(node: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(node)
    row["role"] = _normalize_spud_link_mode(row.get("role"), default=SPUD_LINK_MODE_SPUDLET)
    row["token_set"] = bool(str(row.get("token_hash") or "").strip())
    row.pop("token_hash", None)
    return row


def _spud_link_mode_label(mode: Any) -> str:
    token = _normalize_spud_link_mode(mode)
    if token == SPUD_LINK_MODE_HUB:
        return "Spud Hub"
    if token == SPUD_LINK_MODE_LITTLE_SPUD:
        return "Little Spud"
    if token == SPUD_LINK_MODE_SPUDLET:
        return "Spudlet"
    return "Disabled"


def _save_chat_message(role: str, username: str, content: Any) -> None:
    payload = {
        "role": str(role or "assistant"),
        "username": str(username or "User"),
        "content": content,
    }
    redis_client.rpush(CHAT_HISTORY_KEY, json.dumps(payload))

    max_store = _read_non_negative_int("tater:max_store", DEFAULT_MAX_STORE)
    if max_store > 0:
        redis_client.ltrim(CHAT_HISTORY_KEY, -max_store, -1)


def _normalize_plugin_response_item(item: Any) -> Any:
    if not isinstance(item, dict):
        return item

    raw_type = str(item.get("type") or "").strip().lower()
    name = str(item.get("name") or item.get("filename") or "attachment").strip() or "attachment"
    mimetype_value = str(item.get("mimetype") or item.get("mime_type") or "").strip().lower()
    if not mimetype_value:
        mimetype_value = str(mimetypes.guess_type(name)[0] or "").strip().lower() or "application/octet-stream"

    inferred_type = _media_type_from_mimetype(mimetype_value)
    if raw_type in {"image", "audio", "video"}:
        media_type = raw_type
    elif raw_type == "file":
        media_type = inferred_type if inferred_type != "file" else "file"
    elif inferred_type in {"image", "audio", "video"}:
        media_type = inferred_type
    else:
        return item

    raw = None
    if isinstance(item.get("data"), (bytes, bytearray)):
        raw = bytes(item.get("data"))
    elif isinstance(item.get("bytes"), (bytes, bytearray)):
        raw = bytes(item.get("bytes"))
    elif isinstance(item.get("data_b64"), str):
        try:
            raw = _decode_attachment_data(item.get("data_b64"))
        except Exception:
            raw = None
    elif isinstance(item.get("data"), str):
        try:
            raw = _decode_attachment_data(item.get("data"))
        except Exception:
            raw = None

    if raw is None:
        blob_key = str(item.get("blob_key") or "").strip()
        if blob_key:
            try:
                blob = redis_blob_client.get(blob_key)
                if isinstance(blob, (bytes, bytearray)):
                    raw = bytes(blob)
            except Exception:
                raw = None

    if raw is None:
        path_value = str(item.get("path") or item.get("file_path") or item.get("artifact_path") or "").strip()
        if path_value:
            try:
                path = Path(path_value).expanduser()
                if path.is_file():
                    raw = path.read_bytes()
                    if name == "attachment":
                        name = path.name or name
                    if mimetype_value == "application/octet-stream":
                        mimetype_value = str(mimetypes.guess_type(name)[0] or mimetype_value).strip().lower()
                        inferred_type = _media_type_from_mimetype(mimetype_value)
                        if raw_type in {"", "file"} and inferred_type != "file":
                            media_type = inferred_type
            except Exception:
                raw = None

    safe = dict(item)
    safe.pop("data", None)
    safe.pop("bytes", None)
    safe.pop("data_b64", None)
    safe["type"] = media_type
    safe["name"] = name
    safe["mimetype"] = mimetype_value
    if raw is not None:
        file_id = str(uuid.uuid4())
        _store_file_blob_in_redis(file_id, raw)
        safe["id"] = file_id
        safe["size"] = len(raw)
        safe.pop("blob_key", None)
        safe.pop("path", None)
        safe.pop("file_path", None)
        safe.pop("artifact_path", None)
    elif str(safe.get("file_id") or "").strip() and not str(safe.get("id") or "").strip():
        safe["id"] = str(safe.get("file_id") or "").strip()
    return safe


async def _process_message(
    *,
    user_name: str,
    message_content: str,
    input_artifacts: Optional[List[Dict[str, Any]]] = None,
    session_scope_id: str,
    wait_callback: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    max_llm = _read_positive_int("tater:max_llm", DEFAULT_MAX_LLM)
    loop_messages = _load_loop_messages_for_hydra(max_llm)

    verba_registry_module.ensure_verbas_loaded()
    merged_registry = dict(verba_registry_module.get_verba_registry() or {})

    origin = {
        "platform": "webui",
        "user": user_name,
        "user_id": user_name,
        "session_id": session_scope_id,
    }
    if isinstance(input_artifacts, list) and input_artifacts:
        origin["input_artifacts"] = [dict(item) for item in input_artifacts if isinstance(item, dict)]

    async with get_llm_client_from_env(redis_conn=redis_client) as llm_client:
        async def _wait(
            func_name: str,
            plugin_obj: Any,
            wait_text: str = "",
            wait_payload: Optional[Dict[str, Any]] = None,
        ) -> None:
            progress_payload = dict(wait_payload) if isinstance(wait_payload, dict) else {}
            text = str(wait_text or progress_payload.get("text") or "").strip()
            if not text:
                text = "I'm working on that now."
            progress_payload["text"] = text

            if callable(wait_callback):
                attempts = [
                    (func_name, plugin_obj, text, progress_payload),
                    (func_name, plugin_obj, text),
                    (func_name, plugin_obj),
                ]
                for args in attempts:
                    try:
                        callback_result = wait_callback(*args)
                        if hasattr(callback_result, "__await__"):
                            await callback_result
                        break
                    except TypeError:
                        continue
                    except Exception:
                        logger.exception("wait_callback failed")
                        break

        result = await run_hydra_turn(
            llm_client=llm_client,
            platform="webui",
            history_messages=loop_messages,
            registry=merged_registry,
            enabled_predicate=get_verba_enabled,
            context={"raw_message": message_content, "input_artifacts": list(input_artifacts or [])},
            user_text=(message_content or ""),
            scope=f"session:{session_scope_id}",
            origin=origin,
            redis_client=redis_client,
            wait_callback=(_wait if callable(wait_callback) else None),
            max_rounds=0,
            max_tool_calls=0,
            platform_preamble="",
        )
        try:
            perf = llm_client.get_perf_stats() if hasattr(llm_client, "get_perf_stats") else {}
            if isinstance(perf, dict):
                perf["updated_at"] = time.time()
                _save_last_llm_stats(perf)
        except Exception:
            logger.exception("Failed to save last LLM speed stats")

    responses: List[Any] = []
    text_value = result.get("text") if isinstance(result, dict) else None
    if isinstance(text_value, str) and text_value.strip():
        responses.append(text_value)

    artifacts = result.get("artifacts") if isinstance(result, dict) else []
    for item in artifacts or []:
        responses.append(_normalize_plugin_response_item(item))

    task_name = ""
    if isinstance(result, dict):
        task_name = str(result.get("task_name") or "").strip()
    return {"responses": responses, "agent": True, "task_name": task_name}


class ChatJobManager:
    def __init__(self, *, ttl_seconds: int = 1800, max_jobs: int = 200):
        self.lock = threading.RLock()
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.order: List[str] = []
        self.ttl_seconds = int(ttl_seconds)
        self.max_jobs = int(max_jobs)

    def _emit(self, job: Dict[str, Any], event: Dict[str, Any]) -> None:
        event_queue = job.get("events")
        if not isinstance(event_queue, queue.Queue):
            return
        try:
            event_queue.put_nowait(dict(event))
        except queue.Full:
            try:
                event_queue.get_nowait()
            except Exception:
                pass
            try:
                event_queue.put_nowait(dict(event))
            except Exception:
                pass

    def _cleanup_locked(self) -> None:
        now = time.time()
        keep: List[str] = []
        for job_id in list(self.order):
            job = self.jobs.get(job_id)
            if not isinstance(job, dict):
                continue
            status = str(job.get("status") or "").strip().lower()
            completed_at = float(job.get("completed_at") or 0.0)
            should_drop = False
            if status in {"done", "error"} and completed_at > 0:
                if now - completed_at > self.ttl_seconds:
                    should_drop = True
            if len(keep) >= self.max_jobs:
                should_drop = True
            if should_drop:
                self.jobs.pop(job_id, None)
                continue
            keep.append(job_id)
        self.order = keep[-self.max_jobs :]

    def _set_status_locked(self, job: Dict[str, Any], *, status: str, current_tool: str = "") -> None:
        job["status"] = status
        job["current_tool"] = current_tool

    def _worker(
        self,
        *,
        job_id: str,
        user_name: str,
        message: str,
        input_artifacts: Optional[List[Dict[str, Any]]],
        session_id: str,
    ) -> None:
        with self.lock:
            job = self.jobs.get(job_id)
            if not isinstance(job, dict):
                return
            self._set_status_locked(job, status="running")
            self._emit(
                job,
                {
                    "type": "status",
                    "status": "running",
                    "job_id": job_id,
                    "task_name": str(job.get("task_name") or "").strip(),
                },
            )

        def _on_tool(
            func_name: str,
            plugin_obj: Any,
            wait_text: str = "",
            wait_payload: Optional[Dict[str, Any]] = None,
        ) -> None:
            display_name = ""
            if plugin_obj is None:
                display_name = f"kernel::{func_name}"
            else:
                display_name = (
                    getattr(plugin_obj, "verba_name", None)
                    or getattr(plugin_obj, "pretty_name", None)
                    or getattr(plugin_obj, "name", None)
                    or func_name
                )
            with self.lock:
                job_local = self.jobs.get(job_id)
                if not isinstance(job_local, dict):
                    return
                if str(job_local.get("status") or "") not in {"queued", "running"}:
                    return
                self._set_status_locked(job_local, status="running", current_tool=str(display_name or "").strip())
                self._emit(
                    job_local,
                    {
                        "type": "tool",
                        "status": "running",
                        "current_tool": str(display_name or "").strip(),
                        "job_id": job_id,
                        "task_name": str(job_local.get("task_name") or "").strip(),
                    },
                )
                progress_payload = dict(wait_payload) if isinstance(wait_payload, dict) else {}
                wait_line = str(wait_text or progress_payload.get("text") or "").strip()
                if not wait_line:
                    wait_line = "I'm working on that now."
                if wait_line:
                    _save_chat_message("assistant", "assistant", {"marker": "plugin_wait", "content": wait_line})
                    event_payload: Dict[str, Any] = {
                        "type": "waiting",
                        "status": "running",
                        "wait_text": wait_line,
                        "job_id": job_id,
                    }
                    if progress_payload:
                        event_payload["wait_payload"] = progress_payload
                    self._emit(
                        job_local,
                        event_payload,
                    )

        try:
            payload = asyncio.run(
                _process_message(
                    user_name=user_name,
                    message_content=message,
                    input_artifacts=list(input_artifacts or []),
                    session_scope_id=session_id,
                    wait_callback=_on_tool,
                )
            )
            responses = list(payload.get("responses") or []) if isinstance(payload, dict) else []
            task_name = (
                str(payload.get("task_name") or "").strip()
                if isinstance(payload, dict)
                else ""
            )

            for item in responses:
                _save_chat_message("assistant", "assistant", item)

            with self.lock:
                job = self.jobs.get(job_id)
                if not isinstance(job, dict):
                    return
                self._set_status_locked(job, status="done")
                if task_name:
                    job["task_name"] = task_name
                job["responses"] = responses
                job["completed_at"] = time.time()
                self._emit(
                    job,
                    {
                        "type": "done",
                        "status": "done",
                        "responses": responses,
                        "job_id": job_id,
                        "task_name": str(job.get("task_name") or "").strip(),
                    },
                )
        except Exception as exc:
            logger.error("chat job failed: %s", exc, exc_info=True)
            with self.lock:
                job = self.jobs.get(job_id)
                if not isinstance(job, dict):
                    return
                self._set_status_locked(job, status="error")
                job["error"] = str(exc)
                job["completed_at"] = time.time()
                self._emit(
                    job,
                    {
                        "type": "job_error",
                        "status": "error",
                        "error": str(exc),
                        "job_id": job_id,
                        "task_name": str(job.get("task_name") or "").strip(),
                    },
                )

    def create_job(
        self,
        *,
        user_name: str,
        message: str,
        input_artifacts: Optional[List[Dict[str, Any]]] = None,
        session_id: str,
    ) -> Dict[str, Any]:
        job_id = str(uuid.uuid4())
        with self.lock:
            self._cleanup_locked()
            job = {
                "id": job_id,
                "session_id": str(session_id or ""),
                "user_name": str(user_name or "User"),
                "message": str(message or ""),
                "task_name": (str(message or "").strip()[:72] or "Hydra task"),
                "input_artifacts": list(input_artifacts or []),
                "status": "queued",
                "current_tool": "",
                "responses": [],
                "error": "",
                "created_at": time.time(),
                "completed_at": 0.0,
                "events": queue.Queue(maxsize=512),
            }
            self.jobs[job_id] = job
            self.order = [jid for jid in self.order if jid != job_id]
            self.order.append(job_id)
            self._emit(
                job,
                {
                    "type": "status",
                    "status": "queued",
                    "job_id": job_id,
                    "task_name": str(job.get("task_name") or "").strip(),
                },
            )

        worker = threading.Thread(
            target=self._worker,
            kwargs={
                "job_id": job_id,
                "user_name": user_name,
                "message": message,
                "input_artifacts": list(input_artifacts or []),
                "session_id": session_id,
            },
            daemon=True,
            name=f"chat-job-{job_id[:8]}",
        )
        worker.start()

        return {
            "job_id": job_id,
            "status": "queued",
            "session_id": session_id,
            "task_name": str(job.get("task_name") or "").strip(),
        }

    def get_snapshot(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            job = self.jobs.get(job_id)
            if not isinstance(job, dict):
                return None
            return {
                "job_id": str(job.get("id") or ""),
                "session_id": str(job.get("session_id") or ""),
                "status": str(job.get("status") or ""),
                "task_name": str(job.get("task_name") or "").strip(),
                "current_tool": str(job.get("current_tool") or ""),
                "responses": list(job.get("responses") or []),
                "error": str(job.get("error") or ""),
                "created_at": float(job.get("created_at") or 0.0),
                "completed_at": float(job.get("completed_at") or 0.0),
            }

    def get_event_queue(self, job_id: str) -> Optional[queue.Queue]:
        with self.lock:
            job = self.jobs.get(job_id)
            if not isinstance(job, dict):
                return None
            q = job.get("events")
            return q if isinstance(q, queue.Queue) else None

    def active_count(self) -> int:
        with self.lock:
            count = 0
            for job in self.jobs.values():
                status = str((job or {}).get("status") or "")
                if status in {"queued", "running"}:
                    count += 1
            return count


chat_jobs = ChatJobManager()


def _sse(event_type: str, payload: Dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _normalize_repo_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        url = str(row.get("url") or "").strip()
        if not name and not url:
            continue
        if not url:
            raise HTTPException(status_code=400, detail="Each repo entry must include a URL.")
        out.append({"name": name, "url": url})
    return out


def _shop_payload_sort_key(item: Dict[str, Any]) -> tuple[str, str]:
    name = str(
        item.get("name")
        or item.get("display_name")
        or item.get("label")
        or item.get("title")
        or item.get("id")
        or item.get("module_key")
        or item.get("key")
        or ""
    ).strip()
    item_id = str(item.get("id") or item.get("module_key") or item.get("key") or name).strip()
    return (name.casefold(), item_id.casefold())


def _plugin_platforms(item: Dict[str, Any]) -> List[str]:
    helper = getattr(verba_store_module, "_get_item_platforms", None)
    if callable(helper):
        try:
            platforms = helper(item)
            return [str(x).strip() for x in (platforms or []) if str(x).strip()]
        except Exception:
            pass

    raw = item.get("portals") or item.get("portal") or item.get("platforms") or []
    if isinstance(raw, str):
        raw = [part.strip() for part in raw.replace(",", " ").split() if part.strip()]
    out: List[str] = []
    for entry in raw if isinstance(raw, list) else []:
        value = str(entry or "").strip()
        if value and value not in out:
            out.append(value)
    return out


def _verba_shop_raw() -> Dict[str, Any]:
    verba_registry_module.ensure_verbas_loaded()
    manifest_repos = verba_store_module.get_configured_shop_manifest_repos()
    catalog_items, catalog_errors = verba_store_module.load_shop_catalog(manifest_repos)

    build_entries = getattr(verba_store_module, "_build_installed_entries", None)
    installed_entries = build_entries(catalog_items) if callable(build_entries) else []

    if not isinstance(installed_entries, list):
        installed_entries = []

    installed_payload: List[Dict[str, Any]] = []
    installed_ids = set()
    for entry in installed_entries:
        if not isinstance(entry, dict):
            continue
        plugin_id = str(entry.get("id") or "").strip()
        if not plugin_id:
            continue
        installed_ids.add(plugin_id)
        installed_payload.append(
            {
                "id": plugin_id,
                "name": str(entry.get("display_name") or plugin_id),
                "description": str(entry.get("description") or ""),
                "installed_ver": str(entry.get("installed_ver") or "0.0.0"),
                "store_ver": str(entry.get("store_ver") or ""),
                "source_label": str(entry.get("source_label") or ""),
                "update_available": bool(entry.get("update_available")),
                "catalog_backed": bool(entry.get("catalog_item")),
                "enabled": bool(get_verba_enabled(plugin_id)),
                "platforms": list(entry.get("platforms") or []),
                "platforms_str": str(entry.get("platforms_str") or ""),
            }
        )

    catalog_payload: List[Dict[str, Any]] = []
    for item in catalog_items:
        if not isinstance(item, dict):
            continue
        plugin_id = str(item.get("id") or "").strip()
        if not plugin_id:
            continue
        catalog_payload.append(
            {
                "id": plugin_id,
                "name": str(item.get("name") or plugin_id).strip() or plugin_id,
                "description": str(item.get("description") or "").strip(),
                "version": str(item.get("version") or "").strip(),
                "source_label": str(item.get("_source_label") or "").strip(),
                "installed": plugin_id in installed_ids,
                "platforms": _plugin_platforms(item),
            }
        )

    installed_payload.sort(key=_shop_payload_sort_key)
    catalog_payload.sort(key=_shop_payload_sort_key)

    return {
        "repos": {
            "configured": manifest_repos,
            "additional": verba_store_module.get_additional_shop_manifest_repos(),
            "default": manifest_repos[0] if manifest_repos else {"name": "", "url": ""},
        },
        "errors": list(catalog_errors or []),
        "installed": installed_payload,
        "catalog": catalog_payload,
        "updates_available": len([entry for entry in installed_payload if entry.get("update_available")]),
        "_catalog_items_raw": catalog_items,
        "_installed_entries_raw": installed_entries,
    }


def _core_shop_raw() -> Dict[str, Any]:
    manifest_repos = core_store_module.get_configured_core_shop_manifest_repos()
    catalog_items, catalog_errors = core_store_module.load_core_shop_catalog(manifest_repos)

    build_entries = getattr(core_store_module, "_build_installed_core_entries", None)
    installed_entries = build_entries(catalog_items) if callable(build_entries) else []
    if not isinstance(installed_entries, list):
        installed_entries = []

    installed_payload: List[Dict[str, Any]] = []
    installed_ids = set()
    for entry in installed_entries:
        if not isinstance(entry, dict):
            continue
        core_id = str(entry.get("id") or "").strip()
        if not core_id:
            continue
        installed_ids.add(core_id)
        installed_payload.append(
            {
                "id": core_id,
                "name": str(entry.get("display_name") or core_id),
                "description": str(entry.get("description") or ""),
                "module_key": str(entry.get("module_key") or f"{core_id}_core"),
                "installed_ver": str(entry.get("installed_ver") or "0.0.0"),
                "store_ver": str(entry.get("store_ver") or ""),
                "source_label": str(entry.get("source_label") or ""),
                "update_available": bool(entry.get("update_available")),
                "catalog_backed": bool(entry.get("catalog_item")),
                "running": bool(entry.get("running")),
            }
        )

    catalog_payload: List[Dict[str, Any]] = []
    for item in catalog_items:
        if not isinstance(item, dict):
            continue
        core_id = str(item.get("id") or "").strip()
        if not core_id:
            continue
        catalog_payload.append(
            {
                "id": core_id,
                "name": str(item.get("name") or core_id).strip() or core_id,
                "description": str(item.get("description") or "").strip(),
                "version": str(item.get("version") or "").strip(),
                "source_label": str(item.get("_source_label") or "").strip(),
                "installed": core_id in installed_ids,
            }
        )

    installed_payload.sort(key=_shop_payload_sort_key)
    catalog_payload.sort(key=_shop_payload_sort_key)

    return {
        "repos": {
            "configured": manifest_repos,
            "additional": core_store_module.get_additional_core_shop_manifest_repos(),
            "default": manifest_repos[0] if manifest_repos else {"name": "", "url": ""},
        },
        "errors": list(catalog_errors or []),
        "installed": installed_payload,
        "catalog": catalog_payload,
        "updates_available": len([entry for entry in installed_payload if entry.get("update_available")]),
        "_catalog_items_raw": catalog_items,
        "_installed_entries_raw": installed_entries,
    }


def _portal_shop_raw() -> Dict[str, Any]:
    manifest_repos = portal_store_module.get_configured_portal_shop_manifest_repos()
    catalog_items, catalog_errors = portal_store_module.load_portal_shop_catalog(manifest_repos)

    build_entries = getattr(portal_store_module, "_build_installed_portal_entries", None)
    installed_entries = build_entries(catalog_items) if callable(build_entries) else []
    if not isinstance(installed_entries, list):
        installed_entries = []

    installed_payload: List[Dict[str, Any]] = []
    installed_ids = set()
    for entry in installed_entries:
        if not isinstance(entry, dict):
            continue
        portal_id = str(entry.get("id") or "").strip()
        if not portal_id:
            continue
        installed_ids.add(portal_id)
        installed_payload.append(
            {
                "id": portal_id,
                "name": str(entry.get("display_name") or portal_id),
                "description": str(entry.get("description") or ""),
                "module_key": str(entry.get("module_key") or f"{portal_id}_portal"),
                "installed_ver": str(entry.get("installed_ver") or "0.0.0"),
                "store_ver": str(entry.get("store_ver") or ""),
                "source_label": str(entry.get("source_label") or ""),
                "update_available": bool(entry.get("update_available")),
                "catalog_backed": bool(entry.get("catalog_item")),
                "running": bool(entry.get("running")),
            }
        )

    catalog_payload: List[Dict[str, Any]] = []
    for item in catalog_items:
        if not isinstance(item, dict):
            continue
        portal_id = str(item.get("id") or "").strip()
        if not portal_id:
            continue
        catalog_payload.append(
            {
                "id": portal_id,
                "name": str(item.get("name") or portal_id).strip() or portal_id,
                "description": str(item.get("description") or "").strip(),
                "version": str(item.get("version") or "").strip(),
                "source_label": str(item.get("_source_label") or "").strip(),
                "installed": portal_id in installed_ids,
            }
        )

    installed_payload.sort(key=_shop_payload_sort_key)
    catalog_payload.sort(key=_shop_payload_sort_key)

    return {
        "repos": {
            "configured": manifest_repos,
            "additional": portal_store_module.get_additional_portal_shop_manifest_repos(),
            "default": manifest_repos[0] if manifest_repos else {"name": "", "url": ""},
        },
        "errors": list(catalog_errors or []),
        "installed": installed_payload,
        "catalog": catalog_payload,
        "updates_available": len([entry for entry in installed_payload if entry.get("update_available")]),
        "_catalog_items_raw": catalog_items,
        "_installed_entries_raw": installed_entries,
    }


def _integration_shop_raw() -> Dict[str, Any]:
    manifest_repos = integration_store_module.get_configured_integration_shop_manifest_repos()
    catalog_items, catalog_errors = integration_store_module.load_integration_shop_catalog(manifest_repos)

    build_entries = getattr(integration_store_module, "_build_installed_integration_entries", None)
    installed_entries = build_entries(catalog_items) if callable(build_entries) else []
    if not isinstance(installed_entries, list):
        installed_entries = []

    installed_payload: List[Dict[str, Any]] = []
    installed_ids = set()
    for entry in installed_entries:
        if not isinstance(entry, dict):
            continue
        integration_id = str(entry.get("id") or "").strip()
        if not integration_id:
            continue
        installed_ids.add(integration_id)
        installed_payload.append(
            {
                "id": integration_id,
                "name": str(entry.get("display_name") or integration_id),
                "description": str(entry.get("description") or ""),
                "installed_ver": str(entry.get("installed_ver") or "0.0.0"),
                "store_ver": str(entry.get("store_ver") or ""),
                "source_label": str(entry.get("source_label") or ""),
                "update_available": bool(entry.get("update_available")),
                "catalog_backed": bool(entry.get("catalog_item")),
                "required": bool(entry.get("required")),
                "enabled": bool(entry.get("enabled")),
            }
        )

    catalog_payload: List[Dict[str, Any]] = []
    for item in catalog_items:
        if not isinstance(item, dict):
            continue
        integration_id = str(item.get("id") or "").strip()
        if not integration_id:
            continue
        catalog_payload.append(
            {
                "id": integration_id,
                "name": str(item.get("name") or integration_id).strip() or integration_id,
                "description": str(item.get("description") or "").strip(),
                "version": str(item.get("version") or "").strip(),
                "source_label": str(item.get("_source_label") or "").strip(),
                "installed": integration_id in installed_ids,
                "required": integration_id in integration_store_module.REQUIRED_INTEGRATION_IDS,
                "enabled": integration_store_module.get_integration_enabled(integration_id),
            }
        )

    installed_payload.sort(key=_shop_payload_sort_key)
    catalog_payload.sort(key=_shop_payload_sort_key)

    return {
        "repos": {
            "configured": manifest_repos,
            "additional": integration_store_module.get_additional_integration_shop_manifest_repos(),
            "default": manifest_repos[0] if manifest_repos else {"name": "", "url": ""},
        },
        "errors": list(catalog_errors or []),
        "installed": installed_payload,
        "catalog": catalog_payload,
        "updates_available": len(
            [entry for entry in installed_payload if entry.get("update_available") and entry.get("enabled")]
        ),
        "_catalog_items_raw": catalog_items,
        "_installed_entries_raw": installed_entries,
    }


def _dashboard_shop_update_group(kind: str, label: str, loader: Callable[[], Dict[str, Any]]) -> Dict[str, Any]:
    try:
        snapshot = loader()
    except Exception as exc:
        logger.debug("[dashboard] failed loading %s update snapshot", kind, exc_info=True)
        return {
            "kind": kind,
            "label": label,
            "count": 0,
            "items": [],
            "error": str(exc),
        }

    installed = snapshot.get("installed") if isinstance(snapshot, dict) and isinstance(snapshot.get("installed"), list) else []
    rows: List[Dict[str, Any]] = []
    for entry in installed:
        if not isinstance(entry, dict) or not bool(entry.get("update_available")):
            continue
        if kind == "integrations" and not bool(entry.get("enabled")):
            continue
        item_id = str(entry.get("id") or entry.get("module_key") or "").strip()
        name = str(entry.get("name") or entry.get("display_name") or item_id or label).strip()
        rows.append(
            {
                "id": item_id,
                "name": name,
                "installed": str(entry.get("installed_ver") or "0.0.0").strip(),
                "latest": str(entry.get("store_ver") or "").strip(),
                "source": str(entry.get("source_label") or "").strip(),
            }
        )
    rows.sort(key=_shop_payload_sort_key)
    return {
        "kind": kind,
        "label": label,
        "count": len(rows),
        "items": rows[:8],
        "installed_count": len(installed),
        "errors": list(snapshot.get("errors") or []) if isinstance(snapshot, dict) else [],
    }


def _dashboard_firmware_update_group() -> Dict[str, Any]:
    try:
        payload = esphome_home_module.get_runtime_payload(
            redis_client=redis_client,
            core_key="esphome",
            core_tab=_esphome_platform_tab_spec(),
            panel="firmware",
        )
    except Exception as exc:
        logger.debug("[dashboard] failed loading firmware update snapshot", exc_info=True)
        return {
            "kind": "firmware",
            "label": "Firmware",
            "count": 0,
            "items": [],
            "error": str(exc),
        }
    firmware = payload.get("firmware") if isinstance(payload, dict) and isinstance(payload.get("firmware"), dict) else {}
    update_rows = firmware.get("firmware_updates") if isinstance(firmware.get("firmware_updates"), list) else []
    rows: List[Dict[str, Any]] = []
    for row in update_rows:
        if not isinstance(row, dict):
            continue
        selector = str(row.get("selector") or "").strip()
        title = str(row.get("title") or selector or "ESPHome device").strip()
        rows.append(
            {
                "id": selector,
                "name": title,
                "installed": str(row.get("installed") or "unknown").strip(),
                "latest": str(row.get("latest") or "").strip(),
                "template": str(row.get("template_label") or "").strip(),
            }
        )
    return {
        "kind": "firmware",
        "label": "Firmware",
        "count": len(rows),
        "items": rows[:8],
    }


def _dashboard_updates_snapshot() -> Dict[str, Any]:
    groups = [
        _dashboard_firmware_update_group(),
        _dashboard_shop_update_group("cores", "Cores", _core_shop_raw),
        _dashboard_shop_update_group("integrations", "Integrations", _integration_shop_raw),
        _dashboard_shop_update_group("portals", "Portals", _portal_shop_raw),
        _dashboard_shop_update_group("verbas", "Verba", _verba_shop_raw),
    ]
    total = sum(_dashboard_safe_int(group.get("count")) for group in groups if isinstance(group, dict))
    parts = [
        f"{_dashboard_safe_int(group.get('count'))} {str(group.get('label') or '').strip()}"
        for group in groups
        if isinstance(group, dict) and _dashboard_safe_int(group.get("count")) > 0
    ]
    errors = [
        {
            "kind": str(group.get("kind") or "").strip(),
            "label": str(group.get("label") or "").strip(),
            "error": str(group.get("error") or "").strip(),
        }
        for group in groups
        if isinstance(group, dict) and str(group.get("error") or "").strip()
    ]
    return {
        "total": total,
        "groups": groups,
        "errors": errors,
        "summary": ", ".join(parts) if parts else "No updates currently available.",
    }


def _autostart_enabled_surfaces() -> None:
    core_entries = core_registry_module.refresh_core_registry()
    portal_entries = portal_registry_module.refresh_portal_registry()

    for core in core_entries:
        key = str(core.get("key") or "").strip()
        if not key:
            continue
        should_run = str(redis_client.get(f"{key}_running") or "").strip().lower() == "true"
        if should_run and not core_runtime.is_running(key):
            logger.info("[startup] starting core %s", key)
            core_runtime.start(key)

    for portal in portal_entries:
        key = str(portal.get("key") or "").strip()
        if not key:
            continue
        should_run = str(redis_client.get(f"{key}_running") or "").strip().lower() == "true"
        if should_run and not portal_runtime.is_running(key):
            logger.info("[startup] starting portal %s", key)
            portal_runtime.start(key)


def _run_async_sync(coro: Any, timeout: float = 45.0) -> Any:
    try:
        return asyncio.run(asyncio.wait_for(coro, timeout=timeout))
    except RuntimeError:
        holder: Dict[str, Any] = {"done": False, "result": None, "error": None}

        def _worker() -> None:
            worker_loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(worker_loop)
                holder["result"] = worker_loop.run_until_complete(asyncio.wait_for(coro, timeout=timeout))
            except Exception as exc:
                holder["error"] = exc
            finally:
                with contextlib.suppress(Exception):
                    worker_loop.close()
                holder["done"] = True

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()
        thread.join(timeout + 1.0)
        if not holder.get("done"):
            raise TimeoutError("Timed out waiting for async operation")
        if holder.get("error") is not None:
            raise holder["error"]
        return holder.get("result")


def _start_builtin_esphome() -> None:
    if esphome_home_module.is_running():
        return
    logger.info("[startup] starting built-in ESPHome services")
    _run_async_sync(esphome_home_module.startup(), timeout=60.0)


def _stop_builtin_esphome() -> None:
    if not esphome_home_module.is_running():
        return
    logger.info("[shutdown] stopping built-in ESPHome services")
    _run_async_sync(esphome_home_module.shutdown(), timeout=30.0)


def _redis_reachable_for_startup() -> tuple[bool, str]:
    try:
        status = get_redis_connection_status()
        return bool(status.get("connected")), str(status.get("error") or "")
    except Exception as exc:
        return False, str(exc)


def _restore_progress_logger(label: str):
    prefix = str(label or "restore").strip().lower()

    def _cb(progress_value: float, message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        try:
            pct = max(0.0, min(1.0, float(progress_value))) * 100.0
        except Exception:
            pct = 0.0
        logger.info("[startup-restore][%s] %0.1f%% %s", prefix, pct, text)

    return _cb


def _restore_enabled_surfaces() -> Dict[str, Any]:
    """
    Restore enabled surfaces that are missing on disk.
    """
    summary: Dict[str, Any] = {
        "integrations_enabled": 0,
        "integrations_missing_before": 0,
        "integrations_missing_after": 0,
        "plugins_missing_before": 0,
        "plugins_missing_after": 0,
        "cores_missing_before": 0,
        "cores_missing_after": 0,
        "portals_missing_before": 0,
        "portals_missing_after": 0,
        "builtin_platforms": [],
    }

    # 1) Enabled integrations
    try:
        enabled_integrations = list(integration_store_module.get_enabled_integration_ids() or [])
    except Exception:
        enabled_integrations = []
    summary["integrations_enabled"] = len(enabled_integrations)
    try:
        missing_integrations_before = list(integration_store_module._enabled_missing_integration_ids(enabled_integrations) or [])
    except Exception:
        missing_integrations_before = []
    summary["integrations_missing_before"] = len(missing_integrations_before)
    if missing_integrations_before:
        logger.info("[startup-restore] restoring %d enabled integration(s)", len(missing_integrations_before))
        integration_store_module.ensure_enabled_integrations_ready(progress_cb=_restore_progress_logger("integrations"))
    elif enabled_integrations:
        logger.info("[startup-restore] enabled integrations already present: %s", ", ".join(enabled_integrations))
    else:
        logger.info("[startup-restore] no enabled integrations configured")
    try:
        missing_integrations_after = list(integration_store_module._enabled_missing_integration_ids(enabled_integrations) or [])
    except Exception:
        missing_integrations_after = []
    summary["integrations_missing_after"] = len(missing_integrations_after)
    if missing_integrations_after:
        logger.warning("[startup-restore] enabled integrations still missing: %s", ", ".join(missing_integrations_after))

    # 2) Verbas
    try:
        missing_plugins_before = list(verba_store_module._enabled_missing_verba_ids() or [])
    except Exception:
        missing_plugins_before = []
    summary["plugins_missing_before"] = len(missing_plugins_before)
    if missing_plugins_before:
        logger.info("[startup-restore] restoring %d missing enabled verba(s)", len(missing_plugins_before))
        verba_store_module.ensure_verbas_ready(progress_cb=_restore_progress_logger("verbas"))
        verba_registry_module.reload_verbas()
    try:
        missing_plugins_after = list(verba_store_module._enabled_missing_verba_ids() or [])
    except Exception:
        missing_plugins_after = []
    summary["plugins_missing_after"] = len(missing_plugins_after)

    # 3) Cores
    try:
        missing_cores_before = list(core_store_module._enabled_missing_core_ids() or [])
    except Exception:
        missing_cores_before = []
    summary["cores_missing_before"] = len(missing_cores_before)
    if missing_cores_before:
        logger.info("[startup-restore] restoring %d missing enabled core(s)", len(missing_cores_before))
        core_store_module.ensure_cores_ready(progress_cb=_restore_progress_logger("cores"))
    try:
        missing_cores_after = list(core_store_module._enabled_missing_core_ids() or [])
    except Exception:
        missing_cores_after = []
    summary["cores_missing_after"] = len(missing_cores_after)

    # 4) Portals
    try:
        missing_portals_before = list(portal_store_module._enabled_missing_portal_ids() or [])
    except Exception:
        missing_portals_before = []
    summary["portals_missing_before"] = len(missing_portals_before)
    if missing_portals_before:
        logger.info("[startup-restore] restoring %d missing enabled portal(s)", len(missing_portals_before))
        portal_store_module.ensure_portals_ready(progress_cb=_restore_progress_logger("portals"))
    try:
        missing_portals_after = list(portal_store_module._enabled_missing_portal_ids() or [])
    except Exception:
        missing_portals_after = []
    summary["portals_missing_after"] = len(missing_portals_after)

    # Refresh registries after potential module downloads.
    core_registry_module.refresh_core_registry()
    portal_registry_module.refresh_portal_registry()
    summary["builtin_platforms"] = ["esphome"]

    return summary


def _replay_startup_after_redis_configure() -> Dict[str, Any]:
    """
    Re-run startup restore/autostart flow after Redis is configured at runtime.
    This lets first-run setups recover missing enabled surfaces without requiring
    a manual backend restart.
    """
    result: Dict[str, Any] = {
        "ok": True,
        "ran_restore": False,
        "ran_autostart": False,
        "error": "",
        "restore_summary": {},
        "local_llm_warmup": {},
        "speech_warmup": {},
    }

    bootstrap_state["restore_in_progress"] = True
    bootstrap_state["restore_complete"] = False
    bootstrap_state["restore_error"] = ""

    try:
        if bool(bootstrap_state.get("restore_enabled")):
            summary = _restore_enabled_surfaces()
            bootstrap_state["restore_summary"] = summary
            result["restore_summary"] = dict(summary or {})
            result["ran_restore"] = True
            logger.info("[runtime-restore] summary: %s", summary)
        else:
            logger.info("[runtime-restore] skipped (HTMLUI_RESTORE_ENABLED_SURFACES_ON_STARTUP=false)")

        _run_async_sync(ensure_integration_runtime_started(redis_client), timeout=15.0)
        result["local_llm_warmup"] = _start_local_llm_warmup_for_startup(reason="runtime-bootstrap")
        _start_builtin_esphome()
        if _speech_model_warmup_on_startup_enabled():
            result["speech_warmup"] = _start_speech_model_warmup(
                get_shared_speech_settings(),
                reason="runtime-bootstrap",
            )
            result["speech_warmup"] = _wait_for_speech_model_warmup()
        else:
            logger.info("[speech-warmup] startup warmup skipped (TATER_SPEECH_WARMUP_ON_STARTUP=false)")
        if bool(bootstrap_state.get("autostart_enabled")):
            _autostart_enabled_surfaces()
            result["ran_autostart"] = True
        else:
            logger.info("[runtime-autostart] skipped (HTMLUI_AUTOSTART_ENABLED_SURFACES_ON_STARTUP=false)")
        _start_dashboard_brief_scheduler()
    except Exception as exc:
        err = str(exc)
        bootstrap_state["restore_error"] = err
        result["ok"] = False
        result["error"] = err
        logger.warning("[runtime-bootstrap] failed after redis configure: %s", exc)
    finally:
        bootstrap_state["restore_in_progress"] = False
        bootstrap_state["restore_complete"] = True

    return result


def _running_surface_keys(runtime: SurfaceRuntimeManager) -> List[str]:
    with runtime.lock:
        keys = list(runtime.threads.keys())
    running = [str(key).strip() for key in keys if str(key).strip() and runtime.is_running(key)]
    return sorted(set(running))


def _read_positive_float_env(key: str, default: float) -> float:
    raw = str(os.getenv(key, "") or "").strip()
    if not raw:
        return float(default)
    try:
        value = float(raw)
    except Exception:
        return float(default)
    return value if value > 0 else float(default)


def _stop_surface_keys(
    runtime: SurfaceRuntimeManager,
    keys: List[str],
    *,
    timeout: float,
    late_grace_seconds: float = 8.0,
) -> Dict[str, Any]:
    requested = [str(key).strip() for key in (keys or []) if str(key).strip()]
    stopped: List[str] = []
    failed: List[Dict[str, str]] = []

    for key in requested:
        try:
            status = runtime.stop(key, timeout=timeout)
        except Exception as exc:
            failed.append({"key": key, "error": str(exc)})
            continue

        if bool(status.get("running")):
            failed.append({"key": key, "error": str(status.get("reason") or "stop-timeout")})
        else:
            stopped.append(key)

    stopped_late: List[str] = []
    if failed and late_grace_seconds > 0:
        deadline = time.time() + float(late_grace_seconds)
        pending = [row for row in failed if isinstance(row, dict)]
        while pending and time.time() < deadline:
            still_running: List[Dict[str, str]] = []
            for row in pending:
                key = str(row.get("key") or "").strip()
                if not key:
                    continue
                if runtime.is_running(key):
                    still_running.append(row)
                else:
                    stopped_late.append(key)
            pending = still_running
            if pending:
                time.sleep(0.25)

        if stopped_late:
            seen_late = set(stopped_late)
            failed = [row for row in failed if str((row or {}).get("key") or "").strip() not in seen_late]
            stopped.extend(stopped_late)

    return {
        "requested": requested,
        "stopped": stopped,
        "stopped_late": sorted(set(stopped_late)),
        "failed": failed,
    }


def _resume_surface_keys(runtime: SurfaceRuntimeManager, keys: List[str]) -> Dict[str, Any]:
    requested = [str(key).strip() for key in (keys or []) if str(key).strip()]
    resumed: List[str] = []
    already_running: List[str] = []
    failed: List[Dict[str, str]] = []

    for key in requested:
        if runtime.is_running(key):
            already_running.append(key)
            continue
        try:
            status = runtime.start(key)
        except Exception as exc:
            failed.append({"key": key, "error": str(exc)})
            continue
        if bool(status.get("running")):
            resumed.append(key)
        else:
            failed.append({"key": key, "error": str(status.get("reason") or "start-failed")})

    return {
        "requested": requested,
        "resumed": resumed,
        "already_running": already_running,
        "failed": failed,
    }


def _restart_running_surfaces_for_local_llm_reload(*, reason: str = "") -> Dict[str, Any]:
    core_keys = _running_surface_keys(core_runtime)
    portal_keys = _running_surface_keys(portal_runtime)
    stop_timeout = _read_positive_float_env("TATER_LOCAL_LLM_RESTART_STOP_TIMEOUT_SECONDS", 3.0)
    late_grace_seconds = _read_positive_float_env("TATER_LOCAL_LLM_RESTART_LATE_GRACE_SECONDS", 8.0)
    started_ts = time.time()

    logger.info(
        "[local-llm-warmup] restarting running cores/portals after model load reason=%s cores=%d portals=%d",
        str(reason or "").strip() or "-",
        len(core_keys),
        len(portal_keys),
    )
    stopped_portals = _stop_surface_keys(
        portal_runtime,
        portal_keys,
        timeout=stop_timeout,
        late_grace_seconds=late_grace_seconds,
    )
    stopped_cores = _stop_surface_keys(
        core_runtime,
        core_keys,
        timeout=stop_timeout,
        late_grace_seconds=late_grace_seconds,
    )

    failed_core_keys = {str(row.get("key") or "").strip() for row in stopped_cores.get("failed") or []}
    failed_portal_keys = {str(row.get("key") or "").strip() for row in stopped_portals.get("failed") or []}
    core_resume_keys = [key for key in core_keys if key not in failed_core_keys]
    portal_resume_keys = [key for key in portal_keys if key not in failed_portal_keys]
    resumed_cores = _resume_surface_keys(core_runtime, core_resume_keys)
    resumed_portals = _resume_surface_keys(portal_runtime, portal_resume_keys)
    result = {
        "reason": str(reason or "").strip(),
        "started_ts": started_ts,
        "finished_ts": time.time(),
        "active_before": {
            "cores": core_keys,
            "portals": portal_keys,
        },
        "stopped": {
            "cores": stopped_cores,
            "portals": stopped_portals,
        },
        "resumed": {
            "cores": resumed_cores,
            "portals": resumed_portals,
        },
    }
    failed_count = (
        len(stopped_cores.get("failed") or [])
        + len(stopped_portals.get("failed") or [])
        + len(resumed_cores.get("failed") or [])
        + len(resumed_portals.get("failed") or [])
    )
    logger.info(
        "[local-llm-warmup] runtime restart finished cores=%d portals=%d failures=%d",
        len(core_keys),
        len(portal_keys),
        failed_count,
    )
    return result


def _quiesce_surfaces_for_redis_maintenance(
    *,
    action: str,
    stop_timeout: float,
    late_grace_seconds: float,
) -> Dict[str, Any]:
    core_keys = _running_surface_keys(core_runtime)
    esphome_running = bool(esphome_home_module.is_running())
    portal_keys = _running_surface_keys(portal_runtime)

    logger.info(
        "[%s] pausing runtimes before Redis maintenance (cores=%d esphome=%d portals=%d)",
        action,
        len(core_keys),
        1 if esphome_running else 0,
        len(portal_keys),
    )

    # Stop portals first to reduce inbound chatter, then ESPHome, then cores.
    stopped_portals = _stop_surface_keys(
        portal_runtime,
        portal_keys,
        timeout=stop_timeout,
        late_grace_seconds=late_grace_seconds,
    )
    esphome_failed = False
    if esphome_running:
        try:
            _stop_builtin_esphome()
        except Exception:
            logger.exception("[%s] failed stopping built-in ESPHome services", action)
            esphome_failed = True
    stopped_cores = _stop_surface_keys(
        core_runtime,
        core_keys,
        timeout=stop_timeout,
        late_grace_seconds=late_grace_seconds,
    )

    portal_late = list(stopped_portals.get("stopped_late") or [])
    core_late = list(stopped_cores.get("stopped_late") or [])
    if portal_late or core_late:
        logger.info(
            "[%s] runtimes stopped during grace window (cores=%s esphome=%s portals=%s)",
            action,
            ",".join(core_late) if core_late else "-",
            "stopped" if esphome_running and not esphome_failed else "-",
            ",".join(portal_late) if portal_late else "-",
        )

    stop_failures: List[str] = []
    for row in stopped_portals.get("failed") or []:
        stop_failures.append(f"portal:{str(row.get('key') or '').strip()}")
    if esphome_failed:
        stop_failures.append("esphome:built_in")
    for row in stopped_cores.get("failed") or []:
        stop_failures.append(f"core:{str(row.get('key') or '').strip()}")

    return {
        "action": action,
        "active_before": {
            "cores": core_keys,
            "esphome": esphome_running,
            "portals": portal_keys,
        },
        "stopped": {
            "cores": stopped_cores,
            "esphome": {"requested": ["built_in"] if esphome_running else [], "stopped": ["built_in"] if esphome_running and not esphome_failed else [], "failed": ["built_in"] if esphome_failed else []},
            "portals": stopped_portals,
        },
        "stop_failures": stop_failures,
    }


def _resume_surfaces_after_redis_maintenance(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    active_before = snapshot.get("active_before") if isinstance(snapshot, dict) else {}
    if not isinstance(active_before, dict):
        active_before = {}

    core_keys = [str(key).strip() for key in (active_before.get("cores") or []) if str(key).strip()]
    esphome_was_running = bool(active_before.get("esphome"))
    portal_keys = [str(key).strip() for key in (active_before.get("portals") or []) if str(key).strip()]

    # Start cores first, then built-in ESPHome services, then portals.
    resumed_cores = _resume_surface_keys(core_runtime, core_keys)
    esphome_resumed = {"requested": ["built_in"] if esphome_was_running else [], "resumed": [], "already_running": [], "failed": []}
    if esphome_was_running:
        try:
            if esphome_home_module.is_running():
                esphome_resumed["already_running"].append("built_in")
            else:
                _start_builtin_esphome()
                esphome_resumed["resumed"].append("built_in")
        except Exception as exc:
            esphome_resumed["failed"].append({"key": "built_in", "error": str(exc)})
    resumed_portals = _resume_surface_keys(portal_runtime, portal_keys)

    return {
        "cores": resumed_cores,
        "esphome": esphome_resumed,
        "portals": resumed_portals,
    }


def _wait_for_surface_failures_to_stop(
    failures: List[str],
    *,
    timeout_seconds: float,
    poll_interval_seconds: float = 0.25,
) -> Dict[str, List[str]]:
    pending = [str(item).strip() for item in (failures or []) if str(item).strip()]
    if not pending or timeout_seconds <= 0:
        return {"resolved": [], "pending": pending}

    deadline = time.time() + float(timeout_seconds)
    resolved: List[str] = []
    unresolved = list(pending)

    while unresolved and time.time() < deadline:
        still_pending: List[str] = []
        for item in unresolved:
            kind, sep, key = item.partition(":")
            runtime: Optional[SurfaceRuntimeManager] = None
            if sep and kind == "core":
                runtime = core_runtime
            elif sep and kind == "portal":
                runtime = portal_runtime

            if sep and kind == "esphome":
                if esphome_home_module.is_running():
                    still_pending.append(item)
                else:
                    resolved.append(item)
                continue

            if runtime is None or not key:
                still_pending.append(item)
                continue
            if runtime.is_running(key):
                still_pending.append(item)
            else:
                resolved.append(item)

        unresolved = still_pending
        if unresolved:
            time.sleep(max(0.05, float(poll_interval_seconds)))

    return {
        "resolved": sorted(set(resolved)),
        "pending": unresolved,
    }


def _run_redis_maintenance_with_runtime_pause(
    *,
    action: str,
    operation: Callable[[], Dict[str, Any]],
    stop_timeout: float = 6.0,
) -> Dict[str, Any]:
    effective_stop_timeout = _read_positive_float_env(
        "REDIS_MAINTENANCE_STOP_TIMEOUT_SECONDS",
        stop_timeout,
    )
    effective_late_grace_seconds = _read_positive_float_env(
        "REDIS_MAINTENANCE_LATE_GRACE_SECONDS",
        8.0,
    )
    effective_final_wait_seconds = _read_positive_float_env(
        "REDIS_MAINTENANCE_FINAL_WAIT_SECONDS",
        60.0,
    )
    with redis_maintenance_lock:
        quiesce_snapshot = _quiesce_surfaces_for_redis_maintenance(
            action=action,
            stop_timeout=effective_stop_timeout,
            late_grace_seconds=effective_late_grace_seconds,
        )
        stop_failures = [str(item).strip() for item in (quiesce_snapshot.get("stop_failures") or []) if str(item).strip()]
        late_wait_report = {"resolved": [], "pending": stop_failures}
        if stop_failures:
            late_wait_report = _wait_for_surface_failures_to_stop(
                stop_failures,
                timeout_seconds=effective_final_wait_seconds,
            )
            resolved_late = [str(item).strip() for item in (late_wait_report.get("resolved") or []) if str(item).strip()]
            if resolved_late:
                logger.info(
                    "[%s] runtimes stopped during final wait window: %s",
                    action,
                    ", ".join(resolved_late),
                )
            stop_failures = [str(item).strip() for item in (late_wait_report.get("pending") or []) if str(item).strip()]

        if stop_failures:
            _resume_surfaces_after_redis_maintenance(quiesce_snapshot)
            logger.warning(
                "[%s] aborting Redis maintenance; failed to pause runtimes: %s",
                action,
                ", ".join(stop_failures),
            )
            raise RuntimeError(
                "Failed to pause running cores/portals before Redis maintenance: "
                + ", ".join(stop_failures)
                + ". Retry after stopping those runtimes."
            )

        operation_payload: Dict[str, Any] = {}
        try:
            raw_payload = operation()
            operation_payload = raw_payload if isinstance(raw_payload, dict) else {"result": raw_payload}
        finally:
            resume_report = _resume_surfaces_after_redis_maintenance(quiesce_snapshot)

        resume_failures = []
        for row in (resume_report.get("cores", {}).get("failed") or []):
            key = str(row.get("key") or "").strip()
            if key:
                resume_failures.append(f"core:{key}")
        for row in (resume_report.get("portals", {}).get("failed") or []):
            key = str(row.get("key") or "").strip()
            if key:
                resume_failures.append(f"portal:{key}")
        if resume_failures:
            logger.warning("[%s] failed to resume some runtimes: %s", action, ", ".join(resume_failures))

        operation_payload["runtime_quiesce"] = {
            "action": action,
            "stop_timeout_seconds": effective_stop_timeout,
            "late_grace_seconds": effective_late_grace_seconds,
            "final_wait_seconds": effective_final_wait_seconds,
            "active_before": quiesce_snapshot.get("active_before", {}),
            "stopped": quiesce_snapshot.get("stopped", {}),
            "stop_failures": stop_failures,
            "late_wait_resolved": late_wait_report.get("resolved", []),
            "resumed": resume_report,
        }
        return operation_payload


class PluginToggleRequest(BaseModel):
    enabled: bool


class SettingsUpdateRequest(BaseModel):
    values: Dict[str, Any] = Field(default_factory=dict)


class ChatAttachmentRequest(BaseModel):
    name: Optional[str] = None
    mimetype: Optional[str] = None
    data_url: Optional[str] = None
    data_b64: Optional[str] = None


class ChatRequest(BaseModel):
    message: Optional[str] = None
    username: Optional[str] = None
    session_id: Optional[str] = None
    attachments: List[ChatAttachmentRequest] = Field(default_factory=list)


class OpenAIChatCompletionRequest(BaseModel):
    model: Optional[str] = None
    messages: List[Dict[str, Any]] = Field(default_factory=list)
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    stop: Optional[Any] = None
    stream: Optional[bool] = None
    user: Optional[str] = None
    user_name: Optional[str] = None
    device_name: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SpudLinkTaterChatRequest(BaseModel):
    message: Optional[Any] = None
    history: List[Dict[str, Any]] = Field(default_factory=list)
    attachments: List[Dict[str, Any]] = Field(default_factory=list)
    user: Optional[str] = None
    user_name: Optional[str] = None
    device_name: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SpudLinkPairRequest(BaseModel):
    pairing_code: str = Field(min_length=1)
    role: Optional[str] = None
    node_name: Optional[str] = None
    public_url: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SpudLinkConnectRequest(BaseModel):
    hub_url: str = Field(min_length=1)
    pairing_code: str = Field(min_length=1)
    role: Optional[str] = None
    node_name: Optional[str] = None
    public_url: Optional[str] = None


class SpudLinkHeartbeatRequest(BaseModel):
    node_name: Optional[str] = None
    public_url: Optional[str] = None
    mode: Optional[str] = None
    version: Optional[str] = None
    stats: Dict[str, Any] = Field(default_factory=dict)
    activity: Dict[str, Any] = Field(default_factory=dict)


class SpudLinkTtsRequest(BaseModel):
    text: Optional[str] = None


class SpudLinkSttRequest(BaseModel):
    audio_base64: Optional[str] = None
    content_type: Optional[str] = None
    language: Optional[str] = None


class SpudLinkRevokeNodeRequest(BaseModel):
    node_id: Optional[str] = None


class ShopItemRequest(BaseModel):
    id: str = Field(min_length=1)


class ShopRemoveRequest(BaseModel):
    id: str = Field(min_length=1)
    purge_redis: bool = False


class ShopReposRequest(BaseModel):
    repos: List[Dict[str, Any]] = Field(default_factory=list)


class CoreTabActionRequest(BaseModel):
    action: str = Field(min_length=1)
    payload: Dict[str, Any] = Field(default_factory=dict)


class DashboardBriefRefreshRequest(BaseModel):
    brief_id: Optional[str] = None


class DashboardSettingsRequest(BaseModel):
    personal_person_id: Optional[str] = None
    refresh_interval_seconds: Optional[int] = None
    brief_refresh_interval_seconds: Optional[int] = None


class SpudexSettingsRequest(BaseModel):
    values: Dict[str, Any] = Field(default_factory=dict)


class SpudexRunRequest(BaseModel):
    command: Optional[str] = None
    argv: List[str] = Field(default_factory=list)
    label: Optional[str] = None
    background: bool = False


class SpudexChatRequest(BaseModel):
    message: str = Field(min_length=1)
    session_id: Optional[str] = None


class SpudexChatSessionRequest(BaseModel):
    label: Optional[str] = None


class SpudexFileChangeRequest(BaseModel):
    change_id: str = Field(min_length=1)


class PeopleActionRequest(BaseModel):
    action: str = Field(min_length=1)
    payload: Dict[str, Any] = Field(default_factory=dict)


class OpenWakeWordTrainerModelsRequest(BaseModel):
    trainer_url: Optional[str] = None
    framework: Optional[str] = None


class OpenWakeWordTrainerDownloadRequest(BaseModel):
    trainer_url: Optional[str] = None
    artifact_url: Optional[str] = None
    framework: Optional[str] = None


class NanoWakeWordTrainerModelsRequest(BaseModel):
    trainer_url: Optional[str] = None


class NanoWakeWordTrainerDownloadRequest(BaseModel):
    trainer_url: Optional[str] = None
    artifact_url: Optional[str] = None


class HfModelDownloadRequest(BaseModel):
    provider: Optional[str] = None
    repo_id: Optional[str] = None
    model_id: Optional[str] = None
    filename: Optional[str] = None
    task: Optional[str] = None


class LocalLlmModelDeleteRequest(BaseModel):
    provider: Optional[str] = None
    model: Optional[str] = None


class LlamaCppChatTemplateRequest(BaseModel):
    provider: Optional[str] = None
    model: Optional[str] = None
    template: Optional[str] = None
    reset: Optional[bool] = None


class HfLlmWarmupCancelRequest(BaseModel):
    key: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    cancel_all: Optional[bool] = None


class LocalLlmUnloadRequest(BaseModel):
    provider: Optional[str] = None
    model: Optional[str] = None
    cache_key: Optional[str] = None
    unload_all: Optional[bool] = None


class AppSettingsRequest(BaseModel):
    username: Optional[str] = None
    user_avatar: Optional[str] = None
    tater_avatar: Optional[str] = None
    clear_user_avatar: Optional[bool] = None
    clear_tater_avatar: Optional[bool] = None
    max_display: Optional[int] = None
    show_speed_stats: Optional[bool] = None
    tater_first_name: Optional[str] = None
    tater_last_name: Optional[str] = None
    tater_personality: Optional[str] = None
    max_store: Optional[int] = None
    max_llm: Optional[int] = None
    homeassistant_base_url: Optional[str] = None
    homeassistant_token: Optional[str] = None
    hue_bridge_host: Optional[str] = None
    hue_app_key: Optional[str] = None
    hue_device_type: Optional[str] = None
    hue_timeout_seconds: Optional[int] = None
    aladdin_username: Optional[str] = None
    aladdin_password: Optional[str] = None
    aladdin_timeout_seconds: Optional[int] = None
    sonos_enabled: Optional[bool] = None
    sonos_discovery_timeout_seconds: Optional[int] = None
    sonos_speaker_hosts: Optional[str] = None
    unifi_network_base_url: Optional[str] = None
    unifi_network_api_key: Optional[str] = None
    unifi_protect_base_url: Optional[str] = None
    unifi_protect_api_key: Optional[str] = None
    vision_api_base: Optional[str] = None
    vision_mode: Optional[str] = None
    vision_provider: Optional[str] = None
    vision_model: Optional[str] = None
    vision_api_key: Optional[str] = None
    speech_stt_backend: Optional[str] = None
    speech_acceleration: Optional[str] = None
    speech_wyoming_stt_host: Optional[str] = None
    speech_wyoming_stt_port: Optional[str] = None
    speech_tts_backend: Optional[str] = None
    speech_tts_model: Optional[str] = None
    speech_tts_voice: Optional[str] = None
    speech_kokoro_output_gain: Optional[float] = None
    speech_pocket_tts_output_gain: Optional[float] = None
    speech_wyoming_tts_host: Optional[str] = None
    speech_wyoming_tts_port: Optional[str] = None
    speech_wyoming_tts_voice: Optional[str] = None
    speech_openai_tts_base_url: Optional[str] = None
    speech_openai_tts_api_key: Optional[str] = None
    speech_announcement_tts_backend: Optional[str] = None
    speech_announcement_tts_model: Optional[str] = None
    speech_announcement_tts_voice: Optional[str] = None
    speech_announcement_wyoming_tts_host: Optional[str] = None
    speech_announcement_wyoming_tts_port: Optional[str] = None
    speech_announcement_wyoming_tts_voice: Optional[str] = None
    speech_announcement_openai_tts_base_url: Optional[str] = None
    speech_announcement_openai_tts_api_key: Optional[str] = None
    esphome_settings: Optional[Dict[str, Any]] = None
    emoji_enable_on_reaction_add: Optional[bool] = None
    emoji_enable_auto_reaction_on_reply: Optional[bool] = None
    emoji_reaction_chain_chance_percent: Optional[int] = None
    emoji_reply_reaction_chance_percent: Optional[int] = None
    emoji_reaction_chain_cooldown_seconds: Optional[int] = None
    emoji_reply_reaction_cooldown_seconds: Optional[int] = None
    emoji_min_message_length: Optional[int] = None
    hydra_llm_host: Optional[str] = None
    hydra_llm_port: Optional[str] = None
    hydra_llm_model: Optional[str] = None
    hydra_llm_api_key: Optional[str] = None
    hydra_llm_provider: Optional[str] = None
    hydra_hf_transformers_context_tokens: Optional[Any] = None
    hydra_hf_transformers_device: Optional[str] = None
    hydra_hf_transformers_dtype: Optional[str] = None
    hydra_hf_transformers_device_map: Optional[str] = None
    hydra_hf_transformers_attn_implementation: Optional[str] = None
    hydra_hf_transformers_trust_remote_code: Optional[bool] = None
    hydra_llama_cpp_context_tokens: Optional[Any] = None
    hydra_llama_cpp_vision_context_tokens: Optional[Any] = None
    hydra_llama_cpp_mtp_enabled: Optional[bool] = None
    hydra_llama_cpp_mtp_draft_tokens: Optional[Any] = None
    hydra_llama_cpp_mtp_draft_model: Optional[str] = None
    hydra_llama_cpp_n_batch: Optional[Any] = None
    hydra_llama_cpp_n_ubatch: Optional[Any] = None
    hydra_llama_cpp_flash_attn: Optional[bool] = None
    hydra_llama_cpp_offload_kqv: Optional[bool] = None
    hydra_mlx_lm_context_tokens: Optional[Any] = None
    hydra_mlx_lm_trust_remote_code: Optional[bool] = None
    hydra_mlx_lm_lazy_load: Optional[bool] = None
    hydra_mlx_engine_prefill_step_size: Optional[Any] = None
    hydra_mlx_engine_kv_bits: Optional[Any] = None
    hydra_mlx_engine_kv_group_size: Optional[Any] = None
    hydra_mlx_engine_quantized_kv_start: Optional[Any] = None
    spudex_llm_provider: Optional[str] = None
    spudex_llm_host: Optional[str] = None
    spudex_llm_model: Optional[str] = None
    hydra_base_servers: Optional[List[Dict[str, Any]]] = None
    hydra_local_model_load_targets: Optional[List[Dict[str, Any]]] = None
    hydra_beast_mode_enabled: Optional[bool] = None
    hydra_llm_chat_provider: Optional[str] = None
    hydra_llm_chat_host: Optional[str] = None
    hydra_llm_chat_port: Optional[str] = None
    hydra_llm_chat_model: Optional[str] = None
    hydra_llm_chat_api_key: Optional[str] = None
    hydra_llm_astraeus_provider: Optional[str] = None
    hydra_llm_astraeus_host: Optional[str] = None
    hydra_llm_astraeus_port: Optional[str] = None
    hydra_llm_astraeus_model: Optional[str] = None
    hydra_llm_astraeus_api_key: Optional[str] = None
    hydra_llm_thanatos_provider: Optional[str] = None
    hydra_llm_thanatos_host: Optional[str] = None
    hydra_llm_thanatos_port: Optional[str] = None
    hydra_llm_thanatos_model: Optional[str] = None
    hydra_llm_thanatos_api_key: Optional[str] = None
    hydra_llm_minos_provider: Optional[str] = None
    hydra_llm_minos_host: Optional[str] = None
    hydra_llm_minos_port: Optional[str] = None
    hydra_llm_minos_model: Optional[str] = None
    hydra_llm_minos_api_key: Optional[str] = None
    hydra_llm_hermes_provider: Optional[str] = None
    hydra_llm_hermes_host: Optional[str] = None
    hydra_llm_hermes_port: Optional[str] = None
    hydra_llm_hermes_model: Optional[str] = None
    hydra_llm_hermes_api_key: Optional[str] = None
    hydra_max_ledger_items: Optional[int] = None
    hydra_astraeus_plan_review_enabled: Optional[bool] = None
    hydra_auto_continue_incomplete_final_enabled: Optional[bool] = None
    popup_effect_style: Optional[str] = None
    admin_only_plugins: Optional[List[str]] = None
    tater_api_enabled: Optional[bool] = None
    tater_api_key: Optional[str] = None
    clear_tater_api_key: Optional[bool] = None
    tater_api_mode: Optional[str] = None
    tater_api_hydra_tools_enabled: Optional[bool] = None
    spud_link_mode: Optional[str] = None
    spud_link_node_name: Optional[str] = None
    spud_link_public_url: Optional[str] = None
    spud_link_pairing_enabled: Optional[bool] = None
    spud_link_allow_spudlets: Optional[bool] = None
    spud_link_allow_little_spuds: Optional[bool] = None
    spud_link_little_spud_tools_enabled: Optional[bool] = None
    spud_link_telemetry_enabled: Optional[bool] = None
    spud_link_request_previews_enabled: Optional[bool] = None
    spud_link_hub_url: Optional[str] = None
    spud_link_node_token: Optional[str] = None
    clear_spud_link_node_token: Optional[bool] = None
    webui_password: Optional[str] = None
    webui_password_confirm: Optional[str] = None
    clear_webui_password: Optional[bool] = None


class HueLinkRequest(BaseModel):
    hue_bridge_host: Optional[str] = None
    hue_device_type: Optional[str] = None
    hue_timeout_seconds: Optional[int] = None


class AladdinTestRequest(BaseModel):
    aladdin_username: Optional[str] = None
    aladdin_password: Optional[str] = None
    aladdin_timeout_seconds: Optional[int] = None


class IntegrationSettingsRequest(BaseModel):
    settings: Dict[str, Any] = Field(default_factory=dict)


class IntegrationActionRequest(BaseModel):
    payload: Dict[str, Any] = Field(default_factory=dict)


class SpeechTtsPreviewRequest(BaseModel):
    backend: Optional[str] = None
    model: Optional[str] = None
    voice: Optional[str] = None
    kokoro_output_gain: Optional[float] = None
    pocket_tts_output_gain: Optional[float] = None
    acceleration: Optional[str] = None
    wyoming_host: Optional[str] = None
    wyoming_port: Optional[str] = None
    wyoming_voice: Optional[str] = None
    openai_base_url: Optional[str] = None
    openai_api_key: Optional[str] = None
    text: Optional[str] = None


class WyomingTtsVoicesRequest(BaseModel):
    host: Optional[str] = None
    port: Optional[str] = None
    current_voice: Optional[str] = None


class OpenAiCompatibleTtsVoicesRequest(BaseModel):
    base_url: Optional[str] = None
    api_key: Optional[str] = None


class OpenAiCompatibleTtsModelsRequest(BaseModel):
    base_url: Optional[str] = None
    api_key: Optional[str] = None


class RedisSetupRequest(BaseModel):
    mode: Optional[str] = "internal"
    host: Optional[str] = ""
    port: Optional[int] = 6379
    db: Optional[int] = 0
    username: Optional[str] = ""
    password: Optional[str] = ""
    use_tls: Optional[bool] = False
    verify_tls: Optional[bool] = True
    ca_cert_path: Optional[str] = ""
    data_path: Optional[str] = ""
    keep_existing_password: Optional[bool] = False
    test_only: Optional[bool] = False


class RedisMigrateInternalRequest(BaseModel):
    data_path: Optional[str] = ""
    flush_internal: Optional[bool] = True


class HydraDataClearRequest(BaseModel):
    platform: str = "all"
    mode: str = "all"


class WebUiAuthSetupRequest(BaseModel):
    password: Optional[str] = ""
    confirm_password: Optional[str] = ""


class WebUiAuthLoginRequest(BaseModel):
    password: Optional[str] = ""


app = FastAPI(title="TaterOS", version="0.2.0")


_SPUD_LINK_EXTERNAL_API_PATHS = {
    "/api/spudlink/status",
    "/api/spudlink/pair",
    "/api/spudlink/heartbeat",
}
_SPUD_LINK_EXTERNAL_API_PREFIXES = (
    "/api/spudlink/v1/",
)


def _is_spud_link_path(path: str) -> bool:
    return path == "/api/spudlink" or path.startswith("/api/spudlink/")


def _is_spud_link_external_api_path(path: str) -> bool:
    clean_path = path.rstrip("/") or "/"
    if clean_path in _SPUD_LINK_EXTERNAL_API_PATHS:
        return True
    return any(clean_path.startswith(prefix) for prefix in _SPUD_LINK_EXTERNAL_API_PREFIXES)


@app.middleware("http")
async def _spud_link_cors_middleware(request: Request, call_next: Callable[[Request], Any]) -> Response:
    if not _is_spud_link_path(str(request.url.path or "")):
        return await call_next(request)

    if request.method.upper() == "OPTIONS":
        response = Response(status_code=204)
    else:
        response = await call_next(request)

    origin = str(request.headers.get("origin") or "").strip()
    if origin:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = str(
        request.headers.get("access-control-request-headers")
        or "authorization, content-type, accept, x-spudlink-device, x-spudlink-token, x-spudlink-user"
    )
    response.headers["Access-Control-Allow-Private-Network"] = "true"
    response.headers["Access-Control-Max-Age"] = "600"
    return response


esphome_home_module.include_routes(app)

STATIC_DIR = Path(__file__).resolve().parent / "tateros_static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.on_event("startup")
async def _startup_event() -> None:
    restore_enabled = str(os.getenv("HTMLUI_RESTORE_ENABLED_SURFACES_ON_STARTUP", "true")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    autostart_enabled = str(os.getenv("HTMLUI_AUTOSTART_ENABLED_SURFACES_ON_STARTUP", "true")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    bootstrap_state["restore_enabled"] = restore_enabled
    bootstrap_state["autostart_enabled"] = autostart_enabled
    bootstrap_state["restore_in_progress"] = False
    bootstrap_state["restore_complete"] = False
    bootstrap_state["restore_error"] = ""
    bootstrap_state["restore_summary"] = {}

    redis_ready, redis_error = _redis_reachable_for_startup()
    if not redis_ready:
        bootstrap_state["restore_error"] = redis_error or "Redis is unavailable."
        bootstrap_state["restore_in_progress"] = False
        bootstrap_state["restore_complete"] = True
        logger.warning("Redis unavailable during startup bootstrap: %s", bootstrap_state["restore_error"])
        logger.info("TaterOS backend started")
        return

    set_main_loop(asyncio.get_running_loop())
    bind_integration_runtime_loop()
    verba_registry_module.ensure_verbas_loaded()

    try:
        logger.info("[startup] runtime executor settings applied: %s", configure_runtime_executors())
        if restore_enabled:
            bootstrap_state["restore_in_progress"] = True
            summary = _restore_enabled_surfaces()
            bootstrap_state["restore_summary"] = summary
            logger.info("[startup-restore] summary: %s", summary)
        else:
            logger.info("[startup-restore] skipped (HTMLUI_RESTORE_ENABLED_SURFACES_ON_STARTUP=false)")
        start_integration_runtime(redis_client)
        local_warmup = _start_local_llm_warmup_for_startup(reason="startup")
        logger.info("[local-llm-warmup] startup scheduled: %s", local_warmup)
        await esphome_home_module.startup()
        if _speech_model_warmup_on_startup_enabled():
            warmup = _start_speech_model_warmup(get_shared_speech_settings(), reason="startup")
            warmup = _wait_for_speech_model_warmup()
            logger.info("[speech-warmup] startup scheduled: %s", warmup)
        else:
            logger.info("[speech-warmup] startup warmup skipped (TATER_SPEECH_WARMUP_ON_STARTUP=false)")
        if autostart_enabled:
            _autostart_enabled_surfaces()
        else:
            logger.info("[startup-autostart] skipped (HTMLUI_AUTOSTART_ENABLED_SURFACES_ON_STARTUP=false)")
        _start_dashboard_brief_scheduler()
    except RedisError as exc:
        bootstrap_state["restore_error"] = str(exc)
        logger.warning("Redis unavailable during startup autostart: %s", exc)
    finally:
        bootstrap_state["restore_in_progress"] = False
        bootstrap_state["restore_complete"] = True
    logger.info("TaterOS backend started")


async def _run_shutdown_step(name: str, func: Callable[[], Any], *, timeout: float = 10.0) -> bool:
    try:
        result = func()
        if inspect.isawaitable(result):
            if timeout and timeout > 0:
                await asyncio.wait_for(result, timeout=float(timeout))
            else:
                await result
        return True
    except asyncio.TimeoutError:
        logger.warning("[shutdown] %s timed out after %.1fs", name, float(timeout or 0))
    except asyncio.CancelledError:
        logger.info("[shutdown] %s cancelled during shutdown", name)
    except Exception as exc:
        logger.warning("[shutdown] %s failed: %s", name, exc, exc_info=True)
    return False


@app.on_event("shutdown")
async def _shutdown_event() -> None:
    logger.info("TaterOS backend stopping")
    await _run_shutdown_step("dashboard brief scheduler", _stop_dashboard_brief_scheduler, timeout=5.0)
    await _run_shutdown_step("core runtime", lambda: core_runtime.stop_all(timeout=8.0), timeout=10.0)
    await _run_shutdown_step("ESPHome voice runtime", esphome_home_module.shutdown, timeout=10.0)
    await _run_shutdown_step("portal runtime", lambda: portal_runtime.stop_all(timeout=8.0), timeout=10.0)
    await _run_shutdown_step("integration runtime", stop_integration_runtime, timeout=10.0)
    await _run_shutdown_step("runtime executors", lambda: shutdown_runtime_executors(wait=False, cancel_futures=True), timeout=5.0)
    await _run_shutdown_step("internal Redis", shutdown_internal_redis, timeout=5.0)
    logger.info("TaterOS backend stopped")


@app.exception_handler(RedisError)
async def _redis_error_handler(_request: Request, exc: RedisError):
    return JSONResponse(
        status_code=503,
        content={"detail": f"Redis unavailable: {exc}"},
    )


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.middleware("http")
async def _webui_auth_middleware(request: Request, call_next):
    path = str(request.url.path or "")
    if request.method.upper() == "OPTIONS" and _is_spud_link_path(path):
        return await call_next(request)
    if _is_spud_link_external_api_path(path):
        return await call_next(request)
    if path.startswith("/api/speech/tts/runtime/"):
        return await call_next(request)
    path_parts = [part for part in path.strip("/").split("/") if part]
    if len(path_parts) == 5 and path_parts[0] == "api" and path_parts[1] == "cores" and path_parts[3] == "webhook":
        return await call_next(request)
    if len(path_parts) >= 4 and path_parts[0] == "api" and path_parts[1] == "portals" and path_parts[3] in {"api", "webhook"}:
        return await call_next(request)
    if not path.startswith("/api/") or path.startswith("/api/auth/"):
        return await call_next(request)

    try:
        password_set = _webui_password_is_set()
    except RedisError:
        # Let existing Redis error handling/reporting flows run when Redis is unavailable.
        return await call_next(request)
    except Exception:
        password_set = False

    if not password_set:
        return await call_next(request)

    token = request.cookies.get(WEBUI_AUTH_COOKIE_NAME)
    try:
        authenticated = _webui_session_is_valid(token)
    except RedisError:
        return await call_next(request)
    except Exception:
        authenticated = False

    if not authenticated:
        return JSONResponse(
            status_code=401,
            content={"detail": "WebUI login required.", "code": "webui_login_required"},
        )

    return await call_next(request)


@app.get("/api/auth/status")
def webui_auth_status(
    webui_session: Optional[str] = Cookie(default=None, alias=WEBUI_AUTH_COOKIE_NAME),
) -> Dict[str, Any]:
    authenticated = False
    if _webui_password_is_set():
        authenticated = _webui_session_is_valid(webui_session)
    return _webui_auth_profile_payload(authenticated=authenticated)


@app.post("/api/auth/setup")
def webui_auth_setup(payload: WebUiAuthSetupRequest, response: Response) -> Dict[str, Any]:
    if _webui_password_is_set():
        raise HTTPException(status_code=409, detail="WebUI password is already configured.")

    password = str(payload.password or "")
    confirm_password = str(payload.confirm_password or "")
    if len(password) < int(WEBUI_AUTH_PASSWORD_MIN_LENGTH):
        raise HTTPException(
            status_code=400,
            detail=f"Password must be at least {WEBUI_AUTH_PASSWORD_MIN_LENGTH} characters.",
        )
    if password != confirm_password:
        raise HTTPException(status_code=400, detail="Passwords do not match.")

    salt = secrets.token_bytes(16)
    password_hash = _hash_webui_password(password, salt=salt)
    redis_client.set(WEBUI_AUTH_PASSWORD_HASH_KEY, password_hash)
    _clear_webui_sessions()

    session_token = _new_webui_session_token()
    _store_webui_session(session_token)
    _issue_webui_auth_cookie(response, session_token)
    return _webui_auth_profile_payload(authenticated=True)


@app.post("/api/auth/login")
def webui_auth_login(payload: WebUiAuthLoginRequest, response: Response) -> Dict[str, Any]:
    stored_hash = _load_webui_password_hash()
    if not stored_hash:
        raise HTTPException(status_code=409, detail="WebUI password has not been configured yet.")

    password = str(payload.password or "")
    if not _verify_webui_password(password, stored_hash):
        raise HTTPException(status_code=401, detail="Invalid password.")

    session_token = _new_webui_session_token()
    _store_webui_session(session_token)
    _issue_webui_auth_cookie(response, session_token)
    return _webui_auth_profile_payload(authenticated=True)


@app.post("/api/auth/logout")
def webui_auth_logout(
    response: Response,
    webui_session: Optional[str] = Cookie(default=None, alias=WEBUI_AUTH_COOKIE_NAME),
) -> Dict[str, Any]:
    token = str(webui_session or "").strip()
    if token:
        redis_client.hdel(WEBUI_AUTH_SESSIONS_KEY, _webui_session_digest(token))
    _clear_webui_auth_cookie(response)
    return _webui_auth_profile_payload(authenticated=False)


_RUNTIME_PLATFORM_LABELS: Dict[str, str] = {
    "webui": "WebUI",
    "macos": "macOS",
    "discord": "Discord",
    "irc": "IRC",
    "telegram": "Telegram",
    "matrix": "Matrix",
    "homeassistant": "Home Assistant",
    "voice_core": "Voice Core",
    "little_spud": "Little Spud",
    "homekit": "HomeKit",
    "xbmc": "XBMC",
    "automation": "Automation",
}


def _runtime_platform_label(platform: Any) -> str:
    token = normalize_platform(str(platform or ""))
    if token in _RUNTIME_PLATFORM_LABELS:
        return _RUNTIME_PLATFORM_LABELS[token]
    parts = [part for part in token.replace("-", "_").split("_") if part]
    return " ".join(part.capitalize() for part in parts) if parts else "Unknown"


def _load_chat_job_history_rows(max_items: int = 5000) -> List[Dict[str, Any]]:
    max_rows = max(200, min(int(max_items or 0), 20000))
    try:
        keys = sorted(str(k) for k in redis_client.scan_iter(match="tater:hydra:ledger:*"))
    except Exception:
        keys = []

    rows: List[Dict[str, Any]] = []
    seen_ids = set()
    for key in keys:
        try:
            raw_items = redis_client.lrange(key, -max_rows, -1) or []
        except Exception:
            raw_items = []
        for raw in raw_items:
            try:
                item = json.loads(raw)
            except Exception:
                continue
            if not isinstance(item, dict):
                continue
            turn_id = str(item.get("turn_id") or "").strip()
            if turn_id and turn_id in seen_ids:
                continue
            if turn_id:
                seen_ids.add(turn_id)
            rows.append(dict(item))
    rows.sort(key=lambda row: float(row.get("timestamp") or 0.0), reverse=True)
    return rows


def _chat_job_history_windows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
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
            "jobs": 0,
            "done": 0,
            "blocked": 0,
            "failed": 0,
            "tool_turns": 0,
            "platform_counts": {},
        }
        for key, label, _ in windows
    }

    for row in rows:
        ts = float(row.get("timestamp") or 0.0)
        if ts <= 0:
            continue
        age = max(0.0, now - ts)
        outcome = str(row.get("outcome") or "").strip().lower()
        platform = normalize_platform(row.get("platform")) or "unknown"
        planned_tool = row.get("planned_tool") if isinstance(row.get("planned_tool"), dict) else {}
        has_tool = bool(str(planned_tool.get("function") or "").strip())

        for key, _label, seconds in windows:
            if age > float(seconds):
                continue
            bucket = buckets[key]
            bucket["jobs"] = int(bucket.get("jobs") or 0) + 1
            if outcome == "done":
                bucket["done"] = int(bucket.get("done") or 0) + 1
            elif outcome == "blocked":
                bucket["blocked"] = int(bucket.get("blocked") or 0) + 1
            elif outcome == "failed":
                bucket["failed"] = int(bucket.get("failed") or 0) + 1
            if has_tool:
                bucket["tool_turns"] = int(bucket.get("tool_turns") or 0) + 1
            platform_counts = bucket.get("platform_counts")
            if isinstance(platform_counts, dict):
                platform_counts[platform] = int(platform_counts.get(platform) or 0) + 1

    window_rows: List[Dict[str, Any]] = []
    for key, label, _seconds in windows:
        bucket = buckets.get(key) or {"key": key, "label": label}
        platform_counts = bucket.get("platform_counts") if isinstance(bucket.get("platform_counts"), dict) else {}
        top_platforms = [
            {
                "platform": platform,
                "label": _runtime_platform_label(platform),
                "jobs": int(count),
            }
            for platform, count in platform_counts.items()
        ]
        top_platforms.sort(key=lambda row: (-int(row.get("jobs") or 0), str(row.get("label") or "")))
        window_rows.append(
            {
                "key": key,
                "label": label,
                "jobs": int(bucket.get("jobs") or 0),
                "done": int(bucket.get("done") or 0),
                "blocked": int(bucket.get("blocked") or 0),
                "failed": int(bucket.get("failed") or 0),
                "tool_turns": int(bucket.get("tool_turns") or 0),
                "top_platforms": top_platforms[:4],
            }
        )

    return {
        "windows": window_rows,
        "sample_size": int(len(rows)),
    }


def _chat_job_counts_with_breakdown(*, include_history: bool = False) -> Dict[str, Any]:
    active_turn_rows = get_active_chat_jobs_snapshot()
    running_by_platform: Dict[str, int] = {}
    webui_running_turns = 0
    now = time.time()
    active_turns: List[Dict[str, Any]] = []

    for row in active_turn_rows:
        platform = normalize_platform(row.get("platform"))
        if not platform:
            platform = "unknown"
        running_by_platform[platform] = int(running_by_platform.get(platform, 0)) + 1
        if platform == "webui":
            webui_running_turns += 1

        started_at = float(row.get("started_at") or 0.0)
        age_seconds = max(0, int(now - started_at)) if started_at > 0 else 0
        active_turns.append(
            {
                "id": str(row.get("id") or "").strip(),
                "platform": platform,
                "platform_label": _runtime_platform_label(platform),
                "source": str(row.get("source") or platform).strip() or platform,
                "scope": str(row.get("scope") or "").strip(),
                "task_name": str(row.get("task_name") or "").strip(),
                "current_tool": str(row.get("current_tool") or "").strip(),
                "started_at": started_at,
                "age_seconds": age_seconds,
            }
        )

    active_turns.sort(key=lambda row: float(row.get("started_at") or 0.0))
    webui_jobs = int(chat_jobs.active_count())
    surface_running_turns = max(0, int(len(active_turn_rows)) - int(webui_running_turns))
    total = int(webui_jobs + surface_running_turns)

    by_platform = [
        {
            "platform": platform,
            "label": _runtime_platform_label(platform),
            "running_turns": int(count),
        }
        for platform, count in running_by_platform.items()
    ]
    by_platform.sort(key=lambda row: (-int(row.get("running_turns") or 0), str(row.get("label") or "")))

    out = {
        "total": total,
        "webui_jobs": webui_jobs,
        "webui_running_turns": int(webui_running_turns),
        "surface_running_turns": int(surface_running_turns),
        "by_platform": by_platform,
        "active_turns": active_turns,
    }
    if include_history:
        history_rows = _load_chat_job_history_rows()
        out["history"] = _chat_job_history_windows(history_rows)
    return out


def _latest_webui_user_name(history_rows: List[Dict[str, Any]]) -> str:
    for row in reversed(history_rows or []):
        role = str(row.get("role") or "").strip().lower()
        if role != "user":
            continue
        username = str(row.get("username") or "").strip()
        if username:
            return username

    settings = redis_client.hgetall("chat_settings") or {}
    fallback = str(settings.get("username") or "User").strip()
    return fallback or "User"


def _estimate_webui_chat_context_window(*, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.time()
    cached_at = float(runtime_context_estimate_cache.get("updated_at") or 0.0)
    cached_payload = runtime_context_estimate_cache.get("payload")
    if (
        not force_refresh
        and isinstance(cached_payload, dict)
        and cached_payload
        and (now - cached_at) < float(RUNTIME_CONTEXT_ESTIMATE_TTL_SECONDS)
    ):
        return dict(cached_payload)

    payload: Dict[str, Any] = {}
    try:
        max_llm = _read_positive_int("tater:max_llm", DEFAULT_MAX_LLM)
        history_tail = _load_chat_history_tail(max_llm)
        loop_messages = _loop_messages_from_history_rows(history_tail)
        user_name = _latest_webui_user_name(history_tail)

        verba_registry_module.ensure_verbas_loaded()
        merged_registry = dict(verba_registry_module.get_verba_registry() or {})

        payload = estimate_hydra_chat_context_window(
            platform="webui",
            history_messages=loop_messages,
            registry=merged_registry,
            enabled_predicate=get_verba_enabled,
            redis_client=redis_client,
            scope="session:webui:context_estimate",
            origin={
                "platform": "webui",
                "user": user_name,
                "user_id": user_name,
                "session_id": "context_estimate",
            },
            user_text="",
            platform_preamble="",
        )
        if isinstance(payload, dict):
            payload["max_history_messages"] = int(max_llm)
    except Exception as exc:
        payload = {"error": str(exc)}

    runtime_context_estimate_cache["updated_at"] = now
    runtime_context_estimate_cache["payload"] = dict(payload) if isinstance(payload, dict) else {}
    return dict(runtime_context_estimate_cache.get("payload") or {})


def _runtime_model_memory_kind_from_device(device: Any, fallback: str = "ram") -> str:
    token = str(device or "").strip().lower()
    if any(part in token for part in ("cuda", "gpu", "nvidia", "rocm", "hip")):
        return "vram"
    if any(part in token for part in ("mps", "metal", "apple", "unified")):
        return "unified"
    return str(fallback or "ram").strip().lower() or "ram"


def _runtime_path_bytes(*paths: Any) -> int:
    total = 0
    seen: set[str] = set()
    for path in paths:
        raw = str(path or "").strip()
        if not raw:
            continue
        try:
            token = str(Path(raw).expanduser().resolve())
        except Exception:
            token = raw
        if token in seen:
            continue
        seen.add(token)
        try:
            total += max(0, int(runtime_path_size_bytes(raw) or 0))
        except Exception:
            continue
    return max(0, int(total))


def _runtime_model_estimated_bytes(model_obj: Any = None, *paths: Any) -> int:
    try:
        memory = max(0, int(runtime_object_memory_footprint_bytes(model_obj) or 0))
        if memory > 0:
            return memory
    except Exception:
        pass
    return _runtime_path_bytes(*paths)


def _runtime_managed_model_row(
    *,
    category: str,
    kind_label: str,
    provider: str,
    provider_label: str,
    model: str,
    device: str = "",
    model_path: str = "",
    model_root: str = "",
    estimated_bytes: int = 0,
    memory_kind: str = "",
    warning: str = "",
    details: Optional[List[str]] = None,
    loaded_ts: float = 0.0,
) -> Dict[str, Any]:
    return {
        "cache_key": "",
        "category": str(category or "managed").strip() or "managed",
        "kind_label": str(kind_label or "Managed").strip() or "Managed",
        "provider": str(provider or category or "managed").strip(),
        "provider_label": str(provider_label or provider or "Managed").strip(),
        "model": str(model or "model").strip() or "model",
        "device": str(device or "").strip(),
        "memory_kind": str(memory_kind or _runtime_model_memory_kind_from_device(device)).strip().lower() or "ram",
        "estimated_bytes": max(0, int(estimated_bytes or 0)),
        "model_path": str(model_path or "").strip(),
        "model_root": str(model_root or "").strip(),
        "loaded_ts": float(loaded_ts or 0.0),
        "warning": str(warning or "").strip(),
        "managed": True,
        "unloadable": False,
        "managed_by": "Settings enable/disable",
        "details": [str(item).strip() for item in (details or []) if str(item or "").strip()],
    }


def _runtime_kokoro_model_label(key: Tuple[Any, ...]) -> Tuple[str, str, str, str]:
    parts = [str(item or "").strip() for item in key]
    if parts and parts[0] == "torch":
        repo = parts[1] if len(parts) > 1 else "Kokoro"
        lang = parts[2] if len(parts) > 2 else ""
        device = parts[3] if len(parts) > 3 else ""
        label = repo
        if lang:
            label = f"{label} ({lang})"
        return label, device, "torch", repo
    variant = parts[1] if len(parts) > 1 else "v1.0"
    quality = parts[2] if len(parts) > 2 else "q8"
    provider = parts[3] if len(parts) > 3 else ""
    return f"Kokoro {variant}:{quality}", provider, provider or "onnx", ""


def _runtime_tts_cache_rows(module: Any, *, scope_label: str, provider_prefix: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    with contextlib.suppress(Exception):
        lock = getattr(module, "_kokoro_pipeline_lock", None)
        cache = getattr(module, "_kokoro_pipeline_cache", {})
        with lock:
            kokoro_items = list(cache.items())
        for key, pipeline in kokoro_items:
            key_tuple = tuple(key if isinstance(key, tuple) else (key,))
            model_label, device, engine_label, repo_id = _runtime_kokoro_model_label(key_tuple)
            root_name = "kokoro_torch" if str(engine_label).lower() == "torch" else "kokoro"
            model_root = ""
            with contextlib.suppress(Exception):
                model_root = str(module._tts_backend_model_root(root_name))
            estimated = _runtime_model_estimated_bytes(pipeline, model_root)
            detail_parts = [f"Engine {engine_label}"]
            if repo_id:
                detail_parts.append(repo_id)
            rows.append(
                _runtime_managed_model_row(
                    category="tts",
                    kind_label="TTS",
                    provider=f"{provider_prefix}_kokoro",
                    provider_label=f"{scope_label} • Kokoro",
                    model=model_label,
                    device=device,
                    model_root=model_root,
                    estimated_bytes=estimated,
                    memory_kind=_runtime_model_memory_kind_from_device(device),
                    details=detail_parts,
                )
            )

    with contextlib.suppress(Exception):
        lock = getattr(module, "_pocket_tts_model_lock", None)
        cache = getattr(module, "_pocket_tts_model_cache", {})
        with lock:
            pocket_items = list(cache.items())
        model_root = ""
        with contextlib.suppress(Exception):
            model_root = str(module._tts_backend_model_root("pocket_tts"))
        for token, model_obj in pocket_items:
            model_token = str(token or "").strip() or str(getattr(module, "DEFAULT_POCKET_TTS_MODEL", "") or "PocketTTS")
            estimated = _runtime_model_estimated_bytes(model_obj, model_root)
            rows.append(
                _runtime_managed_model_row(
                    category="tts",
                    kind_label="TTS",
                    provider=f"{provider_prefix}_pocket_tts",
                    provider_label=f"{scope_label} • PocketTTS",
                    model=model_token,
                    model_root=model_root,
                    estimated_bytes=estimated,
                    memory_kind="ram",
                )
            )

    with contextlib.suppress(Exception):
        lock = getattr(module, "_piper_voice_lock", None)
        cache = getattr(module, "_piper_voice_cache", {})
        with lock:
            piper_items = list(cache.items())
        for model_path, voice_obj in piper_items:
            path = str(model_path or "").strip()
            config_path = f"{path}.json" if path and not path.endswith(".json") else ""
            model_label = Path(path).name.replace(".onnx", "") if path else "Piper"
            model_root = str(Path(path).parent) if path else ""
            estimated = _runtime_model_estimated_bytes(voice_obj, path, config_path)
            rows.append(
                _runtime_managed_model_row(
                    category="tts",
                    kind_label="TTS",
                    provider=f"{provider_prefix}_piper",
                    provider_label=f"{scope_label} • Piper",
                    model=model_label,
                    model_path=path,
                    model_root=model_root,
                    estimated_bytes=estimated,
                    memory_kind="ram",
                )
            )
    return rows


def _runtime_voice_pipeline_model_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        from tater_voice import voice_pipeline
    except Exception:
        return rows

    with contextlib.suppress(Exception):
        with voice_pipeline._faster_whisper_model_lock:
            faster_whisper_items = list(voice_pipeline._faster_whisper_model_cache.items())
        for key, model_obj in faster_whisper_items:
            key_tuple = tuple(key if isinstance(key, tuple) else (key,))
            model_source = str(key_tuple[0] if len(key_tuple) > 0 else "").strip()
            device = str(key_tuple[1] if len(key_tuple) > 1 else "").strip()
            compute_type = str(key_tuple[2] if len(key_tuple) > 2 else "").strip()
            model_path = model_source if model_source and os.path.exists(os.path.expanduser(model_source)) else ""
            estimated = _runtime_model_estimated_bytes(model_obj, model_path)
            rows.append(
                _runtime_managed_model_row(
                    category="stt",
                    kind_label="STT",
                    provider="voice_stt_faster_whisper",
                    provider_label="STT • Faster Whisper",
                    model=model_source or str(getattr(voice_pipeline, "DEFAULT_FASTER_WHISPER_MODEL", "") or "Faster Whisper"),
                    device=device,
                    model_path=model_path,
                    model_root=str(getattr(voice_pipeline, "_stt_backend_model_root")("faster_whisper")),
                    estimated_bytes=estimated,
                    memory_kind=_runtime_model_memory_kind_from_device(device),
                    details=[f"Compute {compute_type}"] if compute_type else [],
                )
            )

    with contextlib.suppress(Exception):
        with voice_pipeline._vosk_model_lock:
            vosk_items = list(voice_pipeline._vosk_model_cache.items())
        for model_path, model_obj in vosk_items:
            path = str(model_path or "").strip()
            estimated = _runtime_model_estimated_bytes(model_obj, path)
            rows.append(
                _runtime_managed_model_row(
                    category="stt",
                    kind_label="STT",
                    provider="voice_stt_vosk",
                    provider_label="STT • Vosk",
                    model=Path(path).name if path else "Vosk",
                    model_path=path,
                    model_root=str(getattr(voice_pipeline, "_stt_backend_model_root")("vosk")),
                    estimated_bytes=estimated,
                    memory_kind="ram",
                )
            )

    rows.extend(_runtime_tts_cache_rows(voice_pipeline, scope_label="Voice TTS", provider_prefix="voice_tts"))
    return rows


def _runtime_announcement_tts_model_rows() -> List[Dict[str, Any]]:
    try:
        import speech_tts as speech_tts_module
    except Exception:
        return []
    return _runtime_tts_cache_rows(speech_tts_module, scope_label="Announcement TTS", provider_prefix="announcement_tts")


def _runtime_speechbrain_engine_row(module: Any, *, category: str, kind_label: str, provider: str, provider_label: str) -> List[Dict[str, Any]]:
    try:
        lock = getattr(module, "_ENGINE_LOCK")
        with lock:
            engine = getattr(module, "_ENGINE", None)
            engine_source = str(getattr(module, "_ENGINE_SOURCE", "") or "").strip()
            engine_device = str(getattr(module, "_ENGINE_DEVICE", "") or "").strip()
            requested_device = str(getattr(module, "_ENGINE_REQUESTED_DEVICE", "") or "").strip()
    except Exception:
        return []
    if engine is None:
        return []

    availability: Dict[str, Any] = {}
    with contextlib.suppress(Exception):
        availability = module.runtime_availability()
    model_source = str(availability.get("model_source") or engine_source or provider_label).strip()
    model_dir = str(availability.get("model_dir") or "").strip()
    device = str(availability.get("device") or engine_device or requested_device or "").strip()
    detail = str(availability.get("detail") or "").strip()
    acceleration = str(availability.get("acceleration") or "").strip()
    estimated = _runtime_model_estimated_bytes(engine, model_dir)
    details = [f"Acceleration {acceleration}"] if acceleration else []
    return [
        _runtime_managed_model_row(
            category=category,
            kind_label=kind_label,
            provider=provider,
            provider_label=provider_label,
            model=model_source,
            device=device,
            model_path=model_dir,
            model_root=model_dir,
            estimated_bytes=estimated,
            memory_kind=_runtime_model_memory_kind_from_device(device),
            warning=detail,
            details=details,
        )
    ]


def _runtime_managed_voice_model_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    rows.extend(_runtime_voice_pipeline_model_rows())
    rows.extend(_runtime_announcement_tts_model_rows())
    try:
        from tater_voice import speaker_id as speaker_id_module

        rows.extend(
            _runtime_speechbrain_engine_row(
                speaker_id_module,
                category="speaker_id",
                kind_label="SpeakerID",
                provider="speaker_id_speechbrain",
                provider_label="SpeakerID • SpeechBrain",
            )
        )
    except Exception:
        pass
    try:
        from tater_voice import emotion_id as emotion_id_module

        rows.extend(
            _runtime_speechbrain_engine_row(
                emotion_id_module,
                category="emotion_id",
                kind_label="EmotionID",
                provider="emotion_id_speechbrain",
                provider_label="EmotionID • SpeechBrain",
            )
        )
    except Exception:
        pass
    rows.sort(key=lambda row: (str(row.get("kind_label") or ""), str(row.get("provider_label") or ""), str(row.get("model") or "")))
    return rows


def _runtime_loaded_models_snapshot(*, include_models: bool = True) -> Dict[str, Any]:
    llm_payload = get_local_llm_loaded_models_snapshot(include_models=True)
    llm_rows = [dict(row) for row in list(llm_payload.get("models") or []) if isinstance(row, dict)]
    for row in llm_rows:
        row.setdefault("category", "llm")
        row.setdefault("kind_label", "LLM")
        row.setdefault("managed", False)
        row.setdefault("unloadable", True)
        row.setdefault("managed_by", "")

    managed_rows = _runtime_managed_voice_model_rows()
    rows = [*llm_rows, *managed_rows]
    rows.sort(key=lambda row: (str(row.get("kind_label") or ""), str(row.get("provider_label") or ""), str(row.get("model") or "")))

    totals = {
        "estimated_ram_bytes": 0,
        "estimated_vram_bytes": 0,
        "estimated_unified_bytes": 0,
        "estimated_total_bytes": 0,
    }
    by_provider: Dict[str, int] = {}
    by_category: Dict[str, int] = {}
    unloadable_count = 0
    managed_count = 0
    for row in rows:
        provider = str(row.get("provider") or "").strip()
        category = str(row.get("category") or "managed").strip() or "managed"
        if provider:
            by_provider[provider] = by_provider.get(provider, 0) + 1
        by_category[category] = by_category.get(category, 0) + 1
        if bool(row.get("unloadable", False)):
            unloadable_count += 1
        if bool(row.get("managed", False)):
            managed_count += 1
        estimated = max(0, int(row.get("estimated_bytes") or 0))
        totals["estimated_total_bytes"] += estimated
        kind = str(row.get("memory_kind") or "ram").strip().lower()
        if kind == "vram":
            totals["estimated_vram_bytes"] += estimated
        elif kind == "unified":
            totals["estimated_unified_bytes"] += estimated
        else:
            totals["estimated_ram_bytes"] += estimated

    payload = {
        "loaded_count": len(rows),
        "local_llm_loaded_count": len(llm_rows),
        "managed_loaded_count": managed_count,
        "unloadable_count": unloadable_count,
        "by_provider": by_provider,
        "by_category": by_category,
        "totals": totals,
        "system": dict(llm_payload.get("system") or {}),
    }
    if include_models:
        payload["models"] = rows
    return payload


def _runtime_breakdown_payload() -> Dict[str, Any]:
    hydra_jobs = _chat_job_counts_with_breakdown(include_history=True)
    llm_calls = get_llm_call_runtime_summary(include_history=True)
    vision_calls = get_vision_call_runtime_summary(include_history=True)
    context_estimate = _estimate_webui_chat_context_window()
    loaded_models = _runtime_loaded_models_snapshot(include_models=True)
    return {
        "hydra_jobs": hydra_jobs,
        "chat_jobs": hydra_jobs,  # Backward-compatible key for older clients.
        "llm_calls": llm_calls,
        "voice_calls": vision_calls,  # Alias while voice runtime shares the vision-call tracker.
        "vision_calls": vision_calls,
        "chat_context_window": context_estimate,
        "loaded_models": loaded_models,
    }


def _dashboard_redis_text(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return str(value or "").strip()


def _dashboard_safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return int(default)


def _dashboard_interval_option(value: Any, *, allowed: Tuple[int, ...], default: int) -> int:
    parsed = _dashboard_safe_int(value, default)
    if parsed in allowed:
        return parsed
    return int(default)


def _dashboard_refresh_settings() -> Dict[str, Any]:
    try:
        raw_settings = redis_client.hgetall(DASHBOARD_SETTINGS_KEY) or {}
    except Exception:
        raw_settings = {}
    settings = {
        _dashboard_redis_text(key): _dashboard_redis_text(value)
        for key, value in raw_settings.items()
        if _dashboard_redis_text(key)
    } if isinstance(raw_settings, dict) else {}
    refresh_interval = _dashboard_interval_option(
        settings.get("refresh_interval_seconds"),
        allowed=DASHBOARD_REFRESH_INTERVAL_OPTIONS_SECONDS,
        default=DASHBOARD_SNAPSHOT_STALE_SECONDS,
    )
    brief_interval = _dashboard_interval_option(
        settings.get("brief_refresh_interval_seconds"),
        allowed=DASHBOARD_BRIEF_INTERVAL_OPTIONS_SECONDS,
        default=DASHBOARD_BRIEF_TTL_SECONDS,
    )
    return {
        "refresh_interval_seconds": refresh_interval,
        "brief_refresh_interval_seconds": brief_interval,
        "refresh_interval_options_seconds": list(DASHBOARD_REFRESH_INTERVAL_OPTIONS_SECONDS),
        "brief_refresh_interval_options_seconds": list(DASHBOARD_BRIEF_INTERVAL_OPTIONS_SECONDS),
    }


def _dashboard_save_refresh_settings(
    *,
    refresh_interval_seconds: Optional[int] = None,
    brief_refresh_interval_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    current = _dashboard_refresh_settings()
    updates: Dict[str, str] = {}
    if refresh_interval_seconds is not None:
        refresh_interval = _dashboard_interval_option(
            refresh_interval_seconds,
            allowed=DASHBOARD_REFRESH_INTERVAL_OPTIONS_SECONDS,
            default=DASHBOARD_SNAPSHOT_STALE_SECONDS,
        )
        current["refresh_interval_seconds"] = refresh_interval
        updates["refresh_interval_seconds"] = str(refresh_interval)
    if brief_refresh_interval_seconds is not None:
        brief_interval = _dashboard_interval_option(
            brief_refresh_interval_seconds,
            allowed=DASHBOARD_BRIEF_INTERVAL_OPTIONS_SECONDS,
            default=DASHBOARD_BRIEF_TTL_SECONDS,
        )
        current["brief_refresh_interval_seconds"] = brief_interval
        updates["brief_refresh_interval_seconds"] = str(brief_interval)
    if updates:
        try:
            redis_client.hset(DASHBOARD_SETTINGS_KEY, mapping=updates)
        except Exception:
            logger.debug("[dashboard] failed saving dashboard refresh settings", exc_info=True)
    return current


def _dashboard_snapshot_stale_seconds() -> int:
    settings = _dashboard_refresh_settings()
    seconds = _dashboard_safe_int(settings.get("refresh_interval_seconds"), DASHBOARD_SNAPSHOT_STALE_SECONDS)
    return seconds if seconds > 0 else DASHBOARD_SNAPSHOT_STALE_SECONDS


def _dashboard_brief_ttl_seconds() -> int:
    settings = _dashboard_refresh_settings()
    return _dashboard_safe_int(settings.get("brief_refresh_interval_seconds"), DASHBOARD_BRIEF_TTL_SECONDS)


def _dashboard_brief_check_interval_seconds() -> int:
    return _dashboard_brief_ttl_seconds()


def _dashboard_stats(rows: Any, *, limit: int = 0) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in rows if isinstance(rows, list) else []:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "").strip()
        if not label:
            continue
        out.append({"label": label, "value": item.get("value")})
        if limit > 0 and len(out) >= limit:
            break
    return out


def _dashboard_stat_value(stats: List[Dict[str, Any]], label: str) -> str:
    wanted = str(label or "").strip().lower()
    for item in stats or []:
        if str(item.get("label") or "").strip().lower() == wanted:
            return str(item.get("value") if item.get("value") is not None else "").strip()
    return ""


def _dashboard_stats_have_signal(stats: List[Dict[str, Any]]) -> bool:
    empty_values = {"", "-", "0", "0.0", "n/a", "none", "null", "waiting", "unknown"}
    for item in stats or []:
        value = item.get("value")
        token = str(value if value is not None else "").strip().lower()
        if token not in empty_values:
            return True
    return False


def _dashboard_tab_payload(core_key: str) -> Optional[Dict[str, Any]]:
    key = str(core_key or "").strip()
    if not key:
        return None
    try:
        tabs = _discover_core_webui_tabs(core_registry_module.refresh_core_registry())
        tab = next((item for item in tabs if str(item.get("core_key") or "").strip() == key), None)
        if not isinstance(tab, dict):
            return None
        payload = _load_surface_htmlui_tab_payload(tab)
    except Exception:
        logger.debug("[dashboard] failed loading core tab payload for %s", key, exc_info=True)
        return None
    return payload if isinstance(payload, dict) else None


def _dashboard_esphome_payload() -> Optional[Dict[str, Any]]:
    try:
        payload = esphome_home_module.get_runtime_payload(
            redis_client=redis_client,
            core_key="esphome",
            core_tab=_esphome_platform_tab_spec(),
            panel="satellites",
        )
    except Exception:
        logger.debug("[dashboard] failed loading ESPHome dashboard payload", exc_info=True)
        return None
    if not isinstance(payload, dict):
        return None
    for panel, key in (("speakerid", "speaker_id"), ("emotionid", "emotion_id")):
        try:
            extra = esphome_home_module.get_runtime_payload(
                redis_client=redis_client,
                core_key="esphome",
                core_tab=_esphome_platform_tab_spec(),
                panel=panel,
            )
            if isinstance(extra, dict) and isinstance(extra.get(key), dict):
                payload[key] = extra[key]
        except Exception:
            logger.debug("[dashboard] failed loading ESPHome %s payload", panel, exc_info=True)
    return payload


def _dashboard_item_forms(payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    ui = payload.get("ui") if isinstance(payload.get("ui"), dict) else {}
    rows = ui.get("item_forms") if isinstance(ui.get("item_forms"), list) else []
    return [item for item in rows if isinstance(item, dict)]


def _dashboard_item_fields(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    fields: List[Dict[str, Any]] = []
    raw_fields = item.get("fields") if isinstance(item.get("fields"), list) else []
    fields.extend([field for field in raw_fields if isinstance(field, dict)])
    for section in item.get("sections") if isinstance(item.get("sections"), list) else []:
        if not isinstance(section, dict):
            continue
        section_fields = section.get("fields") if isinstance(section.get("fields"), list) else []
        fields.extend([field for field in section_fields if isinstance(field, dict)])
    return fields


def _dashboard_item_image_fields(item: Dict[str, Any]) -> List[Dict[str, str]]:
    fields = _dashboard_item_fields(item)

    images: List[Dict[str, str]] = []
    for field in fields:
        if str(field.get("type") or "").strip().lower() != "image":
            continue
        src = str(field.get("src") or field.get("url") or field.get("data_url") or field.get("image_url") or "").strip()
        if not src:
            continue
        images.append(
            {
                "src": src,
                "alt": str(field.get("alt") or field.get("label") or item.get("title") or "Dashboard image").strip(),
                "caption": str(field.get("caption") or field.get("label") or "").strip(),
                "display": str(field.get("display") or "").strip(),
            }
        )
    return images


def _dashboard_item_description(item: Dict[str, Any]) -> str:
    fields = _dashboard_item_fields(item)
    for field in fields:
        key = str(field.get("key") or "").strip().lower()
        label = str(field.get("label") or "").strip().lower()
        field_type = str(field.get("type") or "").strip().lower()
        if field_type not in {"text", "textarea"}:
            continue
        if "description" not in key and label not in {"", "description", "summary"}:
            continue
        text = _dashboard_short_text(field.get("value"), limit=420)
        if text:
            return text
    subtitle = str(item.get("subtitle") or "").strip()
    marker = "Summary:"
    if marker in subtitle:
        return _dashboard_short_text(subtitle.split(marker, 1)[1], limit=420)
    return ""


def _dashboard_compact_item(
    item: Dict[str, Any],
    *,
    include_image: bool = True,
) -> Dict[str, Any]:
    images = _dashboard_item_image_fields(item)
    hero_src = str(item.get("hero_image_src") or "").strip()
    image = {"src": hero_src, "alt": str(item.get("hero_image_alt") or item.get("title") or "Dashboard image").strip()} if hero_src else (images[0] if images else {})
    badges = item.get("hero_badges") if isinstance(item.get("hero_badges"), list) else []
    summary_rows = item.get("summary_rows") if isinstance(item.get("summary_rows"), list) else []
    sensor_rows = item.get("sensor_rows") if isinstance(item.get("sensor_rows"), list) else []
    description = _dashboard_item_description(item)
    row = {
        "id": str(item.get("id") or "").strip(),
        "group": str(item.get("group") or "").strip(),
        "title": str(item.get("title") or item.get("id") or "").strip(),
        "subtitle": str(item.get("subtitle") or "").strip(),
        "detail": str(item.get("detail") or "").strip(),
        "description": description,
        "connected": bool(item.get("connected")),
        "badges": [badge for badge in badges if isinstance(badge, dict)][:4],
        "summary_rows": [entry for entry in summary_rows if isinstance(entry, dict)][:6],
        "sensor_title": str(item.get("sensor_title") or "").strip(),
        "sensor_rows": [entry for entry in sensor_rows if isinstance(entry, dict)][:18],
    }
    if include_image and image.get("src"):
        row["image_src"] = str(image.get("src") or "").strip()
        row["image_alt"] = str(image.get("alt") or row.get("title") or "Dashboard image").strip()
        row["image_caption"] = str(image.get("caption") or "").strip()
        row["image_display"] = str(image.get("display") or "").strip()
    return row


def _dashboard_core_items(
    payload: Optional[Dict[str, Any]],
    *,
    groups: Tuple[str, ...],
    limit: int = 4,
    require_image: bool = False,
    include_image: bool = True,
) -> List[Dict[str, Any]]:
    wanted = {str(group or "").strip().lower() for group in groups if str(group or "").strip()}
    items: List[Dict[str, Any]] = []
    for item in _dashboard_item_forms(payload):
        group = str(item.get("group") or "").strip().lower()
        if wanted and group not in wanted:
            continue
        compact = _dashboard_compact_item(item, include_image=include_image)
        if require_image and not compact.get("image_src"):
            continue
        if not compact.get("title") and not compact.get("image_src"):
            continue
        items.append(compact)
        if limit > 0 and len(items) >= limit:
            break
    return items


def _dashboard_environment_table_rows(item: Dict[str, Any], *, tokens: Tuple[str, ...], limit: int = 6) -> List[Dict[str, Any]]:
    wanted = tuple(str(token or "").strip().lower() for token in tokens if str(token or "").strip())
    out: List[Dict[str, Any]] = []
    for field in _dashboard_item_fields(item):
        if not isinstance(field, dict) or str(field.get("type") or "").strip().lower() != "table":
            continue
        haystack = " ".join(
            str(field.get(key) or "").strip().lower()
            for key in ("key", "label")
        )
        if wanted and not any(token in haystack for token in wanted):
            continue
        rows = field.get("rows") if isinstance(field.get("rows"), list) else []
        for row in rows:
            if isinstance(row, dict):
                out.append(row)
            if len(out) >= max(1, int(limit)):
                return out
    return out


def _dashboard_table_rows(item: Dict[str, Any], *, tokens: Tuple[str, ...], limit: int = 8) -> List[Dict[str, Any]]:
    wanted = tuple(str(token or "").strip().lower() for token in tokens if str(token or "").strip())
    out: List[Dict[str, Any]] = []
    for field in _dashboard_item_fields(item):
        if not isinstance(field, dict) or str(field.get("type") or "").strip().lower() != "table":
            continue
        haystack = " ".join(str(field.get(key) or "").strip().lower() for key in ("key", "label"))
        if wanted and not any(token in haystack for token in wanted):
            continue
        rows = field.get("rows") if isinstance(field.get("rows"), list) else []
        for row in rows:
            if isinstance(row, dict):
                out.append(row)
            if len(out) >= max(1, int(limit)):
                return out
    return out


def _dashboard_environment_context(payload: Optional[Dict[str, Any]], section: Dict[str, Any]) -> Dict[str, Any]:
    stats = _dashboard_stats((payload or {}).get("header_stats") or (payload or {}).get("stats"), limit=0)
    if not stats:
        stats = _dashboard_stats(section.get("stats"), limit=0)
    context: Dict[str, Any] = {
        "summary": section.get("summary"),
        "stats": stats,
        "overview_cards": [],
        "forecast_cards": [],
        "selected_sources": [],
    }
    reserved_source_ids = {"source:runtime", "source:ecowitt", "source:discovery"}
    for item in _dashboard_item_forms(payload):
        group = str(item.get("group") or "").strip().lower()
        compact = _dashboard_compact_item(item, include_image=False)
        if group == "overview":
            context["overview_cards"].append(compact)
        elif group == "forecast":
            card = dict(compact)
            daily = _dashboard_environment_table_rows(item, tokens=("daily", "forecast"), limit=5)
            hourly = _dashboard_environment_table_rows(item, tokens=("hourly", "next hours"), limit=6)
            alerts = _dashboard_environment_table_rows(item, tokens=("alert",), limit=3)
            if daily:
                card["daily_forecast"] = daily
            if hourly:
                card["next_hours"] = hourly
            if alerts:
                card["alerts"] = alerts
            context["forecast_cards"].append(card)
        elif group == "source":
            item_id = str(item.get("id") or "").strip()
            if item_id in reserved_source_ids or not item_id.startswith("source:"):
                continue
            context["selected_sources"].append(
                {
                    "title": compact.get("title"),
                    "subtitle": compact.get("subtitle"),
                    "detail": compact.get("detail"),
                    "summary_rows": compact.get("summary_rows") or [],
                }
            )
    context["overview_cards"] = context["overview_cards"][:8]
    context["forecast_cards"] = context["forecast_cards"][:4]
    context["selected_sources"] = context["selected_sources"][:16]
    return context


def _dashboard_invalidate_briefs(*brief_ids: str) -> None:
    fields = [str(item or "").strip() for item in brief_ids if str(item or "").strip()]
    if not fields:
        return
    try:
        redis_client.hdel(DASHBOARD_BRIEFS_KEY, *fields)
    except Exception:
        logger.debug("[dashboard] failed invalidating dashboard briefs", exc_info=True)


def _dashboard_people_options() -> List[Dict[str, str]]:
    try:
        store = people_module.load_store(redis_client)
    except Exception:
        logger.debug("[dashboard] failed loading People options", exc_info=True)
        return []
    raw_people = store.get("people") if isinstance(store, dict) else []
    options: List[Dict[str, str]] = []
    for person in raw_people if isinstance(raw_people, list) else []:
        if not isinstance(person, dict):
            continue
        person_id = _dashboard_redis_text(person.get("id") or person.get("person_id"))
        label = _dashboard_redis_text(person.get("display_name") or person.get("name") or person_id)
        if person_id and label:
            options.append({"value": person_id, "label": label})
    return sorted(options, key=lambda row: str(row.get("label") or "").lower())


def _dashboard_personal_settings() -> Dict[str, Any]:
    selected = _dashboard_redis_text(redis_client.get(DASHBOARD_PERSONAL_PERSON_KEY))
    options = _dashboard_people_options()
    option_values = {str(row.get("value") or "") for row in options}
    if selected and selected not in option_values:
        selected = ""
    selected_label = next((str(row.get("label") or "") for row in options if row.get("value") == selected), "")
    return {
        "person_id": selected,
        "person_label": selected_label or ("All people" if not selected else selected),
        "people_options": options,
    }


def _dashboard_save_personal_settings(person_id: Any) -> Dict[str, Any]:
    wanted = _dashboard_redis_text(person_id)
    options = _dashboard_people_options()
    option_values = {str(row.get("value") or "") for row in options}
    if wanted and wanted not in option_values:
        raise HTTPException(status_code=400, detail="Unknown Personal Core person.")
    if wanted:
        redis_client.set(DASHBOARD_PERSONAL_PERSON_KEY, wanted)
    else:
        redis_client.delete(DASHBOARD_PERSONAL_PERSON_KEY)
    _dashboard_invalidate_briefs("overview", "personal")
    return _dashboard_personal_settings()


def _dashboard_personal_calendar_payload(person_id: str = "") -> Dict[str, Any]:
    try:
        module = core_runtime._import_module("personal_core", reload_module=False)
    except Exception:
        logger.debug("[dashboard] failed importing personal_core for calendar payload", exc_info=True)
        return {}
    provider = getattr(module, "get_personal_calendar_payload", None)
    if not callable(provider):
        return {}
    try:
        query: Dict[str, Any] = {"range": "week", "limit": 250}
        selected_person = _dashboard_redis_text(person_id)
        if selected_person:
            query["person_id"] = selected_person
        payload = provider(query)
    except Exception:
        logger.debug("[dashboard] failed loading personal calendar payload", exc_info=True)
        return {}
    return payload if isinstance(payload, dict) else {}


def _dashboard_personal_item_type_label(value: Any) -> str:
    token = str(value if value is not None else "").strip().replace("_", " ").replace("-", " ")
    if not token:
        return "Item"
    aliases = {
        "calendar event": "Calendar",
        "email event": "Plan",
        "action": "Action",
        "subscription": "Renewal",
        "delivery": "Delivery",
    }
    lowered = " ".join(token.lower().split())
    return aliases.get(lowered, " ".join(part.capitalize() for part in lowered.split()))


def _dashboard_personal_compact_item(item: Dict[str, Any]) -> Dict[str, Any]:
    title = _dashboard_short_text(item.get("title"), limit=140)
    item_type = _dashboard_personal_item_type_label(item.get("item_type") or item.get("type"))
    detail_parts = [
        _dashboard_redis_text(item.get("when")),
        _dashboard_redis_text(item.get("location")),
        _dashboard_redis_text(item.get("status")),
    ]
    detail = " • ".join(part for part in detail_parts if part)
    summary = _dashboard_short_text(item.get("summary"), limit=180)
    if not summary and item.get("merchant"):
        summary = _dashboard_short_text(item.get("merchant"), limit=180)
    return {
        "title": title or item_type,
        "type": item_type,
        "when": _dashboard_redis_text(item.get("when")),
        "source": _dashboard_redis_text(item.get("source")),
        "person": _dashboard_redis_text(item.get("person_name") or item.get("person")),
        "location": _dashboard_redis_text(item.get("location")),
        "status": _dashboard_redis_text(item.get("status")),
        "detail": detail,
        "summary": summary,
    }


def _dashboard_personal_context(
    payload: Optional[Dict[str, Any]],
    section: Dict[str, Any],
    calendar_payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    stats = _dashboard_stats((payload or {}).get("header_stats") or (payload or {}).get("stats"), limit=0)
    if not stats:
        stats = _dashboard_stats(section.get("stats"), limit=0)
    calendar = calendar_payload if isinstance(calendar_payload, dict) else {}
    context: Dict[str, Any] = {
        "summary": section.get("summary"),
        "stats": stats,
        "person_id": section.get("person_id"),
        "person_label": section.get("person_label"),
        "calendar": {
            "range": calendar.get("range"),
            "date_from": calendar.get("date_from"),
            "date_to": calendar.get("date_to"),
            "item_count": calendar.get("item_count"),
            "counts_by_type": calendar.get("counts_by_type") if isinstance(calendar.get("counts_by_type"), dict) else {},
            "counts_by_source": calendar.get("counts_by_source") if isinstance(calendar.get("counts_by_source"), dict) else {},
            "items": [
                _dashboard_personal_compact_item(item)
                for item in (calendar.get("items") if isinstance(calendar.get("items"), list) else [])
                if isinstance(item, dict)
            ][:80],
        },
        "calendar_today_rows": [],
        "calendar_week_rows": [],
        "overview_cards": [],
        "subscriptions": [],
        "deliveries": [],
        "actions": [],
        "notes": [],
    }
    for item in _dashboard_item_forms(payload):
        group = str(item.get("group") or "").strip().lower()
        compact = _dashboard_compact_item(item, include_image=False)
        if group == "calendar_dashboard":
            context["overview_cards"].append(compact)
            context["calendar_today_rows"] = _dashboard_table_rows(item, tokens=("today",), limit=12)
            context["calendar_week_rows"] = _dashboard_table_rows(item, tokens=("next 7", "week"), limit=24)
        elif group == "overview_dashboard":
            context["overview_cards"].append(compact)
            context["subscriptions"] = _dashboard_table_rows(item, tokens=("subscription", "recurring"), limit=12)
            context["deliveries"] = _dashboard_table_rows(item, tokens=("delivery", "tracking"), limit=12)
            context["actions"] = _dashboard_table_rows(item, tokens=("action", "open action"), limit=12)
            context["notes"] = _dashboard_table_rows(item, tokens=("important", "notes"), limit=12)
    if not context["calendar"]["items"] and context["calendar_week_rows"]:
        context["calendar"]["items"] = [
            _dashboard_personal_compact_item(
                {
                    "when": row.get("when"),
                    "item_type": row.get("type"),
                    "title": row.get("title"),
                    "source": row.get("source"),
                    "person_name": row.get("person"),
                    "location": row.get("location"),
                    "status": row.get("status"),
                }
            )
            for row in context["calendar_week_rows"]
            if isinstance(row, dict)
        ][:80]
    calendar_items = [item for item in context["calendar"].get("items") or [] if isinstance(item, dict)]
    if calendar_items and _dashboard_safe_int(context["calendar"].get("item_count"), 0) <= 0:
        context["calendar"]["item_count"] = len(calendar_items)
    counts_by_type = context["calendar"].get("counts_by_type") if isinstance(context["calendar"].get("counts_by_type"), dict) else {}
    if calendar_items and not counts_by_type:
        context["calendar"]["counts_by_type"] = dict(Counter(str(item.get("type") or "Item").strip() or "Item" for item in calendar_items))
    return context


def _dashboard_section(
    *,
    section_id: str,
    title: str,
    subtitle: str,
    payload: Optional[Dict[str, Any]],
    stats_limit: int = 8,
    require_signal: bool = True,
) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    if payload.get("error"):
        return {
            "id": section_id,
            "title": title,
            "subtitle": subtitle,
            "summary": "",
            "stats": [{"label": "Status", "value": "Needs attention"}],
            "tone": "warning",
            "error": str(payload.get("error") or "").strip(),
        }

    stats = _dashboard_stats(payload.get("header_stats") or payload.get("stats"), limit=stats_limit)
    summary = str(payload.get("summary") or "").strip()
    if require_signal and not _dashboard_stats_have_signal(stats):
        return None
    return {
        "id": section_id,
        "title": title,
        "subtitle": subtitle,
        "summary": summary,
        "stats": stats,
        "tone": "normal",
    }


def _dashboard_short_text(value: Any, *, limit: int = 220) -> str:
    text = " ".join(_dashboard_redis_text(value).split())
    if limit <= 0 or len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "..."


def _dashboard_awareness_area_label(source: Any, data: Dict[str, Any]) -> str:
    for value in (data.get("area"), data.get("area_name"), data.get("location"), source):
        text = _dashboard_redis_text(value)
        if text:
            return " ".join(text.replace("_", " ").split()).title()
    return ""


def _dashboard_awareness_event_row(
    event: Dict[str, Any],
    *,
    bucket_fn: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> Dict[str, Any]:
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    source = _dashboard_redis_text(event.get("source"))
    bucket = ""
    if callable(bucket_fn):
        try:
            bucket = _dashboard_redis_text(bucket_fn(event)).lower()
        except Exception:
            bucket = ""
    if not bucket:
        event_type = _dashboard_redis_text(event.get("type")).lower()
        entity_id = _dashboard_redis_text(event.get("entity_id")).lower()
        if event_type == "doorbell":
            bucket = "doorbell"
        elif event_type.startswith("camera") or entity_id.startswith("camera."):
            bucket = "camera"
        elif "_sensor_" in event_type or entity_id.startswith(("sensor.", "binary_sensor.", "cover.")):
            bucket = "sensor"
        else:
            bucket = "other"
    title = _dashboard_short_text(event.get("title"), limit=100)
    message = _dashboard_short_text(event.get("message") or title, limit=260)
    return {
        "time": _dashboard_redis_text(event.get("ha_time") or event.get("time") or event.get("timestamp")),
        "source": source,
        "area": _dashboard_awareness_area_label(source, data),
        "type": _dashboard_redis_text(event.get("type")),
        "bucket": bucket,
        "entity_id": _dashboard_redis_text(event.get("entity_id")),
        "level": _dashboard_redis_text(event.get("level")),
        "title": title,
        "message": message,
    }


def _dashboard_awareness_event_is_camera(event: Dict[str, Any]) -> bool:
    bucket = str((event or {}).get("bucket") or "").strip().lower()
    event_type = str((event or {}).get("type") or "").strip().lower()
    entity_id = str((event or {}).get("entity_id") or "").strip().lower()
    return bool(bucket == "camera" or event_type.startswith("camera") or entity_id.startswith("camera."))


def _dashboard_awareness_camera_events(event_summary: Dict[str, Any], *, limit: int = 180) -> List[Dict[str, Any]]:
    events = event_summary.get("events") if isinstance(event_summary.get("events"), list) else []
    camera_events = [event for event in events if isinstance(event, dict) and _dashboard_awareness_event_is_camera(event)]
    if limit > 0 and len(camera_events) > limit:
        return camera_events[-limit:]
    return camera_events


def _dashboard_awareness_events_summary(
    *,
    hours: int = 12,
    event_limit: int = DASHBOARD_AWARENESS_EVENT_DETAIL_LIMIT,
) -> Dict[str, Any]:
    try:
        module = core_runtime._import_module("awareness_core", reload_module=False)
    except Exception:
        logger.debug("[dashboard] failed importing awareness_core for event summary", exc_info=True)
        return {}

    discover_fn = getattr(module, "_discover_event_sources", None)
    load_fn = getattr(module, "_load_events_for_sources", None)
    if not callable(discover_fn) or not callable(load_fn):
        return {}

    end = datetime.now()
    start = end - timedelta(hours=max(1, int(hours or 12)))
    try:
        sources = discover_fn(redis_client) or []
    except Exception:
        logger.debug("[dashboard] failed discovering awareness event sources", exc_info=True)
        return {}
    sources = [_dashboard_redis_text(source) for source in sources if _dashboard_redis_text(source)]
    if not sources:
        return {
            "window": f"last {max(1, int(hours or 12))} hours",
            "event_count": 0,
            "sources": [],
            "events": [],
            "top_areas": [],
            "top_types": [],
        }

    try:
        events = load_fn(redis_client, sources, start, end, limit_per_source=DASHBOARD_AWARENESS_EVENTS_PER_SOURCE) or []
    except Exception:
        logger.debug("[dashboard] failed loading awareness events for dashboard summary", exc_info=True)
        return {}

    bucket_fn = getattr(module, "_event_type_bucket", None)
    rows_latest = [
        _dashboard_awareness_event_row(event, bucket_fn=bucket_fn if callable(bucket_fn) else None)
        for event in events
        if isinstance(event, dict)
    ]
    rows = list(reversed(rows_latest))
    bucket_counts = Counter(row.get("bucket") or "other" for row in rows)
    area_counts = Counter(row.get("area") or row.get("source") or "Unknown" for row in rows)
    type_counts = Counter(row.get("type") or row.get("bucket") or "event" for row in rows)
    source_counts = Counter(row.get("source") or "unknown" for row in rows)
    highlights = [
        {
            "time": row.get("time"),
            "area": row.get("area"),
            "type": row.get("type") or row.get("bucket"),
            "title": row.get("title"),
            "message": row.get("message"),
        }
        for row in rows_latest[: min(len(rows_latest), DASHBOARD_AWARENESS_EVENT_HIGHLIGHT_LIMIT)]
        if row.get("message") or row.get("title")
    ]
    detail_limit = max(0, int(event_limit or 0))
    event_details = rows[-detail_limit:] if detail_limit else []
    return {
        "window": f"last {max(1, int(hours or 12))} hours",
        "start": start.isoformat(timespec="seconds"),
        "end": end.isoformat(timespec="seconds"),
        "event_count": len(rows),
        "included_event_count": len(event_details),
        "truncated": len(rows) > len(event_details),
        "sources": [{"source": source, "count": count} for source, count in source_counts.most_common(8)],
        "buckets": [{"type": bucket, "count": count} for bucket, count in bucket_counts.most_common()],
        "top_areas": [{"area": area, "count": count} for area, count in area_counts.most_common(8)],
        "top_types": [{"type": event_type, "count": count} for event_type, count in type_counts.most_common(8)],
        "highlights": highlights,
        "events": event_details,
    }


def _dashboard_cache_rows() -> Dict[str, Dict[str, Any]]:
    try:
        raw_rows = redis_client.hgetall(DASHBOARD_BRIEFS_KEY) or {}
    except Exception:
        return {}
    rows: Dict[str, Dict[str, Any]] = {}
    for raw_key, raw_value in raw_rows.items():
        key = _dashboard_redis_text(raw_key)
        text = _dashboard_redis_text(raw_value)
        if not key or not text:
            continue
        try:
            parsed = json.loads(text)
        except Exception:
            continue
        if isinstance(parsed, dict) and _dashboard_safe_int(parsed.get("schema_version")) == DASHBOARD_BRIEF_SCHEMA_VERSION:
            rows[key] = parsed
    return rows


def _dashboard_save_brief(row: Dict[str, Any]) -> None:
    brief_id = str(row.get("id") or "").strip()
    if not brief_id:
        return
    try:
        row["schema_version"] = DASHBOARD_BRIEF_SCHEMA_VERSION
        redis_client.hset(DASHBOARD_BRIEFS_KEY, brief_id, json.dumps(row, separators=(",", ":")))
    except Exception:
        logger.debug("[dashboard] failed saving brief %s", brief_id, exc_info=True)


def _dashboard_snapshot_meta(*, cached_at: float = 0.0, source: str = "live") -> Dict[str, Any]:
    now = time.time()
    age = max(0.0, now - float(cached_at or now))
    stale_after = _dashboard_snapshot_stale_seconds()
    return {
        "source": str(source or "live").strip() or "live",
        "cached_at": float(cached_at or now),
        "age_seconds": age,
        "stale": bool(age > stale_after),
        "stale_after_seconds": stale_after,
    }


def _dashboard_load_snapshot_cache() -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    try:
        raw = redis_client.get(DASHBOARD_SNAPSHOT_KEY)
    except Exception:
        return None, _dashboard_snapshot_meta(source="miss")
    text = _dashboard_redis_text(raw)
    if not text:
        return None, _dashboard_snapshot_meta(source="miss")
    try:
        row = json.loads(text)
    except Exception:
        return None, _dashboard_snapshot_meta(source="invalid")
    if not isinstance(row, dict):
        return None, _dashboard_snapshot_meta(source="invalid")
    if _dashboard_safe_int(row.get("schema_version")) != DASHBOARD_SNAPSHOT_SCHEMA_VERSION:
        return None, _dashboard_snapshot_meta(source="invalid")
    snapshot = row.get("snapshot") if isinstance(row.get("snapshot"), dict) else None
    if not isinstance(snapshot, dict):
        return None, _dashboard_snapshot_meta(source="invalid")
    cached_at = float(row.get("cached_at") or 0.0)
    return snapshot, _dashboard_snapshot_meta(cached_at=cached_at, source="cache")


def _dashboard_save_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    cached_at = time.time()
    if not isinstance(snapshot, dict):
        return _dashboard_snapshot_meta(cached_at=cached_at, source="live")
    try:
        row = {
            "schema_version": DASHBOARD_SNAPSHOT_SCHEMA_VERSION,
            "cached_at": cached_at,
            "snapshot": snapshot,
        }
        redis_client.set(DASHBOARD_SNAPSHOT_KEY, json.dumps(row, default=str, separators=(",", ":")))
    except Exception:
        logger.debug("[dashboard] failed saving snapshot cache", exc_info=True)
    return _dashboard_snapshot_meta(cached_at=cached_at, source="live")


def _dashboard_snapshot_refresh_snapshot() -> Dict[str, Any]:
    with dashboard_snapshot_refresh_lock:
        state = dict(dashboard_snapshot_refresh_state)
    started_at = float(state.get("started_at") or 0.0)
    finished_at = float(state.get("finished_at") or 0.0)
    now = time.time()
    state["age_seconds"] = max(0.0, now - started_at) if bool(state.get("running")) and started_at else 0.0
    state["last_finished_age_seconds"] = max(0.0, now - finished_at) if finished_at else 0.0
    return state


def _dashboard_mark_snapshot_refresh_started(*, reason: str, force: bool = False) -> bool:
    now = time.time()
    with dashboard_snapshot_refresh_lock:
        if bool(dashboard_snapshot_refresh_state.get("running")) and not force:
            return False
        dashboard_snapshot_refresh_state.update(
            {
                "running": True,
                "started_at": now,
                "last_reason": str(reason or "dashboard").strip() or "dashboard",
                "last_error": "",
            }
        )
    return True


def _dashboard_mark_snapshot_refresh_finished(*, error: str = "") -> None:
    with dashboard_snapshot_refresh_lock:
        dashboard_snapshot_refresh_state.update(
            {
                "running": False,
                "finished_at": time.time(),
                "last_error": str(error or "").strip(),
            }
        )


def _dashboard_schedule_snapshot_refresh(*, reason: str = "dashboard", force: bool = False) -> bool:
    if not _dashboard_mark_snapshot_refresh_started(reason=reason, force=force):
        return False

    async def runner() -> None:
        error = ""
        try:
            snapshot = await run_dashboard(_dashboard_build_snapshot)
            await run_dashboard(_dashboard_save_snapshot, snapshot)
        except Exception as exc:
            error = str(exc)
            logger.info("[dashboard] queued snapshot refresh failed reason=%s: %s", reason, exc)
        finally:
            _dashboard_mark_snapshot_refresh_finished(error=error)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _dashboard_mark_snapshot_refresh_finished(error="dashboard event loop unavailable")
        return False
    loop.create_task(runner())
    return True


def _dashboard_time_greeting(now: Optional[datetime] = None) -> Dict[str, Any]:
    local_now = now or datetime.now()
    hour = int(local_now.hour)
    if 5 <= hour < 12:
        greeting = "Good morning"
    elif 12 <= hour < 17:
        greeting = "Good afternoon"
    elif 17 <= hour < 22:
        greeting = "Good evening"
    else:
        greeting = "Good night"
    return {
        "greeting": greeting,
        "date": f"{local_now.strftime('%A, %B')} {local_now.day}",
        "time": local_now.strftime("%-I:%M %p") if os.name != "nt" else local_now.strftime("%I:%M %p").lstrip("0"),
        "hour": hour,
    }


def _dashboard_overview_context_data(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    health_payload = snapshot.get("health") if isinstance(snapshot.get("health"), dict) else {}
    runtime = snapshot.get("runtime") if isinstance(snapshot.get("runtime"), dict) else {}
    updates = snapshot.get("updates") if isinstance(snapshot.get("updates"), dict) else {}
    sections = snapshot.get("sections") if isinstance(snapshot.get("sections"), list) else []
    cards = snapshot.get("cards") if isinstance(snapshot.get("cards"), list) else []

    compact_sections: List[Dict[str, Any]] = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        section_id = str(section.get("id") or "").strip()
        stats = _dashboard_stats(section.get("stats"), limit=8)
        row: Dict[str, Any] = {
            "id": section_id,
            "title": section.get("title"),
            "summary": section.get("summary"),
            "stats": stats,
        }
        if section_id == "voice":
            row["devices"] = [
                {
                    "title": item.get("title"),
                    "subtitle": item.get("subtitle"),
                    "connected": item.get("connected"),
                }
                for item in section.get("devices") or []
                if isinstance(item, dict)
            ][:6]
        elif section_id == "awareness":
            event_summary = section.get("event_summary") if isinstance(section.get("event_summary"), dict) else {}
            camera_events = _dashboard_awareness_camera_events(event_summary, limit=8)
            row["event_summary"] = {
                "window": event_summary.get("window"),
                "event_count": event_summary.get("event_count"),
                "camera_event_count": len(camera_events),
                "top_areas": event_summary.get("top_areas") or [],
                "camera_highlights": [
                    {
                        "time": item.get("time"),
                        "area": item.get("area"),
                        "message": item.get("message") or item.get("title"),
                    }
                    for item in camera_events[-5:]
                    if item.get("message") or item.get("title")
                ],
            }
        elif section_id == "guardian":
            row["devices"] = [
                {
                    "title": item.get("title"),
                    "subtitle": item.get("subtitle"),
                    "detail": item.get("detail"),
                    "description": item.get("description"),
                    "summary_rows": item.get("summary_rows") or [],
                    "sensor_rows": item.get("sensor_rows") or [],
                }
                for item in section.get("devices") or []
                if isinstance(item, dict)
            ][:8]
            row["events"] = [
                {
                    "title": item.get("title"),
                    "subtitle": item.get("subtitle"),
                    "detail": item.get("detail"),
                    "description": item.get("description"),
                    "summary_rows": item.get("summary_rows") or [],
                    "sensor_rows": item.get("sensor_rows") or [],
                }
                for item in section.get("events") or []
                if isinstance(item, dict)
            ][:8]
        elif section_id == "personal":
            personal_context = section.get("brief_context") if isinstance(section.get("brief_context"), dict) else {}
            calendar = personal_context.get("calendar") if isinstance(personal_context.get("calendar"), dict) else {}
            row["person_id"] = section.get("person_id")
            row["person_label"] = section.get("person_label")
            row["calendar"] = {
                "range": calendar.get("range"),
                "date_from": calendar.get("date_from"),
                "date_to": calendar.get("date_to"),
                "item_count": calendar.get("item_count"),
                "counts_by_type": calendar.get("counts_by_type") or {},
                "next_items": [
                    {
                        "when": item.get("when"),
                        "type": item.get("type"),
                        "title": item.get("title"),
                        "status": item.get("status"),
                    }
                    for item in calendar.get("items") or []
                    if isinstance(item, dict)
                ][:8],
            }
        compact_sections.append(row)

    return {
        "local_time": _dashboard_time_greeting(),
        "health": health_payload,
        "runtime": runtime,
        "updates": updates,
        "cards": [
            {
                "id": item.get("id"),
                "label": item.get("label"),
                "value": item.get("value"),
                "detail": item.get("detail"),
                "tone": item.get("tone"),
            }
            for item in cards
            if isinstance(item, dict)
        ],
        "sections": compact_sections,
    }


def _dashboard_brief_contexts(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    contexts: List[Dict[str, Any]] = []
    health_payload = snapshot.get("health") if isinstance(snapshot.get("health"), dict) else {}
    runtime = snapshot.get("runtime") if isinstance(snapshot.get("runtime"), dict) else {}
    contexts.append(
        {
            "id": "overview",
            "title": "Home Brief",
            "source": "tater",
            "instructions": (
                "Write the top dashboard greeting for the user. Start with local_time.greeting as the first words, but do not attach local_time.date directly after it as a comma phrase or standalone date label. "
                "If local_time.date is useful, weave it into a natural dashboard sentence such as 'Good morning. For Friday, May 15, ...' or omit it when it sounds forced. "
                "Use the available cards and sections to summarize the whole home and Tater state in one friendly paragraph. "
                "Prefer concrete useful details: Tater health, weather, connected voice devices, the 7-day Personal outlook, and what cameras recently saw. "
                "If updates.total is greater than zero, mention that updates are waiting and summarize the affected groups from updates.summary. "
                "Mention only data that is present; do not imply a missing core is installed. "
                "Keep it concise, warm, and practical: one to three sentences, no markdown."
            ),
            "data": _dashboard_overview_context_data(snapshot),
        }
    )
    contexts.append(
        {
            "id": "system",
            "title": "Tater Status",
            "source": "tater",
            "instructions": (
                "Write a concise Tater runtime status. Include update availability from updates when updates.total is greater than zero, "
                "otherwise say the installed surfaces and firmware look current only if the data supports that. "
                "Use one or two natural sentences, no markdown."
            ),
            "data": {
                "health": health_payload,
                "runtime": runtime,
                "updates": snapshot.get("updates") if isinstance(snapshot.get("updates"), dict) else {},
            },
        }
    )

    voice_section = snapshot.get("voice_section")
    if isinstance(voice_section, dict):
        contexts.append(
            {
                "id": "voice",
                "title": "Voice Summary",
                "source": "esphome",
                "data": {
                    "summary": voice_section.get("summary"),
                    "stats": voice_section.get("stats"),
                    "devices": [
                        {
                            "title": item.get("title"),
                            "subtitle": item.get("subtitle"),
                            "connected": item.get("connected"),
                        }
                        for item in voice_section.get("devices") or []
                        if isinstance(item, dict)
                    ],
                },
            }
        )

    environment_section = snapshot.get("environment_section")
    if isinstance(environment_section, dict):
        environment_context = environment_section.get("brief_context") if isinstance(environment_section.get("brief_context"), dict) else {}
        contexts.append(
            {
                "id": "environment",
                "title": "Weather Summary",
                "source": "environment_core",
                "instructions": (
                    "Write a natural dashboard Environment brief, like a helpful weather and home sensor check-in. "
                    "Lead with the current outdoor feel using temperature and condition when present, then mention rain or forecast risk, "
                    "wind/humidity if useful, and any notable indoor/room/selected sensor readings. "
                    "Use all available Environment Core context: stats, overview_cards sensor_rows, forecast_cards, and selected_sources. "
                    "Do not recite every value as a list, do not start with 'Environment is live', and do not invent missing sensor readings. "
                    "One or two polished sentences, no markdown."
                ),
                "data": {
                    "summary": environment_context.get("summary") or environment_section.get("summary"),
                    "stats": environment_context.get("stats") or environment_section.get("stats"),
                    "overview_cards": environment_context.get("overview_cards") or [],
                    "forecast_cards": environment_context.get("forecast_cards") or [],
                    "selected_sources": environment_context.get("selected_sources") or [],
                },
            }
        )

    personal_section = snapshot.get("personal_section")
    if isinstance(personal_section, dict):
        personal_context = personal_section.get("brief_context") if isinstance(personal_section.get("brief_context"), dict) else {}
        contexts.append(
            {
                "id": "personal",
                "title": "Personal Outlook",
                "source": "personal_core",
                "instructions": (
                    "Write a 7-day Personal Core outlook for the dashboard. "
                    "When person_label is All people, summarize all linked profiles; otherwise write for that selected person only. "
                    "Use calendar.items plus subscriptions, deliveries, actions, notes, and stats when present. "
                    "Focus on what the user needs to know this week: calendar events, email-derived plans, open actions, deliveries, and upcoming renewals. "
                    "Call out today or tomorrow only when the data supports it. "
                    "Do not expose raw account IDs, internal implementation details, or pretend missing personal data exists. "
                    "One to three practical sentences, no markdown."
                ),
                "data": {
                    "summary": personal_context.get("summary") or personal_section.get("summary"),
                    "stats": personal_context.get("stats") or personal_section.get("stats"),
                    "person_id": personal_context.get("person_id") or personal_section.get("person_id"),
                    "person_label": personal_context.get("person_label") or personal_section.get("person_label"),
                    "calendar": personal_context.get("calendar") or {},
                    "calendar_today_rows": personal_context.get("calendar_today_rows") or [],
                    "calendar_week_rows": personal_context.get("calendar_week_rows") or [],
                    "overview_cards": personal_context.get("overview_cards") or [],
                    "subscriptions": personal_context.get("subscriptions") or [],
                    "deliveries": personal_context.get("deliveries") or [],
                    "actions": personal_context.get("actions") or [],
                    "notes": personal_context.get("notes") or [],
                },
            }
        )

    awareness_section = snapshot.get("awareness_section")
    if isinstance(awareness_section, dict):
        contexts.append(
            {
                "id": "awareness",
                "title": "Awareness Summary",
                "source": "awareness_core",
                "instructions": (
                    "Write a dashboard awareness brief for the last 12 hours using event_summary. "
                    "Treat event_summary.events as chronological evidence from all Awareness event cards and event_summary.highlights as the newest notable items. "
                    "Summarize the story of the home: patterns by area, repeated camera/sensor/doorbell activity, quiet periods if evident, and notable recent messages. "
                    "Use counts only to support the narrative; do not make lifetime Total Events the main point. "
                    "If event_summary.truncated is true, summarize the included latest events plus the aggregate counts without pretending unseen event details were read. "
                    "Keep it useful for a homeowner checking the dashboard."
                ),
                "data": {
                    "summary": awareness_section.get("summary"),
                    "stats": awareness_section.get("stats"),
                    "event_summary": awareness_section.get("event_summary") or {},
                    "recent_events": awareness_section.get("recent_events") or [],
                },
            }
        )

    guardian_section = snapshot.get("guardian_section")
    if isinstance(guardian_section, dict):
        contexts.append(
            {
                "id": "guardian",
                "title": "Guardian Summary",
                "source": "guardian_core",
                "instructions": (
                    "Write a concise Guardian dashboard brief from the available Guardian stats, devices, and events. "
                    "Focus on network/home safety state, device count, recent events, and whether AI analysis is healthy. "
                    "Use only the provided data, do not invent threats, and keep it one or two useful sentences with no markdown."
                ),
                "data": {
                    "summary": guardian_section.get("summary"),
                    "stats": guardian_section.get("stats"),
                    "devices": guardian_section.get("devices") or [],
                    "events": guardian_section.get("events") or [],
                },
            }
        )

    return contexts


def _dashboard_environment_lookup(data: Dict[str, Any], labels: Tuple[str, ...]) -> str:
    wanted = {str(label or "").strip().lower() for label in labels if str(label or "").strip()}
    if not wanted:
        return ""
    for stat in _dashboard_stats(data.get("stats"), limit=0):
        if str(stat.get("label") or "").strip().lower() in wanted:
            return str(stat.get("value") if stat.get("value") is not None else "").strip()
    for card in data.get("overview_cards") if isinstance(data.get("overview_cards"), list) else []:
        if not isinstance(card, dict):
            continue
        for row_key in ("summary_rows", "sensor_rows"):
            for row in card.get(row_key) if isinstance(card.get(row_key), list) else []:
                if not isinstance(row, dict):
                    continue
                if str(row.get("label") or "").strip().lower() in wanted:
                    return str(row.get("value") if row.get("value") is not None else "").strip()
    return ""


def _dashboard_numeric_value(value: Any) -> Optional[float]:
    match = re.search(r"-?\d+(?:\.\d+)?", str(value if value is not None else ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _dashboard_zeroish_value(value: Any) -> bool:
    numeric = _dashboard_numeric_value(value)
    return numeric is not None and abs(numeric) < 0.0001


def _dashboard_temperature_phrase(value: Any) -> str:
    token = str(value if value is not None else "").strip()
    numeric = _dashboard_numeric_value(token)
    if numeric is None:
        return token
    rounded = int(round(numeric))
    if re.search(r"\b[cf]\b|degrees?", token, flags=re.IGNORECASE):
        return f"{rounded} degrees"
    return token


def _dashboard_condition_phrase(value: Any) -> str:
    token = str(value if value is not None else "").strip()
    if not token or token in {"-", "n/a"}:
        return ""
    return token[:1].lower() + token[1:]


def _dashboard_sentence_case(value: str) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    return token[:1].upper() + token[1:]


def _dashboard_environment_forecast_rain_phrase(data: Dict[str, Any]) -> str:
    saw_precip_value = False
    wet_values: List[str] = []
    for card in data.get("forecast_cards") if isinstance(data.get("forecast_cards"), list) else []:
        if not isinstance(card, dict):
            continue
        for row_key in ("next_hours", "daily_forecast"):
            for row in card.get(row_key) if isinstance(card.get(row_key), list) else []:
                if not isinstance(row, dict):
                    continue
                condition = str(row.get("condition") or "").strip().lower()
                if any(token in condition for token in ("rain", "shower", "storm", "drizzle")):
                    wet_values.append(str(row.get("condition") or "").strip())
                    continue
                for key in ("rain", "precip", "chance_of_rain", "chance_rain"):
                    token = str(row.get(key) if row.get(key) is not None else "").strip()
                    if not token or token in {"-", "n/a", "None"}:
                        continue
                    saw_precip_value = True
                    if not _dashboard_zeroish_value(token):
                        wet_values.append(token)
    if wet_values:
        first = wet_values[0]
        return f"the forecast has rain to watch for ({first})" if first else "the forecast has rain to watch for"
    if saw_precip_value:
        return "no rain is showing in the near forecast"
    return ""


def _dashboard_environment_fallback_text(data: Dict[str, Any]) -> str:
    temp = _dashboard_environment_lookup(data, ("Outdoor Temp", "Outdoor", "Temp", "Current"))
    condition = _dashboard_environment_lookup(data, ("Forecast", "Current Conditions", "Condition"))
    humidity = _dashboard_environment_lookup(data, ("Humidity", "Outdoor Humidity"))
    wind = _dashboard_environment_lookup(data, ("Wind", "Wind Speed"))
    rain = _dashboard_environment_lookup(data, ("Rain Today", "Daily Rain", "Piezo Rain Today"))
    indoor = _dashboard_environment_lookup(data, ("Indoor", "Indoor Temp", "Indoor Temperature"))
    indoor_humidity = _dashboard_environment_lookup(data, ("Indoor Humidity",))
    uv = _dashboard_environment_lookup(data, ("UV", "UV Index"))
    aqi = _dashboard_environment_lookup(data, ("AQI", "Air Quality"))

    temp_phrase = _dashboard_temperature_phrase(temp)
    condition_phrase = _dashboard_condition_phrase(condition)
    if temp_phrase and condition_phrase:
        text = f"Outside, it is currently {temp_phrase} and {condition_phrase}."
    elif temp_phrase:
        text = f"Outside, it is currently {temp_phrase}."
    elif condition_phrase:
        text = f"Outside, conditions are {condition_phrase}."
    else:
        text = str(data.get("summary") or "Environment readings are available.").strip()

    detail_parts: List[str] = []
    forecast_rain = _dashboard_environment_forecast_rain_phrase(data)
    if forecast_rain:
        detail_parts.append(forecast_rain)
    elif rain:
        if _dashboard_zeroish_value(rain):
            detail_parts.append("no rain has been recorded today")
        else:
            detail_parts.append(f"rain today is {rain}")
    if wind and not _dashboard_zeroish_value(wind):
        detail_parts.append(f"wind is {wind}")
    if humidity:
        detail_parts.append(f"humidity is {humidity}")
    if indoor:
        indoor_phrase = f"inside is {indoor}"
        if indoor_humidity:
            indoor_phrase = f"{indoor_phrase} with {indoor_humidity} humidity"
        detail_parts.append(indoor_phrase)
    if uv and not _dashboard_zeroish_value(uv):
        detail_parts.append(f"UV is {uv}")
    if aqi and not _dashboard_zeroish_value(aqi):
        detail_parts.append(f"air quality is {aqi}")
    if detail_parts:
        text = f"{text} " + _dashboard_sentence_case(", ".join(detail_parts[:4])) + "."
    return _dashboard_short_text(text, limit=420)


def _dashboard_personal_fallback_text(data: Dict[str, Any]) -> str:
    calendar = data.get("calendar") if isinstance(data.get("calendar"), dict) else {}
    items = [item for item in calendar.get("items") or [] if isinstance(item, dict)]
    counts = calendar.get("counts_by_type") if isinstance(calendar.get("counts_by_type"), dict) else {}
    item_count = _dashboard_safe_int(calendar.get("item_count"), len(items))
    date_from = str(calendar.get("date_from") or "").strip()
    date_to = str(calendar.get("date_to") or "").strip()
    window = f"{date_from} through {date_to}" if date_from and date_to else "the next 7 days"
    person_label = str(data.get("person_label") or "").strip()
    subject = f"Personal for {person_label}" if person_label and person_label != "All people" else "Personal"

    if item_count <= 0 and not items:
        return f"{subject} is available, but there is nothing scheduled in the next 7 days yet."

    parts: List[str] = []
    for raw_type, label, plural in (
        ("calendar_event", "calendar event", "calendar events"),
        ("email_event", "email-derived plan", "email-derived plans"),
        ("action", "action item", "action items"),
        ("delivery", "delivery", "deliveries"),
        ("subscription", "renewal", "renewals"),
    ):
        count = _dashboard_safe_int(counts.get(raw_type), 0) if isinstance(counts, dict) else 0
        if count > 0:
            parts.append(f"{count} {label if count == 1 else plural}")
    if not parts and isinstance(counts, dict):
        for raw_type, raw_count in counts.items():
            count = _dashboard_safe_int(raw_count, 0)
            if count <= 0:
                continue
            label = _dashboard_personal_item_type_label(raw_type).lower()
            plural = f"{label[:-1]}ies" if label.endswith("y") else f"{label}s"
            parts.append(f"{count} {label if count == 1 else plural}")

    text = f"{subject} has {item_count} item{'s' if item_count != 1 else ''} on the {window} outlook"
    if parts:
        text = f"{text}: " + ", ".join(parts[:5])
    highlights = [
        _dashboard_short_text(
            " • ".join(part for part in [item.get("when"), item.get("title"), item.get("status")] if part),
            limit=120,
        )
        for item in items[:3]
    ]
    highlights = [item for item in highlights if item]
    if highlights:
        text = f"{text}. Up next: {'; '.join(highlights)}."
    else:
        text = f"{text}."
    return _dashboard_short_text(text, limit=520)


def _dashboard_fallback_brief(context: Dict[str, Any]) -> Dict[str, Any]:
    brief_id = str(context.get("id") or "").strip()
    title = str(context.get("title") or brief_id.title()).strip()
    data = context.get("data") if isinstance(context.get("data"), dict) else {}
    now = time.time()

    if brief_id == "overview":
        local_time = data.get("local_time") if isinstance(data.get("local_time"), dict) else _dashboard_time_greeting()
        greeting = str(local_time.get("greeting") or "Hello").strip()
        date_label = str(local_time.get("date") or "").strip()
        updates = data.get("updates") if isinstance(data.get("updates"), dict) else {}
        cards = [item for item in data.get("cards") or [] if isinstance(item, dict)]
        sections = [item for item in data.get("sections") or [] if isinstance(item, dict)]
        health_payload = data.get("health") if isinstance(data.get("health"), dict) else {}
        status = "running smoothly" if bool(health_payload.get("ok")) else "needs a little attention"
        parts = [f"Tater is {status}"]
        for section in sections:
            section_id = str(section.get("id") or "").strip()
            stats = _dashboard_stats(section.get("stats"))
            if section_id == "environment":
                temp = _dashboard_stat_value(stats, "Outdoor Temp")
                condition = str(section.get("summary") or "").strip()
                if temp:
                    parts.append(f"outside is {temp}")
                elif condition:
                    parts.append(_dashboard_short_text(condition, limit=110))
            elif section_id == "voice":
                connected = _dashboard_stat_value(stats, "Connected")
                if connected:
                    parts.append(f"{connected} voice satellite{'s are' if connected != '1' else ' is'} connected")
            elif section_id == "awareness":
                event_summary = section.get("event_summary") if isinstance(section.get("event_summary"), dict) else {}
                highlights = [item for item in event_summary.get("camera_highlights") or [] if isinstance(item, dict)]
                if highlights:
                    messages = [
                        _dashboard_short_text(item.get("message"), limit=90)
                        for item in highlights[-2:]
                        if _dashboard_short_text(item.get("message"), limit=90)
                    ]
                    if messages:
                        parts.append("cameras recently saw " + "; ".join(messages))
            elif section_id == "guardian":
                devices = _dashboard_stat_value(stats, "Devices") or _dashboard_stat_value(stats, "Known Devices")
                events = _dashboard_stat_value(stats, "Events") or _dashboard_stat_value(stats, "Recent Events")
                if devices:
                    detail = f"Guardian is tracking {devices} device{'s' if str(devices) != '1' else ''}"
                    if events:
                        detail = f"{detail} with {events} recent event{'s' if str(events) != '1' else ''}"
                    parts.append(detail)
            elif section_id == "personal":
                calendar = section.get("calendar") if isinstance(section.get("calendar"), dict) else {}
                item_count = _dashboard_safe_int(calendar.get("item_count"), 0)
                next_items = [item for item in calendar.get("next_items") or [] if isinstance(item, dict)]
                if item_count > 0:
                    first = next_items[0] if next_items else {}
                    first_title = _dashboard_short_text(first.get("title"), limit=80) if isinstance(first, dict) else ""
                    if first_title:
                        parts.append(f"Personal has {item_count} item{'s' if item_count != 1 else ''} this week, starting with {first_title}")
                    else:
                        parts.append(f"Personal has {item_count} item{'s' if item_count != 1 else ''} on the 7-day outlook")
        update_total = _dashboard_safe_int(updates.get("total")) if isinstance(updates, dict) else 0
        update_summary = str(updates.get("summary") or "").strip() if isinstance(updates, dict) else ""
        if update_total > 0:
            parts.append(f"{update_total} update{'s are' if update_total != 1 else ' is'} waiting ({update_summary})")
        if len(parts) == 1 and cards:
            parts.extend(
                _dashboard_short_text(f"{item.get('label')}: {item.get('value')}", limit=60)
                for item in cards[:3]
                if item.get("label") or item.get("value")
            )
        if date_label:
            text = f"{greeting}. For {date_label}, " + ", ".join(parts) + "."
        else:
            text = f"{greeting}. " + ", ".join(parts) + "."
    elif brief_id == "system":
        health_payload = data.get("health") if isinstance(data.get("health"), dict) else {}
        runtime = data.get("runtime") if isinstance(data.get("runtime"), dict) else {}
        updates = data.get("updates") if isinstance(data.get("updates"), dict) else {}
        status = "online" if bool(health_payload.get("ok")) else "degraded"
        jobs = _dashboard_safe_int(health_payload.get("hydra_jobs_active") or health_payload.get("chat_jobs_active"))
        llm = _dashboard_safe_int(health_payload.get("llm_calls_active"))
        cores = _dashboard_safe_int(health_payload.get("cores_running"))
        portals = _dashboard_safe_int(health_payload.get("portals_running"))
        update_total = _dashboard_safe_int(updates.get("total")) if isinstance(updates, dict) else 0
        update_errors = updates.get("errors") if isinstance(updates.get("errors"), list) else []
        update_sentence = (
            f" There {'are' if update_total != 1 else 'is'} {update_total} update{'s' if update_total != 1 else ''} available: {str(updates.get('summary') or '').strip()}."
            if update_total > 0
            else (
                " Update checks need attention for one or more sources."
                if update_errors
                else " Firmware, cores, portals, and Verba report no pending updates."
            )
        )
        text = (
            f"Tater is {status}. Runtime is calm with {jobs} Hydra job{'s' if jobs != 1 else ''}, "
            f"{llm} active LLM call{'s' if llm != 1 else ''}, {cores} running core{'s' if cores != 1 else ''}, "
            f"and {portals} running portal{'s' if portals != 1 else ''}.{update_sentence}"
        )
        if runtime.get("context_summary"):
            text = f"{text} {str(runtime.get('context_summary')).strip()}"
    elif brief_id == "voice":
        stats = _dashboard_stats(data.get("stats"))
        connected = _dashboard_stat_value(stats, "Connected") or "0"
        known = _dashboard_stat_value(stats, "Known Satellites") or _dashboard_stat_value(stats, "Known") or "0"
        stt = _dashboard_stat_value(stats, "STT Backend")
        tts = _dashboard_stat_value(stats, "TTS Backend")
        text = f"Voice has {connected} connected satellite{'s' if connected != '1' else ''} out of {known} known."
        if stt or tts:
            text = f"{text} STT is {stt or 'unknown'} and TTS is {tts or 'unknown'}."
    elif brief_id == "environment":
        text = _dashboard_environment_fallback_text(data)
    elif brief_id == "personal":
        text = _dashboard_personal_fallback_text(data)
    elif brief_id == "awareness":
        event_summary = data.get("event_summary") if isinstance(data.get("event_summary"), dict) else {}
        has_event_summary = "event_count" in event_summary
        event_count = _dashboard_safe_int(event_summary.get("event_count"))
        window = str(event_summary.get("window") or "last 12 hours").strip()
        if not has_event_summary:
            recent_events = [item for item in data.get("recent_events") or [] if isinstance(item, dict)]
            event_titles = [
                _dashboard_short_text(item.get("title") or item.get("detail") or item.get("subtitle"), limit=120)
                for item in recent_events[:4]
                if _dashboard_short_text(item.get("title") or item.get("detail") or item.get("subtitle"), limit=120)
            ]
            if event_titles:
                text = f"Awareness is live. Recent signals include {'; '.join(event_titles)}."
            else:
                text = "Awareness is live, but no 12-hour event summary is available yet."
        elif event_count <= 0:
            text = f"No awareness activity was recorded in the {window}."
        else:
            camera_events = _dashboard_awareness_camera_events(event_summary, limit=12)
            if not camera_events:
                text = f"Awareness had events in the {window}, but no camera image descriptions were available in the dashboard sample."
            else:
                area_counts = Counter(str(item.get("area") or item.get("source") or "Unknown").strip() for item in camera_events)
                areas = [area for area, _count in area_counts.most_common(3) if area]
                text = f"Cameras recorded {len(camera_events)} image event{'s' if len(camera_events) != 1 else ''} in the {window}"
                if areas:
                    text = f"{text}, mostly around {', '.join(areas)}"
            highlights = camera_events[-4:] if camera_events else []
            messages = [
                _dashboard_short_text(item.get("message") or item.get("title"), limit=140)
                for item in highlights[:4]
                if _dashboard_short_text(item.get("message") or item.get("title"), limit=140)
            ]
            if messages:
                text = f"{text}. Recent highlights: {'; '.join(messages)}."
            else:
                text = f"{text}."
    elif brief_id == "guardian":
        stats = _dashboard_stats(data.get("stats"))
        devices = _dashboard_stat_value(stats, "Devices") or _dashboard_stat_value(stats, "Known Devices") or _dashboard_stat_value(stats, "Device Count")
        events = _dashboard_stat_value(stats, "Events") or _dashboard_stat_value(stats, "Recent Events") or _dashboard_stat_value(stats, "Event Count")
        ok = _dashboard_stat_value(stats, "OK") or _dashboard_stat_value(stats, "Status")
        ai = _dashboard_stat_value(stats, "AI") or _dashboard_stat_value(stats, "AI Enabled")
        parts = []
        if devices:
            parts.append(f"{devices} tracked device{'s' if str(devices) != '1' else ''}")
        if events:
            parts.append(f"{events} recent event{'s' if str(events) != '1' else ''}")
        if ai:
            parts.append(f"AI {ai}")
        if ok:
            parts.append(f"status {ok}")
        event_rows = [item for item in data.get("events") or [] if isinstance(item, dict)]
        if event_rows:
            highlights = [
                _dashboard_short_text(item.get("title") or item.get("detail") or item.get("description") or item.get("subtitle"), limit=110)
                for item in event_rows[:3]
                if _dashboard_short_text(item.get("title") or item.get("detail") or item.get("description") or item.get("subtitle"), limit=110)
            ]
            if highlights:
                parts.append("recent signals include " + "; ".join(highlights))
        text = "Guardian is live"
        if parts:
            text = f"{text}: " + ", ".join(parts)
        text = f"{text}."
    else:
        text = str(data.get("summary") or "").strip() or "No summary is available yet."

    return {
        "id": brief_id,
        "title": title,
        "text": text.strip(),
        "source": str(context.get("source") or "").strip(),
        "updated_at": now,
        "mode": "live",
        "stale": False,
    }


def _dashboard_json_candidate(text: str) -> str:
    raw = str(text or "").strip()
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", raw, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        raw = fenced.group(1).strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]
    return raw.strip()


def _dashboard_relaxed_json_candidates(raw: str) -> List[str]:
    candidates = [raw]
    repaired = re.sub(r",(\s*[}\]])", r"\1", raw)
    repaired = re.sub(r"}\s*{", "},{", repaired)
    repaired = re.sub(r'(?<=[}\]"0-9])\s+(?="(?:id|title|text|briefs)"\s*:)', ", ", repaired)
    if repaired != raw:
        candidates.append(repaired)
    return candidates


def _dashboard_extract_array_body(raw: str, key: str) -> str:
    match = re.search(rf'"{re.escape(key)}"\s*:\s*\[', raw)
    if not match:
        return ""
    start = raw.find("[", match.start())
    if start < 0:
        return ""
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(raw)):
        char = raw[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return raw[start + 1 : index]
    return raw[start + 1 :]


def _dashboard_iter_jsonish_objects(raw: str) -> List[str]:
    objects: List[str] = []
    start: Optional[int] = None
    depth = 0
    in_string = False
    escaped = False
    for index, char in enumerate(raw):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            if depth == 0:
                start = index
            depth += 1
        elif char == "}" and depth:
            depth -= 1
            if depth == 0 and start is not None:
                objects.append(raw[start : index + 1])
                start = None
    return objects


def _dashboard_decode_json_fragment(value: str) -> str:
    try:
        decoded = json.loads(f'"{value}"')
        return str(decoded)
    except Exception:
        return value.replace('\\"', '"').replace("\\n", " ").replace("\\r", " ").strip()


def _dashboard_extract_string_field(raw: str, key: str) -> str:
    match = re.search(rf'"{re.escape(key)}"\s*:\s*"((?:\\.|[^"\\])*)"', raw, flags=re.DOTALL)
    if match:
        return _dashboard_decode_json_fragment(match.group(1)).strip()
    match = re.search(rf"'{re.escape(key)}'\s*:\s*'((?:\\.|[^'\\])*)'", raw, flags=re.DOTALL)
    if match:
        return match.group(1).replace("\\'", "'").replace("\\n", " ").replace("\\r", " ").strip()
    return ""


def _dashboard_salvage_briefs(raw: str) -> Dict[str, Any]:
    body = _dashboard_extract_array_body(raw, "briefs") or raw
    briefs: List[Dict[str, str]] = []
    for fragment in _dashboard_iter_jsonish_objects(body):
        parsed_fragment: Dict[str, Any] = {}
        for candidate in _dashboard_relaxed_json_candidates(fragment):
            try:
                maybe = json.loads(candidate)
            except json.JSONDecodeError:
                continue
            if isinstance(maybe, dict):
                parsed_fragment = maybe
                break
        if not parsed_fragment:
            row_id = _dashboard_extract_string_field(fragment, "id")
            text = _dashboard_extract_string_field(fragment, "text")
            title = _dashboard_extract_string_field(fragment, "title")
            if row_id or text:
                parsed_fragment = {"id": row_id, "text": text}
                if title:
                    parsed_fragment["title"] = title
        row_id = str(parsed_fragment.get("id") or "").strip()
        text = str(parsed_fragment.get("text") or "").strip()
        if row_id and text:
            row = {"id": row_id, "text": text}
            title = str(parsed_fragment.get("title") or "").strip()
            if title:
                row["title"] = title
            briefs.append(row)
    return {"briefs": briefs} if briefs else {}


def _dashboard_extract_json_object(text: str) -> Dict[str, Any]:
    raw = _dashboard_json_candidate(text)
    if not raw:
        return {}
    last_error = ""
    for candidate in _dashboard_relaxed_json_candidates(raw):
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as exc:
            last_error = str(exc)
            continue
        return parsed if isinstance(parsed, dict) else {}
    salvaged = _dashboard_salvage_briefs(raw)
    if salvaged:
        logger.debug("[dashboard] salvaged brief rows from malformed JSON: %s", last_error)
    elif last_error:
        logger.debug("[dashboard] ignored malformed brief JSON: %s", last_error)
    return salvaged


def _dashboard_primary_llm_client_kwargs() -> Dict[str, Any]:
    legacy_provider = _normalize_hydra_llm_provider(redis_client.get(HYDRA_LLM_PROVIDER_KEY))
    legacy_model = str(redis_client.get(HYDRA_LLM_MODEL_KEY) or "").strip()
    if _is_local_hydra_llm_provider(legacy_provider) and legacy_model:
        logger.info(
            "[dashboard] brief LLM using legacy primary local provider=%s model=%s",
            legacy_provider,
            legacy_model,
        )
        return {
            "redis_conn": redis_client,
            "provider": legacy_provider,
            "model": legacy_model,
        }

    try:
        rows = resolve_hydra_base_servers(redis_conn=redis_client, include_legacy=True)
    except Exception:
        rows = []
    primary = rows[0] if rows and isinstance(rows[0], dict) else {}
    provider = _normalize_hydra_llm_provider(primary.get("provider") if isinstance(primary, dict) else "")
    model = str(primary.get("model") or "").strip() if isinstance(primary, dict) else ""
    kwargs: Dict[str, Any] = {"redis_conn": redis_client}
    if provider and model:
        kwargs.update({"provider": provider, "model": model})
        if not _is_local_hydra_llm_provider(provider):
            kwargs["host"] = str(primary.get("endpoint") or "").strip()
            api_key = str(primary.get("api_key") or "").strip()
            if api_key:
                kwargs["api_key"] = api_key
        logger.info(
            "[dashboard] brief LLM using base row provider=%s model=%s host=%s",
            provider,
            model,
            str(kwargs.get("host") or ""),
        )
    else:
        logger.info("[dashboard] brief LLM using default primary resolver")
    return kwargs


async def _dashboard_awareness_camera_brief(llm_client: Any, context: Dict[str, Any]) -> str:
    data = context.get("data") if isinstance(context.get("data"), dict) else {}
    event_summary = data.get("event_summary") if isinstance(data.get("event_summary"), dict) else {}
    camera_events = _dashboard_awareness_camera_events(event_summary, limit=180)
    if not event_summary:
        return ""
    if not camera_events:
        return f"No camera image events were available in the {event_summary.get('window') or 'last 12 hours'}."

    payload = {
        "request": DASHBOARD_AWARENESS_CAMERA_SUMMARY_REQUEST,
        "time_window": {
            "label": event_summary.get("window") or "last 12 hours",
            "start": event_summary.get("start"),
            "end": event_summary.get("end"),
        },
        "candidate_event_count": int(event_summary.get("included_event_count") or len(camera_events)),
        "camera_event_count": len(camera_events),
        "all_event_count": int(event_summary.get("event_count") or 0),
        "truncated": bool(event_summary.get("truncated")),
        "top_areas": event_summary.get("top_areas") or [],
        "sources": event_summary.get("sources") or [],
        "camera_events": camera_events,
    }
    system_prompt = (
        "You write Tater dashboard summaries from Redis-backed Awareness camera events.\n"
        "Rules:\n"
        "- Use only camera_events and the aggregate metadata provided.\n"
        "- Summarize what the cameras saw in the last 12 hours, focusing on image descriptions and notable visual activity.\n"
        "- Group repeated observations naturally by area, subject, and time when the evidence supports it.\n"
        "- Ignore sensor-only events unless the aggregate metadata helps explain camera activity.\n"
        "- If the supplied events are truncated, say the summary is based on the included latest camera events; do not imply unseen details were read.\n"
        "- Do not mention internal tools, prompts, Redis, kernels, or implementation details.\n"
        "- Return two to four conversational sentences, no markdown and no bullet lists."
    )
    try:
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, default=str, ensure_ascii=False)},
            ],
            temperature=0.18,
            max_tokens=560,
            timeout=25,
            activity="dashboard_awareness_camera_brief",
        )
    except Exception:
        logger.debug("[dashboard] awareness camera brief generation failed", exc_info=True)
        return ""
    content = ""
    if isinstance(response, dict):
        message = response.get("message") if isinstance(response.get("message"), dict) else {}
        content = str(message.get("content") or "").strip()
    return _dashboard_short_text(content, limit=1800)


async def _dashboard_generate_briefs(
    contexts: List[Dict[str, Any]],
    *,
    brief_id: str = "",
) -> List[Dict[str, Any]]:
    target = str(brief_id or "").strip()
    selected_contexts = [
        context for context in contexts if isinstance(context, dict) and (not target or str(context.get("id") or "").strip() == target)
    ]
    if target and not selected_contexts:
        raise HTTPException(status_code=404, detail=f"Unknown dashboard brief: {target}")
    fallbacks = {str(context.get("id") or "").strip(): _dashboard_fallback_brief(context) for context in selected_contexts}
    if not selected_contexts:
        return []

    generated: Dict[str, str] = {}
    error_text = ""
    try:
        async with get_llm_client_from_env(**_dashboard_primary_llm_client_kwargs()) as llm_client:
            for context in selected_contexts:
                row_id = str(context.get("id") or "").strip()
                if row_id != "awareness":
                    continue
                text = await _dashboard_awareness_camera_brief(llm_client, context)
                if text:
                    generated[row_id] = text

            generic_contexts = [
                context
                for context in selected_contexts
                if str(context.get("id") or "").strip() not in generated
                and str(context.get("id") or "").strip() != "awareness"
            ]
            if generic_contexts:
                prompt_payload = {
                    "briefs": [
                        {
                            "id": context.get("id"),
                            "title": context.get("title"),
                            "source": context.get("source"),
                            "instructions": context.get("instructions"),
                            "data": context.get("data"),
                        }
                        for context in generic_contexts
                    ]
                }
                messages = [
                    {
                        "role": "system",
                        "content": (
                            "You write short operational dashboard briefs for Tater. "
                            "Use only the provided data and follow each brief's instructions. Return strict JSON as "
                            "{\"briefs\":[{\"id\":\"...\",\"text\":\"...\"}]}. "
                            "Do not wrap the JSON in markdown fences, and escape quotation marks inside JSON strings. "
                            "Most texts should be one or two natural sentences. Awareness may be a richer two to four sentence activity digest "
                            "that names what happened, where it happened, and what seems notable or quiet. "
                            "Use no markdown, no bullet lists, and no invented sensor values."
                        ),
                    },
                    {"role": "user", "content": json.dumps(prompt_payload, default=str, ensure_ascii=False)},
                ]
                response = await llm_client.chat(
                    messages,
                    temperature=0.35,
                    max_tokens=700,
                    timeout=20,
                    activity="dashboard_briefs",
                )
                content = ""
                if isinstance(response, dict):
                    message = response.get("message") if isinstance(response.get("message"), dict) else {}
                    content = str(message.get("content") or "").strip()
                parsed = _dashboard_extract_json_object(content)
                for row in parsed.get("briefs") if isinstance(parsed.get("briefs"), list) else []:
                    if not isinstance(row, dict):
                        continue
                    row_id = str(row.get("id") or "").strip()
                    text = str(row.get("text") or "").strip()
                    if row_id in fallbacks and text:
                        generated[row_id] = text
    except Exception as exc:
        error_text = str(exc)
        logger.info("[dashboard] brief generation fell back: %s", exc)

    now = time.time()
    rows: List[Dict[str, Any]] = []
    for context in selected_contexts:
        row_id = str(context.get("id") or "").strip()
        fallback = dict(fallbacks.get(row_id) or _dashboard_fallback_brief(context))
        text = generated.get(row_id) or str(fallback.get("text") or "").strip()
        row = {
            **fallback,
            "text": text,
            "updated_at": now,
            "mode": "generated" if row_id in generated else "fallback",
            "stale": False,
        }
        if error_text and row_id not in generated:
            row["error"] = error_text
        await run_dashboard(_dashboard_save_brief, row)
        rows.append(row)
    return rows


def _dashboard_stale_brief_ids(
    contexts: List[Dict[str, Any]],
    cached: Dict[str, Dict[str, Any]],
    *,
    now: Optional[float] = None,
    ttl_seconds: Optional[int] = None,
) -> List[str]:
    if ttl_seconds is None:
        ttl = _dashboard_brief_ttl_seconds()
    else:
        ttl = _dashboard_safe_int(ttl_seconds, DASHBOARD_BRIEF_TTL_SECONDS)
    if ttl <= 0:
        return []
    current = time.time() if now is None else float(now)
    stale: List[str] = []
    for context in contexts or []:
        row_id = str((context or {}).get("id") or "").strip()
        if not row_id:
            continue
        row = cached.get(row_id)
        updated_at = float(row.get("updated_at") or 0.0) if isinstance(row, dict) else 0.0
        if not isinstance(row, dict) or not row.get("text") or not updated_at or (current - updated_at) > ttl:
            stale.append(row_id)
    return stale


def _dashboard_brief_refresh_snapshot(stale_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    with dashboard_brief_refresh_lock:
        state = dict(dashboard_brief_refresh_state)
        state["last_ids"] = list(state.get("last_ids") or [])
    state["stale_ids"] = [str(item).strip() for item in (stale_ids or []) if str(item).strip()]
    ttl = _dashboard_brief_ttl_seconds()
    state["check_interval_seconds"] = _dashboard_brief_check_interval_seconds()
    state["ttl_seconds"] = ttl
    state["auto_enabled"] = bool(ttl > 0)
    return state


def _dashboard_mark_brief_refresh_started(*, reason: str, brief_ids: List[str], force: bool = False) -> bool:
    now = time.time()
    cleaned_ids = [str(item).strip() for item in (brief_ids or []) if str(item).strip()]
    with dashboard_brief_refresh_lock:
        if bool(dashboard_brief_refresh_state.get("running")):
            return False
        last_error = str(dashboard_brief_refresh_state.get("last_error") or "").strip()
        finished_at = float(dashboard_brief_refresh_state.get("finished_at") or 0.0)
        if not force and last_error and finished_at and (now - finished_at) < DASHBOARD_BRIEF_RETRY_SECONDS:
            return False
        dashboard_brief_refresh_state.update(
            {
                "running": True,
                "started_at": now,
                "finished_at": 0.0,
                "last_error": "",
                "last_reason": str(reason or "").strip() or "refresh",
                "last_ids": cleaned_ids,
            }
        )
    return True


def _dashboard_mark_brief_refresh_finished(*, error: str = "") -> None:
    with dashboard_brief_refresh_lock:
        dashboard_brief_refresh_state.update(
            {
                "running": False,
                "finished_at": time.time(),
                "last_error": str(error or "").strip(),
            }
        )


async def _dashboard_refresh_briefs_job(
    contexts: List[Dict[str, Any]],
    *,
    brief_id: str = "",
    reason: str = "scheduler",
    force: bool = False,
) -> bool:
    target = str(brief_id or "").strip()
    ids = [target] if target else [str((context or {}).get("id") or "").strip() for context in contexts or []]
    ids = [item for item in ids if item]
    if not ids:
        return False
    if not _dashboard_mark_brief_refresh_started(reason=reason, brief_ids=ids, force=force):
        return False
    error = ""
    try:
        await _dashboard_generate_briefs(contexts, brief_id=target)
    except Exception as exc:
        error = str(exc)
        logger.info("[dashboard] background brief refresh failed reason=%s: %s", reason, exc)
    finally:
        _dashboard_mark_brief_refresh_finished(error=error)
    return not bool(error)


def _dashboard_schedule_brief_refresh(
    contexts: List[Dict[str, Any]],
    *,
    brief_id: str = "",
    stale_ids: Optional[List[str]] = None,
    reason: str = "dashboard",
    force: bool = False,
) -> bool:
    target = str(brief_id or "").strip()
    wanted = {target} if target else {str(item).strip() for item in (stale_ids or []) if str(item).strip()}
    selected = [
        context
        for context in contexts or []
        if isinstance(context, dict)
        and (not wanted or str(context.get("id") or "").strip() in wanted)
    ]
    ids = [str(context.get("id") or "").strip() for context in selected if str(context.get("id") or "").strip()]
    if not ids:
        return False
    if not _dashboard_mark_brief_refresh_started(reason=reason, brief_ids=ids, force=force):
        return False

    async def runner() -> None:
        error = ""
        try:
            await _dashboard_generate_briefs(selected, brief_id=target)
        except Exception as exc:
            error = str(exc)
            logger.info("[dashboard] queued brief refresh failed reason=%s: %s", reason, exc)
        finally:
            _dashboard_mark_brief_refresh_finished(error=error)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _dashboard_mark_brief_refresh_finished(error="dashboard event loop unavailable")
        return False
    loop.create_task(runner())
    return True


async def _dashboard_brief_scheduler_loop() -> None:
    await asyncio.sleep(float(DASHBOARD_BRIEF_STARTUP_DELAY_SECONDS))
    while True:
        interval = _dashboard_brief_check_interval_seconds()
        try:
            if interval > 0:
                snapshot = await run_dashboard(_dashboard_build_snapshot)
                await run_dashboard(_dashboard_save_snapshot, snapshot)
                contexts = _dashboard_brief_contexts(snapshot)
                cached_rows = await run_dashboard(_dashboard_cache_rows)
                stale_ids = _dashboard_stale_brief_ids(contexts, cached_rows)
                if stale_ids:
                    await _dashboard_refresh_briefs_job(
                        [context for context in contexts if str(context.get("id") or "").strip() in set(stale_ids)],
                        reason="scheduler",
                    )
                    await run_dashboard(_dashboard_save_snapshot, snapshot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[dashboard] brief scheduler tick failed: %s", exc, exc_info=True)
        await asyncio.sleep(float(max(60, interval or 300)))


def _start_dashboard_brief_scheduler() -> None:
    global dashboard_brief_scheduler_task
    if dashboard_brief_scheduler_task and not dashboard_brief_scheduler_task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    dashboard_brief_scheduler_task = loop.create_task(_dashboard_brief_scheduler_loop())


async def _stop_dashboard_brief_scheduler() -> None:
    global dashboard_brief_scheduler_task
    task = dashboard_brief_scheduler_task
    dashboard_brief_scheduler_task = None
    if not task:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def _dashboard_build_snapshot() -> Dict[str, Any]:
    health_payload = health()
    hydra_jobs = _chat_job_counts_with_breakdown(include_history=False)
    llm_calls = get_llm_call_runtime_summary(include_history=False)
    vision_calls = get_vision_call_runtime_summary(include_history=False)
    updates = _dashboard_updates_snapshot()

    voice_payload = _dashboard_esphome_payload()
    voice_section = _dashboard_section(
        section_id="voice",
        title="Voice",
        subtitle="Satellites, wake detection, and speech pipeline",
        payload=voice_payload,
        stats_limit=8,
        require_signal=False,
    )
    if isinstance(voice_section, dict):
        voice_devices = _dashboard_core_items(
            voice_payload,
            groups=("satellite",),
            limit=0,
            require_image=False,
        )
        voice_section["devices"] = [device for device in voice_devices if bool(device.get("connected"))][:6]
        if isinstance(voice_payload, dict):
            if isinstance(voice_payload.get("speaker_id"), dict):
                voice_section["speaker_id"] = voice_payload.get("speaker_id")
            if isinstance(voice_payload.get("emotion_id"), dict):
                voice_section["emotion_id"] = voice_payload.get("emotion_id")

    environment_payload = _dashboard_tab_payload("environment_core")
    environment_section = _dashboard_section(
        section_id="environment",
        title="Environment",
        subtitle="Weather and live home readings",
        payload=environment_payload,
        stats_limit=8,
        require_signal=True,
    )
    if isinstance(environment_section, dict):
        environment_section["visuals"] = _dashboard_core_items(
            environment_payload,
            groups=("overview", "forecast"),
            limit=4,
            require_image=True,
        )
        environment_section["brief_context"] = _dashboard_environment_context(environment_payload, environment_section)

    personal_settings = _dashboard_personal_settings()
    personal_payload = _dashboard_tab_payload("personal_core")
    personal_section = _dashboard_section(
        section_id="personal",
        title="Personal",
        subtitle="7-day calendar, plans, deliveries, and actions",
        payload=personal_payload,
        stats_limit=8,
        require_signal=False,
    )
    if isinstance(personal_section, dict):
        personal_section["person_id"] = personal_settings.get("person_id")
        personal_section["person_label"] = personal_settings.get("person_label")
        personal_section["personal_settings"] = personal_settings
        personal_calendar = _dashboard_personal_calendar_payload(str(personal_settings.get("person_id") or ""))
        personal_context = _dashboard_personal_context(personal_payload, personal_section, personal_calendar)
        personal_section["brief_context"] = personal_context
        calendar = personal_context.get("calendar") if isinstance(personal_context.get("calendar"), dict) else {}
        personal_section["outlook_items"] = [
            item for item in calendar.get("items") or [] if isinstance(item, dict)
        ][:8]
        counts = calendar.get("counts_by_type") if isinstance(calendar.get("counts_by_type"), dict) else {}
        personal_section["type_counts"] = [
            {"label": _dashboard_personal_item_type_label(key), "value": value}
            for key, value in counts.items()
            if _dashboard_safe_int(value) > 0
        ][:6]

    awareness_payload = _dashboard_tab_payload("awareness_core")
    awareness_section = _dashboard_section(
        section_id="awareness",
        title="Awareness",
        subtitle="Recent home activity and event signals",
        payload=awareness_payload,
        stats_limit=8,
        require_signal=True,
    )
    if isinstance(awareness_section, dict):
        awareness_section["event_summary"] = _dashboard_awareness_events_summary(hours=12)
        awareness_section["snapshots"] = _dashboard_core_items(
            awareness_payload,
            groups=("event",),
            limit=6,
            require_image=True,
        )
        awareness_section["recent_events"] = _dashboard_core_items(
            awareness_payload,
            groups=("event",),
            limit=8,
            require_image=False,
            include_image=False,
        )

    guardian_payload = _dashboard_tab_payload("guardian_core")
    guardian_section = _dashboard_section(
        section_id="guardian",
        title="Guardian",
        subtitle="Device protection, events, and network signals",
        payload=guardian_payload,
        stats_limit=8,
        require_signal=False,
    )
    if isinstance(guardian_section, dict):
        guardian_items = _dashboard_core_items(
            guardian_payload,
            groups=(),
            limit=10,
            require_image=False,
            include_image=False,
        )
        guardian_section["devices"] = [
            item for item in guardian_items
            if str(item.get("group") or "").strip().lower() in {"device", "devices", "network", "host", "client"}
        ][:6]
        guardian_section["events"] = [
            item for item in guardian_items
            if str(item.get("group") or "").strip().lower() in {"event", "events", "alert", "alerts", "security"}
        ][:6]
        if not guardian_section["devices"] and not guardian_section["events"]:
            guardian_section["devices"] = guardian_items[:6]

    cards: List[Dict[str, Any]] = [
        {
            "id": "tater",
            "label": _dashboard_redis_text(redis_client.get("tater:first_name")) or "Tater",
            "value": "Online" if bool(health_payload.get("ok")) else "Degraded",
            "detail": "Redis connected" if bool(health_payload.get("redis")) else "Redis needs attention",
            "tone": "good" if bool(health_payload.get("ok")) else "warning",
        },
        {
            "id": "hydra",
            "label": "Hydra",
            "value": f"{int(hydra_jobs.get('total') or 0)} active",
            "detail": f"{int(llm_calls.get('active_total') or 0)} LLM calls, {int(vision_calls.get('active_total') or 0)} vision calls",
            "tone": "normal",
        },
        {
            "id": "surfaces",
            "label": "Surfaces",
            "value": f"{int(health_payload.get('cores_running') or 0)} cores",
            "detail": f"{int(health_payload.get('portals_running') or 0)} portals running",
            "tone": "normal",
        },
    ]
    if isinstance(voice_section, dict):
        connected = _dashboard_stat_value(_dashboard_stats(voice_section.get("stats")), "Connected")
        known = _dashboard_stat_value(_dashboard_stats(voice_section.get("stats")), "Known Satellites")
        cards.append(
            {
                "id": "voice",
                "label": "Voice",
                "value": f"{connected or '0'} connected",
                "detail": f"{known or '0'} known satellites",
                "tone": "good" if _dashboard_safe_int(connected) > 0 else "normal",
            }
        )
    if isinstance(environment_section, dict):
        temp = _dashboard_stat_value(_dashboard_stats(environment_section.get("stats")), "Outdoor Temp")
        cards.append(
            {
                "id": "environment",
                "label": "Outside",
                "value": temp or "Live",
                "detail": "Environment core",
                "tone": "normal",
            }
        )
    if isinstance(personal_section, dict):
        personal_context = personal_section.get("brief_context") if isinstance(personal_section.get("brief_context"), dict) else {}
        calendar = personal_context.get("calendar") if isinstance(personal_context.get("calendar"), dict) else {}
        item_count = _dashboard_safe_int(calendar.get("item_count"), len(calendar.get("items") or []))
        counts = calendar.get("counts_by_type") if isinstance(calendar.get("counts_by_type"), dict) else {}
        top_parts = [
            f"{_dashboard_safe_int(value)} {_dashboard_personal_item_type_label(key).lower()}"
            for key, value in counts.items()
            if _dashboard_safe_int(value) > 0
        ]
        selected_label = str(personal_settings.get("person_label") or "All people").strip() or "All people"
        detail = selected_label if selected_label != "All people" else "All people"
        if top_parts:
            detail = f"{detail} • {', '.join(top_parts[:2])}"
        cards.append(
            {
                "id": "personal",
                "label": "Personal",
                "value": f"{item_count} this week",
                "detail": detail,
                "tone": "normal",
            }
        )
    if isinstance(awareness_section, dict):
        total = _dashboard_stat_value(_dashboard_stats(awareness_section.get("stats")), "Total Events")
        cards.append(
            {
                "id": "awareness",
                "label": "Awareness",
                "value": f"{total or '0'} events",
                "detail": "Activity history",
                "tone": "normal",
            }
        )
    if isinstance(guardian_section, dict):
        guardian_stats = _dashboard_stats(guardian_section.get("stats"))
        devices = (
            _dashboard_stat_value(guardian_stats, "Devices")
            or _dashboard_stat_value(guardian_stats, "Known Devices")
            or _dashboard_stat_value(guardian_stats, "Device Count")
        )
        events = (
            _dashboard_stat_value(guardian_stats, "Events")
            or _dashboard_stat_value(guardian_stats, "Recent Events")
            or _dashboard_stat_value(guardian_stats, "Event Count")
        )
        ai = _dashboard_stat_value(guardian_stats, "AI") or _dashboard_stat_value(guardian_stats, "AI Enabled")
        cards.append(
            {
                "id": "guardian",
                "label": "Guardian",
                "value": f"{devices or 'Live'} devices" if devices else "Live",
                "detail": f"{events or '0'} events" + (f" • AI {ai}" if ai else ""),
                "tone": "normal",
            }
        )

    sections = [
        section
        for section in (voice_section, environment_section, personal_section, awareness_section, guardian_section)
        if isinstance(section, dict)
    ]
    return {
        "health": health_payload,
        "runtime": {
            "hydra_jobs": hydra_jobs,
            "llm_calls": llm_calls,
            "vision_calls": vision_calls,
        },
        "updates": updates,
        "cards": cards,
        "sections": sections,
        "voice_section": voice_section,
        "environment_section": environment_section,
        "personal_section": personal_section,
        "awareness_section": awareness_section,
        "guardian_section": guardian_section,
        "settings": {
            "personal": personal_settings,
            "refresh": _dashboard_refresh_settings(),
        },
    }


async def _dashboard_payload(
    *,
    refresh_briefs: bool = False,
    brief_id: str = "",
    refresh_snapshot: bool = False,
) -> Dict[str, Any]:
    snapshot_meta: Dict[str, Any]
    rebuilt_snapshot = False
    if refresh_snapshot:
        snapshot = await run_dashboard(_dashboard_build_snapshot)
        rebuilt_snapshot = True
        snapshot_meta = _dashboard_snapshot_meta(source="live")
    else:
        cached_snapshot, snapshot_meta = await run_dashboard(_dashboard_load_snapshot_cache)
        if isinstance(cached_snapshot, dict) and not bool(snapshot_meta.get("stale")):
            snapshot = cached_snapshot
        elif isinstance(cached_snapshot, dict):
            snapshot = cached_snapshot
            _dashboard_schedule_snapshot_refresh(
                reason="brief-refresh-stale" if refresh_briefs else "dashboard-stale",
                force=False,
            )
        else:
            snapshot = await run_dashboard(_dashboard_build_snapshot)
            rebuilt_snapshot = True
            snapshot_meta = _dashboard_snapshot_meta(source="live")
    contexts = _dashboard_brief_contexts(snapshot)
    context_ids = {str(context.get("id") or "").strip() for context in contexts}
    cached = await run_dashboard(_dashboard_cache_rows)
    now = time.time()
    brief_ttl_seconds = await run_dashboard(_dashboard_brief_ttl_seconds)

    stale_ids = _dashboard_stale_brief_ids(contexts, cached, now=now, ttl_seconds=brief_ttl_seconds)
    target_id = str(brief_id or "").strip()
    if target_id and target_id not in context_ids:
        raise HTTPException(status_code=404, detail=f"Unknown dashboard brief: {target_id}")
    if refresh_briefs:
        _dashboard_schedule_brief_refresh(contexts, brief_id=target_id, reason="manual", force=True)
    elif stale_ids:
        _dashboard_schedule_brief_refresh(contexts, stale_ids=stale_ids, reason="dashboard", force=False)

    if rebuilt_snapshot:
        snapshot_meta = await run_dashboard(_dashboard_save_snapshot, snapshot)

    now = time.time()
    briefs: List[Dict[str, Any]] = []
    for context in contexts:
        row_id = str(context.get("id") or "").strip()
        row = cached.get(row_id)
        if isinstance(row, dict) and row.get("text"):
            out = dict(row)
            updated_at = float(out.get("updated_at") or 0.0)
            out["stale"] = bool(brief_ttl_seconds > 0 and updated_at and (now - updated_at) > brief_ttl_seconds)
            briefs.append(out)
        else:
            briefs.append(_dashboard_fallback_brief(context))

    settings = snapshot.get("settings") if isinstance(snapshot.get("settings"), dict) else {}
    settings = dict(settings)
    settings["refresh"] = await run_dashboard(_dashboard_refresh_settings)

    return {
        "ok": True,
        "generated_at": float(snapshot_meta.get("cached_at") or time.time()),
        "snapshot": snapshot_meta,
        "snapshot_refresh": _dashboard_snapshot_refresh_snapshot(),
        "brief_ttl_seconds": brief_ttl_seconds,
        "brief_refresh": _dashboard_brief_refresh_snapshot(stale_ids),
        "cards": snapshot.get("cards") or [],
        "updates": snapshot.get("updates") if isinstance(snapshot.get("updates"), dict) else {},
        "sections": snapshot.get("sections") or [],
        "briefs": [row for row in briefs if str(row.get("id") or "").strip() in context_ids],
        "settings": settings,
    }


def _redis_setup_payload(payload: RedisSetupRequest) -> Dict[str, Any]:
    raw_password = payload.password
    password = "" if raw_password is None else str(raw_password)
    keep_existing = bool(payload.keep_existing_password)
    if keep_existing and not password:
        existing = get_redis_connection_config(include_secret=True)
        password = str(existing.get("password") or "")
    return {
        "mode": str(payload.mode or "").strip() or "internal",
        "host": str(payload.host or "").strip(),
        "port": int(payload.port if payload.port is not None else 6379),
        "db": int(payload.db if payload.db is not None else 0),
        "username": str(payload.username or "").strip(),
        "password": password,
        "use_tls": bool(payload.use_tls),
        "verify_tls": bool(payload.verify_tls),
        "ca_cert_path": str(payload.ca_cert_path or "").strip(),
        "data_path": str(payload.data_path or "").strip(),
    }


@app.get("/api/redis/status")
def redis_status() -> Dict[str, Any]:
    return get_redis_connection_status()


@app.post("/api/redis/configure")
def redis_configure(payload: RedisSetupRequest) -> Dict[str, Any]:
    config_payload = _redis_setup_payload(payload)
    ok, error = test_redis_connection_settings(config_payload)
    if not ok:
        raise HTTPException(status_code=400, detail=f"Redis connection failed: {error}")
    if bool(payload.test_only):
        # In test-only mode, echo the tested values so the UI doesn't overwrite
        # in-progress form edits with the last saved Redis config.
        status = get_redis_connection_status()
        status.update(
            {
                "configured": str(config_payload.get("mode") or "").strip().lower() != "external"
                or bool(str(config_payload.get("host") or "").strip()),
                "connected": True,
                "error": "",
                "mode": str(config_payload.get("mode") or "internal"),
                "internal": str(config_payload.get("mode") or "").strip().lower() != "external",
                "host": str(config_payload.get("host") or ""),
                "port": int(config_payload.get("port") or 6379),
                "db": int(config_payload.get("db") or 0),
                "username": str(config_payload.get("username") or ""),
                "use_tls": bool(config_payload.get("use_tls")),
                "verify_tls": bool(config_payload.get("verify_tls")),
                "ca_cert_path": str(config_payload.get("ca_cert_path") or ""),
                "password_set": bool(str(config_payload.get("password") or "")),
                "data_path": str(config_payload.get("data_path") or status.get("data_path") or ""),
            }
        )
        return {
            **status,
            "saved": False,
        }

    save_redis_connection_settings(config_payload)
    bootstrap_replay = _replay_startup_after_redis_configure()
    status = get_redis_connection_status()
    return {
        **status,
        "saved": True,
        "bootstrap_replay": bootstrap_replay,
    }


@app.post("/api/redis/migrate/internal")
def redis_migrate_internal(payload: RedisMigrateInternalRequest) -> Dict[str, Any]:
    try:
        migration_payload = _run_redis_maintenance_with_runtime_pause(
            action="redis-migrate-internal",
            operation=lambda: migrate_current_redis_to_internal(
                data_path=str(payload.data_path or "").strip(),
                flush_internal=bool(payload.flush_internal),
            ),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=f"Redis migration blocked: {exc}")
    except RedisError as exc:
        raise HTTPException(status_code=503, detail=f"Redis migration failed: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Redis migration failed: {exc}")

    bootstrap_replay = _replay_startup_after_redis_configure()
    status = get_redis_connection_status()
    return {
        **status,
        "migrated": bool(migration_payload.get("switched")) or bool(status.get("internal")),
        "migration": migration_payload,
        "bootstrap_replay": bootstrap_replay,
    }


@app.get("/api/redis/encryption/status")
def redis_encryption_status() -> Dict[str, Any]:
    try:
        return get_redis_encryption_status()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read Redis encryption status: {exc}")


@app.post("/api/redis/encryption/key")
def redis_encryption_key() -> Dict[str, Any]:
    try:
        return ensure_redis_encryption_key()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to initialize Redis encryption key: {exc}")


@app.post("/api/redis/encryption/encrypt")
def redis_encryption_encrypt() -> Dict[str, Any]:
    try:
        payload = _run_redis_maintenance_with_runtime_pause(
            action="redis-encrypt",
            operation=encrypt_current_redis_snapshot,
        )
        payload["encryption_status"] = get_redis_encryption_status()
        return payload
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=f"Redis encryption blocked: {exc}")
    except RedisError as exc:
        raise HTTPException(status_code=503, detail=f"Redis encryption failed: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Redis encryption failed: {exc}")


@app.post("/api/redis/encryption/decrypt")
def redis_encryption_decrypt() -> Dict[str, Any]:
    try:
        payload = _run_redis_maintenance_with_runtime_pause(
            action="redis-decrypt",
            operation=lambda: decrypt_current_redis_snapshot(flush_before_restore=True),
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=f"Redis decryption blocked: {exc}")
    except RedisError as exc:
        raise HTTPException(status_code=503, detail=f"Redis decryption failed: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Redis decryption failed: {exc}")

    bootstrap_replay = _replay_startup_after_redis_configure()
    return {
        **payload,
        "bootstrap_replay": bootstrap_replay,
        "encryption_status": get_redis_encryption_status(),
    }


@app.get("/api/health")
def health() -> Dict[str, Any]:
    redis_status_payload = get_redis_connection_status()
    redis_ok = bool(redis_status_payload.get("connected"))

    verbas_enabled = 0
    try:
        registry = verba_registry_module.get_verba_registry_snapshot() or {}
        for plugin_id in registry.keys():
            try:
                if get_verba_enabled(str(plugin_id or "").strip()):
                    verbas_enabled += 1
            except Exception:
                continue
    except Exception:
        verbas_enabled = 0

    hydra_job_counts = _chat_job_counts_with_breakdown()
    llm_call_counts = get_llm_call_runtime_summary(include_history=False)
    vision_call_counts = get_vision_call_runtime_summary()
    loaded_models = _runtime_loaded_models_snapshot(include_models=False)

    return {
        "ok": redis_ok,
        "redis": redis_ok,
        "redis_status": redis_status_payload,
        "verbas_enabled": int(verbas_enabled),
        "cores_running": len([k for k in core_runtime.threads if core_runtime.is_running(k)]),
        "esphome_running": bool(esphome_home_module.is_running()),
        "portals_running": len([k for k in portal_runtime.threads if portal_runtime.is_running(k)]),
        "hydra_jobs_active": int(hydra_job_counts.get("total") or 0),
        "chat_jobs_active": int(hydra_job_counts.get("total") or 0),  # Backward-compatible key for older clients.
        "llm_calls_active": int(llm_call_counts.get("active_total") or 0),
        "voice_calls_active": int(vision_call_counts.get("active_total") or 0),  # Alias for UI voice wording.
        "vision_calls_active": int(vision_call_counts.get("active_total") or 0),
        "loaded_models": loaded_models,
        "bootstrap": {
            "restore_enabled": bool(bootstrap_state.get("restore_enabled")),
            "autostart_enabled": bool(bootstrap_state.get("autostart_enabled")),
            "restore_in_progress": bool(bootstrap_state.get("restore_in_progress")),
            "restore_complete": bool(bootstrap_state.get("restore_complete")),
            "restore_error": str(bootstrap_state.get("restore_error") or ""),
            "restore_summary": dict(bootstrap_state.get("restore_summary") or {}),
        },
    }


@app.get("/api/dashboard")
async def dashboard(refresh_briefs: bool = False, refresh_snapshot: bool = False) -> Dict[str, Any]:
    return await _dashboard_payload(refresh_briefs=bool(refresh_briefs), refresh_snapshot=bool(refresh_snapshot))


@app.post("/api/dashboard/briefs/refresh")
async def dashboard_briefs_refresh(payload: DashboardBriefRefreshRequest) -> Dict[str, Any]:
    return await _dashboard_payload(refresh_briefs=True, brief_id=str(payload.brief_id or "").strip())


@app.post("/api/dashboard/settings")
async def dashboard_settings(payload: DashboardSettingsRequest) -> Dict[str, Any]:
    fields_set = getattr(payload, "model_fields_set", None)
    if fields_set is None:
        fields_set = getattr(payload, "__fields_set__", set())
    fields_set = set(fields_set or set())
    if "personal_person_id" in fields_set:
        await run_dashboard(_dashboard_save_personal_settings, payload.personal_person_id)
    if {"refresh_interval_seconds", "brief_refresh_interval_seconds"} & fields_set:
        await run_dashboard(
            _dashboard_save_refresh_settings,
            refresh_interval_seconds=payload.refresh_interval_seconds,
            brief_refresh_interval_seconds=payload.brief_refresh_interval_seconds,
        )
    return await _dashboard_payload(refresh_snapshot=True)


def _spudex_platform_token(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    for suffix in ("_portal",):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return normalize_platform(text) or text


def _spudex_clean_platform_label(value: Any, fallback: str = "") -> str:
    text = str(value or fallback or "").strip()
    text = re.sub(r"\s+Portal\s+Settings$", "", text, flags=re.IGNORECASE)
    return text or fallback or "Unknown"


def _spudex_platform_options(settings: Any) -> List[Dict[str, Any]]:
    settings_map = settings if isinstance(settings, dict) else {}
    allowed = {
        str(item or "").strip().lower()
        for item in settings_map.get("allowed_platforms", [])
        if str(item or "").strip()
    }
    options: Dict[str, Dict[str, Any]] = {}

    def add(value: Any, label: str, *, running: bool, description: str, kind: str) -> None:
        token = _spudex_platform_token(value)
        if not token:
            return
        existing = options.get(token)
        row = {
            "value": token,
            "label": _spudex_clean_platform_label(label, _runtime_platform_label(token)),
            "running": bool(running),
            "description": str(description or "").strip(),
            "kind": str(kind or "").strip(),
            "saved": token in allowed,
        }
        if existing:
            existing["running"] = bool(existing.get("running")) or row["running"]
            existing["saved"] = bool(existing.get("saved")) or row["saved"]
            if row["description"] and not existing.get("description"):
                existing["description"] = row["description"]
            return
        options[token] = row

    add("webui", "Web UI", running=True, description="Tater browser UI", kind="built_in")
    if esphome_home_module.is_running():
        add("voice_core", "Native Voice", running=True, description="ESPHome satellite voice path", kind="built_in")
    if core_runtime.is_running("voice_core"):
        add("voice_core", "Native Voice", running=True, description="Running native voice core", kind="core")

    try:
        portal_entries = portal_registry_module.refresh_portal_registry()
    except Exception:
        portal_entries = []
    for portal in portal_entries:
        key = str(portal.get("key") or "").strip()
        if not key:
            continue
        token = _spudex_platform_token(key)
        running = portal_runtime.is_running(key)
        if not running and token not in allowed:
            continue
        label = _spudex_clean_platform_label(portal.get("label"), _runtime_platform_label(token))
        description = "Running portal" if running else "Saved platform, currently stopped"
        add(token, label, running=running, description=description, kind="portal")

    for token in sorted(allowed):
        if token == "all":
            add("all", "All platforms", running=True, description="Allow Spudex anywhere Hydra is running", kind="special")
        elif token not in options:
            add(token, _runtime_platform_label(token), running=False, description="Saved platform, currently stopped", kind="saved")

    def sort_key(row: Dict[str, Any]) -> Tuple[int, int, str]:
        value = str(row.get("value") or "")
        if value == "webui":
            return (0, 0, "")
        if value == "all":
            return (2, 0, "")
        return (1, 0 if row.get("running") else 1, str(row.get("label") or value).lower())

    return sorted(options.values(), key=sort_key)


@app.get("/api/spudex")
def get_spudex_state() -> Dict[str, Any]:
    from spudex.runner import spudex_payload

    payload = spudex_payload(redis_client)
    payload["platform_options"] = _spudex_platform_options(payload.get("settings") if isinstance(payload, dict) else {})
    return payload


@app.post("/api/spudex/settings")
def update_spudex_settings(payload: SpudexSettingsRequest) -> Dict[str, Any]:
    from spudex.settings import save_spudex_settings

    settings = save_spudex_settings(payload.values, redis_client)
    return {"ok": True, "settings": settings}


@app.post("/api/spudex/run")
async def run_spudex_command(payload: SpudexRunRequest) -> Dict[str, Any]:
    from spudex.runner import start_spudex_command

    return await start_spudex_command(
        command=payload.command,
        argv=payload.argv,
        cwd="",
        label=payload.label or "Spudex command",
        source="ui",
        platform="webui",
        redis_client=redis_client,
        background=payload.background,
    )


@app.post("/api/spudex/chat/session")
def create_spudex_chat_session(payload: SpudexChatSessionRequest) -> Dict[str, Any]:
    from spudex.policy import display_agent_path, resolve_spudex_cwd
    from spudex.runner import append_session_log, create_spudex_session, update_spudex_session
    from spudex.settings import get_spudex_settings

    settings = get_spudex_settings(redis_client)
    cwd_value = settings.get("default_cwd") or "workspace"
    cwd = resolve_spudex_cwd(cwd_value)
    label = str(payload.label or "").strip() or "New Spudex chat"
    session = create_spudex_session(
        label=label,
        cwd=str(cwd),
        goal="",
        source="spudex_chat",
        platform="webui",
    )
    session = update_spudex_session(str(session.get("id") or ""), status="draft", cwd=str(cwd), cwd_display=display_agent_path(cwd))
    append_session_log(
        str(session.get("id") or ""),
        stream="system",
        text="New Spudex chat created. Send a message to start the loop.",
        level="info",
    )
    return {"ok": True, "session": session}


@app.post("/api/spudex/chat")
async def run_spudex_chat(payload: SpudexChatRequest) -> Dict[str, Any]:
    from spudex.chat_loop import run_spudex_chat_turn
    from spudex.policy import display_agent_path, resolve_spudex_cwd
    from spudex.runner import append_session_log, create_spudex_session, finish_spudex_plan, get_spudex_session, register_spudex_task, update_spudex_session
    from spudex.settings import spudex_llm_overrides, get_spudex_settings

    message = str(payload.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Spudex chat message is required.")
    settings = get_spudex_settings(redis_client)
    cwd_value = settings.get("default_cwd") or "workspace"
    cwd = resolve_spudex_cwd(cwd_value)
    wanted_session_id = str(payload.session_id or "").strip()
    session = get_spudex_session(wanted_session_id) if wanted_session_id else {}
    if session and str(session.get("source") or "").strip().lower() != "spudex_chat":
        session = {}
    if session and str(session.get("status") or "").strip().lower() == "running":
        raise HTTPException(status_code=409, detail="Spudex chat session is already running. Wait for it to finish or stop it first.")
    if not session:
        session = create_spudex_session(
            label=f"Spudex chat: {message[:80]}",
            cwd=str(cwd),
            goal=message,
            source="spudex_chat",
            platform="webui",
        )
    else:
        session = update_spudex_session(
            str(session.get("id") or ""),
            label=f"Spudex chat: {message[:80]}",
            cwd=str(cwd),
            cwd_display=display_agent_path(cwd),
            goal=message,
        )

    async def _run() -> None:
        try:
            overrides = spudex_llm_overrides(redis_client)
            llm_kwargs: Dict[str, Any] = {
                "host": overrides.get("host"),
                "model": overrides.get("model"),
                "redis_conn": redis_client,
            }
            if overrides.get("provider") and overrides.get("model"):
                llm_kwargs["provider"] = overrides.get("provider")
            async with get_llm_client_from_env(**llm_kwargs) as llm_client:
                result = await run_spudex_chat_turn(
                    session_id=str(session.get("id") or ""),
                    message=message,
                    platform="webui",
                    llm_client=llm_client,
                    redis_client=redis_client,
                )
            if not bool(result.get("ok")):
                err = result.get("error") if isinstance(result.get("error"), dict) else {}
                append_session_log(
                    str(session.get("id") or ""),
                    stream="system",
                    text=str(err.get("message") or "Spudex chat loop failed."),
                    level="error",
                )
        except asyncio.CancelledError:
            append_session_log(
                str(session.get("id") or ""),
                stream="system",
                text="Spudex chat task cancelled from UI.",
                level="warning",
            )
            finish_spudex_plan(str(session.get("id") or ""), success=False)
            update_spudex_session(str(session.get("id") or ""), status="stopped", finished_ts=time.time())
        except Exception as exc:
            logger.exception("[spudex] chat loop failed")
            append_session_log(
                str(session.get("id") or ""),
                stream="system",
                text=f"Spudex chat loop failed: {exc}",
                level="error",
            )
            finish_spudex_plan(str(session.get("id") or ""), success=False)
            update_spudex_session(str(session.get("id") or ""), status="failed", finished_ts=time.time())
        finally:
            latest = get_spudex_session(str(session.get("id") or ""))
            if str(latest.get("status") or "").strip().lower() in {"queued", "running"}:
                append_session_log(
                    str(session.get("id") or ""),
                    stream="system",
                    text="Spudex chat loop ended without a final status; marking the session failed.",
                    level="error",
                )
                finish_spudex_plan(str(session.get("id") or ""), success=False)
                update_spudex_session(str(session.get("id") or ""), status="failed", finished_ts=time.time())

    task = asyncio.create_task(_run())
    register_spudex_task(str(session.get("id") or ""), task)
    return {"ok": True, "session": session}


@app.get("/api/spudex/sessions/{session_id}/logs")
def get_spudex_session_logs(session_id: str, after_seq: int = 0, limit: int = 200) -> Dict[str, Any]:
    from spudex.runner import read_spudex_logs

    return read_spudex_logs(session_id, after_seq=after_seq, limit=limit)


@app.post("/api/spudex/sessions/{session_id}/file-changes/approve")
def approve_spudex_file_change_api(session_id: str, payload: SpudexFileChangeRequest) -> Dict[str, Any]:
    from spudex.runner import approve_spudex_file_change

    result = approve_spudex_file_change(session_id, payload.change_id)
    if not bool(result.get("ok")):
        err = result.get("error") if isinstance(result.get("error"), dict) else {}
        raise HTTPException(status_code=400, detail=str(err.get("message") or "Failed to approve file change."))
    return result


@app.post("/api/spudex/sessions/{session_id}/file-changes/reject")
def reject_spudex_file_change_api(session_id: str, payload: SpudexFileChangeRequest) -> Dict[str, Any]:
    from spudex.runner import reject_spudex_file_change

    result = reject_spudex_file_change(session_id, payload.change_id)
    if not bool(result.get("ok")):
        err = result.get("error") if isinstance(result.get("error"), dict) else {}
        raise HTTPException(status_code=400, detail=str(err.get("message") or "Failed to reject file change."))
    return result


@app.post("/api/spudex/sessions/{session_id}/stop")
async def stop_spudex_command(session_id: str) -> Dict[str, Any]:
    from spudex.runner import stop_spudex_session

    return await stop_spudex_session(session_id)


@app.delete("/api/spudex/sessions/{session_id}")
async def close_spudex_session_api(session_id: str) -> Dict[str, Any]:
    from spudex.runner import close_spudex_session

    result = await close_spudex_session(session_id)
    if not bool(result.get("ok")):
        err = result.get("error") if isinstance(result.get("error"), dict) else {}
        raise HTTPException(status_code=404, detail=str(err.get("message") or "Spudex session was not found."))
    return result


@app.get("/api/runtime/breakdown")
def runtime_breakdown() -> Dict[str, Any]:
    payload = _runtime_breakdown_payload()
    return {"ok": True, **payload}


@app.get("/api/runtime/llm/debug")
def runtime_llm_debug(since_id: int = 0, limit: int = 200) -> Dict[str, Any]:
    return get_llm_debug_runtime_snapshot(since_id=since_id, limit=limit)


@app.post("/api/runtime/local-llm/unload")
def unload_runtime_local_llm(payload: LocalLlmUnloadRequest) -> Dict[str, Any]:
    try:
        return unload_local_llm_models(
            provider=payload.provider,
            model=payload.model,
            cache_key=payload.cache_key,
            all_models=bool(payload.unload_all),
        )
    except Exception as exc:
        logger.exception("[local-llm] unload failed")
        raise HTTPException(status_code=500, detail=str(exc) or "Failed to unload local model.") from exc


async def _spud_link_follow_up_decision(assistant_content: str) -> Dict[str, Any]:
    text = str(assistant_content or "").strip()
    payload: Dict[str, Any] = {
        "enabled": False,
        "reopen_mic": False,
        "source": "disabled",
    }
    if not text:
        return payload
    try:
        from tater_voice import voice_pipeline

        if not bool(voice_pipeline._continued_chat_enabled()):
            return payload
        reopen_mic = bool(await voice_pipeline._response_is_followup_question(text))
        return {
            "enabled": True,
            "reopen_mic": reopen_mic,
            "source": "ai_classifier",
        }
    except Exception as exc:
        logger.warning("[spudlink] follow-up classifier failed: %s", exc)
        return {
            "enabled": True,
            "reopen_mic": False,
            "source": "ai_classifier_error",
            "error": str(exc)[:240],
        }


def _spud_link_tool_notice_payload(
    func_name: str,
    plugin_obj: Any,
    wait_text: str = "",
    wait_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    progress_payload = dict(wait_payload) if isinstance(wait_payload, dict) else {}
    text = str(wait_text or progress_payload.get("text") or "").strip()
    if not text:
        text = "I'm working on that now."
    tool_name = str(func_name or progress_payload.get("tool") or "").strip()
    display_name = ""
    if plugin_obj is None:
        display_name = f"kernel::{tool_name}" if tool_name else ""
    else:
        display_name = (
            getattr(plugin_obj, "verba_name", None)
            or getattr(plugin_obj, "pretty_name", None)
            or getattr(plugin_obj, "name", None)
            or tool_name
        )
    progress_payload["text"] = text
    if tool_name:
        progress_payload.setdefault("tool", tool_name)
    return {
        "type": "tool",
        "status": "running",
        "phase": str(progress_payload.get("phase") or "tool_start"),
        "tool": tool_name,
        "display_name": str(display_name or tool_name or "").strip(),
        "text": text,
        "wait_text": text,
        "wait_payload": progress_payload,
        "created_at": time.time(),
    }


def _normalize_spud_link_tater_chat_messages(payload: SpudLinkTaterChatRequest) -> List[Dict[str, str]]:
    messages: List[Dict[str, str]] = []
    history = payload.history if isinstance(payload.history, list) else []
    for raw_message in history[-80:]:
        if not isinstance(raw_message, dict):
            continue
        role = str(raw_message.get("role") or "").strip().lower()
        if role not in {"system", "user", "assistant", "tool"}:
            continue
        content = _openai_content_to_text(
            raw_message.get("content")
            if "content" in raw_message
            else raw_message.get("text")
        ).strip()
        if content:
            messages.append({"role": role, "content": content})

    message_text = _openai_content_to_text(payload.message).strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="message is required.")
    messages.append({"role": "user", "content": message_text})
    return messages


async def _stream_spud_link_tater_completion(
    *,
    payload: OpenAIChatCompletionRequest,
    messages: List[Dict[str, str]],
    tools_enabled: bool,
    request: Request,
    platform: str,
    origin_override: Optional[Dict[str, Any]],
    scope_override: Optional[str],
    platform_preamble: str,
    context_extra: Optional[Dict[str, Any]],
    little_spud_identity: Dict[str, str],
):
    event_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
    run_id = f"lsrun_{uuid.uuid4().hex[:16]}"
    tool_notices: List[Dict[str, Any]] = []

    async def _on_tool(
        func_name: str,
        plugin_obj: Any,
        wait_text: str = "",
        wait_payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        notice = _spud_link_tool_notice_payload(
            func_name,
            plugin_obj,
            wait_text=wait_text,
            wait_payload=wait_payload,
        )
        notice["run_id"] = run_id
        tool_notices.append(dict(notice))
        _save_little_spud_active_run(
            little_spud_identity,
            {
                "run_id": run_id,
                "status": "running",
                "phase": str(notice.get("phase") or "tool_start"),
                "tool": str(notice.get("tool") or ""),
                "display_name": str(notice.get("display_name") or ""),
                "text": str(notice.get("text") or "Tater is thinking"),
                "wait_payload": notice.get("wait_payload") if isinstance(notice.get("wait_payload"), dict) else {},
            },
        )
        _save_little_spud_history(
            little_spud_identity,
            role="assistant",
            content=str(notice.get("text") or ""),
            meta={
                "kind": "tool_notice",
                "tool": str(notice.get("display_name") or notice.get("tool") or "").strip(),
                "run_id": run_id,
                "phase": str(notice.get("phase") or "tool_start"),
            },
        )
        await event_queue.put({"type": "tool", "payload": notice})

    async def _runner() -> None:
        try:
            started_payload = {
                "type": "run_start",
                "run_id": run_id,
                "status": "running",
                "text": "Tater is thinking",
                "created_at": time.time(),
            }
            _save_little_spud_active_run(
                little_spud_identity,
                {
                    "run_id": run_id,
                    "status": "running",
                    "phase": "thinking",
                    "text": "Tater is thinking",
                },
            )
            await event_queue.put({"type": "run", "payload": started_payload})
            completion = await _run_spud_link_native_hydra_completion(
                payload,
                messages,
                tools_enabled=tools_enabled,
                request=request,
                platform=platform,
                origin_override=origin_override,
                scope_override=scope_override,
                platform_preamble=platform_preamble,
                context_extra=context_extra,
                wait_callback=_on_tool,
            )
            assistant_content = str(completion.get("content") or "").strip() if isinstance(completion, dict) else ""
            spud_link_artifacts = (
                completion.get("artifacts")
                if isinstance(completion, dict) and isinstance(completion.get("artifacts"), list)
                else []
            )
            if assistant_content:
                _save_little_spud_history(
                    little_spud_identity,
                    role="assistant",
                    content=assistant_content,
                    attachments=[dict(item) for item in spud_link_artifacts if isinstance(item, dict)],
                )
                completion["follow_up"] = await _spud_link_follow_up_decision(assistant_content)
            _clear_little_spud_active_run(little_spud_identity, run_id=run_id)
            await event_queue.put({"type": "completion", "completion": completion})
        except Exception as exc:
            logger.exception("[spudlink] native streamed Hydra completion failed")
            _clear_little_spud_active_run(little_spud_identity, run_id=run_id)
            _save_little_spud_history(little_spud_identity, role="system", content=f"Request failed: {str(exc) or 'Hydra request failed.'}")
            await event_queue.put(
                {
                    "type": "error",
                    "payload": {
                        "type": "error",
                        "run_id": run_id,
                        "message": str(exc) or "Hydra request failed.",
                    },
                }
            )

    def _observe_runner_result(done_task: asyncio.Task) -> None:
        try:
            done_task.exception()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    task = asyncio.create_task(_runner())
    try:
        while True:
            try:
                event = await asyncio.wait_for(event_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if task.done() and event_queue.empty():
                    break
                yield _sse("tater.ping", {"type": "ping", "run_id": run_id, "ts": time.time()})
                continue

            event_type = str(event.get("type") or "").strip()
            if event_type == "run":
                event_payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                yield _sse("tater.run", event_payload)
                continue
            if event_type == "tool":
                event_payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                yield _sse("tater.tool", event_payload)
                continue
            if event_type == "error":
                event_payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                yield _sse("tater.error", event_payload)
                yield _sse("tater.done", {"type": "done", "run_id": run_id, "ok": False})
                yield "data: [DONE]\n\n"
                break
            if event_type == "completion":
                completion = event.get("completion") if isinstance(event.get("completion"), dict) else {}
                content = str(completion.get("content") or "")
                artifacts = completion.get("artifacts") if isinstance(completion.get("artifacts"), list) else []
                follow_up = completion.get("follow_up") if isinstance(completion.get("follow_up"), dict) else {}
                if not content.strip() and not artifacts:
                    yield _sse(
                        "tater.error",
                        {
                            "type": "error",
                            "run_id": run_id,
                            "message": "Tater returned no message content.",
                        },
                    )
                    yield _sse("tater.done", {"type": "done", "run_id": run_id, "ok": False})
                    yield "data: [DONE]\n\n"
                    break
                yield _sse(
                    "tater.message",
                    {
                        "type": "message",
                        "run_id": run_id,
                        "role": "assistant",
                        "content": content,
                        "tool_notices": tool_notices[-12:],
                    },
                )
                if artifacts:
                    yield _sse(
                        "tater.artifacts",
                        {
                            "type": "artifacts",
                            "run_id": run_id,
                            "artifacts": artifacts,
                        },
                    )
                if follow_up:
                    yield _sse(
                        "tater.follow_up",
                        {
                            "type": "follow_up",
                            "run_id": run_id,
                            "follow_up": follow_up,
                        },
                    )
                yield _sse("tater.done", {"type": "done", "run_id": run_id, "ok": True})
                yield "data: [DONE]\n\n"
                break
    finally:
        if not task.done():
            task.add_done_callback(_observe_runner_result)


@app.get("/api/spudlink/status")
def spud_link_status() -> Dict[str, Any]:
    return {"ok": True, "spud_link": _spud_link_public_settings_payload()}


@app.post("/api/spudlink/pairing-code")
def create_spud_link_pairing_code(request: Request) -> Dict[str, Any]:
    settings = _require_spud_link_server_enabled()
    if not bool(settings.get("pairing_enabled")):
        raise HTTPException(status_code=400, detail="Enable Spud Link pairing before creating a pairing code.")
    code = f"spud-{secrets.token_urlsafe(18)}"
    expires_at = time.time() + SPUD_LINK_PAIRING_TTL_SECONDS
    redis_client.hset(
        SPUD_LINK_SETTINGS_KEY,
        mapping={
            "pairing_code_hash": _spud_link_token_digest(code),
            "pairing_expires_at": str(float(expires_at)),
        },
    )
    pairing_payload = _spud_link_pairing_payload(settings=settings, code=code, expires_at=expires_at, request=request)
    pairing_uri = _spud_link_pairing_uri(pairing_payload)
    return {
        "ok": True,
        "pairing_code": code,
        "manual_code": code,
        "pairing_payload": pairing_payload,
        "pairing_payload_text": json.dumps(pairing_payload, separators=(",", ":"), ensure_ascii=False),
        "pairing_uri": pairing_uri,
        "pairing_qr_svg": _spud_link_qr_svg_data_url(pairing_uri),
        "expires_at": expires_at,
        "expires_in_seconds": SPUD_LINK_PAIRING_TTL_SECONDS,
    }


@app.post("/api/spudlink/pair")
def pair_spud_link_node(payload: SpudLinkPairRequest, request: Request) -> Dict[str, Any]:
    settings = _require_spud_link_server_enabled()
    if not bool(settings.get("pairing_enabled")):
        raise HTTPException(status_code=403, detail="Spud Link pairing is disabled.")
    expected_hash = str(settings.get("pairing_code_hash") or "").strip()
    expires_at = float(settings.get("pairing_expires_at") or 0)
    if not expected_hash or expires_at <= time.time():
        raise HTTPException(status_code=400, detail="Spud Link pairing code is expired or missing.")
    supplied_hash = _spud_link_token_digest(payload.pairing_code)
    if not hmac.compare_digest(expected_hash, supplied_hash):
        raise HTTPException(status_code=401, detail="Invalid Spud Link pairing code.")

    role = _normalize_spud_link_mode(payload.role, default=SPUD_LINK_MODE_SPUDLET)
    if role == SPUD_LINK_MODE_HUB:
        raise HTTPException(status_code=400, detail="A linked node cannot pair as another Spud Link server.")
    if role == SPUD_LINK_MODE_DISABLED:
        role = SPUD_LINK_MODE_SPUDLET
    if role == SPUD_LINK_MODE_SPUDLET and not bool(settings.get("allow_spudlets")):
        raise HTTPException(status_code=403, detail="Spudlets are not allowed by this Spud Link server.")
    if role == SPUD_LINK_MODE_LITTLE_SPUD and not bool(settings.get("allow_little_spuds")):
        raise HTTPException(status_code=403, detail="Little Spuds are not allowed by this Spud Link server.")

    node_token = f"spudlink-{secrets.token_urlsafe(32)}"
    now = time.time()
    node = {
        "id": f"spud_{uuid.uuid4().hex[:16]}",
        "name": str(payload.node_name or _spud_link_mode_label(role)).strip()[:120] or _spud_link_mode_label(role),
        "role": role,
        "role_label": _spud_link_mode_label(role),
        "public_url": str(payload.public_url or "").strip()[:500],
        "created_at": now,
        "last_seen_at": now,
        "token_hash": _spud_link_token_digest(node_token),
        "metadata": payload.metadata if isinstance(payload.metadata, dict) else {},
        "stats": {},
        "activity": {},
    }
    _spud_link_touch_node_from_request(node, request)
    stored = _spud_link_store_node(node)
    redis_client.hdel(SPUD_LINK_SETTINGS_KEY, "pairing_code_hash", "pairing_expires_at")
    server_mode = _normalize_spud_link_tater_mode(settings.get("mode"))
    is_server = server_mode in SPUD_LINK_SERVER_MODE_CHOICES
    hydra_tools_enabled = is_server and bool(settings.get("little_spud_tools_enabled"))
    server_payload = {
        "name": settings.get("node_name") or _spud_link_local_node_name(),
        "mode": server_mode,
        "tools_enabled": hydra_tools_enabled,
        "capabilities": {
            "llm": is_server,
            "hydra": is_server,
            "tools": hydra_tools_enabled,
        },
    }
    return {
        "ok": True,
        "node": stored,
        "node_token": node_token,
        "hub": server_payload,
        "server": server_payload,
    }


@app.post("/api/spudlink/connect")
def connect_to_spud_hub(payload: SpudLinkConnectRequest) -> Dict[str, Any]:
    hub_url = str(payload.hub_url or "").strip().rstrip("/")
    if not hub_url:
        raise HTTPException(status_code=400, detail="Spud Link server URL is required.")
    if "://" not in hub_url:
        hub_url = f"http://{hub_url}"
    parsed = urlparse(hub_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(status_code=400, detail="Spud Link server URL must be an http(s) URL.")
    role = _normalize_spud_link_tater_mode(payload.role, default=SPUD_LINK_MODE_SPUDLET)
    if role in {SPUD_LINK_MODE_DISABLED, SPUD_LINK_MODE_HUB}:
        role = SPUD_LINK_MODE_SPUDLET
    node_name = str(payload.node_name or _spud_link_local_node_name()).strip()[:120] or "Tater"
    public_url = str(payload.public_url or "").strip()[:500]
    body = {
        "pairing_code": str(payload.pairing_code or "").strip(),
        "role": role,
        "node_name": node_name,
        "public_url": public_url,
        "metadata": {
            "client": "tater-spudlet",
            "connected_at": time.time(),
        },
    }
    data = json.dumps(body, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        f"{hub_url}/api/spudlink/pair",
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = ""
        with contextlib.suppress(Exception):
            payload_text = exc.read().decode("utf-8", errors="ignore")
            parsed_error = json.loads(payload_text)
            if isinstance(parsed_error, dict):
                detail = str(parsed_error.get("detail") or "")
        raise HTTPException(status_code=exc.code or 502, detail=detail or "Spud Link server rejected pairing.") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not reach Spud Link server: {exc}") from exc
    try:
        result = json.loads(raw.decode("utf-8", errors="ignore"))
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Spud Link server returned an invalid pairing response.") from exc
    if not isinstance(result, dict) or not result.get("node_token"):
        raise HTTPException(status_code=502, detail="Spud Link server did not return a node token.")
    node_token = str(result.get("node_token") or "").strip()
    server_payload = result.get("server") if isinstance(result.get("server"), dict) else result.get("hub") if isinstance(result.get("hub"), dict) else {}
    redis_client.hset(
        SPUD_LINK_SETTINGS_KEY,
        mapping={
            "mode": role,
            "node_name": node_name,
            "public_url": public_url,
            "hub_url": hub_url,
            "hub_name": str(server_payload.get("name") or "").strip()[:120],
            "hub_mode": _normalize_spud_link_tater_mode(server_payload.get("mode"), default=SPUD_LINK_MODE_HUB),
            "hub_connected_at": str(time.time()),
            "node_token": node_token,
        },
    )
    return {
        "ok": True,
        "mode": role,
        "mode_label": _spud_link_mode_label(role),
        "hub_url": hub_url,
        "hub": server_payload,
        "server": server_payload,
        "node": result.get("node") if isinstance(result.get("node"), dict) else {},
        "node_token_set": True,
    }


@app.post("/api/spudlink/revoke-node")
def revoke_spud_link_node(payload: SpudLinkRevokeNodeRequest) -> Dict[str, Any]:
    node_id = str(payload.node_id or "").strip()
    if not node_id:
        raise HTTPException(status_code=400, detail="Spud Link node id is required.")
    removed = int(redis_client.hdel(SPUD_LINK_NODES_KEY, node_id) or 0)
    if removed <= 0:
        raise HTTPException(status_code=404, detail="Linked Spud was not found.")
    return {
        "ok": True,
        "node_id": node_id,
        "linked_nodes": _spud_link_load_nodes(),
    }


@app.post("/api/spudlink/heartbeat")
def spud_link_heartbeat(payload: SpudLinkHeartbeatRequest, request: Request) -> Dict[str, Any]:
    settings, node = _require_spud_link_node_request(request)
    if bool(settings.get("telemetry_enabled")):
        node["stats"] = payload.stats if isinstance(payload.stats, dict) else {}
        node["activity"] = _spud_link_sanitize_activity(
            payload.activity,
            allow_previews=bool(settings.get("request_previews_enabled")),
        )
    node["name"] = str(payload.node_name or node.get("name") or _spud_link_mode_label(node.get("role"))).strip()[:120]
    node["public_url"] = str(payload.public_url or node.get("public_url") or "").strip()[:500]
    node["remote_mode"] = _normalize_spud_link_mode(payload.mode, default=node.get("role"))
    node["version"] = str(payload.version or node.get("version") or "").strip()[:80]
    _spud_link_touch_node_from_request(node, request)
    stored = _spud_link_store_node(node)
    server_mode = _normalize_spud_link_tater_mode(settings.get("mode"))
    is_server = server_mode in SPUD_LINK_SERVER_MODE_CHOICES
    hydra_tools_enabled = is_server and bool(settings.get("little_spud_tools_enabled"))
    server_payload = {
        "name": settings.get("node_name") or _spud_link_local_node_name(),
        "mode": server_mode,
        "telemetry_enabled": bool(settings.get("telemetry_enabled")),
        "request_previews_enabled": bool(settings.get("request_previews_enabled")),
        "little_spud_tools_enabled": hydra_tools_enabled,
        "capabilities": {
            "llm": is_server,
            "hydra": is_server,
            "tools": hydra_tools_enabled,
        },
    }
    return {
        "ok": True,
        "node": stored,
        "hub": server_payload,
        "server": server_payload,
    }


@app.get("/api/spudlink/v1/history")
def spud_link_history(request: Request, limit: int = 80) -> Dict[str, Any]:
    _settings, node = _require_spud_link_node_request(request)
    identity_payload = type(
        "SpudLinkHistoryIdentityPayload",
        (),
        {
            "metadata": {},
            "user_name": request.headers.get("x-spudlink-user"),
            "device_name": request.headers.get("x-spudlink-device"),
            "user": request.headers.get("x-spudlink-user"),
        },
    )()
    identity = _spud_link_identity_from_request(identity_payload, request, node)
    rows = _load_little_spud_history(identity, limit=limit)
    active_runs = _load_little_spud_active_runs(identity)
    active_run = active_runs[0] if active_runs else None
    _spud_link_touch_node_from_request(node, request)
    _spud_link_store_node(node)
    return {
        "ok": True,
        "messages": rows,
        "active_run": active_run,
        "active_runs": active_runs,
        "identity": {
            "user_name": identity.get("user_name"),
            "device_name": identity.get("device_name"),
            "scope": identity.get("scope"),
        },
    }


@app.get("/api/spudlink/v1/files/{file_id}")
def spud_link_file(file_id: str, request: Request, mimetype: str = "application/octet-stream") -> Response:
    _settings, node = _require_spud_link_node_request(request)
    _spud_link_touch_node_from_request(node, request)
    _spud_link_store_node(node)
    blob = _load_file_blob_from_redis(file_id)
    if blob is None:
        raise HTTPException(status_code=404, detail="Attachment not found or expired.")
    media_type = str(mimetype or "application/octet-stream").strip() or "application/octet-stream"
    return Response(content=blob, media_type=media_type)


@app.post("/api/spudlink/v1/tater/chat")
async def spud_link_tater_chat(
    payload: SpudLinkTaterChatRequest,
    request: Request,
) -> Any:
    settings, node = _require_spud_link_node_request(request)
    role = _normalize_spud_link_mode(node.get("role"), default=SPUD_LINK_MODE_SPUDLET)
    server_mode = _normalize_spud_link_tater_mode(settings.get("mode"))
    if server_mode not in SPUD_LINK_SERVER_MODE_CHOICES or role != SPUD_LINK_MODE_LITTLE_SPUD:
        raise HTTPException(status_code=403, detail="Native Spud Link chat requires a paired Little Spud client.")

    messages = _normalize_spud_link_tater_chat_messages(payload)
    user_text, _history_messages = _openai_user_text_and_history(messages)
    little_spud_identity = _spud_link_identity_from_request(payload, request, node)
    tools_enabled = bool(settings.get("little_spud_tools_enabled"))

    _spud_link_touch_node_from_request(node, request)
    node["activity"] = _spud_link_sanitize_activity(
        {
            "last_call_at": time.time(),
            "last_call_mode": "tater_native",
            "last_model": "tater/hydra",
            "last_user": little_spud_identity.get("user_name"),
            "last_device": little_spud_identity.get("device_name"),
            "role": role,
        },
        allow_previews=bool(settings.get("request_previews_enabled")),
    )
    _spud_link_store_node(node)

    if user_text:
        _save_little_spud_history(little_spud_identity, role="user", content=user_text)

    origin_override = {
        "platform": "little_spud",
        "source": "spud_link",
        "protocol": "tater_native",
        "user": little_spud_identity.get("user_name"),
        "username": little_spud_identity.get("user_name"),
        "display_name": little_spud_identity.get("display_name"),
        "user_id": little_spud_identity.get("alias_id"),
        "device_name": little_spud_identity.get("device_name"),
        "device_id": little_spud_identity.get("device_name"),
        "node_id": node.get("id"),
        "node_name": node.get("name"),
        "spud_link_role": role,
        "server_mode": server_mode,
        "session_id": little_spud_identity.get("scope"),
    }
    context_extra = {
        "little_spud_user": little_spud_identity.get("user_name"),
        "little_spud_device": little_spud_identity.get("device_name"),
        "native_spud_link": True,
        "attachments": [dict(item) for item in payload.attachments if isinstance(item, dict)],
    }
    openai_payload = OpenAIChatCompletionRequest(
        model="tater/hydra",
        messages=messages,
        stream=True,
        user=payload.user or payload.user_name or little_spud_identity.get("user_name"),
        user_name=payload.user_name or little_spud_identity.get("user_name"),
        device_name=payload.device_name or little_spud_identity.get("device_name"),
        metadata={
            **(payload.metadata if isinstance(payload.metadata, dict) else {}),
            "protocol": "tater_native",
        },
    )
    return StreamingResponse(
        _stream_spud_link_tater_completion(
            payload=openai_payload,
            messages=messages,
            tools_enabled=tools_enabled,
            request=request,
            platform="little_spud",
            origin_override=origin_override,
            scope_override=little_spud_identity.get("scope"),
            platform_preamble="Little Spud native Tater client request.",
            context_extra=context_extra,
            little_spud_identity=little_spud_identity,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/spudlink/v1/tater/llm")
async def spud_link_tater_llm(
    payload: OpenAIChatCompletionRequest,
    request: Request,
) -> Dict[str, Any]:
    settings, node = _require_spud_link_node_request(request)
    role = _normalize_spud_link_mode(node.get("role"), default=SPUD_LINK_MODE_SPUDLET)
    server_mode = _normalize_spud_link_tater_mode(settings.get("mode"))
    if server_mode not in SPUD_LINK_SERVER_MODE_CHOICES or role != SPUD_LINK_MODE_SPUDLET:
        raise HTTPException(status_code=403, detail="Native Spud Link model calls require a paired Spudlet client.")

    messages = _normalize_openai_chat_messages(payload.messages)
    if not messages:
        raise HTTPException(status_code=400, detail="messages must include at least one text message.")

    _spud_link_touch_node_from_request(node, request)
    node["activity"] = _spud_link_sanitize_activity(
        {
            "last_call_at": time.time(),
            "last_call_mode": "spudlet_native_llm",
            "last_model": str(payload.model or "tater/base"),
            "role": role,
        },
        allow_previews=bool(settings.get("request_previews_enabled")),
    )
    _spud_link_store_node(node)

    return await _run_spud_link_native_llm_completion(payload, messages)


@app.post("/api/spudlink/v1/chat/completions")
async def spud_link_chat_completions(
    payload: OpenAIChatCompletionRequest,
    request: Request,
) -> Any:
    _ = payload
    _settings, _node = _require_spud_link_node_request(request)
    raise HTTPException(
        status_code=410,
        detail="Spud Link uses native Tater endpoints. Use /api/spudlink/v1/tater/llm for Spudlets or /api/spudlink/v1/tater/chat for Little Spuds.",
    )


@app.post("/api/spudlink/v1/tts/speech")
async def spud_link_tts_speech(payload: SpudLinkTtsRequest, request: Request) -> Response:
    settings, node = _require_spud_link_node_request(request)
    _spud_link_touch_node_from_request(node, request)
    text = str(payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="TTS text is required.")
    if len(text) > 4000:
        text = text[:4000].rstrip()

    current_speech = get_shared_speech_settings()
    try:
        wav_bytes = await synthesize_preview_wav(
            text=text,
            backend=str(current_speech.get("tts_backend") or "").strip(),
            model=str(current_speech.get("tts_model") or "").strip(),
            voice=str(current_speech.get("tts_voice") or "").strip(),
            kokoro_output_gain=current_speech.get("kokoro_output_gain"),
            pocket_tts_output_gain=current_speech.get("pocket_tts_output_gain"),
            acceleration=str(current_speech.get("acceleration") or "").strip(),
            wyoming_host=str(current_speech.get("wyoming_tts_host") or "").strip(),
            wyoming_port=str(current_speech.get("wyoming_tts_port") or "").strip(),
            wyoming_voice=str(current_speech.get("wyoming_tts_voice") or "").strip(),
            openai_base_url=str(current_speech.get("openai_tts_base_url") or "").strip(),
            openai_api_key=str(current_speech.get("openai_tts_api_key") or "").strip(),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Spud Link TTS failed.") from exc

    if not wav_bytes:
        raise HTTPException(status_code=400, detail="Spud Link TTS produced no audio.")

    role = _normalize_spud_link_mode(node.get("role"), default=SPUD_LINK_MODE_SPUDLET)
    node["activity"] = _spud_link_sanitize_activity(
        {
            "last_call_at": time.time(),
            "last_call_mode": "tts",
            "last_model": str(current_speech.get("tts_backend") or "tts"),
            "last_user": str(request.headers.get("x-spudlink-user") or "").strip(),
            "last_device": str(request.headers.get("x-spudlink-device") or "").strip(),
            "role": role,
        },
        allow_previews=bool(settings.get("request_previews_enabled")),
    )
    _spud_link_store_node(node)
    return Response(
        content=wav_bytes,
        media_type="audio/wav",
        headers={"Cache-Control": "no-store"},
    )


def _decode_spud_link_wav_audio(wav_bytes: bytes) -> Tuple[bytes, Dict[str, int]]:
    raw = bytes(wav_bytes or b"")
    if len(raw) < 44 or raw[:4] != b"RIFF" or raw[8:12] != b"WAVE":
        raise HTTPException(status_code=400, detail="Little Spud STT expects WAV audio.")
    try:
        with wave.open(io.BytesIO(raw), "rb") as wav_file:
            frames = wav_file.readframes(wav_file.getnframes())
            return frames, {
                "rate": int(wav_file.getframerate() or 16000),
                "width": int(wav_file.getsampwidth() or 2),
                "channels": int(wav_file.getnchannels() or 1),
            }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read Little Spud STT WAV audio: {exc}") from exc


async def _spud_link_transcribe_wyoming_audio(
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    *,
    language: str = "",
) -> str:
    from tater_voice import voice_pipeline

    if (
        voice_pipeline.AsyncTcpClient is None
        or voice_pipeline.Transcribe is None
        or voice_pipeline.Transcript is None
        or voice_pipeline.WyomingAudioStart is None
        or voice_pipeline.WyomingAudioChunk is None
        or voice_pipeline.WyomingAudioStop is None
        or voice_pipeline.WyomingError is None
    ):
        raise RuntimeError(f"Wyoming STT dependency unavailable: {voice_pipeline.WYOMING_IMPORT_ERROR or 'unknown import error'}")

    current_speech = get_shared_speech_settings()
    host = str(current_speech.get("wyoming_stt_host") or voice_pipeline.DEFAULT_WYOMING_STT_HOST).strip()
    port = int(current_speech.get("wyoming_stt_port") or voice_pipeline.DEFAULT_WYOMING_STT_PORT)
    timeout = voice_pipeline._wyoming_timeout_s()
    rate = int(audio_format.get("rate") or 16000)
    width = int(audio_format.get("width") or 2)
    channels = int(audio_format.get("channels") or 1)
    chunk_size = max(width * channels * max(1, rate // 10), 3200)

    async with voice_pipeline.AsyncTcpClient(host, port) as client:
        await asyncio.wait_for(
            client.write_event(voice_pipeline.Transcribe(language=str(language or "").strip() or None).event()),
            timeout=timeout,
        )
        await asyncio.wait_for(
            client.write_event(voice_pipeline.WyomingAudioStart(rate=rate, width=width, channels=channels).event()),
            timeout=timeout,
        )
        for offset in range(0, len(audio_bytes), chunk_size):
            chunk = audio_bytes[offset : offset + chunk_size]
            if not chunk:
                continue
            await asyncio.wait_for(
                client.write_event(
                    voice_pipeline.WyomingAudioChunk(rate=rate, width=width, channels=channels, audio=chunk).event()
                ),
                timeout=timeout,
            )
        await asyncio.wait_for(client.write_event(voice_pipeline.WyomingAudioStop().event()), timeout=timeout)

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            left = max(0.1, deadline - time.monotonic())
            event = await asyncio.wait_for(client.read_event(), timeout=left)
            if event is None:
                break
            if voice_pipeline.Transcript.is_type(event.type):
                return str(voice_pipeline.Transcript.from_event(event).text or "").strip()
            if voice_pipeline.WyomingError.is_type(event.type):
                err = voice_pipeline.WyomingError.from_event(event)
                raise RuntimeError(f"Wyoming STT error: {err.text} ({err.code or 'unknown'})")
    return ""


async def _spud_link_transcribe_pcm_audio(
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    *,
    language: str = "",
) -> Tuple[str, str, str]:
    from tater_voice import voice_pipeline

    current_speech = get_shared_speech_settings()
    requested_backend = voice_pipeline._normalize_stt_backend(str(current_speech.get("stt_backend") or "").strip())
    backend, backend_note = voice_pipeline._resolve_stt_backend_selected(requested_backend)
    if backend == "wyoming":
        transcript = await _spud_link_transcribe_wyoming_audio(audio_bytes, audio_format, language=language)
    else:
        transcript = await voice_pipeline._native_transcribe_local_audio_bytes(
            backend=backend,
            audio_bytes=audio_bytes,
            audio_format=audio_format,
            language=language or None,
            selector="little_spud",
            session_id=f"little-spud-stt-{uuid.uuid4().hex}",
            partial=False,
        )
    return str(transcript or "").strip(), backend, backend_note


@app.post("/api/spudlink/v1/stt/transcribe")
async def spud_link_stt_transcribe(payload: SpudLinkSttRequest, request: Request) -> Dict[str, Any]:
    settings, node = _require_spud_link_node_request(request)
    _spud_link_touch_node_from_request(node, request)
    encoded = str(payload.audio_base64 or "").strip()
    if not encoded:
        raise HTTPException(status_code=400, detail="STT audio is required.")
    if encoded.startswith("data:") and "," in encoded:
        encoded = encoded.split(",", 1)[1]
    try:
        wav_bytes = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="STT audio was not valid base64.") from exc
    if not wav_bytes:
        raise HTTPException(status_code=400, detail="STT audio is empty.")
    if len(wav_bytes) > 16 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="STT audio is too large.")

    audio_bytes, audio_format = _decode_spud_link_wav_audio(wav_bytes)
    if not audio_bytes:
        raise HTTPException(status_code=400, detail="STT audio did not contain any samples.")
    duration_s = len(audio_bytes) / max(
        1.0,
        float(int(audio_format.get("rate") or 16000) * int(audio_format.get("width") or 2) * int(audio_format.get("channels") or 1)),
    )
    if duration_s > 75.0:
        raise HTTPException(status_code=413, detail="STT audio is too long.")

    language = str(payload.language or "").strip()
    try:
        transcript, backend, backend_note = await _spud_link_transcribe_pcm_audio(
            audio_bytes,
            audio_format,
            language=language,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Spud Link STT failed.") from exc

    role = _normalize_spud_link_mode(node.get("role"), default=SPUD_LINK_MODE_SPUDLET)
    node["activity"] = _spud_link_sanitize_activity(
        {
            "last_call_at": time.time(),
            "last_call_mode": "stt",
            "last_model": backend,
            "last_user": str(request.headers.get("x-spudlink-user") or "").strip(),
            "last_device": str(request.headers.get("x-spudlink-device") or "").strip(),
            "role": role,
        },
        allow_previews=bool(settings.get("request_previews_enabled")),
    )
    _spud_link_store_node(node)
    return {
        "ok": True,
        "text": str(transcript or "").strip(),
        "backend": backend,
        "backend_note": backend_note,
        "duration_s": duration_s,
    }


@app.websocket("/api/spudlink/v1/stt/stream")
async def spud_link_stt_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    client_host = getattr(websocket.client, "host", "") if websocket.client is not None else ""
    try:
        settings = _require_spud_link_server_enabled()
        node = _find_spud_link_node_by_token(_extract_spud_link_token(websocket))  # type: ignore[arg-type]
    except HTTPException as exc:
        await websocket.send_json({"ok": False, "type": "error", "error": str(exc.detail or "Spud Link auth failed.")})
        await websocket.close(code=1008)
        return
    except Exception as exc:
        await websocket.send_json({"ok": False, "type": "error", "error": str(exc) or "Spud Link auth failed."})
        await websocket.close(code=1008)
        return

    def _ws_query_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
        raw = str(websocket.query_params.get(name) or "").strip()
        try:
            parsed = int(raw) if raw else int(default)
        except Exception:
            parsed = int(default)
        return max(int(minimum), min(int(maximum), parsed))

    audio_bits = _ws_query_int("bits", 16, minimum=8, maximum=32)
    audio_format = {
        "rate": _ws_query_int("rate", 16000, minimum=8000, maximum=48000),
        "width": max(1, audio_bits // 8),
        "channels": _ws_query_int("channels", 1, minimum=1, maximum=2),
    }
    language = str(websocket.query_params.get("language") or "").strip()
    user_name = str(websocket.query_params.get("user") or websocket.headers.get("x-spudlink-user") or "").strip()
    device_name = str(websocket.query_params.get("device") or websocket.headers.get("x-spudlink-device") or "").strip()
    selector = f"little_spud:{str(node.get('id') or client_host or 'remote').strip()}"

    from tater_voice import voice_pipeline

    cfg = voice_pipeline._voice_config_snapshot()
    eou_engine = voice_pipeline._build_eou_engine(audio_format, selector=selector, cfg=cfg)
    eou_cfg = cfg.get("eou") if isinstance(cfg.get("eou"), dict) else {}
    limits_cfg = cfg.get("limits") if isinstance(cfg.get("limits"), dict) else {}
    max_audio_bytes = int(limits_cfg.get("max_audio_bytes") or voice_pipeline.DEFAULT_MAX_AUDIO_BYTES)
    max_chunk_bytes = 256 * 1024
    audio_buffer = bytearray()
    started_ts = time.time()
    first_audio_ts = 0.0
    speech_started_sent = False
    frame_count = 0
    finalize_reason = "client_stop"
    metrics: Dict[str, Any] = {}

    role = _normalize_spud_link_mode(node.get("role"), default=SPUD_LINK_MODE_SPUDLET)
    logger.info(
        "[spudlink-stt] stream-start node=%s role=%s client=%s rate=%s width=%s ch=%s vad=%s user=%s device=%s",
        node.get("id") or "-",
        role,
        client_host or "-",
        audio_format.get("rate"),
        audio_format.get("width"),
        audio_format.get("channels"),
        getattr(eou_engine, "backend_name", ""),
        user_name or "-",
        device_name or "-",
    )
    await websocket.send_json(
        {
            "ok": True,
            "type": "listening",
            "vad_backend": getattr(eou_engine, "backend_name", ""),
            "silence_s": eou_cfg.get("silence_s"),
            "timeout_s": eou_cfg.get("timeout_s"),
        }
    )

    try:
        while True:
            message = await websocket.receive()
            message_type = str(message.get("type") or "")
            if message_type == "websocket.disconnect":
                return
            text_message = message.get("text")
            if text_message is not None:
                try:
                    command = json.loads(str(text_message or "{}"))
                except Exception:
                    command = {}
                command_type = str(command.get("type") or "").strip().lower()
                if command_type in {"stop", "end"}:
                    finalize_reason = "client_stop"
                    break
                if command_type in {"cancel", "abort"}:
                    await websocket.send_json({"ok": True, "type": "cancelled"})
                    await websocket.close(code=1000)
                    return
                continue

            audio_bytes = message.get("bytes")
            if audio_bytes is None:
                continue
            audio_bytes = bytes(audio_bytes or b"")
            if not audio_bytes:
                continue
            if len(audio_bytes) > max_chunk_bytes:
                await websocket.send_json({"ok": False, "type": "error", "error": "STT audio chunk is too large."})
                await websocket.close(code=1009)
                return
            if len(audio_buffer) + len(audio_bytes) > max_audio_bytes:
                finalize_reason = "max_audio"
                break

            now_ts = time.time()
            if first_audio_ts <= 0.0:
                first_audio_ts = now_ts
            audio_buffer.extend(audio_bytes)
            frame_count += 1

            metrics = eou_engine.process(audio_bytes, audio_format, now_ts)
            if bool(metrics.get("vad_error")):
                await websocket.send_json(
                    {
                        "ok": False,
                        "type": "error",
                        "error": str(metrics.get("error") or "VAD backend unavailable."),
                        "vad_backend": str(metrics.get("backend") or ""),
                    }
                )
                await websocket.close(code=1011)
                return

            if bool(metrics.get("voice_seen")) and not speech_started_sent:
                speech_started_sent = True
                await websocket.send_json(
                    {
                        "ok": True,
                        "type": "speech_start",
                        "speech_s": float(metrics.get("speech_s") or 0.0),
                        "score": float(metrics.get("max_probability", metrics.get("probability", 0.0)) or 0.0),
                    }
                )

            if bool(metrics.get("should_finalize")):
                finalize_reason = "server_vad"
                break
    except WebSocketDisconnect:
        return
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        with contextlib.suppress(Exception):
            await websocket.send_json({"ok": False, "type": "error", "error": str(exc) or type(exc).__name__})
            await websocket.close(code=1011)
        return

    if not audio_buffer:
        with contextlib.suppress(Exception):
            await websocket.send_json({"ok": True, "type": "final", "text": "", "reason": "no_audio"})
            await websocket.close(code=1000)
        return

    await websocket.send_json(
        {
            "ok": True,
            "type": "speech_end",
            "reason": finalize_reason,
            "speech_s": float(metrics.get("speech_s") or 0.0),
            "silence_s": float(metrics.get("silence_s") or 0.0),
            "timed_out": bool(metrics.get("timed_out")),
        }
    )

    duration_s = len(audio_buffer) / max(
        1.0,
        float(int(audio_format.get("rate") or 16000) * int(audio_format.get("width") or 2) * int(audio_format.get("channels") or 1)),
    )
    try:
        transcript, backend, backend_note = await _spud_link_transcribe_pcm_audio(
            bytes(audio_buffer),
            audio_format,
            language=language,
        )
    except Exception as exc:
        with contextlib.suppress(Exception):
            await websocket.send_json({"ok": False, "type": "error", "error": str(exc) or "Spud Link STT failed."})
            await websocket.close(code=1011)
        return

    _spud_link_touch_node_from_request(node, websocket)  # type: ignore[arg-type]
    node["activity"] = _spud_link_sanitize_activity(
        {
            "last_call_at": time.time(),
            "last_call_mode": "stt_stream",
            "last_model": backend,
            "last_user": user_name,
            "last_device": device_name,
            "role": role,
        },
        allow_previews=bool(settings.get("request_previews_enabled")),
    )
    _spud_link_store_node(node)
    logger.info(
        "[spudlink-stt] stream-final node=%s reason=%s frames=%s bytes=%s duration_s=%.2f transcript_len=%s stt=%s elapsed_ms=%.1f",
        node.get("id") or "-",
        finalize_reason,
        frame_count,
        len(audio_buffer),
        duration_s,
        len(transcript),
        backend,
        max(0.0, (time.time() - started_ts) * 1000.0),
    )
    await websocket.send_json(
        {
            "ok": True,
            "type": "final",
            "text": transcript,
            "backend": backend,
            "backend_note": backend_note,
            "reason": finalize_reason,
            "duration_s": duration_s,
        }
    )
    await websocket.close(code=1000)


@app.get("/api/chat/history")
def chat_history(limit: int = 0) -> Dict[str, Any]:
    max_display = _read_positive_int("tater:max_display", DEFAULT_MAX_DISPLAY)
    history = _load_chat_history()
    if limit > 0:
        history = history[-limit:]
    else:
        history = history[-max_display:]
    return {"messages": history}


@app.get("/api/chat/profile")
def chat_profile() -> Dict[str, Any]:
    chat_settings = redis_client.hgetall("chat_settings") or {}
    tater_first_name = str(redis_client.get("tater:first_name") or "Tater").strip() or "Tater"
    tater_last_name = str(redis_client.get("tater:last_name") or "Totterson").strip()
    tater_full_name = " ".join(part for part in [tater_first_name, tater_last_name] if part).strip() or "Tater Totterson"
    return {
        "username": str(chat_settings.get("username") or "User"),
        "user_avatar": _read_user_avatar_data_url(chat_settings),
        "tater_avatar": _read_tater_avatar_data_url(),
        "tater_name": tater_first_name,
        "tater_first_name": tater_first_name,
        "tater_last_name": tater_last_name,
        "tater_full_name": tater_full_name,
        "attach_max_mb_each": int(WEBUI_ATTACH_MAX_MB_EACH),
        "attach_max_mb_total": int(WEBUI_ATTACH_MAX_MB_TOTAL),
        "show_speed_stats": _show_speed_stats_enabled(default=False),
        "popup_effect_style": _normalize_popup_effect_style(
            redis_client.get(WEBUI_POPUP_EFFECT_STYLE_KEY),
            default=DEFAULT_WEBUI_POPUP_EFFECT_STYLE,
        ),
    }


@app.get("/api/chat/stats")
def chat_stats() -> Dict[str, Any]:
    return {
        "enabled": _show_speed_stats_enabled(default=False),
        "stats": _load_last_llm_stats(),
    }


@app.get("/api/notifiers/destinations")
def notifier_destinations(platform: str = "all", limit: int = 80) -> Dict[str, Any]:
    raw_platform = str(platform or "all").strip()
    use_platform: Optional[str] = None
    if raw_platform.lower() not in {"", "all", "*"}:
        use_platform = raw_platform
    payload = notifier_destination_catalog(
        redis_client=redis_client,
        platform=use_platform,
        limit=limit,
    )
    if use_platform is not None and not list(payload.get("platforms") or []):
        raise HTTPException(status_code=400, detail=f"Unknown notifier platform: {use_platform}")
    return {"ok": True, **payload}


@app.get("/api/chat/files/{file_id}")
def chat_file(file_id: str, mimetype: str = "application/octet-stream") -> Response:
    blob = _load_file_blob_from_redis(file_id)
    if blob is None:
        raise HTTPException(status_code=404, detail="Attachment not found or expired.")
    media_type = str(mimetype or "application/octet-stream").strip() or "application/octet-stream"
    return Response(content=blob, media_type=media_type)


@app.get("/v1/models")
def tater_openai_models(request: Request) -> Dict[str, Any]:
    _require_tater_api_request(request)
    created = int(time.time())
    models: List[Dict[str, Any]] = [
        {"id": "tater/base", "object": "model", "created": created, "owned_by": "tater"},
        {"id": "tater/direct", "object": "model", "created": created, "owned_by": "tater"},
        {"id": "tater/hydra", "object": "model", "created": created, "owned_by": "tater"},
    ]
    seen = {str(item.get("id")) for item in models}
    try:
        base_rows = resolve_hydra_base_servers(redis_conn=redis_client, include_legacy=True)
    except Exception:
        base_rows = []
    for row in base_rows or []:
        if not isinstance(row, dict):
            continue
        provider = _normalize_hydra_llm_provider(row.get("provider"))
        model = str(row.get("model") or "").strip()
        if not model:
            continue
        model_id = f"{provider}::{model}" if provider != HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE else model
        if model_id in seen:
            continue
        seen.add(model_id)
        models.append({"id": model_id, "object": "model", "created": created, "owned_by": "tater"})
    return {"object": "list", "data": models}


@app.post("/v1/chat/completions")
async def tater_openai_chat_completions(
    payload: OpenAIChatCompletionRequest,
    request: Request,
) -> Any:
    settings = _require_tater_api_request(request)
    completion = await _run_tater_api_chat_completion(payload, settings=settings, request=request)
    if bool(payload.stream):
        return StreamingResponse(
            _stream_openai_chat_completion(completion),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
    return completion


@app.post("/api/chat/send")
async def chat_send(payload: ChatRequest) -> Dict[str, Any]:
    message = str(payload.message or "").strip()
    raw_attachments = [item.model_dump(exclude_none=True) for item in (payload.attachments or [])]
    try:
        attachment_messages, input_artifacts = _normalize_chat_attachment_payloads(raw_attachments)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not message and not attachment_messages:
        raise HTTPException(status_code=400, detail="Message or attachment is required.")

    settings = redis_client.hgetall("chat_settings") or {}
    username = str(payload.username or settings.get("username") or "User").strip() or "User"
    session_id = str(payload.session_id or "").strip() or str(uuid.uuid4())

    if message:
        _save_chat_message("user", username, message)
    for attachment_item in attachment_messages:
        _save_chat_message("user", username, attachment_item)

    result = await _process_message(
        user_name=username,
        message_content=message,
        input_artifacts=input_artifacts,
        session_scope_id=session_id,
    )

    responses = list(result.get("responses") or [])
    for item in responses:
        _save_chat_message("assistant", "assistant", item)

    return {
        "session_id": session_id,
        "responses": responses,
    }


@app.post("/api/chat/jobs")
def create_chat_job(payload: ChatRequest) -> Dict[str, Any]:
    message = str(payload.message or "").strip()
    raw_attachments = [item.model_dump(exclude_none=True) for item in (payload.attachments or [])]
    try:
        attachment_messages, input_artifacts = _normalize_chat_attachment_payloads(raw_attachments)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not message and not attachment_messages:
        raise HTTPException(status_code=400, detail="Message or attachment is required.")

    settings = redis_client.hgetall("chat_settings") or {}
    username = str(payload.username or settings.get("username") or "User").strip() or "User"
    session_id = str(payload.session_id or "").strip() or str(uuid.uuid4())

    if message:
        _save_chat_message("user", username, message)
    for attachment_item in attachment_messages:
        _save_chat_message("user", username, attachment_item)

    return chat_jobs.create_job(
        user_name=username,
        message=message,
        input_artifacts=input_artifacts,
        session_id=session_id,
    )


@app.get("/api/chat/jobs/{job_id}")
def chat_job_status(job_id: str) -> Dict[str, Any]:
    snapshot = chat_jobs.get_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"Unknown chat job: {job_id}")
    return snapshot


@app.get("/api/chat/jobs/{job_id}/events")
async def chat_job_events(job_id: str, request: Request):
    event_queue = chat_jobs.get_event_queue(job_id)
    if event_queue is None:
        raise HTTPException(status_code=404, detail=f"Unknown chat job: {job_id}")

    async def _event_stream():
        snapshot = chat_jobs.get_snapshot(job_id)
        if snapshot is not None:
            yield _sse("status", snapshot)

        tick = 0
        while True:
            if await request.is_disconnected():
                break

            try:
                event = event_queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(1.0)
                tick += 1
                status_snapshot = chat_jobs.get_snapshot(job_id)
                if status_snapshot is None:
                    break
                if str(status_snapshot.get("status") or "") in {"done", "error"} and event_queue.empty():
                    break
                if tick % 15 == 0:
                    yield _sse("ping", {"job_id": job_id, "ts": time.time()})
                continue

            tick = 0
            event_type = str(event.get("type") or "status")
            yield _sse(event_type, event)
            if event_type in {"done", "job_error"}:
                break

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/chat/clear")
def clear_chat() -> Dict[str, Any]:
    redis_client.delete(CHAT_HISTORY_KEY)
    file_ids = redis_client.lrange(FILE_INDEX_KEY, 0, -1)
    if file_ids:
        for file_id in file_ids:
            token = str(file_id or "").strip()
            if not token:
                continue
            try:
                redis_blob_client.delete(f"{FILE_BLOB_KEY_PREFIX}{token}")
            except Exception:
                continue
    redis_client.delete(FILE_INDEX_KEY)
    redis_client.delete(LAST_LLM_STATS_KEY)
    return {"ok": True}


@app.get("/api/verbas")
def list_verbas() -> Dict[str, Any]:
    verba_registry_module.ensure_verbas_loaded()
    registry = verba_registry_module.get_verba_registry_snapshot()

    items: List[Dict[str, Any]] = []
    for plugin_id, plugin in registry.items():
        settings_category = str(getattr(plugin, "settings_category", "") or "").strip()
        required_settings = getattr(plugin, "required_settings", None)
        required_settings = required_settings if isinstance(required_settings, dict) else {}
        current_settings = get_verba_settings(settings_category) if settings_category else {}

        items.append(
            {
                "id": plugin_id,
                "name": _verba_display_name(plugin) or plugin_id,
                "description": str(getattr(plugin, "verba_dec", "") or getattr(plugin, "description", "") or "").strip(),
                "platforms": list(getattr(plugin, "platforms", []) or []),
                "enabled": get_verba_enabled(plugin_id),
                "settings_category": settings_category,
                "settings": _verba_setting_fields(plugin, required_settings, current_settings),
            }
        )

    items.sort(key=lambda row: str(row.get("name") or "").lower())
    return {"items": items}


@app.post("/api/verbas/{plugin_id}/enabled")
def set_verba_enabled_endpoint(plugin_id: str, payload: PluginToggleRequest) -> Dict[str, Any]:
    verba_registry_module.ensure_verbas_loaded()
    registry = verba_registry_module.get_verba_registry_snapshot()
    if plugin_id not in registry:
        raise HTTPException(status_code=404, detail=f"Unknown verba: {plugin_id}")

    set_verba_enabled_flag(plugin_id, bool(payload.enabled))
    return {"id": plugin_id, "enabled": bool(payload.enabled)}


@app.post("/api/verbas/{plugin_id}/settings")
def save_verba_settings_endpoint(plugin_id: str, payload: SettingsUpdateRequest) -> Dict[str, Any]:
    verba_registry_module.ensure_verbas_loaded()
    registry = verba_registry_module.get_verba_registry_snapshot()
    plugin = registry.get(plugin_id)
    if plugin is None:
        raise HTTPException(status_code=404, detail=f"Unknown verba: {plugin_id}")

    category = str(getattr(plugin, "settings_category", "") or "").strip()
    if not category:
        raise HTTPException(status_code=400, detail=f"{plugin_id} has no settings category")

    values = _verba_prepare_settings_values(plugin, dict(payload.values or {}))
    save_verba_settings_values(category, values)
    return {"id": plugin_id, "saved": True}


@app.get("/api/cores")
def list_cores() -> Dict[str, Any]:
    entries = core_registry_module.refresh_core_registry()
    rows: List[Dict[str, Any]] = []

    for core in entries:
        key = str(core.get("key") or "").strip()
        if not key:
            continue

        current_settings = redis_client.hgetall(f"{key}_settings") or {}
        desired_running = str(redis_client.get(f"{key}_running") or "").strip().lower() == "true"
        actual_running = core_runtime.is_running(key)

        rows.append(
            {
                "key": key,
                "label": core.get("label", key),
                "desired_running": desired_running,
                "running": actual_running,
                "settings": _setting_fields(core.get("required", {}), current_settings),
            }
        )

    rows.sort(key=lambda row: str(row.get("label") or "").lower())
    return {"items": rows}


@app.get("/api/cores/tabs")
def list_core_tabs() -> Dict[str, Any]:
    tabs = _discover_core_webui_tabs(core_registry_module.refresh_core_registry())
    dynamic_tabs: List[Dict[str, Any]] = []

    for tab in tabs:
        label = str(tab.get("label") or "").strip()
        if not label:
            continue
        if label.lower() == "hydra":
            continue
        dynamic_tabs.append(
            {
                "label": label,
                "core_key": str(tab.get("core_key") or "").strip(),
                "order": int(tab.get("order", 1000)),
                "requires_running": bool(tab.get("requires_running")),
                "running": bool(tab.get("running")),
            }
        )

    return {
        "manage_label": "Manage",
        "tabs": dynamic_tabs,
    }


@app.get("/api/cores/{core_key}/tab")
def get_core_tab_payload(core_key: str) -> Dict[str, Any]:
    key = str(core_key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="Missing core key.")

    tabs = _discover_core_webui_tabs(core_registry_module.refresh_core_registry())
    tab = next((item for item in tabs if str(item.get("core_key") or "").strip() == key), None)
    if not isinstance(tab, dict):
        raise HTTPException(status_code=404, detail=f"Unknown or unavailable core tab: {key}")

    return _load_surface_htmlui_tab_payload(tab)


@app.post("/api/cores/{core_key}/tab-action")
def run_core_tab_action(core_key: str, payload: CoreTabActionRequest) -> Dict[str, Any]:
    key = str(core_key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="Missing core key.")

    tabs = _discover_core_webui_tabs(core_registry_module.refresh_core_registry())
    tab = next((item for item in tabs if str(item.get("core_key") or "").strip() == key), None)
    if not isinstance(tab, dict):
        raise HTTPException(status_code=404, detail=f"Unknown core: {key}")

    return _run_surface_htmlui_tab_action(tab, payload)


async def _surface_request_payload(request: Request) -> Tuple[Dict[str, Any], str]:
    body_text = ""
    payload: Dict[str, Any] = {}
    if request.method.upper() not in {"POST", "PUT", "PATCH", "DELETE"}:
        return payload, body_text

    content_type = str(request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        try:
            parsed = await request.json()
        except Exception:
            parsed = {}
        if isinstance(parsed, dict):
            payload = parsed
        else:
            body_text = json.dumps(parsed)
    elif "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
        try:
            form = await request.form()
            payload = dict(form)
        except Exception:
            body_text = (await request.body()).decode("utf-8", "ignore")
    else:
        body_text = (await request.body()).decode("utf-8", "ignore")
    return payload, body_text


async def _maybe_await_result(result: Any) -> Any:
    if asyncio.iscoroutine(result):
        return await result
    return result


def _coerce_surface_result(result: Any) -> Any:
    if result is None:
        return {"ok": True}
    if isinstance(result, Response):
        return result
    if not isinstance(result, dict):
        return {"ok": True, "result": result}
    return result


async def _call_portal_asgi_app(module: Any, request: Request, api_path: str) -> Response:
    app_obj = getattr(module, "app", None)
    if not callable(app_obj):
        raise HTTPException(status_code=404, detail="Portal does not expose an API handler.")

    ready_hook = getattr(module, "ensure_portal_api_ready", None)
    if callable(ready_hook):
        try:
            await _maybe_await_result(ready_hook(redis_client=redis_client))
        except TypeError:
            await _maybe_await_result(ready_hook())

    body = await request.body()
    forwarded_path = "/" + str(api_path or "").strip("/")
    if forwarded_path == "/":
        forwarded_path = "/"

    scope = dict(request.scope)
    scope["path"] = forwarded_path
    scope["raw_path"] = forwarded_path.encode("utf-8")
    scope["root_path"] = ""
    scope["headers"] = list(request.scope.get("headers") or [])
    scope.pop("route", None)
    scope.pop("endpoint", None)
    scope["path_params"] = {}

    sent_body = False

    async def receive() -> Dict[str, Any]:
        nonlocal sent_body
        if not sent_body:
            sent_body = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    status_code = 500
    response_headers: List[Tuple[bytes, bytes]] = []
    chunks: List[bytes] = []

    async def send(message: Dict[str, Any]) -> None:
        nonlocal status_code, response_headers
        msg_type = str(message.get("type") or "")
        if msg_type == "http.response.start":
            status_code = int(message.get("status") or 500)
            response_headers = list(message.get("headers") or [])
        elif msg_type == "http.response.body":
            chunk = message.get("body") or b""
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            chunks.append(chunk)

    await app_obj(scope, receive, send)

    headers: Dict[str, str] = {}
    media_type = None
    for raw_key, raw_value in response_headers:
        key = raw_key.decode("latin-1")
        value = raw_value.decode("latin-1")
        key_l = key.lower()
        if key_l == "content-type":
            media_type = value
            continue
        if key_l in {"content-length", "transfer-encoding", "connection"}:
            continue
        headers[key] = value
    return Response(content=b"".join(chunks), status_code=status_code, headers=headers, media_type=media_type)


@app.api_route("/api/cores/{core_key}/webhook/{webhook_name}", methods=["GET", "POST"])
async def run_core_webhook(core_key: str, webhook_name: str, request: Request) -> Dict[str, Any]:
    key = str(core_key or "").strip()
    hook = str(webhook_name or "").strip()
    if not key or not hook:
        raise HTTPException(status_code=400, detail="Missing core key or webhook name.")

    client_host = request.client.host if request.client else "unknown"
    logger.info("[core-webhook] %s %s/%s from %s", request.method.upper(), key, hook, client_host)

    try:
        module = core_runtime._import_module(key, reload_module=False)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Unknown core: {key} ({exc})")

    handler = getattr(module, "handle_core_webhook", None)
    if not callable(handler):
        raise HTTPException(status_code=404, detail=f"{key} does not expose core webhooks.")

    payload, body_text = await _surface_request_payload(request)

    query = dict(request.query_params)
    try:
        result = handler(
            webhook=hook,
            payload=payload,
            query=query,
            body=body_text,
            method=request.method,
            headers=dict(request.headers),
            redis_client=redis_client,
            core_key=key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Core webhook failed: {exc}")

    return _coerce_surface_result(result)


@app.api_route("/api/portals/{portal_key}/webhook/{webhook_name}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def run_portal_webhook(portal_key: str, webhook_name: str, request: Request) -> Any:
    key = str(portal_key or "").strip()
    hook = str(webhook_name or "").strip()
    if not key or not hook:
        raise HTTPException(status_code=400, detail="Missing portal key or webhook name.")

    client_host = request.client.host if request.client else "unknown"
    logger.info("[portal-webhook] %s %s/%s from %s", request.method.upper(), key, hook, client_host)

    try:
        module = portal_runtime._import_module(key, reload_module=False)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Unknown portal: {key} ({exc})")

    handler = getattr(module, "handle_portal_webhook", None)
    if not callable(handler):
        raise HTTPException(status_code=404, detail=f"{key} does not expose portal webhooks.")

    payload, body_text = await _surface_request_payload(request)
    query = dict(request.query_params)
    try:
        result = handler(
            webhook=hook,
            payload=payload,
            query=query,
            body=body_text,
            method=request.method,
            headers=dict(request.headers),
            redis_client=redis_client,
            portal_key=key,
        )
        result = await _maybe_await_result(result)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Portal webhook failed: {exc}")

    return _coerce_surface_result(result)


@app.api_route("/api/portals/{portal_key}/api", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.api_route("/api/portals/{portal_key}/api/{api_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def run_portal_api(portal_key: str, request: Request, api_path: str = "") -> Any:
    key = str(portal_key or "").strip()
    route_path = str(api_path or "").strip("/")
    if not key:
        raise HTTPException(status_code=400, detail="Missing portal key.")

    client_host = request.client.host if request.client else "unknown"
    logger.info("[portal-api] %s %s/%s from %s", request.method.upper(), key, route_path or "-", client_host)

    try:
        module = portal_runtime._import_module(key, reload_module=False)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Unknown portal: {key} ({exc})")

    handler = getattr(module, "handle_portal_api", None)
    if callable(handler):
        payload, body_text = await _surface_request_payload(request)
        query = dict(request.query_params)
        try:
            result = handler(
                path=route_path,
                payload=payload,
                query=query,
                body=body_text,
                method=request.method,
                headers=dict(request.headers),
                redis_client=redis_client,
                portal_key=key,
            )
            result = await _maybe_await_result(result)
        except HTTPException:
            raise
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Portal API failed: {exc}")
        return _coerce_surface_result(result)

    try:
        return await _call_portal_asgi_app(module, request, route_path)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Portal API failed: {exc}")


@app.get("/api/settings/esphome/runtime")
def get_esphome_runtime_payload(panel: str = "") -> Dict[str, Any]:
    tab = _esphome_platform_tab_spec()
    return {
        "tab": {
            "label": str(tab.get("label") or "Voice"),
            "core_key": str(tab.get("core_key") or "esphome"),
            "surface_kind": str(tab.get("surface_kind") or "esphome"),
            "running": bool(tab.get("running")),
        },
        "payload": esphome_home_module.get_runtime_payload(
            redis_client=redis_client,
            core_key=str(tab.get("core_key") or "esphome"),
            core_tab=tab,
            panel=panel,
        ),
    }


@app.post("/api/settings/esphome/runtime/action")
def run_esphome_runtime_action(payload: CoreTabActionRequest) -> Dict[str, Any]:
    try:
        return esphome_home_module.handle_runtime_action(
            action=str(payload.action or "").strip(),
            payload=payload.payload if isinstance(payload.payload, dict) else {},
            redis_client=redis_client,
            core_key="esphome",
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"ESPHome action failed: {exc}")


@app.get("/api/settings/esphome/firmware-web/{artifact_id}/{relative_path:path}")
def get_esphome_firmware_web_artifact(artifact_id: str, relative_path: str) -> FileResponse:
    try:
        path = esphome_firmware_module.browser_flash_artifact_path(artifact_id, relative_path)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    media_type = "application/json" if path.suffix.lower() == ".json" else "application/octet-stream"
    return FileResponse(path, media_type=media_type)


@app.post("/api/cores/{core_key}/start")
def start_core(core_key: str) -> Dict[str, Any]:
    entries = core_registry_module.refresh_core_registry()
    if core_key not in {str(item.get("key") or "") for item in entries}:
        raise HTTPException(status_code=404, detail=f"Unknown core: {core_key}")

    status = core_runtime.start(core_key)
    redis_client.set(f"{core_key}_running", "true")
    return {"key": core_key, **status}


@app.post("/api/cores/{core_key}/stop")
def stop_core(core_key: str) -> Dict[str, Any]:
    status = core_runtime.stop(core_key)
    redis_client.set(f"{core_key}_running", "false")
    redis_client.set(f"tater:cooldown:{core_key}", str(time.time()))
    return {"key": core_key, **status}


@app.post("/api/cores/{core_key}/settings")
def save_core_settings(core_key: str, payload: SettingsUpdateRequest) -> Dict[str, Any]:
    mapping = {
        k: json.dumps(v) if isinstance(v, (dict, list, bool)) else str(v)
        for k, v in (payload.values or {}).items()
    }
    if mapping:
        redis_client.hset(f"{core_key}_settings", mapping=mapping)
    return {"key": core_key, "saved": True}


@app.get("/api/portals")
def list_portals() -> Dict[str, Any]:
    entries = portal_registry_module.refresh_portal_registry()
    rows: List[Dict[str, Any]] = []

    for portal in entries:
        key = str(portal.get("key") or "").strip()
        if not key:
            continue

        current_settings = redis_client.hgetall(f"{key}_settings") or {}
        desired_running = str(redis_client.get(f"{key}_running") or "").strip().lower() == "true"
        actual_running = portal_runtime.is_running(key)

        rows.append(
            {
                "key": key,
                "label": portal.get("label", key),
                "desired_running": desired_running,
                "running": actual_running,
                "settings": _portal_setting_fields(key, portal.get("required", {}), current_settings),
            }
        )

    rows.sort(key=lambda row: str(row.get("label") or "").lower())
    return {"items": rows}


@app.post("/api/portals/{portal_key}/start")
def start_portal(portal_key: str) -> Dict[str, Any]:
    entries = portal_registry_module.refresh_portal_registry()
    if portal_key not in {str(item.get("key") or "") for item in entries}:
        raise HTTPException(status_code=404, detail=f"Unknown portal: {portal_key}")

    status = portal_runtime.start(portal_key)
    redis_client.set(f"{portal_key}_running", "true")
    return {"key": portal_key, **status}


@app.post("/api/portals/{portal_key}/stop")
def stop_portal(portal_key: str) -> Dict[str, Any]:
    status = portal_runtime.stop(portal_key)
    redis_client.set(f"{portal_key}_running", "false")
    redis_client.set(f"tater:cooldown:{portal_key}", str(time.time()))
    return {"key": portal_key, **status}


@app.post("/api/portals/{portal_key}/settings")
def save_portal_settings(portal_key: str, payload: SettingsUpdateRequest) -> Dict[str, Any]:
    values = _portal_prepare_settings_values(portal_key, dict(payload.values or {}))
    mapping = {
        k: json.dumps(v) if isinstance(v, (dict, list, bool)) else str(v)
        for k, v in values.items()
    }
    if mapping:
        redis_client.hset(f"{portal_key}_settings", mapping=mapping)
    return {"key": portal_key, "saved": True}


def _restart_integration_runtime_if_running() -> str:
    try:
        running = bool(integration_runtime_status(redis_client).get("running"))
    except Exception:
        running = False
    if not running:
        return ""
    _run_async_sync(restart_integration_runtime(redis_client), timeout=15.0)
    return "runtime restarted"


@app.get("/api/shop/integrations")
def get_integration_shop() -> Dict[str, Any]:
    snapshot = _integration_shop_raw()
    return {
        "repos": snapshot["repos"],
        "errors": snapshot["errors"],
        "installed": snapshot["installed"],
        "catalog": snapshot["catalog"],
        "updates_available": snapshot["updates_available"],
    }


@app.post("/api/shop/integrations/repos")
def save_integration_repos(payload: ShopReposRequest) -> Dict[str, Any]:
    rows = _normalize_repo_rows(payload.repos)
    integration_store_module.save_additional_integration_shop_manifest_repos(rows)
    snapshot = _integration_shop_raw()
    return {"ok": True, "repos": snapshot["repos"]}


@app.post("/api/shop/integrations/install")
def install_integration(payload: ShopItemRequest) -> Dict[str, Any]:
    integration_id = str(payload.id or "").strip()
    if not integration_id:
        raise HTTPException(status_code=400, detail="Integration id is required.")

    snapshot = _integration_shop_raw()
    item = None
    for raw in snapshot.get("_catalog_items_raw", []):
        if str((raw or {}).get("id") or "").strip() == integration_id:
            item = raw
            break
    if not isinstance(item, dict):
        raise HTTPException(status_code=404, detail=f"Integration not found in catalog: {integration_id}")

    ok, msg = integration_store_module.install_integration_from_shop_item(item)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    return {"ok": True, "message": msg}


@app.post("/api/shop/integrations/enable")
def enable_integration(payload: ShopItemRequest) -> Dict[str, Any]:
    integration_id = str(payload.id or "").strip()
    if not integration_id:
        raise HTTPException(status_code=400, detail="Integration id is required.")

    snapshot = _integration_shop_raw()
    item = None
    for raw in snapshot.get("_catalog_items_raw", []):
        if str((raw or {}).get("id") or "").strip() == integration_id:
            item = raw
            break
    if not isinstance(item, dict):
        raise HTTPException(status_code=404, detail=f"Integration not found in catalog: {integration_id}")

    msg = f"Enabled {integration_id}"
    if not integration_store_module.is_integration_installed(integration_id):
        ok, msg = integration_store_module.install_integration_from_shop_item(item)
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
    integration_store_module.set_integration_enabled(integration_id, True)
    restart_note = _restart_integration_runtime_if_running()
    return {
        "ok": True,
        "message": msg if msg.lower().startswith("enabled") else f"{msg}; enabled {integration_id}",
        "restart": restart_note,
    }


@app.post("/api/shop/integrations/disable")
def disable_integration(payload: ShopItemRequest) -> Dict[str, Any]:
    integration_id = str(payload.id or "").strip()
    if not integration_id:
        raise HTTPException(status_code=400, detail="Integration id is required.")
    if integration_id in integration_store_module.REQUIRED_INTEGRATION_IDS:
        raise HTTPException(status_code=400, detail=f"{integration_id} is required by Tater and cannot be disabled.")

    integration_store_module.set_integration_enabled(integration_id, False)
    cleanup = clear_integration_runtime_provider(integration_id, redis_client)
    restart_note = _restart_integration_runtime_if_running()
    return {
        "ok": True,
        "message": f"Disabled {integration_id}",
        "cleanup": cleanup,
        "restart": restart_note,
    }


@app.post("/api/shop/integrations/update")
def update_integration(payload: ShopItemRequest) -> Dict[str, Any]:
    integration_id = str(payload.id or "").strip()
    if not integration_id:
        raise HTTPException(status_code=400, detail="Integration id is required.")

    snapshot = _integration_shop_raw()
    entry = None
    for raw in snapshot.get("_installed_entries_raw", []):
        if str((raw or {}).get("id") or "").strip() == integration_id:
            entry = raw
            break
    if not isinstance(entry, dict):
        raise HTTPException(status_code=404, detail=f"Installed integration not found: {integration_id}")

    catalog_item = entry.get("catalog_item")
    if not isinstance(catalog_item, dict):
        raise HTTPException(status_code=400, detail=f"No catalog update source for {integration_id}")

    ok, msg = integration_store_module.install_integration_from_shop_item(catalog_item)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    restart_note = _restart_integration_runtime_if_running()
    return {
        "ok": True,
        "message": msg,
        "restart": restart_note,
    }


@app.post("/api/shop/integrations/update-all")
def update_all_integrations() -> Dict[str, Any]:
    snapshot = _integration_shop_raw()
    updated: List[str] = []
    failed: List[str] = []

    for entry in snapshot.get("_installed_entries_raw", []):
        if not isinstance(entry, dict):
            continue
        if not bool(entry.get("update_available")):
            continue
        if not bool(entry.get("enabled")):
            continue
        integration_id = str(entry.get("id") or "").strip()
        catalog_item = entry.get("catalog_item")
        if not integration_id or not isinstance(catalog_item, dict):
            continue
        ok, msg = integration_store_module.install_integration_from_shop_item(catalog_item)
        if ok:
            updated.append(integration_id)
        else:
            failed.append(msg)

    restart_note = _restart_integration_runtime_if_running() if updated else ""
    return {
        "ok": True,
        "updated": updated,
        "failed": failed,
        "restart": restart_note,
    }


@app.post("/api/shop/integrations/remove")
def remove_integration(payload: ShopRemoveRequest) -> Dict[str, Any]:
    integration_id = str(payload.id or "").strip()
    if not integration_id:
        raise HTTPException(status_code=400, detail="Integration id is required.")

    snapshot = _integration_shop_raw()
    catalog_item = None
    for raw in snapshot.get("_installed_entries_raw", []):
        if str((raw or {}).get("id") or "").strip() == integration_id:
            catalog_item = raw.get("catalog_item") if isinstance(raw, dict) else None
            break

    integration_store_module.set_integration_enabled(integration_id, False)
    cleanup = clear_integration_runtime_provider(integration_id, redis_client)
    ok, msg = integration_store_module.uninstall_integration_file(integration_id)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    cleanup_message = ""
    if bool(payload.purge_redis):
        ok2, msg2 = integration_store_module.clear_integration_redis_data(
            integration_id,
            catalog_item=catalog_item if isinstance(catalog_item, dict) else None,
        )
        cleanup_message = msg2
        if not ok2:
            raise HTTPException(status_code=400, detail=f"Removed file, but Redis cleanup failed: {msg2}")

    restart_note = _restart_integration_runtime_if_running()
    return {
        "ok": True,
        "message": msg,
        "cleanup": cleanup_message,
        "runtime_cleanup": cleanup,
        "restart": restart_note,
    }


@app.get("/api/shop/verbas")
def get_verba_shop() -> Dict[str, Any]:
    snapshot = _verba_shop_raw()
    return {
        "repos": snapshot["repos"],
        "errors": snapshot["errors"],
        "installed": snapshot["installed"],
        "catalog": snapshot["catalog"],
        "updates_available": snapshot["updates_available"],
    }


@app.post("/api/shop/verbas/repos")
def save_verba_repos(payload: ShopReposRequest) -> Dict[str, Any]:
    rows = _normalize_repo_rows(payload.repos)
    verba_store_module.save_additional_shop_manifest_repos(rows)
    snapshot = _verba_shop_raw()
    return {"ok": True, "repos": snapshot["repos"]}


@app.post("/api/shop/verbas/install")
def install_verba(payload: ShopItemRequest) -> Dict[str, Any]:
    plugin_id = str(payload.id or "").strip()
    if not plugin_id:
        raise HTTPException(status_code=400, detail="Plugin id is required.")

    snapshot = _verba_shop_raw()
    item = None
    for raw in snapshot.get("_catalog_items_raw", []):
        if str((raw or {}).get("id") or "").strip() == plugin_id:
            item = raw
            break
    if not isinstance(item, dict):
        raise HTTPException(status_code=404, detail=f"Plugin not found in catalog: {plugin_id}")

    ok, msg = verba_store_module.install_verba_from_shop_item(item)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    verba_registry_module.reload_verbas()
    return {"ok": True, "message": msg}


@app.post("/api/shop/verbas/update")
def update_verba(payload: ShopItemRequest) -> Dict[str, Any]:
    plugin_id = str(payload.id or "").strip()
    if not plugin_id:
        raise HTTPException(status_code=400, detail="Plugin id is required.")

    snapshot = _verba_shop_raw()
    entry = None
    for raw in snapshot.get("_installed_entries_raw", []):
        if str((raw or {}).get("id") or "").strip() == plugin_id:
            entry = raw
            break
    if not isinstance(entry, dict):
        raise HTTPException(status_code=404, detail=f"Installed plugin not found: {plugin_id}")

    catalog_item = entry.get("catalog_item")
    if not isinstance(catalog_item, dict):
        raise HTTPException(status_code=400, detail=f"No catalog update source for {plugin_id}")

    ok, msg = verba_store_module.install_verba_from_shop_item(catalog_item)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    verba_registry_module.reload_verbas()
    return {"ok": True, "message": msg}


@app.post("/api/shop/verbas/update-all")
def update_all_verbas() -> Dict[str, Any]:
    snapshot = _verba_shop_raw()
    updated: List[str] = []
    failed: List[str] = []

    for entry in snapshot.get("_installed_entries_raw", []):
        if not isinstance(entry, dict):
            continue
        if not bool(entry.get("update_available")):
            continue
        plugin_id = str(entry.get("id") or "").strip()
        catalog_item = entry.get("catalog_item")
        if not plugin_id or not isinstance(catalog_item, dict):
            continue
        ok, msg = verba_store_module.install_verba_from_shop_item(catalog_item)
        if ok:
            updated.append(plugin_id)
        else:
            failed.append(msg)

    if updated:
        verba_registry_module.reload_verbas()

    return {
        "ok": True,
        "updated": updated,
        "failed": failed,
    }


@app.post("/api/shop/verbas/remove")
def remove_verba(payload: ShopRemoveRequest) -> Dict[str, Any]:
    plugin_id = str(payload.id or "").strip()
    if not plugin_id:
        raise HTTPException(status_code=400, detail="Plugin id is required.")

    loaded = verba_registry_module.get_verba_registry_snapshot().get(plugin_id)
    category_hint = getattr(loaded, "settings_category", None) if loaded else None

    ok, msg = verba_store_module.uninstall_verba_file(plugin_id)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    try:
        set_verba_enabled_flag(plugin_id, False)
    except Exception:
        pass

    cleanup_message = ""
    if bool(payload.purge_redis):
        ok2, msg2 = verba_store_module.clear_verba_redis_data(plugin_id, category_hint=category_hint)
        cleanup_message = msg2
        if not ok2:
            raise HTTPException(status_code=400, detail=f"Removed file, but Redis cleanup failed: {msg2}")

    verba_registry_module.reload_verbas()
    return {
        "ok": True,
        "message": msg,
        "cleanup": cleanup_message,
    }


@app.get("/api/shop/cores")
def get_core_shop() -> Dict[str, Any]:
    snapshot = _core_shop_raw()
    return {
        "repos": snapshot["repos"],
        "errors": snapshot["errors"],
        "installed": snapshot["installed"],
        "catalog": snapshot["catalog"],
        "updates_available": snapshot["updates_available"],
    }


@app.post("/api/shop/cores/repos")
def save_core_repos(payload: ShopReposRequest) -> Dict[str, Any]:
    rows = _normalize_repo_rows(payload.repos)
    core_store_module.save_additional_core_shop_manifest_repos(rows)
    snapshot = _core_shop_raw()
    return {"ok": True, "repos": snapshot["repos"]}


@app.post("/api/shop/cores/install")
def install_core(payload: ShopItemRequest) -> Dict[str, Any]:
    core_id = str(payload.id or "").strip()
    if not core_id:
        raise HTTPException(status_code=400, detail="Core id is required.")

    snapshot = _core_shop_raw()
    item = None
    for raw in snapshot.get("_catalog_items_raw", []):
        if str((raw or {}).get("id") or "").strip() == core_id:
            item = raw
            break
    if not isinstance(item, dict):
        raise HTTPException(status_code=404, detail=f"Core not found in catalog: {core_id}")

    ok, msg = core_store_module.install_core_from_shop_item(item)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    module_key = f"{core_id}_core"
    should_run = str(redis_client.get(f"{module_key}_running") or "").strip().lower() == "true"
    if should_run and not core_runtime.is_running(module_key):
        core_runtime.start(module_key)

    return {"ok": True, "message": msg}


@app.post("/api/shop/cores/update")
def update_core(payload: ShopItemRequest) -> Dict[str, Any]:
    core_id = str(payload.id or "").strip()
    if not core_id:
        raise HTTPException(status_code=400, detail="Core id is required.")

    snapshot = _core_shop_raw()
    entry = None
    for raw in snapshot.get("_installed_entries_raw", []):
        if str((raw or {}).get("id") or "").strip() == core_id:
            entry = raw
            break
    if not isinstance(entry, dict):
        raise HTTPException(status_code=404, detail=f"Installed core not found: {core_id}")

    catalog_item = entry.get("catalog_item")
    if not isinstance(catalog_item, dict):
        raise HTTPException(status_code=400, detail=f"No catalog update source for {core_id}")

    ok, msg = core_store_module.install_core_from_shop_item(catalog_item)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    module_key = str(entry.get("module_key") or f"{core_id}_core").strip() or f"{core_id}_core"
    was_running = bool(entry.get("running"))
    restart_note = ""
    if was_running:
        core_runtime.stop(module_key)
        core_runtime.start(module_key)
        restart_note = "runtime restarted"

    return {
        "ok": True,
        "message": msg,
        "restart": restart_note,
    }


@app.post("/api/shop/cores/update-all")
def update_all_cores() -> Dict[str, Any]:
    snapshot = _core_shop_raw()
    updated: List[str] = []
    failed: List[str] = []

    for entry in snapshot.get("_installed_entries_raw", []):
        if not isinstance(entry, dict):
            continue
        if not bool(entry.get("update_available")):
            continue

        core_id = str(entry.get("id") or "").strip()
        catalog_item = entry.get("catalog_item")
        if not core_id or not isinstance(catalog_item, dict):
            continue

        ok, msg = core_store_module.install_core_from_shop_item(catalog_item)
        if not ok:
            failed.append(msg)
            continue

        module_key = str(entry.get("module_key") or f"{core_id}_core").strip() or f"{core_id}_core"
        if bool(entry.get("running")):
            try:
                core_runtime.stop(module_key)
                core_runtime.start(module_key)
            except Exception as exc:
                failed.append(f"{core_id}: updated but restart failed ({exc})")
                continue

        updated.append(core_id)

    return {
        "ok": True,
        "updated": updated,
        "failed": failed,
    }


@app.post("/api/shop/cores/remove")
def remove_core(payload: ShopRemoveRequest) -> Dict[str, Any]:
    core_id = str(payload.id or "").strip()
    if not core_id:
        raise HTTPException(status_code=400, detail="Core id is required.")

    module_key = f"{core_id}_core"
    if core_runtime.is_running(module_key):
        core_runtime.stop(module_key)

    ok, msg = core_store_module.uninstall_core_file(core_id)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    redis_client.set(f"{module_key}_running", "false")

    cleanup_message = ""
    if bool(payload.purge_redis):
        ok2, msg2 = core_store_module.clear_core_redis_data(core_id, module_key=module_key)
        cleanup_message = msg2
        if not ok2:
            raise HTTPException(status_code=400, detail=f"Removed file, but Redis cleanup failed: {msg2}")

    return {
        "ok": True,
        "message": msg,
        "cleanup": cleanup_message,
    }


@app.get("/api/shop/portals")
def get_portal_shop() -> Dict[str, Any]:
    snapshot = _portal_shop_raw()
    return {
        "repos": snapshot["repos"],
        "errors": snapshot["errors"],
        "installed": snapshot["installed"],
        "catalog": snapshot["catalog"],
        "updates_available": snapshot["updates_available"],
    }


@app.post("/api/shop/portals/repos")
def save_portal_repos(payload: ShopReposRequest) -> Dict[str, Any]:
    rows = _normalize_repo_rows(payload.repos)
    portal_store_module.save_additional_portal_shop_manifest_repos(rows)
    snapshot = _portal_shop_raw()
    return {"ok": True, "repos": snapshot["repos"]}


@app.post("/api/shop/portals/install")
def install_portal(payload: ShopItemRequest) -> Dict[str, Any]:
    portal_id = str(payload.id or "").strip()
    if not portal_id:
        raise HTTPException(status_code=400, detail="Portal id is required.")

    snapshot = _portal_shop_raw()
    item = None
    for raw in snapshot.get("_catalog_items_raw", []):
        if str((raw or {}).get("id") or "").strip() == portal_id:
            item = raw
            break
    if not isinstance(item, dict):
        raise HTTPException(status_code=404, detail=f"Portal not found in catalog: {portal_id}")

    ok, msg = portal_store_module.install_portal_from_shop_item(item)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    module_key = f"{portal_id}_portal"
    should_run = str(redis_client.get(f"{module_key}_running") or "").strip().lower() == "true"
    if should_run and not portal_runtime.is_running(module_key):
        portal_runtime.start(module_key)

    return {"ok": True, "message": msg}


@app.post("/api/shop/portals/update")
def update_portal(payload: ShopItemRequest) -> Dict[str, Any]:
    portal_id = str(payload.id or "").strip()
    if not portal_id:
        raise HTTPException(status_code=400, detail="Portal id is required.")

    snapshot = _portal_shop_raw()
    entry = None
    for raw in snapshot.get("_installed_entries_raw", []):
        if str((raw or {}).get("id") or "").strip() == portal_id:
            entry = raw
            break
    if not isinstance(entry, dict):
        raise HTTPException(status_code=404, detail=f"Installed portal not found: {portal_id}")

    catalog_item = entry.get("catalog_item")
    if not isinstance(catalog_item, dict):
        raise HTTPException(status_code=400, detail=f"No catalog update source for {portal_id}")

    ok, msg = portal_store_module.install_portal_from_shop_item(catalog_item)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    module_key = str(entry.get("module_key") or f"{portal_id}_portal").strip() or f"{portal_id}_portal"
    was_running = bool(entry.get("running"))
    restart_note = ""
    if was_running:
        portal_runtime.stop(module_key)
        portal_runtime.start(module_key)
        restart_note = "runtime restarted"

    return {
        "ok": True,
        "message": msg,
        "restart": restart_note,
    }


@app.post("/api/shop/portals/update-all")
def update_all_portals() -> Dict[str, Any]:
    snapshot = _portal_shop_raw()
    updated: List[str] = []
    failed: List[str] = []

    for entry in snapshot.get("_installed_entries_raw", []):
        if not isinstance(entry, dict):
            continue
        if not bool(entry.get("update_available")):
            continue

        portal_id = str(entry.get("id") or "").strip()
        catalog_item = entry.get("catalog_item")
        if not portal_id or not isinstance(catalog_item, dict):
            continue

        ok, msg = portal_store_module.install_portal_from_shop_item(catalog_item)
        if not ok:
            failed.append(msg)
            continue

        module_key = str(entry.get("module_key") or f"{portal_id}_portal").strip() or f"{portal_id}_portal"
        if bool(entry.get("running")):
            try:
                portal_runtime.stop(module_key)
                portal_runtime.start(module_key)
            except Exception as exc:
                failed.append(f"{portal_id}: updated but restart failed ({exc})")
                continue

        updated.append(portal_id)

    return {
        "ok": True,
        "updated": updated,
        "failed": failed,
    }


@app.post("/api/shop/portals/remove")
def remove_portal(payload: ShopRemoveRequest) -> Dict[str, Any]:
    portal_id = str(payload.id or "").strip()
    if not portal_id:
        raise HTTPException(status_code=400, detail="Portal id is required.")

    module_key = f"{portal_id}_portal"
    if portal_runtime.is_running(module_key):
        portal_runtime.stop(module_key)

    ok, msg = portal_store_module.uninstall_portal_file(portal_id)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    redis_client.set(f"{module_key}_running", "false")

    cleanup_message = ""
    if bool(payload.purge_redis):
        ok2, msg2 = portal_store_module.clear_portal_redis_data(portal_id, module_key=module_key)
        cleanup_message = msg2
        if not ok2:
            raise HTTPException(status_code=400, detail=f"Removed file, but Redis cleanup failed: {msg2}")

    return {
        "ok": True,
        "message": msg,
        "cleanup": cleanup_message,
    }


_HYDRA_METRIC_NAMES = (
    "total_turns",
    "total_tools_called",
    "total_repairs",
    "validation_failures",
    "tool_failures",
)
_HYDRA_METRIC_PLATFORMS = (
    "webui",
    "discord",
    "irc",
    "telegram",
    "matrix",
    "homeassistant",
    "voice_core",
    "little_spud",
    "homekit",
    "xbmc",
    "automation",
)


def _coerce_redis_counter(value: Any) -> int:
    try:
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="ignore")
        return int(str(value).strip())
    except Exception:
        return 0


def _hydra_platform_display_label(platform: str) -> str:
    labels = {
        "all": "All",
        "webui": "WebUI",
        "homeassistant": "Home Assistant",
        "voice_core": "Voice Core",
        "little_spud": "Little Spud",
        "homekit": "HomeKit",
        "xbmc": "XBMC",
        "automation": "Automations",
    }
    token = str(platform or "").strip().lower()
    return labels.get(token, token.title())


def _hydra_ledger_keys_for_platform(platform: str) -> List[str]:
    plat = str(platform or "all").strip().lower() or "all"
    if plat == "all":
        keys: List[str] = []
        try:
            keys.extend(sorted(str(k) for k in redis_client.scan_iter(match="tater:hydra:ledger:*")))
        except Exception:
            pass
        deduped: List[str] = []
        seen = set()
        for key in keys:
            if key in seen:
                continue
            deduped.append(key)
            seen.add(key)
        return deduped
    normalized = normalize_platform(plat or "webui")
    return [f"tater:hydra:ledger:{normalized}"]


def _load_hydra_ledger_entries(platform: str, limit: int) -> List[Dict[str, Any]]:
    max_limit = max(10, min(int(limit or 50), 300))
    rows: List[Dict[str, Any]] = []
    for key in _hydra_ledger_keys_for_platform(platform):
        try:
            raw_items = redis_client.lrange(key, -max_limit, -1) or []
        except Exception:
            raw_items = []
        for raw in raw_items:
            try:
                item = json.loads(raw)
            except Exception:
                continue
            if not isinstance(item, dict):
                continue
            row = dict(item)
            row["_ledger_key"] = key
            rows.append(row)
    rows.sort(key=lambda item: float(item.get("timestamp") or 0.0), reverse=True)
    return rows[:max_limit]


def _normalize_hydra_validation_for_view(
    validation: Any,
    *,
    planned_tool: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    raw = validation if isinstance(validation, dict) else {}
    status = str(raw.get("status") or "").strip().lower()
    if status in {"skipped", "ok", "failed"}:
        out = {
            "status": status,
            "repair_used": bool(raw.get("repair_used")),
            "reason": str(raw.get("reason") or ""),
        }
        try:
            out["attempts"] = int(raw.get("attempts"))
        except Exception:
            out["attempts"] = 0 if status == "skipped" else (2 if out["repair_used"] else 1)
        if raw.get("error") is not None:
            out["error"] = str(raw.get("error") or "")
        return out

    if "ok" in raw:
        ok = bool(raw.get("ok"))
        reason = str(raw.get("reason") or "")
        repair_used = bool(raw.get("repair_used"))
        if not ok and reason == "no_tool":
            return {"status": "skipped", "repair_used": False, "reason": "no_tool", "attempts": 0}
        if ok:
            return {
                "status": "ok",
                "repair_used": repair_used,
                "reason": "repaired" if repair_used else (reason or "ok"),
                "attempts": 2 if repair_used else 1,
            }
        return {
            "status": "failed",
            "repair_used": repair_used,
            "reason": reason or "invalid_tool_call",
            "attempts": 2 if repair_used else 1,
        }

    has_planned_tool = isinstance(planned_tool, dict) and bool(str(planned_tool.get("function") or "").strip())
    if not has_planned_tool:
        return {"status": "skipped", "repair_used": False, "reason": "no_tool", "attempts": 0}
    return {"status": "failed", "repair_used": False, "reason": "invalid_tool_call", "attempts": 1}


def _safe_rate(numerator: int, denominator: int) -> float:
    denom = max(1, int(denominator or 0))
    return float(numerator or 0) / float(denom)


def _hydra_rate_rows(metrics: Dict[str, int]) -> List[Dict[str, Any]]:
    turns = int(metrics.get("total_turns", 0) or 0)
    tools = int(metrics.get("total_tools_called", 0) or 0)
    repairs = int(metrics.get("total_repairs", 0) or 0)
    validation_failures = int(metrics.get("validation_failures", 0) or 0)
    tool_failures = int(metrics.get("tool_failures", 0) or 0)
    return [
        {"metric": "tool_call_rate", "value": round(_safe_rate(tools, turns), 4)},
        {"metric": "repair_rate", "value": round(_safe_rate(repairs, turns), 4)},
        {"metric": "validation_failure_rate", "value": round(_safe_rate(validation_failures, turns), 4)},
        {"metric": "tool_failure_rate", "value": round(_safe_rate(tool_failures, max(1, tools)), 4)},
    ]


def _load_hydra_metrics(platform: str) -> Tuple[str, Dict[str, int], Dict[str, int]]:
    selected = str(platform or "").strip().lower()
    metric_platform = normalize_platform(selected if selected and selected != "all" else "webui")
    global_metrics: Dict[str, int] = {}
    platform_metrics: Dict[str, int] = {}
    for name in _HYDRA_METRIC_NAMES:
        global_metrics[name] = _coerce_redis_counter(redis_client.get(f"tater:hydra:metrics:{name}"))
        if selected == "all":
            platform_metrics[name] = global_metrics[name]
        else:
            platform_metrics[name] = _coerce_redis_counter(
                redis_client.get(f"tater:hydra:metrics:{name}:{metric_platform}")
            )
    return metric_platform, global_metrics, platform_metrics


def _load_hydra_platform_metric_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for platform in _HYDRA_METRIC_PLATFORMS:
        row: Dict[str, Any] = {
            "platform": platform,
            "platform_label": _hydra_platform_display_label(platform),
        }
        for name in _HYDRA_METRIC_NAMES:
            row[name] = _coerce_redis_counter(redis_client.get(f"tater:hydra:metrics:{name}:{platform}"))
        rates = _hydra_rate_rows(row)
        for rate_row in rates:
            row[str(rate_row.get("metric") or "")] = float(rate_row.get("value") or 0.0)
        rows.append(row)
    return rows


def _reset_hydra_metrics(platform: str) -> int:
    plat = str(platform or "").strip().lower()
    keys: List[str] = []
    if plat == "all":
        try:
            keys = [str(k) for k in redis_client.scan_iter(match="tater:hydra:metrics:*")]
        except Exception:
            keys = []
    else:
        metric_platform = normalize_platform(plat or "webui")
        for name in _HYDRA_METRIC_NAMES:
            keys.append(f"tater:hydra:metrics:{name}")
            keys.append(f"tater:hydra:metrics:{name}:{metric_platform}")

    deleted = 0
    for key in keys:
        try:
            deleted += int(redis_client.delete(key) or 0)
        except Exception:
            continue
    return deleted


def _clear_hydra_ledger(platform: str) -> int:
    deleted = 0
    for key in _hydra_ledger_keys_for_platform(platform):
        try:
            deleted += int(redis_client.delete(key) or 0)
        except Exception:
            continue
    return deleted


@app.get("/api/settings/hydra/metrics")
def get_hydra_metrics(
    platform: str = "webui",
    limit: int = 50,
    outcome: str = "all",
    tool: str = "all",
    show_only_tool_turns: bool = False,
) -> Dict[str, Any]:
    selected_platform = str(platform or "webui").strip().lower() or "webui"
    if selected_platform != "all":
        selected_platform = normalize_platform(selected_platform)

    allowed_outcomes = {"all", "done", "blocked", "failed"}
    outcome_filter = str(outcome or "all").strip().lower()
    if outcome_filter not in allowed_outcomes:
        outcome_filter = "all"

    selected_tool = str(tool or "all").strip()
    selected_tool_cmp = selected_tool.lower() or "all"
    max_limit = max(10, min(int(limit or 50), 300))

    metric_platform, global_metrics, platform_metrics = _load_hydra_metrics(selected_platform)
    ledger_rows = _load_hydra_ledger_entries(selected_platform, max_limit)

    tool_options = sorted(
        {
            str((row.get("planned_tool") or {}).get("function") or "").strip()
            for row in ledger_rows
            if isinstance(row.get("planned_tool"), dict)
            and str((row.get("planned_tool") or {}).get("function") or "").strip()
        }
    )
    if selected_tool_cmp != "all" and selected_tool not in tool_options:
        selected_tool = "all"
        selected_tool_cmp = "all"

    filtered_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    tool_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}

    for idx, row in enumerate(ledger_rows):
        planned_tool = row.get("planned_tool") if isinstance(row.get("planned_tool"), dict) else {}
        planned_tool_name = str(planned_tool.get("function") or "").strip()
        row_outcome = str(row.get("outcome") or "").strip().lower()

        if outcome_filter != "all" and row_outcome != outcome_filter:
            continue
        if show_only_tool_turns and not planned_tool_name:
            continue
        if selected_tool_cmp != "all" and planned_tool_name != selected_tool:
            continue

        filtered_rows.append(row)
        validation = _normalize_hydra_validation_for_view(row.get("validation"), planned_tool=planned_tool)
        tool_result = row.get("tool_result") if isinstance(row.get("tool_result"), dict) else {}
        tool_result_summary = str(tool_result.get("summary") or row.get("tool_result_summary") or "").strip()
        ts = float(row.get("timestamp") or 0.0)
        time_text = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else ""

        summary_rows.append(
            {
                "#": len(summary_rows) + 1 if filtered_rows else (idx + 1),
                "time": time_text,
                "platform": str(row.get("platform") or ""),
                "scope": str(row.get("scope") or ""),
                "planner_kind": str(row.get("planner_kind") or ""),
                "astraeus_thanatos_kind": str(row.get("planner_kind") or ""),
                "outcome": str(row.get("outcome") or ""),
                "outcome_reason": str(row.get("outcome_reason") or ""),
                "planned_tool": planned_tool_name,
                "validation_status": str(validation.get("status") or ""),
                "validation_reason": str(validation.get("reason") or ""),
                "checker_action": str(row.get("checker_action") or ""),
                "minos_action": str(row.get("checker_action") or ""),
                "tool_result_ok": tool_result.get("ok") if isinstance(tool_result, dict) else row.get("tool_result_ok"),
                "tool_result_summary": tool_result_summary,
                "total_ms": int(row.get("total_ms") or 0),
                "raw": row,
            }
        )

        if planned_tool_name:
            tool_counts[planned_tool_name] = int(tool_counts.get(planned_tool_name, 0)) + 1

        validation_reason = str(validation.get("reason") or "").strip()
        if validation_reason:
            key = f"validation:{validation_reason}"
            reason_counts[key] = int(reason_counts.get(key, 0)) + 1

        checker_reason = str(row.get("checker_reason") or "").strip()
        if checker_reason:
            key = f"minos:{checker_reason}"
            reason_counts[key] = int(reason_counts.get(key, 0)) + 1

        outcome_reason = str(row.get("outcome_reason") or "").strip()
        if outcome_reason and str(row.get("outcome") or "").strip().lower() == "failed":
            key = f"outcome:{outcome_reason}"
            reason_counts[key] = int(reason_counts.get(key, 0)) + 1

    top_tools = [
        {"label": name, "value": int(count)}
        for name, count in sorted(tool_counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[:12]
    ]
    top_reasons = [
        {"label": name, "value": int(count)}
        for name, count in sorted(reason_counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[:12]
    ]

    return {
        "selected_platform": selected_platform,
        "selected_platform_label": _hydra_platform_display_label(selected_platform),
        "metric_platform": metric_platform,
        "platform_options": ["all", *_HYDRA_METRIC_PLATFORMS],
        "metric_names": list(_HYDRA_METRIC_NAMES),
        "global_metrics": global_metrics,
        "global_rates": _hydra_rate_rows(global_metrics),
        "platform_metrics": platform_metrics,
        "platform_rates": _hydra_rate_rows(platform_metrics),
        "platform_rows": _load_hydra_platform_metric_rows(),
        "ledger_limit": max_limit,
        "ledger_total": len(ledger_rows),
        "ledger_filtered": len(summary_rows),
        "summary_rows": summary_rows,
        "tool_options": tool_options,
        "selected_tool": selected_tool if selected_tool_cmp != "all" else "all",
        "outcome_filter": outcome_filter,
        "show_only_tool_turns": bool(show_only_tool_turns),
        "top_tools": top_tools,
        "top_reasons": top_reasons,
    }


@app.get("/api/settings/hydra/data")
def get_hydra_data() -> Dict[str, Any]:
    metric_keys: List[str] = []
    try:
        metric_keys = [str(k) for k in redis_client.scan_iter(match="tater:hydra:metrics:*")]
    except Exception:
        metric_keys = []

    ledger_rows: List[Dict[str, Any]] = []
    ledger_entries_total = 0
    for key in _hydra_ledger_keys_for_platform("all"):
        count = 0
        try:
            count = int(redis_client.llen(key) or 0)
        except Exception:
            count = 0
        ledger_entries_total += max(0, int(count))
        suffix = str(key).split("tater:hydra:ledger:", 1)[-1]
        platform = normalize_platform(suffix or "webui")
        ledger_rows.append(
            {
                "platform": platform,
                "platform_label": _hydra_platform_display_label(platform),
                "ledger_key": key,
                "entries": int(max(0, count)),
            }
        )
    ledger_rows.sort(key=lambda row: (-int(row.get("entries") or 0), str(row.get("platform") or "")))

    platform_rows: List[Dict[str, Any]] = []
    for platform in _HYDRA_METRIC_PLATFORMS:
        _, _, platform_metrics = _load_hydra_metrics(platform)
        platform_row: Dict[str, Any] = {
            "platform": platform,
            "platform_label": _hydra_platform_display_label(platform),
        }
        for name in _HYDRA_METRIC_NAMES:
            platform_row[name] = int(platform_metrics.get(name) or 0)
        try:
            platform_row["ledger_entries"] = int(redis_client.llen(f"tater:hydra:ledger:{platform}") or 0)
        except Exception:
            platform_row["ledger_entries"] = 0
        platform_rows.append(platform_row)

    turns_chart = [
        {"label": str(row.get("platform_label") or row.get("platform") or ""), "value": int(row.get("total_turns") or 0)}
        for row in platform_rows
    ]
    turns_chart = [row for row in turns_chart if int(row.get("value") or 0) > 0]
    turns_chart.sort(key=lambda row: (-int(row.get("value") or 0), str(row.get("label") or "")))

    ledger_chart = [
        {"label": str(row.get("platform_label") or row.get("platform") or ""), "value": int(row.get("entries") or 0)}
        for row in ledger_rows
    ]
    ledger_chart = [row for row in ledger_chart if int(row.get("value") or 0) > 0]
    ledger_chart.sort(key=lambda row: (-int(row.get("value") or 0), str(row.get("label") or "")))

    return {
        "platform_options": list(_HYDRA_METRIC_PLATFORMS),
        "summary": {
            "metric_keys": int(len(metric_keys)),
            "ledger_lists": int(len(ledger_rows)),
            "ledger_entries_total": int(ledger_entries_total),
        },
        "platform_rows": platform_rows,
        "ledger_rows": ledger_rows,
        "turns_chart": turns_chart,
        "ledger_chart": ledger_chart,
    }


@app.post("/api/settings/hydra/data/clear")
def clear_hydra_data(payload: HydraDataClearRequest) -> Dict[str, Any]:
    mode = str(payload.mode or "all").strip().lower() or "all"
    platform = str(payload.platform or "all").strip().lower() or "all"
    if mode not in {"all", "metrics", "ledger"}:
        raise HTTPException(status_code=400, detail="mode must be one of: all, metrics, ledger")
    if platform != "all":
        platform = normalize_platform(platform)

    metrics_removed = 0
    ledger_removed = 0
    if mode in {"all", "metrics"}:
        metrics_removed = _reset_hydra_metrics(platform)
    if mode in {"all", "ledger"}:
        ledger_removed = _clear_hydra_ledger(platform)

    return {
        "ok": True,
        "mode": mode,
        "platform": platform,
        "metrics_removed": int(metrics_removed),
        "ledger_removed": int(ledger_removed),
    }


@app.get("/api/settings/people")
def get_people_settings() -> Dict[str, Any]:
    return people_module.panel_payload(redis_client)


@app.post("/api/settings/people/action")
def run_people_settings_action(payload: PeopleActionRequest) -> Dict[str, Any]:
    try:
        return people_module.handle_action(payload.action, payload.payload, redis_client)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc) or "People item not found.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Invalid People action.") from exc
    except Exception as exc:
        logger.exception("[people] action failed action=%s", payload.action)
        raise HTTPException(status_code=500, detail=str(exc) or "People action failed.") from exc


@app.get("/api/settings")
def get_settings() -> Dict[str, Any]:
    chat_settings = redis_client.hgetall("chat_settings") or {}
    homeassistant_module = _integration_module("homeassistant", auto_restore=False)
    hue_module = _integration_module("hue", auto_restore=False)
    aladdin_module = _integration_module("aladdin", auto_restore=False)
    sonos_module = _integration_module("sonos", auto_restore=False)
    unifi_network_module = _integration_module("unifi_network", auto_restore=False)
    unifi_protect_module = _integration_module("unifi_protect", auto_restore=False)
    homeassistant_settings = (
        homeassistant_module.load_homeassistant_config(required=False) if homeassistant_module is not None else {}
    )
    hue_settings = hue_module.read_hue_settings() if hue_module is not None else {}
    aladdin_settings = aladdin_module.read_aladdin_settings() if aladdin_module is not None else {}
    sonos_settings = sonos_module.read_sonos_settings() if sonos_module is not None else {}
    unifi_network_settings = (
        unifi_network_module.read_unifi_network_settings() if unifi_network_module is not None else {}
    )
    unifi_protect_settings = (
        unifi_protect_module.read_unifi_protect_settings() if unifi_protect_module is not None else {}
    )

    vision_settings = get_shared_vision_settings(
        default_api_base="http://127.0.0.1:1234",
        default_model="qwen2.5-vl-7b-instruct",
    )
    speech_settings = get_shared_speech_settings()
    speech_ui = get_speech_ui_payload(speech_settings)
    esphome_fields = _esphome_settings_fields()
    esphome_settings_item = esphome_home_module.settings_item_form() if hasattr(esphome_home_module, "settings_item_form") else {}
    esphome_sections = (
        esphome_settings_item.get("sections")
        if isinstance(esphome_settings_item, dict) and isinstance(esphome_settings_item.get("sections"), list)
        else []
    )
    try:
        voice_model_sections = (
            esphome_home_module.model_settings_sections()
            if hasattr(esphome_home_module, "model_settings_sections")
            else []
        )
        if not isinstance(voice_model_sections, list):
            voice_model_sections = []
    except Exception:
        logger.exception("[settings] failed building voice model settings sections")
        voice_model_sections = []
    esphome_ui = {
        "label": "Voice",
        "description": "Built-in voice satellite services for Tater. Satellites, firmware, live entities, and device logs live here.",
        "fields": esphome_fields,
        "sections": esphome_sections,
        "running": bool(esphome_home_module.is_running()),
        "runtime_tab_label": "Voice",
        "runtime_tab_hint": "Satellites, live entities, rooms, and logs are managed directly in this Voice area.",
    }
    voice_model_ui = {
        "label": "Voice Models",
        "description": "Remote wake word, VAD, and voice model runtime settings.",
        "sections": voice_model_sections,
        "openwakeword_trainer": {
            "trainer_url": esphome_openwakeword_module.trainer_url(),
            "model_root": str(esphome_openwakeword_module.OPENWAKEWORD_MODEL_ROOT),
        },
        "nanowakeword_trainer": {
            "trainer_url": esphome_nanowakeword_module.trainer_url(),
            "model_root": str(esphome_nanowakeword_module.NANOWAKEWORD_MODEL_ROOT),
        },
        "nanowakeword": {
            "model_root": str(esphome_nanowakeword_module.NANOWAKEWORD_MODEL_ROOT),
        },
    }
    announcement_speech_ui = get_announcement_tts_ui_payload(
        backend=speech_settings.get("announcement_tts_backend"),
        model=speech_settings.get("announcement_tts_model"),
        voice=speech_settings.get("announcement_tts_voice"),
        default_backend=str(speech_settings.get("announcement_tts_backend") or speech_settings.get("tts_backend") or "wyoming"),
    )
    emoji_settings = get_core_emoji_settings() or {}

    verba_registry_module.ensure_verbas_loaded()
    registry_snapshot = verba_registry_module.get_verba_registry_snapshot()
    admin_plugin_options = sorted(str(plugin_id or "").strip() for plugin_id in registry_snapshot.keys() if str(plugin_id or "").strip())
    admin_only_plugins = sorted(get_admin_only_plugins(redis_client))

    hydra_base_servers_raw = resolve_hydra_base_servers(redis_conn=redis_client, include_legacy=True)
    hydra_base_servers: List[Dict[str, str]] = []
    for row in hydra_base_servers_raw:
        if not isinstance(row, dict):
            continue
        provider = _normalize_hydra_llm_provider(row.get("provider"))
        hydra_base_servers.append(
            {
                "provider": provider,
                "host": str(row.get("host") or "").strip(),
                "port": str(row.get("port") or "").strip(),
                "model": str(row.get("model") or "").strip(),
                "api_key": "" if provider == HYDRA_LLM_PROVIDER_SPUD_LINK else str(row.get("api_key") or "").strip(),
            }
        )
    first_hydra_base = hydra_base_servers[0] if hydra_base_servers else {}
    hydra_llm_provider = _normalize_hydra_llm_provider(
        first_hydra_base.get("provider") or redis_client.get(HYDRA_LLM_PROVIDER_KEY)
    )
    hydra_llm_host = str(first_hydra_base.get("host") or redis_client.get(HYDRA_LLM_HOST_KEY) or "").strip()
    hydra_llm_port = str(first_hydra_base.get("port") or redis_client.get(HYDRA_LLM_PORT_KEY) or "").strip()
    hydra_llm_model = str(first_hydra_base.get("model") or redis_client.get(HYDRA_LLM_MODEL_KEY) or "").strip()
    hydra_llm_api_key = str(first_hydra_base.get("api_key") or "").strip()
    try:
        from spudex.settings import get_spudex_settings

        spudex_settings = get_spudex_settings(redis_client)
    except Exception:
        logger.exception("[settings] failed loading spudex model settings")
        spudex_settings = {}
    hydra_beast_mode_enabled = _as_bool_flag(redis_client.get(HYDRA_BEAST_MODE_ENABLED_KEY), default=False)
    hydra_role_model_values: Dict[str, str] = {}
    for role in HYDRA_BEAST_CONFIG_ROLE_IDS:
        role_provider = _normalize_hydra_llm_provider(redis_client.get(_hydra_role_llm_key(role, "provider")))
        role_host = str(redis_client.get(_hydra_role_llm_key(role, "host")) or "").strip()
        role_port = str(redis_client.get(_hydra_role_llm_key(role, "port")) or "").strip()
        role_model = str(redis_client.get(_hydra_role_llm_key(role, "model")) or "").strip()
        role_api_key = str(redis_client.get(_hydra_role_llm_key(role, "api_key")) or "").strip()
        hydra_role_model_values[f"hydra_llm_{role}_provider"] = role_provider
        hydra_role_model_values[f"hydra_llm_{role}_host"] = role_host
        hydra_role_model_values[f"hydra_llm_{role}_port"] = role_port
        hydra_role_model_values[f"hydra_llm_{role}_model"] = role_model
        hydra_role_model_values[f"hydra_llm_{role}_api_key"] = role_api_key

    hydra_defaults = {
        "hydra_llm_provider": HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE,
        "hydra_llm_host": "",
        "hydra_llm_port": "",
        "hydra_llm_model": "",
        "hydra_llm_api_key": "",
        "hydra_hf_transformers_context_tokens": str(DEFAULT_HF_TRANSFORMERS_CONTEXT_TOKENS),
        "hydra_hf_transformers_device": DEFAULT_HF_TRANSFORMERS_DEVICE,
        "hydra_hf_transformers_dtype": DEFAULT_HF_TRANSFORMERS_DTYPE,
        "hydra_hf_transformers_device_map": DEFAULT_HF_TRANSFORMERS_DEVICE_MAP,
        "hydra_hf_transformers_attn_implementation": DEFAULT_HF_TRANSFORMERS_ATTN_IMPLEMENTATION,
        "hydra_hf_transformers_trust_remote_code": bool(DEFAULT_HF_TRANSFORMERS_TRUST_REMOTE_CODE),
        "hydra_llama_cpp_context_tokens": str(DEFAULT_LLAMA_CPP_CONTEXT_TOKENS),
        "hydra_llama_cpp_vision_context_tokens": str(DEFAULT_LLAMA_CPP_VISION_CONTEXT_TOKENS),
        "hydra_llama_cpp_mtp_enabled": bool(DEFAULT_LLAMA_CPP_MTP_ENABLED),
        "hydra_llama_cpp_mtp_draft_tokens": str(DEFAULT_LLAMA_CPP_MTP_DRAFT_TOKENS),
        "hydra_llama_cpp_mtp_draft_model": DEFAULT_LLAMA_CPP_MTP_DRAFT_MODEL,
        "hydra_llama_cpp_n_batch": str(DEFAULT_LLAMA_CPP_N_BATCH),
        "hydra_llama_cpp_n_ubatch": "",
        "hydra_llama_cpp_flash_attn": bool(DEFAULT_LLAMA_CPP_FLASH_ATTN),
        "hydra_llama_cpp_offload_kqv": bool(DEFAULT_LLAMA_CPP_OFFLOAD_KQV),
        "hydra_mlx_lm_context_tokens": "",
        "hydra_mlx_lm_trust_remote_code": bool(DEFAULT_MLX_LM_TRUST_REMOTE_CODE),
        "hydra_mlx_lm_lazy_load": bool(DEFAULT_MLX_LM_LAZY_LOAD),
        "hydra_mlx_engine_prefill_step_size": "",
        "hydra_mlx_engine_kv_bits": "",
        "hydra_mlx_engine_kv_group_size": "",
        "hydra_mlx_engine_quantized_kv_start": "",
        "hydra_beast_mode_enabled": False,
        "hydra_max_ledger_items": int(DEFAULT_MAX_LEDGER_ITEMS),
        "hydra_astraeus_plan_review_enabled": bool(DEFAULT_ASTRAEUS_PLAN_REVIEW_ENABLED),
        "hydra_auto_continue_incomplete_final_enabled": bool(DEFAULT_AUTO_CONTINUE_INCOMPLETE_FINAL_ENABLED),
    }
    integration_shop_snapshot = _integration_shop_raw()
    integration_shop_payload = {
        "repos": integration_shop_snapshot.get("repos") or {},
        "errors": integration_shop_snapshot.get("errors") or [],
        "installed": integration_shop_snapshot.get("installed") or [],
        "catalog": integration_shop_snapshot.get("catalog") or [],
        "updates_available": integration_shop_snapshot.get("updates_available") or 0,
    }
    tater_api_settings = _load_tater_api_settings(include_secret=False)
    spud_link_settings = _spud_link_public_settings_payload()

    return {
        "username": chat_settings.get("username", "User"),
        "user_avatar": _read_user_avatar_data_url(chat_settings),
        "tater_avatar": _read_tater_avatar_data_url(),
        "max_display": _read_positive_int("tater:max_display", DEFAULT_MAX_DISPLAY),
        "show_speed_stats": _show_speed_stats_enabled(default=False),
        "tater_first_name": redis_client.get("tater:first_name") or "Tater",
        "tater_last_name": redis_client.get("tater:last_name") or "Totterson",
        "tater_personality": redis_client.get("tater:personality") or "",
        "max_store": _read_non_negative_int("tater:max_store", DEFAULT_MAX_STORE),
        "max_llm": _read_positive_int("tater:max_llm", DEFAULT_MAX_LLM),
        "popup_effect_style": _normalize_popup_effect_style(
            redis_client.get(WEBUI_POPUP_EFFECT_STYLE_KEY),
            default=DEFAULT_WEBUI_POPUP_EFFECT_STYLE,
        ),
        "homeassistant_base_url": homeassistant_settings.get("base") or HOMEASSISTANT_DEFAULT_BASE_URL,
        "homeassistant_token": homeassistant_settings.get("token", ""),
        "hue_bridge_host": hue_settings.get("HUE_BRIDGE_HOST", HUE_DEFAULT_BRIDGE_HOST),
        "hue_app_key": hue_settings.get("HUE_APP_KEY", ""),
        "hue_device_type": hue_settings.get("HUE_DEVICE_TYPE", HUE_DEFAULT_DEVICE_TYPE),
        "hue_timeout_seconds": int(hue_settings.get("HUE_TIMEOUT_SECONDS") or HUE_DEFAULT_TIMEOUT_SECONDS),
        "aladdin_username": aladdin_settings.get("ALADDIN_USERNAME", ""),
        "aladdin_password": aladdin_settings.get("ALADDIN_PASSWORD", ""),
        "aladdin_timeout_seconds": int(
            aladdin_settings.get("ALADDIN_TIMEOUT_SECONDS") or ALADDIN_DEFAULT_TIMEOUT_SECONDS
        ),
        "sonos_enabled": _as_bool_flag(sonos_settings.get("SONOS_ENABLED"), default=SONOS_DEFAULT_ENABLED),
        "sonos_discovery_timeout_seconds": int(
            sonos_settings.get("SONOS_DISCOVERY_TIMEOUT_SECONDS") or SONOS_DEFAULT_DISCOVERY_TIMEOUT_SECONDS
        ),
        "sonos_speaker_hosts": str(sonos_settings.get("SONOS_SPEAKER_HOSTS") or ""),
        "unifi_network_base_url": unifi_network_settings.get("UNIFI_BASE_URL") or UNIFI_NETWORK_DEFAULT_BASE_URL,
        "unifi_network_api_key": unifi_network_settings.get("UNIFI_API_KEY", ""),
        "unifi_protect_base_url": unifi_protect_settings.get("base") or UNIFI_PROTECT_DEFAULT_BASE_URL,
        "unifi_protect_api_key": unifi_protect_settings.get("api_key", ""),
        "integrations": get_integration_catalog(),
        "integration_shop": integration_shop_payload,
        "integration_runtime": integration_runtime_status(redis_client),
        "hf_browser_integration": _hf_browser_integration_status(),
        "people": people_module.panel_payload(redis_client),
        "vision_mode": str(vision_settings.get("mode") or "api"),
        "vision_provider": _normalize_hydra_llm_provider(vision_settings.get("provider") or HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE),
        "vision_api_base": str(vision_settings.get("api_base") or "http://127.0.0.1:1234"),
        "vision_model": str(vision_settings.get("model") or "qwen2.5-vl-7b-instruct"),
        "vision_api_key": str(vision_settings.get("api_key") or ""),
        "speech_stt_backend": str(speech_settings.get("stt_backend") or ""),
        "speech_acceleration": str(speech_settings.get("acceleration") or ""),
        "speech_wyoming_stt_host": str(speech_settings.get("wyoming_stt_host") or ""),
        "speech_wyoming_stt_port": str(speech_settings.get("wyoming_stt_port") or ""),
        "speech_tts_backend": str(speech_settings.get("tts_backend") or ""),
        "speech_tts_model": str(speech_settings.get("tts_model") or ""),
        "speech_tts_voice": str(speech_settings.get("tts_voice") or ""),
        "speech_kokoro_output_gain": str(speech_settings.get("kokoro_output_gain") or ""),
        "speech_pocket_tts_output_gain": str(speech_settings.get("pocket_tts_output_gain") or ""),
        "speech_wyoming_tts_host": str(speech_settings.get("wyoming_tts_host") or ""),
        "speech_wyoming_tts_port": str(speech_settings.get("wyoming_tts_port") or ""),
        "speech_wyoming_tts_voice": str(speech_settings.get("wyoming_tts_voice") or ""),
        "speech_openai_tts_base_url": str(speech_settings.get("openai_tts_base_url") or ""),
        "speech_openai_tts_api_key": str(speech_settings.get("openai_tts_api_key") or ""),
        "speech_announcement_tts_backend": str(speech_settings.get("announcement_tts_backend") or ""),
        "speech_announcement_tts_model": str(speech_settings.get("announcement_tts_model") or ""),
        "speech_announcement_tts_voice": str(speech_settings.get("announcement_tts_voice") or ""),
        "speech_announcement_wyoming_tts_host": str(speech_settings.get("announcement_wyoming_tts_host") or ""),
        "speech_announcement_wyoming_tts_port": str(speech_settings.get("announcement_wyoming_tts_port") or ""),
        "speech_announcement_wyoming_tts_voice": str(speech_settings.get("announcement_wyoming_tts_voice") or ""),
        "speech_announcement_openai_tts_base_url": str(speech_settings.get("announcement_openai_tts_base_url") or ""),
        "speech_announcement_openai_tts_api_key": str(speech_settings.get("announcement_openai_tts_api_key") or ""),
        "speech_model_warmup": _speech_model_warmup_snapshot(),
        "hf_llm_warmup": _hf_llm_warmup_snapshot(),
        "hydra_llm_recovery_notice": str(redis_client.get(HYDRA_LLM_RECOVERY_NOTICE_KEY) or ""),
        "local_llm_models": _local_llm_models_payload(),
        "speech_ui": speech_ui,
        "announcement_speech_ui": announcement_speech_ui,
        "esphome_ui": esphome_ui,
        "voice_model_ui": voice_model_ui,
        "emoji_enable_on_reaction_add": bool(emoji_settings.get("enable_on_reaction_add", True)),
        "emoji_enable_auto_reaction_on_reply": bool(emoji_settings.get("enable_auto_reaction_on_reply", True)),
        "emoji_reaction_chain_chance_percent": int(emoji_settings.get("reaction_chain_chance_percent", 100)),
        "emoji_reply_reaction_chance_percent": int(emoji_settings.get("reply_reaction_chance_percent", 12)),
        "emoji_reaction_chain_cooldown_seconds": int(emoji_settings.get("reaction_chain_cooldown_seconds", 30)),
        "emoji_reply_reaction_cooldown_seconds": int(emoji_settings.get("reply_reaction_cooldown_seconds", 120)),
        "emoji_min_message_length": int(emoji_settings.get("min_message_length", 4)),
        "hydra_llm_provider": hydra_llm_provider,
        "hydra_llm_host": hydra_llm_host,
        "hydra_llm_port": hydra_llm_port,
        "hydra_llm_model": hydra_llm_model,
        "hydra_llm_api_key": hydra_llm_api_key,
        "hydra_hf_transformers_context_tokens": _read_local_llm_context_setting(
            HYDRA_HF_TRANSFORMERS_CONTEXT_TOKENS_KEY,
            ("TATER_HF_TRANSFORMERS_MAX_INPUT_TOKENS",),
            DEFAULT_HF_TRANSFORMERS_CONTEXT_TOKENS,
        ),
        "hydra_hf_transformers_device": _read_text_choice_setting(
            HYDRA_HF_TRANSFORMERS_DEVICE_KEY,
            ("TATER_HF_TRANSFORMERS_DEVICE",),
            DEFAULT_HF_TRANSFORMERS_DEVICE,
            allowed=("auto", "cuda", "mps", "cpu"),
        ),
        "hydra_hf_transformers_dtype": _read_text_choice_setting(
            HYDRA_HF_TRANSFORMERS_DTYPE_KEY,
            ("TATER_HF_TRANSFORMERS_DTYPE",),
            DEFAULT_HF_TRANSFORMERS_DTYPE,
            allowed=("auto", "float16", "bfloat16", "float32"),
        ),
        "hydra_hf_transformers_device_map": _read_text_choice_setting(
            HYDRA_HF_TRANSFORMERS_DEVICE_MAP_KEY,
            ("TATER_HF_TRANSFORMERS_DEVICE_MAP",),
            DEFAULT_HF_TRANSFORMERS_DEVICE_MAP,
            allowed=("default", "disabled", "auto", "balanced"),
        ),
        "hydra_hf_transformers_attn_implementation": _read_text_choice_setting(
            HYDRA_HF_TRANSFORMERS_ATTN_IMPLEMENTATION_KEY,
            ("TATER_HF_TRANSFORMERS_ATTN_IMPLEMENTATION",),
            DEFAULT_HF_TRANSFORMERS_ATTN_IMPLEMENTATION,
            allowed=("", "auto", "sdpa", "flash_attention_2", "eager"),
        ),
        "hydra_hf_transformers_trust_remote_code": _read_bool_setting(
            HYDRA_HF_TRANSFORMERS_TRUST_REMOTE_CODE_KEY,
            ("TATER_HF_TRANSFORMERS_TRUST_REMOTE_CODE",),
            DEFAULT_HF_TRANSFORMERS_TRUST_REMOTE_CODE,
        ),
        "hydra_llama_cpp_context_tokens": _read_local_llm_context_setting(
            HYDRA_LLAMA_CPP_CONTEXT_TOKENS_KEY,
            ("TATER_LLAMA_CPP_N_CTX", "LLM_CONTEXT_SIZE"),
            DEFAULT_LLAMA_CPP_CONTEXT_TOKENS,
        ),
        "hydra_llama_cpp_vision_context_tokens": _read_local_llm_context_setting(
            HYDRA_LLAMA_CPP_VISION_CONTEXT_TOKENS_KEY,
            ("TATER_LLAMA_CPP_VISION_N_CTX", "TATER_LLAMA_CPP_VISION_CONTEXT_TOKENS"),
            DEFAULT_LLAMA_CPP_VISION_CONTEXT_TOKENS,
        ),
        "hydra_llama_cpp_mtp_enabled": _read_bool_setting(
            HYDRA_LLAMA_CPP_MTP_ENABLED_KEY,
            ("TATER_LLAMA_CPP_MTP_ENABLED",),
            DEFAULT_LLAMA_CPP_MTP_ENABLED,
        ),
        "hydra_llama_cpp_mtp_draft_tokens": _read_bounded_int_setting(
            HYDRA_LLAMA_CPP_MTP_DRAFT_TOKENS_KEY,
            ("TATER_LLAMA_CPP_MTP_DRAFT_TOKENS",),
            DEFAULT_LLAMA_CPP_MTP_DRAFT_TOKENS,
            minimum=1,
            maximum=16,
        ),
        "hydra_llama_cpp_mtp_draft_model": _read_text_setting(
            HYDRA_LLAMA_CPP_MTP_DRAFT_MODEL_KEY,
            ("TATER_LLAMA_CPP_MTP_DRAFT_MODEL",),
            DEFAULT_LLAMA_CPP_MTP_DRAFT_MODEL,
        ),
        "hydra_llama_cpp_n_batch": _read_bounded_int_setting(
            HYDRA_LLAMA_CPP_N_BATCH_KEY,
            ("TATER_LLAMA_CPP_N_BATCH",),
            DEFAULT_LLAMA_CPP_N_BATCH,
            minimum=32,
            maximum=8192,
        ),
        "hydra_llama_cpp_n_ubatch": _read_bounded_int_setting(
            HYDRA_LLAMA_CPP_N_UBATCH_KEY,
            ("TATER_LLAMA_CPP_N_UBATCH",),
            DEFAULT_LLAMA_CPP_N_UBATCH,
            minimum=0,
            maximum=8192,
        ),
        "hydra_llama_cpp_flash_attn": _read_bool_setting(
            HYDRA_LLAMA_CPP_FLASH_ATTN_KEY,
            ("TATER_LLAMA_CPP_FLASH_ATTN",),
            DEFAULT_LLAMA_CPP_FLASH_ATTN,
        ),
        "hydra_llama_cpp_offload_kqv": _read_bool_setting(
            HYDRA_LLAMA_CPP_OFFLOAD_KQV_KEY,
            ("TATER_LLAMA_CPP_OFFLOAD_KQV",),
            DEFAULT_LLAMA_CPP_OFFLOAD_KQV,
        ),
        "hydra_mlx_lm_context_tokens": _read_local_llm_context_setting(
            HYDRA_MLX_LM_CONTEXT_TOKENS_KEY,
            ("TATER_MLX_LM_MAX_KV_SIZE",),
            None,
            minimum=128,
        ),
        "hydra_mlx_lm_trust_remote_code": _read_bool_setting(
            HYDRA_MLX_LM_TRUST_REMOTE_CODE_KEY,
            ("TATER_MLX_LM_TRUST_REMOTE_CODE",),
            DEFAULT_MLX_LM_TRUST_REMOTE_CODE,
        ),
        "hydra_mlx_lm_lazy_load": _read_bool_setting(
            HYDRA_MLX_LM_LAZY_LOAD_KEY,
            ("TATER_MLX_LM_LAZY",),
            DEFAULT_MLX_LM_LAZY_LOAD,
        ),
        "hydra_mlx_engine_prefill_step_size": _read_optional_int_setting(
            HYDRA_MLX_ENGINE_PREFILL_STEP_SIZE_KEY,
            ("TATER_MLX_ENGINE_PREFILL_STEP_SIZE",),
            minimum=1,
            maximum=32768,
        ),
        "hydra_mlx_engine_kv_bits": _read_text_choice_setting(
            HYDRA_MLX_ENGINE_KV_BITS_KEY,
            ("TATER_MLX_ENGINE_KV_BITS",),
            "",
            allowed=("", "2", "3", "4", "6", "8"),
        ),
        "hydra_mlx_engine_kv_group_size": _read_text_choice_setting(
            HYDRA_MLX_ENGINE_KV_GROUP_SIZE_KEY,
            ("TATER_MLX_ENGINE_KV_GROUP_SIZE",),
            "",
            allowed=("", "32", "64", "128"),
        ),
        "hydra_mlx_engine_quantized_kv_start": _read_optional_int_setting(
            HYDRA_MLX_ENGINE_QUANTIZED_KV_START_KEY,
            ("TATER_MLX_ENGINE_QUANTIZED_KV_START",),
            minimum=0,
            maximum=1_048_576,
            allow_zero=True,
        ),
        "spudex_llm_provider": _normalize_hydra_llm_provider(spudex_settings.get("llm_provider") or ""),
        "spudex_llm_host": str(spudex_settings.get("llm_host") or ""),
        "spudex_llm_model": str(spudex_settings.get("llm_model") or ""),
        "hydra_base_servers": hydra_base_servers,
        "hydra_beast_mode_enabled": hydra_beast_mode_enabled,
        "hydra_max_ledger_items": _read_positive_int(HYDRA_MAX_LEDGER_ITEMS_KEY, DEFAULT_MAX_LEDGER_ITEMS),
        "hydra_astraeus_plan_review_enabled": _as_bool_flag(
            redis_client.get(HYDRA_ASTRAEUS_PLAN_REVIEW_ENABLED_KEY),
            default=DEFAULT_ASTRAEUS_PLAN_REVIEW_ENABLED,
        ),
        "hydra_auto_continue_incomplete_final_enabled": _as_bool_flag(
            redis_client.get(HYDRA_AUTO_CONTINUE_INCOMPLETE_FINAL_ENABLED_KEY),
            default=DEFAULT_AUTO_CONTINUE_INCOMPLETE_FINAL_ENABLED,
        ),
        **hydra_role_model_values,
        "hydra_defaults": hydra_defaults,
        "admin_plugin_options": admin_plugin_options,
        "admin_only_plugins": admin_only_plugins,
        "admin_only_plugins_defaults": sorted(DEFAULT_ADMIN_ONLY_PLUGINS),
        "tater_api_enabled": bool(tater_api_settings.get("enabled")),
        "tater_api_key_set": bool(tater_api_settings.get("api_key_set")),
        "tater_api_mode": _normalize_tater_api_mode(tater_api_settings.get("mode")),
        "tater_api_hydra_tools_enabled": bool(tater_api_settings.get("hydra_tools_enabled")),
        "spud_link": spud_link_settings,
        "spud_link_mode": _normalize_spud_link_mode(spud_link_settings.get("mode")),
        "spud_link_node_name": str(spud_link_settings.get("node_name") or ""),
        "spud_link_public_url": str(spud_link_settings.get("public_url") or ""),
        "spud_link_pairing_enabled": bool(spud_link_settings.get("pairing_enabled")),
        "spud_link_allow_spudlets": bool(spud_link_settings.get("allow_spudlets")),
        "spud_link_allow_little_spuds": bool(spud_link_settings.get("allow_little_spuds")),
        "spud_link_little_spud_tools_enabled": bool(spud_link_settings.get("little_spud_tools_enabled")),
        "spud_link_telemetry_enabled": bool(spud_link_settings.get("telemetry_enabled")),
        "spud_link_request_previews_enabled": bool(spud_link_settings.get("request_previews_enabled")),
        "spud_link_hub_url": str(spud_link_settings.get("hub_url") or ""),
        "spud_link_node_token_set": bool(spud_link_settings.get("node_token_set")),
        "webui_password_set": _webui_password_is_set(),
    }


@app.post("/api/settings/speech/tts-preview")
async def preview_speech_tts(payload: SpeechTtsPreviewRequest) -> Response:
    current_speech = get_shared_speech_settings()
    preview_text = str(payload.text or "").strip() or "Hello from Tater. This is a voice preview."
    try:
        wav_bytes = await synthesize_preview_wav(
            text=preview_text,
            backend=str(payload.backend or current_speech.get("tts_backend") or "").strip(),
            model=str(payload.model or current_speech.get("tts_model") or "").strip(),
            voice=str(payload.voice or current_speech.get("tts_voice") or "").strip(),
            kokoro_output_gain=(
                payload.kokoro_output_gain
                if payload.kokoro_output_gain is not None
                else current_speech.get("kokoro_output_gain")
            ),
            pocket_tts_output_gain=(
                payload.pocket_tts_output_gain
                if payload.pocket_tts_output_gain is not None
                else current_speech.get("pocket_tts_output_gain")
            ),
            acceleration=str(payload.acceleration or current_speech.get("acceleration") or "").strip(),
            wyoming_host=str(payload.wyoming_host or current_speech.get("wyoming_tts_host") or "").strip(),
            wyoming_port=str(payload.wyoming_port or current_speech.get("wyoming_tts_port") or "").strip(),
            wyoming_voice=str(payload.wyoming_voice or current_speech.get("wyoming_tts_voice") or "").strip(),
            openai_base_url=(
                str(payload.openai_base_url).strip()
                if payload.openai_base_url is not None
                else str(current_speech.get("openai_tts_base_url") or "").strip()
            ),
            openai_api_key=(
                str(payload.openai_api_key).strip()
                if payload.openai_api_key is not None
                else str(current_speech.get("openai_tts_api_key") or "").strip()
            ),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "TTS preview failed.") from exc

    if not wav_bytes:
        raise HTTPException(status_code=400, detail="TTS preview produced no audio.")
    return Response(content=wav_bytes, media_type="audio/wav")


@app.post("/api/settings/openwakeword/trainer-models")
def list_openwakeword_trainer_models(payload: OpenWakeWordTrainerModelsRequest) -> Dict[str, Any]:
    try:
        return esphome_openwakeword_module.trainer_model_catalog(
            trainer_url_value=payload.trainer_url or "",
            framework=payload.framework or "",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc) or "Failed to load openWakeWord trainer models.",
        ) from exc


@app.post("/api/settings/openwakeword/download-trainer-model")
def download_openwakeword_trainer_model(payload: OpenWakeWordTrainerDownloadRequest) -> Dict[str, Any]:
    try:
        return esphome_openwakeword_module.download_trainer_model(
            trainer_url_value=payload.trainer_url or "",
            artifact_url=payload.artifact_url or "",
            framework=payload.framework or "",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc) or "Failed to download openWakeWord trainer model.",
        ) from exc


@app.post("/api/settings/nanowakeword/trainer-models")
def list_nanowakeword_trainer_models(payload: NanoWakeWordTrainerModelsRequest) -> Dict[str, Any]:
    try:
        return esphome_nanowakeword_module.trainer_model_catalog(
            trainer_url_value=payload.trainer_url or "",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc) or "Failed to load NanoWakeWord trainer models.",
        ) from exc


@app.post("/api/settings/nanowakeword/download-trainer-model")
def download_nanowakeword_trainer_model(payload: NanoWakeWordTrainerDownloadRequest) -> Dict[str, Any]:
    try:
        return esphome_nanowakeword_module.download_trainer_model(
            trainer_url_value=payload.trainer_url or "",
            artifact_url=payload.artifact_url or "",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc) or "Failed to download NanoWakeWord trainer model.",
        ) from exc


def _openwakeword_query_int(
    websocket: WebSocket,
    param_name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw = str(websocket.query_params.get(param_name) or "").strip()
    try:
        parsed = int(raw) if raw else int(default)
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def _openwakeword_detector_selector(selector: str, client_host: str) -> str:
    selector_token = str(selector or "").strip() or "remote"
    client_token = str(client_host or "").strip()
    if client_token and client_token != selector_token:
        return f"{selector_token}@{client_token}"
    return selector_token


def _record_openwakeword_detect_request(
    *,
    selector: str,
    status: str,
    elapsed_s: float,
    audio_bytes_len: int,
    detected: bool = False,
    detail: str = "",
    force_log: bool = False,
) -> None:
    selector_key = str(selector or "remote").strip() or "remote"
    now_ts = time.time()
    with openwakeword_detect_stats_lock:
        row = openwakeword_detect_stats.setdefault(
            selector_key,
            {
                "count": 0,
                "errors": 0,
                "detected": 0,
                "slow": 0,
                "last_log_ts": 0.0,
            },
        )
        row["count"] = int(row.get("count") or 0) + 1
        if status != "ok":
            row["errors"] = int(row.get("errors") or 0) + 1
        if detected:
            row["detected"] = int(row.get("detected") or 0) + 1
        if elapsed_s >= OPENWAKEWORD_DETECT_SLOW_LOG_S:
            row["slow"] = int(row.get("slow") or 0) + 1
        count = int(row.get("count") or 0)
        errors = int(row.get("errors") or 0)
        detections = int(row.get("detected") or 0)
        slow = int(row.get("slow") or 0)
        last_log_ts = float(row.get("last_log_ts") or 0.0)
        should_log = (
            status != "ok"
            or detected
            or force_log
            or elapsed_s >= OPENWAKEWORD_DETECT_SLOW_LOG_S
            or count == 1
            or count % OPENWAKEWORD_DETECT_LOG_EVERY == 0
            or (now_ts - last_log_ts) >= 300.0
        )
        if should_log:
            row["last_log_ts"] = now_ts

    if not should_log:
        return
    logger.info(
        "[openwakeword] detect selector=%s status=%s detected=%s elapsed_ms=%.1f bytes=%s count=%s errors=%s slow=%s detections=%s%s",
        selector_key,
        status,
        bool(detected),
        elapsed_s * 1000.0,
        int(audio_bytes_len or 0),
        count,
        errors,
        slow,
        detections,
        f" detail={detail}" if detail else "",
    )


def _record_nanowakeword_detect_request(
    *,
    selector: str,
    status: str,
    elapsed_s: float,
    audio_bytes_len: int,
    detected: bool = False,
    detail: str = "",
    force_log: bool = False,
) -> None:
    selector_key = str(selector or "remote").strip() or "remote"
    now_ts = time.time()
    with nanowakeword_detect_stats_lock:
        row = nanowakeword_detect_stats.setdefault(
            selector_key,
            {
                "count": 0,
                "errors": 0,
                "detected": 0,
                "slow": 0,
                "last_log_ts": 0.0,
            },
        )
        row["count"] = int(row.get("count") or 0) + 1
        if status != "ok":
            row["errors"] = int(row.get("errors") or 0) + 1
        if detected:
            row["detected"] = int(row.get("detected") or 0) + 1
        if elapsed_s >= OPENWAKEWORD_DETECT_SLOW_LOG_S:
            row["slow"] = int(row.get("slow") or 0) + 1
        count = int(row.get("count") or 0)
        errors = int(row.get("errors") or 0)
        detections = int(row.get("detected") or 0)
        slow = int(row.get("slow") or 0)
        last_log_ts = float(row.get("last_log_ts") or 0.0)
        should_log = (
            status != "ok"
            or detected
            or force_log
            or elapsed_s >= OPENWAKEWORD_DETECT_SLOW_LOG_S
            or count == 1
            or count % OPENWAKEWORD_DETECT_LOG_EVERY == 0
            or (now_ts - last_log_ts) >= 300.0
        )
        if should_log:
            row["last_log_ts"] = now_ts

    if not should_log:
        return
    logger.info(
        "[nanowakeword] detect selector=%s status=%s detected=%s elapsed_ms=%.1f bytes=%s count=%s errors=%s slow=%s detections=%s%s",
        selector_key,
        status,
        bool(detected),
        elapsed_s * 1000.0,
        int(audio_bytes_len or 0),
        count,
        errors,
        slow,
        detections,
        f" detail={detail}" if detail else "",
    )


@app.websocket("/api/openwakeword/stream")
async def stream_openwakeword(websocket: WebSocket) -> None:
    client_host = getattr(websocket.client, "host", "") if websocket.client is not None else ""
    selector = str(
        websocket.query_params.get("selector")
        or websocket.query_params.get("source_device")
        or client_host
        or "remote"
    ).strip()
    detector_selector = _openwakeword_detector_selector(selector, client_host)
    wake_word_hint = str(websocket.query_params.get("wake_word") or "").strip()
    audio_bits = _openwakeword_query_int(websocket, "bits", 16, minimum=8, maximum=32)
    audio_format = {
        "rate": _openwakeword_query_int(websocket, "rate", 16000, minimum=8000, maximum=48000),
        "width": max(1, audio_bits // 8),
        "channels": _openwakeword_query_int(websocket, "channels", 1, minimum=1, maximum=2),
    }
    stream_settings = esphome_openwakeword_module.settings_snapshot()
    try:
        stream_queue_max = max(1, min(120, int(stream_settings.get("stream_queue_max") or OPENWAKEWORD_STREAM_QUEUE_MAX)))
    except Exception:
        stream_queue_max = OPENWAKEWORD_STREAM_QUEUE_MAX
    drop_queued_frames = bool(stream_settings.get("drop_queued_frames", True))
    await websocket.accept()
    logger.info(
        "[openwakeword] stream-start selector=%s detector=%s client=%s queue_max=%s drop_queued_frames=%s",
        selector,
        detector_selector,
        client_host or "-",
        stream_queue_max,
        drop_queued_frames,
    )
    frame_count = 0
    processed_count = 0
    dropped_count = 0
    audio_queue: asyncio.Queue[Tuple[float, bytes]] = asyncio.Queue(maxsize=stream_queue_max)
    receiver_done = asyncio.Event()
    receiver_task: Optional[asyncio.Task[Any]] = None

    def drop_queued_frame(*, count_drop: bool = True) -> None:
        nonlocal dropped_count
        try:
            audio_queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        if count_drop:
            dropped_count += 1

    async def receive_audio_frames() -> None:
        nonlocal frame_count
        try:
            while True:
                message = await websocket.receive()
                message_type = str(message.get("type") or "")
                if message_type == "websocket.disconnect":
                    break
                audio_bytes = message.get("bytes")
                if audio_bytes is None:
                    continue
                frame_count += 1
                audio_bytes_len = len(audio_bytes or b"")
                if not audio_bytes:
                    continue
                if audio_bytes_len > 512 * 1024:
                    await websocket.send_json({"ok": False, "error": "openWakeWord audio chunk is too large."})
                    await websocket.close(code=1009)
                    break
                if drop_queued_frames:
                    while audio_queue.full():
                        drop_queued_frame()
                    audio_queue.put_nowait((time.time(), bytes(audio_bytes)))
                else:
                    await audio_queue.put((time.time(), bytes(audio_bytes)))
        except WebSocketDisconnect:
            pass
        finally:
            receiver_done.set()

    try:
        if not esphome_openwakeword_module.openwakeword_enabled():
            await websocket.send_json({"ok": False, "error": "openWakeWord is disabled."})
            await websocket.close(code=1013)
            return

        receiver_task = asyncio.create_task(receive_audio_frames())
        while True:
            if receiver_done.is_set() and audio_queue.empty():
                break
            try:
                received_ts, audio_bytes = await asyncio.wait_for(audio_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            started_ts = time.time()
            audio_bytes_len = len(audio_bytes or b"")
            processed_count += 1
            queue_delay_ms = max(0.0, (started_ts - received_ts) * 1000.0)
            detail_prefix = f"transport=ws queue_ms={queue_delay_ms:.1f} dropped={dropped_count}"

            try:
                detection = await run_wake(
                    esphome_openwakeword_module.process_audio,
                    detector_selector,
                    bytes(audio_bytes),
                    audio_format,
                )
            except Exception as exc:
                detail = str(exc) or type(exc).__name__
                _record_openwakeword_detect_request(
                    selector=detector_selector,
                    status="error",
                    elapsed_s=time.time() - started_ts,
                    audio_bytes_len=audio_bytes_len,
                    detail=f"{detail_prefix} error={detail}",
                )
                await websocket.send_json({"ok": False, "error": detail})
                continue

            if not isinstance(detection, dict) or not detection.get("detected"):
                non_detect_detail = detail_prefix
                force_log = False
                if isinstance(detection, dict):
                    diagnostic_logging = bool(detection.get("diagnostic_logging"))
                    if diagnostic_logging:
                        best_label = str(detection.get("best_label") or "")
                        model_source = str(detection.get("model_source") or "")
                        try:
                            score = float(detection.get("score") or 0.0)
                        except Exception:
                            score = 0.0
                        try:
                            threshold = float(detection.get("threshold") or 0.0)
                        except Exception:
                            threshold = 0.0
                        try:
                            hit_count = int(detection.get("hit_count") or 0)
                        except Exception:
                            hit_count = 0
                        try:
                            patience = int(detection.get("patience") or 0)
                        except Exception:
                            patience = 0
                        if best_label or model_source or score > 0.0:
                            non_detect_detail = (
                                f"{detail_prefix} best_label={best_label or '-'} score={score:.3f} "
                                f"hits={hit_count}/{patience} threshold={threshold:.3f} model={model_source or '-'}"
                            )
                        if threshold > 0.0 and score >= max(0.2, threshold - 0.25):
                            force_log = True
                        if hit_count > 0:
                            force_log = True
                _record_openwakeword_detect_request(
                    selector=detector_selector,
                    status="ok",
                    elapsed_s=time.time() - started_ts,
                    audio_bytes_len=audio_bytes_len,
                    detail=non_detect_detail,
                    force_log=force_log,
                )
                continue

            try:
                score = float(detection.get("score") or 0.0)
            except Exception:
                score = 0.0
            wake_word = str(detection.get("wake_word") or wake_word_hint or "openwakeword")
            _record_openwakeword_detect_request(
                selector=detector_selector,
                status="ok",
                detected=True,
                elapsed_s=time.time() - started_ts,
                audio_bytes_len=audio_bytes_len,
                detail=f"{detail_prefix} wake_word={wake_word} score={score:.3f}",
            )
            await websocket.send_json(
                {
                    "ok": True,
                    "detected": True,
                    "wake_word": wake_word,
                    "score": score,
                    "engine": "openwakeword",
                    "model_source": str(detection.get("model_source") or ""),
                }
            )
            while not audio_queue.empty():
                drop_queued_frame(count_drop=False)
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        with contextlib.suppress(Exception):
            await websocket.send_json({"ok": False, "error": str(exc) or type(exc).__name__})
    finally:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            if receiver_task is not None:
                receiver_task.cancel()
                await receiver_task
        logger.info(
            "[openwakeword] stream-stop selector=%s frames=%s processed=%s dropped=%s",
            detector_selector,
            frame_count,
            processed_count,
            dropped_count,
        )


@app.websocket("/api/nanowakeword/stream")
async def stream_nanowakeword(websocket: WebSocket) -> None:
    client_host = getattr(websocket.client, "host", "") if websocket.client is not None else ""
    selector = str(
        websocket.query_params.get("selector")
        or websocket.query_params.get("source_device")
        or client_host
        or "remote"
    ).strip()
    detector_selector = _openwakeword_detector_selector(selector, client_host)
    wake_word_hint = str(websocket.query_params.get("wake_word") or "").strip()
    audio_bits = _openwakeword_query_int(websocket, "bits", 16, minimum=8, maximum=32)
    audio_format = {
        "rate": _openwakeword_query_int(websocket, "rate", 16000, minimum=8000, maximum=48000),
        "width": max(1, audio_bits // 8),
        "channels": _openwakeword_query_int(websocket, "channels", 1, minimum=1, maximum=2),
    }
    stream_settings = esphome_nanowakeword_module.settings_snapshot()
    try:
        stream_queue_max = max(1, min(120, int(stream_settings.get("stream_queue_max") or OPENWAKEWORD_STREAM_QUEUE_MAX)))
    except Exception:
        stream_queue_max = OPENWAKEWORD_STREAM_QUEUE_MAX
    drop_queued_frames = bool(stream_settings.get("drop_queued_frames", True))
    await websocket.accept()
    logger.info(
        "[nanowakeword] stream-start selector=%s detector=%s client=%s queue_max=%s drop_queued_frames=%s",
        selector,
        detector_selector,
        client_host or "-",
        stream_queue_max,
        drop_queued_frames,
    )
    frame_count = 0
    processed_count = 0
    dropped_count = 0
    audio_queue: asyncio.Queue[Tuple[float, bytes]] = asyncio.Queue(maxsize=stream_queue_max)
    receiver_done = asyncio.Event()
    receiver_task: Optional[asyncio.Task[Any]] = None

    def drop_queued_frame(*, count_drop: bool = True) -> None:
        nonlocal dropped_count
        try:
            audio_queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        if count_drop:
            dropped_count += 1

    async def receive_audio_frames() -> None:
        nonlocal frame_count
        try:
            while True:
                message = await websocket.receive()
                message_type = str(message.get("type") or "")
                if message_type == "websocket.disconnect":
                    break
                audio_bytes = message.get("bytes")
                if audio_bytes is None:
                    continue
                frame_count += 1
                audio_bytes_len = len(audio_bytes or b"")
                if not audio_bytes:
                    continue
                if audio_bytes_len > 512 * 1024:
                    await websocket.send_json({"ok": False, "error": "NanoWakeWord audio chunk is too large."})
                    await websocket.close(code=1009)
                    break
                if drop_queued_frames:
                    while audio_queue.full():
                        drop_queued_frame()
                    audio_queue.put_nowait((time.time(), bytes(audio_bytes)))
                else:
                    await audio_queue.put((time.time(), bytes(audio_bytes)))
        except WebSocketDisconnect:
            pass
        finally:
            receiver_done.set()

    try:
        if not esphome_nanowakeword_module.nanowakeword_enabled():
            await websocket.send_json({"ok": False, "error": "NanoWakeWord is disabled or missing a model."})
            await websocket.close(code=1013)
            return

        receiver_task = asyncio.create_task(receive_audio_frames())
        while True:
            if receiver_done.is_set() and audio_queue.empty():
                break
            try:
                received_ts, audio_bytes = await asyncio.wait_for(audio_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            started_ts = time.time()
            audio_bytes_len = len(audio_bytes or b"")
            processed_count += 1
            queue_delay_ms = max(0.0, (started_ts - received_ts) * 1000.0)
            detail_prefix = f"transport=ws queue_ms={queue_delay_ms:.1f} dropped={dropped_count}"

            try:
                detection = await run_wake(
                    esphome_nanowakeword_module.process_audio,
                    detector_selector,
                    bytes(audio_bytes),
                    audio_format,
                )
            except Exception as exc:
                detail = str(exc) or type(exc).__name__
                _record_nanowakeword_detect_request(
                    selector=detector_selector,
                    status="error",
                    elapsed_s=time.time() - started_ts,
                    audio_bytes_len=audio_bytes_len,
                    detail=f"{detail_prefix} error={detail}",
                )
                await websocket.send_json({"ok": False, "error": detail})
                continue

            if not isinstance(detection, dict) or not detection.get("detected"):
                non_detect_detail = detail_prefix
                force_log = False
                if isinstance(detection, dict) and bool(detection.get("diagnostic_logging")):
                    best_label = str(detection.get("best_label") or "")
                    model_source = str(detection.get("model_source") or "")
                    runtime = str(detection.get("runtime") or "")
                    device = str(detection.get("device") or "")
                    try:
                        score = float(detection.get("score") or 0.0)
                    except Exception:
                        score = 0.0
                    try:
                        threshold = float(detection.get("threshold") or 0.0)
                    except Exception:
                        threshold = 0.0
                    try:
                        hit_count = int(detection.get("hit_count") or 0)
                    except Exception:
                        hit_count = 0
                    try:
                        patience = int(detection.get("patience") or 0)
                    except Exception:
                        patience = 0
                    if best_label or model_source or runtime or device or score > 0.0:
                        non_detect_detail = (
                            f"{detail_prefix} best_label={best_label or '-'} score={score:.3f} "
                            f"hits={hit_count}/{patience} threshold={threshold:.3f} model={model_source or '-'} "
                            f"runtime={runtime or '-'} device={device or '-'}"
                        )
                    if threshold > 0.0 and score >= max(0.2, threshold - 0.25):
                        force_log = True
                    if hit_count > 0:
                        force_log = True
                _record_nanowakeword_detect_request(
                    selector=detector_selector,
                    status="ok",
                    elapsed_s=time.time() - started_ts,
                    audio_bytes_len=audio_bytes_len,
                    detail=non_detect_detail,
                    force_log=force_log,
                )
                continue

            try:
                score = float(detection.get("score") or 0.0)
            except Exception:
                score = 0.0
            wake_word = str(wake_word_hint or detection.get("wake_word") or "nanowakeword")
            _record_nanowakeword_detect_request(
                selector=detector_selector,
                status="ok",
                detected=True,
                elapsed_s=time.time() - started_ts,
                audio_bytes_len=audio_bytes_len,
                detail=(
                    f"{detail_prefix} wake_word={wake_word} score={score:.3f} "
                    f"runtime={str(detection.get('runtime') or '-')}"
                    f" device={str(detection.get('device') or '-')}"
                ),
            )
            await websocket.send_json(
                {
                    "ok": True,
                    "detected": True,
                    "wake_word": wake_word,
                    "score": score,
                    "engine": "nanowakeword",
                    "model_source": str(detection.get("model_source") or ""),
                }
            )
            while not audio_queue.empty():
                drop_queued_frame(count_drop=False)
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        with contextlib.suppress(Exception):
            await websocket.send_json({"ok": False, "error": str(exc) or type(exc).__name__})
    finally:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            if receiver_task is not None:
                receiver_task.cancel()
                await receiver_task
        logger.info(
            "[nanowakeword] stream-stop selector=%s frames=%s processed=%s dropped=%s",
            detector_selector,
            frame_count,
            processed_count,
            dropped_count,
        )


@app.get("/api/nanowakeword/status")
def nanowakeword_status() -> Dict[str, Any]:
    return esphome_nanowakeword_module.status()


@app.post("/api/nanowakeword/reset")
def nanowakeword_reset() -> Dict[str, Any]:
    esphome_nanowakeword_module.reset_detectors()
    return esphome_nanowakeword_module.status()


@app.get("/api/speech/tts/runtime/{asset_id}.wav")
async def get_runtime_tts_asset(asset_id: str) -> Response:
    row = get_runtime_tts_wav(asset_id)
    if not isinstance(row, dict):
        raise HTTPException(status_code=404, detail="TTS audio not found or expired.")
    wav_bytes = bytes(row.get("bytes") or b"")
    if not wav_bytes:
        raise HTTPException(status_code=404, detail="TTS audio is empty or expired.")
    return Response(content=wav_bytes, media_type=str(row.get("content_type") or "audio/wav"))


@app.post("/api/settings/speech/wyoming-tts-voices")
async def get_speech_wyoming_tts_voices(payload: WyomingTtsVoicesRequest) -> Dict[str, Any]:
    current_speech = get_shared_speech_settings()
    try:
        return await fetch_wyoming_tts_voice_options(
            host=str(payload.host or current_speech.get("wyoming_tts_host") or "").strip(),
            port=str(payload.port or current_speech.get("wyoming_tts_port") or "").strip(),
            current_value=str(payload.current_voice or current_speech.get("wyoming_tts_voice") or "").strip(),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Failed to fetch Wyoming TTS voices.") from exc


@app.post("/api/settings/speech/openai-compatible-tts-voices")
async def get_speech_openai_compatible_tts_voices(payload: OpenAiCompatibleTtsVoicesRequest) -> Dict[str, Any]:
    current_speech = get_shared_speech_settings()
    try:
        return await fetch_openai_compatible_tts_voice_options(
            base_url=(
                str(payload.base_url).strip()
                if payload.base_url is not None
                else str(current_speech.get("openai_tts_base_url") or "").strip()
            ),
            api_key=(
                str(payload.api_key).strip()
                if payload.api_key is not None
                else str(current_speech.get("openai_tts_api_key") or "").strip()
            ),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Failed to fetch OpenAI-compatible TTS voices.") from exc


@app.post("/api/settings/speech/openai-compatible-tts-models")
async def get_speech_openai_compatible_tts_models(payload: OpenAiCompatibleTtsModelsRequest) -> Dict[str, Any]:
    current_speech = get_shared_speech_settings()
    try:
        return await fetch_openai_compatible_tts_model_options(
            base_url=(
                str(payload.base_url).strip()
                if payload.base_url is not None
                else str(current_speech.get("openai_tts_base_url") or "").strip()
            ),
            api_key=(
                str(payload.api_key).strip()
                if payload.api_key is not None
                else str(current_speech.get("openai_tts_api_key") or "").strip()
            ),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Failed to fetch OpenAI-compatible TTS models.") from exc


@app.get("/api/settings/speech/warmup")
def get_speech_model_warmup() -> Dict[str, Any]:
    return _speech_model_warmup_snapshot()


@app.get("/api/settings/hf-llm/warmup")
def get_hf_llm_warmup() -> Dict[str, Any]:
    return _hf_llm_warmup_snapshot()


@app.post("/api/settings/hf-llm/warmup/cancel")
def cancel_hf_llm_warmup(request: HfLlmWarmupCancelRequest) -> Dict[str, Any]:
    key = str(request.key or "").strip()
    if not key and request.provider and request.model:
        key = _hf_llm_warmup_item_key(str(request.provider or ""), str(request.model or ""))
    cancel_all = bool(request.cancel_all) or not key
    now = time.time()
    with hf_llm_warmup_lock:
        if not bool(hf_llm_warmup_state.get("running")):
            snapshot = _hf_llm_warmup_snapshot()
            snapshot["cancel_requested"] = False
            return snapshot

        items = []
        for raw_item in list(hf_llm_warmup_state.get("items") or []):
            item = dict(raw_item) if isinstance(raw_item, dict) else {}
            item_key = str(
                item.get("key") or _hf_llm_warmup_item_key(item.get("provider", ""), item.get("model", ""))
            ).strip()
            status = str(item.get("status") or "").strip().lower()
            if cancel_all or item_key == key:
                if item_key:
                    hf_llm_warmup_cancel_keys.add(item_key)
                if status not in {"loaded", "error", "cancelled", "canceled"}:
                    item["status"] = "cancelling"
                    item["message"] = "Cancel requested."
                    item["cancel_requested"] = True
                    item["updated_ts"] = now
            items.append(item)

        if cancel_all:
            hf_llm_warmup_state["cancel_requested"] = True
        hf_llm_warmup_state["items"] = items

    snapshot = _hf_llm_warmup_snapshot()
    snapshot["cancel_requested"] = True
    return snapshot


@app.get("/api/settings/local-llm/models")
def get_local_llm_models(provider: str = "") -> Dict[str, Any]:
    return _local_llm_models_payload(provider=provider)


@app.post("/api/settings/local-llm/models/delete")
def delete_local_llm_model(request: LocalLlmModelDeleteRequest) -> Dict[str, Any]:
    result = _delete_local_llm_model(str(request.provider or ""), str(request.model or ""))
    return {
        "ok": True,
        "deleted": result,
        "local_llm_models": _local_llm_models_payload(),
    }


@app.get("/api/settings/llama-cpp/diagnostics")
def get_llama_cpp_diagnostics() -> Dict[str, Any]:
    return get_llama_cpp_runtime_diagnostics()


@app.get("/api/settings/llama-cpp/chat-template")
def get_llama_cpp_chat_template(model: str) -> Dict[str, Any]:
    return _llama_cpp_chat_template_model_info(model)


@app.post("/api/settings/llama-cpp/chat-template")
def save_llama_cpp_chat_template(request: LlamaCppChatTemplateRequest) -> Dict[str, Any]:
    model = str(request.model or "").strip()
    if not model:
        raise HTTPException(status_code=400, detail="llama.cpp model is required.")
    try:
        if request.reset:
            clear_llama_cpp_chat_template_override(model)
        else:
            set_llama_cpp_chat_template_override(model, request.template or "")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    unload_result = unload_local_llm_models(provider=HYDRA_LLM_PROVIDER_LLAMA_CPP, model=model)
    info = _llama_cpp_chat_template_model_info(model)
    info["ok"] = True
    info["unload"] = unload_result
    return info


@app.get("/api/settings/local-llm/chat-template")
def get_local_llm_chat_template(provider: str = "", model: str = "") -> Dict[str, Any]:
    return _local_llm_chat_template_model_info(provider, model)


@app.post("/api/settings/local-llm/chat-template")
def save_local_llm_chat_template(request: LlamaCppChatTemplateRequest) -> Dict[str, Any]:
    provider = _normalize_hydra_llm_provider(request.provider)
    if not _is_local_hydra_llm_provider(provider):
        raise HTTPException(status_code=400, detail="Local LLM provider is required.")
    model = str(request.model or "").strip()
    if not model:
        raise HTTPException(status_code=400, detail="Local model is required.")
    try:
        if request.reset:
            clear_local_llm_chat_template_override(provider, model)
        else:
            set_local_llm_chat_template_override(provider, model, request.template or "")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    unload_result = unload_local_llm_models(provider=provider, model=model)
    info = _local_llm_chat_template_model_info(provider, model)
    info["ok"] = True
    info["unload"] = unload_result
    return info


@app.get("/api/settings/huggingface/models")
def get_huggingface_models(
    provider: str = "hf_transformers",
    view: str = "trending",
    query: str = "",
    task: str = "text-generation",
    limit: int = 24,
    cursor: str = "",
) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(provider)
    if not _is_local_hydra_llm_provider(provider_token):
        provider_token = HYDRA_LLM_PROVIDER_HF_TRANSFORMERS
    view_token = str(view or "trending").strip().lower().replace("_", "-")
    task_token = _normalize_hf_browser_task(task)
    clean_limit = max(4, min(48, int(limit or 24)))
    search = _hf_browser_provider_search(provider_token, query, task_token)
    integration_status = _hf_browser_integration_status()
    if view_token in {"picks", "tater", "tater-picks", "recommended"}:
        models = _hf_browser_tater_pick_models(query=search, task=task_token, limit=clean_limit, provider=provider_token)
        return {
            "provider": provider_token,
            "provider_label": _hydra_llm_provider_label(provider_token),
            "view": "picks",
            "query": search,
            "task": task_token,
            "library": "",
            "app_filter": "",
            "integration": integration_status,
            "limit": clean_limit,
            "has_next": False,
            "next_cursor": "",
            "models": models,
        }
    if view_token in {"new", "recent", "latest"}:
        sort = "lastModified"
        response_view = "new"
    elif view_token in {"downloads", "downloaded", "most-downloaded", "popular"}:
        sort = "downloads"
        response_view = "downloads"
    else:
        sort = "trendingScore"
        response_view = "trending"
    page_url = _hf_browser_cursor_decode(cursor) if str(cursor or "").strip() else _hf_browser_models_api_url(
        provider=provider_token,
        search=search,
        sort=sort,
        limit=clean_limit,
        task=task_token,
    )
    try:
        raw_models, next_url = _hf_browser_fetch_models_page(page_url)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=_hf_browser_error_detail(exc, "Failed to search Hugging Face models."),
        ) from exc

    try:
        filtered_models = [
            model
            for model in raw_models[:clean_limit]
            if (
                (_hf_browser_is_vision_model(model) if task_token == "image-text-to-text" else _hf_browser_is_text_generation_model(model))
                and _hf_browser_provider_matches(model, provider_token)
            )
        ]
        models = [_hf_browser_model_summary(model, provider=provider_token) for model in filtered_models]
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=_hf_browser_error_detail(exc, "Failed to read Hugging Face model results."),
        ) from exc
    next_cursor = _hf_browser_cursor_encode(next_url)
    return {
        "provider": provider_token,
        "provider_label": _hydra_llm_provider_label(provider_token),
        "view": response_view,
        "query": search,
        "task": task_token,
        "library": _hf_browser_provider_library(provider_token),
        "app_filter": _hf_browser_provider_app_filter(provider_token, task_token),
        "integration": integration_status,
        "limit": clean_limit,
        "has_next": bool(next_cursor),
        "next_cursor": next_cursor,
        "models": models,
    }


@app.get("/api/settings/huggingface/model")
def get_huggingface_model_detail(
    repo_id: str,
    provider: str = "hf_transformers",
) -> Dict[str, Any]:
    repo = str(repo_id or "").strip()
    if not repo or "/" not in repo:
        raise HTTPException(status_code=400, detail="repo_id must look like owner/repo")
    provider_token = _normalize_hydra_llm_provider(provider)
    if not _is_local_hydra_llm_provider(provider_token):
        provider_token = HYDRA_LLM_PROVIDER_HF_TRANSFORMERS
    integration_status = _hf_browser_integration_status()
    try:
        from huggingface_hub import HfApi  # type: ignore
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Hugging Face model browser needs huggingface_hub installed.") from exc

    api = HfApi(token=_hf_browser_token())
    try:
        try:
            info = api.model_info(repo_id=repo, files_metadata=True)
        except TypeError:
            info = api.model_info(repo_id=repo)
        try:
            fallback_files = list(api.list_repo_files(repo_id=repo, repo_type="model"))
        except Exception:
            fallback_files = []
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=_hf_browser_error_detail(exc, "Failed to fetch Hugging Face model details."),
        ) from exc

    files = _hf_browser_file_rows(info, fallback_files)
    summary = _hf_browser_model_summary(info, provider=provider_token)
    preferred_gguf = _hf_browser_preferred_gguf(files)
    preferred_mmproj = _hf_browser_preferred_mmproj(files, preferred_gguf)
    effective_model_id = _hf_browser_effective_model_id(provider_token, repo, preferred_gguf)
    return {
        "provider": provider_token,
        "provider_label": _hydra_llm_provider_label(provider_token),
        "model": summary,
        "files": files,
        "preferred_gguf": preferred_gguf,
        "preferred_mmproj": preferred_mmproj,
        "supports_vision": bool(summary.get("supports_vision") or preferred_mmproj),
        "effective_model_id": effective_model_id,
        "integration": integration_status,
    }


@app.post("/api/settings/huggingface/download")
def start_huggingface_model_download(request: HfModelDownloadRequest) -> Dict[str, Any]:
    provider_token = _normalize_hydra_llm_provider(request.provider)
    if not _is_local_hydra_llm_provider(provider_token):
        raise HTTPException(status_code=400, detail="Choose a local provider before downloading.")
    repo = str(request.repo_id or "").strip()
    model_id = str(request.model_id or "").strip()
    filename = str(request.filename or "").strip()
    task_token = _normalize_hf_browser_task(request.task)
    effective_model_id = model_id or _hf_browser_effective_model_id(provider_token, repo, filename)
    if not effective_model_id:
        raise HTTPException(status_code=400, detail="Model id is required.")
    result = _start_hf_llm_warmup(
        [{"provider": provider_token, "model": effective_model_id}],
        reason="huggingface-browser",
        load_models=False,
    )
    result["provider"] = provider_token
    result["provider_label"] = _hydra_llm_provider_label(provider_token)
    result["model_id"] = effective_model_id
    result["task"] = task_token
    return result


@app.get("/api/settings/integrations")
def get_settings_integrations() -> Dict[str, Any]:
    return {"integrations": get_integration_catalog()}


@app.get("/api/settings/integrations/runtime")
def get_settings_integrations_runtime() -> Dict[str, Any]:
    return {"runtime": integration_runtime_status(redis_client)}


@app.get("/api/settings/integrations/runtime/events")
def get_settings_integrations_runtime_events(after_seq: int = 0, limit: int = 200) -> Dict[str, Any]:
    return integration_runtime_events(redis_client, after_seq=after_seq, limit=limit)


@app.get("/api/settings/logs")
def get_settings_logs(after_seq: int = 0, limit: int = 300, level: str = "", logger_name: str = "") -> Dict[str, Any]:
    requested_limit = max(1, min(1000, int(limit or 300)))
    after = max(0, int(after_seq or 0))
    level_token = str(level or "").strip().lower()
    logger_filter = str(logger_name or "").strip().lower()
    min_levels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "warn": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    min_level = min_levels.get(level_token, 0)
    with app_log_lock:
        rows = list(app_log_entries)
        latest_seq = app_log_next_seq - 1

    level_counts = Counter(str(row.get("level") or "info").lower() for row in rows)
    logger_names = sorted(
        {
            str(row.get("logger") or "root").strip()
            for row in rows
            if str(row.get("logger") or "root").strip()
        }
    )

    filtered: List[Dict[str, Any]] = []
    for row in rows:
        seq = int(row.get("seq") or 0)
        if after and seq <= after:
            continue
        if min_level and int(row.get("levelno") or 0) < min_level:
            continue
        if logger_filter and logger_filter not in str(row.get("logger") or "").lower():
            continue
        filtered.append(dict(row))

    return {
        "entries": filtered[-requested_limit:],
        "next_seq": latest_seq,
        "oldest_seq": int(rows[0].get("seq") or 0) if rows else 0,
        "buffer_limit": APP_LOG_BUFFER_LIMIT,
        "level_counts": dict(level_counts),
        "loggers": logger_names,
        "filtered_total": len(filtered),
    }


@app.get("/api/settings/integrations/runtime/states")
def get_settings_integrations_runtime_states() -> Dict[str, Any]:
    return integration_runtime_states(redis_client)


@app.get("/api/settings/integrations/devices")
def get_settings_integrations_devices(capability: str = "") -> Dict[str, Any]:
    if str(capability or "").strip():
        return {
            "capability": str(capability or "").strip(),
            "devices": get_integration_devices_by_capability(capability),
        }
    return get_integration_devices()


@app.get("/api/settings/integrations/{integration_id}/devices")
def get_settings_integration_devices(integration_id: str) -> Dict[str, Any]:
    try:
        return get_integration_device_group(integration_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Integration device load failed.") from exc


@app.post("/api/settings/integrations/{integration_id}/settings")
def update_registered_integration_settings(
    integration_id: str,
    payload: IntegrationSettingsRequest,
) -> Dict[str, Any]:
    try:
        return save_registered_integration_settings(integration_id, payload.settings)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Integration settings save failed.") from exc


@app.post("/api/settings/integrations/{integration_id}/actions/{action_id}")
def run_registered_settings_integration_action(
    integration_id: str,
    action_id: str,
    payload: IntegrationActionRequest,
) -> Dict[str, Any]:
    try:
        return run_registered_integration_action(integration_id, action_id, payload.payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Integration action failed.") from exc


@app.post("/api/settings/hue/link")
def link_hue_bridge(payload: HueLinkRequest) -> Dict[str, Any]:
    hue_module = _integration_module_or_400("hue", "Hue")
    return hue_module.pair_hue_bridge(
        bridge_host=payload.hue_bridge_host,
        device_type=payload.hue_device_type,
        timeout_seconds=payload.hue_timeout_seconds,
    )


@app.post("/api/settings/aladdin/test")
def test_aladdin_settings(payload: AladdinTestRequest) -> Dict[str, Any]:
    try:
        aladdin_module = _integration_module_or_400("aladdin", "Aladdin")
        return aladdin_module.test_aladdin_connection(
            username=payload.aladdin_username,
            password=payload.aladdin_password,
            timeout_seconds=payload.aladdin_timeout_seconds,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "Aladdin Connect test failed.") from exc


@app.post("/api/settings")
def update_settings(payload: AppSettingsRequest, response: Response) -> Dict[str, Any]:
    updates = payload.model_dump(exclude_none=True)
    speech_warmup_result: Dict[str, Any] = {}
    hf_llm_warmup_result: Dict[str, Any] = {}
    tts_reload_result: Dict[str, Any] = {}

    def _bounded_int(
        value: Any,
        *,
        default: int,
        min_value: int = 0,
        max_value: Optional[int] = None,
    ) -> int:
        try:
            parsed = int(float(value))
        except Exception:
            parsed = int(default)
        if parsed < int(min_value):
            parsed = int(min_value)
        if max_value is not None and parsed > int(max_value):
            parsed = int(max_value)
        return int(parsed)

    def _save_local_llm_context_setting(
        payload_key: str,
        redis_key: str,
        *,
        min_value: int = 256,
        max_value: int = 1_048_576,
    ) -> None:
        if payload_key not in updates:
            return
        raw = str(updates.get(payload_key) or "").strip()
        if not raw:
            redis_client.delete(redis_key)
            return
        try:
            parsed = int(float(raw))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{payload_key} must be a whole number of tokens.") from exc
        if parsed < min_value or parsed > max_value:
            raise HTTPException(
                status_code=400,
                detail=f"{payload_key} must be between {min_value} and {max_value} tokens.",
            )
        redis_client.set(redis_key, str(int(parsed)))

    def _save_bool_setting(payload_key: str, redis_key: str, *, default: bool = False) -> None:
        if payload_key not in updates:
            return
        redis_client.set(redis_key, "true" if _as_bool_flag(updates.get(payload_key), default=default) else "false")

    def _save_bounded_int_setting(
        payload_key: str,
        redis_key: str,
        *,
        default: int,
        min_value: int = 1,
        max_value: int = 16,
    ) -> None:
        if payload_key not in updates:
            return
        raw = str(updates.get(payload_key) or "").strip()
        if not raw:
            redis_client.set(redis_key, str(int(default)))
            return
        try:
            parsed = int(float(raw))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{payload_key} must be a whole number.") from exc
        if parsed < min_value or parsed > max_value:
            raise HTTPException(
                status_code=400,
                detail=f"{payload_key} must be between {min_value} and {max_value}.",
            )
        redis_client.set(redis_key, str(int(parsed)))

    def _save_text_setting(payload_key: str, redis_key: str, *, max_length: int = 4096) -> None:
        if payload_key not in updates:
            return
        raw = str(updates.get(payload_key) or "").strip()
        if not raw:
            redis_client.delete(redis_key)
            return
        if len(raw) > int(max_length):
            raise HTTPException(status_code=400, detail=f"{payload_key} is too long.")
        redis_client.set(redis_key, raw)

    def _save_text_choice_setting(
        payload_key: str,
        redis_key: str,
        *,
        allowed: Tuple[str, ...],
        default: str = "",
    ) -> None:
        if payload_key not in updates:
            return
        token = str(updates.get(payload_key) or default or "").strip().lower()
        aliases = {
            "fp16": "float16",
            "half": "float16",
            "bf16": "bfloat16",
            "fp32": "float32",
            "none": "disabled",
            "off": "disabled",
            "false": "disabled",
            "0": "disabled",
            "default": "default",
            "auto": "auto",
        }
        token = aliases.get(token, token)
        allowed_set = {str(item).strip().lower() for item in allowed}
        if token not in allowed_set:
            raise HTTPException(status_code=400, detail=f"{payload_key} has an unsupported value.")
        redis_client.set(redis_key, token)

    def _save_optional_int_setting(
        payload_key: str,
        redis_key: str,
        *,
        min_value: int = 0,
        max_value: int = 1_048_576,
        allow_zero: bool = False,
    ) -> None:
        if payload_key not in updates:
            return
        raw = str(updates.get(payload_key) or "").strip()
        if not raw:
            redis_client.delete(redis_key)
            return
        try:
            parsed = int(float(raw))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{payload_key} must be a whole number.") from exc
        if parsed < min_value or parsed > max_value or (parsed == 0 and not allow_zero):
            lower = min_value if allow_zero or min_value > 0 else 1
            raise HTTPException(
                status_code=400,
                detail=f"{payload_key} must be between {lower} and {max_value}.",
            )
        redis_client.set(redis_key, str(int(parsed)))

    _save_tater_api_settings_from_updates(updates)
    _save_spud_link_settings_from_updates(updates)

    local_model_rows_cache: Optional[List[Dict[str, Any]]] = None

    def _downloaded_local_model_rows() -> List[Dict[str, Any]]:
        nonlocal local_model_rows_cache
        if local_model_rows_cache is None:
            local_model_rows_cache = [
                row
                for row in _local_llm_models_payload().get("models", [])
                if isinstance(row, dict)
            ]
        return local_model_rows_cache

    def _resolve_downloaded_local_model(provider: Any, model: Any) -> str:
        provider_token = _normalize_hydra_llm_provider(provider)
        model_token = str(model or "").strip()
        if not provider_token or not model_token:
            return ""
        rows = _downloaded_local_model_rows()
        for row in rows:
            if _normalize_hydra_llm_provider(row.get("provider")) == provider_token and str(row.get("model") or "").strip() == model_token:
                return model_token
        if provider_token == HYDRA_LLM_PROVIDER_LLAMA_CPP and "::" not in model_token:
            candidates = [
                str(row.get("model") or "").strip()
                for row in rows
                if _normalize_hydra_llm_provider(row.get("provider")) == provider_token
                and str(row.get("repo_id") or "").strip() == model_token
                and str(row.get("filename") or "").strip()
                and str(row.get("model") or "").strip()
            ]
            if candidates:
                return candidates[0]
        return ""

    def _require_downloaded_local_model(provider: Any, model: Any, label: str) -> str:
        provider_token = _normalize_hydra_llm_provider(provider)
        model_token = str(model or "").strip()
        resolved_model = _resolve_downloaded_local_model(provider_token, model_token)
        if (
            _is_local_hydra_llm_provider(provider_token)
            and model_token
            and not resolved_model
        ):
            raise HTTPException(
                status_code=400,
                detail=f"{label} {_hydra_llm_provider_label(provider_token)} model must be downloaded from the Hugging Face tab before saving.",
            )
        return resolved_model or model_token

    explicit_local_model_load_targets = "hydra_local_model_load_targets" in updates

    def _normalize_local_model_load_targets(value: Any) -> List[Dict[str, str]]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise HTTPException(status_code=400, detail="hydra_local_model_load_targets must be a list.")
        targets: List[Dict[str, str]] = []
        seen: set[Tuple[str, str]] = set()
        for raw_target in value:
            if not isinstance(raw_target, dict):
                continue
            provider_token = _normalize_hydra_llm_provider(raw_target.get("provider"))
            model_token = str(raw_target.get("model") or "").strip()
            if not _is_local_hydra_llm_provider(provider_token) or not model_token:
                continue
            model_token = _require_downloaded_local_model(provider_token, model_token, "Load target")
            key = (provider_token, model_token)
            if key in seen:
                continue
            seen.add(key)
            targets.append({"provider": provider_token, "model": model_token})
        return targets

    local_model_load_targets = (
        _normalize_local_model_load_targets(updates.get("hydra_local_model_load_targets"))
        if explicit_local_model_load_targets
        else []
    )
    base_settings_keys = {
        "hydra_llm_host",
        "hydra_llm_port",
        "hydra_llm_model",
        "hydra_llm_api_key",
        "hydra_llm_provider",
        "hydra_base_servers",
        "hydra_hf_transformers_context_tokens",
        "hydra_hf_transformers_device",
        "hydra_hf_transformers_dtype",
        "hydra_hf_transformers_device_map",
        "hydra_hf_transformers_attn_implementation",
        "hydra_hf_transformers_trust_remote_code",
        "hydra_llama_cpp_context_tokens",
        "hydra_llama_cpp_mtp_enabled",
        "hydra_llama_cpp_mtp_draft_tokens",
        "hydra_llama_cpp_mtp_draft_model",
        "hydra_llama_cpp_n_batch",
        "hydra_llama_cpp_n_ubatch",
        "hydra_llama_cpp_flash_attn",
        "hydra_llama_cpp_offload_kqv",
        "hydra_mlx_lm_context_tokens",
        "hydra_mlx_lm_trust_remote_code",
        "hydra_mlx_lm_lazy_load",
        "hydra_mlx_engine_prefill_step_size",
        "hydra_mlx_engine_kv_bits",
        "hydra_mlx_engine_kv_group_size",
        "hydra_mlx_engine_quantized_kv_start",
    }
    spudex_model_keys = {"spudex_llm_provider", "spudex_llm_host", "spudex_llm_model"}
    vision_model_keys = {
        "vision_mode",
        "vision_provider",
        "vision_model",
        "vision_api_base",
        "vision_api_key",
        "hydra_llama_cpp_vision_context_tokens",
    }

    def _single_local_model_target(provider: Any, model: Any) -> List[Dict[str, str]]:
        provider_token = _normalize_hydra_llm_provider(provider)
        model_token = str(model or "").strip()
        if not model_token or not _is_local_hydra_llm_provider(provider_token):
            return []
        return [_hf_llm_warmup_base_item(provider_token, model_token)]

    def _current_spudex_local_targets() -> List[Dict[str, str]]:
        try:
            from spudex.settings import get_spudex_settings

            current = get_spudex_settings(redis_client)
        except Exception:
            current = {}
        return _single_local_model_target(current.get("llm_provider"), current.get("llm_model"))

    def _current_beast_local_targets() -> List[Dict[str, str]]:
        targets: List[Dict[str, str]] = []
        for role_id in HYDRA_BEAST_CONFIG_ROLE_IDS:
            role_provider = _normalize_hydra_llm_provider(redis_client.get(_hydra_role_llm_key(role_id, "provider")))
            role_model = str(redis_client.get(_hydra_role_llm_key(role_id, "model")) or "").strip()
            targets.extend(_single_local_model_target(role_provider, role_model))
        return _dedupe_hf_llm_warmup_targets(targets)

    def _current_vision_local_targets() -> List[Dict[str, str]]:
        try:
            current = get_shared_vision_settings(
                default_api_base="http://127.0.0.1:1234",
                default_model="qwen2.5-vl-7b-instruct",
            )
        except Exception:
            current = {}
        mode = str(current.get("mode") or "api").strip().lower()
        if mode != "dedicated":
            return []
        return _single_local_model_target(current.get("provider"), current.get("model"))

    current_scope_local_targets = {
        "base": _hf_llm_warmup_models(resolve_hydra_base_servers(redis_conn=redis_client, include_legacy=True)),
        "spudex": _current_spudex_local_targets(),
        "vision": _current_vision_local_targets(),
        "beast": _current_beast_local_targets(),
    }
    touched_local_model_scopes: set[str] = set()
    if any(key in updates for key in base_settings_keys):
        touched_local_model_scopes.add("base")
    if any(key in updates for key in spudex_model_keys):
        touched_local_model_scopes.add("spudex")
    if any(key in updates for key in vision_model_keys):
        touched_local_model_scopes.add("vision")
    if "hydra_beast_mode_enabled" in updates or any(
        f"hydra_llm_{role_id}_{field}" in updates
        for role_id in HYDRA_BEAST_CONFIG_ROLE_IDS
        for field in ("provider", "host", "port", "model", "api_key")
    ):
        touched_local_model_scopes.add("beast")

    protected_local_model_keys = {
        str(target.get("key") or _hf_llm_warmup_item_key(target.get("provider", ""), target.get("model", ""))).strip()
        for scope, targets in current_scope_local_targets.items()
        if scope not in touched_local_model_scopes
        for target in targets
    }
    local_model_unload_targets = _dedupe_hf_llm_warmup_targets(
        [
            target
            for scope in sorted(touched_local_model_scopes)
            for target in current_scope_local_targets.get(scope, [])
            if str(target.get("key") or _hf_llm_warmup_item_key(target.get("provider", ""), target.get("model", ""))).strip()
            not in protected_local_model_keys
        ]
        if explicit_local_model_load_targets and local_model_load_targets
        else []
    )

    _save_local_llm_context_setting(
        "hydra_hf_transformers_context_tokens",
        HYDRA_HF_TRANSFORMERS_CONTEXT_TOKENS_KEY,
    )
    _save_text_choice_setting(
        "hydra_hf_transformers_device",
        HYDRA_HF_TRANSFORMERS_DEVICE_KEY,
        allowed=("auto", "cuda", "mps", "cpu"),
        default=DEFAULT_HF_TRANSFORMERS_DEVICE,
    )
    _save_text_choice_setting(
        "hydra_hf_transformers_dtype",
        HYDRA_HF_TRANSFORMERS_DTYPE_KEY,
        allowed=("auto", "float16", "bfloat16", "float32"),
        default=DEFAULT_HF_TRANSFORMERS_DTYPE,
    )
    _save_text_choice_setting(
        "hydra_hf_transformers_device_map",
        HYDRA_HF_TRANSFORMERS_DEVICE_MAP_KEY,
        allowed=("default", "disabled", "auto", "balanced"),
        default=DEFAULT_HF_TRANSFORMERS_DEVICE_MAP,
    )
    _save_text_choice_setting(
        "hydra_hf_transformers_attn_implementation",
        HYDRA_HF_TRANSFORMERS_ATTN_IMPLEMENTATION_KEY,
        allowed=("", "auto", "sdpa", "flash_attention_2", "eager"),
        default=DEFAULT_HF_TRANSFORMERS_ATTN_IMPLEMENTATION,
    )
    _save_bool_setting(
        "hydra_hf_transformers_trust_remote_code",
        HYDRA_HF_TRANSFORMERS_TRUST_REMOTE_CODE_KEY,
        default=DEFAULT_HF_TRANSFORMERS_TRUST_REMOTE_CODE,
    )
    _save_local_llm_context_setting(
        "hydra_llama_cpp_context_tokens",
        HYDRA_LLAMA_CPP_CONTEXT_TOKENS_KEY,
    )
    _save_local_llm_context_setting(
        "hydra_llama_cpp_vision_context_tokens",
        HYDRA_LLAMA_CPP_VISION_CONTEXT_TOKENS_KEY,
    )
    _save_bool_setting(
        "hydra_llama_cpp_mtp_enabled",
        HYDRA_LLAMA_CPP_MTP_ENABLED_KEY,
        default=DEFAULT_LLAMA_CPP_MTP_ENABLED,
    )
    _save_bounded_int_setting(
        "hydra_llama_cpp_mtp_draft_tokens",
        HYDRA_LLAMA_CPP_MTP_DRAFT_TOKENS_KEY,
        default=DEFAULT_LLAMA_CPP_MTP_DRAFT_TOKENS,
        min_value=1,
        max_value=16,
    )
    _save_text_setting(
        "hydra_llama_cpp_mtp_draft_model",
        HYDRA_LLAMA_CPP_MTP_DRAFT_MODEL_KEY,
        max_length=512,
    )
    _save_bounded_int_setting(
        "hydra_llama_cpp_n_batch",
        HYDRA_LLAMA_CPP_N_BATCH_KEY,
        default=DEFAULT_LLAMA_CPP_N_BATCH,
        min_value=32,
        max_value=8192,
    )
    _save_bounded_int_setting(
        "hydra_llama_cpp_n_ubatch",
        HYDRA_LLAMA_CPP_N_UBATCH_KEY,
        default=DEFAULT_LLAMA_CPP_N_UBATCH,
        min_value=0,
        max_value=8192,
    )
    _save_bool_setting(
        "hydra_llama_cpp_flash_attn",
        HYDRA_LLAMA_CPP_FLASH_ATTN_KEY,
        default=DEFAULT_LLAMA_CPP_FLASH_ATTN,
    )
    _save_bool_setting(
        "hydra_llama_cpp_offload_kqv",
        HYDRA_LLAMA_CPP_OFFLOAD_KQV_KEY,
        default=DEFAULT_LLAMA_CPP_OFFLOAD_KQV,
    )
    _save_local_llm_context_setting(
        "hydra_mlx_lm_context_tokens",
        HYDRA_MLX_LM_CONTEXT_TOKENS_KEY,
        min_value=128,
    )
    _save_bool_setting(
        "hydra_mlx_lm_trust_remote_code",
        HYDRA_MLX_LM_TRUST_REMOTE_CODE_KEY,
        default=DEFAULT_MLX_LM_TRUST_REMOTE_CODE,
    )
    _save_bool_setting(
        "hydra_mlx_lm_lazy_load",
        HYDRA_MLX_LM_LAZY_LOAD_KEY,
        default=DEFAULT_MLX_LM_LAZY_LOAD,
    )
    _save_optional_int_setting(
        "hydra_mlx_engine_prefill_step_size",
        HYDRA_MLX_ENGINE_PREFILL_STEP_SIZE_KEY,
        min_value=1,
        max_value=32768,
    )
    _save_text_choice_setting(
        "hydra_mlx_engine_kv_bits",
        HYDRA_MLX_ENGINE_KV_BITS_KEY,
        allowed=("", "2", "3", "4", "6", "8"),
        default="",
    )
    _save_text_choice_setting(
        "hydra_mlx_engine_kv_group_size",
        HYDRA_MLX_ENGINE_KV_GROUP_SIZE_KEY,
        allowed=("", "32", "64", "128"),
        default="",
    )
    _save_optional_int_setting(
        "hydra_mlx_engine_quantized_kv_start",
        HYDRA_MLX_ENGINE_QUANTIZED_KV_START_KEY,
        min_value=0,
        max_value=1_048_576,
        allow_zero=True,
    )

    username = updates.get("username")
    if isinstance(username, str):
        redis_client.hset("chat_settings", "username", username.strip() or "User")

    clear_webui_password = bool(updates.get("clear_webui_password"))
    webui_password_raw = str(updates.get("webui_password") or "")
    webui_password_present = "webui_password" in updates
    webui_password_confirm = str(updates.get("webui_password_confirm") or "")
    if not webui_password_present:
        updates.pop("webui_password_confirm", None)

    if clear_webui_password and webui_password_raw.strip():
        raise HTTPException(status_code=400, detail="Set a WebUI password or clear it, not both.")

    if clear_webui_password:
        updates.pop("webui_password", None)
        updates.pop("webui_password_confirm", None)
        redis_client.delete(WEBUI_AUTH_PASSWORD_HASH_KEY)
        _clear_webui_sessions()
        _clear_webui_auth_cookie(response)
    elif webui_password_present:
        if webui_password_raw:
            if len(webui_password_raw) < int(WEBUI_AUTH_PASSWORD_MIN_LENGTH):
                raise HTTPException(
                    status_code=400,
                    detail=f"WebUI password must be at least {WEBUI_AUTH_PASSWORD_MIN_LENGTH} characters.",
                )
            if webui_password_raw != webui_password_confirm:
                raise HTTPException(status_code=400, detail="WebUI password confirmation does not match.")
            salt = secrets.token_bytes(16)
            password_hash = _hash_webui_password(webui_password_raw, salt=salt)
            redis_client.set(WEBUI_AUTH_PASSWORD_HASH_KEY, password_hash)
            _clear_webui_sessions()
            session_token = _new_webui_session_token()
            _store_webui_session(session_token)
            _issue_webui_auth_cookie(response, session_token)
        else:
            updates.pop("webui_password", None)
            updates.pop("webui_password_confirm", None)

    if bool(updates.get("clear_user_avatar")):
        redis_client.hdel("chat_settings", "avatar")
    elif "user_avatar" in updates:
        raw_avatar = str(updates.get("user_avatar") or "").strip()
        if raw_avatar:
            try:
                normalized_avatar = _normalize_avatar_b64(raw_avatar)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=f"Invalid user avatar: {exc}") from exc
            redis_client.hset("chat_settings", "avatar", normalized_avatar)

    if bool(updates.get("clear_tater_avatar")):
        redis_client.delete("tater:avatar")
    elif "tater_avatar" in updates:
        raw_avatar = str(updates.get("tater_avatar") or "").strip()
        if raw_avatar:
            try:
                normalized_avatar = _normalize_avatar_b64(raw_avatar)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=f"Invalid Tater avatar: {exc}") from exc
            redis_client.set("tater:avatar", normalized_avatar)

    if "max_display" in updates:
        redis_client.set("tater:max_display", max(1, int(updates["max_display"])))

    if "show_speed_stats" in updates:
        redis_client.set("tater:show_speed_stats", "true" if updates["show_speed_stats"] else "false")

    if "tater_first_name" in updates:
        redis_client.set("tater:first_name", str(updates["tater_first_name"]).strip() or "Tater")

    if "tater_last_name" in updates:
        redis_client.set("tater:last_name", str(updates["tater_last_name"]).strip() or "Totterson")

    if "tater_personality" in updates:
        redis_client.set("tater:personality", str(updates["tater_personality"]))

    if "max_store" in updates:
        redis_client.set("tater:max_store", max(0, int(updates["max_store"])))

    if "max_llm" in updates:
        redis_client.set("tater:max_llm", max(1, int(updates["max_llm"])))

    if "popup_effect_style" in updates:
        raw_style = str(updates.get("popup_effect_style") or "").strip().lower()
        if raw_style and raw_style not in WEBUI_POPUP_EFFECT_STYLE_CHOICES:
            allowed = ", ".join(sorted(WEBUI_POPUP_EFFECT_STYLE_CHOICES))
            raise HTTPException(status_code=400, detail=f"popup_effect_style must be one of: {allowed}")
        normalized_style = _normalize_popup_effect_style(raw_style, default=DEFAULT_WEBUI_POPUP_EFFECT_STYLE)
        redis_client.set(WEBUI_POPUP_EFFECT_STYLE_KEY, normalized_style)

    if "homeassistant_base_url" in updates or "homeassistant_token" in updates:
        homeassistant_module = _integration_module_or_400("homeassistant", "Home Assistant")
        current_ha = homeassistant_module.load_homeassistant_config(required=False)
        homeassistant_module.save_homeassistant_config(
            base_url=updates.get("homeassistant_base_url", current_ha.get("base")),
            token=updates.get("homeassistant_token", current_ha.get("token")),
        )

    hue_update_keys = {"hue_bridge_host", "hue_app_key", "hue_device_type", "hue_timeout_seconds"}
    if any(key in updates for key in hue_update_keys):
        hue_module = _integration_module_or_400("hue", "Hue")
        current_hue = hue_module.read_hue_settings()
        hue_module.save_hue_settings(
            bridge_host=updates.get("hue_bridge_host", current_hue.get("HUE_BRIDGE_HOST")),
            app_key=updates.get("hue_app_key", current_hue.get("HUE_APP_KEY")),
            device_type=updates.get("hue_device_type", current_hue.get("HUE_DEVICE_TYPE")),
            timeout_seconds=updates.get("hue_timeout_seconds", current_hue.get("HUE_TIMEOUT_SECONDS")),
        )

    aladdin_update_keys = {"aladdin_username", "aladdin_password", "aladdin_timeout_seconds"}
    if any(key in updates for key in aladdin_update_keys):
        aladdin_module = _integration_module_or_400("aladdin", "Aladdin")
        current_aladdin = aladdin_module.read_aladdin_settings()
        aladdin_module.save_aladdin_settings(
            username=updates.get("aladdin_username", current_aladdin.get("ALADDIN_USERNAME")),
            password=updates.get("aladdin_password", current_aladdin.get("ALADDIN_PASSWORD")),
            timeout_seconds=updates.get("aladdin_timeout_seconds", current_aladdin.get("ALADDIN_TIMEOUT_SECONDS")),
        )

    sonos_update_keys = {"sonos_enabled", "sonos_discovery_timeout_seconds", "sonos_speaker_hosts"}
    if any(key in updates for key in sonos_update_keys):
        sonos_module = _integration_module_or_400("sonos", "Sonos")
        current_sonos = sonos_module.read_sonos_settings()
        sonos_module.save_sonos_settings(
            enabled=updates.get("sonos_enabled", current_sonos.get("SONOS_ENABLED")),
            discovery_timeout_seconds=updates.get(
                "sonos_discovery_timeout_seconds",
                current_sonos.get("SONOS_DISCOVERY_TIMEOUT_SECONDS"),
            ),
            speaker_hosts=updates.get("sonos_speaker_hosts", current_sonos.get("SONOS_SPEAKER_HOSTS")),
        )

    if "unifi_network_base_url" in updates or "unifi_network_api_key" in updates:
        unifi_network_module = _integration_module_or_400("unifi_network", "UniFi Network")
        current_network = unifi_network_module.read_unifi_network_settings()
        unifi_network_module.save_unifi_network_settings(
            base_url=updates.get("unifi_network_base_url", current_network.get("UNIFI_BASE_URL")),
            api_key=updates.get("unifi_network_api_key", current_network.get("UNIFI_API_KEY")),
        )

    if "unifi_protect_base_url" in updates or "unifi_protect_api_key" in updates:
        unifi_protect_module = _integration_module_or_400("unifi_protect", "UniFi Protect")
        current_protect = unifi_protect_module.read_unifi_protect_settings()
        unifi_protect_module.save_unifi_protect_settings(
            base_url=updates.get("unifi_protect_base_url", current_protect.get("base")),
            api_key=updates.get("unifi_protect_api_key", current_protect.get("api_key")),
        )

    vision_update_keys = {"vision_api_base", "vision_model", "vision_api_key", "vision_mode", "vision_provider"}
    if any(key in updates for key in vision_update_keys):
        current_vision = get_shared_vision_settings(
            default_api_base="http://127.0.0.1:1234",
            default_model="qwen2.5-vl-7b-instruct",
        )
        vision_mode = str(updates.get("vision_mode", current_vision.get("mode") or "api") or "api").strip().lower()
        if vision_mode not in {"api", "auto", "base", "dedicated"}:
            raise HTTPException(status_code=400, detail="vision_mode must be api, auto, base, or dedicated.")
        vision_provider = _normalize_hydra_llm_provider(
            updates.get("vision_provider", current_vision.get("provider") or HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE)
        )
        vision_model = str(updates.get("vision_model", current_vision.get("model") or "")).strip()
        if vision_mode == "dedicated" and _is_local_hydra_llm_provider(vision_provider):
            vision_model = _require_downloaded_local_model(vision_provider, vision_model, "Vision")
        save_shared_vision_settings(
            api_base=str(updates.get("vision_api_base", current_vision.get("api_base") or "")).strip(),
            model=vision_model,
            api_key=str(updates.get("vision_api_key", current_vision.get("api_key") or "")).strip(),
            mode=vision_mode,
            provider=vision_provider,
        )

    speech_keys = {
        "speech_stt_backend",
        "speech_acceleration",
        "speech_wyoming_stt_host",
        "speech_wyoming_stt_port",
        "speech_tts_backend",
        "speech_tts_model",
        "speech_tts_voice",
        "speech_kokoro_output_gain",
        "speech_pocket_tts_output_gain",
        "speech_wyoming_tts_host",
        "speech_wyoming_tts_port",
        "speech_wyoming_tts_voice",
        "speech_openai_tts_base_url",
        "speech_openai_tts_api_key",
        "speech_announcement_tts_backend",
        "speech_announcement_tts_model",
        "speech_announcement_tts_voice",
        "speech_announcement_wyoming_tts_host",
        "speech_announcement_wyoming_tts_port",
        "speech_announcement_wyoming_tts_voice",
        "speech_announcement_openai_tts_base_url",
        "speech_announcement_openai_tts_api_key",
    }
    tts_reload_keys = {
        "speech_acceleration",
        "speech_tts_backend",
        "speech_tts_model",
        "speech_tts_voice",
        "speech_announcement_tts_backend",
        "speech_announcement_tts_model",
        "speech_announcement_tts_voice",
    }
    speech_warmup_keys = set(speech_keys) - {
        "speech_kokoro_output_gain",
        "speech_pocket_tts_output_gain",
    }
    if any(key in updates for key in speech_keys):
        current_speech = get_shared_speech_settings()
        save_shared_speech_settings(
            stt_backend=str(updates.get("speech_stt_backend", current_speech.get("stt_backend") or "")).strip(),
            acceleration=str(updates.get("speech_acceleration", current_speech.get("acceleration") or "")).strip(),
            wyoming_stt_host=str(
                updates.get("speech_wyoming_stt_host", current_speech.get("wyoming_stt_host") or "")
            ).strip(),
            wyoming_stt_port=str(
                updates.get("speech_wyoming_stt_port", current_speech.get("wyoming_stt_port") or "")
            ).strip(),
            tts_backend=str(updates.get("speech_tts_backend", current_speech.get("tts_backend") or "")).strip(),
            tts_model=str(updates.get("speech_tts_model", current_speech.get("tts_model") or "")).strip(),
            tts_voice=str(updates.get("speech_tts_voice", current_speech.get("tts_voice") or "")).strip(),
            kokoro_output_gain=updates.get("speech_kokoro_output_gain", current_speech.get("kokoro_output_gain")),
            pocket_tts_output_gain=updates.get(
                "speech_pocket_tts_output_gain",
                current_speech.get("pocket_tts_output_gain"),
            ),
            wyoming_tts_host=str(
                updates.get("speech_wyoming_tts_host", current_speech.get("wyoming_tts_host") or "")
            ).strip(),
            wyoming_tts_port=str(
                updates.get("speech_wyoming_tts_port", current_speech.get("wyoming_tts_port") or "")
            ).strip(),
            wyoming_tts_voice=str(
                updates.get("speech_wyoming_tts_voice", current_speech.get("wyoming_tts_voice") or "")
            ).strip(),
            openai_tts_base_url=str(
                updates.get("speech_openai_tts_base_url", current_speech.get("openai_tts_base_url") or "")
            ).strip(),
            openai_tts_api_key=str(
                updates.get("speech_openai_tts_api_key", current_speech.get("openai_tts_api_key") or "")
            ).strip(),
            announcement_tts_backend=str(
                updates.get("speech_announcement_tts_backend", current_speech.get("announcement_tts_backend") or "")
            ).strip(),
            announcement_tts_model=str(
                updates.get("speech_announcement_tts_model", current_speech.get("announcement_tts_model") or "")
            ).strip(),
            announcement_tts_voice=str(
                updates.get("speech_announcement_tts_voice", current_speech.get("announcement_tts_voice") or "")
            ).strip(),
            announcement_wyoming_tts_host=str(
                updates.get(
                    "speech_announcement_wyoming_tts_host",
                    current_speech.get("announcement_wyoming_tts_host") or "",
                )
            ).strip(),
            announcement_wyoming_tts_port=str(
                updates.get(
                    "speech_announcement_wyoming_tts_port",
                    current_speech.get("announcement_wyoming_tts_port") or "",
                )
            ).strip(),
            announcement_wyoming_tts_voice=str(
                updates.get(
                    "speech_announcement_wyoming_tts_voice",
                    current_speech.get("announcement_wyoming_tts_voice") or "",
                )
            ).strip(),
            announcement_openai_tts_base_url=str(
                updates.get(
                    "speech_announcement_openai_tts_base_url",
                    current_speech.get("announcement_openai_tts_base_url") or "",
                )
            ).strip(),
            announcement_openai_tts_api_key=str(
                updates.get(
                    "speech_announcement_openai_tts_api_key",
                    current_speech.get("announcement_openai_tts_api_key") or "",
                )
            ).strip(),
        )
        if any(key in updates for key in tts_reload_keys):
            tts_reload_result = _reload_local_tts_model_caches(reason="settings-save")
        if any(key in updates for key in speech_warmup_keys):
            speech_warmup_result = _start_speech_model_warmup(get_shared_speech_settings(), reason="settings-save")

    spudex_model_keys = {"spudex_llm_provider", "spudex_llm_host", "spudex_llm_model"}
    if any(key in updates for key in spudex_model_keys):
        from spudex.settings import get_spudex_settings, save_spudex_settings

        current_spudex = get_spudex_settings(redis_client)
        spudex_provider = _normalize_hydra_llm_provider(
            updates.get("spudex_llm_provider", current_spudex.get("llm_provider") or "")
        )
        spudex_host = str(updates.get("spudex_llm_host", current_spudex.get("llm_host") or "") or "").strip()
        spudex_model = str(updates.get("spudex_llm_model", current_spudex.get("llm_model") or "") or "").strip()
        if _is_local_hydra_llm_provider(spudex_provider):
            spudex_model = _require_downloaded_local_model(spudex_provider, spudex_model, "Spudex")
            spudex_host = ""
        save_spudex_settings(
            {
                **current_spudex,
                "llm_provider": spudex_provider,
                "llm_host": spudex_host,
                "llm_model": spudex_model,
            },
            redis_client,
        )

    esphome_result: Dict[str, Any] = {}
    if "esphome_settings" in updates:
        raw_esphome_settings = updates.get("esphome_settings")
        if raw_esphome_settings is not None and not isinstance(raw_esphome_settings, dict):
            raise HTTPException(status_code=400, detail="esphome_settings must be an object.")
        esphome_result = _save_esphome_settings_values(dict(raw_esphome_settings or {}))
        changed_esphome_keys = [
            str(key or "")
            for key in (esphome_result.get("changed_keys") or [])
            if str(key or "")
        ]
        openwakeword_warmup_keys = {
            "VOICE_OPENWAKEWORD_ENABLED",
            "VOICE_OPENWAKEWORD_MODEL_SOURCE",
            "VOICE_OPENWAKEWORD_INFERENCE_FRAMEWORK",
            "VOICE_OPENWAKEWORD_DEVICE",
            "VOICE_OPENWAKEWORD_THRESHOLD",
            "VOICE_OPENWAKEWORD_PATIENCE",
            "VOICE_OPENWAKEWORD_DEBOUNCE_S",
            "VOICE_OPENWAKEWORD_VAD_THRESHOLD",
        }
        nanowakeword_warmup_keys = {
            "VOICE_NANOWAKEWORD_ENABLED",
            "VOICE_NANOWAKEWORD_MODEL_SOURCE",
            "VOICE_NANOWAKEWORD_DEVICE",
            "VOICE_NANOWAKEWORD_THRESHOLD",
            "VOICE_NANOWAKEWORD_PATIENCE",
            "VOICE_NANOWAKEWORD_DEBOUNCE_S",
        }
        if any(key in openwakeword_warmup_keys or key in nanowakeword_warmup_keys for key in changed_esphome_keys):
            speech_warmup_result = _start_speech_model_warmup(
                get_shared_speech_settings(),
                reason="wakeword-settings-save",
            )

    emoji_keys = {
        "emoji_enable_on_reaction_add",
        "emoji_enable_auto_reaction_on_reply",
        "emoji_reaction_chain_chance_percent",
        "emoji_reply_reaction_chance_percent",
        "emoji_reaction_chain_cooldown_seconds",
        "emoji_reply_reaction_cooldown_seconds",
        "emoji_min_message_length",
    }
    if any(key in updates for key in emoji_keys):
        current_emoji = get_core_emoji_settings() or {}
        save_core_emoji_settings(
            {
                "enable_on_reaction_add": bool(
                    updates.get("emoji_enable_on_reaction_add", current_emoji.get("enable_on_reaction_add", True))
                ),
                "enable_auto_reaction_on_reply": bool(
                    updates.get(
                        "emoji_enable_auto_reaction_on_reply",
                        current_emoji.get("enable_auto_reaction_on_reply", True),
                    )
                ),
                "reaction_chain_chance_percent": _bounded_int(
                    updates.get(
                        "emoji_reaction_chain_chance_percent",
                        current_emoji.get("reaction_chain_chance_percent", 100),
                    ),
                    default=100,
                    min_value=0,
                    max_value=100,
                ),
                "reply_reaction_chance_percent": _bounded_int(
                    updates.get(
                        "emoji_reply_reaction_chance_percent",
                        current_emoji.get("reply_reaction_chance_percent", 12),
                    ),
                    default=12,
                    min_value=0,
                    max_value=100,
                ),
                "reaction_chain_cooldown_seconds": _bounded_int(
                    updates.get(
                        "emoji_reaction_chain_cooldown_seconds",
                        current_emoji.get("reaction_chain_cooldown_seconds", 30),
                    ),
                    default=30,
                    min_value=0,
                    max_value=86_400,
                ),
                "reply_reaction_cooldown_seconds": _bounded_int(
                    updates.get(
                        "emoji_reply_reaction_cooldown_seconds",
                        current_emoji.get("reply_reaction_cooldown_seconds", 120),
                    ),
                    default=120,
                    min_value=0,
                    max_value=86_400,
                ),
                "min_message_length": _bounded_int(
                    updates.get("emoji_min_message_length", current_emoji.get("min_message_length", 4)),
                    default=4,
                    min_value=0,
                    max_value=200,
                ),
            }
        )

    current_llm_provider = _normalize_hydra_llm_provider(redis_client.get(HYDRA_LLM_PROVIDER_KEY))
    current_llm_host = str(redis_client.get(HYDRA_LLM_HOST_KEY) or "").strip()
    current_llm_port = str(redis_client.get(HYDRA_LLM_PORT_KEY) or "").strip()
    current_llm_model = str(redis_client.get(HYDRA_LLM_MODEL_KEY) or "").strip()
    current_base_rows = resolve_hydra_base_servers(redis_conn=redis_client, include_legacy=True)
    if current_base_rows:
        current_llm_provider = _normalize_hydra_llm_provider(current_base_rows[0].get("provider"))
    current_llm_api_key = str((current_base_rows[0] if current_base_rows else {}).get("api_key") or "").strip()

    if "hydra_llm_provider" in updates:
        current_llm_provider = _normalize_hydra_llm_provider(updates.get("hydra_llm_provider"))
    if "hydra_llm_host" in updates:
        current_llm_host = str(updates.get("hydra_llm_host") or "").strip()
    if "hydra_llm_port" in updates:
        current_llm_port = str(updates.get("hydra_llm_port") or "").strip()
    if "hydra_llm_model" in updates:
        current_llm_model = str(updates.get("hydra_llm_model") or "").strip()
    if "hydra_llm_api_key" in updates:
        current_llm_api_key = str(updates.get("hydra_llm_api_key") or "").strip()

    if current_llm_port:
        if not str(current_llm_port).isdigit():
            raise HTTPException(status_code=400, detail="hydra_llm_port must be an integer between 1 and 65535")
        parsed_port = int(current_llm_port)
        if parsed_port < 1 or parsed_port > 65535:
            raise HTTPException(status_code=400, detail="hydra_llm_port must be an integer between 1 and 65535")
        current_llm_port = str(parsed_port)

    base_settings_keys = {
        "hydra_llm_host",
        "hydra_llm_port",
        "hydra_llm_model",
        "hydra_llm_api_key",
        "hydra_llm_provider",
        "hydra_base_servers",
    }
    if any(key in updates for key in base_settings_keys):
        normalized_base_rows: List[Dict[str, str]] = []
        if "hydra_base_servers" in updates:
            normalized_base_rows = _normalize_hydra_base_server_rows(updates.get("hydra_base_servers"))
        elif _is_local_hydra_llm_provider(current_llm_provider) and current_llm_model:
            normalized_base_rows = [
                {
                    "provider": current_llm_provider,
                    "host": "",
                    "port": "",
                    "model": current_llm_model,
                    "api_key": "",
                }
            ]
        elif current_llm_host and current_llm_model:
            endpoint = _build_hydra_llm_endpoint(current_llm_host, current_llm_port)
            if endpoint:
                parsed = urlparse(endpoint)
                hostname = str(parsed.hostname or "").strip()
                if hostname:
                    host_with_scheme = current_llm_host.startswith(("http://", "https://"))
                    normalized_base_rows = [
                        {
                            "provider": current_llm_provider,
                            "host": f"{parsed.scheme}://{hostname}" if host_with_scheme else hostname,
                            "port": str(parsed.port) if parsed.port is not None else "",
                            "model": current_llm_model,
                            "api_key": current_llm_api_key,
                        }
                    ]

        for row in normalized_base_rows:
            row_provider = _normalize_hydra_llm_provider(row.get("provider"))
            row_model = str(row.get("model") or "").strip()
            row["model"] = _require_downloaded_local_model(row_provider, row_model, "Base")

        if normalized_base_rows:
            redis_client.set(HYDRA_LLM_BASE_SERVERS_KEY, json.dumps(normalized_base_rows))
        else:
            redis_client.delete(HYDRA_LLM_BASE_SERVERS_KEY)
        _set_hydra_legacy_base_keys(normalized_base_rows)
        redis_client.delete(HYDRA_LLM_RECOVERY_NOTICE_KEY)
        hf_models = _hf_llm_warmup_models(normalized_base_rows)
        if hf_models and not explicit_local_model_load_targets and _hf_llm_warmup_on_save_enabled():
            hf_llm_warmup_result = _start_hf_llm_warmup(hf_models, reason="settings-save")

    # Remove deprecated legacy key if it exists.
    redis_client.delete("tater:hydra:llm_url")

    beast_mode_enabled_value: Optional[bool] = None
    if "hydra_beast_mode_enabled" in updates:
        beast_mode_enabled_value = _as_bool_flag(
            updates.get("hydra_beast_mode_enabled"),
            default=False,
        )
        redis_client.set(
            HYDRA_BEAST_MODE_ENABLED_KEY,
            "true" if beast_mode_enabled_value else "false",
        )

    for role in HYDRA_BEAST_CONFIG_ROLE_IDS:
        role_payload_keys = {f"hydra_llm_{role}_{field}" for field in ("provider", "host", "port", "model", "api_key")}
        if not any(key in updates for key in role_payload_keys):
            continue

        provider_payload_key = f"hydra_llm_{role}_provider"
        raw_provider = updates.get(provider_payload_key, redis_client.get(_hydra_role_llm_key(role, "provider")) or "")
        role_provider = _normalize_hydra_llm_provider(raw_provider)
        role_host = str(updates.get(f"hydra_llm_{role}_host", redis_client.get(_hydra_role_llm_key(role, "host")) or "") or "").strip()
        role_port = str(updates.get(f"hydra_llm_{role}_port", redis_client.get(_hydra_role_llm_key(role, "port")) or "") or "").strip()
        role_model = str(updates.get(f"hydra_llm_{role}_model", redis_client.get(_hydra_role_llm_key(role, "model")) or "") or "").strip()
        role_api_key = str(updates.get(f"hydra_llm_{role}_api_key", redis_client.get(_hydra_role_llm_key(role, "api_key")) or "") or "").strip()

        if _is_local_hydra_llm_provider(role_provider):
            role_model = _require_downloaded_local_model(role_provider, role_model, role.title())
            role_host = ""
            role_port = ""
            role_api_key = ""
        elif role_port:
            if not role_port.isdigit():
                raise HTTPException(
                    status_code=400,
                    detail=f"hydra_llm_{role}_port must be an integer between 1 and 65535",
                )
            port_int = int(role_port)
            if port_int < 1 or port_int > 65535:
                raise HTTPException(
                    status_code=400,
                    detail=f"hydra_llm_{role}_port must be an integer between 1 and 65535",
                )
            role_port = str(port_int)

        role_values = {
            "provider": role_provider,
            "host": role_host,
            "port": role_port,
            "model": role_model,
            "api_key": role_api_key,
        }
        for field, raw_value in role_values.items():
            redis_key = _hydra_role_llm_key(role, field)
            if raw_value:
                redis_client.set(redis_key, raw_value)
            else:
                redis_client.delete(redis_key)

    if explicit_local_model_load_targets and local_model_load_targets:
        hf_llm_warmup_result = _start_hf_llm_warmup(
            local_model_load_targets,
            reason="settings-save-load",
            load_models=True,
            unload_before=local_model_unload_targets,
        )

    hydra_mappings = {
        "hydra_max_ledger_items": (HYDRA_MAX_LEDGER_ITEMS_KEY, DEFAULT_MAX_LEDGER_ITEMS, 1, None),
    }
    for payload_key, (redis_key, default, min_value, max_value) in hydra_mappings.items():
        if payload_key not in updates:
            continue
        normalized = _bounded_int(
            updates.get(payload_key),
            default=int(default),
            min_value=int(min_value),
            max_value=max_value,
        )
        redis_client.set(redis_key, int(normalized))

    if "hydra_astraeus_plan_review_enabled" in updates:
        enabled_value = _as_bool_flag(
            updates.get("hydra_astraeus_plan_review_enabled"),
            default=DEFAULT_ASTRAEUS_PLAN_REVIEW_ENABLED,
        )
        redis_client.set(
            HYDRA_ASTRAEUS_PLAN_REVIEW_ENABLED_KEY,
            "true" if enabled_value else "false",
        )

    if "hydra_auto_continue_incomplete_final_enabled" in updates:
        enabled_value = _as_bool_flag(
            updates.get("hydra_auto_continue_incomplete_final_enabled"),
            default=DEFAULT_AUTO_CONTINUE_INCOMPLETE_FINAL_ENABLED,
        )
        redis_client.set(
            HYDRA_AUTO_CONTINUE_INCOMPLETE_FINAL_ENABLED_KEY,
            "true" if enabled_value else "false",
        )

    if "admin_only_plugins" in updates:
        values = [str(item).strip().lower() for item in (updates.get("admin_only_plugins") or []) if str(item).strip()]
        cleaned = sorted(set(values))
        redis_client.set(ADMIN_GATE_KEY, json.dumps(cleaned))

    return {
        "ok": True,
        "updated": sorted(updates.keys()),
        "esphome": esphome_result,
        "tts_reload": tts_reload_result,
        "speech_warmup": speech_warmup_result or _speech_model_warmup_snapshot(),
        "hf_llm_warmup": hf_llm_warmup_result or _hf_llm_warmup_snapshot(),
    }
