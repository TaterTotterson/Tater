import asyncio
import base64
import importlib
import json
import logging
import os
import queue
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import dotenv
import redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from redis.exceptions import RedisError

import core_registry as core_registry_module
import verba_registry as verba_registry_module
import portal_registry as portal_registry_module
from admin_gate import DEFAULT_ADMIN_ONLY_PLUGINS, REDIS_KEY as ADMIN_GATE_KEY, get_admin_only_plugins
from hydra import estimate_hydra_chat_context_window, get_active_chat_jobs_snapshot, run_hydra_turn
from hydra import (
    HYDRA_AGENT_STATE_TTL_SECONDS_KEY,
    HYDRA_ASTRAEUS_PLAN_REVIEW_ENABLED_KEY,
    HYDRA_BEAST_CONFIG_ROLE_IDS,
    HYDRA_BEAST_MODE_ENABLED_KEY,
    HYDRA_LLM_HOST_KEY,
    HYDRA_LLM_MODEL_KEY,
    HYDRA_LLM_PORT_KEY,
    HYDRA_MAX_LEDGER_ITEMS_KEY,
    HYDRA_ROLE_LLM_KEY_PREFIX,
    HYDRA_STEP_RETRY_LIMIT_KEY,
    DEFAULT_AGENT_STATE_TTL_SECONDS,
    DEFAULT_ASTRAEUS_PLAN_REVIEW_ENABLED,
    DEFAULT_MAX_LEDGER_ITEMS,
    DEFAULT_STEP_RETRY_LIMIT,
)
from emoji_responder import get_emoji_settings as get_core_emoji_settings, save_emoji_settings as save_core_emoji_settings
from helpers import (
    HYDRA_LLM_BASE_SERVERS_KEY,
    get_llm_call_runtime_summary,
    get_llm_client_from_env,
    resolve_hydra_base_servers,
    set_main_loop,
)
from verba_settings import (
    get_verba_enabled,
    get_verba_settings,
    save_verba_settings as save_verba_settings_values,
    set_verba_enabled as set_verba_enabled_flag,
)
from verba_kernel import normalize_platform
from vision_settings import get_vision_settings as get_shared_vision_settings, save_vision_settings as save_shared_vision_settings
from tateros import core_store as core_store_module
from tateros import verba_store as verba_store_module
from tateros import portal_store as portal_store_module


dotenv.load_dotenv()

logger = logging.getLogger("tateros")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)-20s %(message)s",
)

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True,
)

redis_blob_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=False,
)

CHAT_HISTORY_KEY = "webui:chat_history"
DEFAULT_MAX_STORE = 20
DEFAULT_MAX_DISPLAY = 8
DEFAULT_MAX_LLM = 8
DEFAULT_TATER_AVATAR_PATH = Path(__file__).resolve().parent / "images" / "tater.png"
WEBUI_ATTACH_MAX_MB_EACH = int(os.getenv("WEBUI_ATTACH_MAX_MB_EACH", "25"))
WEBUI_ATTACH_MAX_MB_TOTAL = int(os.getenv("WEBUI_ATTACH_MAX_MB_TOTAL", "50"))
WEBUI_ATTACH_TTL_SECONDS = int(os.getenv("WEBUI_ATTACH_TTL_SECONDS", "0"))
WEBUI_ATTACH_INDEX_MAX = int(os.getenv("WEBUI_ATTACH_INDEX_MAX", "500"))
FILE_BLOB_KEY_PREFIX = "webui:file:"
FILE_INDEX_KEY = "webui:file_index"
LAST_LLM_STATS_KEY = "webui:last_llm_stats"
WEBUI_POPUP_EFFECT_STYLE_KEY = "tater:webui:popup_effect_style"
DEFAULT_WEBUI_POPUP_EFFECT_STYLE = "flame"
WEBUI_POPUP_EFFECT_STYLE_CHOICES = {"disabled", "flame", "dust", "glitch", "portal", "melt"}
RUNTIME_CONTEXT_ESTIMATE_TTL_SECONDS = 20
runtime_context_estimate_cache: Dict[str, Any] = {"updated_at": 0.0, "payload": {}}


bootstrap_state: Dict[str, Any] = {
    "restore_enabled": True,
    "restore_in_progress": False,
    "restore_complete": False,
    "restore_error": "",
    "restore_summary": {},
    "autostart_enabled": True,
    "redis_migration": {},
}


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

        thread.start()
        return {"started": True, "running": True, "reason": "started"}

    def stop(self, module_key: str, *, timeout: float = 3.0) -> Dict[str, Any]:
        key = str(module_key or "").strip()
        if not key:
            raise ValueError(f"Missing {self.kind} module key")

        with self.lock:
            thread = self.threads.get(key)
            stop_flag = self.stop_flags.get(key)

        if stop_flag:
            stop_flag.set()

        if thread and thread.is_alive():
            thread.join(timeout=timeout)

        running = bool(thread and thread.is_alive())
        with self.lock:
            if not running:
                self.threads.pop(key, None)
                self.stop_flags.pop(key, None)

        return {
            "stopped": not running,
            "running": running,
            "reason": "stop-timeout" if running else "stopped",
        }

    def stop_all(self, *, timeout: float = 2.0) -> None:
        with self.lock:
            keys = list(self.threads.keys())

        for key in keys:
            try:
                self.stop(key, timeout=timeout)
            except Exception:  # pragma: no cover - best effort during shutdown
                logger.exception("[%s] failed stopping %s", self.kind, key)


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
                "value": raw_value,
                "default": default_value,
            }
        )
    return fields


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


def _normalize_hydra_base_server_rows(rows: Any) -> List[Dict[str, str]]:
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="hydra_base_servers must be a list")

    normalized: List[Dict[str, str]] = []
    seen: set[Tuple[str, str]] = set()

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            raise HTTPException(status_code=400, detail=f"hydra_base_servers[{idx}] must be an object")

        host = str(row.get("host") or "").strip()
        port = str(row.get("port") or "").strip()
        model = str(row.get("model") or "").strip()

        if not host and not port and not model:
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
        signature = (endpoint, model)
        if signature in seen:
            continue
        seen.add(signature)
        normalized.append(
            {
                "host": canonical_host,
                "port": canonical_port,
                "model": model,
            }
        )

    return normalized


def _set_hydra_legacy_base_keys(base_rows: List[Dict[str, str]]) -> None:
    rows = [row for row in (base_rows or []) if isinstance(row, dict)]
    if not rows:
        redis_client.delete(HYDRA_LLM_HOST_KEY)
        redis_client.delete(HYDRA_LLM_PORT_KEY)
        redis_client.delete(HYDRA_LLM_MODEL_KEY)
        return

    first = rows[0]
    first_host = str(first.get("host") or "").strip()
    first_port = str(first.get("port") or "").strip()
    first_model = str(first.get("model") or "").strip()

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


def _discover_core_webui_tabs(core_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    discovered: List[Dict[str, Any]] = []
    seen_labels = set()

    for core in core_entries or []:
        if not isinstance(core, dict):
            continue

        key = str(core.get("key") or "").strip()
        if not key:
            continue
        if not _as_bool_flag(core.get("has_webui_tab_renderer"), False):
            continue

        tab_cfg = core.get("webui_tab") if isinstance(core.get("webui_tab"), dict) else {}
        label = str(tab_cfg.get("label") or "").strip()
        if not label:
            continue
        if label in seen_labels:
            continue

        requires_running = _as_bool_flag(tab_cfg.get("requires_running"), True)
        desired_running = str(redis_client.get(f"{key}_running") or "").strip().lower() == "true"
        if requires_running and not desired_running:
            continue

        try:
            order = int(tab_cfg.get("order", 1000))
        except Exception:
            order = 1000

        discovered.append(
            {
                "label": label,
                "core_key": key,
                "order": order,
                "requires_running": requires_running,
                "running": bool(core_runtime.is_running(key)),
            }
        )
        seen_labels.add(label)

    discovered.sort(key=lambda row: (int(row.get("order", 1000)), str(row.get("label") or "").lower()))
    return discovered


def _load_core_htmlui_tab_payload(tab_spec: Dict[str, Any]) -> Dict[str, Any]:
    key = str((tab_spec or {}).get("core_key") or "").strip()
    if not key:
        return {"error": "Missing core key for tab."}

    try:
        module = core_runtime._import_module(key, reload_module=False)
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


def _load_chat_history_tail(count: int) -> List[Dict[str, Any]]:
    if count <= 0:
        return []
    raw = redis_client.lrange(CHAT_HISTORY_KEY, -count, -1)
    out: List[Dict[str, Any]] = []
    for line in raw:
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                out.append(parsed)
        except Exception:
            continue
    return out


def _load_chat_history() -> List[Dict[str, Any]]:
    raw = redis_client.lrange(CHAT_HISTORY_KEY, 0, -1)
    out: List[Dict[str, Any]] = []
    for line in raw:
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                out.append(parsed)
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
        "prompt_tokens": str(int(stats.get("prompt_tokens") or 0)),
        "completion_tokens": str(int(stats.get("completion_tokens") or 0)),
        "total_tokens": str(int(stats.get("total_tokens") or 0)),
        "tps_total": str(float(stats.get("tps_total") or 0.0)),
        "tps_comp": str(float(stats.get("tps_comp") or 0.0)),
        "calls": str(int(stats.get("calls") or 0)),
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
        "prompt_tokens": max(0, _as_int(raw.get("prompt_tokens"))),
        "completion_tokens": max(0, _as_int(raw.get("completion_tokens"))),
        "total_tokens": max(0, _as_int(raw.get("total_tokens"))),
        "tps_total": max(0.0, _as_float(raw.get("tps_total"))),
        "tps_comp": max(0.0, _as_float(raw.get("tps_comp"))),
        "calls": max(0, _as_int(raw.get("calls"))),
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
    per_file_limit = max(1, int(WEBUI_ATTACH_MAX_MB_EACH)) * 1024 * 1024
    total_limit = max(1, int(WEBUI_ATTACH_MAX_MB_TOTAL)) * 1024 * 1024

    try:
        for idx, item in enumerate(attachments_raw):
            if not isinstance(item, dict):
                raise ValueError(f"attachment #{idx + 1} is invalid")

            payload_raw = item.get("data_url")
            if payload_raw is None:
                payload_raw = item.get("data_b64")
            raw = _decode_attachment_data(payload_raw)

            size = len(raw)
            if size > per_file_limit:
                raise ValueError(
                    f"attachment '{str(item.get('name') or f'attachment-{idx + 1}').strip()}' "
                    f"exceeds {WEBUI_ATTACH_MAX_MB_EACH}MB per-file limit"
                )
            total_bytes += size
            if total_bytes > total_limit:
                raise ValueError(f"combined attachment size exceeds {WEBUI_ATTACH_MAX_MB_TOTAL}MB limit")

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

    media_type = str(item.get("type") or "").strip().lower()
    if media_type not in {"image", "audio", "video", "file"}:
        return item

    raw = None
    if isinstance(item.get("data"), (bytes, bytearray)):
        raw = bytes(item.get("data"))
    elif isinstance(item.get("bytes"), (bytes, bytearray)):
        raw = bytes(item.get("bytes"))

    if raw is None:
        return item

    safe = dict(item)
    safe.pop("data", None)
    safe.pop("bytes", None)
    safe["data_b64"] = base64.b64encode(raw).decode("utf-8")
    safe["size"] = len(raw)
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

    return {"responses": responses, "agent": True}


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
            self._emit(job, {"type": "status", "status": "running", "job_id": job_id})

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

            for item in responses:
                _save_chat_message("assistant", "assistant", item)

            with self.lock:
                job = self.jobs.get(job_id)
                if not isinstance(job, dict):
                    return
                self._set_status_locked(job, status="done")
                job["responses"] = responses
                job["completed_at"] = time.time()
                self._emit(
                    job,
                    {
                        "type": "done",
                        "status": "done",
                        "responses": responses,
                        "job_id": job_id,
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
            self._emit(job, {"type": "status", "status": "queued", "job_id": job_id})

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


def _merge_hash_fields_without_overwrite(target_key: str, source: Dict[str, Any]) -> tuple[int, int]:
    existing = redis_client.hgetall(target_key) or {}
    to_write: Dict[str, str] = {}
    skipped_existing = 0

    for raw_field, raw_value in (source or {}).items():
        field = str(raw_field or "").strip()
        if not field:
            continue
        if field in existing:
            skipped_existing += 1
            continue
        to_write[field] = str(raw_value) if raw_value is not None else ""

    if to_write:
        redis_client.hset(target_key, mapping=to_write)
    return len(to_write), skipped_existing


def _history_keys_for_legacy_marker_migration() -> List[str]:
    keys = {CHAT_HISTORY_KEY}
    for pattern in ("tater:channel:*:history", "tater:telegram:*:history", "tater:matrix:*:history"):
        for raw_key in redis_client.scan_iter(match=pattern):
            key = str(raw_key or "").strip()
            if key:
                keys.add(key)
    return sorted(keys)


def _migrate_legacy_plugin_history_markers() -> Dict[str, Any]:
    """
    One-way chat history migration:
    - Drops legacy transient plugin wrappers: plugin_call, plugin_wait
    - Unwraps plugin_response payloads into plain assistant content

    This keeps conversation history readable after plugin -> verba rename and
    avoids feeding wrapper-only tool markers back into Hydra context.
    """
    summary: Dict[str, Any] = {
        "changed": False,
        "history_keys_scanned": 0,
        "history_keys_migrated": 0,
        "history_entries_scanned": 0,
        "history_entries_removed": 0,
        "history_entries_unwrapped": 0,
    }

    history_keys = _history_keys_for_legacy_marker_migration()
    summary["history_keys_scanned"] = len(history_keys)

    for key in history_keys:
        try:
            rows = list(redis_client.lrange(key, 0, -1) or [])
        except Exception:
            continue
        if not rows:
            continue

        migrated_rows: List[str] = []
        key_changed = False

        for raw_line in rows:
            line = str(raw_line or "")
            summary["history_entries_scanned"] += 1

            try:
                parsed = json.loads(line)
            except Exception:
                migrated_rows.append(line)
                continue

            if not isinstance(parsed, dict):
                migrated_rows.append(line)
                continue

            content = parsed.get("content")
            if not isinstance(content, dict):
                migrated_rows.append(line)
                continue

            marker = str(content.get("marker") or "").strip().lower()
            if marker not in {"plugin_call", "plugin_wait", "plugin_response"}:
                migrated_rows.append(line)
                continue

            key_changed = True

            if marker in {"plugin_call", "plugin_wait"}:
                summary["history_entries_removed"] += 1
                continue

            payload = content.get("content")
            if payload is None or (isinstance(payload, str) and not payload.strip()):
                summary["history_entries_removed"] += 1
                continue

            migrated = dict(parsed)
            migrated["content"] = payload
            migrated_rows.append(json.dumps(migrated, ensure_ascii=False))
            summary["history_entries_unwrapped"] += 1

        if not key_changed:
            continue

        pipe = redis_client.pipeline()
        pipe.delete(key)
        if migrated_rows:
            pipe.rpush(key, *migrated_rows)
        pipe.execute()
        summary["history_keys_migrated"] += 1
        summary["changed"] = True

    return summary


def _migrate_legacy_plugin_redis_keys() -> Dict[str, Any]:
    """
    One-way Redis key migration:
    - plugin_enabled -> verba_enabled
    - plugin_settings:* -> verba_settings:*

    Migration is no-op when legacy keys are absent.
    """
    summary: Dict[str, Any] = {
        "changed": False,
        "legacy_plugin_enabled_fields": 0,
        "verba_enabled_fields_migrated": 0,
        "verba_enabled_fields_skipped_existing": 0,
        "legacy_plugin_settings_keys": 0,
        "verba_settings_fields_migrated": 0,
        "verba_settings_fields_skipped_existing": 0,
        "legacy_keys_deleted": 0,
        "legacy_history_keys_scanned": 0,
        "legacy_history_keys_migrated": 0,
        "legacy_history_entries_scanned": 0,
        "legacy_history_entries_removed": 0,
        "legacy_history_entries_unwrapped": 0,
    }

    old_enabled = redis_client.hgetall("plugin_enabled") or {}
    if old_enabled:
        summary["legacy_plugin_enabled_fields"] = len(old_enabled)
        moved, skipped = _merge_hash_fields_without_overwrite("verba_enabled", old_enabled)
        summary["verba_enabled_fields_migrated"] = moved
        summary["verba_enabled_fields_skipped_existing"] = skipped
        redis_client.delete("plugin_enabled")
        summary["legacy_keys_deleted"] += 1
        summary["changed"] = True

    old_settings_keys = [str(k) for k in redis_client.scan_iter(match="plugin_settings:*")]
    summary["legacy_plugin_settings_keys"] = len(old_settings_keys)
    for old_key in old_settings_keys:
        suffix = old_key[len("plugin_settings:") :]
        new_key = f"verba_settings:{suffix}"
        old_fields = redis_client.hgetall(old_key) or {}
        moved, skipped = _merge_hash_fields_without_overwrite(new_key, old_fields)
        summary["verba_settings_fields_migrated"] += moved
        summary["verba_settings_fields_skipped_existing"] += skipped
        redis_client.delete(old_key)
        summary["legacy_keys_deleted"] += 1
        summary["changed"] = True

    history_summary = _migrate_legacy_plugin_history_markers()
    summary["legacy_history_keys_scanned"] = int(history_summary.get("history_keys_scanned") or 0)
    summary["legacy_history_keys_migrated"] = int(history_summary.get("history_keys_migrated") or 0)
    summary["legacy_history_entries_scanned"] = int(history_summary.get("history_entries_scanned") or 0)
    summary["legacy_history_entries_removed"] = int(history_summary.get("history_entries_removed") or 0)
    summary["legacy_history_entries_unwrapped"] = int(history_summary.get("history_entries_unwrapped") or 0)
    if history_summary.get("changed"):
        summary["changed"] = True

    return summary


def _restore_enabled_surfaces() -> Dict[str, Any]:
    """
    Match legacy startup behavior:
    - restore missing enabled verbas
    - restore missing enabled cores
    - restore missing enabled portals
    """
    summary: Dict[str, Any] = {
        "plugins_missing_before": 0,
        "plugins_missing_after": 0,
        "cores_missing_before": 0,
        "cores_missing_after": 0,
        "portals_missing_before": 0,
        "portals_missing_after": 0,
    }

    # 1) Verbas
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

    # 2) Cores
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

    # 3) Portals
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

    return summary


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
    web_search_google_api_key: Optional[str] = None
    web_search_google_cx: Optional[str] = None
    homeassistant_base_url: Optional[str] = None
    homeassistant_token: Optional[str] = None
    vision_api_base: Optional[str] = None
    vision_model: Optional[str] = None
    vision_api_key: Optional[str] = None
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
    hydra_base_servers: Optional[List[Dict[str, Any]]] = None
    hydra_beast_mode_enabled: Optional[bool] = None
    hydra_llm_chat_host: Optional[str] = None
    hydra_llm_chat_port: Optional[str] = None
    hydra_llm_chat_model: Optional[str] = None
    hydra_llm_astraeus_host: Optional[str] = None
    hydra_llm_astraeus_port: Optional[str] = None
    hydra_llm_astraeus_model: Optional[str] = None
    hydra_llm_thanatos_host: Optional[str] = None
    hydra_llm_thanatos_port: Optional[str] = None
    hydra_llm_thanatos_model: Optional[str] = None
    hydra_llm_minos_host: Optional[str] = None
    hydra_llm_minos_port: Optional[str] = None
    hydra_llm_minos_model: Optional[str] = None
    hydra_llm_hermes_host: Optional[str] = None
    hydra_llm_hermes_port: Optional[str] = None
    hydra_llm_hermes_model: Optional[str] = None
    hydra_agent_state_ttl_seconds: Optional[int] = None
    hydra_max_ledger_items: Optional[int] = None
    hydra_step_retry_limit: Optional[int] = None
    hydra_astraeus_plan_review_enabled: Optional[bool] = None
    popup_effect_style: Optional[str] = None
    admin_only_plugins: Optional[List[str]] = None


class HydraDataClearRequest(BaseModel):
    platform: str = "all"
    mode: str = "all"


app = FastAPI(title="TaterOS", version="0.2.0")

STATIC_DIR = Path(__file__).resolve().parent / "tateros_static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.on_event("startup")
async def _startup_event() -> None:
    set_main_loop(asyncio.get_running_loop())
    verba_registry_module.ensure_verbas_loaded()
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
    bootstrap_state["redis_migration"] = {}

    try:
        migration_summary = _migrate_legacy_plugin_redis_keys()
        bootstrap_state["redis_migration"] = migration_summary
        if migration_summary.get("changed"):
            logger.info("[startup-migrate] migrated legacy plugin redis keys: %s", migration_summary)
        if restore_enabled:
            bootstrap_state["restore_in_progress"] = True
            summary = _restore_enabled_surfaces()
            bootstrap_state["restore_summary"] = summary
            logger.info("[startup-restore] summary: %s", summary)
        else:
            logger.info("[startup-restore] skipped (HTMLUI_RESTORE_ENABLED_SURFACES_ON_STARTUP=false)")
        if autostart_enabled:
            _autostart_enabled_surfaces()
        else:
            logger.info("[startup-autostart] skipped (HTMLUI_AUTOSTART_ENABLED_SURFACES_ON_STARTUP=false)")
    except RedisError as exc:
        bootstrap_state["restore_error"] = str(exc)
        logger.warning("Redis unavailable during startup autostart: %s", exc)
    finally:
        bootstrap_state["restore_in_progress"] = False
        bootstrap_state["restore_complete"] = True
    logger.info("TaterOS backend started")


@app.on_event("shutdown")
async def _shutdown_event() -> None:
    core_runtime.stop_all()
    portal_runtime.stop_all()
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


_RUNTIME_PLATFORM_LABELS: Dict[str, str] = {
    "webui": "WebUI",
    "macos": "macOS",
    "discord": "Discord",
    "irc": "IRC",
    "telegram": "Telegram",
    "matrix": "Matrix",
    "homeassistant": "Home Assistant",
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
    keys: List[str] = []
    try:
        if redis_client.exists("tater:hydra:ledger"):
            keys = ["tater:hydra:ledger"]
    except Exception:
        keys = []
    if not keys:
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


def _runtime_breakdown_payload() -> Dict[str, Any]:
    hydra_jobs = _chat_job_counts_with_breakdown(include_history=True)
    llm_calls = get_llm_call_runtime_summary(include_history=True)
    context_estimate = _estimate_webui_chat_context_window()
    return {
        "hydra_jobs": hydra_jobs,
        "chat_jobs": hydra_jobs,  # Backward-compatible key for older clients.
        "llm_calls": llm_calls,
        "chat_context_window": context_estimate,
    }


@app.get("/api/health")
def health() -> Dict[str, Any]:
    try:
        redis_client.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

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

    return {
        "ok": redis_ok,
        "redis": redis_ok,
        "verbas_enabled": int(verbas_enabled),
        "cores_running": len([k for k in core_runtime.threads if core_runtime.is_running(k)]),
        "portals_running": len([k for k in portal_runtime.threads if portal_runtime.is_running(k)]),
        "hydra_jobs_active": int(hydra_job_counts.get("total") or 0),
        "chat_jobs_active": int(hydra_job_counts.get("total") or 0),  # Backward-compatible key for older clients.
        "llm_calls_active": int(llm_call_counts.get("active_total") or 0),
        "bootstrap": {
            "restore_enabled": bool(bootstrap_state.get("restore_enabled")),
            "autostart_enabled": bool(bootstrap_state.get("autostart_enabled")),
            "restore_in_progress": bool(bootstrap_state.get("restore_in_progress")),
            "restore_complete": bool(bootstrap_state.get("restore_complete")),
            "restore_error": str(bootstrap_state.get("restore_error") or ""),
            "restore_summary": dict(bootstrap_state.get("restore_summary") or {}),
            "redis_migration": dict(bootstrap_state.get("redis_migration") or {}),
        },
    }


@app.get("/api/runtime/breakdown")
def runtime_breakdown() -> Dict[str, Any]:
    payload = _runtime_breakdown_payload()
    return {"ok": True, **payload}


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


@app.get("/api/chat/files/{file_id}")
def chat_file(file_id: str, mimetype: str = "application/octet-stream") -> Response:
    blob = _load_file_blob_from_redis(file_id)
    if blob is None:
        raise HTTPException(status_code=404, detail="Attachment not found or expired.")
    media_type = str(mimetype or "application/octet-stream").strip() or "application/octet-stream"
    return Response(content=blob, media_type=media_type)


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
                event = await asyncio.to_thread(event_queue.get, True, 1.0)
            except queue.Empty:
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
                "settings": _setting_fields(required_settings, current_settings),
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

    values = dict(payload.values or {})
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
    entries = core_registry_module.refresh_core_registry()
    tabs = _discover_core_webui_tabs(entries)
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
                "payload": _load_core_htmlui_tab_payload(tab),
            }
        )

    return {
        "manage_label": "Manage",
        "tabs": dynamic_tabs,
    }


@app.post("/api/cores/{core_key}/tab-action")
def run_core_tab_action(core_key: str, payload: CoreTabActionRequest) -> Dict[str, Any]:
    key = str(core_key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="Missing core key.")

    entries = core_registry_module.refresh_core_registry()
    known = {str(item.get("key") or "").strip() for item in entries}
    if key not in known:
        raise HTTPException(status_code=404, detail=f"Unknown core: {key}")

    try:
        module = core_runtime._import_module(key, reload_module=False)
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
        raise HTTPException(status_code=500, detail=f"Core tab action failed: {exc}")

    if result is None:
        return {"ok": True}
    if not isinstance(result, dict):
        return {"ok": True, "result": result}
    return result


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
                "settings": _setting_fields(portal.get("required", {}), current_settings),
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
    mapping = {
        k: json.dumps(v) if isinstance(v, (dict, list, bool)) else str(v)
        for k, v in (payload.values or {}).items()
    }
    if mapping:
        redis_client.hset(f"{portal_key}_settings", mapping=mapping)
    return {"key": portal_key, "saved": True}


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
            if redis_client.exists("tater:hydra:ledger"):
                keys.append("tater:hydra:ledger")
        except Exception:
            pass
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
        if key == "tater:hydra:ledger":
            platform = "legacy"
        else:
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


@app.get("/api/settings")
def get_settings() -> Dict[str, Any]:
    chat_settings = redis_client.hgetall("chat_settings") or {}
    legacy_web_search = redis_client.hgetall("verba_settings:Web Search") or {}
    homeassistant_settings = redis_client.hgetall("homeassistant_settings") or {}

    vision_settings = get_shared_vision_settings(
        default_api_base="http://127.0.0.1:1234",
        default_model="qwen2.5-vl-7b-instruct",
    )
    emoji_settings = get_core_emoji_settings() or {}

    verba_registry_module.ensure_verbas_loaded()
    registry_snapshot = verba_registry_module.get_verba_registry_snapshot()
    admin_plugin_options = sorted(str(plugin_id or "").strip() for plugin_id in registry_snapshot.keys() if str(plugin_id or "").strip())
    admin_only_plugins = sorted(get_admin_only_plugins(redis_client))

    hydra_base_servers_raw = resolve_hydra_base_servers(redis_conn=redis_client, include_legacy=True)
    hydra_base_servers: List[Dict[str, str]] = [
        {
            "host": str(row.get("host") or "").strip(),
            "port": str(row.get("port") or "").strip(),
            "model": str(row.get("model") or "").strip(),
        }
        for row in hydra_base_servers_raw
        if isinstance(row, dict)
    ]
    first_hydra_base = hydra_base_servers[0] if hydra_base_servers else {}
    hydra_llm_host = str(first_hydra_base.get("host") or redis_client.get(HYDRA_LLM_HOST_KEY) or "").strip()
    hydra_llm_port = str(first_hydra_base.get("port") or redis_client.get(HYDRA_LLM_PORT_KEY) or "").strip()
    hydra_llm_model = str(first_hydra_base.get("model") or redis_client.get(HYDRA_LLM_MODEL_KEY) or "").strip()
    hydra_beast_mode_enabled = _as_bool_flag(redis_client.get(HYDRA_BEAST_MODE_ENABLED_KEY), default=False)
    hydra_role_model_values: Dict[str, str] = {}
    for role in HYDRA_BEAST_CONFIG_ROLE_IDS:
        role_host = str(redis_client.get(_hydra_role_llm_key(role, "host")) or "").strip()
        role_port = str(redis_client.get(_hydra_role_llm_key(role, "port")) or "").strip()
        role_model = str(redis_client.get(_hydra_role_llm_key(role, "model")) or "").strip()
        hydra_role_model_values[f"hydra_llm_{role}_host"] = role_host
        hydra_role_model_values[f"hydra_llm_{role}_port"] = role_port
        hydra_role_model_values[f"hydra_llm_{role}_model"] = role_model

    hydra_defaults = {
        "hydra_llm_host": "",
        "hydra_llm_port": "",
        "hydra_llm_model": "",
        "hydra_beast_mode_enabled": False,
        "hydra_agent_state_ttl_seconds": int(DEFAULT_AGENT_STATE_TTL_SECONDS),
        "hydra_max_ledger_items": int(DEFAULT_MAX_LEDGER_ITEMS),
        "hydra_step_retry_limit": int(DEFAULT_STEP_RETRY_LIMIT),
        "hydra_astraeus_plan_review_enabled": bool(DEFAULT_ASTRAEUS_PLAN_REVIEW_ENABLED),
    }

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
        "web_search_google_api_key": redis_client.get("tater:web_search:google_api_key")
        or legacy_web_search.get("GOOGLE_API_KEY")
        or "",
        "web_search_google_cx": redis_client.get("tater:web_search:google_cx")
        or legacy_web_search.get("GOOGLE_CX")
        or "",
        "homeassistant_base_url": homeassistant_settings.get("HA_BASE_URL", "http://homeassistant.local:8123"),
        "homeassistant_token": homeassistant_settings.get("HA_TOKEN", ""),
        "vision_api_base": str(vision_settings.get("api_base") or "http://127.0.0.1:1234"),
        "vision_model": str(vision_settings.get("model") or "qwen2.5-vl-7b-instruct"),
        "vision_api_key": str(vision_settings.get("api_key") or ""),
        "emoji_enable_on_reaction_add": bool(emoji_settings.get("enable_on_reaction_add", True)),
        "emoji_enable_auto_reaction_on_reply": bool(emoji_settings.get("enable_auto_reaction_on_reply", True)),
        "emoji_reaction_chain_chance_percent": int(emoji_settings.get("reaction_chain_chance_percent", 100)),
        "emoji_reply_reaction_chance_percent": int(emoji_settings.get("reply_reaction_chance_percent", 12)),
        "emoji_reaction_chain_cooldown_seconds": int(emoji_settings.get("reaction_chain_cooldown_seconds", 30)),
        "emoji_reply_reaction_cooldown_seconds": int(emoji_settings.get("reply_reaction_cooldown_seconds", 120)),
        "emoji_min_message_length": int(emoji_settings.get("min_message_length", 4)),
        "hydra_llm_host": hydra_llm_host,
        "hydra_llm_port": hydra_llm_port,
        "hydra_llm_model": hydra_llm_model,
        "hydra_base_servers": hydra_base_servers,
        "hydra_beast_mode_enabled": hydra_beast_mode_enabled,
        "hydra_agent_state_ttl_seconds": _read_non_negative_int(
            HYDRA_AGENT_STATE_TTL_SECONDS_KEY,
            DEFAULT_AGENT_STATE_TTL_SECONDS,
        ),
        "hydra_max_ledger_items": _read_positive_int(HYDRA_MAX_LEDGER_ITEMS_KEY, DEFAULT_MAX_LEDGER_ITEMS),
        "hydra_step_retry_limit": _read_positive_int(HYDRA_STEP_RETRY_LIMIT_KEY, DEFAULT_STEP_RETRY_LIMIT),
        "hydra_astraeus_plan_review_enabled": _as_bool_flag(
            redis_client.get(HYDRA_ASTRAEUS_PLAN_REVIEW_ENABLED_KEY),
            default=DEFAULT_ASTRAEUS_PLAN_REVIEW_ENABLED,
        ),
        **hydra_role_model_values,
        "hydra_defaults": hydra_defaults,
        "admin_plugin_options": admin_plugin_options,
        "admin_only_plugins": admin_only_plugins,
        "admin_only_plugins_defaults": sorted(DEFAULT_ADMIN_ONLY_PLUGINS),
    }


@app.post("/api/settings")
def update_settings(payload: AppSettingsRequest) -> Dict[str, Any]:
    updates = payload.model_dump(exclude_none=True)

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

    username = updates.get("username")
    if isinstance(username, str):
        redis_client.hset("chat_settings", "username", username.strip() or "User")

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

    if "web_search_google_api_key" in updates:
        redis_client.set("tater:web_search:google_api_key", str(updates["web_search_google_api_key"]).strip())

    if "web_search_google_cx" in updates:
        redis_client.set("tater:web_search:google_cx", str(updates["web_search_google_cx"]).strip())

    if "homeassistant_base_url" in updates or "homeassistant_token" in updates:
        current_ha = redis_client.hgetall("homeassistant_settings") or {}
        base_url = str(updates.get("homeassistant_base_url", current_ha.get("HA_BASE_URL") or "")).strip()
        token = str(updates.get("homeassistant_token", current_ha.get("HA_TOKEN") or "")).strip()
        redis_client.hset(
            "homeassistant_settings",
            mapping={
                "HA_BASE_URL": base_url or "http://homeassistant.local:8123",
                "HA_TOKEN": token,
            },
        )

    if "vision_api_base" in updates or "vision_model" in updates or "vision_api_key" in updates:
        current_vision = get_shared_vision_settings(
            default_api_base="http://127.0.0.1:1234",
            default_model="qwen2.5-vl-7b-instruct",
        )
        save_shared_vision_settings(
            api_base=str(updates.get("vision_api_base", current_vision.get("api_base") or "")).strip(),
            model=str(updates.get("vision_model", current_vision.get("model") or "")).strip(),
            api_key=str(updates.get("vision_api_key", current_vision.get("api_key") or "")).strip(),
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

    current_llm_host = str(redis_client.get(HYDRA_LLM_HOST_KEY) or "").strip()
    current_llm_port = str(redis_client.get(HYDRA_LLM_PORT_KEY) or "").strip()
    current_llm_model = str(redis_client.get(HYDRA_LLM_MODEL_KEY) or "").strip()

    if "hydra_llm_host" in updates:
        current_llm_host = str(updates.get("hydra_llm_host") or "").strip()
    if "hydra_llm_port" in updates:
        current_llm_port = str(updates.get("hydra_llm_port") or "").strip()
    if "hydra_llm_model" in updates:
        current_llm_model = str(updates.get("hydra_llm_model") or "").strip()

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
        "hydra_base_servers",
    }
    if any(key in updates for key in base_settings_keys):
        normalized_base_rows: List[Dict[str, str]] = []
        if "hydra_base_servers" in updates:
            normalized_base_rows = _normalize_hydra_base_server_rows(updates.get("hydra_base_servers"))
        elif current_llm_host and current_llm_model:
            endpoint = _build_hydra_llm_endpoint(current_llm_host, current_llm_port)
            if endpoint:
                parsed = urlparse(endpoint)
                hostname = str(parsed.hostname or "").strip()
                if hostname:
                    host_with_scheme = current_llm_host.startswith(("http://", "https://"))
                    normalized_base_rows = [
                        {
                            "host": f"{parsed.scheme}://{hostname}" if host_with_scheme else hostname,
                            "port": str(parsed.port) if parsed.port is not None else "",
                            "model": current_llm_model,
                        }
                    ]

        if normalized_base_rows:
            redis_client.set(HYDRA_LLM_BASE_SERVERS_KEY, json.dumps(normalized_base_rows))
        else:
            redis_client.delete(HYDRA_LLM_BASE_SERVERS_KEY)
        _set_hydra_legacy_base_keys(normalized_base_rows)

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
        for field in ("host", "port", "model"):
            payload_key = f"hydra_llm_{role}_{field}"
            if payload_key not in updates:
                continue
            raw_value = str(updates.get(payload_key) or "").strip()
            redis_key = _hydra_role_llm_key(role, field)
            if field == "port" and raw_value:
                if not raw_value.isdigit():
                    raise HTTPException(
                        status_code=400,
                        detail=f"{payload_key} must be an integer between 1 and 65535",
                    )
                port_int = int(raw_value)
                if port_int < 1 or port_int > 65535:
                    raise HTTPException(
                        status_code=400,
                        detail=f"{payload_key} must be an integer between 1 and 65535",
                    )
                raw_value = str(port_int)
            if raw_value:
                redis_client.set(redis_key, raw_value)
            else:
                redis_client.delete(redis_key)

    hydra_mappings = {
        "hydra_agent_state_ttl_seconds": (
            HYDRA_AGENT_STATE_TTL_SECONDS_KEY,
            DEFAULT_AGENT_STATE_TTL_SECONDS,
            0,
            None,
        ),
        "hydra_max_ledger_items": (HYDRA_MAX_LEDGER_ITEMS_KEY, DEFAULT_MAX_LEDGER_ITEMS, 1, None),
        "hydra_step_retry_limit": (HYDRA_STEP_RETRY_LIMIT_KEY, DEFAULT_STEP_RETRY_LIMIT, 1, 10),
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

    if "admin_only_plugins" in updates:
        values = [str(item).strip().lower() for item in (updates.get("admin_only_plugins") or []) if str(item).strip()]
        cleaned = sorted(set(values))
        redis_client.set(ADMIN_GATE_KEY, json.dumps(cleaned))

    return {"ok": True, "updated": sorted(updates.keys())}
