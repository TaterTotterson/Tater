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

import dotenv
import redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from redis.exceptions import RedisError

import core_registry as core_registry_module
import plugin_registry as plugin_registry_module
import portal_registry as portal_registry_module
from admin_gate import DEFAULT_ADMIN_ONLY_PLUGINS, REDIS_KEY as ADMIN_GATE_KEY, get_admin_only_plugins
from cerberus import resolve_agent_limits, run_cerberus_turn
from cerberus import (
    AGENT_MAX_ROUNDS_KEY,
    AGENT_MAX_TOOL_CALLS_KEY,
    CERBERUS_AGENT_STATE_TTL_SECONDS_KEY,
    CERBERUS_PLANNER_MAX_TOKENS_KEY,
    CERBERUS_CHECKER_MAX_TOKENS_KEY,
    CERBERUS_DOER_MAX_TOKENS_KEY,
    CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY,
    CERBERUS_RECOVERY_MAX_TOKENS_KEY,
    CERBERUS_MAX_LEDGER_ITEMS_KEY,
    DEFAULT_AGENT_STATE_TTL_SECONDS,
    DEFAULT_CHECKER_MAX_TOKENS,
    DEFAULT_DOER_MAX_TOKENS,
    DEFAULT_MAX_LEDGER_ITEMS,
    DEFAULT_MAX_ROUNDS,
    DEFAULT_MAX_TOOL_CALLS,
    DEFAULT_PLANNER_MAX_TOKENS,
    DEFAULT_RECOVERY_MAX_TOKENS,
    DEFAULT_TOOL_REPAIR_MAX_TOKENS,
)
from emoji_responder import get_emoji_settings as get_core_emoji_settings, save_emoji_settings as save_core_emoji_settings
from helpers import get_llm_client_from_env, set_main_loop
from plugin_settings import (
    get_plugin_enabled,
    get_plugin_settings,
    save_plugin_settings,
    set_plugin_enabled,
)
from plugin_kernel import normalize_platform
from vision_settings import get_vision_settings as get_shared_vision_settings, save_vision_settings as save_shared_vision_settings
from tateros import core_store as core_store_module
from tateros import plugin_store as plugin_store_module
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


bootstrap_state: Dict[str, Any] = {
    "restore_enabled": True,
    "restore_in_progress": False,
    "restore_complete": False,
    "restore_error": "",
    "restore_summary": {},
    "autostart_enabled": True,
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


def _plugin_display_name(plugin: Any) -> str:
    return (
        str(getattr(plugin, "plugin_name", "") or "").strip()
        or str(getattr(plugin, "pretty_name", "") or "").strip()
        or str(getattr(plugin, "name", "") or "").strip()
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
    history = _load_chat_history_tail(max_llm)

    loop_messages: List[Dict[str, Any]] = []
    for msg in history:
        role = str(msg.get("role") or "assistant")
        if role not in {"user", "assistant"}:
            role = "assistant"
        templ = _to_template_msg(role, msg.get("content"))
        if templ is not None:
            loop_messages.append(templ)
    loop_messages = _enforce_user_assistant_alternation(loop_messages)

    plugin_registry_module.ensure_plugins_loaded()
    merged_registry = dict(plugin_registry_module.get_registry() or {})

    origin = {
        "platform": "webui",
        "user": user_name,
        "user_id": user_name,
        "session_id": session_scope_id,
    }
    if isinstance(input_artifacts, list) and input_artifacts:
        origin["input_artifacts"] = [dict(item) for item in input_artifacts if isinstance(item, dict)]
    agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)

    async with get_llm_client_from_env() as llm_client:
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

        result = await run_cerberus_turn(
            llm_client=llm_client,
            platform="webui",
            history_messages=loop_messages,
            registry=merged_registry,
            enabled_predicate=get_plugin_enabled,
            context={"raw_message": message_content, "input_artifacts": list(input_artifacts or [])},
            user_text=(message_content or ""),
            scope=f"session:{session_scope_id}",
            origin=origin,
            redis_client=redis_client,
            wait_callback=(_wait if callable(wait_callback) else None),
            max_rounds=agent_max_rounds,
            max_tool_calls=agent_max_tool_calls,
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
                    getattr(plugin_obj, "plugin_name", None)
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
    helper = getattr(plugin_store_module, "_get_item_platforms", None)
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
    plugin_registry_module.ensure_plugins_loaded()
    manifest_repos = plugin_store_module.get_configured_shop_manifest_repos()
    catalog_items, catalog_errors = plugin_store_module.load_shop_catalog(manifest_repos)

    build_entries = getattr(plugin_store_module, "_build_installed_entries", None)
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
                "enabled": bool(get_plugin_enabled(plugin_id)),
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
            "additional": plugin_store_module.get_additional_shop_manifest_repos(),
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

    # 1) Verbas (plugins)
    try:
        missing_plugins_before = list(plugin_store_module._enabled_missing_plugin_ids() or [])
    except Exception:
        missing_plugins_before = []
    summary["plugins_missing_before"] = len(missing_plugins_before)
    if missing_plugins_before:
        logger.info("[startup-restore] restoring %d missing enabled verba(s)", len(missing_plugins_before))
        plugin_store_module.ensure_plugins_ready(progress_cb=_restore_progress_logger("verbas"))
        plugin_registry_module.reload_plugins()
    try:
        missing_plugins_after = list(plugin_store_module._enabled_missing_plugin_ids() or [])
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
    cerberus_max_rounds: Optional[int] = None
    cerberus_max_tool_calls: Optional[int] = None
    cerberus_agent_state_ttl_seconds: Optional[int] = None
    cerberus_planner_max_tokens: Optional[int] = None
    cerberus_checker_max_tokens: Optional[int] = None
    cerberus_doer_max_tokens: Optional[int] = None
    cerberus_tool_repair_max_tokens: Optional[int] = None
    cerberus_recovery_max_tokens: Optional[int] = None
    cerberus_max_ledger_items: Optional[int] = None
    admin_only_plugins: Optional[List[str]] = None


class CerberusDataClearRequest(BaseModel):
    platform: str = "all"
    mode: str = "all"


app = FastAPI(title="TaterOS", version="0.2.0")

STATIC_DIR = Path(__file__).resolve().parent / "tateros_static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.on_event("startup")
async def _startup_event() -> None:
    set_main_loop(asyncio.get_running_loop())
    plugin_registry_module.ensure_plugins_loaded()
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

    try:
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


@app.get("/api/health")
def health() -> Dict[str, Any]:
    try:
        redis_client.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    verbas_enabled = 0
    try:
        registry = plugin_registry_module.get_registry_snapshot() or {}
        for plugin_id in registry.keys():
            try:
                if get_plugin_enabled(str(plugin_id or "").strip()):
                    verbas_enabled += 1
            except Exception:
                continue
    except Exception:
        verbas_enabled = 0

    return {
        "ok": redis_ok,
        "redis": redis_ok,
        "verbas_enabled": int(verbas_enabled),
        "cores_running": len([k for k in core_runtime.threads if core_runtime.is_running(k)]),
        "portals_running": len([k for k in portal_runtime.threads if portal_runtime.is_running(k)]),
        "chat_jobs_active": chat_jobs.active_count(),
        "bootstrap": {
            "restore_enabled": bool(bootstrap_state.get("restore_enabled")),
            "autostart_enabled": bool(bootstrap_state.get("autostart_enabled")),
            "restore_in_progress": bool(bootstrap_state.get("restore_in_progress")),
            "restore_complete": bool(bootstrap_state.get("restore_complete")),
            "restore_error": str(bootstrap_state.get("restore_error") or ""),
            "restore_summary": dict(bootstrap_state.get("restore_summary") or {}),
        },
    }


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
    plugin_registry_module.ensure_plugins_loaded()
    registry = plugin_registry_module.get_registry_snapshot()

    items: List[Dict[str, Any]] = []
    for plugin_id, plugin in registry.items():
        settings_category = str(getattr(plugin, "settings_category", "") or "").strip()
        required_settings = getattr(plugin, "required_settings", None)
        required_settings = required_settings if isinstance(required_settings, dict) else {}
        current_settings = get_plugin_settings(settings_category) if settings_category else {}

        items.append(
            {
                "id": plugin_id,
                "name": _plugin_display_name(plugin) or plugin_id,
                "description": str(getattr(plugin, "plugin_dec", "") or getattr(plugin, "description", "") or "").strip(),
                "platforms": list(getattr(plugin, "platforms", []) or []),
                "enabled": get_plugin_enabled(plugin_id),
                "settings_category": settings_category,
                "settings": _setting_fields(required_settings, current_settings),
            }
        )

    items.sort(key=lambda row: str(row.get("name") or "").lower())
    return {"items": items}


@app.post("/api/verbas/{plugin_id}/enabled")
def set_verba_enabled(plugin_id: str, payload: PluginToggleRequest) -> Dict[str, Any]:
    plugin_registry_module.ensure_plugins_loaded()
    registry = plugin_registry_module.get_registry_snapshot()
    if plugin_id not in registry:
        raise HTTPException(status_code=404, detail=f"Unknown verba: {plugin_id}")

    set_plugin_enabled(plugin_id, bool(payload.enabled))
    return {"id": plugin_id, "enabled": bool(payload.enabled)}


@app.post("/api/verbas/{plugin_id}/settings")
def save_verba_settings(plugin_id: str, payload: SettingsUpdateRequest) -> Dict[str, Any]:
    plugin_registry_module.ensure_plugins_loaded()
    registry = plugin_registry_module.get_registry_snapshot()
    plugin = registry.get(plugin_id)
    if plugin is None:
        raise HTTPException(status_code=404, detail=f"Unknown verba: {plugin_id}")

    category = str(getattr(plugin, "settings_category", "") or "").strip()
    if not category:
        raise HTTPException(status_code=400, detail=f"{plugin_id} has no settings category")

    values = dict(payload.values or {})
    save_plugin_settings(category, values)
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
        if label.lower() == "cerberus":
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
    plugin_store_module.save_additional_shop_manifest_repos(rows)
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

    ok, msg = plugin_store_module.install_plugin_from_shop_item(item)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    plugin_registry_module.reload_plugins()
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

    ok, msg = plugin_store_module.install_plugin_from_shop_item(catalog_item)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    plugin_registry_module.reload_plugins()
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
        ok, msg = plugin_store_module.install_plugin_from_shop_item(catalog_item)
        if ok:
            updated.append(plugin_id)
        else:
            failed.append(msg)

    if updated:
        plugin_registry_module.reload_plugins()

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

    loaded = plugin_registry_module.get_registry_snapshot().get(plugin_id)
    category_hint = getattr(loaded, "settings_category", None) if loaded else None

    ok, msg = plugin_store_module.uninstall_plugin_file(plugin_id)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    try:
        set_plugin_enabled(plugin_id, False)
    except Exception:
        pass

    cleanup_message = ""
    if bool(payload.purge_redis):
        ok2, msg2 = plugin_store_module.clear_plugin_redis_data(plugin_id, category_hint=category_hint)
        cleanup_message = msg2
        if not ok2:
            raise HTTPException(status_code=400, detail=f"Removed file, but Redis cleanup failed: {msg2}")

    plugin_registry_module.reload_plugins()
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


_CERBERUS_METRIC_NAMES = (
    "total_turns",
    "total_tools_called",
    "total_repairs",
    "validation_failures",
    "tool_failures",
)
_CERBERUS_METRIC_PLATFORMS = (
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


def _cerberus_platform_display_label(platform: str) -> str:
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


def _cerberus_ledger_keys_for_platform(platform: str) -> List[str]:
    plat = str(platform or "all").strip().lower() or "all"
    if plat == "all":
        keys: List[str] = []
        try:
            if redis_client.exists("tater:cerberus:ledger"):
                keys.append("tater:cerberus:ledger")
        except Exception:
            pass
        try:
            keys.extend(sorted(str(k) for k in redis_client.scan_iter(match="tater:cerberus:ledger:*")))
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
    return [f"tater:cerberus:ledger:{normalized}"]


def _load_cerberus_ledger_entries(platform: str, limit: int) -> List[Dict[str, Any]]:
    max_limit = max(10, min(int(limit or 50), 300))
    rows: List[Dict[str, Any]] = []
    for key in _cerberus_ledger_keys_for_platform(platform):
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


def _normalize_cerberus_validation_for_view(
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


def _cerberus_rate_rows(metrics: Dict[str, int]) -> List[Dict[str, Any]]:
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


def _load_cerberus_metrics(platform: str) -> Tuple[str, Dict[str, int], Dict[str, int]]:
    selected = str(platform or "").strip().lower()
    metric_platform = normalize_platform(selected if selected and selected != "all" else "webui")
    global_metrics: Dict[str, int] = {}
    platform_metrics: Dict[str, int] = {}
    for name in _CERBERUS_METRIC_NAMES:
        global_metrics[name] = _coerce_redis_counter(redis_client.get(f"tater:cerberus:metrics:{name}"))
        if selected == "all":
            platform_metrics[name] = global_metrics[name]
        else:
            platform_metrics[name] = _coerce_redis_counter(
                redis_client.get(f"tater:cerberus:metrics:{name}:{metric_platform}")
            )
    return metric_platform, global_metrics, platform_metrics


def _load_cerberus_platform_metric_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for platform in _CERBERUS_METRIC_PLATFORMS:
        row: Dict[str, Any] = {
            "platform": platform,
            "platform_label": _cerberus_platform_display_label(platform),
        }
        for name in _CERBERUS_METRIC_NAMES:
            row[name] = _coerce_redis_counter(redis_client.get(f"tater:cerberus:metrics:{name}:{platform}"))
        rates = _cerberus_rate_rows(row)
        for rate_row in rates:
            row[str(rate_row.get("metric") or "")] = float(rate_row.get("value") or 0.0)
        rows.append(row)
    return rows


def _reset_cerberus_metrics(platform: str) -> int:
    plat = str(platform or "").strip().lower()
    keys: List[str] = []
    if plat == "all":
        try:
            keys = [str(k) for k in redis_client.scan_iter(match="tater:cerberus:metrics:*")]
        except Exception:
            keys = []
    else:
        metric_platform = normalize_platform(plat or "webui")
        for name in _CERBERUS_METRIC_NAMES:
            keys.append(f"tater:cerberus:metrics:{name}")
            keys.append(f"tater:cerberus:metrics:{name}:{metric_platform}")

    deleted = 0
    for key in keys:
        try:
            deleted += int(redis_client.delete(key) or 0)
        except Exception:
            continue
    return deleted


def _clear_cerberus_ledger(platform: str) -> int:
    deleted = 0
    for key in _cerberus_ledger_keys_for_platform(platform):
        try:
            deleted += int(redis_client.delete(key) or 0)
        except Exception:
            continue
    return deleted


@app.get("/api/settings/cerberus/metrics")
def get_cerberus_metrics(
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

    metric_platform, global_metrics, platform_metrics = _load_cerberus_metrics(selected_platform)
    ledger_rows = _load_cerberus_ledger_entries(selected_platform, max_limit)

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
        validation = _normalize_cerberus_validation_for_view(row.get("validation"), planned_tool=planned_tool)
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
                "outcome": str(row.get("outcome") or ""),
                "outcome_reason": str(row.get("outcome_reason") or ""),
                "planned_tool": planned_tool_name,
                "validation_status": str(validation.get("status") or ""),
                "validation_reason": str(validation.get("reason") or ""),
                "checker_action": str(row.get("checker_action") or ""),
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
            key = f"checker:{checker_reason}"
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
        "selected_platform_label": _cerberus_platform_display_label(selected_platform),
        "metric_platform": metric_platform,
        "platform_options": ["all", *_CERBERUS_METRIC_PLATFORMS],
        "metric_names": list(_CERBERUS_METRIC_NAMES),
        "global_metrics": global_metrics,
        "global_rates": _cerberus_rate_rows(global_metrics),
        "platform_metrics": platform_metrics,
        "platform_rates": _cerberus_rate_rows(platform_metrics),
        "platform_rows": _load_cerberus_platform_metric_rows(),
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


@app.get("/api/settings/cerberus/data")
def get_cerberus_data() -> Dict[str, Any]:
    metric_keys: List[str] = []
    try:
        metric_keys = [str(k) for k in redis_client.scan_iter(match="tater:cerberus:metrics:*")]
    except Exception:
        metric_keys = []

    ledger_rows: List[Dict[str, Any]] = []
    ledger_entries_total = 0
    for key in _cerberus_ledger_keys_for_platform("all"):
        count = 0
        try:
            count = int(redis_client.llen(key) or 0)
        except Exception:
            count = 0
        ledger_entries_total += max(0, int(count))
        if key == "tater:cerberus:ledger":
            platform = "legacy"
        else:
            suffix = str(key).split("tater:cerberus:ledger:", 1)[-1]
            platform = normalize_platform(suffix or "webui")
        ledger_rows.append(
            {
                "platform": platform,
                "platform_label": _cerberus_platform_display_label(platform),
                "ledger_key": key,
                "entries": int(max(0, count)),
            }
        )
    ledger_rows.sort(key=lambda row: (-int(row.get("entries") or 0), str(row.get("platform") or "")))

    platform_rows: List[Dict[str, Any]] = []
    for platform in _CERBERUS_METRIC_PLATFORMS:
        _, _, platform_metrics = _load_cerberus_metrics(platform)
        platform_row: Dict[str, Any] = {
            "platform": platform,
            "platform_label": _cerberus_platform_display_label(platform),
        }
        for name in _CERBERUS_METRIC_NAMES:
            platform_row[name] = int(platform_metrics.get(name) or 0)
        try:
            platform_row["ledger_entries"] = int(redis_client.llen(f"tater:cerberus:ledger:{platform}") or 0)
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
        "platform_options": list(_CERBERUS_METRIC_PLATFORMS),
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


@app.post("/api/settings/cerberus/data/clear")
def clear_cerberus_data(payload: CerberusDataClearRequest) -> Dict[str, Any]:
    mode = str(payload.mode or "all").strip().lower() or "all"
    platform = str(payload.platform or "all").strip().lower() or "all"
    if mode not in {"all", "metrics", "ledger"}:
        raise HTTPException(status_code=400, detail="mode must be one of: all, metrics, ledger")
    if platform != "all":
        platform = normalize_platform(platform)

    metrics_removed = 0
    ledger_removed = 0
    if mode in {"all", "metrics"}:
        metrics_removed = _reset_cerberus_metrics(platform)
    if mode in {"all", "ledger"}:
        ledger_removed = _clear_cerberus_ledger(platform)

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
    legacy_web_search = redis_client.hgetall("plugin_settings:Web Search") or {}
    homeassistant_settings = redis_client.hgetall("homeassistant_settings") or {}

    vision_settings = get_shared_vision_settings(
        default_api_base="http://127.0.0.1:1234",
        default_model="qwen2.5-vl-7b-instruct",
    )
    emoji_settings = get_core_emoji_settings() or {}

    plugin_registry_module.ensure_plugins_loaded()
    registry_snapshot = plugin_registry_module.get_registry_snapshot()
    admin_plugin_options = sorted(str(plugin_id or "").strip() for plugin_id in registry_snapshot.keys() if str(plugin_id or "").strip())
    admin_only_plugins = sorted(get_admin_only_plugins(redis_client))

    cerberus_defaults = {
        "cerberus_max_rounds": int(DEFAULT_MAX_ROUNDS),
        "cerberus_max_tool_calls": int(DEFAULT_MAX_TOOL_CALLS),
        "cerberus_agent_state_ttl_seconds": int(DEFAULT_AGENT_STATE_TTL_SECONDS),
        "cerberus_planner_max_tokens": int(DEFAULT_PLANNER_MAX_TOKENS),
        "cerberus_checker_max_tokens": int(DEFAULT_CHECKER_MAX_TOKENS),
        "cerberus_doer_max_tokens": int(DEFAULT_DOER_MAX_TOKENS),
        "cerberus_tool_repair_max_tokens": int(DEFAULT_TOOL_REPAIR_MAX_TOKENS),
        "cerberus_recovery_max_tokens": int(DEFAULT_RECOVERY_MAX_TOKENS),
        "cerberus_max_ledger_items": int(DEFAULT_MAX_LEDGER_ITEMS),
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
        "cerberus_max_rounds": _read_non_negative_int(AGENT_MAX_ROUNDS_KEY, DEFAULT_MAX_ROUNDS),
        "cerberus_max_tool_calls": _read_non_negative_int(AGENT_MAX_TOOL_CALLS_KEY, DEFAULT_MAX_TOOL_CALLS),
        "cerberus_agent_state_ttl_seconds": _read_non_negative_int(
            CERBERUS_AGENT_STATE_TTL_SECONDS_KEY,
            DEFAULT_AGENT_STATE_TTL_SECONDS,
        ),
        "cerberus_planner_max_tokens": _read_positive_int(CERBERUS_PLANNER_MAX_TOKENS_KEY, DEFAULT_PLANNER_MAX_TOKENS),
        "cerberus_checker_max_tokens": _read_positive_int(CERBERUS_CHECKER_MAX_TOKENS_KEY, DEFAULT_CHECKER_MAX_TOKENS),
        "cerberus_doer_max_tokens": _read_positive_int(CERBERUS_DOER_MAX_TOKENS_KEY, DEFAULT_DOER_MAX_TOKENS),
        "cerberus_tool_repair_max_tokens": _read_positive_int(
            CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY,
            DEFAULT_TOOL_REPAIR_MAX_TOKENS,
        ),
        "cerberus_recovery_max_tokens": _read_positive_int(CERBERUS_RECOVERY_MAX_TOKENS_KEY, DEFAULT_RECOVERY_MAX_TOKENS),
        "cerberus_max_ledger_items": _read_positive_int(CERBERUS_MAX_LEDGER_ITEMS_KEY, DEFAULT_MAX_LEDGER_ITEMS),
        "cerberus_defaults": cerberus_defaults,
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

    cerberus_mappings = {
        "cerberus_max_rounds": (AGENT_MAX_ROUNDS_KEY, DEFAULT_MAX_ROUNDS, 0, None),
        "cerberus_max_tool_calls": (AGENT_MAX_TOOL_CALLS_KEY, DEFAULT_MAX_TOOL_CALLS, 0, None),
        "cerberus_agent_state_ttl_seconds": (
            CERBERUS_AGENT_STATE_TTL_SECONDS_KEY,
            DEFAULT_AGENT_STATE_TTL_SECONDS,
            0,
            None,
        ),
        "cerberus_planner_max_tokens": (CERBERUS_PLANNER_MAX_TOKENS_KEY, DEFAULT_PLANNER_MAX_TOKENS, 1, None),
        "cerberus_checker_max_tokens": (CERBERUS_CHECKER_MAX_TOKENS_KEY, DEFAULT_CHECKER_MAX_TOKENS, 1, None),
        "cerberus_doer_max_tokens": (CERBERUS_DOER_MAX_TOKENS_KEY, DEFAULT_DOER_MAX_TOKENS, 1, None),
        "cerberus_tool_repair_max_tokens": (
            CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY,
            DEFAULT_TOOL_REPAIR_MAX_TOKENS,
            1,
            None,
        ),
        "cerberus_recovery_max_tokens": (CERBERUS_RECOVERY_MAX_TOKENS_KEY, DEFAULT_RECOVERY_MAX_TOKENS, 1, None),
        "cerberus_max_ledger_items": (CERBERUS_MAX_LEDGER_ITEMS_KEY, DEFAULT_MAX_LEDGER_ITEMS, 1, None),
    }
    for payload_key, (redis_key, default, min_value, max_value) in cerberus_mappings.items():
        if payload_key not in updates:
            continue
        normalized = _bounded_int(
            updates.get(payload_key),
            default=int(default),
            min_value=int(min_value),
            max_value=max_value,
        )
        redis_client.set(redis_key, int(normalized))

    if "admin_only_plugins" in updates:
        values = [str(item).strip().lower() for item in (updates.get("admin_only_plugins") or []) if str(item).strip()]
        cleaned = sorted(set(values))
        redis_client.set(ADMIN_GATE_KEY, json.dumps(cleaned))

    return {"ok": True, "updated": sorted(updates.keys())}
