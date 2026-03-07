# webui.py
import streamlit as st
import redis
import os
import time
import json
import asyncio
import dotenv
import logging
import base64
import importlib
import threading
import sys
import uuid
import plugin_registry as plugin_registry_mod
import core_registry as core_registry_module
from datetime import datetime
from typing import Any, Dict, List, Optional
from PIL import Image
from io import BytesIO
import portal_registry as portal_registry_module
from helpers import (
    set_main_loop,
    get_tater_name,
    get_llm_client_from_env,
    build_llm_host_from_env
)
from plugin_settings import (
    get_plugin_enabled,
)
from admin_gate import (
    REDIS_KEY as ADMIN_GATE_KEY,
    get_admin_only_plugins,
)
from cerberus import (
    run_cerberus_turn,
    resolve_agent_limits,
)
from vision_settings import (
    get_vision_settings as get_shared_vision_settings,
    save_vision_settings as save_shared_vision_settings,
)
from emoji_responder import get_emoji_settings as get_core_emoji_settings, save_emoji_settings as save_core_emoji_settings
from webui.webui_cerberus import (
    render_cerberus_settings,
    render_cerberus_metrics_dashboard,
    render_cerberus_data_tools,
)
from webui.webui_ai_tasks import render_ai_tasks_page
from webui.webui_memory import render_memory_page, wipe_memory_core_data
from webui.webui_plugin_store import (
    _enabled_missing_plugin_ids,
    ensure_plugins_ready,
    render_plugin_store_page,
)
from webui.webui_core_store import (
    _enabled_missing_core_ids,
    ensure_cores_ready,
    render_core_store_page,
)
from webui.webui_portal_store import (
    _enabled_missing_portal_ids,
    ensure_portals_ready,
    render_portal_store_page,
)
from webui.webui_settings import render_settings_page
from webui.webui_chat import (
    configure_chat_helpers,
    save_message,
    _media_type_from_mimetype,
    load_chat_history_tail,
    load_chat_history,
    clear_chat_history,
    get_tater_avatar,
    get_chat_settings,
    save_chat_settings,
    load_avatar_image,
    _to_template_msg,
    _enforce_user_assistant_alternation,
)

# Remove any prior handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Global logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)-20s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Quiet/noisy modules
logging.getLogger("discord.http").setLevel(logging.WARNING)
logging.getLogger("irc3.TaterBot").setLevel(logging.WARNING)  # Optional: suppress join/config spam

dotenv.load_dotenv()

# ------------------ Plugin Registry Access ------------------
def get_registry():
    return plugin_registry_mod.plugin_registry

# ------------------ Upload / Attachment Limits ------------------
# Per-file max upload size (MB) for st.chat_input + our Redis storage
WEBUI_ATTACH_MAX_MB_EACH = int(os.getenv("WEBUI_ATTACH_MAX_MB_EACH", "25"))
# Total max bytes allowed per message (sum of uploaded files)
WEBUI_ATTACH_MAX_MB_TOTAL = int(os.getenv("WEBUI_ATTACH_MAX_MB_TOTAL", "50"))
# Optional TTL for stored file blobs (0 = keep forever)
WEBUI_ATTACH_TTL_SECONDS = int(os.getenv("WEBUI_ATTACH_TTL_SECONDS", "0"))
# Store only a limited number of recent attachment ids (index)
WEBUI_ATTACH_INDEX_MAX = int(os.getenv("WEBUI_ATTACH_INDEX_MAX", "500"))

FILE_BLOB_KEY_PREFIX = "webui:file:"
FILE_INDEX_KEY = "webui:file_index"
# Redis configuration for the web UI (using a separate DB)
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))

# Text/JSON Redis (current behavior)
redis_client = redis.Redis(
    host=redis_host,
    port=redis_port,
    db=0,
    decode_responses=True
)

# Binary Redis for file blobs
redis_blob_client = redis.Redis(
    host=redis_host,
    port=redis_port,
    db=0,
    decode_responses=False
)

configure_chat_helpers(
    redis_client=redis_client,
)

# ------------------ FILE BLOB HELPERS ------------------

def _bytes_to_mb(n: int) -> float:
    return n / (1024 * 1024)

def _store_file_blob_in_redis(file_id: str, data: bytes):
    key = f"{FILE_BLOB_KEY_PREFIX}{file_id}"
    redis_blob_client.set(key, data)
    if WEBUI_ATTACH_TTL_SECONDS and WEBUI_ATTACH_TTL_SECONDS > 0:
        redis_blob_client.expire(key, WEBUI_ATTACH_TTL_SECONDS)

    # Track recent ids for optional cleanup (text redis is fine here)
    redis_client.rpush(FILE_INDEX_KEY, file_id)
    redis_client.ltrim(FILE_INDEX_KEY, -WEBUI_ATTACH_INDEX_MAX, -1)

def _load_file_blob_from_redis(file_id: str):
    data = redis_blob_client.get(f"{FILE_BLOB_KEY_PREFIX}{file_id}")
    return data if data else None

def _normalize_plugin_response_item(item):
    """
    Convert plugin media payloads into redis blob references so JSON serialization works.
    Supports both keys: data / bytes.
    Produces: {type,name,mimetype,size,id}
    """
    if not isinstance(item, dict):
        return item

    t = item.get("type")
    if t not in ("image", "audio", "video", "file"):
        return item

    raw = None
    if isinstance(item.get("data"), (bytes, bytearray)):
        raw = bytes(item["data"])
    elif isinstance(item.get("bytes"), (bytes, bytearray)):
        raw = bytes(item["bytes"])

    if raw is None:
        return item

    file_id = str(uuid.uuid4())
    _store_file_blob_in_redis(file_id, raw)

    safe = dict(item)
    safe.pop("data", None)
    safe.pop("bytes", None)
    safe["id"] = file_id
    safe["size"] = len(raw)
    return safe

def _get_media_blob_from_content(content: dict):
    """
    Supports:
      - blob reference in content["id"]           (preferred: webui:file:<id>)
      - plugin blob reference in content["blob_key"] (tater:blob:...)
      - legacy base64 in content["data"] (str)
      - legacy bytes in content["data"] (bytes)
    """
    if not isinstance(content, dict):
        return None

    # Preferred: WebUI file store
    file_id = content.get("id")
    if file_id:
        return _load_file_blob_from_redis(file_id)

    # Compatibility: plugin-managed blob keys (your new plugins)
    blob_key = content.get("blob_key")
    if blob_key:
        try:
            data = redis_blob_client.get(blob_key)  # decode_responses=False ✅
            return data if data else None
        except Exception:
            return None

    # Legacy: base64 string
    if "data" in content and isinstance(content["data"], str):
        try:
            return base64.b64decode(content["data"])
        except Exception:
            return None

    # Legacy: raw bytes
    if "data" in content and isinstance(content["data"], (bytes, bytearray)):
        return bytes(content["data"])

    return None

@st.cache_resource
def _portal_runtime():
    # Shared across reruns/sessions within this Streamlit process.
    return {
        "lock": threading.RLock(),
        "threads": {},
        "stop_flags": {},
    }


@st.cache_resource
def _core_runtime():
    return {
        "lock": threading.RLock(),
        "threads": {},
        "stop_flags": {},
    }


@st.cache_resource
def _chat_job_runtime():
    return {
        "lock": threading.RLock(),
        "jobs": {},
        "order": [],
    }


def _set_webui_rerun_flag() -> None:
    try:
        redis_client.set("webui:needs_rerun", "true")
    except Exception:
        pass


def _ensure_webui_session_id() -> str:
    session_scope_id = str(st.session_state.get("webui_session_id") or "").strip()
    if not session_scope_id:
        session_scope_id = str(uuid.uuid4())
        st.session_state["webui_session_id"] = session_scope_id
    return session_scope_id


def _job_label(message_content: str, input_artifacts: Optional[List[Dict[str, Any]]]) -> str:
    content = " ".join(str(message_content or "").split())
    if content:
        return content[:119] + "…" if len(content) > 120 else content
    names: List[str] = []
    for item in input_artifacts or []:
        if not isinstance(item, dict):
            continue
        name = " ".join(str(item.get("name") or "").split())
        if not name:
            continue
        names.append(name[:39] + "…" if len(name) > 40 else name)
        if len(names) >= 3:
            break
    if names:
        return "files: " + ", ".join(names)
    return "request"


def _list_active_chat_jobs(*, session_id: str) -> List[Dict[str, Any]]:
    runtime = _chat_job_runtime()
    active: List[Dict[str, Any]] = []
    with runtime["lock"]:
        order = list(runtime.get("order") or [])
        jobs = runtime.get("jobs") or {}
        for job_id in order:
            job = jobs.get(job_id)
            if not isinstance(job, dict):
                continue
            if str(job.get("session_id") or "") != str(session_id or ""):
                continue
            status = str(job.get("status") or "").strip().lower()
            if status not in {"queued", "running"}:
                continue
            active.append(
                {
                    "id": str(job.get("id") or ""),
                    "status": status,
                    "current_tool": str(job.get("current_tool") or "").strip(),
                }
            )
    return active


def _collect_finished_chat_jobs(*, session_id: str) -> List[Dict[str, Any]]:
    runtime = _chat_job_runtime()
    finished: List[Dict[str, Any]] = []
    with runtime["lock"]:
        order = list(runtime.get("order") or [])
        jobs = runtime.get("jobs") or {}
        remove_ids: List[str] = []
        for job_id in order:
            job = jobs.get(job_id)
            if not isinstance(job, dict):
                continue
            if str(job.get("session_id") or "") != str(session_id or ""):
                continue
            status = str(job.get("status") or "").strip().lower()
            if status not in {"done", "error"}:
                continue
            finished.append(
                {
                    "status": status,
                    "label": str(job.get("label") or "request"),
                    "responses": list(job.get("responses") or []),
                    "error": str(job.get("error") or "").strip(),
                    "completed_at": float(job.get("completed_at") or 0.0),
                }
            )
            remove_ids.append(str(job_id))
        for job_id in remove_ids:
            jobs.pop(job_id, None)
        if remove_ids:
            remove_set = set(remove_ids)
            runtime["order"] = [jid for jid in order if str(jid) not in remove_set]
    finished.sort(key=lambda item: float(item.get("completed_at") or 0.0))
    return finished


def _enqueue_chat_job(
    *,
    user_name: str,
    message_content: str,
    input_artifacts: Optional[List[Dict[str, Any]]],
    session_scope_id: str,
) -> str:
    runtime = _chat_job_runtime()
    job_id = str(uuid.uuid4())
    with runtime["lock"]:
        runtime["jobs"][job_id] = {
            "id": job_id,
            "session_id": str(session_scope_id or ""),
            "status": "queued",
            "label": _job_label(message_content, input_artifacts),
            "current_tool": "",
            "responses": [],
            "error": "",
            "completed_at": 0.0,
        }
        runtime["order"] = [jid for jid in runtime.get("order", []) if jid != job_id]
        runtime["order"].append(job_id)

    def _run():
        logger = logging.getLogger("webui")
        with runtime["lock"]:
            job = runtime["jobs"].get(job_id)
            if isinstance(job, dict):
                job["status"] = "running"
                job["current_tool"] = ""
        _set_webui_rerun_flag()

        async def _wait_callback(func_name, plugin_obj):
            if plugin_obj is None:
                display_name = f"kernel::{func_name}"
            else:
                display_name = (
                    getattr(plugin_obj, "plugin_name", None)
                    or getattr(plugin_obj, "pretty_name", None)
                    or getattr(plugin_obj, "name", None)
                    or func_name
                )
            with runtime["lock"]:
                job = runtime["jobs"].get(job_id)
                if not isinstance(job, dict):
                    return
                if str(job.get("status") or "").strip().lower() not in {"queued", "running"}:
                    return
                job["status"] = "running"
                job["current_tool"] = str(display_name or "").strip()
            _set_webui_rerun_flag()

        try:
            job_llm_client = get_llm_client_from_env()
            response_payload = asyncio.run(
                process_message(
                    user_name,
                    message_content,
                    input_artifacts=input_artifacts,
                    wait_callback=_wait_callback,
                    session_scope_id=session_scope_id,
                    llm_client_override=job_llm_client,
                )
            )
            if isinstance(response_payload, dict) and isinstance(response_payload.get("responses"), list):
                responses = response_payload.get("responses") or []
            else:
                responses = [response_payload]
            with runtime["lock"]:
                job = runtime["jobs"].get(job_id)
                if not isinstance(job, dict):
                    return
                if str(job.get("status") or "").strip().lower() not in {"queued", "running"}:
                    return
                job["status"] = "done"
                job["responses"] = responses
                job["current_tool"] = ""
                job["completed_at"] = time.time()
        except Exception as e:
            logger.error(f"WebUI background job failed: {e}", exc_info=True)
            with runtime["lock"]:
                job = runtime["jobs"].get(job_id)
                if not isinstance(job, dict):
                    return
                if str(job.get("status") or "").strip().lower() not in {"queued", "running"}:
                    return
                job["status"] = "error"
                job["error"] = str(e)
                job["current_tool"] = ""
                job["completed_at"] = time.time()
        finally:
            _set_webui_rerun_flag()

    worker = threading.Thread(target=_run, daemon=True, name=f"webui-chat-job-{job_id[:8]}")
    worker.start()
    return job_id

AUTO_START_COOLDOWN_SEC = 30


def _portal_thread_alive(key: str) -> bool:
    runtime = _portal_runtime()
    with runtime["lock"]:
        thread = runtime["threads"].get(key)
        return bool(thread and thread.is_alive())


def _should_autostart(kind: str, key: str) -> bool:
    kind_token = str(kind or "").strip().lower() or "portal"
    token = f"{kind_token}:{key}"
    attempts = st.session_state.setdefault("autostart_attempts", set())
    if token in attempts:
        return False

    attempts.add(token)
    redis_key = f"webui:autostart:{kind_token}:{key}"
    last = redis_client.get(redis_key)
    if last:
        try:
            if time.time() - float(last) < AUTO_START_COOLDOWN_SEC:
                return False
        except Exception:
            pass

    redis_client.set(redis_key, str(time.time()))
    return True


def _import_portal_module(key: str):
    module_key = str(key or "").strip()
    if not module_key:
        raise ImportError("Missing portal module key")
    errors = []
    try:
        return importlib.import_module(f"portals.{module_key}")
    except Exception as exc:
        errors.append(f"portals.{module_key}: {exc}")
    raise ImportError("; ".join(errors))


def _start_portal(key: str):
    runtime = _portal_runtime()
    with runtime["lock"]:
        thread = runtime["threads"].get(key)
        stop_flag = runtime["stop_flags"].get(key)

        if thread and thread.is_alive():
            return thread, stop_flag

        stop_flag = threading.Event()

    def runner():
        try:
            module = _import_portal_module(key)
            if hasattr(module, "run"):
                module.run(stop_event=stop_flag)
            else:
                logging.getLogger("webui").warning(f"⚠️ No run(stop_event) in module for {key}")
        except Exception as e:
            logging.getLogger("webui").error(f"❌ Error in portal {key}: {e}", exc_info=True)
        finally:
            # Clean stale references when a platform exits/crashes.
            with runtime["lock"]:
                current = runtime["threads"].get(key)
                if current is threading.current_thread():
                    runtime["threads"].pop(key, None)
                    runtime["stop_flags"].pop(key, None)

    thread = threading.Thread(target=runner, daemon=True)
    with runtime["lock"]:
        runtime["threads"][key] = thread
        runtime["stop_flags"][key] = stop_flag
    thread.start()

    return thread, stop_flag


def _stop_portal(key: str):
    runtime = _portal_runtime()
    with runtime["lock"]:
        thread = runtime["threads"].get(key)
        stop_flag = runtime["stop_flags"].get(key)

    if stop_flag:
        stop_flag.set()

    if thread and thread.is_alive():
        thread.join(timeout=0.5)

    with runtime["lock"]:
        if not thread or not thread.is_alive():
            runtime["threads"].pop(key, None)
            runtime["stop_flags"].pop(key, None)


def _core_thread_alive(key: str) -> bool:
    runtime = _core_runtime()
    with runtime["lock"]:
        thread = runtime["threads"].get(key)
        return bool(thread and thread.is_alive())


def _import_core_module(key: str):
    module_key = str(key or "").strip()
    if not module_key:
        raise ImportError("Missing core module key")
    errors = []
    try:
        return importlib.import_module(f"cores.{module_key}")
    except Exception as exc:
        errors.append(f"cores.{module_key}: {exc}")
    raise ImportError("; ".join(errors))


def _start_core(key: str):
    runtime = _core_runtime()
    with runtime["lock"]:
        thread = runtime["threads"].get(key)
        stop_flag = runtime["stop_flags"].get(key)

        if thread and thread.is_alive():
            return thread, stop_flag

        stop_flag = threading.Event()

    def runner():
        try:
            module = _import_core_module(key)
            if hasattr(module, "run"):
                module.run(stop_event=stop_flag)
            else:
                logging.getLogger("webui").warning(f"⚠️ No run(stop_event) in module for {key}")
        except Exception as e:
            logging.getLogger("webui").error(f"❌ Error in core {key}: {e}", exc_info=True)
        finally:
            with runtime["lock"]:
                current = runtime["threads"].get(key)
                if current is threading.current_thread():
                    runtime["threads"].pop(key, None)
                    runtime["stop_flags"].pop(key, None)

    thread = threading.Thread(target=runner, daemon=True)
    with runtime["lock"]:
        runtime["threads"][key] = thread
        runtime["stop_flags"][key] = stop_flag
    thread.start()

    return thread, stop_flag


def _stop_core(key: str):
    runtime = _core_runtime()
    with runtime["lock"]:
        thread = runtime["threads"].get(key)
        stop_flag = runtime["stop_flags"].get(key)

    if stop_flag:
        stop_flag.set()

    if thread and thread.is_alive():
        thread.join(timeout=0.5)

    with runtime["lock"]:
        if not thread or not thread.is_alive():
            runtime["threads"].pop(key, None)
            runtime["stop_flags"].pop(key, None)


def _legacy_module_key_to_portal(module_key: str, *, ensure_suffix: bool = False) -> str:
    token = str(module_key or "").strip()
    if not token:
        return ""
    if token.endswith("_portal"):
        return token
    if token.endswith("_platform"):
        return f"{token[:-len('_platform')]}_portal"
    return f"{token}_portal" if ensure_suffix else token


_PORTAL_TO_CORE_MODULE_KEY_MAP = {
    "ai_task_portal": "ai_task_core",
    "memory_portal": "memory_core",
    "rss_portal": "rss_core",
}


def _migrate_legacy_string_key(
    old_key: str,
    new_key: str,
    *,
    summary: Dict[str, int],
    prefer_true: bool = False,
) -> None:
    old_val = redis_client.get(old_key)
    if old_val is None:
        return

    old_ttl = int(redis_client.ttl(old_key) or -1)
    new_val = redis_client.get(new_key)

    migrated = False
    if new_val is None:
        redis_client.set(new_key, old_val)
        if old_ttl > 0:
            redis_client.expire(new_key, old_ttl)
        migrated = True
    elif prefer_true:
        old_is_true = str(old_val).strip().lower() == "true"
        new_is_true = str(new_val).strip().lower() == "true"
        if old_is_true and not new_is_true:
            redis_client.set(new_key, "true")
            migrated = True

    deleted = int(redis_client.delete(old_key) or 0)
    if migrated:
        summary["string_keys_migrated"] += 1
    if deleted:
        summary["keys_deleted"] += deleted


def _migrate_legacy_hash_key(old_key: str, new_key: str, *, summary: Dict[str, int]) -> None:
    old_type = str(redis_client.type(old_key) or "none").strip().lower()
    if old_type in {"none", ""}:
        return

    if old_type != "hash":
        _migrate_legacy_string_key(old_key, new_key, summary=summary)
        return

    old_map = redis_client.hgetall(old_key) or {}
    old_ttl = int(redis_client.ttl(old_key) or -1)
    new_type = str(redis_client.type(new_key) or "none").strip().lower()

    if new_type not in {"none", "hash"}:
        return

    new_map = redis_client.hgetall(new_key) if new_type == "hash" else {}
    merged = dict(old_map)
    merged.update(new_map)  # Keep existing portal values when both are present.

    if merged:
        redis_client.hset(new_key, mapping=merged)
        if old_ttl > 0 and int(redis_client.ttl(new_key) or -1) < 0:
            redis_client.expire(new_key, old_ttl)
        summary["hash_keys_migrated"] += 1

    deleted = int(redis_client.delete(old_key) or 0)
    if deleted:
        summary["keys_deleted"] += deleted


def _migrate_legacy_surface_data() -> Dict[str, int]:
    summary = {
        "string_keys_migrated": 0,
        "hash_keys_migrated": 0,
        "keys_deleted": 0,
    }

    for raw_key in redis_client.scan_iter(match="*_platform_running", count=200):
        old_key = str(raw_key or "").strip()
        if not old_key.endswith("_platform_running"):
            continue
        old_module_key = old_key[:-len("_running")]
        new_module_key = _legacy_module_key_to_portal(old_module_key)
        if not new_module_key or new_module_key == old_module_key:
            continue
        _migrate_legacy_string_key(
            old_key,
            f"{new_module_key}_running",
            summary=summary,
            prefer_true=True,
        )

    for raw_key in redis_client.scan_iter(match="*_platform_settings", count=200):
        old_key = str(raw_key or "").strip()
        if not old_key.endswith("_platform_settings"):
            continue
        old_module_key = old_key[:-len("_settings")]
        new_module_key = _legacy_module_key_to_portal(old_module_key)
        if not new_module_key or new_module_key == old_module_key:
            continue
        _migrate_legacy_hash_key(old_key, f"{new_module_key}_settings", summary=summary)

    for raw_key in redis_client.scan_iter(match="tater:cooldown:*_platform", count=200):
        old_key = str(raw_key or "").strip()
        prefix = "tater:cooldown:"
        if not old_key.startswith(prefix):
            continue
        old_module_key = old_key[len(prefix):]
        new_module_key = _legacy_module_key_to_portal(old_module_key)
        if not new_module_key or new_module_key == old_module_key:
            continue
        _migrate_legacy_string_key(old_key, f"{prefix}{new_module_key}", summary=summary)

    for raw_key in redis_client.scan_iter(match="webui:autostart:platform:*", count=200):
        old_key = str(raw_key or "").strip()
        prefix = "webui:autostart:platform:"
        if not old_key.startswith(prefix):
            continue
        old_suffix = old_key[len(prefix):]
        new_suffix = _legacy_module_key_to_portal(old_suffix, ensure_suffix=True)
        if not new_suffix:
            continue
        _migrate_legacy_string_key(
            old_key,
            f"webui:autostart:portal:{new_suffix}",
            summary=summary,
        )

    _migrate_legacy_hash_key("mem:stats:memory_platform", "mem:stats:memory_portal", summary=summary)
    _migrate_legacy_string_key(
        "tater:platform_shop_manifest_urls",
        "tater:portal_shop_manifest_urls",
        summary=summary,
    )

    legacy_single_shop_url = str(redis_client.get("tater:platform_shop_manifest_url") or "").strip()
    if legacy_single_shop_url:
        existing_portal_repo_key = str(redis_client.get("tater:portal_shop_manifest_urls") or "").strip()
        if not existing_portal_repo_key:
            redis_client.set(
                "tater:portal_shop_manifest_urls",
                json.dumps([{"name": "", "url": legacy_single_shop_url}]),
            )
            summary["string_keys_migrated"] += 1
        deleted = int(redis_client.delete("tater:platform_shop_manifest_url") or 0)
        if deleted:
            summary["keys_deleted"] += deleted

    for legacy_module_key, core_module_key in _PORTAL_TO_CORE_MODULE_KEY_MAP.items():
        _migrate_legacy_string_key(
            f"{legacy_module_key}_running",
            f"{core_module_key}_running",
            summary=summary,
            prefer_true=True,
        )
        _migrate_legacy_hash_key(
            f"{legacy_module_key}_settings",
            f"{core_module_key}_settings",
            summary=summary,
        )
        _migrate_legacy_string_key(
            f"tater:cooldown:{legacy_module_key}",
            f"tater:cooldown:{core_module_key}",
            summary=summary,
        )
        _migrate_legacy_string_key(
            f"webui:autostart:portal:{legacy_module_key}",
            f"webui:autostart:core:{core_module_key}",
            summary=summary,
        )

    _migrate_legacy_hash_key("memory_portal_settings", "memory_core_settings", summary=summary)
    _migrate_legacy_hash_key("mem:stats:memory_portal", "mem:stats:memory_core", summary=summary)
    _migrate_legacy_string_key(
        "tater:memory_portal:cerberus_max_items",
        "tater:memory_core:cerberus_max_items",
        summary=summary,
    )

    legacy_portal_repo_payload = str(redis_client.get("tater:portal_shop_manifest_urls") or "").strip()
    existing_core_repo_payload = str(redis_client.get("tater:core_shop_manifest_urls") or "").strip()
    if legacy_portal_repo_payload and not existing_core_repo_payload:
        redis_client.set("tater:core_shop_manifest_urls", legacy_portal_repo_payload)
        summary["string_keys_migrated"] += 1

    return summary


try:
    migration_summary = _migrate_legacy_surface_data()
    migrated_total = (
        int(migration_summary.get("string_keys_migrated") or 0)
        + int(migration_summary.get("hash_keys_migrated") or 0)
    )
    if migrated_total or int(migration_summary.get("keys_deleted") or 0):
        logging.getLogger("webui").info(
            "[surface-migration] Migrated legacy platform/core/portal data: %s",
            migration_summary,
        )
except Exception as exc:
    logging.getLogger("webui").error(
        "[surface-migration] Failed to migrate legacy platform/core/portal data: %s",
        exc,
        exc_info=True,
    )


missing_plugins = _enabled_missing_plugin_ids()

if missing_plugins:
    # Only show UI when we truly need downloads
    title = f"Restoring {len(missing_plugins)} missing plugin(s)…"

    # If your Streamlit supports st.modal, this feels like a real popup
    if hasattr(st, "modal"):
        with st.modal(title):
            status = st.empty()
            bar = st.progress(0.0)

            def progress_cb(p, txt):
                try:
                    bar.progress(max(0.0, min(1.0, float(p))))
                except Exception:
                    pass
                try:
                    status.write(txt)
                except Exception:
                    pass

            ensure_plugins_ready(progress_cb=progress_cb)
    else:
        # Fallback: inline status (still only shows when downloading)
        status = st.empty()
        bar = st.progress(0.0)

        def progress_cb(p, txt):
            try:
                bar.progress(max(0.0, min(1.0, float(p))))
            except Exception:
                pass
            try:
                status.write(txt)
            except Exception:
                pass

        ensure_plugins_ready(progress_cb=progress_cb)
else:
    ensure_plugins_ready()

missing_cores = _enabled_missing_core_ids()

if missing_cores:
    title = f"Restoring {len(missing_cores)} missing core(s)…"

    if hasattr(st, "modal"):
        with st.modal(title):
            status = st.empty()
            bar = st.progress(0.0)

            def progress_cb(p, txt):
                try:
                    bar.progress(max(0.0, min(1.0, float(p))))
                except Exception:
                    pass
                try:
                    status.write(txt)
                except Exception:
                    pass

            ensure_cores_ready(progress_cb=progress_cb)
    else:
        status = st.empty()
        bar = st.progress(0.0)

        def progress_cb(p, txt):
            try:
                bar.progress(max(0.0, min(1.0, float(p))))
            except Exception:
                pass
            try:
                status.write(txt)
            except Exception:
                pass

        ensure_cores_ready(progress_cb=progress_cb)
else:
    ensure_cores_ready()

missing_portals = _enabled_missing_portal_ids()

if missing_portals:
    title = f"Restoring {len(missing_portals)} missing portal(s)…"

    if hasattr(st, "modal"):
        with st.modal(title):
            status = st.empty()
            bar = st.progress(0.0)

            def progress_cb(p, txt):
                try:
                    bar.progress(max(0.0, min(1.0, float(p))))
                except Exception:
                    pass
                try:
                    status.write(txt)
                except Exception:
                    pass

            ensure_portals_ready(progress_cb=progress_cb)
    else:
        status = st.empty()
        bar = st.progress(0.0)

        def progress_cb(p, txt):
            try:
                bar.progress(max(0.0, min(1.0, float(p))))
            except Exception:
                pass
            try:
                status.write(txt)
            except Exception:
                pass

        ensure_portals_ready(progress_cb=progress_cb)
else:
    ensure_portals_ready()

core_registry = core_registry_module.refresh_core_registry()
portal_registry = portal_registry_module.refresh_portal_registry()

llm_client = get_llm_client_from_env()
logging.getLogger("webui").debug(f"LLM client → {build_llm_host_from_env()}")

# Set a main event loop reference for shared async helpers.
try:
    main_loop = asyncio.get_running_loop()
except RuntimeError:
    main_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(main_loop)
set_main_loop(main_loop)

first_name, last_name = get_tater_name()

st.set_page_config(
    page_title=f"{first_name} Chat",
    page_icon=":material/tooltip_2:"
)

assistant_avatar = get_tater_avatar()

# ----------------- SETTINGS HELPER FUNCTIONS -----------------
def get_homeassistant_settings():
    settings = redis_client.hgetall("homeassistant_settings")
    return {
        "HA_BASE_URL": settings.get("HA_BASE_URL", "http://homeassistant.local:8123"),
        "HA_TOKEN": settings.get("HA_TOKEN", ""),
    }

def save_homeassistant_settings(base_url: str, token: str) -> None:
    redis_client.hset(
        "homeassistant_settings",
        mapping={
            "HA_BASE_URL": base_url,
            "HA_TOKEN": token,
        },
    )


def get_vision_settings():
    settings = get_shared_vision_settings(
        default_api_base="http://127.0.0.1:1234",
        default_model="qwen2.5-vl-7b-instruct",
    )
    return {
        "api_base": settings.get("api_base") or "http://127.0.0.1:1234",
        "model": settings.get("model") or "qwen2.5-vl-7b-instruct",
        "api_key": settings.get("api_key") or "",
    }


def save_vision_settings(api_base: str, model: str, api_key: str) -> None:
    save_shared_vision_settings(api_base=api_base, model=model, api_key=api_key)


def get_emoji_responder_settings() -> Dict[str, Any]:
    settings = get_core_emoji_settings() or {}
    return {
        "enable_on_reaction_add": bool(settings.get("enable_on_reaction_add", True)),
        "enable_auto_reaction_on_reply": bool(settings.get("enable_auto_reaction_on_reply", True)),
        "reaction_chain_chance_percent": int(settings.get("reaction_chain_chance_percent", 100)),
        "reply_reaction_chance_percent": int(settings.get("reply_reaction_chance_percent", 12)),
        "reaction_chain_cooldown_seconds": int(settings.get("reaction_chain_cooldown_seconds", 30)),
        "reply_reaction_cooldown_seconds": int(settings.get("reply_reaction_cooldown_seconds", 120)),
        "min_message_length": int(settings.get("min_message_length", 4)),
    }


def save_emoji_responder_settings(
    *,
    enable_on_reaction_add: bool,
    enable_auto_reaction_on_reply: bool,
    reaction_chain_chance_percent: int,
    reply_reaction_chance_percent: int,
    reaction_chain_cooldown_seconds: int,
    reply_reaction_cooldown_seconds: int,
    min_message_length: int,
) -> None:
    save_core_emoji_settings(
        {
            "enable_on_reaction_add": bool(enable_on_reaction_add),
            "enable_auto_reaction_on_reply": bool(enable_auto_reaction_on_reply),
            "reaction_chain_chance_percent": max(0, min(100, int(reaction_chain_chance_percent))),
            "reply_reaction_chance_percent": max(0, min(100, int(reply_reaction_chance_percent))),
            "reaction_chain_cooldown_seconds": max(0, min(86_400, int(reaction_chain_cooldown_seconds))),
            "reply_reaction_cooldown_seconds": max(0, min(86_400, int(reply_reaction_cooldown_seconds))),
            "min_message_length": max(0, min(200, int(min_message_length))),
        }
    )


# ----------------- PROCESSING FUNCTIONS -----------------
async def process_message(
    user_name,
    message_content,
    input_artifacts=None,
    wait_callback=None,
    session_scope_id: Optional[str] = None,
    llm_client_override=None,
):
    max_llm = int(redis_client.get("tater:max_llm") or 8)
    history = load_chat_history_tail(max_llm)

    loop_messages = []
    for msg in history:
        role = msg["role"]
        content = msg["content"]
        if role not in ("user", "assistant"):
            role = "assistant"
        templ_msg = _to_template_msg(role, content)
        if templ_msg is not None:
            loop_messages.append(templ_msg)
    loop_messages = _enforce_user_assistant_alternation(loop_messages)
    messages_list = loop_messages

    merged_registry = dict(get_registry() or {})
    merged_enabled = get_plugin_enabled

    resolved_scope_id = str(session_scope_id or "").strip()
    if not resolved_scope_id:
        resolved_scope_id = _ensure_webui_session_id()

    origin = {
        "platform": "webui",
        "user": user_name,
        "user_id": user_name,
        "session_id": resolved_scope_id,
    }
    if isinstance(input_artifacts, list) and input_artifacts:
        origin["input_artifacts"] = [dict(item) for item in input_artifacts if isinstance(item, dict)]
    agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
    result = await run_cerberus_turn(
        llm_client=(llm_client_override or llm_client),
        platform="webui",
        history_messages=messages_list,
        registry=merged_registry,
        enabled_predicate=merged_enabled,
        context={"raw_message": message_content},
        user_text=message_content or "",
        scope=f"session:{resolved_scope_id}",
        origin=origin,
        redis_client=redis_client,
        wait_callback=wait_callback,
        max_rounds=agent_max_rounds,
        max_tool_calls=agent_max_tool_calls,
        platform_preamble="",
    )
    responses = []
    if result.get("text"):
        responses.append(result["text"])
    for item in result.get("artifacts") or []:
        responses.append(_normalize_plugin_response_item(item))
    return {"responses": responses, "agent": True}


# ------------------ NAVIGATION ------------------
ai_tasks_enabled = str(redis_client.get("ai_task_core_running") or "").strip().lower() == "true"
memory_core_enabled = str(redis_client.get("memory_core_running") or "").strip().lower() == "true"
nav_options = ["Chat", "Verba Manager", "Portal Manager", "Core Manager", "Settings"]
if ai_tasks_enabled:
    nav_options.insert(1, "AI Tasks")
if memory_core_enabled:
    insert_idx = nav_options.index("Verba Manager") if "Verba Manager" in nav_options else 1
    nav_options.insert(insert_idx, "Memory")
if "active_view" not in st.session_state:
    st.session_state.active_view = nav_options[0]
elif st.session_state.active_view in {"Plugins", "Verba Plugins", "Verba's", "Plugin Manager", "Auto Plugins", "Automation Plugins"}:
    st.session_state.active_view = "Verba Manager"
elif st.session_state.active_view == "AI Tasks" and not ai_tasks_enabled:
    st.session_state.active_view = "Core Manager"
elif st.session_state.active_view == "Memory" and not memory_core_enabled:
    st.session_state.active_view = "Core Manager"
elif st.session_state.active_view not in nav_options:
    st.session_state.active_view = nav_options[0]

st.sidebar.markdown("**Navigation**")
for opt in nav_options:
    if st.sidebar.button(opt, width="stretch", key=f"nav_btn_{opt}"):
        st.session_state.active_view = opt
        st.rerun()

active_view = st.session_state.active_view
st.sidebar.markdown("---")

# ------------------ CORE MANAGEMENT ------------------
for core in core_registry:
    key = core["key"]  # e.g. ai_task_core
    state_key = f"{key}_running"

    core_should_run = redis_client.get(state_key) == "true"

    if core_should_run:
        if not _core_thread_alive(key) and _should_autostart("core", key):
            _start_core(key)

# ------------------ PORTAL MANAGEMENT ------------------
for portal in portal_registry:
    key = portal["key"]  # e.g. irc_portal
    state_key = f"{key}_running"

    # Check Redis to determine if this portal should be running
    portal_should_run = redis_client.get(state_key) == "true"

    if portal_should_run:
        if not _portal_thread_alive(key) and _should_autostart("portal", key):
            _start_portal(key)

# Ensure chat history is available for any view
if "chat_messages" not in st.session_state:
    full_history = load_chat_history()
    max_display = int(redis_client.get("tater:max_display") or 8)
    st.session_state.chat_messages = full_history[-max_display:]

if active_view == "Chat":
    st.title(f"{first_name} Chat Web UI")

    chat_settings = get_chat_settings()
    avatar_b64  = chat_settings.get("avatar")
    user_avatar = load_avatar_image(avatar_b64) if avatar_b64 else None
    session_scope_id = _ensure_webui_session_id()

    finished_jobs = _collect_finished_chat_jobs(session_id=session_scope_id)
    for update in finished_jobs:
        status = str(update.get("status") or "").strip().lower()
        if status == "done":
            for item in (update.get("responses") or []):
                st.session_state.chat_messages.append({"role": "assistant", "content": item})
                save_message("assistant", "assistant", item)
        elif status == "error":
            label = str(update.get("label") or "request")
            err = str(update.get("error") or "unknown error")
            msg = f"I couldn't finish this request ({label}): {err}"
            st.session_state.chat_messages.append({"role": "assistant", "content": msg})
            save_message("assistant", "assistant", msg)

    # ------------------ render chat history ------------------
    for msg in st.session_state.chat_messages:
        role = msg["role"]
        avatar = user_avatar if role == "user" else assistant_avatar
        content = msg["content"]

        while isinstance(content, dict) and content.get("marker") in ("plugin_response", "plugin_wait"):
            content = content.get("content")

        if isinstance(content, dict) and content.get("marker") == "plugin_call":
            continue

        with st.chat_message(role, avatar=avatar):
            # ---------- IMAGE ----------
            if isinstance(content, dict) and content.get("type") == "image":
                blob = _get_media_blob_from_content(content)
                if blob is None:
                    st.warning("Image missing/expired.")
                else:
                    mimetype = content.get("mimetype") or "image/png"
                    name = content.get("name", "")

                    if mimetype == "image/webp":
                        b64 = base64.b64encode(blob).decode("utf-8")
                        html = (
                            f'<img src="data:image/webp;base64,{b64}" '
                            f'alt="{name}" style="max-width: 100%; border-radius: 0.5rem;">'
                        )
                        st.markdown(html, unsafe_allow_html=True)
                    else:
                        try:
                            st.image(Image.open(BytesIO(blob)), caption=name)
                        except Exception:
                            st.image(blob, caption=name)

            # ---------- AUDIO ----------
            elif isinstance(content, dict) and content.get("type") == "audio":
                blob = _get_media_blob_from_content(content)
                if blob is None:
                    st.warning("Audio missing/expired.")
                else:
                    st.audio(blob, format=content.get("mimetype") or "audio/mpeg")

            # ---------- VIDEO ----------
            elif isinstance(content, dict) and content.get("type") == "video":
                blob = _get_media_blob_from_content(content)
                if blob is None:
                    st.warning("Video missing/expired.")
                else:
                    st.video(blob, format=content.get("mimetype") or "video/mp4")

            # ---------- FILE (unchanged) ----------
            elif isinstance(content, dict) and content.get("type") == "file":
                file_id = content.get("id")
                name = content.get("name") or "file"
                mimetype = content.get("mimetype") or "application/octet-stream"
                size = int(content.get("size") or 0)

                blob = _load_file_blob_from_redis(file_id) if file_id else None
                if blob is None:
                    st.warning("Attachment expired or missing.")
                else:
                    if mimetype.startswith("image/"):
                        try:
                            st.image(Image.open(BytesIO(blob)), caption=name)
                        except Exception:
                            st.caption(f"📎 {name} ({_bytes_to_mb(size):.2f} MB)")
                            st.download_button(
                                label=f"Download {name}",
                                data=blob,
                                file_name=name,
                                mime=mimetype,
                                width="stretch"
                            )

                    elif mimetype.startswith("audio/"):
                        st.audio(blob, format=mimetype)

                    elif mimetype.startswith("video/"):
                        st.video(blob, format=mimetype)

                    else:
                        st.caption(f"📎 {name} ({_bytes_to_mb(size):.2f} MB)")
                        st.download_button(
                            label=f"Download {name}",
                            data=blob,
                            file_name=name,
                            mime=mimetype,
                            width="stretch"
                        )

            else:
                st.write(content)

    active_jobs = _list_active_chat_jobs(session_id=session_scope_id)
    poll_interval = "2s" if active_jobs else None

    @st.fragment(run_every=poll_interval)
    def _render_active_job_statuses():
        latest_jobs = _list_active_chat_jobs(session_id=session_scope_id)
        for job in latest_jobs:
            current_tool = str(job.get("current_tool") or "").strip()
            label = f"{first_name} is working on: {current_tool}" if current_tool else f"{first_name} is thinking…"
            if hasattr(st, "status"):
                st.status(
                    label,
                    state="running",
                    expanded=False,
                )
            else:
                st.info(label)

        completed = _collect_finished_chat_jobs(session_id=session_scope_id)
        if completed:
            for update in completed:
                status = str(update.get("status") or "").strip().lower()
                if status == "done":
                    for item in (update.get("responses") or []):
                        st.session_state.chat_messages.append({"role": "assistant", "content": item})
                        save_message("assistant", "assistant", item)
                elif status == "error":
                    label = str(update.get("label") or "request")
                    err = str(update.get("error") or "unknown error")
                    msg = f"I couldn't finish this request ({label}): {err}"
                    st.session_state.chat_messages.append({"role": "assistant", "content": msg})
                    save_message("assistant", "assistant", msg)
            st.rerun(scope="app")

    _render_active_job_statuses()

    # ------------------ Chat Input (NOW SUPPORTS FILES) ------------------
    # Streamlit versions differ on chat_input kwargs (e.g., max_upload_size)
    _chat_kwargs = {
        "accept_file": "multiple",  # change to "directory" if you want folder uploads
        "max_upload_size": WEBUI_ATTACH_MAX_MB_EACH,
    }
    try:
        prompt = st.chat_input(f"Chat with {first_name}…", **_chat_kwargs)
    except TypeError:
        # Fallback for older Streamlit versions without max_upload_size
        _chat_kwargs.pop("max_upload_size", None)
        prompt = st.chat_input(f"Chat with {first_name}…", **_chat_kwargs)

    user_text = (getattr(prompt, "text", None) or "").strip() if prompt else ""
    files = list(getattr(prompt, "files", None) or []) if prompt else []

    if user_text or files:
        uname = chat_settings["username"]
        input_artifacts = []

        # Validate total size early
        total_bytes = 0
        for uf in files:
            try:
                total_bytes += len(uf.getvalue())
            except Exception:
                pass

        if _bytes_to_mb(total_bytes) > WEBUI_ATTACH_MAX_MB_TOTAL:
            st.error(
                f"Total upload size ({_bytes_to_mb(total_bytes):.2f}MB) exceeds limit of {WEBUI_ATTACH_MAX_MB_TOTAL}MB."
            )
            st.stop()

        # Save text message (if any)
        if user_text:
            save_message("user", uname, user_text)
            st.session_state.chat_messages.append({"role": "user", "content": user_text})
            st.chat_message("user", avatar=user_avatar or "🦖").write(user_text)

        # Save each uploaded file as a chat message (metadata only; blob stored separately)
        for uf in files:
            data = uf.getvalue()
            mimetype = getattr(uf, "type", "") or "application/octet-stream"
            name = getattr(uf, "name", None) or "file"

            file_id = str(uuid.uuid4())
            _store_file_blob_in_redis(file_id, data)

            msg = {
                "type": _media_type_from_mimetype(mimetype),
                "id": file_id,
                "name": name,
                "mimetype": mimetype,
                "size": len(data),
            }
            save_message("user", uname, msg)
            st.session_state.chat_messages.append({"role": "user", "content": msg})
            input_artifacts.append(
                {
                    "type": msg["type"],
                    "file_id": file_id,
                    "name": name,
                    "mimetype": mimetype,
                    "size": len(data),
                    "source": "webui_attachment",
                }
            )

        _enqueue_chat_job(
            user_name=uname,
            message_content=user_text,
            input_artifacts=input_artifacts,
            session_scope_id=session_scope_id,
        )

        st.rerun()

    # ------------------ perf stats ------------------
    if (redis_client.get("tater:show_speed_stats") or "true").lower() == "true":
        stats = redis_client.hgetall("webui:last_llm_stats")
        if stats:
            try:
                model = stats.get("model") or "LLM"
                elapsed = float(stats.get("elapsed", "0"))
                prompt_tokens = int(stats.get("prompt_tokens", "0"))
                completion_tokens = int(stats.get("completion_tokens", "0"))
                total_tokens = int(stats.get("total_tokens", "0"))
                tps_total = float(stats.get("tps_total", "0"))
                tps_comp_str = stats.get("tps_comp") or ""
                comp_part = f" | completion: {tps_comp_str} tok/s" if tps_comp_str else ""
                st.caption(
                    f"⚡️ {model} — {tps_total:.0f} tok/s{comp_part} • "
                    f"{total_tokens} tok in {elapsed:.2f}s (prompt {prompt_tokens}, completion {completion_tokens})"
                )
            except Exception:
                pass

elif active_view == "AI Tasks":
    render_ai_tasks_page(redis_client=redis_client)

elif active_view == "Memory":
    render_memory_page()

elif active_view == "Verba Manager":
    render_plugin_store_page()

elif active_view == "Portal Manager":
    render_portal_store_page(
        portal_registry=portal_registry,
        start_portal_fn=_start_portal,
        stop_portal_fn=_stop_portal,
        wipe_memory_core_data_fn=wipe_memory_core_data,
    )

elif active_view == "Core Manager":
    render_core_store_page(
        core_registry=core_registry,
        start_core_fn=_start_core,
        stop_core_fn=_stop_core,
        wipe_memory_core_data_fn=wipe_memory_core_data,
    )

elif active_view == "Settings":
    render_settings_page(
        redis_client=redis_client,
        redis_blob_client=redis_blob_client,
        first_name=first_name,
        last_name=last_name,
        get_registry_fn=get_registry,
        admin_gate_key=ADMIN_GATE_KEY,
        get_admin_only_plugins_fn=get_admin_only_plugins,
        get_chat_settings_fn=get_chat_settings,
        save_chat_settings_fn=save_chat_settings,
        clear_chat_history_fn=clear_chat_history,
        get_homeassistant_settings_fn=get_homeassistant_settings,
        save_homeassistant_settings_fn=save_homeassistant_settings,
        get_vision_settings_fn=get_vision_settings,
        save_vision_settings_fn=save_vision_settings,
        get_emoji_responder_settings_fn=get_emoji_responder_settings,
        save_emoji_responder_settings_fn=save_emoji_responder_settings,
        webui_attach_max_mb_each=WEBUI_ATTACH_MAX_MB_EACH,
        webui_attach_max_mb_total=WEBUI_ATTACH_MAX_MB_TOTAL,
        webui_attach_ttl_seconds=WEBUI_ATTACH_TTL_SECONDS,
        file_index_key=FILE_INDEX_KEY,
        file_blob_key_prefix=FILE_BLOB_KEY_PREFIX,
        render_cerberus_settings_fn=render_cerberus_settings,
        render_cerberus_metrics_dashboard_fn=render_cerberus_metrics_dashboard,
        render_cerberus_data_tools_fn=render_cerberus_data_tools,
    )
