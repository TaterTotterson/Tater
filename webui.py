# webui.py
import streamlit as st
import redis
import os
import time
import json
import re
import asyncio
import dotenv
import logging
import base64
import requests
import importlib
import threading
import sys
import uuid
import hashlib
import feedparser
import plugin_registry as plugin_registry_mod
from urllib.parse import urljoin
from datetime import datetime
from typing import Any, Dict, List, Optional
from PIL import Image
from io import BytesIO
from platform_registry import platform_registry
from plugin_loader import load_plugins_from_directory
from plugin_kernel import normalize_platform
from helpers import (
    run_async,
    set_main_loop,
    get_tater_name,
    get_tater_personality,
    get_llm_client_from_env,
    build_llm_host_from_env
)
from plugin_settings import (
    get_plugin_enabled,
    set_plugin_enabled,
    get_plugin_settings,
    save_plugin_settings,
)
from admin_gate import (
    REDIS_KEY as ADMIN_GATE_KEY,
    CREATION_GATE_KEY,
    get_admin_only_plugins,
    is_agent_lab_creation_admin_gated,
)
from agent_lab_registry import build_agent_registry
from rss_store import get_all_feeds, set_feed, update_feed, delete_feed
from kernel_tools import (
    AGENT_PLUGINS_DIR,
    AGENT_PLATFORMS_DIR,
    validate_plugin,
    validate_platform,
    delete_file,
    memory_list,
    memory_delete,
)
from cerberus import (
    run_cerberus_turn,
    resolve_agent_limits,
    DEFAULT_MAX_ROUNDS,
    DEFAULT_MAX_TOOL_CALLS,
    DEFAULT_AGENT_STATE_TTL_SECONDS,
    DEFAULT_PLANNER_MAX_TOKENS,
    DEFAULT_CHECKER_MAX_TOKENS,
    DEFAULT_DOER_MAX_TOKENS,
    DEFAULT_TOOL_REPAIR_MAX_TOKENS,
    DEFAULT_OVERCLAR_REPAIR_MAX_TOKENS,
    DEFAULT_SEND_REPAIR_MAX_TOKENS,
    DEFAULT_RECOVERY_MAX_TOKENS,
    DEFAULT_MAX_LEDGER_ITEMS,
    AGENT_MAX_ROUNDS_KEY,
    AGENT_MAX_TOOL_CALLS_KEY,
    CERBERUS_AGENT_STATE_TTL_SECONDS_KEY,
    CERBERUS_PLANNER_MAX_TOKENS_KEY,
    CERBERUS_CHECKER_MAX_TOKENS_KEY,
    CERBERUS_DOER_MAX_TOKENS_KEY,
    CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY,
    CERBERUS_OVERCLAR_REPAIR_MAX_TOKENS_KEY,
    CERBERUS_SEND_REPAIR_MAX_TOKENS_KEY,
    CERBERUS_RECOVERY_MAX_TOKENS_KEY,
    CERBERUS_MAX_LEDGER_ITEMS_KEY,
)
from vision_settings import (
    get_vision_settings as get_shared_vision_settings,
    save_vision_settings as save_shared_vision_settings,
)
from conversation_media_refs import load_recent_media_refs, save_media_ref
from emoji_responder import get_emoji_settings as get_core_emoji_settings, save_emoji_settings as save_core_emoji_settings

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
WEBUI_IMAGE_SCOPE = "chat"

# ------------------ Plugin Store / Installed Plugins ------------------
PLUGIN_DIR = os.getenv("TATER_PLUGIN_DIR", "plugins")  # where installed plugin .py files live
SHOP_MANIFEST_URL_DEFAULT = os.getenv(
    "TATER_SHOP_MANIFEST_URL",
    "https://raw.githubusercontent.com/TaterTotterson/Tater_Shop/main/manifest.json"
)
RETIRED_PLUGIN_IDS = {
    "web_search",
    "send_message",
    "notify_discord",
    "notify_irc",
    "notify_matrix",
    "notify_homeassistant",
    "notify_ntfy",
    "notify_telegram",
    "notify_wordpress",
}

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

# ------------------ RESTORE ENABLED PLUGINS IF MISSING ------------------

def _enabled_missing_plugin_ids() -> list[str]:
    """
    Returns a list of plugin ids that are ENABLED in Redis but missing on disk.
    This is fast and lets us avoid showing UI unless we truly need to download.
    """
    def _to_bool(v) -> bool:
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("true", "1", "yes", "on")

    missing: list[str] = []
    seen = set()

    try:
        enabled_states = redis_client.hgetall("plugin_enabled") or {}
    except Exception:
        return missing

    for pid, raw in enabled_states.items():
        # normalize redis bytes -> str (just in case)
        if isinstance(pid, (bytes, bytearray)):
            pid = pid.decode("utf-8", "ignore")
        pid = str(pid).strip()
        if not pid:
            continue
        if pid in RETIRED_PLUGIN_IDS:
            try:
                redis_client.hdel("plugin_enabled", pid)
            except Exception:
                pass
            continue

        if _to_bool(raw) and not is_plugin_installed(pid):
            if pid not in seen:
                seen.add(pid)
                missing.append(pid)

    return missing

def _safe_plugin_file_path(plugin_id: str) -> str:
    # plugins are single .py files; enforce simple ids
    if not re.fullmatch(r"[a-zA-Z0-9_\-]+", plugin_id or ""):
        raise ValueError("Invalid plugin id")
    return os.path.join(PLUGIN_DIR, f"{plugin_id}.py")

def fetch_shop_manifest(url: str) -> dict:
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def is_plugin_installed(plugin_id: str) -> bool:
    try:
        return os.path.exists(_safe_plugin_file_path(plugin_id))
    except Exception:
        return False

def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def install_plugin_from_shop_item(item: dict, manifest_url: str) -> tuple[bool, str]:
    """
    Downloads a plugin .py from the shop manifest entry, verifies sha256 if provided,
    and writes it to PLUGIN_DIR as <id>.py.
    Supports relative 'entry' paths.
    """
    try:
        plugin_id = (item.get("id") or "").strip()
        entry = (item.get("entry") or "").strip()
        expected_sha = (item.get("sha256") or "").strip().lower()

        if not plugin_id:
            return False, "Manifest item missing 'id'."
        if not entry:
            return False, f"{plugin_id}: manifest item missing 'entry'."

        # Resolve relative paths against the manifest URL
        entry = entry.lstrip("/")  # IMPORTANT: urljoin breaks raw GitHub paths if entry starts with /
        full_url = urljoin(manifest_url, entry)

        path = _safe_plugin_file_path(plugin_id)
        os.makedirs(PLUGIN_DIR, exist_ok=True)

        r = requests.get(full_url, timeout=30)
        r.raise_for_status()
        data = r.content

        # Verify checksum if present
        if expected_sha:
            got = _sha256_bytes(data)
            if got.lower() != expected_sha:
                return False, f"SHA256 mismatch for {plugin_id}. expected={expected_sha} got={got}"

        try:
            text = data.decode("utf-8")
        except Exception:
            return False, f"{plugin_id}: downloaded file is not valid UTF-8 text."

        if "class " not in text and "def " not in text:
            return False, f"{plugin_id}: file does not look like a python plugin."

        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

        return True, f"Installed {plugin_id}"
    except Exception as e:
        return False, f"Install failed: {e}"

def uninstall_plugin_file(plugin_id: str) -> tuple[bool, str]:
    """
    Remove only the plugin .py file.
    Do NOT clear Redis settings.
    """
    try:
        path = _safe_plugin_file_path(plugin_id)
        if not os.path.exists(path):
            return True, "Plugin file not found (already removed)."

        os.remove(path)
        return True, f"Removed {path}"
    except Exception as e:
        return False, f"Uninstall failed: {e}"

# ------------------ OPTIONAL: CLEAN REDIS DATA FOR A PLUGIN ------------------

def clear_plugin_redis_data(plugin_id: str, category_hint: str | None = None) -> tuple[bool, str]:
    """
    Best-effort cleanup for plugin-related Redis keys.

    What we delete:
      - plugin_settings:<category> (if we can determine the category)
      - plugin_enabled hash field for this plugin_id
    """
    try:
        deleted = []

        # 1) Delete settings hash (plugin_settings:<category>)
        category = (category_hint or "").strip() or None
        if not category:
            loaded = get_registry().get(plugin_id)
            category = getattr(loaded, "settings_category", None) if loaded else None

        if category:
            settings_key = f"plugin_settings:{category}"
            if redis_client.exists(settings_key):
                redis_client.delete(settings_key)
                deleted.append(settings_key)

        # 2) Delete enabled toggle field (hash field inside "plugin_enabled")
        # (matches plugin_settings.py)
        if redis_client.hexists("plugin_enabled", plugin_id):
            redis_client.hdel("plugin_enabled", plugin_id)
            deleted.append(f"plugin_enabled[{plugin_id}]")

        if deleted:
            return True, "Deleted: " + ", ".join(deleted)

        return True, "No Redis keys found for this plugin."
    except Exception as e:
        return False, f"Redis cleanup failed: {e}"

def _refresh_plugins_after_fs_change():
    plugin_registry_mod.reload_plugins()

def auto_restore_missing_plugins(manifest_url: str, progress_cb=None) -> tuple[bool, list[str], list[str]]:
    """
    Restore any plugins that are ENABLED in Redis but missing on disk.
    Uses the shop manifest as the source of install URLs.

    progress_cb (optional): callable(progress_float_0_to_1, status_text)
    """
    enabled_missing: list[str] = []
    restored: list[str] = []
    changed = False

    # Find enabled plugins that are missing on disk
    try:
        enabled_states = redis_client.hgetall("plugin_enabled") or {}
    except Exception as e:
        logging.error(f"[restore] Failed to read plugin_enabled: {e}")
        return changed, restored, enabled_missing

    for plugin_id, raw in enabled_states.items():
        enabled = str(raw).lower() == "true"
        if enabled and not is_plugin_installed(plugin_id):
            enabled_missing.append(plugin_id)

    if not enabled_missing:
        return changed, restored, enabled_missing

    total = len(enabled_missing)
    if progress_cb:
        try:
            progress_cb(0.0, f"Found {total} enabled plugin(s) missing — preparing downloads…")
        except Exception:
            pass

    # Load shop manifest
    try:
        manifest = fetch_shop_manifest(manifest_url)
    except Exception as e:
        logging.error(f"[restore] Failed to load manifest: {e}")
        if progress_cb:
            try:
                progress_cb(0.0, f"Failed to load manifest: {e}")
            except Exception:
                pass
        return changed, restored, enabled_missing

    items = manifest.get("plugins") or manifest.get("items") or manifest.get("data") or []
    if not isinstance(items, list):
        logging.error("[restore] Manifest format unexpected.")
        if progress_cb:
            try:
                progress_cb(0.0, "Manifest format unexpected (expected list under plugins/items/data).")
            except Exception:
                pass
        return changed, restored, enabled_missing

    # Index manifest by id
    by_id: dict[str, dict] = {}
    for item in items:
        pid = (item.get("id") or "").strip()
        if pid:
            by_id[pid] = item

    # Download/install missing enabled plugins with progress updates
    for idx, pid in enumerate(enabled_missing, start=1):
        item = by_id.get(pid)
        if not item:
            logging.error(f"[restore] {pid} enabled but not found in manifest")
            # Plugin is enabled, missing on disk, and cannot be restored from the shop.
            # Remove stale enabled flag so it no longer blocks startup with restore prompts.
            try:
                redis_client.hdel("plugin_enabled", pid)
                logging.info(f"[restore] Removed stale plugin_enabled key for {pid}")
            except Exception as e:
                logging.error(f"[restore] Failed to remove stale plugin_enabled key for {pid}: {e}")
            if progress_cb:
                try:
                    progress_cb(
                        (idx - 1) / max(1, total),
                        f"{pid} missing and not in manifest; removed stale enable key ({idx}/{total})",
                    )
                except Exception:
                    pass
            continue

        if progress_cb:
            try:
                progress_cb((idx - 1) / max(1, total), f"Downloading {pid}… ({idx}/{total})")
            except Exception:
                pass

        ok, msg = install_plugin_from_shop_item(item, manifest_url)
        if ok:
            restored.append(pid)
            changed = True
            logging.info(f"[restore] {pid}: {msg}")
        else:
            logging.error(f"[restore] {pid}: {msg}")

        if progress_cb:
            try:
                progress_cb(idx / max(1, total), f"Finished {pid} ({idx}/{total})")
            except Exception:
                pass

    return changed, restored, enabled_missing

def ensure_plugins_ready(progress_cb=None):
    """
    Ensure any ENABLED plugins that are missing on disk are restored from the shop.

    progress_cb (optional): callable(progress_float_0_to_1, status_text)
    """
    os.makedirs(PLUGIN_DIR, exist_ok=True)

    shop_url = (redis_client.get("tater:shop_manifest_url") or SHOP_MANIFEST_URL_DEFAULT)
    shop_url = (shop_url or "").strip()

    if not shop_url:
        # No manifest URL configured; nothing we can do.
        if progress_cb:
            try:
                progress_cb(1.0, "Plugin shop manifest URL is not configured.")
            except Exception:
                pass
        return

    # Fast pre-check so the UI can decide whether to show popup/progress
    missing = _enabled_missing_plugin_ids()
    if not missing:
        if progress_cb:
            try:
                progress_cb(1.0, "All enabled plugins are present.")
            except Exception:
                pass
        return

    if progress_cb:
        try:
            progress_cb(0.0, f"Restoring {len(missing)} missing plugin(s)…")
        except Exception:
            pass

    changed, restored, enabled_missing = auto_restore_missing_plugins(
        shop_url,
        progress_cb=progress_cb
    )

    if changed:
        if progress_cb:
            try:
                progress_cb(0.98, "Reloading plugins…")
            except Exception:
                pass
        _refresh_plugins_after_fs_change()

    # Don’t spam “Restored: …” or “Failed: …” here.
    # If you want *any* final message, keep it short/blank.
    if progress_cb:
        try:
            # Empty text so your popup can close cleanly without showing a giant list.
            progress_cb(1.0, "")
        except Exception:
            pass

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
def _platform_runtime():
    # Shared across reruns/sessions within this Streamlit process.
    return {
        "lock": threading.RLock(),
        "threads": {},
        "stop_flags": {},
    }


@st.cache_resource
def _exp_platform_runtime():
    return {
        "lock": threading.RLock(),
        "threads": {},
        "stop_flags": {},
    }

AUTO_START_COOLDOWN_SEC = 30


def _platform_thread_alive(key: str) -> bool:
    runtime = _platform_runtime()
    with runtime["lock"]:
        thread = runtime["threads"].get(key)
        return bool(thread and thread.is_alive())


def _exp_platform_thread_alive(key: str) -> bool:
    runtime = _exp_platform_runtime()
    with runtime["lock"]:
        thread = runtime["threads"].get(key)
        return bool(thread and thread.is_alive())


def _should_autostart(key: str, exp: bool = False) -> bool:
    bucket = "exp" if exp else "platform"
    token = f"{bucket}:{key}"
    attempts = st.session_state.setdefault("autostart_attempts", set())
    if token in attempts:
        return False

    attempts.add(token)
    redis_key = f"webui:autostart:{bucket}:{key}"
    last = redis_client.get(redis_key)
    if last:
        try:
            if time.time() - float(last) < AUTO_START_COOLDOWN_SEC:
                return False
        except Exception:
            pass

    redis_client.set(redis_key, str(time.time()))
    return True


def _start_platform(key: str):
    runtime = _platform_runtime()
    with runtime["lock"]:
        thread = runtime["threads"].get(key)
        stop_flag = runtime["stop_flags"].get(key)

        if thread and thread.is_alive():
            return thread, stop_flag

        stop_flag = threading.Event()

    def runner():
        try:
            module = importlib.import_module(f"platforms.{key}")
            if hasattr(module, "run"):
                module.run(stop_event=stop_flag)
            else:
                logging.getLogger("webui").warning(f"⚠️ No run(stop_event) in platforms.{key}")
        except Exception as e:
            logging.getLogger("webui").error(f"❌ Error in platform {key}: {e}", exc_info=True)
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

def _stop_platform(key: str):
    runtime = _platform_runtime()
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


def _start_agent_lab_platform(key: str, path: str):
    runtime = _exp_platform_runtime()
    with runtime["lock"]:
        thread = runtime["threads"].get(key)
        stop_flag = runtime["stop_flags"].get(key)

        if thread and thread.is_alive():
            return thread, stop_flag

        stop_flag = threading.Event()

    def runner():
        try:
            spec = importlib.util.spec_from_file_location(f"agent_lab_platform_{key}_{int(time.time())}", path)
            if spec is None or spec.loader is None:
                logging.getLogger("webui").warning(f"⚠️ Agent Lab platform {key} has no loader.")
                return
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore[attr-defined]
            run_fn = getattr(module, "run", None)
            if callable(run_fn):
                run_fn(stop_event=stop_flag)
            else:
                logging.getLogger("webui").warning(f"⚠️ Agent Lab platform {key} missing run().")
        except Exception as e:
            logging.getLogger("webui").error(f"❌ Error in Agent Lab platform {key}: {e}", exc_info=True)
        finally:
            with runtime["lock"]:
                current = runtime["threads"].get(key)
                if current is threading.current_thread():
                    runtime["threads"].pop(key, None)
                    runtime["stop_flags"].pop(key, None)

    thread = threading.Thread(target=runner, daemon=True, name=f"agent-lab-platform-{key}")
    with runtime["lock"]:
        runtime["threads"][key] = thread
        runtime["stop_flags"][key] = stop_flag
    thread.start()

    return thread, stop_flag


def _stop_agent_lab_platform(key: str):
    runtime = _exp_platform_runtime()
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


def _discover_agent_lab_plugins():
    AGENT_PLUGINS_DIR.mkdir(parents=True, exist_ok=True)
    plugins = load_plugins_from_directory(str(AGENT_PLUGINS_DIR), id_from_filename=True)
    items = []
    for path in sorted(AGENT_PLUGINS_DIR.glob("*.py")):
        name = path.stem
        plugin = plugins.get(name)
        items.append({"name": name, "plugin": plugin, "path": path, "loaded": bool(plugin)})
    return items


def _discover_agent_lab_platforms():
    AGENT_PLATFORMS_DIR.mkdir(parents=True, exist_ok=True)
    items = []
    for path in sorted(AGENT_PLATFORMS_DIR.glob("*.py")):
        name = path.stem
        label = name
        ok = True
        error = ""
        required = {}
        try:
            spec = importlib.util.spec_from_file_location(f"exp_platform_meta_{name}_{int(time.time())}", str(path))
            if spec is None or spec.loader is None:
                ok = False
                error = "Missing loader"
            else:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)  # type: ignore[attr-defined]
                meta = getattr(module, "PLATFORM", None)
                if isinstance(meta, dict):
                    label = meta.get("label") or meta.get("category") or meta.get("key") or label
                    required = meta.get("required") or {}
                else:
                    ok = False
                    error = "Missing PLATFORM dict"
                if not callable(getattr(module, "run", None)):
                    ok = False
                    error = "Missing run()"
        except Exception as e:
            ok = False
            error = str(e)
        items.append({"key": name, "label": label, "path": str(path), "ok": ok, "error": error, "required": required})
    return items

# ---- background job refresh hook ----
if redis_client.get("webui:needs_rerun") == "true":
    redis_client.delete("webui:needs_rerun")
    st.rerun()

missing = _enabled_missing_plugin_ids()

if missing:
    # Only show UI when we truly need downloads
    title = f"Restoring {len(missing)} missing plugin(s)…"

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

llm_client = get_llm_client_from_env()
logging.getLogger("webui").debug(f"LLM client → {build_llm_host_from_env()}")

# Set the main event loop used for run_async.
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

def save_message(role, username, content):
    message_data = {
        "role": role,
        "username": username,
        "content": content
    }

    # renamed from `key` -> `history_key` (no behavior change)
    history_key = "webui:chat_history"
    redis_client.rpush(history_key, json.dumps(message_data))

    try:
        max_store = int(redis_client.get("tater:max_store") or 20)
    except (ValueError, TypeError):
        max_store = 20

    if max_store > 0:
        redis_client.ltrim(history_key, -max_store, -1)


def _media_type_from_mimetype(mimetype: str) -> str:
    mm = str(mimetype or "").strip().lower()
    if mm.startswith("image/"):
        return "image"
    if mm.startswith("audio/"):
        return "audio"
    if mm.startswith("video/"):
        return "video"
    return "file"


def _extract_media_refs(content):
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        return _extract_media_refs(content.get("content"))
    if isinstance(content, list):
        refs = []
        for item in content:
            refs.extend(_extract_media_refs(item))
        return refs
    if not isinstance(content, dict):
        return []

    media_type = str(content.get("type") or "").strip().lower()
    mimetype = str(content.get("mimetype") or "").strip().lower()
    inferred_type = media_type if media_type in {"image", "audio", "video", "file"} else _media_type_from_mimetype(mimetype)
    if inferred_type not in {"image", "audio", "video", "file"}:
        return []

    ref = {
        "type": inferred_type,
        "file_id": str(content.get("id") or "").strip() or None,
        "blob_key": str(content.get("blob_key") or "").strip() or None,
        "name": str(content.get("name") or f"{inferred_type}.bin").strip() or f"{inferred_type}.bin",
        "mimetype": mimetype or "application/octet-stream",
        "source": "webui",
        "updated_at": time.time(),
    }
    if not ref.get("file_id") and not ref.get("blob_key"):
        return []
    return [ref]


def _save_recent_webui_media_refs(content):
    refs = _extract_media_refs(content)
    if not refs:
        return
    for ref in refs:
        try:
            save_media_ref(
                redis_client,
                platform="webui",
                scope=WEBUI_IMAGE_SCOPE,
                ref=ref,
            )
        except Exception:
            continue


def load_chat_history_tail(n: int):
    if n <= 0:
        return []
    raw = redis_client.lrange("webui:chat_history", -n, -1)
    out = []
    for msg in raw:
        try:
            out.append(json.loads(msg))
        except Exception:
            continue
    return out

def load_chat_history():
    history = redis_client.lrange("webui:chat_history", 0, -1)
    return [json.loads(msg) for msg in history]

def clear_chat_history():
    # Clear persisted history
    redis_client.delete("webui:chat_history")
    # Clear in-memory session list
    st.session_state.pop("chat_messages", None)

def load_default_tater_avatar():
    return Image.open("images/tater.png")

def get_tater_avatar():
    avatar_b64 = redis_client.get("tater:avatar")
    if avatar_b64:
        try:
            avatar_bytes = base64.b64decode(avatar_b64)
            return Image.open(BytesIO(avatar_bytes))
        except Exception:
            redis_client.delete("tater:avatar")
    return load_default_tater_avatar()

assistant_avatar = get_tater_avatar()

# ----------------- SETTINGS HELPER FUNCTIONS -----------------
def get_chat_settings():
    settings = redis_client.hgetall("chat_settings")
    return {
        "username": settings.get("username", "User"),
        "avatar": settings.get("avatar", None)
    }

def save_chat_settings(username, avatar=None):
    mapping = {"username": username}
    if avatar is not None:
        mapping["avatar"] = avatar
    redis_client.hset("chat_settings", mapping=mapping)

def load_avatar_image(avatar_b64):
    try:
        avatar_bytes = base64.b64decode(avatar_b64)
        return Image.open(BytesIO(avatar_bytes))
    except Exception:
        # Clear corrupted avatar from settings
        redis_client.hdel("chat_settings", "avatar")
        return None

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


def render_plugin_controls(plugin_name, label=None):
    current_state = get_plugin_enabled(plugin_name)
    toggle_state = st.toggle(label or plugin_name, value=current_state, key=f"plugin_toggle_{plugin_name}")

    if toggle_state != current_state:
        set_plugin_enabled(plugin_name, toggle_state)
        st.rerun()


# ----------------- EXPERIMENTAL PLUGIN HELPERS -----------------
def exp_get_plugin_enabled(plugin_name: str) -> bool:
    raw = redis_client.hget("exp:plugin_enabled", plugin_name)
    return str(raw or "").strip().lower() == "true"


def exp_set_plugin_enabled(plugin_name: str, enabled: bool) -> None:
    redis_client.hset("exp:plugin_enabled", plugin_name, "true" if enabled else "false")


def exp_get_plugin_settings(category: str) -> dict:
    return redis_client.hgetall(f"exp:plugin_settings:{category}") or {}


def exp_save_plugin_settings(category: str, settings: dict) -> None:
    redis_client.hset(f"exp:plugin_settings:{category}", mapping={k: str(v) for k, v in settings.items()})


def exp_get_platform_settings(platform_key: str) -> dict:
    return redis_client.hgetall(f"exp:platform_settings:{platform_key}") or {}


def exp_save_platform_settings(platform_key: str, settings: dict) -> None:
    redis_client.hset(f"exp:platform_settings:{platform_key}", mapping={k: str(v) for k, v in settings.items()})


def _load_exp_validation(kind: str, name: str) -> dict | None:
    key = f"exp:validation:{kind}:{name}"
    raw = redis_client.get(key)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _validation_status(report: dict | None, fallback_error: str | None = None) -> tuple[str, str]:
    if report:
        if report.get("ok"):
            return ("Valid", "")
        missing_deps = report.get("missing_dependencies") or []
        if missing_deps:
            return ("Missing dependencies", ", ".join(map(str, missing_deps)))
        err = report.get("error") or report.get("missing_fields") or "Invalid"
        if isinstance(err, list):
            err = ", ".join(map(str, err))
        return ("Invalid", str(err))
    if fallback_error:
        return ("Load error", fallback_error)
    return ("Not validated", "")


def _dependency_lines(report: dict | None) -> list[str]:
    if not report:
        return []
    lines: list[str] = []
    declared = report.get("declared_dependencies") or []
    missing = report.get("missing_dependencies") or []
    installed = report.get("installed_dependencies") or []
    install_errors = report.get("install_errors") or []
    if declared:
        lines.append(f"Declared deps: {', '.join(map(str, declared))}")
    if missing:
        lines.append(f"Missing deps: {', '.join(map(str, missing))}")
    if installed:
        lines.append(f"Installed deps: {', '.join(map(str, installed))}")
    if install_errors:
        lines.append(f"Install errors: {', '.join(map(str, install_errors))}")
    return lines


def render_exp_plugin_settings_form(plugin):
    category = getattr(plugin, "settings_category", None)
    settings = getattr(plugin, "required_settings", None)
    if not category or not settings:
        return
    if not isinstance(settings, dict):
        with st.expander("Settings", expanded=False):
            st.warning("Settings schema invalid (expected a dictionary).")
        return

    with st.expander("Settings", expanded=False):
        current_settings = exp_get_plugin_settings(category)
        new_settings = {}
        has_fields = False

        for key, info in settings.items():
            input_type = info.get("type", "text")
            label = info.get("label", key)
            desc = info.get("description", "")
            default_value = current_settings.get(key, info.get("default", ""))

            if input_type == "button":
                if st.button(label, key=f"exp_{plugin.name}_{category}_{key}_button"):
                    if hasattr(plugin, "handle_setting_button"):
                        try:
                            result = plugin.handle_setting_button(key)
                            if asyncio.iscoroutine(result):
                                result = run_async(result)
                            if result:
                                st.success(result)
                        except Exception as e:
                            st.error(f"Error running {label}: {e}")
                if desc:
                    st.caption(desc)
                continue

            has_fields = True

            if input_type == "password":
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    type="password",
                    key=f"exp_{plugin.name}_{category}_{key}"
                )
            elif input_type == "file":
                uploaded_file = st.file_uploader(
                    label,
                    type=["json"],
                    key=f"exp_{plugin.name}_{category}_{key}"
                )
                if uploaded_file is not None:
                    try:
                        file_content = uploaded_file.read().decode("utf-8")
                        json.loads(file_content)
                        new_value = file_content
                    except Exception as e:
                        st.error(f"Error in uploaded file for {key}: {e}")
                        new_value = default_value
                else:
                    new_value = default_value
            elif input_type == "select":
                options = info.get("options", []) or ["Option 1", "Option 2"]
                current_index = options.index(default_value) if default_value in options else 0
                new_value = st.selectbox(
                    label,
                    options,
                    index=current_index,
                    help=desc,
                    key=f"exp_{plugin.name}_{category}_{key}"
                )
            elif input_type == "checkbox":
                is_checked = (
                    default_value if isinstance(default_value, bool)
                    else str(default_value).lower() in ("true", "1", "yes")
                )
                new_value = st.checkbox(
                    label,
                    value=is_checked,
                    help=desc,
                    key=f"exp_{plugin.name}_{category}_{key}"
                )
            elif input_type == "number":
                raw_value = str(default_value).strip()
                is_int_like = bool(re.fullmatch(r"-?\d+", raw_value))

                if is_int_like:
                    try:
                        current_num = int(raw_value)
                    except Exception:
                        current_num = 0
                    new_value = st.number_input(
                        label,
                        value=current_num,
                        step=1,
                        format="%d",
                        help=desc,
                        key=f"exp_{plugin.name}_{category}_{key}"
                    )
                else:
                    try:
                        current_num = float(raw_value) if raw_value else 0.0
                    except Exception:
                        current_num = 0.0
                    new_value = st.number_input(
                        label,
                        value=current_num,
                        step=1.0,
                        help=desc,
                        key=f"exp_{plugin.name}_{category}_{key}"
                    )
            elif input_type in ("textarea", "multiline") or info.get("multiline") is True:
                rows = int(info.get("rows") or 8)
                height = int(info.get("height") or (rows * 24 + 40))
                placeholder = info.get("placeholder", None)
                new_value = st.text_area(
                    label,
                    value=str(default_value),
                    help=desc,
                    height=height,
                    placeholder=placeholder,
                    key=f"exp_{plugin.name}_{category}_{key}"
                )
            else:
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    key=f"exp_{plugin.name}_{category}_{key}"
                )

            new_settings[key] = new_value

        if has_fields and st.button(f"Save {category} Settings", key=f"exp_save_{plugin.name}_{category}"):
            exp_save_plugin_settings(category, new_settings)
            st.success(f"{category} settings saved.")
            st.rerun()


def render_exp_platform_settings_form(platform_key: str, required: dict):
    if not required:
        return

    with st.expander("Settings", expanded=False):
        current_settings = exp_get_platform_settings(platform_key)
        new_settings = {}
        has_fields = False

        for key, info in required.items():
            input_type = info.get("type", "text")
            label = info.get("label", key)
            desc = info.get("description", "")
            default_value = current_settings.get(key, info.get("default", ""))

            has_fields = True

            if input_type == "password":
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    type="password",
                    key=f"exp_platform_{platform_key}_{key}"
                )
            elif input_type == "file":
                uploaded_file = st.file_uploader(
                    label,
                    type=["json"],
                    key=f"exp_platform_{platform_key}_{key}"
                )
                if uploaded_file is not None:
                    try:
                        file_content = uploaded_file.read().decode("utf-8")
                        json.loads(file_content)
                        new_value = file_content
                    except Exception as e:
                        st.error(f"Error in uploaded file for {key}: {e}")
                        new_value = default_value
                else:
                    new_value = default_value
            elif input_type == "select":
                options = info.get("options", []) or ["Option 1", "Option 2"]
                current_index = options.index(default_value) if default_value in options else 0
                new_value = st.selectbox(
                    label,
                    options,
                    index=current_index,
                    help=desc,
                    key=f"exp_platform_{platform_key}_{key}"
                )
            elif input_type in ("checkbox", "boolean", "bool"):
                is_checked = (
                    default_value if isinstance(default_value, bool)
                    else str(default_value).lower() in ("true", "1", "yes")
                )
                new_value = st.checkbox(
                    label,
                    value=is_checked,
                    help=desc,
                    key=f"exp_platform_{platform_key}_{key}"
                )
            elif input_type == "number":
                raw_value = str(default_value).strip()
                is_int_like = bool(re.fullmatch(r"-?\d+", raw_value))

                if is_int_like:
                    try:
                        current_num = int(raw_value)
                    except Exception:
                        current_num = 0
                    new_value = st.number_input(
                        label,
                        value=current_num,
                        step=1,
                        format="%d",
                        help=desc,
                        key=f"exp_platform_{platform_key}_{key}"
                    )
                else:
                    try:
                        current_num = float(raw_value) if raw_value else 0.0
                    except Exception:
                        current_num = 0.0
                    new_value = st.number_input(
                        label,
                        value=current_num,
                        step=1.0,
                        help=desc,
                        key=f"exp_platform_{platform_key}_{key}"
                    )
            elif input_type in ("textarea", "multiline") or info.get("multiline") is True:
                rows = int(info.get("rows") or 8)
                height = int(info.get("height") or (rows * 24 + 40))
                placeholder = info.get("placeholder", None)
                new_value = st.text_area(
                    label,
                    value=str(default_value),
                    help=desc,
                    height=height,
                    placeholder=placeholder,
                    key=f"exp_platform_{platform_key}_{key}"
                )
            else:
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    key=f"exp_platform_{platform_key}_{key}"
                )

            new_settings[key] = new_value

        if has_fields and st.button(f"Save {platform_key} Settings", key=f"exp_save_platform_{platform_key}"):
            exp_save_platform_settings(platform_key, new_settings)
            st.success("Platform settings saved.")
            st.rerun()

def render_platform_controls(platform, redis_client):
    category     = platform["label"]
    key          = platform["key"]
    required     = platform["required"]
    short_name   = category.replace(" Settings", "").strip()
    state_key    = f"{key}_running"
    cooldown_key = f"tater:cooldown:{key}"
    cooldown_secs = 10
    toggle_key = f"{category}_toggle"
    cooldown_notice_key = f"{category}_cooldown_notice"
    toggle_reset_key = f"{category}_toggle_reset_to"

    # read current on/off from Redis
    is_running = (redis_client.get(state_key) == "true")
    emoji      = "🟢" if is_running else "🔴"

    # show one-time cooldown notice if we blocked restart on previous click
    cooldown_notice = st.session_state.pop(cooldown_notice_key, None)
    if cooldown_notice:
        st.warning(cooldown_notice)

    # If we asked for a reset on the previous run, apply it before rendering widget.
    if toggle_reset_key in st.session_state:
        st.session_state[toggle_key] = bool(st.session_state.pop(toggle_reset_key))
    elif toggle_key not in st.session_state:
        st.session_state[toggle_key] = is_running

    new_toggle = st.toggle(
        f"{emoji} Enable {short_name}",
        value=is_running,
        key=toggle_key,
    )
    is_enabled = new_toggle

    # --- TURNING ON ---
    if is_enabled and not is_running:
        # cooldown check
        last = redis_client.get(cooldown_key)
        now  = time.time()
        if last and now - float(last) < cooldown_secs:
            remaining = int(cooldown_secs - (now - float(last)))
            st.session_state[cooldown_notice_key] = f"⏳ Wait {remaining}s before restarting {short_name}."
            st.session_state[toggle_reset_key] = False
            st.rerun()

        # actually start it
        _start_platform(key)
        redis_client.set(state_key, "true")
        st.success(f"{short_name} started.")
        st.rerun()

    # --- TURNING OFF ---
    elif not is_enabled and is_running:
        _stop_platform(key)
        redis_client.set(state_key, "false")
        redis_client.set(cooldown_key, str(time.time()))
        st.success(f"{short_name} stopped.")
        st.rerun()

    # --- SETTINGS FORM ---
    redis_key = f"{key}_settings"
    current_settings = redis_client.hgetall(redis_key)
    new_settings = {}

    for setting_key, setting in required.items():
        label       = setting.get("label", setting_key)
        input_type  = setting.get("type", "text")
        desc        = setting.get("description", "")
        default_val = setting.get("default", "")
        current_val = current_settings.get(setting_key, default_val)

        # normalize bools from redis strings
        def _to_bool(v):
            if isinstance(v, bool):
                return v
            return str(v).lower() in ("true", "1", "yes", "on")

        if input_type == "number":
            s = str(current_val).strip()

            # Decide if int-like or float-like
            is_int_like = bool(re.fullmatch(r"-?\d+", s))
            if is_int_like:
                try:
                    current_num = int(s)
                except Exception:
                    # fallback to default as int, then 0
                    try:
                        current_num = int(str(default_val).strip())
                    except Exception:
                        current_num = 0
                new_val = st.number_input(
                    label,
                    value=current_num,
                    step=1,
                    format="%d",
                    help=desc,
                    key=f"{category}_{setting_key}"
                )
            else:
                # treat everything else as float (including "8787.0", "0.5", "")
                try:
                    current_num = float(s)
                except Exception:
                    try:
                        current_num = float(str(default_val).strip())
                    except Exception:
                        current_num = 0.0
                new_val = st.number_input(
                    label,
                    value=current_num,
                    step=1.0,
                    help=desc,
                    key=f"{category}_{setting_key}"
                )

            # store back (Redis expects strings later)
            new_settings[setting_key] = new_val

        elif input_type == "password":
            new_val = st.text_input(
                label, value=str(current_val), help=desc, type="password",
                key=f"{category}_{setting_key}"
            )
            new_settings[setting_key] = new_val

        elif input_type == "checkbox":
            new_val = st.checkbox(
                label, value=_to_bool(current_val), help=desc,
                key=f"{category}_{setting_key}"
            )
            new_settings[setting_key] = new_val

        elif input_type == "select":
            options = setting.get("options", [])
            # keep current if present; else fall back to default or first option
            if current_val not in options:
                current_val = default_val if default_val in options else (options[0] if options else "")
            new_val = st.selectbox(
                label, options,
                index=(options.index(current_val) if options else 0),
                help=desc, key=f"{category}_{setting_key}"
            )
            new_settings[setting_key] = new_val

        else:
            # default: text
            new_val = st.text_input(
                label, value=str(current_val), help=desc,
                key=f"{category}_{setting_key}"
            )
            new_settings[setting_key] = new_val

    if new_settings:
        if st.button(f"Save {short_name} Settings", key=f"save_{category}_unique"):
            # coerce all values to strings for Redis HSET
            save_map = {
                k: (json.dumps(v) if isinstance(v, (dict, list, bool)) else str(v))
                for k, v in new_settings.items()
            }
            redis_client.hset(redis_key, mapping=save_map)
            st.success(f"{short_name} settings saved.")
    else:
        st.caption("No platform settings to configure.")

    if key == "rss_platform":
        st.markdown("---")
        render_rss_feed_manager()

def render_rss_feed_manager():
    st.subheader("Feeds")
    st.caption("Add feeds and customize delivery per feed. Leave targets blank to use default routing.")

    add_url = st.text_input("RSS Feed URL", key="rss_add_url")
    cols = st.columns([1, 1, 2])
    if cols[0].button("Add Feed", key="rss_add_btn"):
        feed_url = (add_url or "").strip()
        if not feed_url:
            st.warning("Please enter a feed URL.")
            st.stop()
        existing = get_all_feeds(redis_client) or {}
        if feed_url in existing:
            st.warning("That feed is already configured.")
            st.stop()
        try:
            parsed = feedparser.parse(feed_url)
        except Exception:
            parsed = None
        if not parsed or (getattr(parsed, "bozo", 0) and not getattr(parsed, "entries", None)):
            st.error("Failed to parse that feed URL.")
            st.stop()
        # Set last_ts=0 so the poller posts only the newest item once.
        set_feed(redis_client, feed_url, {"last_ts": 0.0, "enabled": True, "platforms": {}})
        st.success("Feed added.")
        st.rerun()

    feeds = get_all_feeds(redis_client) or {}
    if not feeds:
        st.info("No feeds configured yet.")
        return

    default_cfg = {
        "send_discord": True,
        "discord_channel_id": "",
        "send_irc": True,
        "irc_channel": "",
        "send_matrix": True,
        "matrix_room_id": "",
        "send_homeassistant": True,
        "ha_device_service": "",
        "send_ntfy": True,
        "send_telegram": True,
        "send_wordpress": True,
    }

    for idx, (feed_url, cfg) in enumerate(sorted(feeds.items(), key=lambda kv: kv[0].lower())):
        exp_key = f"rss_feed_{idx}"
        with st.expander(feed_url, expanded=False):
            enabled_key = f"{exp_key}_enabled"
            enabled_val = st.checkbox("Enabled", value=cfg.get("enabled", True), key=enabled_key)

            platforms = cfg.get("platforms") or {}

            # Discord
            discord_override = platforms.get("discord") or {}
            discord_enabled = st.checkbox(
                "Send to Discord",
                value=discord_override.get("enabled", default_cfg["send_discord"]),
                key=f"{exp_key}_discord_enabled",
            )
            discord_channel_id = st.text_input(
                "Discord Channel ID (override)",
                value=(discord_override.get("targets") or {}).get("channel_id", ""),
                placeholder=default_cfg["discord_channel_id"],
                key=f"{exp_key}_discord_channel_id",
            )

            # IRC
            irc_override = platforms.get("irc") or {}
            irc_enabled = st.checkbox(
                "Send to IRC",
                value=irc_override.get("enabled", default_cfg["send_irc"]),
                key=f"{exp_key}_irc_enabled",
            )
            irc_channel = st.text_input(
                "IRC Channel (override)",
                value=(irc_override.get("targets") or {}).get("channel", ""),
                placeholder=default_cfg["irc_channel"],
                key=f"{exp_key}_irc_channel",
            )

            # Matrix
            matrix_override = platforms.get("matrix") or {}
            matrix_enabled = st.checkbox(
                "Send to Matrix",
                value=matrix_override.get("enabled", default_cfg["send_matrix"]),
                key=f"{exp_key}_matrix_enabled",
            )
            matrix_room_id = st.text_input(
                "Matrix Room ID or Alias (override)",
                value=(matrix_override.get("targets") or {}).get("room_id", ""),
                placeholder=default_cfg["matrix_room_id"],
                key=f"{exp_key}_matrix_room_id",
            )

            # Home Assistant
            ha_override = platforms.get("homeassistant") or {}
            ha_enabled = st.checkbox(
                "Send to Home Assistant Notifications",
                value=ha_override.get("enabled", default_cfg["send_homeassistant"]),
                key=f"{exp_key}_ha_enabled",
            )
            ha_device = st.text_input(
                "HA Mobile Notify Service (optional override)",
                value=(ha_override.get("targets") or {}).get("device_service", ""),
                placeholder=default_cfg["ha_device_service"],
                key=f"{exp_key}_ha_device_service",
            )
            ha_persistent = (ha_override.get("targets") or {}).get("persistent")
            if isinstance(ha_persistent, str):
                ha_persistent = ha_persistent.strip().lower() in ("1", "true", "yes", "on")
            if ha_persistent is True:
                ha_persist_choice = "Force on"
            elif ha_persistent is False:
                ha_persist_choice = "Force off"
            else:
                ha_persist_choice = "Use default"
            ha_persist_choice = st.selectbox(
                "HA Persistent Notification",
                options=["Use default", "Force on", "Force off"],
                index=["Use default", "Force on", "Force off"].index(ha_persist_choice),
                key=f"{exp_key}_ha_persist_choice",
            )

            # Other notifiers
            ntfy_override = platforms.get("ntfy") or {}
            ntfy_enabled = st.checkbox(
                "Send to Ntfy",
                value=ntfy_override.get("enabled", default_cfg["send_ntfy"]),
                key=f"{exp_key}_ntfy_enabled",
            )
            telegram_override = platforms.get("telegram") or {}
            telegram_enabled = st.checkbox(
                "Send to Telegram",
                value=telegram_override.get("enabled", default_cfg["send_telegram"]),
                key=f"{exp_key}_telegram_enabled",
            )
            wp_override = platforms.get("wordpress") or {}
            wp_enabled = st.checkbox(
                "Send to WordPress",
                value=wp_override.get("enabled", default_cfg["send_wordpress"]),
                key=f"{exp_key}_wp_enabled",
            )

            save_cols = st.columns([1, 1, 2])
            if save_cols[0].button("Save Feed Settings", key=f"{exp_key}_save"):
                new_platforms = {}

                if discord_enabled != default_cfg["send_discord"] or discord_channel_id:
                    new_platforms["discord"] = {
                        "enabled": discord_enabled,
                        "targets": {"channel_id": discord_channel_id} if discord_channel_id else {},
                    }
                if irc_enabled != default_cfg["send_irc"] or irc_channel:
                    new_platforms["irc"] = {
                        "enabled": irc_enabled,
                        "targets": {"channel": irc_channel} if irc_channel else {},
                    }
                if matrix_enabled != default_cfg["send_matrix"] or matrix_room_id:
                    new_platforms["matrix"] = {
                        "enabled": matrix_enabled,
                        "targets": {"room_id": matrix_room_id} if matrix_room_id else {},
                    }
                if ha_enabled != default_cfg["send_homeassistant"] or ha_device or ha_persist_choice != "Use default":
                    targets = {}
                    if ha_device:
                        targets["device_service"] = ha_device
                    if ha_persist_choice == "Force on":
                        targets["persistent"] = True
                    elif ha_persist_choice == "Force off":
                        targets["persistent"] = False
                    new_platforms["homeassistant"] = {
                        "enabled": ha_enabled,
                        "targets": targets,
                    }
                if ntfy_enabled != default_cfg["send_ntfy"]:
                    new_platforms["ntfy"] = {"enabled": ntfy_enabled, "targets": {}}
                if telegram_enabled != default_cfg["send_telegram"]:
                    new_platforms["telegram"] = {"enabled": telegram_enabled, "targets": {}}
                if wp_enabled != default_cfg["send_wordpress"]:
                    new_platforms["wordpress"] = {"enabled": wp_enabled, "targets": {}}

                update_feed(redis_client, feed_url, {"enabled": enabled_val, "platforms": new_platforms})
                st.success("Feed settings saved.")
                st.rerun()

            if save_cols[1].button("Remove Feed", key=f"{exp_key}_remove"):
                delete_feed(redis_client, feed_url)
                st.success("Feed removed.")
                st.rerun()

# ----------------- PLUGIN SETTINGS -----------------

def get_plugin_description(plugin):
    return getattr(plugin, "plugin_dec", None) or getattr(plugin, "description", "")

def render_plugin_settings_form(plugin):
    category = getattr(plugin, "settings_category", None)
    settings = getattr(plugin, "required_settings", None) or {}
    if not category or not settings:
        return

    with st.expander("Settings", expanded=False):
        current_settings = get_plugin_settings(category)
        new_settings = {}
        has_fields = False

        for key, info in settings.items():
            input_type    = info.get("type", "text")
            label         = info.get("label", key)
            desc          = info.get("description", "")
            default_value = current_settings.get(key, info.get("default", ""))

            if input_type == "button":
                if st.button(label, key=f"{plugin.name}_{category}_{key}_button"):
                    if hasattr(plugin, "handle_setting_button"):
                        try:
                            result = plugin.handle_setting_button(key)
                            if asyncio.iscoroutine(result):
                                result = run_async(result)
                            if result:
                                st.success(result)
                        except Exception as e:
                            st.error(f"Error running {label}: {e}")
                if desc:
                    st.caption(desc)
                continue

            has_fields = True

            if input_type == "password":
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    type="password",
                    key=f"{plugin.name}_{category}_{key}"
                )

            elif input_type == "file":
                uploaded_file = st.file_uploader(
                    label,
                    type=["json"],
                    key=f"{plugin.name}_{category}_{key}"
                )
                if uploaded_file is not None:
                    try:
                        file_content = uploaded_file.read().decode("utf-8")
                        json.loads(file_content)  # validate
                        new_value = file_content
                    except Exception as e:
                        st.error(f"Error in uploaded file for {key}: {e}")
                        new_value = default_value
                else:
                    new_value = default_value

            elif input_type == "select":
                options = info.get("options", []) or ["Option 1", "Option 2"]
                current_index = options.index(default_value) if default_value in options else 0
                new_value = st.selectbox(
                    label,
                    options,
                    index=current_index,
                    help=desc,
                    key=f"{plugin.name}_{category}_{key}"
                )

            elif input_type == "checkbox":
                is_checked = (
                    default_value if isinstance(default_value, bool)
                    else str(default_value).lower() in ("true", "1", "yes")
                )
                new_value = st.checkbox(
                    label,
                    value=is_checked,
                    help=desc,
                    key=f"{plugin.name}_{category}_{key}"
                )

            elif input_type == "number":
                raw_value = str(default_value).strip()
                is_int_like = bool(re.fullmatch(r"-?\d+", raw_value))

                if is_int_like:
                    try:
                        current_num = int(raw_value)
                    except Exception:
                        current_num = 0
                    new_value = st.number_input(
                        label,
                        value=current_num,
                        step=1,
                        format="%d",
                        help=desc,
                        key=f"{plugin.name}_{category}_{key}"
                    )
                else:
                    try:
                        current_num = float(raw_value) if raw_value else 0.0
                    except Exception:
                        current_num = 0.0
                    new_value = st.number_input(
                        label,
                        value=current_num,
                        step=1.0,
                        help=desc,
                        key=f"{plugin.name}_{category}_{key}"
                    )

            elif input_type in ("textarea", "multiline") or info.get("multiline") is True:
                rows = int(info.get("rows") or 8)
                height = int(info.get("height") or (rows * 24 + 40))
                placeholder = info.get("placeholder", None)

                new_value = st.text_area(
                    label,
                    value=str(default_value),
                    help=desc,
                    height=height,
                    placeholder=placeholder,
                    key=f"{plugin.name}_{category}_{key}"
                )

            else:
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    key=f"{plugin.name}_{category}_{key}"
                )

            new_settings[key] = new_value

        if has_fields and st.button(f"Save {category} Settings", key=f"save_{plugin.name}_{category}"):
            save_plugin_settings(category, new_settings)
            st.success(f"{category} settings saved.")
            st.rerun()

def render_plugin_card(plugin):
    display_name = (
        getattr(plugin, "plugin_name", None)
        or getattr(plugin, "pretty_name", None)
        or plugin.name
    )
    description = get_plugin_description(plugin)
    platforms = getattr(plugin, "platforms", []) or []

    # In your system, plugin.name is the actual registry id used by enable/disable
    registry_id = plugin.name

    # This is the id you'd use for uninstall (usually same as plugin.name unless you add a manifest id later)
    plugin_id = getattr(plugin, "id", None) or registry_id

    # Decide if this plugin can be removed (file exists and id is sane)
    removable = False
    try:
        removable = os.path.exists(_safe_plugin_file_path(plugin_id))
    except Exception:
        removable = False

    # Optional: best-effort Redis purge checkbox (default OFF)
    purge_key = f"purge_plugin_redis_{plugin_id}"
    purge_label = "Delete Data?"

    with st.container(border=True):
        header_cols = st.columns([4, 1, 1])

        with header_cols[0]:
            st.subheader(display_name)
            st.caption(f"ID: {registry_id}")

        with header_cols[1]:
            render_plugin_controls(registry_id, label="Enabled")

        with header_cols[2]:
            if removable:
                # Show purge option next to remove (unchecked by default)
                purge_redis = st.checkbox(purge_label, value=False, key=purge_key)

                if st.button("Remove", key=f"uninstall_{plugin_id}"):
                    # Grab category before uninstall (plugin may disappear from registry after file removal)
                    loaded = get_registry().get(registry_id)
                    category_hint = getattr(loaded, "settings_category", None) if loaded else None

                    ok, msg = uninstall_plugin_file(plugin_id)
                    if ok:
                        st.success(msg)

                        # Disable toggle only
                        try:
                            set_plugin_enabled(registry_id, False)
                        except Exception:
                            pass

                        # Optional: purge plugin settings from Redis if requested
                        if purge_redis:
                            try:
                                ok2, msg2 = clear_plugin_redis_data(plugin_id, category_hint=category_hint)
                                if ok2:
                                    st.success(f"Redis cleanup: {msg2}")
                                else:
                                    st.error(msg2)
                            except Exception as e:
                                st.error(f"Redis cleanup failed: {e}")

                        _refresh_plugins_after_fs_change()
                        st.rerun()
                    else:
                        st.error(msg)
            else:
                # Optional: show a disabled button so layout stays consistent
                st.button("Remove", disabled=True, key=f"uninstall_disabled_{plugin_id}")

        # Body
        if description:
            st.write(description)
        if platforms:
            st.caption(f"Platforms: {', '.join(platforms)}")

        render_plugin_settings_form(plugin)


def render_agent_lab_plugin_card(plugin):
    display_name = (
        getattr(plugin, "plugin_name", None)
        or getattr(plugin, "pretty_name", None)
        or plugin.name
    )
    description = get_plugin_description(plugin)
    platforms = getattr(plugin, "platforms", []) or []
    registry_id = plugin.name

    enabled = exp_get_plugin_enabled(registry_id)
    plugin_file = AGENT_PLUGINS_DIR / f"{registry_id}.py"
    report = _load_exp_validation("plugin", registry_id)
    status_label, status_detail = _validation_status(report)

    with st.container(border=True):
        header_cols = st.columns([4, 1.1, 1.1, 1.1])

        with header_cols[0]:
            st.subheader(display_name)
            st.caption(f"ID: {registry_id}")
            st.caption(f"Enabled: {'yes' if enabled else 'no'}")

        with header_cols[1]:
            if st.button("Validate", key=f"exp_validate_{registry_id}"):
                report = validate_plugin(registry_id)
                if report.get("ok"):
                    st.success("Validation passed.")
                else:
                    st.error(f"Validation failed: {report.get('error') or report.get('missing_fields')}")

        with header_cols[2]:
            if st.button("Enable", key=f"exp_enable_{registry_id}"):
                report = validate_plugin(registry_id)
                if report.get("ok"):
                    exp_set_plugin_enabled(registry_id, True)
                    st.success("Enabled.")
                    st.rerun()
                else:
                    st.error(f"Enable blocked: {report.get('error') or report.get('missing_fields')}")

        with header_cols[3]:
            if st.button("Disable", key=f"exp_disable_{registry_id}"):
                exp_set_plugin_enabled(registry_id, False)
                st.success("Disabled.")
                st.rerun()

        if description:
            st.write(description)
        if platforms:
            st.caption(f"Platforms: {', '.join(platforms)}")
        if status_label:
            status_line = f"Validation: {status_label}"
            if status_detail:
                status_line = f"{status_line} ({status_detail})"
            st.caption(status_line)
        for line in _dependency_lines(report):
            st.caption(line)

        if plugin_file.exists():
            if st.button("Delete", key=f"exp_delete_{registry_id}"):
                result = delete_file(str(plugin_file))
                if result.get("ok"):
                    exp_set_plugin_enabled(registry_id, False)
                    st.success("Deleted Agent Lab plugin file.")
                    st.rerun()
                else:
                    st.error(result.get("error") or "Delete failed.")

        render_exp_plugin_settings_form(plugin)


def render_agent_lab_plugin_error_card(name: str, path: str):
    report = _load_exp_validation("plugin", name)
    status_label, status_detail = _validation_status(report, fallback_error="Failed to load")
    missing_fields = report.get("missing_fields") if isinstance(report, dict) else []

    with st.container(border=True):
        header_cols = st.columns([4, 1, 1])
        with header_cols[0]:
            st.subheader(name)
            st.caption(f"ID: {name}")
            status_line = f"Validation: {status_label}"
            if status_detail:
                status_line = f"{status_line} ({status_detail})"
            st.caption(status_line)
        with header_cols[1]:
            if st.button("Validate", key=f"exp_validate_error_{name}"):
                report = validate_plugin(name)
                if report.get("ok"):
                    st.success("Validation passed.")
                else:
                    st.error(f"Validation failed: {report.get('error') or report.get('missing_fields')}")
        with header_cols[2]:
            if st.button("Delete", key=f"exp_delete_error_{name}"):
                result = delete_file(str(path))
                if result.get("ok"):
                    exp_set_plugin_enabled(name, False)
                    st.success("Deleted Agent Lab plugin file.")
                    st.rerun()
                else:
                    st.error(result.get("error") or "Delete failed.")

        if isinstance(missing_fields, list) and "name" in missing_fields:
            st.caption("Tip: set plugin class `name` to match this file id exactly.")

        for line in _dependency_lines(report):
            st.caption(line)

def _platform_sort_name(p):
    return (p.get("label") or p.get("category") or p.get("key") or "").lower()

def render_platforms_panel(auto_connected=None):
    st.subheader("Platforms")
    for platform in sorted(platform_registry, key=_platform_sort_name):
        label = platform.get("label") or platform.get("category") or platform.get("key")
        with st.expander(label, expanded=False):
            render_platform_controls(platform, redis_client)

def render_webui_settings():
    st.subheader("WebUI Settings")
    current_chat = get_chat_settings()
    username = st.text_input("Username", value=current_chat["username"], key="webui_username")

    raw_display = redis_client.get("tater:max_display") or 8
    try:
        display_count = int(float(raw_display))
    except (TypeError, ValueError):
        display_count = 8

    new_display = st.number_input(
        "Messages Shown in WebUI",
        min_value=1,
        max_value=500,
        value=display_count,
        step=1,
        format="%d",
        key="webui_display_count"
    )

    show_speed_default = (redis_client.get("tater:show_speed_stats") or "true").lower() == "true"
    show_speed = st.checkbox("Show tokens/sec", value=show_speed_default, key="show_speed_stats")

    uploaded_avatar = st.file_uploader(
        "Upload your avatar", type=["png", "jpg", "jpeg"], key="avatar_uploader"
    )

    if st.button("Save WebUI Settings", key="save_webui_settings"):
        if uploaded_avatar is not None:
            avatar_bytes = uploaded_avatar.read()
            avatar_b64 = base64.b64encode(avatar_bytes).decode("utf-8")
            save_chat_settings(username, avatar_b64)
        else:
            save_chat_settings(username)

        redis_client.set("tater:max_display", new_display)
        redis_client.set("tater:show_speed_stats", "true" if show_speed else "false")
        st.success("WebUI settings updated.")

    if st.button("Clear Chat History", key="clear_history"):
        clear_chat_history()
        st.success("Chat history cleared.")

    st.markdown("---")
    st.subheader("Attachments")
    st.caption("Uploaded files are stored in Redis. Images/audio/video render inline. Other files appear as attachments with a download button.")
    st.caption(f"Per-file limit: {WEBUI_ATTACH_MAX_MB_EACH}MB • Per-message total limit: {WEBUI_ATTACH_MAX_MB_TOTAL}MB • TTL: {'none' if WEBUI_ATTACH_TTL_SECONDS<=0 else str(WEBUI_ATTACH_TTL_SECONDS)+'s'}")

    if st.button("Clear Stored Attachment Blobs", key="clear_attachment_blobs"):
        ids = redis_client.lrange(FILE_INDEX_KEY, 0, -1)
        if ids:
            pipe = redis_blob_client.pipeline()
            for fid in ids:
                pipe.delete(f"{FILE_BLOB_KEY_PREFIX}{fid}")
            pipe.execute()

            # clear the index (text client is fine here)
            redis_client.delete(FILE_INDEX_KEY)

        st.success("Attachment blobs cleared (chat history entries remain).")
        st.rerun()


def render_web_search_settings():
    st.subheader("Web Search")
    st.caption("Used by kernel `search_web` for research and current information.")

    legacy_web_search = redis_client.hgetall("plugin_settings:Web Search") or {}
    web_search_api_default = (
        redis_client.get("tater:web_search:google_api_key")
        or legacy_web_search.get("GOOGLE_API_KEY")
        or ""
    )
    web_search_cx_default = (
        redis_client.get("tater:web_search:google_cx")
        or legacy_web_search.get("GOOGLE_CX")
        or ""
    )

    web_search_api = st.text_input(
        "Google API Key",
        value=web_search_api_default,
        type="password",
        key="web_search_google_api_key",
    )
    web_search_cx = st.text_input(
        "Google Search Engine ID (CX)",
        value=web_search_cx_default,
        key="web_search_google_cx",
    )

    if st.button("Save Web Search Settings", key="save_web_search_settings"):
        redis_client.set("tater:web_search:google_api_key", web_search_api.strip())
        redis_client.set("tater:web_search:google_cx", web_search_cx.strip())
        st.success("Web Search settings updated.")


def _memory_scope_discovery() -> Dict[str, Any]:
    users: set[str] = set()
    rooms_by_platform: Dict[str, set[str]] = {}

    try:
        global_entries = int(redis_client.hlen("tater:memory:global") or 0)
    except Exception:
        global_entries = 0

    try:
        for raw_key in redis_client.scan_iter(match="tater:memory:user:*", count=200):
            key = str(raw_key or "").strip()
            if not key:
                continue
            user_id = key.split("tater:memory:user:", 1)[-1].strip()
            if not user_id:
                continue
            try:
                if int(redis_client.hlen(key) or 0) <= 0:
                    continue
            except Exception:
                continue
            users.add(user_id)
    except Exception:
        pass

    try:
        for raw_key in redis_client.scan_iter(match="tater:memory:room:*", count=200):
            key = str(raw_key or "").strip()
            if not key:
                continue
            payload = key.split("tater:memory:room:", 1)[-1].strip()
            platform_name, sep, room_id = payload.partition(":")
            if not sep:
                continue
            platform_name = (platform_name or "webui").strip()
            room_id = room_id.strip()
            if not room_id:
                continue
            try:
                if int(redis_client.hlen(key) or 0) <= 0:
                    continue
            except Exception:
                continue
            rooms_by_platform.setdefault(platform_name, set()).add(room_id)
    except Exception:
        pass

    room_options = {platform: sorted(room_ids) for platform, room_ids in rooms_by_platform.items()}
    room_count = sum(len(room_ids) for room_ids in room_options.values())
    return {
        "global_entries": global_entries,
        "users": sorted(users),
        "room_options": room_options,
        "room_count": room_count,
    }


def render_memory_settings():
    st.subheader("Memory")
    st.caption("Manage durable memory behavior and inspect stored memory entries.")

    explicit_default = str(
        redis_client.get("tater:memory:explicit_only") or "true"
    ).strip().lower() in {"1", "true", "yes", "on"}
    raw_ttl = redis_client.get("tater:memory:default_ttl_sec") or 0
    try:
        ttl_default = int(float(raw_ttl))
    except (TypeError, ValueError):
        ttl_default = 0

    explicit_only = st.checkbox(
        "Require explicit user intent before memory writes",
        value=explicit_default,
        key="memory_explicit_only",
        help="When enabled, memory_set writes are blocked unless the user clearly asks to remember/save defaults.",
    )
    default_ttl = st.number_input(
        "Default TTL For Volatile Memory (seconds, 0 = no TTL)",
        min_value=0,
        max_value=31_536_000,
        value=max(0, ttl_default),
        step=60,
        format="%d",
        key="memory_default_ttl",
    )
    st.caption("TTL applies only to volatile keys (for example: last.*, temp.*, volatile.*, cache.*).")

    if st.button("Save Memory Settings", key="save_memory_settings"):
        redis_client.set("tater:memory:explicit_only", "true" if explicit_only else "false")
        redis_client.set("tater:memory:default_ttl_sec", int(default_ttl))
        st.success("Memory settings updated.")

    st.markdown("---")
    st.caption("Inspect stored memory entries.")
    scope_discovery = _memory_scope_discovery()
    if (
        int(scope_discovery.get("global_entries", 0) or 0) <= 0
        and not scope_discovery.get("users")
        and int(scope_discovery.get("room_count", 0) or 0) <= 0
    ):
        st.info("No stored memory entries found yet. Ask Tater to remember something, then inspect again.")
    else:
        st.caption(
            "Detected scopes: "
            f"global entries={int(scope_discovery.get('global_entries', 0) or 0)}, "
            f"user scopes={len(scope_discovery.get('users') or [])}, "
            f"room scopes={int(scope_discovery.get('room_count', 0) or 0)}."
        )

    inspect_scope = st.selectbox(
        "Inspect Scope",
        options=["global", "user", "room"],
        key="memory_inspect_scope",
    )
    inspect_prefix = st.text_input("Prefix Filter (optional)", value="", key="memory_inspect_prefix")
    inspect_limit = st.number_input(
        "Max Entries",
        min_value=1,
        max_value=200,
        value=25,
        step=1,
        format="%d",
        key="memory_inspect_limit",
    )

    inspect_user_id = None
    inspect_room_id = None
    inspect_platform = "webui"
    can_inspect = True
    if inspect_scope == "user":
        stored_user_ids = list(scope_discovery.get("users") or [])
        if stored_user_ids:
            inspect_user_id = st.selectbox(
                "Stored User Ids",
                options=stored_user_ids,
                key="memory_inspect_user_select",
            )
        else:
            can_inspect = False
            st.info("No stored user scopes found yet.")
    elif inspect_scope == "room":
        room_options_by_platform = dict(scope_discovery.get("room_options") or {})
        if room_options_by_platform:
            platform_options = sorted(room_options_by_platform.keys())
            if "webui" in platform_options:
                platform_options.remove("webui")
                platform_options.insert(0, "webui")
            inspect_platform = st.selectbox(
                "Stored Room Platforms",
                options=platform_options,
                key="memory_inspect_platform_select",
            )
            room_options = list(room_options_by_platform.get(inspect_platform) or [])
            if room_options:
                inspect_room_id = st.selectbox(
                    "Stored Room Ids",
                    options=room_options,
                    key="memory_inspect_room_select",
                )
            else:
                can_inspect = False
                st.info("No stored room ids found for the selected platform.")
        else:
            can_inspect = False
            st.info("No stored room scopes found yet.")

    if can_inspect:
        snapshot = memory_list(
            prefix=inspect_prefix or None,
            scope=inspect_scope,
            user_id=inspect_user_id,
            room_id=inspect_room_id,
            platform=inspect_platform,
            limit=int(inspect_limit),
        )

        if snapshot.get("ok"):
            st.caption(
                f"Showing {snapshot.get('count', 0)} of {snapshot.get('total_count', 0)} entries "
                f"for `{snapshot.get('scope', inspect_scope)}` scope."
            )
            if int(snapshot.get("total_count", 0) or 0) <= 0:
                st.info("No entries found for this scope/filter.")
            else:
                items = snapshot.get("items", [])
                st.json(items)

                key_options = []
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    key_name = str(item.get("key") or "").strip()
                    if key_name and key_name not in key_options:
                        key_options.append(key_name)

                if key_options:
                    st.markdown("Remove memory entries")
                    selected_delete_keys = st.multiselect(
                        "Select keys to delete",
                        options=key_options,
                        key=f"memory_delete_keys_{inspect_scope}",
                    )
                    if st.button(
                        "Delete Selected Memory Entries",
                        key=f"memory_delete_selected_{inspect_scope}",
                        disabled=not selected_delete_keys,
                    ):
                        delete_result = memory_delete(
                            keys=selected_delete_keys,
                            scope=inspect_scope,
                            user_id=inspect_user_id,
                            room_id=inspect_room_id,
                            platform=inspect_platform,
                        )
                        if delete_result.get("ok"):
                            st.success(
                                f"Deleted {int(delete_result.get('deleted') or 0)} memory entr"
                                f"{'y' if int(delete_result.get('deleted') or 0) == 1 else 'ies'}."
                            )
                            st.rerun()
                        else:
                            st.error(delete_result.get("error") or "Failed to delete memory entries.")
        else:
            st.error(snapshot.get("error") or "Unable to load memory entries.")


def render_homeassistant_settings():
    st.subheader("Home Assistant Settings")
    current_settings = get_homeassistant_settings()

    base_url = st.text_input(
        "Home Assistant Base URL",
        value=current_settings["HA_BASE_URL"],
        help="Example: http://homeassistant.local:8123 or http://192.168.1.50:8123",
        key="homeassistant_base_url",
    )
    token = st.text_input(
        "Home Assistant Long-Lived Access Token",
        value=current_settings["HA_TOKEN"],
        help="Create in Home Assistant Profile → Long-Lived Access Tokens.",
        type="password",
        key="homeassistant_token",
    )

    if st.button("Save Home Assistant Settings", key="save_homeassistant_settings"):
        save_homeassistant_settings(base_url.strip(), token.strip())
        st.success("Home Assistant settings updated.")


def render_vision_settings():
    st.subheader("Vision Settings")
    current_settings = get_vision_settings()

    api_base = st.text_input(
        "Vision API Base URL",
        value=current_settings["api_base"],
        help="OpenAI-compatible base URL for vision calls (example: http://127.0.0.1:1234).",
        key="vision_api_base",
    )
    model = st.text_input(
        "Vision Model",
        value=current_settings["model"],
        help="Shared vision model used by all vision-enabled plugins.",
        key="vision_model",
    )
    api_key = st.text_input(
        "Vision API Key (optional)",
        value=current_settings["api_key"],
        help="Leave blank for local stacks that do not require authentication.",
        type="password",
        key="vision_api_key",
    )

    if st.button("Save Vision Settings", key="save_vision_settings"):
        save_vision_settings(api_base.strip(), model.strip(), api_key.strip())
        st.success("Vision settings updated.")


def render_emoji_responder_settings():
    st.subheader("Emoji Responder Settings")
    settings = get_emoji_responder_settings()

    enable_on_reaction_add = st.checkbox(
        "Enable reaction-chain mode (Discord)",
        value=bool(settings["enable_on_reaction_add"]),
        help="When a user reacts to a Discord message, optionally add one matching emoji reaction.",
        key="emoji_enable_on_reaction_add",
    )
    enable_auto_reaction_on_reply = st.checkbox(
        "Enable auto reactions on replies",
        value=bool(settings["enable_auto_reaction_on_reply"]),
        help="When the assistant replies on Discord/Telegram/Matrix, occasionally add a matching emoji reaction.",
        key="emoji_enable_auto_reaction_on_reply",
    )
    reaction_chain_chance_percent = int(
        st.number_input(
            "Reaction-chain chance (%)",
            min_value=0,
            max_value=100,
            value=int(settings["reaction_chain_chance_percent"]),
            step=1,
            format="%d",
            key="emoji_reaction_chain_chance_percent",
        )
    )
    reply_reaction_chance_percent = int(
        st.number_input(
            "Reply reaction chance (%)",
            min_value=0,
            max_value=100,
            value=int(settings["reply_reaction_chance_percent"]),
            step=1,
            format="%d",
            key="emoji_reply_reaction_chance_percent",
        )
    )
    reaction_chain_cooldown_seconds = int(
        st.number_input(
            "Reaction-chain cooldown (seconds)",
            min_value=0,
            max_value=86_400,
            value=int(settings["reaction_chain_cooldown_seconds"]),
            step=1,
            format="%d",
            key="emoji_reaction_chain_cooldown_seconds",
        )
    )
    reply_reaction_cooldown_seconds = int(
        st.number_input(
            "Reply reaction cooldown (seconds)",
            min_value=0,
            max_value=86_400,
            value=int(settings["reply_reaction_cooldown_seconds"]),
            step=1,
            format="%d",
            key="emoji_reply_reaction_cooldown_seconds",
        )
    )
    min_message_length = int(
        st.number_input(
            "Minimum message length",
            min_value=0,
            max_value=200,
            value=int(settings["min_message_length"]),
            step=1,
            format="%d",
            key="emoji_min_message_length",
        )
    )

    if st.button("Save Emoji Settings", key="save_emoji_settings"):
        save_emoji_responder_settings(
            enable_on_reaction_add=enable_on_reaction_add,
            enable_auto_reaction_on_reply=enable_auto_reaction_on_reply,
            reaction_chain_chance_percent=reaction_chain_chance_percent,
            reply_reaction_chance_percent=reply_reaction_chance_percent,
            reaction_chain_cooldown_seconds=reaction_chain_cooldown_seconds,
            reply_reaction_cooldown_seconds=reply_reaction_cooldown_seconds,
            min_message_length=min_message_length,
        )
        st.success("Emoji settings updated.")


def render_tater_settings():
    def _read_non_negative_int_setting(key: str, default: int) -> int:
        raw = redis_client.get(key)
        try:
            value = int(str(raw).strip()) if raw is not None else int(default)
        except Exception:
            value = int(default)
        if value < 0:
            return 0
        return value

    st.subheader(f"{first_name} Settings")
    stored_count = _read_non_negative_int_setting("tater:max_store", 20)
    llm_count = max(1, _read_non_negative_int_setting("tater:max_llm", 8))
    default_first = redis_client.get("tater:first_name") or first_name
    default_last = redis_client.get("tater:last_name") or last_name
    default_personality = redis_client.get("tater:personality") or ""
    creation_explicit_env = os.getenv("TATER_CREATION_EXPLICIT_ONLY")
    creation_explicit_env_forced = (
        creation_explicit_env is not None and str(creation_explicit_env).strip() != ""
    )
    if creation_explicit_env_forced:
        creation_explicit_default = str(creation_explicit_env).strip().lower() in ("1", "true", "yes", "on")
    else:
        creation_explicit_default = str(
            redis_client.get("tater:agent_creation:explicit_only") or "true"
        ).strip().lower() in ("1", "true", "yes", "on")
    first_input = st.text_input("First Name", value=default_first, key="tater_first_name")
    last_input = st.text_input("Last Name", value=default_last, key="tater_last_name")

    personality_input = st.text_area(
        "Personality / Style (optional)",
        value=default_personality,
        help=(
            "Describe how you want Tater to talk and behave. "
            "Examples:\n"
            "- A calm and confident starship captain.\n"
            "- Captain Jahn-Luek Picard of the Starship Enterprise.\n"
            "- A laid-back hippy stoner who still explains things clearly."
        ),
        height=120,
        key="tater_personality"
    )

    uploaded_tater_avatar = st.file_uploader(
        f"Upload {first_input}'s avatar", type=["png", "jpg", "jpeg"], key="tater_avatar_uploader"
    )
    if uploaded_tater_avatar is not None:
        avatar_bytes = uploaded_tater_avatar.read()
        avatar_b64 = base64.b64encode(avatar_bytes).decode("utf-8")
        redis_client.set("tater:avatar", avatar_b64)

    new_store = st.number_input("Max Stored Messages (0 = unlimited)", min_value=0, value=stored_count, key="tater_store_limit")
    if new_store == 0:
        st.warning("⚠️ Unlimited history enabled — this may grow Redis memory usage over time.")
    new_llm = st.number_input("Messages Sent to LLM", min_value=1, value=llm_count, key="tater_llm_limit")
    if new_store > 0 and new_llm > new_store:
        st.warning("⚠️ You're trying to send more messages to LLM than you’re storing. Consider increasing Max Stored Messages.")
    creation_explicit_only = st.checkbox(
        "Require explicit create wording for plugin/platform creation",
        value=creation_explicit_default,
        key="tater_creation_explicit_only",
        disabled=creation_explicit_env_forced,
        help=(
            "When enabled, Agent Lab only switches into create mode when your request explicitly "
            "asks to create/build/make a plugin or platform."
        ),
    )
    if creation_explicit_env_forced:
        st.caption("Creation explicit-only is currently locked by TATER_CREATION_EXPLICIT_ONLY.")

    if st.button("Save Tater Settings", key="save_tater_settings"):
        redis_client.set("tater:max_store", new_store)
        redis_client.set("tater:max_llm", new_llm)
        redis_client.set("tater:first_name", first_input)
        redis_client.set("tater:last_name", last_input)
        redis_client.set("tater:personality", personality_input)
        redis_client.set(
            "tater:agent_creation:explicit_only",
            "true" if creation_explicit_only else "false",
        )
        st.success("Tater settings updated.")
        st.rerun()


def render_cerberus_settings():
    def _read_non_negative_int_setting(key: str, default: int) -> int:
        raw = redis_client.get(key)
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = raw.decode("utf-8", errors="ignore")
            except Exception:
                raw = None
        try:
            value = int(str(raw).strip()) if raw is not None else int(default)
        except Exception:
            value = int(default)
        if value < 0:
            return 0
        return value

    def _read_positive_int_setting(key: str, default: int) -> int:
        value = _read_non_negative_int_setting(key, default)
        if value <= 0:
            return int(default)
        return value

    st.subheader("Cerberus")
    st.caption("Planner / Doer / Critic runtime limits and token budgets.")

    max_rounds = _read_non_negative_int_setting(AGENT_MAX_ROUNDS_KEY, DEFAULT_MAX_ROUNDS)
    max_tool_calls = _read_non_negative_int_setting(AGENT_MAX_TOOL_CALLS_KEY, DEFAULT_MAX_TOOL_CALLS)
    state_ttl_seconds = _read_non_negative_int_setting(
        CERBERUS_AGENT_STATE_TTL_SECONDS_KEY,
        DEFAULT_AGENT_STATE_TTL_SECONDS,
    )
    planner_max_tokens = _read_positive_int_setting(
        CERBERUS_PLANNER_MAX_TOKENS_KEY,
        DEFAULT_PLANNER_MAX_TOKENS,
    )
    checker_max_tokens = _read_positive_int_setting(
        CERBERUS_CHECKER_MAX_TOKENS_KEY,
        DEFAULT_CHECKER_MAX_TOKENS,
    )
    doer_max_tokens = _read_positive_int_setting(
        CERBERUS_DOER_MAX_TOKENS_KEY,
        DEFAULT_DOER_MAX_TOKENS,
    )
    tool_repair_max_tokens = _read_positive_int_setting(
        CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY,
        DEFAULT_TOOL_REPAIR_MAX_TOKENS,
    )
    overclar_repair_max_tokens = _read_positive_int_setting(
        CERBERUS_OVERCLAR_REPAIR_MAX_TOKENS_KEY,
        DEFAULT_OVERCLAR_REPAIR_MAX_TOKENS,
    )
    send_repair_max_tokens = _read_positive_int_setting(
        CERBERUS_SEND_REPAIR_MAX_TOKENS_KEY,
        DEFAULT_SEND_REPAIR_MAX_TOKENS,
    )
    recovery_max_tokens = _read_positive_int_setting(
        CERBERUS_RECOVERY_MAX_TOKENS_KEY,
        DEFAULT_RECOVERY_MAX_TOKENS,
    )
    max_ledger_items = _read_positive_int_setting(
        CERBERUS_MAX_LEDGER_ITEMS_KEY,
        DEFAULT_MAX_LEDGER_ITEMS,
    )

    new_max_rounds = int(
        st.number_input(
            "Agent Max Rounds (0 = unlimited)",
            min_value=0,
            value=max_rounds,
            step=1,
            format="%d",
            key="cerberus_max_rounds",
        )
    )
    new_max_tool_calls = int(
        st.number_input(
            "Agent Max Tool Calls (0 = unlimited)",
            min_value=0,
            value=max_tool_calls,
            step=1,
            format="%d",
            key="cerberus_max_tool_calls",
        )
    )
    new_state_ttl_seconds = int(
        st.number_input(
            "Agent State TTL Seconds (0 = no TTL)",
            min_value=0,
            value=state_ttl_seconds,
            step=60,
            format="%d",
            key="cerberus_agent_state_ttl_seconds",
        )
    )
    new_planner_max_tokens = int(
        st.number_input(
            "Planner Max Tokens",
            min_value=1,
            value=planner_max_tokens,
            step=10,
            format="%d",
            key="cerberus_planner_max_tokens",
        )
    )
    new_checker_max_tokens = int(
        st.number_input(
            "Checker Max Tokens",
            min_value=1,
            value=checker_max_tokens,
            step=10,
            format="%d",
            key="cerberus_checker_max_tokens",
        )
    )
    new_doer_max_tokens = int(
        st.number_input(
            "Doer Max Tokens",
            min_value=1,
            value=doer_max_tokens,
            step=10,
            format="%d",
            key="cerberus_doer_max_tokens",
        )
    )
    new_tool_repair_max_tokens = int(
        st.number_input(
            "Tool-Repair Max Tokens",
            min_value=1,
            value=tool_repair_max_tokens,
            step=10,
            format="%d",
            key="cerberus_tool_repair_max_tokens",
        )
    )
    new_overclar_repair_max_tokens = int(
        st.number_input(
            "Over-Clarification Repair Max Tokens",
            min_value=1,
            value=overclar_repair_max_tokens,
            step=10,
            format="%d",
            key="cerberus_overclar_repair_max_tokens",
        )
    )
    new_send_repair_max_tokens = int(
        st.number_input(
            "Send-Message Repair Max Tokens",
            min_value=1,
            value=send_repair_max_tokens,
            step=10,
            format="%d",
            key="cerberus_send_repair_max_tokens",
        )
    )
    new_recovery_max_tokens = int(
        st.number_input(
            "Recovery Max Tokens",
            min_value=1,
            value=recovery_max_tokens,
            step=10,
            format="%d",
            key="cerberus_recovery_max_tokens",
        )
    )
    new_max_ledger_items = int(
        st.number_input(
            "Max Ledger Items",
            min_value=1,
            value=max_ledger_items,
            step=10,
            format="%d",
            key="cerberus_max_ledger_items",
        )
    )

    if new_max_rounds == 0 or new_max_tool_calls == 0:
        st.warning("Unlimited round/tool-call limits are enabled.")

    if st.button("Save Cerberus Settings", key="save_cerberus_settings"):
        redis_client.set(AGENT_MAX_ROUNDS_KEY, int(new_max_rounds))
        redis_client.set(AGENT_MAX_TOOL_CALLS_KEY, int(new_max_tool_calls))
        redis_client.set(CERBERUS_AGENT_STATE_TTL_SECONDS_KEY, int(new_state_ttl_seconds))
        redis_client.set(CERBERUS_PLANNER_MAX_TOKENS_KEY, int(new_planner_max_tokens))
        redis_client.set(CERBERUS_CHECKER_MAX_TOKENS_KEY, int(new_checker_max_tokens))
        redis_client.set(CERBERUS_DOER_MAX_TOKENS_KEY, int(new_doer_max_tokens))
        redis_client.set(CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY, int(new_tool_repair_max_tokens))
        redis_client.set(CERBERUS_OVERCLAR_REPAIR_MAX_TOKENS_KEY, int(new_overclar_repair_max_tokens))
        redis_client.set(CERBERUS_SEND_REPAIR_MAX_TOKENS_KEY, int(new_send_repair_max_tokens))
        redis_client.set(CERBERUS_RECOVERY_MAX_TOKENS_KEY, int(new_recovery_max_tokens))
        redis_client.set(CERBERUS_MAX_LEDGER_ITEMS_KEY, int(new_max_ledger_items))
        st.success("Cerberus settings updated.")


def render_admin_gating_settings():
    st.subheader("Admin Tool Gating")
    st.caption(
        "Only the configured admin user can run these plugins on Discord, Telegram, Matrix, and IRC. "
        "If a platform’s admin user setting is blank, these tools are disabled for everyone on that platform."
    )

    registry = get_registry() or {}
    plugin_ids = sorted(registry.keys())
    current = sorted(get_admin_only_plugins(redis_client))
    known_current = [p for p in current if p in plugin_ids]
    unknown_current = [p for p in current if p not in plugin_ids]

    using_default_plugin_list = redis_client.get(ADMIN_GATE_KEY) is None
    using_default_creation_gate = redis_client.get(CREATION_GATE_KEY) is None
    if using_default_plugin_list and using_default_creation_gate:
        st.info("Currently using default admin-gating values. Save to customize.")
    elif using_default_plugin_list:
        st.info("Currently using the default admin-only plugin list. Save to customize.")
    elif using_default_creation_gate:
        st.info("Agent Lab creation admin-gating is at default (off). Save to customize.")

    if unknown_current:
        st.warning(f"Unknown plugin IDs currently stored: {', '.join(unknown_current)}")

    selected = st.multiselect(
        "Admin-only plugins (by plugin id)",
        options=plugin_ids,
        default=known_current,
        help="Selected plugins can only be run by the admin user on Discord/Telegram/Matrix/IRC.",
        key="admin_gate_plugins",
    )
    creation_gate_enabled = st.checkbox(
        "Require admin user for Agent Lab plugin/platform creation",
        value=is_agent_lab_creation_admin_gated(redis_client),
        key="admin_gate_agent_lab_creation",
        help="When enabled, `create_plugin` and `create_platform` can only be run by the configured admin user.",
    )

    col1, col2 = st.columns(2)
    if col1.button("Save Admin Tool Gating", key="save_admin_gating"):
        redis_client.set(ADMIN_GATE_KEY, json.dumps(selected))
        redis_client.set(CREATION_GATE_KEY, "true" if creation_gate_enabled else "false")
        st.success("Admin tool gating saved.")

    if col2.button("Reset to Defaults", key="reset_admin_gating"):
        redis_client.delete(ADMIN_GATE_KEY)
        redis_client.delete(CREATION_GATE_KEY)
        st.success("Admin tool gating reset to defaults.")
        st.rerun()


def _cerberus_ledger_keys_for_platform(platform: str) -> List[str]:
    plat = str(platform or "all").strip().lower()
    if plat == "all":
        keys = []
        if redis_client.exists("tater:cerberus:ledger"):
            keys.append("tater:cerberus:ledger")
        else:
            keys.extend(sorted(str(k) for k in redis_client.scan_iter(match="tater:cerberus:ledger:*")))
        return keys
    return [f"tater:cerberus:ledger:{normalize_platform(plat)}"]


def _load_cerberus_ledger_entries(platform: str, limit: int) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    keys = _cerberus_ledger_keys_for_platform(platform)
    max_limit = max(1, int(limit or 50))
    for key in keys:
        raw_items = redis_client.lrange(key, -max_limit, -1) or []
        for raw in raw_items:
            try:
                item = json.loads(raw)
            except Exception:
                continue
            if not isinstance(item, dict):
                continue
            item["_ledger_key"] = key
            entries.append(item)
    entries.sort(key=lambda x: float(x.get("timestamp") or 0.0), reverse=True)
    return entries[:max_limit]


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

    # Backcompat for old entries that only had validation.ok.
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

    # If no validation object exists, infer from presence of a planned tool.
    has_planned_tool = isinstance(planned_tool, dict) and bool(str(planned_tool.get("function") or "").strip())
    if not has_planned_tool:
        return {"status": "skipped", "repair_used": False, "reason": "no_tool", "attempts": 0}
    return {"status": "failed", "repair_used": False, "reason": "invalid_tool_call", "attempts": 1}


def _clear_cerberus_ledger(platform: str) -> int:
    keys = _cerberus_ledger_keys_for_platform(platform)
    deleted = 0
    for key in keys:
        try:
            deleted += int(redis_client.delete(key) or 0)
        except Exception:
            continue
    return deleted


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


def _load_cerberus_metrics(platform: str) -> tuple[str, Dict[str, int], Dict[str, int]]:
    selected = str(platform or "").strip().lower()
    metric_platform = normalize_platform(selected if selected and selected != "all" else "webui")
    global_metrics: Dict[str, int] = {}
    platform_metrics: Dict[str, int] = {}
    for name in _CERBERUS_METRIC_NAMES:
        global_metrics[name] = _coerce_redis_counter(redis_client.get(f"tater:cerberus:metrics:{name}"))
        if selected == "all":
            platform_metrics[name] = global_metrics[name]
        else:
            platform_metrics[name] = _coerce_redis_counter(redis_client.get(f"tater:cerberus:metrics:{name}:{metric_platform}"))
    return metric_platform, global_metrics, platform_metrics


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


def _load_cerberus_platform_metric_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for platform in _CERBERUS_METRIC_PLATFORMS:
        row: Dict[str, Any] = {"platform": platform}
        for name in _CERBERUS_METRIC_NAMES:
            row[name] = _coerce_redis_counter(redis_client.get(f"tater:cerberus:metrics:{name}:{platform}"))
        rows.append(row)
    return rows


def render_cerberus_metrics_dashboard(*, key_prefix: str, allow_controls: bool):
    st.subheader("Cerberus Metrics")
    st.caption("Planner/Doer/Critic counters and recent ledger rows.")

    platforms = ["all", *list(_CERBERUS_METRIC_PLATFORMS)]
    selected_platform = st.selectbox(
        "Platform",
        options=platforms,
        index=1,
        key=f"{key_prefix}_platform",
    )
    limit = int(
        st.slider(
            "Ledger entries",
            min_value=10,
            max_value=300,
            value=50,
            step=10,
            key=f"{key_prefix}_ledger_limit",
        )
    )

    metric_platform, global_metrics, platform_metrics = _load_cerberus_metrics(selected_platform)

    st.markdown("**Global Counters**")
    global_cols = st.columns(len(_CERBERUS_METRIC_NAMES))
    for idx, name in enumerate(_CERBERUS_METRIC_NAMES):
        global_cols[idx].metric(name.replace("_", " ").title(), global_metrics.get(name, 0))

    st.markdown("**Global Rates**")
    st.dataframe(_cerberus_rate_rows(global_metrics), use_container_width=True)

    if selected_platform != "all":
        st.markdown(f"**Selected Platform Counters ({metric_platform})**")
        platform_cols = st.columns(len(_CERBERUS_METRIC_NAMES))
        for idx, name in enumerate(_CERBERUS_METRIC_NAMES):
            platform_cols[idx].metric(name.replace("_", " ").title(), platform_metrics.get(name, 0))
        st.markdown(f"**Selected Platform Rates ({metric_platform})**")
        st.dataframe(_cerberus_rate_rows(platform_metrics), use_container_width=True)

    st.markdown("**Per-Platform Totals**")
    st.dataframe(_load_cerberus_platform_metric_rows(), use_container_width=True)

    if allow_controls:
        st.caption("Advanced controls")
        control_cols = st.columns(2)
        if control_cols[0].button("Reset Metrics", key=f"{key_prefix}_reset_metrics"):
            removed = _reset_cerberus_metrics(selected_platform)
            st.success(f"Removed {removed} metric key(s).")
            st.rerun()
        if control_cols[1].button("Clear Ledger", key=f"{key_prefix}_clear_ledger"):
            removed = _clear_cerberus_ledger(selected_platform)
            st.success(f"Deleted {removed} ledger list(s).")
            st.rerun()

    rows = _load_cerberus_ledger_entries(selected_platform, limit)
    if not rows:
        st.info("No Cerberus ledger entries found for this selection.")
        return

    outcome_filter = st.selectbox(
        "Outcome Filter",
        options=["all", "done", "blocked", "failed"],
        index=0,
        key=f"{key_prefix}_outcome_filter",
    )
    show_only_tool_turns = st.checkbox(
        "Show Only Tool Turns",
        value=False,
        key=f"{key_prefix}_tool_turns_only",
    )
    tool_options = ["all"] + sorted(
        {
            str((row.get("planned_tool") or {}).get("function") or "").strip()
            for row in rows
            if isinstance(row.get("planned_tool"), dict)
            and str((row.get("planned_tool") or {}).get("function") or "").strip()
        }
    )
    selected_tool = st.selectbox(
        "Tool Filter",
        options=tool_options,
        index=0,
        key=f"{key_prefix}_tool_filter",
    )

    filtered_rows: List[Dict[str, Any]] = []
    for row in rows:
        planned_tool_obj = row.get("planned_tool") if isinstance(row.get("planned_tool"), dict) else {}
        planned_tool_name = str(planned_tool_obj.get("function") or "").strip()
        outcome = str(row.get("outcome") or "").strip().lower()
        if outcome_filter != "all" and outcome != outcome_filter:
            continue
        if show_only_tool_turns and not planned_tool_name:
            continue
        if selected_tool != "all" and planned_tool_name != selected_tool:
            continue
        filtered_rows.append(row)

    if not filtered_rows:
        st.info("No ledger rows matched the current filters.")
        return

    summary_rows = []
    for idx, item in enumerate(filtered_rows):
        ts = float(item.get("timestamp") or 0.0)
        planned_tool = item.get("planned_tool") if isinstance(item.get("planned_tool"), dict) else {}
        validation = _normalize_cerberus_validation_for_view(
            item.get("validation"),
            planned_tool=planned_tool if isinstance(planned_tool, dict) else None,
        )
        tool_result = item.get("tool_result") if isinstance(item.get("tool_result"), dict) else {}
        tool_summary = str(
            tool_result.get("summary")
            or item.get("tool_result_summary")
            or ""
        )
        summary_rows.append(
            {
                "#": idx + 1,
                "time": datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "",
                "platform": str(item.get("platform") or ""),
                "scope": str(item.get("scope") or ""),
                "planner_kind": str(item.get("planner_kind") or ""),
                "outcome": str(item.get("outcome") or ""),
                "outcome_reason": str(item.get("outcome_reason") or ""),
                "planned_tool": str(planned_tool.get("function") or ""),
                "validation_status": str(validation.get("status") or ""),
                "tool_result_ok": tool_result.get("ok") if isinstance(tool_result, dict) else item.get("tool_result_ok"),
                "tool_result_summary": tool_summary,
                "validation_reason": str(validation.get("reason") or ""),
                "checker_action": str(item.get("checker_action") or ""),
                "total_ms": int(item.get("total_ms") or 0),
            }
        )

    st.dataframe(summary_rows, use_container_width=True)

    tool_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    for item in filtered_rows:
        planned_tool = item.get("planned_tool") if isinstance(item.get("planned_tool"), dict) else {}
        tool_name = str(planned_tool.get("function") or "").strip()
        if tool_name:
            tool_counts[tool_name] = int(tool_counts.get(tool_name, 0)) + 1

        validation_reason = str(item.get("validation_reason") or "").strip()
        if validation_reason:
            key = f"validation:{validation_reason}"
            reason_counts[key] = int(reason_counts.get(key, 0)) + 1
        checker_reason = str(item.get("checker_reason") or "").strip()
        if checker_reason:
            key = f"checker:{checker_reason}"
            reason_counts[key] = int(reason_counts.get(key, 0)) + 1

    top_tools_rows = [
        {"tool": name, "count": count}
        for name, count in sorted(tool_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    ]
    top_reasons_rows = [
        {"reason": name, "count": count}
        for name, count in sorted(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    ]

    rollup_cols = st.columns(2)
    with rollup_cols[0]:
        st.markdown("**Top Tools (Last N Filtered)**")
        if top_tools_rows:
            st.dataframe(top_tools_rows, use_container_width=True)
        else:
            st.caption("No tool calls in current filtered set.")
    with rollup_cols[1]:
        st.markdown("**Top Failure Reasons (Last N Filtered)**")
        if top_reasons_rows:
            st.dataframe(top_reasons_rows, use_container_width=True)
        else:
            st.caption("No failure reasons in current filtered set.")

    for idx, item in enumerate(filtered_rows):
        ts = float(item.get("timestamp") or 0.0)
        ts_text = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "unknown time"
        platform_text = str(item.get("platform") or "unknown")
        outcome_text = str(item.get("outcome") or "unknown")
        with st.expander(f"Details #{idx + 1} - {ts_text} - {platform_text} - {outcome_text}", expanded=False):
            st.code(json.dumps(item, indent=2, ensure_ascii=False), language="json")


def render_cerberus_ledger_settings():
    render_cerberus_metrics_dashboard(key_prefix="cerberus_advanced_dashboard", allow_controls=True)

def render_settings_page():
    st.title("Settings")
    tab_general, tab_integrations, tab_memory, tab_ai_tasks, tab_emoji, tab_cerberus, tab_advanced = st.tabs(
        ["General", "Integrations", "Memory", "AI Tasks", "Emoji", "Cerberus", "Advanced"]
    )

    with tab_general:
        render_webui_settings()
        st.markdown("---")
        render_tater_settings()

    with tab_integrations:
        render_web_search_settings()
        st.markdown("---")
        render_homeassistant_settings()
        st.markdown("---")
        render_vision_settings()

    with tab_memory:
        render_memory_settings()

    with tab_ai_tasks:
        render_ai_tasks_page(embedded=True)

    with tab_emoji:
        render_emoji_responder_settings()

    with tab_cerberus:
        render_cerberus_settings()
        st.markdown("---")
        render_cerberus_metrics_dashboard(key_prefix="cerberus_tab_dashboard", allow_controls=False)

    with tab_advanced:
        render_admin_gating_settings()
        st.markdown("---")
        render_cerberus_ledger_settings()

def _load_schedules():
    items_by_id = {}
    due_by_id = {}

    # Primary source for next run timing.
    due_rows = redis_client.zrange("reminders:due", 0, -1, withscores=True) or []
    for reminder_id, due_ts in due_rows:
        rid = str(reminder_id)
        try:
            due_by_id[rid] = float(due_ts)
        except Exception:
            pass

    # Source of truth for task objects (also catches tasks currently in-flight).
    for key in redis_client.scan_iter(match="reminders:*", count=500):
        key_s = str(key)
        if key_s == "reminders:due":
            continue
        if not key_s.startswith("reminders:"):
            continue
        rid = key_s.split("reminders:", 1)[1].strip()
        if not rid:
            continue

        raw = redis_client.get(key_s)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue

        due_ts = due_by_id.get(rid)
        if due_ts is None:
            try:
                due_ts = float((obj.get("schedule") or {}).get("next_run_ts") or 0.0)
            except Exception:
                due_ts = 0.0

        obj["_id"] = rid
        obj["_due_ts"] = float(due_ts or 0.0)
        items_by_id[rid] = obj

    return list(items_by_id.values())


def _delete_schedule(reminder_id: str):
    rid = str(reminder_id or "").strip()
    if not rid:
        return
    redis_client.zrem("reminders:due", rid)
    redis_client.delete(f"reminders:{rid}")


def render_ai_tasks_page(*, embedded: bool = False):
    if embedded:
        st.subheader("AI Tasks")
    else:
        st.title("AI Tasks")
    st.caption("Manage AI tasks and reminders.")

    schedules = _load_schedules()
    if not schedules:
        st.info("No schedules yet.")
        return

    def _sort_key(item):
        return float(item.get("_due_ts") or 0.0)

    def _recurrence_label(schedule: Dict[str, Any], interval: float) -> str:
        recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
        kind = str(recurrence.get("kind") or "").strip().lower()
        hour = int(recurrence.get("hour") or 0)
        minute = int(recurrence.get("minute") or 0)
        second = int(recurrence.get("second") or 0)
        weekdays = recurrence.get("weekdays") if isinstance(recurrence.get("weekdays"), list) else []
        time_part = f"{hour:02d}:{minute:02d}" + (f":{second:02d}" if second else "")

        day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        valid_days = []
        for day in weekdays:
            try:
                day_i = int(day)
            except Exception:
                continue
            if 0 <= day_i <= 6:
                valid_days.append(day_names[day_i])
        valid_days = sorted(set(valid_days), key=day_names.index)

        if kind == "daily_local_time":
            if valid_days:
                return f"Weekly ({', '.join(valid_days)}) at {time_part}"
            return f"Daily at {time_part}"
        if kind == "weekly_local_time":
            if valid_days:
                return f"Weekly ({', '.join(valid_days)}) at {time_part}"
            return f"Weekly at {time_part}"
        if interval > 0:
            return f"Every {int(interval)}s"
        return "One-shot"

    for row in sorted(schedules, key=_sort_key):
        rid = row.get("_id", "")
        due_ts = float(row.get("_due_ts") or 0.0)
        schedule = row.get("schedule") or {}
        interval = 0.0
        try:
            interval = float(schedule.get("interval_sec") or 0.0)
        except Exception:
            interval = 0.0

        platform = str(row.get("platform") or "").strip() or "unknown"
        title = str(row.get("title") or "").strip()
        task_prompt = str(row.get("task_prompt") or "").strip()
        message = str(row.get("message") or "").strip()
        preview = task_prompt or message or "(empty)"
        preview = preview[:140] + ("..." if len(preview) > 140 else "")

        due_local = datetime.fromtimestamp(due_ts).strftime("%Y-%m-%d %H:%M:%S")
        mode_label = "AI Task"
        recur_label = _recurrence_label(schedule, interval)
        summary = f"{mode_label} -> {platform} -> {recur_label} -> next {due_local}"

        with st.container():
            cols = st.columns([10, 2])
            with cols[0]:
                st.markdown(f"**{title or 'Untitled schedule'}**")
                st.caption(summary)
                st.write(preview)
            with cols[1]:
                if st.button("🗑️", key=f"del_sched_{rid}", help="Delete schedule"):
                    _delete_schedule(rid)
                    st.success("Schedule removed.")
                    st.rerun()
        st.markdown("---")

def _sort_plugins_for_display(plugins):
    return sorted(
        plugins,
        key=lambda p: (
            getattr(p, "plugin_name", None)
            or getattr(p, "pretty_name", None)
            or p.name
        ).lower()
    )

def render_plugin_list(plugins, empty_message):
    sorted_plugins = _sort_plugins_for_display(plugins)
    if not sorted_plugins:
        st.info(empty_message)
        return
    for plugin in sorted_plugins:
        render_plugin_card(plugin)


def render_agent_lab_page():
    st.title("Agent Lab")
    st.caption("Agent Lab tools are isolated under agent_lab/.")

    st.subheader("Agent Plugins")
    exp_items = _discover_agent_lab_plugins()
    exp_errors = [f"{item['name']}.py (failed to load)" for item in exp_items if not item.get("loaded")]
    if exp_errors:
        st.warning("Some Agent Lab plugins failed to load: " + ", ".join(exp_errors))
    if not exp_items:
        st.info("No Agent Lab plugins found in agent_lab/plugins.")
    else:
        for item in exp_items:
            plugin = item.get("plugin")
            if plugin:
                render_agent_lab_plugin_card(plugin)
            else:
                render_agent_lab_plugin_error_card(item.get("name", "unknown"), item.get("path", ""))

    st.markdown("---")
    st.subheader("Agent Platforms")
    exp_platforms = _discover_agent_lab_platforms()
    if not exp_platforms:
        st.info("No Agent Lab platforms found in agent_lab/platforms.")
        return

    for platform in exp_platforms:
        key = platform.get("key")
        label = platform.get("label") or key
        path = platform.get("path")
        ok = platform.get("ok", True)
        error = platform.get("error") or ""
        required = platform.get("required") or {}

        running_key = f"exp:{key}_running"
        is_running = (redis_client.get(running_key) or "") == "true"
        report = _load_exp_validation("platform", key)
        status_label, status_detail = _validation_status(report, fallback_error=error if not ok else None)

        with st.container(border=True):
            cols = st.columns([4, 1, 1, 1])
            with cols[0]:
                st.subheader(label)
                st.caption(f"Key: {key}")
                st.caption(f"Running: {'yes' if is_running else 'no'}")
                status_line = f"Validation: {status_label}"
                if status_detail:
                    status_line = f"{status_line} ({status_detail})"
                st.caption(status_line)
                for line in _dependency_lines(report):
                    st.caption(line)
            with cols[1]:
                if st.button("Validate", key=f"exp_platform_validate_{key}"):
                    report = validate_platform(key)
                    if report.get("ok"):
                        st.success("Validation passed.")
                    else:
                        st.error(f"Validation failed: {report.get('error') or report.get('missing_fields')}")
            with cols[2]:
                if st.button("Start", key=f"exp_platform_start_{key}"):
                    report = validate_platform(key)
                    if report.get("ok"):
                        redis_client.set(running_key, "true")
                        if path:
                            _start_agent_lab_platform(key, path)
                        st.success("Started.")
                        st.rerun()
                    else:
                        st.error(f"Start blocked: {report.get('error') or report.get('missing_fields')}")
            with cols[3]:
                if st.button("Stop", key=f"exp_platform_stop_{key}"):
                    redis_client.set(running_key, "false")
                    _stop_agent_lab_platform(key)
                    st.success("Stopped.")
                    st.rerun()

            if path and st.button("Delete", key=f"exp_platform_delete_{key}"):
                result = delete_file(str(path))
                if result.get("ok"):
                    redis_client.set(running_key, "false")
                    _stop_agent_lab_platform(key)
                    st.success("Deleted Agent Lab platform file.")
                    st.rerun()
                else:
                    st.error(result.get("error") or "Delete failed.")

            if ok and required:
                render_exp_platform_settings_form(key, required)

def render_plugin_store_page():
    import re as _re

    st.title("Plugin Store")
    st.caption("Browse and install plugins from the Tater Shop manifest.")

    url = st.text_input(
        "Shop manifest URL",
        value=(redis_client.get("tater:shop_manifest_url") or SHOP_MANIFEST_URL_DEFAULT),
        key="shop_manifest_url"
    )

    try:
        manifest = fetch_shop_manifest(url.strip())
    except Exception as e:
        st.error(f"Failed to load manifest: {e}")
        return

    plugins = manifest.get("plugins") or manifest.get("items") or manifest.get("data") or []
    if not isinstance(plugins, list):
        st.error("Manifest format unexpected (expected a list under 'plugins').")
        return

    # ------------------ helpers ------------------
    def _semver_tuple(v: str):
        """
        Very small semver-ish parser.
        - Accepts: "1.2.3", "v1.2.3", "1.2", "1"
        - Ignores suffix like "-beta" by keeping only leading digits/dots.
        """
        if not v:
            return (0, 0, 0)

        v = str(v).strip().lower()
        if v.startswith("v"):
            v = v[1:].strip()

        m = _re.match(r"^([0-9]+(\.[0-9]+){0,2})", v)  # keep "1", "1.2", "1.2.3"
        core = m.group(1) if m else "0.0.0"

        parts = core.split(".")
        parts = (parts + ["0", "0", "0"])[:3]
        try:
            return (int(parts[0]), int(parts[1]), int(parts[2]))
        except Exception:
            return (0, 0, 0)

    def _get_installed_version(plugin_id: str) -> str:
        """
        If installed, prefer the loaded plugin object's version attribute.
        Falls back to 0.0.0.
        """
        if not plugin_id:
            return "0.0.0"
        loaded = get_registry().get(plugin_id)
        if not loaded:
            return "0.0.0"

        v = (
            getattr(loaded, "version", None)
            or getattr(loaded, "__version__", None)
            or getattr(loaded, "plugin_version", None)
        )
        v = str(v).strip() if v is not None else ""
        return v or "0.0.0"

    def _normalize_plats(plats):
        if not plats:
            return []
        if isinstance(plats, str):
            plats = [plats]
        if not isinstance(plats, list):
            return []
        out = []
        for p in plats:
            if not p:
                continue
            out.append(str(p).strip().lower())
        # unique, stable order
        seen = set()
        uniq = []
        for p in out:
            if p not in seen:
                seen.add(p)
                uniq.append(p)
        return uniq

    def _get_item_platforms(item):
        """
        Prefer installed plugin.platforms (authoritative for installed),
        else fall back to manifest.
        """
        pid = (item.get("id") or "").strip()
        if pid and is_plugin_installed(pid):
            loaded = get_registry().get(pid)
            if loaded:
                lp = getattr(loaded, "platforms", []) or []
                norm = _normalize_plats(lp)
                if norm:
                    return norm
        # manifest fallback
        return _normalize_plats(item.get("platforms") or item.get("platform") or [])

    def _get_item_display_platforms(item):
        plats = _get_item_platforms(item)
        return ", ".join(p.title() for p in plats) if plats else "(not provided)"

    def _is_update_available(item: dict) -> tuple[bool, str, str]:
        """
        Returns (update_available, installed_ver, store_ver)
        """
        pid = (item.get("id") or "").strip()
        store_ver = (item.get("version") or "0.0.0").strip()
        if not pid:
            return (False, "0.0.0", store_ver)

        if not is_plugin_installed(pid):
            return (False, "0.0.0", store_ver)

        installed_ver = _get_installed_version(pid)
        return (_semver_tuple(store_ver) > _semver_tuple(installed_ver), installed_ver, store_ver)

    # ------------------ Update All (top bar) ------------------
    updatable = []
    for it in plugins:
        pid = (it.get("id") or "").strip()
        if not pid:
            continue
        ok, inst_v, store_v = _is_update_available(it)
        if ok:
            updatable.append((it, inst_v, store_v))

    # Save URL | Refresh | Update All | Updates available
    bar1, bar2, bar3, bar4 = st.columns([1, 1, 1, 3])

    with bar1:
        if st.button("Save URL", key="shop_save_url"):
            redis_client.set("tater:shop_manifest_url", url.strip())
            st.success("Saved.")
            st.rerun()

    with bar2:
        if st.button("Refresh", key="shop_refresh"):
            st.rerun()

    with bar3:
        if st.button("Update All", disabled=(len(updatable) == 0), key="shop_update_all"):
            updated = []
            failed = []

            prog = st.progress(0)
            total = max(1, len(updatable))

            for idx, (it, inst_v, store_v) in enumerate(updatable, start=1):
                pid = (it.get("id") or "").strip()
                ok, msg = install_plugin_from_shop_item(it, url.strip())
                if ok:
                    updated.append(f"{pid} ({inst_v} → {store_v})")
                else:
                    failed.append(f"{pid}: {msg}")

                prog.progress(min(1.0, idx / total))

            if updated:
                st.success("Updated:\n" + "\n".join(updated))
            if failed:
                st.error("Failed:\n" + "\n".join(failed))

            _refresh_plugins_after_fs_change()
            st.rerun()

    with bar4:
        if updatable:
            st.caption(f"Updates available: {len(updatable)}")
        else:
            st.caption("No updates available.")

    st.markdown("---")

    # ------------------ build filter options ------------------
    all_platforms = set()
    for it in plugins:
        for p in _get_item_platforms(it):
            all_platforms.add(p)

    # Stable-ish ordering with common ones first (optional)
    common_order = ["discord", "webui", "homeassistant", "homekit", "irc", "matrix", "telegram", "wordpress", "xbmc", "automation"]
    ordered = [p for p in common_order if p in all_platforms]
    ordered += sorted([p for p in all_platforms if p not in set(common_order)])

    filter_options = ["All"] + [p.title() for p in ordered]
    selected_platform_label = st.selectbox(
        "Filter by platform",
        options=filter_options,
        index=0,
        key="shop_platform_filter"
    )

    search_q = st.text_input(
        "Search",
        value="",
        placeholder="Search name, id, description…",
        key="shop_search"
    ).strip().lower()

    selected_platform = None
    if selected_platform_label != "All":
        selected_platform = selected_platform_label.strip().lower()

    # ------------------ filter plugins ------------------
    filtered = []
    for item in plugins:
        pid = (item.get("id") or "").strip()
        name = (item.get("name") or pid).strip()
        desc = (item.get("description") or "").strip()

        # platform filter
        if selected_platform:
            plats = _get_item_platforms(item)
            if selected_platform not in plats:
                continue

        # search filter
        if search_q:
            hay = f"{pid}\n{name}\n{desc}".lower()
            if search_q not in hay:
                continue

        filtered.append(item)

    st.caption(f"Showing {len(filtered)} of {len(plugins)} plugin(s).")

    # ------------------ render ------------------
    for item in filtered:
        pid = (item.get("id") or "").strip()
        name = (item.get("name") or pid).strip()
        desc = (item.get("description") or "").strip()
        min_ver = (item.get("min_tater_version") or "0.0.0").strip()
        store_ver = (item.get("version") or "0.0.0").strip()

        installed = is_plugin_installed(pid)
        platforms_str = _get_item_display_platforms(item)

        installed_ver = _get_installed_version(pid) if installed else "0.0.0"
        update_available = installed and (_semver_tuple(store_ver) > _semver_tuple(installed_ver))

        with st.container(border=True):
            st.subheader(name)

            if installed:
                if update_available:
                    st.caption(
                        f"ID: {pid} • installed: {installed_ver} • store: {store_ver} • min tater: {min_ver}  ✅ update available"
                    )
                else:
                    st.caption(
                        f"ID: {pid} • installed: {installed_ver} • store: {store_ver} • min tater: {min_ver}"
                    )
            else:
                st.caption(f"ID: {pid} • version: {store_ver} • min tater: {min_ver}")

            if desc:
                st.write(desc)

            st.caption(f"Platforms: {platforms_str}")

            cols = st.columns([1, 1, 3])

            purge_store = cols[2].checkbox(
                "Delete Data?",
                value=False,
                key=f"store_purge_{pid}"
            )

            if installed:
                if update_available:
                    cols[0].warning("Update available")
                    if cols[1].button("Update", key=f"store_update_{pid}"):
                        ok, msg = install_plugin_from_shop_item(item, url.strip())
                        if ok:
                            st.success(f"{msg} (updated {installed_ver} → {store_ver})")

                            plugin_registry_mod.reload_plugins()
                            st.session_state.pop("shop_platform_filter", None)
                            st.session_state.pop("shop_search", None)

                            st.rerun()
                        else:
                            st.error(msg)
                else:
                    cols[0].success("Installed")
                    if cols[1].button("Remove", key=f"store_remove_{pid}"):
                        ok, msg = uninstall_plugin_file(pid)
                        if ok:
                            st.success(msg)
                            try:
                                set_plugin_enabled(pid, False)
                            except Exception:
                                pass

                            if purge_store:
                                ok2, msg2 = clear_plugin_redis_data(pid)
                                if ok2:
                                    st.success(f"Redis cleanup: {msg2}")
                                else:
                                    st.error(msg2)

                            _refresh_plugins_after_fs_change()
                            st.rerun()
                        else:
                            st.error(msg)
            else:
                cols[0].warning("Not installed")
                if cols[1].button("Install", key=f"store_install_{pid}"):
                    ok, msg = install_plugin_from_shop_item(item, url.strip())
                    if ok:
                        st.success(msg)

                        plugin_registry_mod.reload_plugins()
                        st.session_state.pop("shop_platform_filter", None)
                        st.session_state.pop("shop_search", None)

                        st.rerun()
                    else:
                        st.error(msg)

# ----------------- SYSTEM PROMPT -----------------
def build_system_prompt():
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")

    first, last = get_tater_name()
    personality = get_tater_personality().strip()

    persona_clause = ""
    if personality:
        persona_clause = (
            f"Voice style: {personality}. "
            "This affects tone only and never overrides tool/safety rules.\n\n"
        )

    # Planner mode injects canonical tool-use rules and enabled-tool index each turn.
    return (
        f"Current Date and Time is: {now}\n\n"
        f"You are {first} {last}, an AI assistant with tool access.\n"
        "Current platform: webui.\n"
        "Keep replies concise and clear.\n\n"
        f"{persona_clause}"
    )

# ----------------- UNIVERSAL MESSAGE NORMALIZATION ----------------------------
def _to_template_msg(role, content):
    """
    Return a dict shaped for the Jinja template:
      - string -> {"role": role, "content": "text"}
      - image  -> {"role": role, "content": "[Image attached]"}
      - audio  -> {"role": role, "content": "[Audio attached]"}
      - video  -> {"role": role, "content": "[Video attached]"}
      - file   -> {"role": role, "content": "[File attached] name (mimetype, size)"}
      - plugin_call -> stringify call as assistant text
      - plugin_response -> include final responses (skip waiting lines)
    """

    # --- Skip waiting lines from tools ---
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    # --- Include final plugin responses in context (text only / placeholders) ---
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        phase = content.get("phase", "final")
        if phase != "final":
            return None

        payload = content.get("content")

        # 1) Plain string
        if isinstance(payload, str):
            txt = payload.strip()
            if len(txt) > 4000:
                txt = txt[:4000] + " …"
            return {"role": "assistant", "content": txt}

        # 2) Media placeholders
        if isinstance(payload, dict) and payload.get("type") in ("image", "audio", "video"):
            kind = payload.get("type")
            name = payload.get("name") or ""
            return {"role": "assistant", "content": f"[{kind.capitalize()} from tool]{f' {name}' if name else ''}".strip()}

        # 3) Structured text fields
        if isinstance(payload, dict):
            for key in ("summary", "text", "message", "content"):
                if isinstance(payload.get(key), str) and payload.get(key).strip():
                    txt = payload[key].strip()
                    if len(txt) > 4000:
                        txt = txt[:4000] + " …"
                    return {"role": "assistant", "content": txt}

            # Fallback: compact JSON
            try:
                compact = json.dumps(payload, ensure_ascii=False)
                if len(compact) > 2000:
                    compact = compact[:2000] + " …"
                return {"role": "assistant", "content": compact}
            except Exception:
                return None

    # --- Represent plugin calls as plain text (so history still makes sense) ---
    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps({
            "function": content.get("plugin"),
            "arguments": content.get("arguments", {})
        }, indent=2)
        return {"role": "assistant", "content": as_text} if role == "assistant" else {"role": role, "content": as_text}

    # --- Media types ---
    if isinstance(content, dict) and content.get("type") == "image":
        return {"role": role, "content": "[Image attached]"}
    if isinstance(content, dict) and content.get("type") == "audio":
        return {"role": role, "content": "[Audio attached]"}
    if isinstance(content, dict) and content.get("type") == "video":
        return {"role": role, "content": "[Video attached]"}
    if isinstance(content, dict) and content.get("type") == "file":
        name = content.get("name") or "file"
        mimetype = content.get("mimetype") or ""
        size = content.get("size") or ""
        return {"role": role, "content": f"[File attached] {name} ({mimetype}, {size} bytes)"}

    # --- Strings and other fallbacks ---
    if isinstance(content, str):
        return {"role": role, "content": content}
    return {"role": role, "content": str(content)}

def _enforce_user_assistant_alternation(loop_messages):
    """
    Your template enforces strict alternation and requires the first message to be 'user'.
    This merges consecutive same-role messages and ensures the list starts with 'user'.
    """
    merged = []
    for m in loop_messages:
        if not m:  # None from filtered items
            continue
        if not merged:
            merged.append(m)
            continue
        if merged[-1]["role"] == m["role"]:
            # Merge by concatenation/extension depending on content type
            a, b = merged[-1]["content"], m["content"]
            if isinstance(a, str) and isinstance(b, str):
                merged[-1]["content"] = (a + "\n\n" + b).strip()
            elif isinstance(a, list) and isinstance(b, list):
                merged[-1]["content"] = a + b
            else:
                # Coerce to string merge if mixed
                merged[-1]["content"] = ( (a if isinstance(a, str) else str(a)) +
                                          "\n\n" +
                                          (b if isinstance(b, str) else str(b)) ).strip()
        else:
            merged.append(m)

    # Ensure first is user; if not, prepend an empty user turn
    if merged and merged[0]["role"] != "user":
        merged.insert(0, {"role": "user", "content": ""})

    return merged

# ----------------- PROCESSING FUNCTIONS -----------------
async def process_message(user_name, message_content, wait_callback=None):
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

    merged_registry, merged_enabled, _collisions = build_agent_registry(
        get_registry(),
        get_plugin_enabled,
    )

    session_scope_id = str(st.session_state.get("webui_session_id") or "").strip()
    if not session_scope_id:
        session_scope_id = str(uuid.uuid4())
        st.session_state["webui_session_id"] = session_scope_id

    origin = {
        "platform": "webui",
        "user": user_name,
        "user_id": user_name,
        "session_id": session_scope_id,
    }
    recent_media_refs = load_recent_media_refs(
        redis_client,
        platform="webui",
        scope=WEBUI_IMAGE_SCOPE,
        limit=8,
    )
    if recent_media_refs:
        origin["media_refs"] = recent_media_refs
    agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
    result = await run_cerberus_turn(
        llm_client=llm_client,
        platform="webui",
        history_messages=messages_list,
        registry=merged_registry,
        enabled_predicate=merged_enabled,
        context={"raw_message": message_content},
        user_text=message_content or "",
        scope=f"session:{session_scope_id}",
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
nav_options = ["Chat", "Plugins", "Auto Plugins", "Platforms", "Agent Lab", "Plugin Store", "Settings"]
if "active_view" not in st.session_state:
    st.session_state.active_view = nav_options[0]
elif st.session_state.active_view == "AI Tasks":
    st.session_state.active_view = "Settings"
elif st.session_state.active_view not in nav_options:
    st.session_state.active_view = nav_options[0]

st.sidebar.markdown("**Navigation**")
for opt in nav_options:
    if st.sidebar.button(opt, use_container_width=True, key=f"nav_btn_{opt}"):
        st.session_state.active_view = opt
        st.rerun()

active_view = st.session_state.active_view
st.sidebar.markdown("---")

# ------------------ PLATFORM MANAGEMENT ------------------
# Always-on core background services (not user-toggleable platforms).
if not _platform_thread_alive("ai_task_platform") and _should_autostart("ai_task_platform", exp=False):
    _start_platform("ai_task_platform")

auto_connected = []
for platform in platform_registry:
    key = platform["key"]  # e.g. irc_platform
    state_key = f"{key}_running"

    # Check Redis to determine if this platform should be running
    platform_should_run = redis_client.get(state_key) == "true"

    if platform_should_run:
        if _platform_thread_alive(key):
            auto_connected.append(platform.get("label") or platform.get("category") or platform.get("key"))
        elif _should_autostart(key, exp=False):
            _start_platform(key)
            auto_connected.append(platform.get("label") or platform.get("category") or platform.get("key"))

# ------------------ EXPERIMENTAL PLATFORM MANAGEMENT ------------------
exp_platforms = _discover_agent_lab_platforms()
for exp in exp_platforms:
    exp_key = exp.get("key")
    exp_path = exp.get("path")
    if not exp_key or not exp_path:
        continue
    state_key = f"exp:{exp_key}_running"
    if (redis_client.get(state_key) or "") == "true":
        if not _exp_platform_thread_alive(exp_key) and _should_autostart(exp_key, exp=True):
            _start_experiment_platform(exp_key, exp_path)

# Prepare plugin groupings
automation_plugins = _sort_plugins_for_display([
    p for p in get_registry().values()
    if set(getattr(p, "platforms", []) or []) == {"automation"} and not getattr(p, "notifier", False)
])
regular_plugins = _sort_plugins_for_display([
    p for p in get_registry().values()
    if set(getattr(p, "platforms", []) or []) != {"automation"} and not getattr(p, "notifier", False)
])

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
                                use_container_width=True
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
                            use_container_width=True
                        )

            else:
                st.write(content)

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
            _save_recent_webui_media_refs(msg)
            st.session_state.chat_messages.append({"role": "user", "content": msg})

        # ---- Send to LLM (even if only files were uploaded) ----
        status_box = None
        if hasattr(st, "status"):
            status_box = st.status(
                f"{first_name} is thinking…",
                state="running",
                expanded=False,
            )

        async def _wait_callback(func_name, plugin_obj):
            if not status_box:
                return
            if plugin_obj is None:
                display_name = f"kernel::{func_name}"
            else:
                display_name = (
                    getattr(plugin_obj, "plugin_name", None)
                    or getattr(plugin_obj, "pretty_name", None)
                    or getattr(plugin_obj, "name", None)
                    or func_name
                )
            status_box.update(
                label=f"{first_name} is working on: {display_name}",
                state="running",
            )

        if status_box is None:
            spinner_label = f"{first_name} is thinking..."
            with st.spinner(spinner_label):
                response_payload = run_async(process_message(uname, user_text, wait_callback=_wait_callback))
        else:
            response_payload = run_async(process_message(uname, user_text, wait_callback=_wait_callback))

        if status_box is not None:
            status_box.update(label=f"{first_name} finished.", state="complete")

        if isinstance(response_payload, dict) and isinstance(response_payload.get("responses"), list):
            responses = response_payload.get("responses") or []
        else:
            responses = [response_payload]

        for item in responses:
            st.session_state.chat_messages.append({"role": "assistant", "content": item})
            save_message("assistant", "assistant", item)
            _save_recent_webui_media_refs(item)

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

elif active_view == "Plugins":
    st.title("Plugins")
    st.write("Browse available plugins. Automation-only tools are listed separately.")
    render_plugin_list(regular_plugins, "No plugins available.")

elif active_view == "Auto Plugins":
    st.title("Automation Plugins")
    st.write("These plugins are available to the automation platform.")
    render_plugin_list(automation_plugins, "No plugins available.")

elif active_view == "Platforms":
    st.title("Platforms")
    render_platforms_panel(auto_connected)

elif active_view == "Agent Lab":
    render_agent_lab_page()

elif active_view == "Plugin Store":
    render_plugin_store_page()

elif active_view == "Settings":
    render_settings_page()
