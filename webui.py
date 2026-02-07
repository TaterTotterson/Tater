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
from PIL import Image
from io import BytesIO
from platform_registry import platform_registry
from plugin_loader import load_plugins_from_directory
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
from admin_gate import DEFAULT_ADMIN_ONLY_PLUGINS, REDIS_KEY as ADMIN_GATE_KEY, get_admin_only_plugins
from agent_lab_registry import build_agent_registry
from rss_store import get_all_feeds, set_feed, update_feed, delete_feed
from kernel_tools import (
    AGENT_PLUGINS_DIR,
    AGENT_PLATFORMS_DIR,
    validate_plugin,
    validate_platform,
    delete_file,
)
from planner_loop import should_use_agent_mode, run_planner_loop

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

# ------------------ Plugin Store / Installed Plugins ------------------
PLUGIN_DIR = os.getenv("TATER_PLUGIN_DIR", "plugins")  # where installed plugin .py files live
SHOP_MANIFEST_URL_DEFAULT = os.getenv(
    "TATER_SHOP_MANIFEST_URL",
    "https://raw.githubusercontent.com/TaterTotterson/Tater_Shop/main/manifest.json"
)

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
    plugins = load_plugins_from_directory(str(AGENT_PLUGINS_DIR))
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
    st.caption("Add feeds and customize delivery per feed. Leave targets blank to use notifier defaults.")

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

def render_tater_settings():
    st.subheader(f"{first_name} Settings")
    stored_count = int(redis_client.get("tater:max_store") or 20)
    llm_count = int(redis_client.get("tater:max_llm") or 8)
    default_first = redis_client.get("tater:first_name") or first_name
    default_last = redis_client.get("tater:last_name") or last_name
    default_personality = redis_client.get("tater:personality") or ""
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

    if st.button("Save Tater Settings", key="save_tater_settings"):
        redis_client.set("tater:max_store", new_store)
        redis_client.set("tater:max_llm", new_llm)
        redis_client.set("tater:first_name", first_input)
        redis_client.set("tater:last_name", last_input)
        redis_client.set("tater:personality", personality_input)
        st.success("Tater settings updated.")
        st.rerun()

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

    if redis_client.get(ADMIN_GATE_KEY) is None:
        st.info("Currently using the default admin-only list. Save to customize.")

    if unknown_current:
        st.warning(f"Unknown plugin IDs currently stored: {', '.join(unknown_current)}")

    selected = st.multiselect(
        "Admin-only plugins (by plugin id)",
        options=plugin_ids,
        default=known_current,
        help="Selected plugins can only be run by the admin user on Discord/Telegram/Matrix/IRC.",
        key="admin_gate_plugins",
    )

    col1, col2 = st.columns(2)
    if col1.button("Save Admin Tool Gating", key="save_admin_gating"):
        redis_client.set(ADMIN_GATE_KEY, json.dumps(selected))
        st.success("Admin-only plugin list saved.")

    if col2.button("Reset to Defaults", key="reset_admin_gating"):
        redis_client.delete(ADMIN_GATE_KEY)
        st.success("Admin-only plugin list reset to defaults.")
        st.rerun()

def render_settings_page():
    st.title("Settings")
    render_webui_settings()
    st.markdown("---")
    render_homeassistant_settings()
    st.markdown("---")
    render_tater_settings()
    st.markdown("---")
    render_admin_gating_settings()

def render_notifiers_page(notifier_plugins):
    st.title("Notifiers")
    st.write("Manage notifier plugins used by RSS and other systems.")
    st.subheader("Notifier Plugins")
    st.caption("Enable at least one notifier to receive notifications.")
    render_plugin_list(notifier_plugins, "No notifier plugins available.")


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


def render_ai_tasks_page():
    st.title("AI Tasks")
    st.caption("Manage AI tasks and reminders.")

    schedules = _load_schedules()
    if not schedules:
        st.info("No schedules yet.")
        return

    def _sort_key(item):
        return float(item.get("_due_ts") or 0.0)

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
        recur_label = f"Every {int(interval)}s" if interval > 0 else "One-shot"
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
            "This affects tone only. "
            "You must always follow tool and safety rules.\n\n"
        )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"You are {first} {last}, an AI assistant with tool access.\n\n"
        f"{persona_clause}"
        "Current platform: webui.\n"
        "Tool strategy:\n"
        "- Answer directly when no tool is required.\n"
        "- Tools are discovered on-demand; not all tools are described here. If unsure, call list_plugins.\n"
        "- Examples that require list_plugins: weather/forecast, news, stocks, sports scores, downloads, music/song generation, image/video generation, camera feeds/snapshots (front/back yard, porch, driveway, garage), camera/sensor status, smart-home actions.\n"
        "- The user does not need to explicitly request tool use; if a tool is appropriate, use it.\n"
        "- Prefer using a tool over attempting to answer from scratch when a tool could fulfill the request.\n"
        "- If external actions/live data are needed, call list_plugins first.\n- If the user asks to control devices or services or interact with external systems, call list_plugins first.\n"
        "- If the user asks about a specific tool/plugin by name or asks what a tool can do, call list_plugins or get_plugin_help instead of guessing.\n"
        "- If you might need a tool or are unsure a capability exists, call list_plugins before saying it is unavailable.\n"
        "- Optionally call get_plugin_help before calling a plugin.\n"
        "- If missing required information, ask concise follow-up questions.\n"
        "- Only ask for inputs a tool explicitly requires (from list_plugins needs or get_plugin_help required_args). If defaults exist, proceed without asking.\n"
        "- Only call plugins compatible with webui.\n"
        "- If a capability is unavailable here, explain and list where it is available.\n"
        "- For tool calls, output only JSON: {\"function\":\"name\",\"arguments\":{...}}\n"
        "- Meta-tools always available: list_plugins, get_plugin_help, list_platforms_for_plugin.\n"
        "- Never claim tool success unless the tool result confirms success.\n"
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
    final_system_prompt = build_system_prompt()
    max_llm = int(redis_client.get("tater:max_llm") or 8)
    history = load_chat_history_tail(max_llm)

    messages_list = [{"role": "system", "content": final_system_prompt}]

    user_display = (get_chat_settings().get("username") or "User").strip()
    messages_list.append({
        "role": "system",
        "content": f"The user's name is {user_display}. When addressing them, use their name naturally and sparingly."
    })

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
    messages_list.extend(loop_messages)

    merged_registry, merged_enabled, _collisions = build_agent_registry(
        get_registry(),
        get_plugin_enabled,
    )

    _use_agent, active_task_id, _reason = should_use_agent_mode(
        user_text=message_content or "",
        platform="webui",
        scope="chat",
        r=redis_client,
    )
    origin = {
        "platform": "webui",
        "user": user_name,
    }
    result = await run_planner_loop(
        llm_client=llm_client,
        platform="webui",
        history_messages=messages_list,
        registry=merged_registry,
        enabled_predicate=merged_enabled,
        context={"raw_message": message_content},
        user_text=message_content or "",
        scope="chat",
        task_id=active_task_id,
        origin=origin,
        redis_client=redis_client,
        wait_callback=wait_callback,
    )
    responses = []
    if result.get("text"):
        responses.append(result["text"])
    for item in result.get("artifacts") or []:
        responses.append(_normalize_plugin_response_item(item))
    return {"responses": responses, "agent": True}


# ------------------ NAVIGATION ------------------
nav_options = ["Chat", "Plugins", "Auto Plugins", "Notifiers", "Platforms", "AI Tasks", "Agent Lab", "Plugin Store", "Settings"]
if "active_view" not in st.session_state:
    st.session_state.active_view = nav_options[0]

st.sidebar.markdown("**Navigation**")
for opt in nav_options:
    if st.sidebar.button(opt, use_container_width=True, key=f"nav_btn_{opt}"):
        st.session_state.active_view = opt
        st.rerun()

active_view = st.session_state.active_view
st.sidebar.markdown("---")

# ------------------ PLATFORM MANAGEMENT ------------------
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
notifier_plugins = _sort_plugins_for_display([
    p for p in get_registry().values()
    if getattr(p, "notifier", False)
])
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
                "type": "file",
                "id": file_id,
                "name": name,
                "mimetype": mimetype,
                "size": len(data),
            }
            save_message("user", uname, msg)
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

elif active_view == "Notifiers":
    render_notifiers_page(notifier_plugins)

elif active_view == "AI Tasks":
    render_ai_tasks_page()

elif active_view == "Agent Lab":
    render_agent_lab_page()

elif active_view == "Plugin Store":
    render_plugin_store_page()

elif active_view == "Settings":
    render_settings_page()
