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
import plugin_registry as plugin_registry_mod
from plugin_registry import plugin_registry
from urllib.parse import urljoin
from datetime import datetime
from PIL import Image
from io import BytesIO
from rss import RSSManager
from platform_registry import platform_registry
from helpers import (
    run_async,
    set_main_loop,
    parse_function_json,
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
            loaded = plugin_registry.get(plugin_id)
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

def auto_restore_missing_plugins(manifest_url: str):
    """
    Restore any plugins that are ENABLED in Redis but missing on disk.
    Uses the shop manifest as the source of install URLs.
    """
    try:
        manifest = fetch_shop_manifest(manifest_url)
    except Exception as e:
        logging.error(f"Failed to load manifest for restore: {e}")
        return

    items = manifest.get("plugins") or manifest.get("items") or manifest.get("data") or []
    if not isinstance(items, list):
        logging.error("Manifest format unexpected during restore.")
        return

    restored = []

    # If a plugin was enabled, it should be restorable. We can only restore things
    # that exist in the manifest, so we iterate manifest ids and check Redis enabled.
    for item in items:
        pid = (item.get("id") or "").strip()
        if not pid:
            continue

        # IMPORTANT: avoid bool("false") == True
        try:
            raw = get_plugin_enabled(pid)
            enabled = raw if isinstance(raw, bool) else str(raw).lower() == "true"
        except Exception:
            enabled = False

        if enabled and not is_plugin_installed(pid):
            ok, msg = install_plugin_from_shop_item(item, manifest_url)
            if ok:
                restored.append(pid)
            else:
                logging.error(f"[restore] {pid}: {msg}")

    if restored:
        logging.info(f"Restored missing enabled plugins: {', '.join(restored)}")
    else:
        logging.info("No enabled plugins needed restoring (or none were enabled).")

if not st.session_state.get("did_auto_restore_plugins"):
    shop_url = (redis_client.get("tater:shop_manifest_url") or SHOP_MANIFEST_URL_DEFAULT).strip()
    auto_restore_missing_plugins(shop_url)
    _refresh_plugins_after_fs_change()
    st.session_state["did_auto_restore_plugins"] = True


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

# ------------------ RSS (PLATFORM-STYLE SINGLETON) ------------------

@st.cache_resource(show_spinner=False)
def _start_rss(_llm_client):
    """
    Platform-style singleton:
    - cache_resource prevents re-starting on Streamlit reruns
    - returns (thread, stop_event)
    - resilient restart loop inside thread
    """
    stop_event = st.session_state.setdefault("rss_stop_event", threading.Event())
    thread = st.session_state.get("rss_thread")

    # Don't start again if already running
    if thread and thread.is_alive():
        return thread, stop_event

    stop_event.clear()

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        backoff = 1.0  # seconds (will grow up to 10s)
        max_backoff = 10.0

        while not stop_event.is_set():
            try:
                rss_manager = RSSManager(llm_client=_llm_client)
                loop.run_until_complete(rss_manager.poll_feeds(stop_event=stop_event))

                if stop_event.is_set():
                    break

                logging.getLogger("RSS").warning("poll_feeds() returned; restarting shortly…")
                time.sleep(1.0)
                backoff = 1.0

            except asyncio.CancelledError:
                logging.getLogger("RSS").info("RSS poller cancelled; exiting thread.")
                break
            except KeyboardInterrupt:
                logging.getLogger("RSS").info("RSS poller interrupted; exiting thread.")
                break
            except Exception as e:
                logging.getLogger("RSS").error(f"RSS crashed: {e}", exc_info=True)
                time.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    st.session_state["rss_thread"] = t
    st.session_state["rss_stop_event"] = stop_event
    return t, stop_event


def _stop_rss():
    stop_event = st.session_state.get("rss_stop_event")
    thread = st.session_state.get("rss_thread")

    if stop_event:
        stop_event.set()

    if thread and thread.is_alive():
        thread.join(timeout=0.25)

    # IMPORTANT: allow restarting later
    try:
        _start_rss.clear()
    except Exception:
        pass

    st.session_state["rss_thread"] = None
    return thread, stop_event

@st.cache_resource(show_spinner=False)
def _start_platform(key: str):
    st.session_state.setdefault("platform_threads", {})
    st.session_state.setdefault("platform_stop_flags", {})

    thread = st.session_state["platform_threads"].get(key)
    stop_flag = st.session_state["platform_stop_flags"].get(key)

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

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()

    st.session_state["platform_threads"][key] = thread
    st.session_state["platform_stop_flags"][key] = stop_flag
    return thread, stop_flag

def _stop_platform(key: str):
    threads = st.session_state.get("platform_threads", {})
    flags = st.session_state.get("platform_stop_flags", {})

    thread = threads.get(key)
    stop_flag = flags.get(key)

    if stop_flag:
        stop_flag.set()

    if thread and thread.is_alive():
        thread.join(timeout=0.5)

    # keep entries (or optionally clear them)
    # threads[key] = None
    # flags[key] = None

# ---- background job refresh hook ----
if redis_client.get("webui:needs_rerun") == "true":
    redis_client.delete("webui:needs_rerun")
    st.rerun()

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

# ---- session_state containers (must exist before platform start/stop) ----
st.session_state.setdefault("platform_threads", {})
st.session_state.setdefault("platform_stop_flags", {})

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

def get_rss_enabled():
    enabled = redis_client.get("rss:enabled")
    if enabled is None:
        return True
    return enabled.lower() == "true"

def set_rss_enabled(enabled):
    redis_client.set("rss:enabled", "true" if enabled else "false")

def get_rss_running():
    return (redis_client.get("rss_running") or "false").lower() == "true"

def set_rss_running(enabled: bool):
    redis_client.set("rss_running", "true" if enabled else "false")

def render_plugin_controls(plugin_name, label=None):
    current_state = get_plugin_enabled(plugin_name)
    toggle_state = st.toggle(label or plugin_name, value=current_state, key=f"plugin_toggle_{plugin_name}")

    if toggle_state != current_state:
        set_plugin_enabled(plugin_name, toggle_state)
        st.rerun()

def render_platform_controls(platform, redis_client):
    category     = platform["label"]
    key          = platform["key"]
    required     = platform["required"]
    short_name   = category.replace(" Settings", "").strip()
    state_key    = f"{key}_running"
    cooldown_key = f"tater:cooldown:{key}"
    cooldown_secs = 10

    # read current on/off from Redis
    is_running = (redis_client.get(state_key) == "true")
    emoji      = "🟢" if is_running else "🔴"

    # force‐off gadget for cooldown toggle feedback
    force_off_key = f"{category}_toggle_force_off"
    if st.session_state.get(force_off_key):
        del st.session_state[force_off_key]
        is_enabled = False
        new_toggle = False
    else:
        new_toggle = st.toggle(f"{emoji} Enable {short_name}",
                               value=is_running,
                               key=f"{category}_toggle")
        is_enabled = new_toggle

    # --- TURNING ON ---
    if is_enabled and not is_running:
        # cooldown check
        last = redis_client.get(cooldown_key)
        now  = time.time()
        if last and now - float(last) < cooldown_secs:
            remaining = int(cooldown_secs - (now - float(last)))
            st.warning(f"⏳ Wait {remaining}s before restarting {short_name}.")
            st.session_state[force_off_key] = True
            st.rerun()

        # actually start it
        _start_platform(key)
        redis_client.set(state_key, "true")
        st.success(f"{short_name} started.")

    # --- TURNING OFF ---
    elif not is_enabled and is_running:
        _stop_platform(key)
        redis_client.set(state_key, "false")
        redis_client.set(cooldown_key, str(time.time()))
        st.success(f"{short_name} stopped.")

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

    if st.button(f"Save {short_name} Settings", key=f"save_{category}_unique"):
        # coerce all values to strings for Redis HSET
        save_map = {
            k: (json.dumps(v) if isinstance(v, (dict, list, bool)) else str(v))
            for k, v in new_settings.items()
        }
        redis_client.hset(redis_key, mapping=save_map)
        st.success(f"{short_name} settings saved.")

    # Trigger refresh if toggle changed
    if new_toggle != is_running:
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
                    loaded = plugin_registry.get(plugin_id)
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

def _platform_sort_name(p):
    return (p.get("label") or p.get("category") or p.get("key") or "").lower()

def render_platforms_panel(auto_connected=None):
    st.subheader("Platforms")
    if auto_connected:
        st.success(f"{', '.join(auto_connected)} auto-connected.")
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

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Clear Chat History", key="clear_history"):
            clear_chat_history()
            st.success("Chat history cleared.")
    with col2:
        if st.button("Clear Active Plugin Jobs", key="clear_plugin_jobs"):
            for key in redis_client.scan_iter("webui:plugin_jobs:*"):
                redis_client.delete(key)
            st.success("All active plugin jobs have been cleared.")
            st.rerun()

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

def render_settings_page():
    st.title("Settings")
    render_webui_settings()
    st.markdown("---")
    render_tater_settings()

def render_rss_settings_page(rss_plugins, rss_enabled):
    st.title("RSS Feed")
    st.write("Control RSS feed monitoring and manage notification plugins.")

    toggle_state = st.toggle(
        "Enable RSS feed watcher",
        value=rss_enabled,
        help="When enabled, RSS feeds will be polled and new items will be sent to the selected notifier plugins.",
        key="rss_enabled_toggle"
    )

    if toggle_state != rss_enabled:
        set_rss_enabled(toggle_state)
        set_rss_running(toggle_state)

        if toggle_state:
            _start_rss(llm_client)
        else:
            _stop_rss()

        st.rerun()

    st.markdown("---")
    st.subheader("RSS Notifier Plugins")
    st.caption("These plugins deliver RSS updates. Enable at least one to receive notifications.")
    render_plugin_list(rss_plugins, "No RSS notifier plugins available.")

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
        loaded = plugin_registry.get(plugin_id)
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
            loaded = plugin_registry.get(pid)
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
            f"You should speak and behave like {personality}. "
            "This affects tone, voice, and phrasing only. "
            "You must always follow system instructions, tool rules, and safety constraints exactly.\n\n"
        )

    base_prompt = (
        f"You are {first} {last}, an AI assistant with access to various tools and plugins.\n\n"
        f"{persona_clause}"
        "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else):\n\n"
    )

    tool_instructions = "\n\n".join(
        f"Tool: {plugin.name}\n"
        f"Description: {getattr(plugin, 'description', 'No description provided.')}\n"
        f"{plugin.usage}"
        for plugin in plugin_registry.values()
        if ("webui" in plugin.platforms or "both" in plugin.platforms) and get_plugin_enabled(plugin.name)
    )

    behavior_guard = (
        "Only call a tool if the user's latest message clearly requests an action — such as 'generate', 'summarize', or 'download'.\n"
        "Never call a tool in response to casual or friendly messages like 'thanks', 'lol', or 'cool'.\n"
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"{base_prompt}\n\n"
        f"{tool_instructions}\n\n"
        f"{behavior_guard}"
        "If no function is needed, reply normally.\n"
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
async def process_message(user_name, message_content):
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

    # --- timing + usage capture ---
    start_ts = time.time()
    response = await llm_client.chat(messages_list)
    elapsed = max(1e-6, time.time() - start_ts)

    # Try to use real usage; fall back to rough estimate if missing
    usage = response.get("usage") or {}
    prompt_tok = int(usage.get("prompt_tokens") or 0)
    comp_tok   = int(usage.get("completion_tokens") or 0)
    total_tok  = int(usage.get("total_tokens") or (prompt_tok + comp_tok))

    if total_tok == 0:
        # heuristic: ~4 chars/token on average
        out_text = (response.get("message", {}) or {}).get("content", "") or ""
        total_tok = max(1, int(len(out_text) / 4))
        comp_tok = total_tok  # best-effort; no prompt/comp split

    tps_total = total_tok / elapsed
    tps_comp  = (comp_tok / elapsed) if comp_tok else None

    # Store for UI
    redis_client.hset("webui:last_llm_stats", mapping={
        "model": str(response.get("model") or ""),
        "elapsed": f"{elapsed:.6f}",
        "prompt_tokens": str(prompt_tok),
        "completion_tokens": str(comp_tok),
        "total_tokens": str(total_tok),
        "tps_total": f"{tps_total:.2f}",
        "tps_comp": f"{tps_comp:.2f}" if tps_comp is not None else "",
        "ts": str(time.time()),
    })

    return response["message"]["content"].strip()


# ----------------- BACKGROUND PLUGIN JOBS -----------------
def start_plugin_job(plugin_name, args, llm_client):
    job_id = str(uuid.uuid4())
    redis_key = f"webui:plugin_jobs:{job_id}"
    redis_client.hset(redis_key, mapping={
        "status": "pending",
        "is_running": "1",
        "is_done": "0",
        "plugin": plugin_name,
        "args": json.dumps(args),
        "created_at": time.time()
    })

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

        # plugin media may come as bytes in `data` or `bytes`
        raw = None
        if isinstance(item.get("data"), (bytes, bytearray)):
            raw = bytes(item["data"])
        elif isinstance(item.get("bytes"), (bytes, bytearray)):
            raw = bytes(item["bytes"])

        # If it's already a reference (id present) or no raw bytes, leave it alone
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

    def job_runner():
        try:
            plugin = plugin_registry.get(plugin_name)
            if not plugin:
                raise RuntimeError(f"Plugin '{plugin_name}' is no longer installed.")

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(plugin.handle_webui(args, llm_client))
            finally:
                loop.close()

            responses = result if isinstance(result, list) else [result]

            # normalize all responses so bytes never hit json.dumps
            normalized = []
            for r in responses:
                if isinstance(r, list):
                    normalized.append([_normalize_plugin_response_item(x) for x in r])
                else:
                    normalized.append(_normalize_plugin_response_item(r))

            redis_client.hset(redis_key, mapping={
                "status": "done",
                "is_running": "0",
                "is_done": "1",
                "responses": json.dumps(normalized)
            })
        except Exception as e:
            redis_client.hset(redis_key, mapping={
                "status": "error",
                "is_running": "0",
                "is_done": "0",
                "error": str(e)
            })
        finally:
            redis_client.set("webui:needs_rerun", "true")

    threading.Thread(target=job_runner, daemon=True).start()
    return job_id

async def process_function_call(response_json, user_question=""):
    func = response_json.get("function")
    args = response_json.get("arguments", {})

    plugin = plugin_registry.get(func)
    if not plugin:
        return "Received an unknown or uninstalled function call."

    # Save structured plugin_call marker
    save_message("assistant", "assistant", {
        "marker": "plugin_call",
        "plugin": func,
        "arguments": args
    })

    # Optional: waiting status line
    if hasattr(plugin, "waiting_prompt_template"):
        wait_msg = plugin.waiting_prompt_template.format(mention="User")

        wait_response = await llm_client.chat(
            messages=[
                {"role": "system", "content": "Write one short, friendly status line for the user."},
                {"role": "user", "content": wait_msg}
            ]
        )
        wait_text = wait_response["message"]["content"].strip()

        # Save waiting message to Redis
        save_message("assistant", "assistant", {
            "marker": "plugin_wait",
            "content": wait_text
        })

        # Append waiting message to session state for persistence
        st.session_state.chat_messages.append({
            "role": "assistant",
            "content": {
                "marker": "plugin_wait",
                "content": wait_text
            }
        })

        # Immediate feedback: render waiting message now before rerun
        with st.chat_message("assistant", avatar=assistant_avatar):
            st.write(wait_text)

    # Start background plugin job (new logic)
    start_plugin_job(func, args, llm_client)

    # Let the job return its response later
    return []

# ------------------ NAVIGATION ------------------
nav_options = ["Chat", "Plugins", "Auto Plugins", "Platforms", "RSS Feed", "Plugin Store", "Settings"]
if "active_view" not in st.session_state:
    st.session_state.active_view = nav_options[0]

st.sidebar.markdown("**Navigation**")
for opt in nav_options:
    if st.sidebar.button(opt, use_container_width=True, key=f"nav_btn_{opt}"):
        st.session_state.active_view = opt
        st.rerun()

active_view = st.session_state.active_view
st.sidebar.markdown("---")

# ------------------ RSS ------------------
rss_enabled = get_rss_enabled()

# If you've never set rss_running before, initialize it from rss:enabled once.
if redis_client.get("rss_running") is None:
    set_rss_running(rss_enabled)

rss_should_run = get_rss_running()

if rss_should_run:
    _start_rss(llm_client)
else:
    _stop_rss()

# ------------------ PLATFORM MANAGEMENT ------------------
auto_connected = []
for platform in platform_registry:
    key = platform["key"]  # e.g. irc_platform
    state_key = f"{key}_running"

    # Check Redis to determine if this platform should be running
    platform_should_run = redis_client.get(state_key) == "true"

    if platform_should_run:
        _start_platform(key)
        auto_connected.append(platform.get("label") or platform.get("category") or platform.get("key"))

# Prepare plugin groupings
rss_plugins = _sort_plugins_for_display([
    p for p in plugin_registry.values()
    if getattr(p, "notifier", False)
])
automation_plugins = _sort_plugins_for_display([
    p for p in plugin_registry.values()
    if set(getattr(p, "platforms", []) or []) == {"automation"} and not getattr(p, "notifier", False)
])
regular_plugins = _sort_plugins_for_display([
    p for p in plugin_registry.values()
    if set(getattr(p, "platforms", []) or []) != {"automation"} and not getattr(p, "notifier", False)
])

# Ensure chat history is available for any view
if "chat_messages" not in st.session_state:
    full_history = load_chat_history()
    max_display = int(redis_client.get("tater:max_display") or 8)
    st.session_state.chat_messages = full_history[-max_display:]

# Check for completed plugin jobs (same behavior, fewer Redis round trips)
job_keys = list(redis_client.scan_iter(match="webui:plugin_jobs:*", count=2000))
if job_keys:
    pipe = redis_client.pipeline()
    for key in job_keys:
        pipe.hgetall(key)
    jobs = pipe.execute()

    for key, job in zip(job_keys, jobs):
        status = job.get("status")

        if status == "done":
            try:
                responses = json.loads(job.get("responses", "[]"))
            except Exception:
                responses = []

            for r in responses:
                item = {
                    "role": "assistant",
                    "content": {
                        "marker": "plugin_response",
                        "phase": "final",
                        "content": r
                    }
                }
                st.session_state.chat_messages.append(item)
                save_message("assistant", "assistant", item["content"])

            redis_client.delete(key)

        elif status == "error":
            err = job.get("error") or "Unknown error"
            item = {
                "role": "assistant",
                "content": f"❌ Plugin error: {err}"
            }
            st.session_state.chat_messages.append(item)
            save_message("assistant", "assistant", item["content"])
            redis_client.delete(key)

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
    prompt = st.chat_input(
        f"Chat with {first_name}…",
        accept_file="multiple",              # change to "directory" if you want folder uploads
        max_upload_size=WEBUI_ATTACH_MAX_MB_EACH
    )

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
        with st.spinner(f"{first_name} is thinking..."):
            response_text = run_async(process_message(uname, user_text))

        func_call = parse_function_json(response_text)
        if func_call:
            func_result = run_async(process_function_call(func_call, user_text))
            responses = func_result if isinstance(func_result, list) else [func_result]
        else:
            responses = [response_text]

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

    # ------------------ pending plugin jobs indicator (BOTTOM, WAIT FOR TRANSITION) ------------------
    pending_plugins = []
    pending_keys = []

    job_keys = list(redis_client.scan_iter(match="webui:plugin_jobs:*", count=2000))
    if job_keys:
        pipe = redis_client.pipeline()
        for key in job_keys:
            pipe.hget(key, "status")
            pipe.hget(key, "plugin")
        results = pipe.execute()

        for i, key in enumerate(job_keys):
            status = results[i * 2]
            plugin_name = results[i * 2 + 1]

            if status == "pending":
                pending_keys.append(key)

                plugin = plugin_registry.get(plugin_name) if plugin_name else None
                if plugin:
                    display_name = (
                        getattr(plugin, "plugin_name", None)
                        or getattr(plugin, "pretty_name", None)
                        or plugin.name
                    )
                    pending_plugins.append(display_name)
                elif plugin_name:
                    pending_plugins.append(plugin_name)

    if pending_plugins:
        names_str = ", ".join(pending_plugins)

        # Prefer newer Streamlit status UI when available; fall back to spinner.
        use_status = hasattr(st, "status")

        if use_status:
            status_box = st.status(
                f"{first_name} is working on: {names_str}",
                state="running",
                expanded=False
            )
        else:
            status_box = None

        def _update_label():
            # Recompute names each tick so the list stays accurate as jobs finish/start
            cur_names = []
            cur_keys = []

            keys = list(redis_client.scan_iter(match="webui:plugin_jobs:*", count=2000))
            if not keys:
                return "", []

            pipe = redis_client.pipeline()
            for k in keys:
                pipe.hget(k, "status")
                pipe.hget(k, "plugin")
            vals = pipe.execute()

            for idx, k in enumerate(keys):
                stt = vals[idx * 2]
                plg = vals[idx * 2 + 1]
                if stt == "pending":
                    cur_keys.append(k)
                    p = plugin_registry.get(plg) if plg else None
                    if p:
                        cur_names.append(
                            getattr(p, "plugin_name", None)
                            or getattr(p, "pretty_name", None)
                            or p.name
                        )
                    elif plg:
                        cur_names.append(plg)

            return ", ".join(cur_names), cur_keys

        previous_pending = set(pending_keys)

        while True:
            transition_detected = False
            current_pending = set()

            job_keys_now = list(redis_client.scan_iter(match="webui:plugin_jobs:*", count=2000))
            if job_keys_now:
                pipe = redis_client.pipeline()
                for key in job_keys_now:
                    pipe.hget(key, "status")
                statuses = pipe.execute()

                for key, status in zip(job_keys_now, statuses):
                    if status == "pending":
                        current_pending.add(key)
                    else:
                        # pending -> done/error transition
                        if key in previous_pending:
                            transition_detected = True

            # A key that used to exist vanished (deleted after processing) = transition
            for key in list(previous_pending):
                if not redis_client.exists(key):
                    transition_detected = True

            # Update the nicer UI label while we wait
            if status_box is not None:
                new_names, _ = _update_label()
                if new_names:
                    status_box.update(
                        label=f"{first_name} is working on: {new_names}",
                        state="running"
                    )
                else:
                    # Don't mark complete here — a transition likely happened and we're about to rerun
                    status_box.update(
                        label=f"{first_name} is updating plugin results…",
                        state="running"
                    )

            # Stop waiting if anything finished OR nothing is pending anymore
            if transition_detected or not current_pending:
                break

            previous_pending = current_pending
            time.sleep(1)

        # Close out the status box nicely (only if truly finished)
        if status_box is not None:
            # Are there still pending jobs right now?
            keys = list(redis_client.scan_iter(match="webui:plugin_jobs:*", count=2000))
            still_pending = False
            if keys:
                pipe = redis_client.pipeline()
                for k in keys:
                    pipe.hget(k, "status")
                stts = pipe.execute()
                still_pending = any(s == "pending" for s in stts)

            if still_pending:
                # Keep it "running" — rerun will refresh the label anyway
                status_box.update(
                    label=f"{first_name} is still working on plugins…",
                    state="running"
                )
            else:
                status_box.update(
                    label=f"{first_name} updated plugin results.",
                    state="complete"
                )

        st.rerun()

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

elif active_view == "RSS Feed":
    render_rss_settings_page(rss_plugins, rss_enabled)

elif active_view == "Plugin Store":
    render_plugin_store_page()

elif active_view == "Settings":
    render_settings_page()