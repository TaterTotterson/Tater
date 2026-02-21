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
from datetime import datetime
from typing import Any, Dict, List, Optional
from PIL import Image
from io import BytesIO
from platform_registry import platform_registry
from helpers import (
    run_async,
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
from kernel_tools import (
    AGENT_PLATFORMS_DIR,
)
from cerberus import (
    run_cerberus_turn,
    resolve_agent_limits,
)
from vision_settings import (
    get_vision_settings as get_shared_vision_settings,
    save_vision_settings as save_shared_vision_settings,
)
from conversation_media_refs import load_recent_media_refs, save_media_ref
from emoji_responder import get_emoji_settings as get_core_emoji_settings, save_emoji_settings as save_core_emoji_settings
from webui_cerberus import (
    render_cerberus_settings,
    render_cerberus_metrics_dashboard,
    render_cerberus_ledger_settings,
)
from webui_platforms import (
    render_platforms_panel,
)
from webui_plugins import (
    _sort_plugins_for_display,
    render_plugin_list,
)
from webui_ai_tasks import render_ai_tasks_page
from webui_memory import render_memory_page, wipe_memory_platform_data
from webui_plugin_store import (
    _enabled_missing_plugin_ids,
    ensure_plugins_ready,
    render_plugin_store_page,
)
from webui_settings import render_settings_page
from webui_chat import (
    configure_chat_helpers,
    save_message,
    _media_type_from_mimetype,
    _save_recent_webui_media_refs,
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
WEBUI_IMAGE_SCOPE = "chat"

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
    save_media_ref_fn=save_media_ref,
    webui_image_scope=WEBUI_IMAGE_SCOPE,
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

    merged_registry = dict(get_registry() or {})
    merged_enabled = get_plugin_enabled

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
ai_tasks_enabled = str(redis_client.get("ai_task_platform_running") or "").strip().lower() == "true"
memory_platform_enabled = str(redis_client.get("memory_platform_running") or "").strip().lower() == "true"
nav_options = ["Chat", "Plugins", "Auto Plugins", "Platforms", "Plugin Store", "Settings"]
if ai_tasks_enabled:
    nav_options.insert(4, "AI Tasks")
if memory_platform_enabled:
    insert_idx = nav_options.index("Plugin Store") if "Plugin Store" in nav_options else nav_options.index("Settings")
    nav_options.insert(insert_idx, "Memory")
if "active_view" not in st.session_state:
    st.session_state.active_view = nav_options[0]
elif st.session_state.active_view == "AI Tasks" and not ai_tasks_enabled:
    st.session_state.active_view = "Platforms"
elif st.session_state.active_view == "Memory" and not memory_platform_enabled:
    st.session_state.active_view = "Platforms"
elif st.session_state.active_view not in nav_options:
    st.session_state.active_view = nav_options[0]

st.sidebar.markdown("**Navigation**")
for opt in nav_options:
    if st.sidebar.button(opt, width="stretch", key=f"nav_btn_{opt}"):
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
    render_platforms_panel(
        platform_registry=platform_registry,
        redis_client=redis_client,
        start_platform_fn=_start_platform,
        stop_platform_fn=_stop_platform,
        wipe_memory_platform_data_fn=wipe_memory_platform_data,
        auto_connected=auto_connected,
    )

elif active_view == "AI Tasks":
    render_ai_tasks_page(redis_client=redis_client)

elif active_view == "Memory":
    render_memory_page()

elif active_view == "Plugin Store":
    render_plugin_store_page()

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
        render_cerberus_ledger_settings_fn=render_cerberus_ledger_settings,
    )
