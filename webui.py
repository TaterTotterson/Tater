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
from datetime import datetime
from PIL import Image
from io import BytesIO
from plugin_registry import plugin_registry
from rss import RSSManager
from platform_registry import platform_registry
from helpers import (
    LLMClientWrapper,
    run_async,
    set_main_loop,
    parse_function_json,
    get_tater_name,
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

@st.cache_resource(show_spinner=False)
def _start_rss(_llm_client):
    """
    Start the RSS poller in a resilient background thread.
    If poll_feeds() exits or throws, we log it and restart after a short backoff.
    """
    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        backoff = 1.0  # seconds (will grow up to 10s)
        max_backoff = 10.0

        while True:
            try:
                rss_manager = RSSManager(llm_client=_llm_client)
                # If poll_feeds() is a long-lived loop, this blocks until it raises or returns.
                loop.run_until_complete(rss_manager.poll_feeds())
                # If it returned cleanly, restart after a tiny pause to avoid hot-looping.
                logging.getLogger("RSS").warning("poll_feeds() returned; restarting shortly…")
                time.sleep(1.0)
                backoff = 1.0  # reset backoff after a clean return
            except asyncio.CancelledError:
                logging.getLogger("RSS").info("RSS poller cancelled; exiting thread.")
                break
            except KeyboardInterrupt:
                logging.getLogger("RSS").info("RSS poller interrupted; exiting thread.")
                break
            except Exception as e:
                logging.getLogger("RSS").error(f"RSS crashed: {e}", exc_info=True)
                time.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)  # exponential backoff

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t

@st.cache_resource(show_spinner=False)
def _start_platform(key):
    stop_flag = st.session_state.setdefault("platform_stop_flags", {}).get(key)
    thread = st.session_state.setdefault("platform_threads", {}).get(key)

    # Don't start again if already running
    if thread and thread.is_alive():
        return thread, stop_flag

    stop_flag = threading.Event()

    def runner():
        try:
            module = importlib.import_module(f"platforms.{key}")
            if hasattr(module, "run"):
                module.run(stop_event=stop_flag)
            else:
                print(f"⚠️ No run(stop_event) in platforms.{key}")
        except Exception as e:
            print(f"❌ Error in platform {key}: {e}")

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()

    # Save for future shutdown/restart
    st.session_state["platform_threads"][key] = thread
    st.session_state["platform_stop_flags"][key] = stop_flag

    return thread, stop_flag

# Redis configuration for the web UI (using a separate DB)
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

llm_host = os.getenv('LLM_HOST', '127.0.0.1')
llm_port = os.getenv('LLM_PORT', '11434')

# If LLM_HOST already includes http(s), don't append port
if llm_host.startswith("http://") or llm_host.startswith("https://"):
    # Allow skipping port entirely for APIs like OpenAI
    if llm_port and not llm_host.endswith(f":{llm_port}"):
        llm_client = LLMClientWrapper(host=f'{llm_host}:{llm_port}')
    else:
        llm_client = LLMClientWrapper(host=llm_host)
else:
    llm_client = LLMClientWrapper(host=f'http://{llm_host}:{llm_port}')

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
        "content": content  # can be string or dict
    }

    key = "webui:chat_history"
    redis_client.rpush(key, json.dumps(message_data))

    try:
        max_store = int(redis_client.get("tater:max_store") or 20)
    except (ValueError, TypeError):
        max_store = 20

    if max_store > 0:
        redis_client.ltrim(key, -max_store, -1)
        
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

def get_plugin_enabled(plugin_name):
    # Try to get the state from Redis; default to False (disabled) if not set.
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    if enabled is None:
        return False
    return enabled.lower() == "true"

def set_plugin_enabled(plugin_name, enabled):
    redis_client.hset("plugin_enabled", plugin_name, "true" if enabled else "false")

def render_plugin_controls(plugin_name):
    current_state = get_plugin_enabled(plugin_name)
    toggle_state = st.toggle(plugin_name, value=current_state, key=f"plugin_toggle_{plugin_name}")

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
        _, stop_flag = _start_platform(key)
        stop_flag.set()
        _start_platform.clear()
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
def get_plugin_settings(category):
    key = f"plugin_settings:{category}"
    return redis_client.hgetall(key)

def save_plugin_settings(category, settings_dict):
    key = f"plugin_settings:{category}"
    # Ensure all values are converted to strings before saving to Redis
    str_settings = {k: str(v) for k, v in settings_dict.items()}
    redis_client.hset(key, mapping=str_settings)

# Updated: Gather distinct plugin settings categories from the plugin registry,
# only including plugins that are enabled.
plugin_categories = {}

for plugin in plugin_registry.values():
    if not get_plugin_enabled(plugin.name):
        continue

    cat = getattr(plugin, "settings_category", None)
    settings = getattr(plugin, "required_settings", None)

    if not cat or not settings:
        continue  # Skip plugins with no settings UI

    if cat not in plugin_categories:
        plugin_categories[cat] = {}

    plugin_categories[cat].update(settings)

# Display an expander for each plugin settings category.
for category, settings in sorted(plugin_categories.items()):
    with st.sidebar.expander(category, expanded=False):
        st.subheader(f"{category} Settings")
        current_settings = get_plugin_settings(category)
        new_settings = {}

        for key, info in settings.items():
            input_type    = info.get("type", "text")
            default_value = current_settings.get(key, info.get("default", ""))

            # --- BUTTONS go first ---
            if input_type == "button":
                if st.button(info["label"], key=f"{category}_{key}_button"):
                    plugin_obj = next(
                        (p for p in plugin_registry.values()
                         if getattr(p, "settings_category", "") == category),
                        None
                    )
                    if plugin_obj and hasattr(plugin_obj, "handle_setting_button"):
                        result = plugin_obj.handle_setting_button(key)
                        if result:
                            st.success(result)
                if info.get("description"):
                    st.caption(info["description"])
                continue  # skip saving a value for buttons

            # PASSWORD
            if input_type == "password":
                new_value = st.text_input(
                    info.get("label", key),
                    value=default_value,
                    help=info.get("description", ""),
                    type="password",
                    key=f"{category}_{key}"
                )

            # FILE
            elif input_type == "file":
                uploaded_file = st.file_uploader(
                    info.get("label", key),
                    type=["json"],
                    key=f"{category}_{key}"
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

            # SELECT
            elif input_type == "select":
                options = info.get("options", []) or ["Option 1", "Option 2"]
                current_index = options.index(default_value) if default_value in options else 0
                new_value = st.selectbox(
                    info.get("label", key),
                    options,
                    index=current_index,
                    help=info.get("description", ""),
                    key=f"{category}_{key}"
                )

            # CHECKBOX
            elif input_type == "checkbox":
                is_checked = (
                    default_value if isinstance(default_value, bool)
                    else str(default_value).lower() in ("true", "1", "yes")
                )
                new_value = st.checkbox(
                    info.get("label", key),
                    value=is_checked,
                    help=info.get("description", ""),
                    key=f"{category}_{key}"
                )

            # TEXT (fallback)
            else:
                new_value = st.text_input(
                    info.get("label", key),
                    value=default_value,
                    help=info.get("description", ""),
                    key=f"{category}_{key}"  # 🔹 unique per plugin+setting
                )

            new_settings[key] = new_value

        if st.button(f"Save {category} Settings", key=f"save_{category}_unique"):
            save_plugin_settings(category, new_settings)
            st.success(f"{category} settings saved.")

# ----------------- SYSTEM PROMPT -----------------
def build_system_prompt():
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")

    first, last = get_tater_name()
    base_prompt = (
        f"You are {first} {last}, an AI assistant with access to various tools and plugins.\n\n"
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
        "Never call a tool in response to casual or friendly messages like 'thanks', 'lol', or 'cool'\n"
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
      - image  -> {"role": role, "content": [{"type":"image"}]}
      - audio  -> {"role": role, "content": [{"type":"text","text":"[Audio]"}]}
      - plugin_call -> stringify call as assistant text
      - plugin_response -> skip (return None)
    """
    # Skip plugin responses from LLM context
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        return None

    # Represent plugin calls as plain text (so history still makes sense)
    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps({
            "function": content.get("plugin"),
            "arguments": content.get("arguments", {})
        }, indent=2)
        return {"role": "assistant", "content": as_text} if role == "assistant" else {"role": role, "content": as_text}

    # Media types
    if isinstance(content, dict) and content.get("type") == "image":
        return {"role": role, "content": "[Image attached]"}  # instead of [{"type":"image"}]

    if isinstance(content, dict) and content.get("type") == "audio":
        return {"role": role, "content": "[Audio attached]"}  # you already do similar

    # Strings and other fallbacks
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
    history = load_chat_history()[-max_llm:]

    # Start with system (your template handles an optional system at index 0)
    messages_list = [{"role": "system", "content": final_system_prompt}]

    # Convert persisted history into template-ready messages
    loop_messages = []
    for msg in history:
        role = msg["role"]
        content = msg["content"]

        # Normalize assistant/user roles only; ignore others if they ever appear
        if role not in ("user", "assistant"):
            # Convert unknown roles to 'assistant' text for safety
            role = "assistant"

        templ_msg = _to_template_msg(role, content)
        if templ_msg is not None:
            loop_messages.append(templ_msg)

    # Merge consecutive same-role messages and ensure we start with 'user'
    loop_messages = _enforce_user_assistant_alternation(loop_messages)

    # Compose final list sent to the LLM
    messages_list.extend(loop_messages)

    # Call the (template-aware) client — this will hit /v1/completions when the jinja file exists
    response = await llm_client.chat(messages_list)
    return response["message"]["content"].strip()

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

    def job_runner():
        try:
            plugin = plugin_registry[plugin_name]
            result = asyncio.run(plugin.handle_webui(args, llm_client))
            responses = result if isinstance(result, list) else [result]
            redis_client.hset(redis_key, mapping={
                "status": "done",
                "is_running": "0",
                "is_done": "1",
                "responses": json.dumps(responses)
            })
        except Exception as e:
            redis_client.hset(redis_key, mapping={
                "status": "error",
                "is_running": "0",
                "is_done": "0",
                "error": str(e)
            })
        finally:
            # one-shot refresh signal (any job finishing should refresh)
            redis_client.set("webui:needs_rerun", "true")

    threading.Thread(target=job_runner, daemon=True).start()
    return job_id

async def process_function_call(response_json, user_question=""):
    func = response_json.get("function")
    args = response_json.get("arguments", {})
    from plugin_registry import plugin_registry

    if func in plugin_registry:
        # Save structured plugin_call marker
        save_message("assistant", "assistant", {
            "marker": "plugin_call",
            "plugin": func,
            "arguments": args
        })

        plugin = plugin_registry[func]
        if hasattr(plugin, "waiting_prompt_template"):
            wait_msg = plugin.waiting_prompt_template.format(mention="User")

            # Ask LLM for a natural "waiting" message
            wait_response = await llm_client.chat(
                messages=[
                    {"role": "system", "content": "Write one short, friendly status line for the user."},
                    {"role": "user", "content": wait_msg}
                ]
            )
            wait_text = wait_response["message"]["content"].strip()

            # Save waiting message to Redis
            save_message("assistant", "assistant", {
                "marker": "plugin_response",
                "content": wait_text
            })

            # Append waiting message to session state for persistence
            st.session_state.chat_messages.append({
                "role": "assistant",
                "content": {
                    "marker": "plugin_response",
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

    else:
        return "Received an unknown function call."

# ------------------ SIDEBAR EXPANDERS ------------------
st.sidebar.markdown("---")

with st.sidebar.expander("Plugin Settings", expanded=False):
    st.subheader("Enable/Disable Plugins")
    for plugin_name in sorted(plugin_registry.keys(), key=lambda n: n.lower()):
        render_plugin_controls(plugin_name)

for platform in sorted(platform_registry, key=lambda p: p["category"].lower()):
    label = platform["label"]
    with st.sidebar.expander(label, expanded=False):
        render_platform_controls(platform, redis_client)

with st.sidebar.expander("WebUI Settings", expanded=False):
    st.subheader("WebUI Settings")
    current_chat = get_chat_settings()
    username = st.text_input("USERNAME", value=current_chat["username"])

    # safely cast to int — handles strings or floats in Redis
    raw_display = redis_client.get("tater:max_display") or 8
    try:
        display_count = int(float(raw_display))
    except (TypeError, ValueError):
        display_count = 8

    # number input with integer step & format
    new_display = st.number_input(
        "Messages Shown in WebUI",
        min_value=1,
        max_value=500,
        value=display_count,
        step=1,
        format="%d",
        key="webui_display_count"
    )

    uploaded_avatar = st.file_uploader(
        "Upload your avatar", type=["png", "jpg", "jpeg"], key="avatar_uploader"
    )

    if uploaded_avatar is not None:
        avatar_bytes = uploaded_avatar.read()
        avatar_b64 = base64.b64encode(avatar_bytes).decode("utf-8")
        save_chat_settings(username, avatar_b64)
    else:
        save_chat_settings(username)

    if st.button("Save WebUI Settings", key="save_webui_settings"):
        redis_client.set("tater:max_display", new_display)
        st.success("WebUI settings updated.")

    if st.button("Clear Chat History", key="clear_history"):
        clear_chat_history()
        st.success("Chat history cleared.")

    if st.button("Clear Active Plugin Jobs", key="clear_plugin_jobs"):
        for key in redis_client.scan_iter("webui:plugin_jobs:*"):
            redis_client.delete(key)
        st.success("All active plugin jobs have been cleared.")
        st.rerun()

with st.sidebar.expander(f"{first_name} Settings", expanded=False):
    st.subheader("Tater Runtime Configuration")
    stored_count = int(redis_client.get("tater:max_store") or 20)
    llm_count = int(redis_client.get("tater:max_llm") or 8)
    default_first = redis_client.get("tater:first_name") or first_name
    default_last = redis_client.get("tater:last_name") or last_name

    first_input = st.text_input("First Name", value=default_first)
    last_input = st.text_input("Last Name", value=default_last)
    uploaded_tater_avatar = st.file_uploader(
        f"Upload {first_input}'s avatar", type=["png", "jpg", "jpeg"], key="tater_avatar_uploader"
    )
    if uploaded_tater_avatar is not None:
        avatar_bytes = uploaded_tater_avatar.read()
        avatar_b64 = base64.b64encode(avatar_bytes).decode("utf-8")
        redis_client.set("tater:avatar", avatar_b64)

    new_store = st.number_input("Max Stored Messages (0 = unlimited)", min_value=0, value=stored_count)
    if new_store == 0:
        st.warning("⚠️ Unlimited history enabled — this may grow Redis memory usage over time.")
    new_llm = st.number_input("Messages Sent to LLM", min_value=1, value=llm_count)
    if new_store > 0 and new_llm > new_store:
        st.warning("⚠️ You're trying to send more messages to LLM than you’re storing. Consider increasing Max Stored Messages.")

    if st.button("Save Tater Settings"):
        redis_client.set("tater:max_store", new_store)
        redis_client.set("tater:max_llm", new_llm)
        redis_client.set("tater:first_name", first_input)
        redis_client.set("tater:last_name", last_input)
        st.success("Tater settings updated.")
        st.rerun()

# ------------------ RSS ------------------
_start_rss(llm_client)

# ------------------ PLATFORM MANAGEMENT ------------------
for platform in platform_registry:
    key = platform["key"]  # e.g. irc_platform
    state_key = f"{key}_running"

    # Check Redis to determine if this platform should be running
    platform_should_run = redis_client.get(state_key) == "true"

    if platform_should_run:
        _start_platform(key)
        st.success(f"{platform['category']} auto-connected.")

# ------------------ Chat ------------------
st.title(f"{first_name} Chat Web UI")

chat_settings = get_chat_settings()
avatar_b64  = chat_settings.get("avatar")
user_avatar = load_avatar_image(avatar_b64) if avatar_b64 else None

if "chat_messages" not in st.session_state:
    full_history = load_chat_history()
    max_display = int(redis_client.get("tater:max_display") or 8)
    st.session_state.chat_messages = full_history[-max_display:]

# Check for completed plugin jobs
for key in redis_client.scan_iter("webui:plugin_jobs:*"):
    job = redis_client.hgetall(key)
    if job.get("status") == "done":
        try:
            responses = json.loads(job.get("responses", "[]"))
            for r in responses:
                item = {
                    "role": "assistant",
                    "content": {
                        "marker": "plugin_response",
                        "content": r
                    }
                }
                st.session_state.chat_messages.append(item)
                save_message("assistant", "assistant", item["content"])
        finally:
            redis_client.delete(key)

# Render all messages from session state
for msg in st.session_state.chat_messages:
    role = msg["role"]
    avatar = user_avatar if role == "user" else assistant_avatar
    content = msg["content"]

    # 🔧 Robust unwrap of nested plugin_response wrappers first
    while isinstance(content, dict) and content.get("marker") == "plugin_response":
        content = content.get("content")

    # 🔧 After unwrap, check for plugin_call marker and skip render if needed
    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        continue

    with st.chat_message(role, avatar=avatar):
        if isinstance(content, dict) and content.get("type") == "image":
            if content.get("mimetype") == "image/webp":
                st.markdown(
                    f'''
                    <img src="data:image/webp;base64,{content["data"]}"
                         alt="{content.get("name", "")}"
                         style="max-width: 100%; border-radius: 0.5rem;" autoplay loop>
                    ''',
                    unsafe_allow_html=True
                )
            else:
                data = base64.b64decode(content["data"])
                st.image(Image.open(BytesIO(data)), caption=content.get("name", ""))

        elif isinstance(content, dict) and content.get("type") == "audio":
            data = base64.b64decode(content["data"])
            st.audio(data, format=content.get("mimetype", "audio/mpeg"))

        elif isinstance(content, dict) and content.get("type") == "video":
            data = base64.b64decode(content["data"])
            st.video(data, format=content.get("mimetype", "video/mp4"))

        else:
            st.write(content)

if user_input := st.chat_input(f"Chat with {first_name}…"):
    uname = chat_settings["username"]

    # Save and append user message right away
    save_message("user", uname, user_input)
    st.session_state.chat_messages.append({
        "role": "user",
        "content": user_input
    })

    # Show user's message immediately for feedback
    st.chat_message("user", avatar=user_avatar or "🦖").write(user_input)

    # Show spinner while thinking
    with st.spinner(f"{first_name} is thinking..."):
        response_text = run_async(process_message(uname, user_input))

    func_call = parse_function_json(response_text)
    if func_call:
        func_result = run_async(process_function_call(func_call, user_input))
        responses = func_result if isinstance(func_result, list) else [func_result]
    else:
        responses = [response_text]

    # Just append assistant responses — no immediate rendering
    for item in responses:
        st.session_state.chat_messages.append({
            "role": "assistant",
            "content": item
        })
        save_message("assistant", "assistant", item)

    # Force rerun, assistant reply will render on next pass
    st.rerun()

# ---------------- Pending jobs spinner with transition-based refresh ----------------
# Gather pending plugin names for the UI label (unchanged)
pending_plugins = []
pending_keys = set()
for key in redis_client.scan_iter("webui:plugin_jobs:*"):
    if redis_client.hget(key, "status") == "pending":
        pending_keys.add(key)
        job = redis_client.hgetall(key)
        plugin_name = job.get("plugin")
        if plugin_name in plugin_registry:
            plugin = plugin_registry[plugin_name]
            display_name = getattr(plugin, "pretty_name", plugin.name)
            pending_plugins.append(display_name)

# If any are pending, show spinner and poll for either
if pending_plugins:
    names_str = ", ".join(pending_plugins)
    with st.spinner(f"{first_name} is working on: {names_str}"):
        previous_pending = set(pending_keys)
        while True:
            transition_detected = False
            current_pending = set()

            for key in redis_client.scan_iter("webui:plugin_jobs:*"):
                status = redis_client.hget(key, "status")

                if status == "pending":
                    current_pending.add(key)
                else:
                    if key in previous_pending:
                        transition_detected = True

            # safety: handle vanished keys too
            for key in list(previous_pending):
                if not redis_client.exists(key):
                    transition_detected = True

            if transition_detected:
                break
            if not current_pending:
                break

            previous_pending = current_pending
            time.sleep(1)

        st.rerun()