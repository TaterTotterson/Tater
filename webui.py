# webui.py
import streamlit as st
import redis
import os
import time
import json
import re
import asyncio
import dotenv
import feedparser
import logging
import base64
import requests
from datetime import datetime
from PIL import Image
from io import BytesIO
from plugin_registry import plugin_registry
from helpers import send_waiting_message, OllamaClientWrapper, load_image_from_url, run_async, set_main_loop
from discord_control import connect_discord, disconnect_discord
from rss import RSSManager
import threading

dotenv.load_dotenv()

if "discord_bot_started" not in st.session_state:
    st.session_state["discord_bot_started"] = False

# Redis configuration for the web UI (using a separate DB)
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# Setup Ollama client for web UI using our wrapper.
ollama_host = os.getenv('OLLAMA_HOST', '127.0.0.1')
ollama_port = int(os.getenv('OLLAMA_PORT', 11434))
ollama_client = OllamaClientWrapper(host=f'http://{ollama_host}:{ollama_port}')

# Set the main event loop used for run_async.
main_loop = asyncio.get_event_loop()
set_main_loop(main_loop)

CHAT_HISTORY_KEY = "webui:chat_history"

st.set_page_config(
    page_title="Tater Chat",
    page_icon=":material/tooltip_2:"  # can be a URL or a local file path
)

def load_chat_history():
    history = redis_client.lrange(CHAT_HISTORY_KEY, 0, -1)
    return [json.loads(msg) for msg in history]

def save_message(role, username, content):
    message_data = {"role": role, "content": content, "username": username}
    redis_client.rpush(CHAT_HISTORY_KEY, json.dumps(message_data))
    redis_client.ltrim(CHAT_HISTORY_KEY, -20, -1)

def clear_chat_history():
    redis_client.delete(CHAT_HISTORY_KEY)

def extract_json(text):
    match = re.search(r'(\{.*\})', text, re.DOTALL)
    if match:
        return match.group(1)
    return None

assistant_avatar = load_image_from_url()

# ----------------- SETTINGS HELPER FUNCTIONS -----------------
def get_discord_settings():
    settings = redis_client.hgetall("discord_settings")
    return {
        "discord_token": settings.get("discord_token", "0"),
        "admin_user_id": settings.get("admin_user_id", "0"),
        "response_channel_id": settings.get("response_channel_id", "0")
    }

def save_discord_settings(discord_token, admin_user_id, response_channel_id, username):
    redis_client.hset("discord_settings", mapping={
        "discord_token": discord_token,
        "admin_user_id": admin_user_id,
        "response_channel_id": response_channel_id,
        "username": username
    })

def get_discord_connection_state():
    state = redis_client.get("discord_connected")
    if state is None:
        return "disconnected"
    return state

def set_discord_connection_state(state):
    redis_client.set("discord_connected", state)

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
    except Exception as e:
        return None

def get_plugin_enabled(plugin_name):
    # Try to get the state from Redis; default to False (disabled) if not set.
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    if enabled is None:
        return False
    return enabled.lower() == "true"

def set_plugin_enabled(plugin_name, enabled):
    redis_client.hset("plugin_enabled", plugin_name, "true" if enabled else "false")

# ----------------- PLUGIN SETTINGS -----------------
def get_plugin_settings(category):
    key = f"plugin_settings:{category}"
    return redis_client.hgetall(key)

def save_plugin_settings(category, settings_dict):
    key = f"plugin_settings:{category}"
    redis_client.hset(key, mapping=settings_dict)

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
for category, settings in plugin_categories.items():
    with st.sidebar.expander(category, expanded=False):
        st.subheader(f"{category} Settings")
        current_settings = get_plugin_settings(category)
        new_settings = {}
        for key, info in settings.items():
            default_value = current_settings.get(key, info.get("default", ""))
            input_type = info.get("type", "text")  # default to text if not specified
            if input_type == "password":
                new_value = st.text_input(info.get("label", key), value=default_value, help=info.get("description", ""), type="password")
            elif input_type == "file":
                uploaded_file = st.file_uploader(info.get("label", key), type=["json"], key=f"{category}_{key}")
                if uploaded_file is not None:
                    try:
                        file_content = uploaded_file.read().decode("utf-8")
                        json.loads(file_content)  # Validate the JSON
                        new_value = file_content
                    except Exception as e:
                        st.error(f"Error in uploaded file for {key}: {e}")
                        new_value = default_value
                else:
                    new_value = default_value
            else:
                new_value = st.text_input(info.get("label", key), value=default_value, help=info.get("description", ""))
            new_settings[key] = new_value
        if st.button(f"Save {category} Settings", key=f"save_{category}"):
            save_plugin_settings(category, new_settings)
            st.success(f"{category} settings saved.")

# ----------------- SYSTEM PROMPT -----------------
def build_system_prompt(base_prompt):
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    # We include the full string directly here
    tool_instructions = "\n\n".join(
        f"Tool: {plugin.name}\n"
        f"Description: {getattr(plugin, 'description', 'No description provided.')}\n"
        f"{plugin.usage}"
        for plugin in plugin_registry.values()
        if ("webui" in plugin.platforms or "both" in plugin.platforms) and get_plugin_enabled(plugin.name)
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"{base_prompt}\n\n"
        f"{tool_instructions}\n\n"
        "If no function is needed, reply normally."
    )

BASE_PROMPT = (
    "You are Tater Totterson, a helpful AI assistant with access to various tools and plugins.\n\n"
    "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else).:\n\n"
)
SYSTEM_PROMPT = build_system_prompt(BASE_PROMPT)

# ----------------- PROCESSING FUNCTIONS -----------------
async def process_message(user_name, message_content):
    final_system_prompt = SYSTEM_PROMPT
    history = load_chat_history()[-20:]
    messages_list = [{"role": "system", "content": final_system_prompt}]
    for msg in history:
        if msg["role"] == "user":
            formatted = f"{msg['username']}: {msg['content']}"
        else:
            formatted = msg["content"]
        messages_list.append({"role": msg["role"], "content": formatted})
    
    response = await ollama_client.chat(
        model=ollama_client.model,
        messages=messages_list,
        stream=False,
        keep_alive=-1,
        options={"num_ctx": ollama_client.context_length}
    )
    response_text = response['message'].get('content', '').strip()
    return response_text

async def process_function_call(response_json, user_question=""):
    func = response_json.get("function")
    args = response_json.get("arguments", {})
    chat_settings = get_chat_settings()
    username = chat_settings["username"]
    from plugin_registry import plugin_registry
    if func in plugin_registry:
        plugin = plugin_registry[func]
        result = await plugin.handle_webui(args, ollama_client, ollama_client.context_length)
        return result
    else:
        return "Received an unknown function call."

# ------------------ SIDEBAR EXPANDERS ------------------
with st.sidebar.expander("Plugin Settings", expanded=False):
    st.subheader("Enable/Disable Plugins")
    # Iterate over plugins in the registry and create a toggle for each.
    for plugin_name in plugin_registry.keys():
        current_state = get_plugin_enabled(plugin_name)
        toggle_state = st.toggle(plugin_name, value=current_state, key=f"plugin_toggle_{plugin_name}")
        # Save the state back to redis whenever it is toggled.
        set_plugin_enabled(plugin_name, toggle_state)

with st.sidebar.expander("Discord Settings", expanded=False):
    st.subheader("Discord Bot Settings")
    current_settings = get_discord_settings()
    discord_token = st.text_input("DISCORD_TOKEN", value=current_settings["discord_token"], type="password")
    admin_user_id_str = st.text_input("ADMIN_USER_ID", value=current_settings["admin_user_id"])
    response_channel_id_str = st.text_input("RESPONSE_CHANNEL_ID", value=current_settings["response_channel_id"])
    
    try:
        admin_user_id = int(admin_user_id_str)
    except ValueError:
        admin_user_id = 0
    try:
        response_channel_id = int(response_channel_id_str)
    except ValueError:
        response_channel_id = 0
    
    save_discord_settings(discord_token, admin_user_id, response_channel_id, current_settings.get("username", "User"))
    
    current_state = get_discord_connection_state()
    if "discord_connected" not in st.session_state:
        st.session_state["discord_connected"] = get_discord_connection_state() == "connected"

    if st.session_state["discord_connected"] and not st.session_state["discord_bot_started"]:
        connect_discord(discord_token, admin_user_id, response_channel_id)
        st.session_state["discord_bot_started"] = True
        st.success("Discord bot auto‑connected.")

    discord_connected = st.toggle("Discord Connection", value=st.session_state["discord_connected"], key="discord_toggle")
    if discord_connected and not st.session_state["discord_connected"]:
        if not st.session_state["discord_bot_started"]:
            connect_discord(discord_token, admin_user_id, response_channel_id)
            st.session_state["discord_bot_started"] = True
            st.success("Discord bot connected.")
        else:
            st.warning("Discord bot is already running.")
        st.session_state["discord_connected"] = True
        set_discord_connection_state("connected")
    elif not discord_connected and st.session_state["discord_connected"]:
        disconnect_discord()
        st.session_state["discord_bot_started"] = False
        st.session_state["discord_connected"] = False
        set_discord_connection_state("disconnected")
        st.success("Discord bot disconnected.")

with st.sidebar.expander("Chat Settings", expanded=False):
    st.subheader("User Settings")
    current_chat = get_chat_settings()
    username = st.text_input("USERNAME", value=current_chat["username"])
    uploaded_avatar = st.file_uploader("Upload your avatar", type=["png", "jpg", "jpeg"], key="avatar_uploader")
    if uploaded_avatar is not None:
        avatar_bytes = uploaded_avatar.read()
        avatar_b64 = base64.b64encode(avatar_bytes).decode("utf-8")
        save_chat_settings(username, avatar_b64)
    else:
        save_chat_settings(username)
    if st.button("Clear Chat History", key="clear_history"):
        clear_chat_history()
        st.success("Chat history cleared.")

if "uploader_key" not in st.session_state:
    st.session_state["uploader_key"] = f"sidebar_uploader_{int(time.time())}"
uploaded_files = st.sidebar.file_uploader(
    "Attach torrent or image", 
    accept_multiple_files=True, 
    key=st.session_state["uploader_key"]
)

if "rss_started" not in st.session_state:
    st.session_state["rss_started"] = True

    def start_rss_background():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        rss_manager = RSSManager(ollama_client=ollama_client)
        loop.run_until_complete(rss_manager.poll_feeds())

    threading.Thread(target=start_rss_background, daemon=True).start()

st.title("Tater Chat Web UI")

chat_settings = get_chat_settings()
user_avatar = None
if chat_settings.get("avatar"):
    user_avatar = load_avatar_image(chat_settings["avatar"])

for msg in load_chat_history():
    if msg["role"] == "user":
        st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").write(msg["content"])
    else:
        st.chat_message("assistant", avatar=assistant_avatar).write(msg["content"])

user_input = st.chat_input("Chat with Tater...")

if uploaded_files:
    allowed_extensions = [".torrent", ".png", ".jpg", ".jpeg", ".gif"]
    # Check for disallowed attachments.
    for uploaded_file in uploaded_files:
        if not any(uploaded_file.name.lower().endswith(ext) for ext in allowed_extensions):
            error_response = run_async(
                ollama_client.chat(
                    model=ollama_client.model,
                    messages=[{
                        "role": "system",
                        "content": "Tell the user, Only torrent files and image files are allowed. Only tell the user do not respond to this message"
                    }],
                    stream=False,
                    keep_alive=-1,
                    options={"num_ctx": ollama_client.context_length}
                )
            )
            st.stop()  # Stop further processing.
    
    # If we reach here, all files are allowed.
    torrent_file = None
    image_file = None
    for uploaded_file in uploaded_files:
        lower_name = uploaded_file.name.lower()
        if lower_name.endswith(".torrent"):
            torrent_file = uploaded_file
            break  # Prioritize torrent files if present.
        elif lower_name.endswith((".png", ".jpg", ".jpeg", ".gif")):
            image_file = uploaded_file

    if torrent_file:
        st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").write(f"[Torrent attachment: {torrent_file.name}]")
        save_message("user", chat_settings["username"], f"[Torrent attachment: {torrent_file.name}]")
        
        run_async(
            send_waiting_message(
                ollama_client=ollama_client,
                prompt_text="Please wait while I check if the torrent file is cached. Only generate the waiting message.",
                save_callback=lambda text: save_message("assistant", "assistant", text),
                send_callback=lambda text: st.chat_message("assistant", avatar=assistant_avatar).write(text)
            )
        )
        
        premiumize_plugin = plugin_registry.get("premiumize_torrent")
        if premiumize_plugin:
            torrent_result = run_async(premiumize_plugin.process_torrent_web(torrent_file.read(), torrent_file.name))
        else:
            torrent_result = "Error: premiumize plugin not available."
        
        if "uploader_key" in st.session_state:
            st.session_state.pop("uploader_key")
        save_message("assistant", "assistant", torrent_result)
        st.chat_message("assistant", avatar=assistant_avatar).write(torrent_result)
    
    elif image_file:
        st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").write(f"[Image attachment: {image_file.name}]")
        save_message("user", chat_settings["username"], f"[Image attachment: {image_file.name}]")
        
        run_async(
            send_waiting_message(
                ollama_client=ollama_client,
                prompt_text="Please wait while I analyze the image and generate a description. Only generate the waiting message.",
                save_callback=lambda text: save_message("assistant", "assistant", text),
                send_callback=lambda text: st.chat_message("assistant", avatar=assistant_avatar).write(text)
            )
        )
        
        vision_plugin = plugin_registry.get("vision_describer")
        if vision_plugin:
            image_result = run_async(vision_plugin.process_image_web(image_file.read(), image_file.name))
        else:
            image_result = "Error: vision plugin not available."
        
        if "uploader_key" in st.session_state:
            st.session_state.pop("uploader_key")
        save_message("assistant", "assistant", image_result)
        st.chat_message("assistant", avatar=assistant_avatar).write(image_result)

elif user_input:
    chat_settings = get_chat_settings()
    save_message("user", chat_settings["username"], user_input)
    st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").write(user_input)
    
    response_text = run_async(process_message(chat_settings["username"], user_input))
    
    response_json = None
    try:
        response_json = json.loads(response_text)
    except json.JSONDecodeError:
        json_str = extract_json(response_text)
        if json_str:
            try:
                response_json = json.loads(json_str)
            except Exception:
                response_json = None

    if response_json and isinstance(response_json, dict) and "function" in response_json:
        func_response = run_async(process_function_call(response_json, user_question=user_input))
        if func_response:
            response_text = func_response

    save_message("assistant", "assistant", response_text)
    st.chat_message("assistant", avatar=assistant_avatar).write(response_text)