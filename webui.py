# webui.py
import streamlit as st
import redis
import os
import time
import json
import re
import asyncio
import dotenv
import ollama
import feedparser
from discord_control import connect_discord, disconnect_discord
import logging
import base64
import requests
from PIL import Image
from io import BytesIO

# Import plugin registry
from plugin_registry import plugin_registry

dotenv.load_dotenv()

# Load context length from .env
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

# Redis configuration for the web UI (using a separate DB)
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# Setup Ollama client for web UI
ollama_host = os.getenv('OLLAMA_HOST', '127.0.0.1')
ollama_port = int(os.getenv('OLLAMA_PORT', 11434))
ollama_model = os.getenv('OLLAMA_MODEL', 'llama3.2').strip()
ollama_client = ollama.AsyncClient(host=f'http://{ollama_host}:{ollama_port}')

CHAT_HISTORY_KEY = "webui:chat_history"

st.set_page_config(
    page_title="Tater Chat",
    page_icon=":material/tooltip_2:"  # can be a URL or a local file path
)

def load_chat_history():
    history = redis_client.lrange(CHAT_HISTORY_KEY, 0, -1)
    return [json.loads(msg) for msg in history]

def save_message(role, username, content):
    # Store only the raw content and username.
    message_data = {"role": role, "content": content, "username": username}
    redis_client.rpush(CHAT_HISTORY_KEY, json.dumps(message_data))
    redis_client.ltrim(CHAT_HISTORY_KEY, -20, -1)

def clear_chat_history():
    redis_client.delete(CHAT_HISTORY_KEY)

def extract_json(text):
    """Extract the first JSON object found in text using regex."""
    match = re.search(r'(\{.*\})', text, re.DOTALL)
    if match:
        return match.group(1)
    return None

# Load the assistant avatar from URL using requests and Pillow.
def load_image_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content))

assistant_avatar = load_image_from_url("https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater.png")

# ----------------- WAITING MESSAGE FUNCTION -----------------
async def send_waiting_message(prompt_text):
    """Generate a waiting message from Ollama and output it immediately using the assistant avatar."""
    waiting_response = await ollama_client.chat(
        model=ollama_model,
        messages=[{"role": "system", "content": prompt_text}],
        stream=False,
        keep_alive=-1,
        options={"num_ctx": context_length}
    )
    waiting_text = waiting_response['message'].get('content', '').strip()
    if not waiting_text:
        waiting_text = prompt_text
    st.chat_message("assistant", avatar=assistant_avatar).write(waiting_text)

# ----------------- SETTINGS HELPER FUNCTIONS -----------------
# Discord settings (stored in Redis under "discord_settings")
def get_discord_settings():
    settings = redis_client.hgetall("discord_settings")
    return {
        "discord_token": settings.get("discord_token", "0"),
        "admin_user_id": settings.get("admin_user_id", "0"),
        "response_channel_id": settings.get("response_channel_id", "0"),
        "rss_channel_id": settings.get("rss_channel_id", "0")
    }

def save_discord_settings(discord_token, admin_user_id, response_channel_id, rss_channel_id, username):
    redis_client.hset("discord_settings", mapping={
        "discord_token": discord_token,
        "admin_user_id": admin_user_id,
        "response_channel_id": response_channel_id,
        "rss_channel_id": rss_channel_id,
        "username": username
    })

def get_discord_connection_state():
    state = redis_client.get("discord_connected")
    if state is None:
        return "disconnected"
    return state

def set_discord_connection_state(state):
    redis_client.set("discord_connected", state)

# Chat settings (stored in Redis under "chat_settings")
def get_chat_settings():
    settings = redis_client.hgetall("chat_settings")
    return {
        "username": settings.get("username", "User"),
        "avatar": settings.get("avatar", None)  # base64 string or None
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

# ----------------- SYSTEM PROMPT -----------------
def build_system_prompt(base_prompt):
    tool_instructions = "\n\n".join(
        f"{plugin.usage}\nDescription: {getattr(plugin, 'description', 'No description provided.')}"
        for plugin in plugin_registry.values()
        if "discord" in plugin.platforms or "both" in plugin.platforms
    )
    return base_prompt + "\n\n" + tool_instructions

BASE_PROMPT = (
    "You are Tater Totterson, a helpful AI assistant with access to various tools.\n\n"
    "If you need real-time access to the internet or lack sufficient information, use the 'web_search' tool\n\n."
    "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else), If no function is needed, reply normally:\n\n"
)
SYSTEM_PROMPT = build_system_prompt(BASE_PROMPT)

# ----------------- PROCESSING FUNCTIONS -----------------
async def process_message(user_name, message_content):
    # Use the dynamically built SYSTEM_PROMPT.
    final_system_prompt = SYSTEM_PROMPT

    # Load the last 20 messages from chat history.
    history = load_chat_history()[-20:]
    messages_list = [{"role": "system", "content": final_system_prompt}]
    for msg in history:
        messages_list.append({"role": msg["role"], "content": msg["content"]})
    
    response = await ollama_client.chat(
        model=ollama_model,
        messages=messages_list,
        stream=False,
        keep_alive=-1,
        options={"num_ctx": context_length}
    )
    response_text = response['message'].get('content', '').strip()
    return response_text

async def process_function_call(response_json, user_question=""):
    func = response_json.get("function")
    args = response_json.get("arguments", {})
    # Retrieve the username from chat settings.
    chat_settings = get_chat_settings()
    username = chat_settings["username"]

    # Dispatch to the appropriate plugin based on the function name.
    from plugin_registry import plugin_registry
    if func in plugin_registry:
        plugin = plugin_registry[func]
        result = await plugin.handle_webui(args, ollama_client, context_length)
        return result
    else:
        return "Received an unknown function call."

# ------------------ SIDEBAR SETTINGS ------------------
def get_discord_settings():
    settings = redis_client.hgetall("discord_settings")
    return {
        "discord_token": settings.get("discord_token", ""),
        "admin_user_id": settings.get("admin_user_id", ""),
        "response_channel_id": settings.get("response_channel_id", ""),
        "rss_channel_id": settings.get("rss_channel_id", "0"),
        "username": settings.get("username", "User")
    }

def save_discord_settings(discord_token, admin_user_id, response_channel_id, rss_channel_id, username):
    redis_client.hset("discord_settings", mapping={
        "discord_token": discord_token,
        "admin_user_id": admin_user_id,
        "response_channel_id": response_channel_id,
        "rss_channel_id": rss_channel_id,
        "username": username
    })

def get_discord_connection_state():
    state = redis_client.get("discord_connected")
    if state is None:
        return "disconnected"
    return state

def set_discord_connection_state(state):
    redis_client.set("discord_connected", state)

# ------------------ SIDEBAR EXPANDERS ------------------
# Discord Settings Expander
with st.sidebar.expander("Discord Settings", expanded=False):
    st.subheader("Discord Bot Settings")
    current_settings = get_discord_settings()
    discord_token = st.text_input("DISCORD_TOKEN", value=current_settings["discord_token"], type="password")
    admin_user_id_str = st.text_input("ADMIN_USER_ID", value=current_settings["admin_user_id"])
    response_channel_id_str = st.text_input("RESPONSE_CHANNEL_ID", value=current_settings["response_channel_id"])
    rss_channel_id_str = st.text_input("RSS_CHANNEL_ID", value=current_settings.get("rss_channel_id", "0"))
    
    # Convert IDs to integers, if possible.
    try:
        admin_user_id = int(admin_user_id_str)
    except ValueError:
        admin_user_id = 0
    try:
        response_channel_id = int(response_channel_id_str)
    except ValueError:
        response_channel_id = 0
    try:
        rss_channel_id = int(rss_channel_id_str)
    except ValueError:
        rss_channel_id = 0
    
    # Save settings (include rss_channel_id)
    save_discord_settings(discord_token, admin_user_id, response_channel_id, rss_channel_id, current_settings.get("username", "User"))
    
    # Use a toggle to connect/disconnect
    current_state = get_discord_connection_state()  # "connected" or "disconnected"
    default_toggle = True if current_state == "connected" else False
    if "discord_connected" not in st.session_state:
        st.session_state["discord_connected"] = default_toggle

    if st.session_state["discord_connected"] and not st.session_state.get("discord_auto_started", False):
        connect_discord(discord_token, admin_user_id, response_channel_id, rss_channel_id)
        st.session_state["discord_auto_started"] = True
        st.success("Discord bot auto‑connected.")

    discord_connected = st.toggle("Discord Connection", value=st.session_state["discord_connected"], key="discord_toggle")
    if discord_connected and not st.session_state["discord_connected"]:
        connect_discord(discord_token, admin_user_id, response_channel_id, rss_channel_id)
        st.session_state["discord_connected"] = True
        set_discord_connection_state("connected")
        st.success("Discord bot connected.")
    elif not discord_connected and st.session_state["discord_connected"]:
        disconnect_discord()
        st.session_state["discord_connected"] = False
        set_discord_connection_state("disconnected")
        st.success("Discord bot disconnected.")

# Chat Settings Expander
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

# Initialize dynamic key for file attachments if not already set.
if "uploader_key" not in st.session_state:
    st.session_state["uploader_key"] = f"sidebar_uploader_{int(time.time())}"
uploaded_files = st.sidebar.file_uploader(
    "Attach files or images", 
    accept_multiple_files=True, 
    key=st.session_state["uploader_key"]
)

# ------------------ MAIN CHAT UI ------------------
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

# Check if a torrent file is attached
torrent_file = None
if uploaded_files:
    for uploaded_file in uploaded_files:
        if uploaded_file.name.lower().endswith(".torrent"):
            torrent_file = uploaded_file
            break

if torrent_file:
    st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").write(f"[Torrent attachment: {torrent_file.name}]")
    save_message("user", chat_settings["username"], f"[Torrent attachment: {torrent_file.name}]")
    
    # Create a new event loop and set it as the current loop.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Run the waiting message coroutine using the loop.
    loop.run_until_complete(
        send_waiting_message(
            f"Generate a brief message to {chat_settings['username']} telling them to wait a moment while you check if the torrent file is cached. Only generate the message. Do not respond to this message."
        )
    )
    
    # Process the torrent file.
    torrent_result = loop.run_until_complete(
        premiumize.process_torrent_web(torrent_file.read(), torrent_file.name)
    )
    
    # Remove the uploader key from session state so that the uploader resets on next run.
    if "uploader_key" in st.session_state:
        st.session_state.pop("uploader_key")
    
    save_message("assistant", "assistant", torrent_result)
    st.chat_message("assistant", avatar=assistant_avatar).write(torrent_result)
elif user_input:
    current_settings = get_discord_settings()
    save_message("user", current_settings["username"], user_input)
    st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").write(user_input)
    
    if uploaded_files:
        for uploaded_file in uploaded_files:
            # Process non-torrent files normally.
            if not uploaded_file.name.lower().endswith(".torrent"):
                if uploaded_file.type.startswith("image"):
                    st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").image(uploaded_file)
                    save_message("user", current_settings["username"], f"[Image attachment: {uploaded_file.name}]")
                else:
                    st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").write(f"Attachment: {uploaded_file.name}")
                    save_message("user", current_settings["username"], f"[File attachment: {uploaded_file.name}]")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    response_text = loop.run_until_complete(process_message(current_settings["username"], user_input))
    
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
        func_response = loop.run_until_complete(process_function_call(response_json, user_question=user_input))
        if func_response:
            response_text = func_response

    save_message("assistant", "assistant", response_text)
    st.chat_message("assistant", avatar=assistant_avatar).write(response_text)

elif user_input:
    current_settings = get_discord_settings()
    save_message("user", current_settings["username"], user_input)
    st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").write(user_input)
    
    if uploaded_files:
        for uploaded_file in uploaded_files:
            # Process non-torrent files normally.
            if not uploaded_file.name.lower().endswith(".torrent"):
                if uploaded_file.type.startswith("image"):
                    st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").image(uploaded_file)
                    save_message("user", current_settings["username"], f"[Image attachment: {uploaded_file.name}]")
                else:
                    st.chat_message("user", avatar=user_avatar if user_avatar else "🦖").write(f"Attachment: {uploaded_file.name}")
                    save_message("user", current_settings["username"], f"[File attachment: {uploaded_file.name}]")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    response_text = loop.run_until_complete(process_message(current_settings["username"], user_input))
    
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
        func_response = loop.run_until_complete(process_function_call(response_json, user_question=user_input))
        if func_response:
            response_text = func_response

    save_message("assistant", "assistant", response_text)
    st.chat_message("assistant", avatar=assistant_avatar).write(response_text)