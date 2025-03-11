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
import YouTube
import web      # your existing module for webpage summarization/search
import premiumize  # your existing module for Premiumize functions
import image
import logging
import base64
import requests
from PIL import Image
from io import BytesIO
from search import search_web, format_search_results  # Import search functions

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
    page_icon=":material/tooltip_2:"  # can be a URL or a local file path tooltip_2
)

def load_chat_history():
    history = redis_client.lrange(CHAT_HISTORY_KEY, 0, -1)
    return [json.loads(msg) for msg in history]

def save_message(role, username, content):
    # Store only the raw content and username.
    message_data = {"role": role, "content": content, "username": username}
    redis_client.rpush(CHAT_HISTORY_KEY, json.dumps(message_data))
    # Optionally, you can trim the history here if desired.

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
SYSTEM_PROMPT = (
    "You are Tater Totterson, a helpful AI assistant with access to various tools.\n\n"
    "If you need real-time access to the internet or lack sufficient information, use the 'web_search' tool. \n\n"
    "Use the tools to help users with various tasks. You have access to the following tools:\n\n"
    "1. 'youtube_summary' for summarizing YouTube videos.\n\n"
    "2. 'web_summary' for summarizing webpage text.\n\n"
    "3. 'draw_picture' for generating images.\n\n"
    "4. 'premiumize_download' for retrieving download links from Premiumize.me.\n\n"
    "5. 'premiumize_torrent' for retrieving torrent download links from Premiumize.me.\n\n"
    "6. 'watch_feed' for adding an RSS feed to the watch list, add a rss link to the watch list when aa user asks.\n\n"
    "7. 'unwatch_feed' for removing an RSS feed from the watch list, remove a rss link from the watch list when aa user asks.\n\n"
    "8. 'list_feeds' for listing RSS feeds that are currently on the watch list.\n\n"
    "9. 'web_search' for searching the web when additional or up-to-date information is needed to answer a user's question.\n\n"
    "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else):\n\n"
    "For YouTube videos:\n"
    "{\n"
    '  "function": "youtube_summary",\n'
    '  "arguments": {"video_url": "<YouTube URL>"}\n'
    "}\n\n"
    "For webpages:\n"
    "{\n"
    '  "function": "web_summary",\n'
    '  "arguments": {"url": "<Webpage URL>"}\n'
    "}\n\n"
    "For drawing images:\n"
    "{\n"
    '  "function": "draw_picture",\n'
    '  "arguments": {"prompt": "<Text prompt for the image>"}\n'
    "}\n\n"
    "For Premiumize URL download check:\n"
    "{\n"
    '  "function": "premiumize_download",\n'
    '  "arguments": {"url": "<URL to check>"}\n'
    "}\n\n"
    "For Premiumize torrent check:\n"
    "{\n"
    '  "function": "premiumize_torrent",\n'
    '  "arguments": {}\n'
    "}\n\n"
    "For adding an RSS feed:\n"
    "{\n"
    '  "function": "watch_feed",\n'
    '  "arguments": {"feed_url": "<RSS feed URL>"}\n'
    "}\n\n"
    "For removing an RSS feed:\n"
    "{\n"
    '  "function": "unwatch_feed",\n'
    '  "arguments": {"feed_url": "<RSS feed URL>"}\n'
    "}\n\n"
    "For listing RSS feeds:\n"
    "{\n"
    '  "function": "list_feeds",\n'
    '  "arguments": {}\n'
    "}\n\n"
    "{\n"
    '  "function": "web_search",\n'
    '  "arguments": {"query": "<search query>"}\n'
    "}\n\n"
    "If no function is needed, reply normally."
)

# ----------------- PROCESSING FUNCTIONS -----------------
async def process_message(user_name, message_content):
    # No embedding is generated; use the static system prompt.
    final_system_prompt = SYSTEM_PROMPT

    # Load the last 20 messages from chat history.
    history = load_chat_history()[-20:]
    messages_list = [{"role": "system", "content": final_system_prompt}]
    for msg in history:
        messages_list.append({"role": msg["role"], "content": msg["content"]})
    messages_list.append({"role": "user", "content": message_content})
    
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
    
    if func == "youtube_summary":
        await send_waiting_message(f"Generate a brief message to {username} telling them to wait a moment while you summarize the YouTube video. Only generate the message. Do not respond to this message.")
        video_url = args.get("video_url")
        target_lang = args.get("target_lang", "en")
        if video_url:
            video_id = YouTube.extract_video_id(video_url)
            if video_id:
                article = await asyncio.to_thread(YouTube.fetch_youtube_summary, video_id, target_lang)
                if article:
                    formatted_article = YouTube.format_article_for_discord(article)
                    chunks = YouTube.split_message(formatted_article)
                    final_response = "\n".join(chunks)
                    return final_response
                else:
                    return "Failed to retrieve summary from YouTube."
            else:
                return "Invalid YouTube URL."
        else:
            return "No YouTube URL provided."
    elif func == "web_summary":
        await send_waiting_message(f"Generate a brief message to {username} telling them to wait a moment while you read this boring article for them, and that you will provide a summary shortly. Only generate the message. Do not respond to this message.")
        webpage_url = args.get("url")
        if webpage_url:
            summary = await asyncio.to_thread(web.fetch_web_summary, webpage_url)
            if summary:
                formatted_summary = web.format_summary_for_discord(summary)
                chunks = web.split_message(formatted_summary)
                final_response = "\n".join(chunks)
                return final_response
            else:
                return "Failed to retrieve summary from the webpage."
        else:
            return "No webpage URL provided."
    elif func == "draw_picture":
        await send_waiting_message(f"Generate a brief message to {username} telling them to wait a moment while you draw them a masterpiece. Only generate the message. Do not respond to this message.")
        prompt_text = args.get("prompt")
        if prompt_text:
            image_bytes = await asyncio.to_thread(image.generate_image, prompt_text)
            st.image(image_bytes, caption="Generated Image")
            return "Nailed It!"
        else:
            return "No prompt provided for drawing a picture."
    elif func == "premiumize_download":
        await send_waiting_message(f"Generate a brief message to {username} telling them to wait a moment while you check if the linked file is cached. Only generate the message. Do not respond to this message.")
        url = args.get("url")
        if url:
            result = await premiumize.process_download_web(url)
            return result
        else:
            return "No URL provided for Premiumize download check."
    elif func == "watch_feed":
        await send_waiting_message(
            f"Generate a brief message to {username} telling them to wait a moment while you add the RSS feed to the watch list. Only generate the message. Do not respond to this message."
        )
        feed_url = args.get("feed_url")
        if feed_url:
            parsed_feed = feedparser.parse(feed_url)
            if parsed_feed.bozo:
                return f"Failed to parse feed: {feed_url}"
            last_ts = 0.0
            if parsed_feed.entries:
                for entry in parsed_feed.entries:
                    if 'published_parsed' in entry:
                        entry_ts = time.mktime(entry.published_parsed)
                        if entry_ts > last_ts:
                            last_ts = entry_ts
            else:
                last_ts = time.time()
            redis_client.hset("rss:feeds", feed_url, last_ts)
            return f"Now watching feed: {feed_url}"
        else:
            return "No feed URL provided for watching."
    elif func == "unwatch_feed":
        await send_waiting_message(
            f"Generate a brief message to {username} telling them to wait a moment while you remove the RSS feed. Only generate the message. Do not respond to this message."
        )
        feed_url = args.get("feed_url")
        if feed_url:
            removed = redis_client.hdel("rss:feeds", feed_url)
            if removed:
                return f"Stopped watching feed: {feed_url}"
            else:
                return f"Feed {feed_url} was not found in the watch list."
        else:
            return "No feed URL provided for unwatching."
    elif func == "list_feeds":
        await send_waiting_message(
            f"Generate a brief message to {username} telling them to wait a moment while you list all currently watched feeds. Only generate the message. Do not respond to this message."
        )
        feeds = redis_client.hgetall("rss:feeds")
        if feeds:
            feed_list = "\n".join(f"{feed} (last update: {feeds[feed]})" for feed in feeds)
            return f"Currently watched feeds:\n{feed_list}"
        else:
            return "No RSS feeds are currently being watched."
    elif func == "web_search":
        await send_waiting_message(f"Generate a brief message to {username} telling them to wait a moment while you search the internet for more information. Only generate the message. Do not respond to this message.")
        query = args.get("query")
        if query:
            results = search_web(query)
            if results:
                formatted_results = format_search_results(results)
                choice_prompt = (
                    f"You are looking for more information on '{query}' because the user asked: '{user_question}'.\n\n"
                    f"Here are the top search results:\n\n"
                    f"{formatted_results}\n\n"
                    "Please choose the most relevant link. Use the following tool for fetching web details and insert the chosen link. "
                    "Respond ONLY with a valid JSON object in the following exact format (and nothing else):\n\n"
                    "For fetching web details:\n"
                    "{\n"
                    '  "function": "web_fetch",\n'
                    '  "arguments": {\n'
                    '      "link": "<chosen link>",\n'
                    f'      "query": "{query}",\n'
                    f'      "user_question": "{user_question}"\n'
                    "  }\n"
                    "}"
                )
                choice_response = await ollama_client.chat(
                    model=ollama_model,
                    messages=[{"role": "system", "content": choice_prompt}],
                    stream=False,
                    keep_alive=-1,
                    options={"num_ctx": context_length}
                )
                choice_text = choice_response['message'].get('content', '').strip()
                try:
                    choice_json = json.loads(choice_text)
                except Exception:
                    json_str = extract_json(choice_text)
                    if json_str:
                        try:
                            choice_json = json.loads(json_str)
                        except Exception:
                            choice_json = None
                    else:
                        choice_json = None
                if not choice_json:
                    final_answer = "Failed to parse the search result choice."
                elif choice_json.get("function") == "web_fetch":
                    args_choice = choice_json.get("arguments", {})
                    link = args_choice.get("link")
                    original_query = args_choice.get("query", query)
                    if link:
                        summary = await asyncio.to_thread(web.fetch_web_summary, link)
                        if summary:
                            info_prompt = (
                                f"Using the detailed information from the selected page below, please provide a clear and concise answer to the original query.\n\n"
                                f"Original Query: '{original_query}'\n"
                                f"User Question: '{user_question}'\n\n"
                                f"Detailed Information:\n{summary}\n\n"
                                "Answer:"
                            )
                            final_response = await ollama_client.chat(
                                model=ollama_model,
                                messages=[{"role": "system", "content": info_prompt}],
                                stream=False,
                                keep_alive=-1,
                                options={"num_ctx": context_length}
                            )
                            final_answer = final_response['message'].get('content', '').strip()
                            if not final_answer:
                                final_answer = "Failed to generate a final answer from the detailed info."
                        else:
                            final_answer = "Failed to extract information from the selected webpage."
                    else:
                        final_answer = "No link provided to fetch web info."
                else:
                    final_answer = "No valid function call for fetching web info was returned."
            else:
                final_answer = "I couldn't find any relevant search results."
            return final_answer
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