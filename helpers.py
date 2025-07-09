import os
import asyncio
import ollama
import requests
from PIL import Image
from io import BytesIO
import nest_asyncio
import redis
from dotenv import load_dotenv
import re
import json
import base64

load_dotenv()
nest_asyncio.apply()

# Redis setup
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', '127.0.0.1'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True
)

DEFAULT_KEEP_ALIVE = -1
DEFAULT_ASSISTANT_AVATAR_URL = "https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater.png"

# ---------------------------------------------------------
# Image loading utility
# ---------------------------------------------------------
def load_image_from_url(url: str = DEFAULT_ASSISTANT_AVATAR_URL) -> Image.Image:
    response = requests.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content))

# ---------------------------------------------------------
# Save assistant message to Redis history
# ---------------------------------------------------------
def save_assistant_message(channel_id, content):
    if isinstance(channel_id, str) and (channel_id.startswith("#") or channel_id.startswith("irc:")):
        key = f"tater:irc:{channel_id}:history"
    elif isinstance(channel_id, str) and channel_id.startswith("webui"):
        key = f"tater:channel:{channel_id}:history"
    else:
        key = f"tater:channel:{channel_id}:history"

    redis_client.rpush(key, json.dumps({
        "role": "assistant",
        "username": "assistant",
        "content": content
    }))
    redis_client.ltrim(key, -20, -1)

# ---------------------------------------------------------
# Send a waiting message via Ollama and optionally save/send
# ---------------------------------------------------------
async def send_waiting_message(
    ollama_client,
    prompt_text,
    model=None,
    context_length=None,
    save_callback=None,
    send_callback=None
):
    if model is None:
        model = getattr(ollama_client, "model", os.getenv("OLLAMA_MODEL", "gemma3:27b"))
    if context_length is None:
        context_length = getattr(ollama_client, "context_length", int(os.getenv("CONTEXT_LENGTH", 20000)))
    keep_alive = getattr(ollama_client, "keep_alive", DEFAULT_KEEP_ALIVE)

    waiting_response = await ollama_client.chat(
        model=model,
        messages=[{"role": "system", "content": prompt_text}],
        stream=False,
        keep_alive=keep_alive,
        options={"num_ctx": context_length}
    )

    waiting_text = waiting_response["message"].get("content", "").strip() or prompt_text

    if save_callback:
        save_callback(waiting_text)
    if send_callback:
        ret = send_callback(waiting_text)
        if asyncio.iscoroutine(ret):
            await ret
    return waiting_text

# ---------------------------------------------------------
# Main event loop reference + run_async helper
# ---------------------------------------------------------
_main_loop = None

def set_main_loop(loop):
    global _main_loop
    _main_loop = loop

def run_async(coro):
    loop = _main_loop or asyncio.get_event_loop()
    return loop.run_until_complete(coro)

# ---------------------------------------------------------
# Ollama client wrapper
# ---------------------------------------------------------
class OllamaClientWrapper(ollama.AsyncClient):
    def __init__(self, host, model=None, context_length=None, keep_alive=-1, **kwargs):
        model = model or os.getenv("OLLAMA_MODEL", "command-r:35B")
        context_length = context_length or int(os.getenv("CONTEXT_LENGTH", 10000))
        super().__init__(host=host, **kwargs)
        self.host = host
        self.model = model
        self.context_length = context_length
        self.keep_alive = keep_alive

    async def chat(self, messages, **kwargs):
        options = kwargs.get("options", {}).copy()
        options.setdefault("num_ctx", self.context_length)

        return await super().chat(
            model=kwargs.get("model", self.model),
            messages=messages,
            stream=kwargs.get("stream", False),
            keep_alive=kwargs.get("keep_alive", self.keep_alive),
            options=options,
        )

# ---------------------------------------------------------
# Function JSON parsing helpers
# ---------------------------------------------------------
def extract_json(text):
    text = text.strip()
    if text.startswith("```") and text.endswith("```"):
        text = re.sub(r"^```(?:json)?\n?|```$", "", text, flags=re.MULTILINE).strip()

    stack = []
    start_idx = None
    for i, char in enumerate(text):
        if char == '{':
            if not stack:
                start_idx = i
            stack.append('{')
        elif char == '}':
            if stack:
                stack.pop()
                if not stack and start_idx is not None:
                    candidate = text[start_idx:i+1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except json.JSONDecodeError:
                        continue
    return None

def parse_function_json(response_text):
    try:
        response_json = json.loads(response_text)
    except json.JSONDecodeError:
        json_str = extract_json(response_text)
        if json_str:
            try:
                response_json = json.loads(json_str)
            except Exception:
                return None
        else:
            return None

    # If it's a single function object
    if isinstance(response_json, dict) and "function" in response_json:
        return response_json

    # If it's a list of functions, return the first valid one
    if isinstance(response_json, list):
        for item in response_json:
            if isinstance(item, dict) and "function" in item:
                return item

    return None

# ---------------------------------------------------------
# IRC formatting cleanup
# ---------------------------------------------------------
def format_irc(text):
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)  # Bold
    text = re.sub(r"\*(.*?)\*", r"\1", text)      # Italic
    text = re.sub(r"_([^_]+)_", r"\1", text)      # Underline/Italic
    text = re.sub(r"`([^`]+)`", r"\1", text)      # Inline code
    text = re.sub(r"#+\s*", "", text)             # Headers
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"^- ", "* ", text, flags=re.MULTILINE)
    text = re.sub(r"\n\s*\n", "\n\n", text)
    return text.strip()

# ---------------------------------------------------------
# Get latest image from redis
# ---------------------------------------------------------
def get_latest_image_from_history(key: str, allowed_mimetypes=None):
    if allowed_mimetypes is None:
        allowed_mimetypes = ["image/png", "image/jpeg"]

    # Scan the full history stored in Redis
    history = redis_client.lrange(key, 0, -1)
    for entry in reversed(history):  # Start from the newest
        try:
            msg = json.loads(entry)
            content = msg.get("content")
            if isinstance(content, dict):
                mimetype = content.get("mimetype", "")
                filename = content.get("name", "").lower()

                if (
                    content.get("type") == "image"
                    and content.get("data")
                    and mimetype in allowed_mimetypes
                    and not filename.endswith(".webp")
                ):
                    image_bytes = base64.b64decode(content["data"])
                    return image_bytes, filename or "input.png"
        except Exception:
            continue

    return None, None

# ---------------------------------------------------------
# Get latest file from redis
# ---------------------------------------------------------
def get_latest_file_from_history(channel_id, filetype="file", extensions=None):
    """
    Search Redis chat history for the latest file-type entry with optional extensions (e.g. .torrent).
    """
    history_key = f"tater:channel:{channel_id}:history"
    raw_history = redis_client.lrange(history_key, 0, -1)  # Read full history

    for entry in reversed(raw_history):
        data = json.loads(entry)
        content = data.get("content")
        if isinstance(content, dict) and content.get("type") == filetype:
            filename = content.get("name", "").lower()
            if not extensions or any(filename.endswith(ext) for ext in extensions):
                return content
    return None