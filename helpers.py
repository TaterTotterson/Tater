import os
import asyncio
from openai import AsyncOpenAI
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
# LLM client wrapper
# ---------------------------------------------------------
class LLMClientWrapper:
    def __init__(self, host, model=None, context_length=None, keep_alive=-1, **kwargs):
        model = model or os.getenv("LLM_MODEL", "gemma3:27b")
        context_length = context_length or int(os.getenv("CONTEXT_LENGTH", 10000))
        base_url = host.rstrip('/')
        if not base_url.startswith("http"):
            base_url = f"http://{base_url}"
        if not base_url.endswith("/v1"):
            base_url = f"{base_url}/v1"
        self.client = AsyncOpenAI(base_url=base_url, api_key=os.getenv("LLM_API_KEY", "not-needed"), **kwargs)
        self.host = base_url
        self.model = model
        self.context_length = context_length
        self.keep_alive = keep_alive

    async def chat(self, messages, **kwargs):
        options = kwargs.get("options", {})
        stream = kwargs.get("stream", False)
        response = await self.client.chat.completions.create(
            model=kwargs.get("model", self.model),
            messages=messages,
            stream=stream,
            **options,
        )
        if stream:
            return response
        choice = response.choices[0].message
        return {"model": response.model, "message": {"role": choice.role, "content": choice.content}}

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

    history = redis_client.lrange(key, 0, -1)
    for entry in reversed(history):
        try:
            msg = json.loads(entry)
            content = msg.get("content")

            # 🔥 Unwrap plugin_response wrapper
            if isinstance(content, dict) and content.get("marker") == "plugin_response":
                content = content.get("content", {})

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
    history_key = f"tater:channel:{channel_id}:history"
    raw_history = redis_client.lrange(history_key, 0, -1)

    for entry in reversed(raw_history):
        try:
            data = json.loads(entry)
            content = data.get("content")

            # 🔥 Unwrap plugin_response wrapper
            if isinstance(content, dict) and content.get("marker") == "plugin_response":
                content = content.get("content", {})

            if isinstance(content, dict) and content.get("type") == filetype:
                filename = content.get("name", "").lower()
                if not extensions or any(filename.endswith(ext) for ext in extensions):
                    return content
        except Exception:
            continue

    return None