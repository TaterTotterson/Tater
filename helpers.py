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
import uuid
import time
import websocket
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, urlunparse

load_dotenv()
nest_asyncio.apply()

# Redis setup
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', '127.0.0.1'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True
)

def get_tater_name():
    """Return the assistant's first and last name from Redis."""
    first = redis_client.get("tater:first_name")
    if not first:
        first = "Tater"
        redis_client.set("tater:first_name", first)

    last = redis_client.get("tater:last_name")
    if not last:
        last = "Totterson"
        redis_client.set("tater:last_name", last)

    return first, last

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
# LLM client wrapper (OpenAI-compatible /v1/chat/completions)
# ---------------------------------------------------------
def _normalize_base_url(host: str) -> str:
    """
    Ensure base_url ends with /v1 and includes scheme.
    Accepts: 127.0.0.1:11434  -> http://127.0.0.1:11434/v1
             http://host:port -> http://host:port/v1 (if missing)
             https://api.foo/v1 -> unchanged
    """
    if not host.startswith("http://") and not host.startswith("https://"):
        host = f"http://{host}"
    parsed = urlparse(host.rstrip("/"))
    path = parsed.path
    if not path.endswith("/v1"):
        path = (path + "/v1").replace("//", "/")
    return urlunparse(parsed._replace(path=path))

class LLMClientWrapper:
    def __init__(self, host, model=None, **kwargs):
        model = model or os.getenv("LLM_MODEL", "gemma-3-27b-it-abliterated")
        base_url = _normalize_base_url(host)

        self.client = AsyncOpenAI(
            base_url=base_url,
            api_key=os.getenv("LLM_API_KEY", "not-needed"),
            **kwargs
        )

        self.host = base_url.rstrip("/")
        self.model = model

        # Common generation defaults (caller can override per-call)
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.7"))

    async def chat(self, messages, **kwargs):
        """
        Thin wrapper around OpenAI-compatible /v1/chat/completions.
        Accepts either timeout (seconds) or timeout_ms (milliseconds).
        Returns: {"model": str, "message": {"role": "assistant", "content": "..."}}
        """
        # Normalize timeout variants
        timeout = kwargs.pop("timeout", None)
        timeout_ms = kwargs.pop("timeout_ms", None)
        if timeout is None and timeout_ms is not None:
            try:
                timeout = float(timeout_ms) / 1000.0
            except Exception:
                timeout = None

        stream = kwargs.pop("stream", False)
        model = kwargs.pop("model", self.model)

        # Provide sensible defaults if not supplied
        if "max_tokens" not in kwargs:
            kwargs["max_tokens"] = self.max_tokens
        if "temperature" not in kwargs:
            kwargs["temperature"] = self.temperature

        response = await self.client.chat.completions.create(
            model=model,
            messages=messages,
            stream=stream,
            timeout=timeout,
            **kwargs,
        )

        if stream:
            return response

        # Defensive: choices can be empty in edge cases / errors
        if not getattr(response, "choices", None):
            return {"model": getattr(response, "model", model), "message": {"role": "assistant", "content": ""}}

        choice = response.choices[0].message
        # Some providers can return None for content in tool-use scenarios; normalize to empty string
        content = choice.content if isinstance(choice.content, str) else (choice.content or "")
        return {
            "model": response.model,
            "message": {"role": getattr(choice, "role", "assistant"), "content": content}
        }

# ---------------------------------------------------------
# Function JSON parsing helpers (unchanged)
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
    def _pick(obj):
        # prefer "function", but allow "tool" as an alias used by some models
        if isinstance(obj, dict):
            if "function" in obj and isinstance(obj["function"], str):
                return {"function": obj["function"], "arguments": obj.get("arguments", {})}
            if "tool" in obj and isinstance(obj["tool"], str):
                return {"function": obj["tool"], "arguments": obj.get("arguments", {})}
        return None

    try:
        response_json = json.loads(response_text)
    except json.JSONDecodeError:
        json_str = extract_json(response_text)
        if not json_str:
            return None
        try:
            response_json = json.loads(json_str)
        except Exception:
            return None

    # dict case
    picked = _pick(response_json)
    if picked:
        return picked

    # list case: return the first valid tool/function object
    if isinstance(response_json, list):
        for item in response_json:
            picked = _pick(item)
            if picked:
                return picked

    return None

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

# ---------------------------------------------------------
# ComfyUI websocket (no timeouts, Ctrl-C friendly)
# ---------------------------------------------------------
def run_comfy_prompt(base_http: str, base_ws: str, prompt: dict):
    client_id = str(uuid.uuid4())

    # 1) Open dedicated WS for this job (no timeout)
    ws = websocket.create_connection(f"{base_ws}/ws?clientId={client_id}")

    try:
        # 2) POST the prompt, include client_id (no timeout)
        resp = requests.post(
            f"{base_http}/prompt",
            json={"prompt": prompt, "client_id": client_id}
        )
        resp.raise_for_status()
        data = resp.json()
        prompt_id = data.get("prompt_id") or data.get("promptId")
        if not prompt_id:
            raise RuntimeError(f"ComfyUI /prompt did not return prompt_id: {data}")

        # 3) Listen until our prompt is finished
        while True:
            try:
                raw = ws.recv()  # blocks; KeyboardInterrupt will break out cleanly
            except KeyboardInterrupt:
                # Graceful cancel: close socket and bubble up so caller can handle it
                try:
                    ws.close()
                finally:
                    raise
            except Exception as e:
                # Other WS errors bubble as runtime errors
                raise RuntimeError(f"ComfyUI WS error for prompt {prompt_id}: {e}")

            if not raw:
                continue

            try:
                evt = json.loads(raw)
            except Exception:
                continue

            etype = evt.get("type")
            edata = evt.get("data") or {}
            evt_prompt_id = edata.get("prompt_id") or evt.get("prompt_id")

            # Only react to our own prompt
            if evt_prompt_id != prompt_id:
                continue

            # Finished: 'executing' with node == None indicates completion
            if etype == "executing" and edata.get("node") is None:
                return prompt_id, evt

            # (Optional: handle other terminal frames here if your setup emits them.)

    finally:
        try:
            ws.close()
        except Exception:
            pass