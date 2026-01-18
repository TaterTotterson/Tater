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

def get_tater_personality():
    """
    Return the assistant's personality / style description from Redis.
    Empty string means 'no forced personality'.
    """
    personality = redis_client.get("tater:personality")
    if not personality:
        personality = ""
        redis_client.set("tater:personality", personality)

    return personality

# ---------------------------------------------------------
# Main event loop reference + run_async helper
# ---------------------------------------------------------
_main_loop = None

def set_main_loop(loop):
    global _main_loop
    _main_loop = loop

def run_async(coro):
    loop = _main_loop or asyncio.get_event_loop_policy().get_event_loop()
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

def _sanitize_chat_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Defensive sanitizer for OpenAI-compatible chat endpoints.

    - Drops empty user turns (content == "" after coercion)
      These can cause some backends (LM Studio / some Qwen templates) to return empty completions.
    - Coerces non-string message content (lists/dicts) into plain text,
      so we don't send multimodal structures to backends that don't support them.
    - Drops messages with missing role/content.
    """
    if not isinstance(messages, list):
        return []

    cleaned: List[Dict[str, Any]] = []
    for m in messages:
        if not isinstance(m, dict):
            continue

        role = (m.get("role") or "").strip()
        if role not in ("system", "user", "assistant", "tool"):
            # keep it strict; unknown roles can confuse some servers
            continue

        raw_content = m.get("content", None)

        # Convert any non-string (list/dict/etc) to text for maximum compatibility
        content_text = _coerce_content_to_text(raw_content).strip()

        # Drop empty user turns entirely (this is the big one)
        if role == "user" and content_text == "":
            continue

        # Also drop empty assistant/tool turns (optional but generally helpful)
        if role in ("assistant", "tool") and content_text == "":
            continue

        # System messages should not be empty either
        if role == "system" and content_text == "":
            continue

        cleaned.append({"role": role, "content": content_text})

    return cleaned

def _coerce_content_to_text(content) -> str:
    if isinstance(content, str):
        return content

    if isinstance(content, dict):
        # Common places providers hide text
        for k in ("text", "content", "value"):
            v = content.get(k)
            if isinstance(v, str):
                return v.strip()
        # Sometimes it's a list inside a dict
        for k in ("parts", "content", "messages"):
            v = content.get(k)
            if isinstance(v, list):
                return _coerce_content_to_text(v)
        return ""

    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, str):
                parts.append(p)
            elif isinstance(p, dict):
                # Prefer explicit text/content/value keys
                for k in ("text", "content", "value"):
                    v = p.get(k)
                    if isinstance(v, str):
                        parts.append(v)
                        break
        return "\n".join(s for s in parts if s).strip()

    return "" if content is None else str(content)

def build_llm_host_from_env(default_host="127.0.0.1", default_port="11434") -> str:
    """
    Build a usable host string for LLMClientWrapper from env vars.

    Supports:
      - LLM_HOST=192.168.1.50, LLM_PORT=11434 -> http://192.168.1.50:11434
      - LLM_HOST=http://192.168.1.50, LLM_PORT=11434 -> http://192.168.1.50:11434
      - LLM_HOST=http://192.168.1.50:11434, LLM_PORT=11434 -> http://192.168.1.50:11434
      - LLM_HOST=https://example.com/v1, LLM_PORT= -> https://example.com/v1
    """
    llm_host = (os.getenv("LLM_HOST", default_host) or "").strip()
    llm_port = (os.getenv("LLM_PORT", default_port) or "").strip()

    # If the user included scheme, don't prepend http://
    if llm_host.startswith("http://") or llm_host.startswith("https://"):
        # If a port is specified and the URL doesn't already end with that port, append it.
        # This is mainly for "http://host" + LLM_PORT=11434 style configs.
        if llm_port:
            # Parse to see if port already present
            p = urlparse(llm_host)
            if p.port is None:
                # No port present -> append
                return f"{llm_host.rstrip('/') }:{llm_port}"
        return llm_host.rstrip("/")

    # No scheme provided -> assume http:// and append port
    return f"http://{llm_host}:{llm_port}"

def get_llm_client_from_env(**kwargs) -> "LLMClientWrapper":
    """
    Construct an LLMClientWrapper using LLM_HOST/LLM_PORT env vars.
    """
    host = build_llm_host_from_env()
    return LLMClientWrapper(host=host, **kwargs)

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

        # sanitize messages (prevents empty-user poison + normalizes non-string content)
        try:
            messages = _sanitize_chat_messages(messages if isinstance(messages, list) else [])
        except Exception:
            # fail open: at least avoid crashing
            messages = messages if isinstance(messages, list) else []

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
            return {"model": getattr(response, "model", model),
                    "message": {"role": "assistant", "content": ""}}

        choice = response.choices[0].message or {}
        raw_content = getattr(choice, "content", "") if hasattr(choice, "content") else choice.get("content", "")
        content_text = _coerce_content_to_text(raw_content)

        return {
            "model": getattr(response, "model", model),
            "message": {"role": getattr(choice, "role", "assistant"), "content": content_text}
        }

# ---------------------------------------------------------
# Function JSON parsing helpers (unchanged)
# ---------------------------------------------------------
def extract_json(text: str):
    """
    Extract the first valid JSON object or array from text.
    Strips code fences and tolerates extra prose around it.
    Works for both { ... } and [ ... ] blocks.
    """
    if not text:
        return None

    s = text.strip()

    # Remove ```json fences
    if s.startswith("```") and s.endswith("```"):
        s = re.sub(r"^```(?:json)?\s*|\s*```$", "", s, flags=re.MULTILINE).strip()

    # Try whole text first
    try:
        json.loads(s)
        return s
    except Exception:
        pass

    # Bracket scanning for either {...} or [...]
    stack = []
    start_idx = None
    for i, char in enumerate(s):
        if char in "{[":
            if not stack:
                start_idx = i
            stack.append(char)
        elif char in "}]":
            if stack:
                opening = stack.pop()
                if not stack and start_idx is not None:
                    candidate = s[start_idx:i+1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except json.JSONDecodeError:
                        continue
    return None

def parse_function_json(response_text: str):
    def _pick(obj):
        if isinstance(obj, dict):
            if "function" in obj and isinstance(obj["function"], str):
                return {"function": obj["function"], "arguments": obj.get("arguments", {}) or {}}
            if "tool" in obj and isinstance(obj["tool"], str):
                return {"function": obj["tool"], "arguments": obj.get("arguments", {}) or {}}
        return None

    if not response_text:
        return None

    s = str(response_text).strip()

    # strip code fences early so shorthand/prefix parsing still works
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.MULTILINE).strip()
    if s.endswith("```"):
        s = re.sub(r"\s*```$", "", s, flags=re.MULTILINE).strip()

    # ---------------------------------------------------------
    # NEW: Embedded shorthand support:
    #   "Sure, here you go ha_control{...}"
    # ---------------------------------------------------------
    m_anywhere = re.search(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(\{[^{}]*\})', s)
    if m_anywhere:
        func = m_anywhere.group(1)
        blob = m_anywhere.group(2)
        try:
            args = json.loads(blob)
            if isinstance(args, dict):
                return {"function": func, "arguments": args}
        except Exception:
            pass

    # Accept shorthand ONLY when it's the whole message: ha_control{"query":"..."}
    m = re.match(r'^([a-zA-Z0-9_]+)\s*(\{.*\})\s*$', s, re.DOTALL)
    if m:
        func = m.group(1)
        blob = m.group(2)
        try:
            args = json.loads(blob)
            if isinstance(args, dict):
                return {"function": func, "arguments": args}
        except Exception:
            pass

    try:
        response_json = json.loads(s)
    except json.JSONDecodeError:
        json_str = extract_json(s)
        if not json_str:
            return None

        prefix = s.split(json_str, 1)[0].strip()

        # Slightly stricter "possible func" match
        m2 = re.search(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*[:(]*\s*$', prefix)
        if m2:
            possible_func = m2.group(1)
            try:
                args = json.loads(json_str)
                if isinstance(args, dict):
                    return {"function": possible_func, "arguments": args}
            except Exception:
                pass

        try:
            response_json = json.loads(json_str)
        except Exception:
            return None

    picked = _pick(response_json)
    if picked:
        return picked

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