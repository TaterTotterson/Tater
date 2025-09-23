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

try:
    from jinja2 import Template
except ImportError:
    Template = None  # pip install jinja2 if not already

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
# LLM client wrapper (hardcoded template mode)
# ---------------------------------------------------------
def _normalize_base_url(host: str) -> str:
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

        # --- Hardcoded template ---
        self.prompt_template_path = os.path.join(os.path.dirname(__file__), "lmstudio_gemma3.jinja")
        self.prompt_template_text: Optional[str] = None
        if Template and os.path.exists(self.prompt_template_path):
            with open(self.prompt_template_path, "r", encoding="utf-8") as f:
                self.prompt_template_text = f.read()

        self.bos_token = ""  # no BOS token by default
        self.add_generation_prompt = True
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.7"))

        # Allow disabling template mode via env if desired
        template_enabled_env = os.getenv("LLM_TEMPLATE_ENABLED", "1").strip()
        template_enabled = template_enabled_env not in ("0", "false", "False", "no")
        self._template_mode = bool(self.prompt_template_text and Template is not None and template_enabled)

        # Cached probe: does the server support /v1/completions?
        # None = unknown; True/False = remembered result after first attempt.
        self._completions_supported: Optional[bool] = None

    # ---------- Template rendering helpers ----------
    def _render_jinja_prompt(self, messages: List[Dict[str, Any]]) -> str:
        if not self.prompt_template_text:
            raise RuntimeError("Prompt template not found next to helpers.py")
        tmpl = Template(self.prompt_template_text)
        return tmpl.render(
            bos_token=self.bos_token,
            messages=messages,
            add_generation_prompt=self.add_generation_prompt
        )

    def _coerce_for_template(self, messages):
        """
        If a plugin sends only a single system message, coerce to [system, user=<same text>]
        so the Jinja chat template always has a user turn to render.
        """
        if isinstance(messages, list) and len(messages) == 1:
            m0 = messages[0]
            if isinstance(m0, dict) and m0.get("role") == "system":
                sys_txt = m0.get("content", "")
                return [m0, {"role": "user", "content": sys_txt}]
        return messages

    def _looks_like_unsupported_completions(self, err: Exception) -> bool:
        """Heuristic: detect servers that don't support /v1/completions (e.g., Ollama OpenAI compat)."""
        from requests import HTTPError
        if isinstance(err, HTTPError) and err.response is not None:
            code = err.response.status_code
            text = (err.response.text or "").lower()
            if code in (404, 405):
                return True
            # Common proxy/server messages:
            needles = ["unknown route", "not found", "no such path", "this route does not exist"]
            if any(n in text for n in needles):
                return True
        # Connection errors or JSON shape mismatches can also indicate lack of support
        msg = str(err).lower()
        return "endpoint" in msg and "completions" in msg or "not found" in msg

    async def _complete_with_template(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        messages = self._coerce_for_template(messages)
        prompt_text = self._render_jinja_prompt(messages)
        url = f"{self.host}/completions"
        headers = {"Content-Type": "application/json"}
        payload = {
            "model": self.model,
            "prompt": prompt_text,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "stream": False
        }
        resp = await asyncio.to_thread(
            requests.post, url, headers=headers, data=json.dumps(payload), timeout=600
        )
        resp.raise_for_status()
        data = resp.json()
        text = (data.get("choices") or [{}])[0].get("text", "")
        return {"model": data.get("model", self.model),
                "message": {"role": "assistant", "content": text}}

    # ---------- Public API ----------
    async def chat(self, messages, **kwargs):
        """
        If template mode is enabled, try /v1/completions first.
        If the server doesn't support it (Ollama), fall back to /v1/chat/completions and
        remember that for future calls (so we don't keep trying /completions).
        """
        # Try template path if enabled and not known to be unsupported
        if self._template_mode and self._completions_supported is not False:
            try:
                result = await self._complete_with_template(messages)
                self._completions_supported = True
                return result
            except Exception as e:
                # If this looks like "route unsupported", flip the bit and fall through
                if self._looks_like_unsupported_completions(e):
                    self._completions_supported = False
                else:
                    # Real error during LM Studio call; surface it
                    raise

        # Fallback: OpenAI-compatible /v1/chat/completions (works on Ollama et al.)
        stream = kwargs.pop("stream", False)
        model = kwargs.pop("model", self.model)
        response = await self.client.chat.completions.create(
            model=model,
            messages=messages,
            stream=stream,
            **kwargs,
        )
        if stream:
            return response
        choice = response.choices[0].message
        return {"model": response.model, "message": {"role": choice.role, "content": choice.content}}

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

    if isinstance(response_json, dict) and "function" in response_json:
        return response_json
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