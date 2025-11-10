# platforms/homekit_platform.py
import os
import json
import asyncio
import logging
import threading
import time
from typing import Optional, Dict, Any, List
from datetime import datetime

import redis
import uvicorn
from fastapi import FastAPI, HTTPException

from dotenv import load_dotenv
load_dotenv()

from helpers import LLMClientWrapper, parse_function_json, get_tater_name
from plugin_registry import plugin_registry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("homekit")

# -------------------- Platform defaults --------------------
BIND_HOST = "0.0.0.0"
DEFAULT_PORT = 8789
TIMEOUT_SECONDS = 60
DEFAULT_SESSION_HISTORY_MAX = 4
DEFAULT_MAX_HISTORY_CAP = 12
DEFAULT_SESSION_TTL_SECONDS = 60 * 60  # 1h

# -------------------- Redis --------------------
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# -------------------- Platform settings --------------------
PLATFORM_SETTINGS = {
    "category": "HomeKit / Siri",
    "required": {
        "bind_port": {
            "label": "Bind Port",
            "type": "number",
            "default": DEFAULT_PORT,
            "description": "TCP port for the Tater ↔ Siri / Shortcuts bridge"
        },
        "SESSION_HISTORY_MAX": {
            "label": "Session History (turns)",
            "type": "number",
            "default": DEFAULT_SESSION_HISTORY_MAX,
            "description": "How many recent turns to include per Siri conversation."
        },
        "MAX_HISTORY_CAP": {
            "label": "Max History Cap",
            "type": "number",
            "default": DEFAULT_MAX_HISTORY_CAP,
            "description": "Hard ceiling to prevent runaway context sizes."
        },
        "SESSION_TTL_SECONDS": {
            "label": "Session TTL (seconds)",
            "type": "number",
            "default": DEFAULT_SESSION_TTL_SECONDS,
            "description": "How long to keep a Siri session alive."
        },
        "AUTH_TOKEN": {
            "label": "Auth Token (optional)",
            "type": "string",
            "default": "",
            "description": "If set, Shortcuts must send this token in X-Tater-Token header."
        },
    }
}

def _platform_settings() -> Dict[str, str]:
    return redis_client.hgetall("homekit_platform_settings") or {}

def _get_int_setting(name: str, default: int) -> int:
    s = _platform_settings().get(name)
    if s is None or str(s).strip() == "":
        return default
    try:
        return int(str(s).strip())
    except Exception:
        return default

def _get_str_setting(name: str, default: str = "") -> str:
    s = _platform_settings().get(name)
    return s if s is not None else default

# -------------------- Plugin gating --------------------
def _get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")

# -------------------- History helpers --------------------
def _flatten_to_text(res: Any) -> str:
    if res is None:
        return ""
    if isinstance(res, str):
        return res
    if isinstance(res, list):
        parts = []
        for item in res:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                t = item.get("type") or "content"
                name = item.get("name") or ""
                parts.append(f"[{t}{(':'+name) if name else ''}]")
            else:
                parts.append(str(item))
        return "\n".join(p for p in parts if p).strip()
    if isinstance(res, dict):
        if "message" in res and isinstance(res["message"], str):
            return res["message"]
        try:
            return json.dumps(res, ensure_ascii=False)
        except Exception:
            return str(res)
    return str(res)

def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    # skip tool “waiting” markers if you ever add them
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    # include FINAL plugin responses as plain assistant text
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        if content.get("phase", "final") != "final":
            return None
        payload = content.get("content", "")
        txt = _flatten_to_text(payload).strip()
        if len(txt) > 4000:
            txt = txt[:4000] + " …"
        return {"role": "assistant", "content": txt}

    # stringify plugin_call so the model “sees” prior actions
    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps({
            "function": content.get("plugin"),
            "arguments": content.get("arguments", {})
        }, ensure_ascii=False)
        return {"role": "assistant", "content": as_text}

    # default cases
    if isinstance(content, str):
        return {"role": role, "content": content}

    # anything else, compact to string
    return {"role": role, "content": _flatten_to_text(content)}

def _enforce_user_assistant_alternation(loop_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for m in loop_messages:
        if not m:
            continue
        if not merged:
            merged.append(m); continue
        if merged[-1]["role"] == m["role"]:
            a, b = merged[-1]["content"], m["content"]
            merged[-1]["content"] = (str(a) + "\n\n" + str(b)).strip()
        else:
            merged.append(m)
    if merged and merged[0]["role"] != "user":
        merged.insert(0, {"role": "user", "content": ""})
    return merged

def _sess_key(session_id: Optional[str]) -> str:
    return f"tater:homekit:session:{session_id or 'default'}:history"

async def _load_history(session_id: Optional[str], limit: int) -> List[Dict[str, Any]]:
    key = _sess_key(session_id)
    raw = redis_client.lrange(key, -limit, -1)
    loop_messages: List[Dict[str, Any]] = []
    for entry in raw:
        try:
            obj = json.loads(entry)
            role = obj.get("role", "user")
            content = obj.get("content")
            templ = _to_template_msg(role, content)
            if templ is not None:
                loop_messages.append(templ)
        except Exception:
            continue
    return _enforce_user_assistant_alternation(loop_messages)

async def _save_message(session_id: Optional[str], role: str, content: Any, max_store: int, ttl: int):
    key = _sess_key(session_id)
    pipe = redis_client.pipeline()
    pipe.rpush(key, json.dumps({"role": role, "content": content}))
    if max_store > 0:
        pipe.ltrim(key, -max_store, -1)
    pipe.expire(key, ttl)
    pipe.execute()

# -------------------- System prompt (HA-style) --------------------
def build_system_prompt() -> str:
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    first, last = get_tater_name()

    base_prompt = (
        f"You are {first} {last}, an AI assistant being accessed through Apple Siri or the Shortcuts app.\n"
        "Your responses are spoken aloud by Siri, so keep them short, natural, and free of emojis.\n\n"
        "When the user requests an action that needs a plugin or tool, reply ONLY with a valid JSON tool call.\n"
        "For simple questions or small talk, answer briefly in one friendly sentence.\n"
    )

    # HomeKit tool list (Discord-style filtering)
    tool_instructions = "\n\n".join(
        f"Tool: {plugin.name}\n"
        f"Description: {getattr(plugin, 'description', 'No description provided.')}\n"
        f"{plugin.usage}"
        for plugin in plugin_registry.values()
        if (
            ("homekit" in getattr(plugin, "platforms", []))
            or ("both" in getattr(plugin, "platforms", []))
        ) and _get_plugin_enabled(plugin.name)
    )

    behavior_guard = (
        "Only call a tool if the user's message clearly requests an action — such as 'turn on', 'set', "
        "'summarize', 'download', 'generate', or 'post'.\n"
        "Never call a tool for casual messages like 'thanks', 'ok', or 'goodnight'.\n"
        "Keep non-tool replies under 25 words so Siri can read them naturally.\n"
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"{base_prompt}\n\n"
        f"{tool_instructions}\n\n"
        f"{behavior_guard}"
    )

# -------------------- FastAPI app --------------------
app = FastAPI(title="Tater HomeKit / Siri Bridge", version="1.0")
_llm: Optional[LLMClientWrapper] = None

@app.on_event("startup")
async def _on_startup():
    global _llm
    llm_host = os.getenv("LLM_HOST", "127.0.0.1")
    llm_port = os.getenv("LLM_PORT", "11434")
    _llm = LLMClientWrapper(host=f"http://{llm_host}:{llm_port}")
    logger.info(f"[HomeKit] LLM client → http://{llm_host}:{llm_port}")

@app.post("/tater-homekit/v1/message")
async def handle_message(payload: Dict[str, Any], x_tater_token: Optional[str] = None):
    """
    Expected JSON from Shortcut:
    {
      "text": "ask tater to turn on the office light",
      "session_id": "iphone-masta"
    }
    """
    if _llm is None:
        raise HTTPException(503, "LLM not ready")

    configured_token = _get_str_setting("AUTH_TOKEN", "")
    if configured_token:
        if not x_tater_token or x_tater_token != configured_token:
            raise HTTPException(401, "Bad token")

    text_in = (payload.get("text") or "").strip()
    if not text_in:
        return {"reply": "(no text provided)"}

    session_id = payload.get("session_id") or "default"
    session_history_max = _get_int_setting("SESSION_HISTORY_MAX", DEFAULT_SESSION_HISTORY_MAX)
    max_history_cap = _get_int_setting("MAX_HISTORY_CAP", DEFAULT_MAX_HISTORY_CAP)
    history_max = min(max(session_history_max, 0), max_history_cap)
    session_ttl = _get_int_setting("SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS)

    await _save_message(session_id, "user", text_in, history_max, session_ttl)

    system_prompt = build_system_prompt()
    loop_messages = await _load_history(session_id, history_max)
    messages_list = [{"role": "system", "content": system_prompt}] + loop_messages
    messages_list.append({"role": "user", "content": text_in})

    try:
        resp = await _llm.chat(messages_list, timeout=TIMEOUT_SECONDS)
    except Exception as e:
        logger.exception("[HomeKit] LLM error")
        await _save_message(session_id, "assistant", f"LLM error: {e}", history_max, session_ttl)
        return {"reply": "Sorry, I had a problem talking to Tater."}

    if isinstance(resp, dict):
        text = (resp.get("message") or {}).get("content", "") or ""
    else:
        text = str(resp)

    if not text:
        await _save_message(session_id, "assistant", "", history_max, session_ttl)
        return {"reply": "Sorry, I didn't catch that."}

    fn = parse_function_json(text)
    if fn:
        func = fn.get("function")
        args = fn.get("arguments", {}) or {}

        await _save_message(session_id, "assistant",
                            {"marker": "plugin_call", "plugin": func, "arguments": args},
                            history_max, session_ttl)

        plugin = plugin_registry.get(func)
        if not plugin or not _get_plugin_enabled(func):
            msg = f"Function `{func}` is not available for HomeKit/Siri."
            await _save_message(session_id, "assistant", msg, history_max, session_ttl)
            return {"reply": msg}

        try:
            if hasattr(plugin, "handle_homeassistant"):
                result = await plugin.handle_homeassistant(args, _llm)
            elif hasattr(plugin, "handle_webui"):
                result = await plugin.handle_webui(args=args)
            elif hasattr(plugin, "handle_discord"):
                result = await plugin.handle_discord(None, None, args)
            else:
                result = f"Plugin `{func}` does not support this platform."
        except Exception as e:
            logger.exception(f"[HomeKit] plugin {func} error")
            result = f"I tried to run {func} but hit an error: {e}"

        if isinstance(result, str):
            final_text = result
        elif isinstance(result, dict) and "message" in result:
            final_text = str(result["message"])
        else:
            final_text = json.dumps(result)[:2000]

        await _save_message(session_id, "assistant",
                            {"marker": "plugin_response", "phase": "final", "content": final_text},
                            history_max, session_ttl)
        return {"reply": final_text}

    final_text = text.strip()
    if len(final_text) > 2000:
        final_text = final_text[:2000] + "…"

    await _save_message(session_id, "assistant", final_text, history_max, session_ttl)
    return {"reply": final_text}

def run(stop_event: Optional[threading.Event] = None):
    raw_port = redis_client.hget("homekit_platform_settings", "bind_port")
    try:
        port = int(raw_port) if raw_port is not None else DEFAULT_PORT
    except (TypeError, ValueError):
        logger.warning(f"[HomeKit] Invalid bind_port '{raw_port}', defaulting to {DEFAULT_PORT}")
        port = DEFAULT_PORT

    config = uvicorn.Config(app, host=BIND_HOST, port=port, log_level="info")
    server = uvicorn.Server(config)

    def _serve():
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()

        async def _start():
            await server.serve()

        task = loop.create_task(_start())

        def _watch():
            if not stop_event:
                return
            while not stop_event.is_set():
                time.sleep(0.5)
            try:
                server.should_exit = True
            except Exception:
                pass

        if stop_event:
            threading.Thread(target=_watch, daemon=True).start()

        try:
            loop.run_until_complete(task)
        finally:
            if not loop.is_closed():
                loop.stop()
                loop.close()

    threading.Thread(target=_serve, daemon=True).start()
    logger.info(f"[HomeKit] Listening on http://{BIND_HOST}:{port}")