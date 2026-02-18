# platforms/homekit_platform.py
import os
import json
import asyncio
import logging
import threading
import time
from typing import Optional, Dict, Any, List
import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Header
from dotenv import load_dotenv
import plugin_registry as pr
from helpers import (
    get_llm_client_from_env,
    build_llm_host_from_env,
)
from cerberus import run_cerberus_turn, resolve_agent_limits

load_dotenv()

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
    """
    Merge consecutive same-role turns to keep history compact.

    IMPORTANT:
    Do NOT insert a blank user message at the beginning.
    Some LLM backends/models can return empty completions when an empty
    user turn (content="") appears in the prompt.
    """
    merged: List[Dict[str, Any]] = []
    for m in loop_messages:
        if not m:
            continue
        if not merged:
            merged.append(m)
            continue
        if merged[-1]["role"] == m["role"]:
            a, b = merged[-1]["content"], m["content"]
            merged[-1]["content"] = (str(a) + "\n\n" + str(b)).strip()
        else:
            merged.append(m)

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

# -------------------- System prompt (HomeKit voice preamble) --------------------
def build_system_prompt() -> str:
    # Platform preamble should be style/format only. Keep voice-focused for Siri playback.
    return (
        "You are a HomeKit/Siri-savvy AI assistant.\n"
        "Responses are spoken aloud by Siri; keep replies short, plain text, and natural.\n"
        "Do not use emojis or markdown.\n"
    )

# -------------------- FastAPI app --------------------
app = FastAPI(title="Tater HomeKit / Siri Bridge", version="1.0")

_llm = None

@app.on_event("startup")
async def _on_startup():
    global _llm
    _llm = get_llm_client_from_env()
    logger.info(f"[HomeKit] LLM client → {build_llm_host_from_env()}")

@app.post("/tater-homekit/v1/message")
async def handle_message(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)):
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

    system_prompt = build_system_prompt()
    loop_messages = await _load_history(session_id, history_max)
    messages_list = loop_messages + [{"role": "user", "content": text_in}]

    await _save_message(session_id, "user", text_in, history_max, session_ttl)

    merged_registry = dict(pr.get_registry_snapshot() or {})
    merged_enabled = _get_plugin_enabled

    try:
        origin = {
            "platform": "homekit",
            "session_id": session_id,
            "device_id": payload.get("device_id"),
            "user_id": payload.get("user_id"),
            "request_id": session_id,
        }
        origin = {k: v for k, v in origin.items() if v not in (None, "")}
        agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
        result = await run_cerberus_turn(
            llm_client=_llm,
            platform="homekit",
            history_messages=messages_list,
            registry=merged_registry,
            enabled_predicate=merged_enabled,
            context={},
            user_text=text_in,
            scope=f"session:{session_id}" if str(session_id or "").strip() else "",
            origin=origin,
            redis_client=redis_client,
            max_rounds=agent_max_rounds,
            max_tool_calls=agent_max_tool_calls,
            platform_preamble=system_prompt,
        )
        final_text = str(result.get("text") or "").strip()
        if len(final_text) > 2000:
            final_text = final_text[:2000] + "…"
        await _save_message(
            session_id,
            "assistant",
            {"marker": "plugin_response", "phase": "final", "content": final_text},
            history_max,
            session_ttl,
        )
        return {"reply": final_text}

    except Exception as e:
        logger.exception("[HomeKit] LLM error")
        await _save_message(session_id, "assistant", f"LLM error: {e}", history_max, session_ttl)
        return {"reply": "Sorry, I had a problem talking to Tater."}

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
            try:
                await server.serve()
            except SystemExit as exc:
                code = getattr(exc, "code", 1)
                if code not in (None, 0):
                    logger.error(
                        f"[HomeKit] Server failed to start on {BIND_HOST}:{port} (likely already in use)."
                    )
            except Exception:
                logger.exception(f"[HomeKit] Server failed on {BIND_HOST}:{port}")

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

    logger.info(f"[HomeKit] Listening on http://{BIND_HOST}:{port}")
    _serve()
