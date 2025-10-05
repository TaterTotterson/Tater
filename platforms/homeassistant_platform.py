# platforms/homeassistant_platform.py
import json
import os
import asyncio
import logging
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

from helpers import LLMClientWrapper, parse_function_json, get_tater_name
from plugin_registry import plugin_registry

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("homeassistant")

# -------------------- Hard-coded platform constants --------------------
BIND_HOST = "0.0.0.0"

ENABLE_PLUGINS = True
SESSION_HISTORY_MAX = 6            # recent turns kept per HA conversation_id
TIMEOUT_MS = 60_000               # LLM request timeout (ms)
MAX_HISTORY_CAP = 20               # safety clamp
SESSION_TTL_SECONDS = 2 * 60 * 60  # auto-expire session history after 2h

# Redis (history + plugin toggles only — NOT for LLM settings)
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

PLATFORM_SETTINGS = {
    "category": "Home Assistant Settings",
    "required": {
        "bind_port": {
            "label": "Bind Port",
            "type": "number",
            "default": 8787,
            "description": "TCP port for the Tater ↔ HA bridge"
        }
    }
}

# -------------------- FastAPI DTOs --------------------
class HARequest(BaseModel):
    text: str
    user_id: Optional[str] = None
    device_id: Optional[str] = None
    area_id: Optional[str] = None
    session_id: Optional[str] = None  # Usually HA's conversation_id

class HAResponse(BaseModel):
    response: str

# -------------------- Plugin gating --------------------
def get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")

# -------------------- System prompt (Discord/IRC style, HA scoped) --------------------
def build_system_prompt() -> str:
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    first, last = get_tater_name()

    base_prompt = (
        f"You are {first} {last}, a Home Assistant–savvy AI assistant with access to various tools and plugins.\n\n"
        "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else):\n\n"
    )

    # Only show enabled tools usable on HA, and only those that actually implement handle_homeassistant
    tool_instructions = "\n\n".join(
        f"Tool: {plugin.name}\n"
        f"Description: {getattr(plugin, 'description', 'No description provided.')}\n"
        f"{plugin.usage}"
        for plugin in plugin_registry.values()
        if (("homeassistant" in getattr(plugin, "platforms", [])) or ("both" in getattr(plugin, "platforms", [])))
        and get_plugin_enabled(plugin.name)
        and hasattr(plugin, "handle_homeassistant")
    )

    behavior_guard = (
        "Only call a tool if the user's latest message clearly requests an action — such as 'turn on', "
        "'set', 'generate', 'summarize', or 'download'.\n"
        "Never call a tool in response to casual or friendly messages like 'thanks', 'lol', or 'cool' — reply normally instead.\n"
        "Keep normal (non-tool) replies brief and TTS-friendly.\n"
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"{base_prompt}\n\n"
        f"{tool_instructions}\n\n"
        f"{behavior_guard}"
        "If no function is needed, reply normally."
    )

# -------------------- History shaping (Discord-style alternation) --------------------
def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    # Skip plugin_response markers from prior turns (they're side-effects)
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        return None

    # For plugin_call, stringify so the LLM "sees" the previous structured action
    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps({
            "function": content.get("plugin"),
            "arguments": content.get("arguments", {})
        }, indent=2)
        return {"role": "assistant", "content": as_text}

    if isinstance(content, str):
        return {"role": role, "content": content}

    return {"role": role, "content": str(content)}

def _enforce_user_assistant_alternation(loop_messages: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    merged: list[Dict[str, Any]] = []
    for m in loop_messages:
        if not m:
            continue
        if not merged:
            merged.append(m); continue
        if merged[-1]["role"] == m["role"]:
            a, b = merged[-1]["content"], m["content"]
            if isinstance(a, str) and isinstance(b, str):
                merged[-1]["content"] = (a + "\n\n" + b).strip()
            else:
                merged[-1]["content"] = (str(a) + "\n\n" + str(b)).strip()
        else:
            merged.append(m)
    if merged and merged[0]["role"] != "user":
        merged.insert(0, {"role": "user", "content": ""})
    return merged

# -------------------- Redis history helpers --------------------
def _sess_key(session_id: Optional[str]) -> str:
    return f"tater:ha:session:{session_id or 'default'}:history"

async def _load_history(session_id: Optional[str], limit: int) -> list[Dict[str, Any]]:
    key = _sess_key(session_id)
    raw = redis_client.lrange(key, -limit, -1)
    loop_messages: list[Dict[str, Any]] = []
    for entry in raw:
        try:
            obj = json.loads(entry)
            role = obj.get("role", "user")
            content = obj.get("content")
            if role not in ("user", "assistant"):
                role = "assistant"
            templ = _to_template_msg(role, content)
            if templ is not None:
                loop_messages.append(templ)
        except Exception:
            continue
    return _enforce_user_assistant_alternation(loop_messages)

async def _save_message(session_id: Optional[str], role: str, content: Any, max_store: int):
    key = _sess_key(session_id)
    pipe = redis_client.pipeline()
    pipe.rpush(key, json.dumps({"role": role, "content": content}))
    if max_store > 0:
        pipe.ltrim(key, -max_store, -1)
    # optional TTL so old voice sessions clean themselves up
    pipe.expire(key, SESSION_TTL_SECONDS)
    pipe.execute()

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
            return json.dumps(res)
        except Exception:
            return str(res)
    return str(res)

# -------------------- App + LLM client --------------------
app = FastAPI(title="Tater Home Assistant Bridge", version="1.3")
_llm: Optional[LLMClientWrapper] = None

@app.on_event("startup")
async def _on_startup():
    global _llm
    llm_host = os.getenv("LLM_HOST", "127.0.0.1")
    llm_port = os.getenv("LLM_PORT", "11434")
    _llm = LLMClientWrapper(host=f"http://{llm_host}:{llm_port}")
    logger.info(f"[HA Bridge] LLM client → http://{llm_host}:{llm_port}")

@app.get("/tater-ha/v1/health")
async def health():
    return {"ok": True, "version": "1.3"}

@app.post("/tater-ha/v1/message", response_model=HAResponse)
async def handle_message(payload: HARequest):
    """
    Home Assistant bridge:
    - Builds a Discord/IRC-style system prompt (HA-scoped)
    - Shapes loop history
    - Executes ONLY plugins that implement handle_homeassistant
    - Normalizes results to simple TTS-friendly text
    """
    if _llm is None:
        raise HTTPException(status_code=503, detail="LLM backend not initialized")

    text_in = (payload.text or "").strip()
    if not text_in:
        return HAResponse(response="(no text provided)")

    history_max = min(max(SESSION_HISTORY_MAX, 0), MAX_HISTORY_CAP)

    # Save the user turn (raw)
    await _save_message(payload.session_id, "user", text_in, history_max)

    # Build the messages list: system + shaped history
    system_prompt = build_system_prompt()
    loop_messages = await _load_history(payload.session_id, history_max)
    messages_list = [{"role": "system", "content": system_prompt}] + loop_messages

    try:
        # Ask the LLM
        response = await _llm.chat(messages_list, timeout_ms=TIMEOUT_MS)
        text = (response.get("message", {}) or {}).get("content", "") if isinstance(response, dict) else ""

        if not text:
            await _save_message(payload.session_id, "assistant", "", history_max)
            return HAResponse(response="Sorry, I didn't catch that.")

        # Detect tool call JSON
        fn = parse_function_json(text) if ENABLE_PLUGINS else None
        if fn:
            func = fn.get("function")
            args = fn.get("arguments", {}) or {}

            # Save structured plugin_call marker (for continuity with Discord template style)
            await _save_message(
                payload.session_id,
                "assistant",
                {"marker": "plugin_call", "plugin": func, "arguments": args},
                history_max
            )

            plugin = plugin_registry.get(func)
            # Only allow enabled plugins, usable on Home Assistant, AND implementing handle_homeassistant
            is_ha_plugin = plugin and (
                ("homeassistant" in getattr(plugin, "platforms", [])) or ("both" in getattr(plugin, "platforms", []))
            )
            if not plugin or not get_plugin_enabled(func) or not is_ha_plugin:
                msg = f"Function `{func}` is not available for Home Assistant."
                await _save_message(payload.session_id, "assistant", msg, history_max)
                return HAResponse(response=msg)

            if not hasattr(plugin, "handle_homeassistant"):
                msg = f"Function `{func}` is not supported in this platform."
                await _save_message(payload.session_id, "assistant", msg, history_max)
                return HAResponse(response=msg)

            try:
                result = await plugin.handle_homeassistant(args, _llm)

                final_text = _flatten_to_text(result).strip() or f"Done with {func}."
                if len(final_text) > 4000:
                    final_text = final_text[:4000] + "…"

                # Save plugin_response marker (not fed back to LLM; for traceability)
                await _save_message(
                    payload.session_id,
                    "assistant",
                    {"marker": "plugin_response", "content": final_text},
                    history_max
                )
                return HAResponse(response=final_text)

            except Exception:
                logger.exception(f"[HA Bridge] Plugin '{func}' error")
                msg = f"I tried to run {func} but hit an error."
                await _save_message(payload.session_id, "assistant", msg, history_max)
                return HAResponse(response=msg)

        # Plain text answer
        final_text = text.strip()
        if len(final_text) > 4000:
            final_text = final_text[:4000] + "…"
        await _save_message(payload.session_id, "assistant", final_text, history_max)
        return HAResponse(response=final_text)

    except Exception:
        logger.exception("[HA Bridge] LLM error")
        msg = "Sorry, I ran into a problem processing that."
        await _save_message(payload.session_id, "assistant", msg, history_max)
        return HAResponse(response=msg)

def run(stop_event: Optional[threading.Event] = None):
    """Match your other platforms’ run signature and graceful stop behavior."""
    # Pull port from Redis settings (set via WebUI) with a safe fallback
    raw_port = redis_client.hget("homeassistant_platform_settings", "bind_port")
    try:
        port = int(raw_port) if raw_port is not None else 8787
    except (TypeError, ValueError):
        logger.warning(f"[HA Bridge] Invalid bind_port value '{raw_port}', defaulting to 8787")
        port = 8787

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
    logger.info(f"[HA Bridge] Listening on http://{BIND_HOST}:{port}")