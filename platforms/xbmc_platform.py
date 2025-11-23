# platforms/xbmc_platform.py
import json
import os
import asyncio
import logging
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

from dotenv import load_dotenv
load_dotenv()

from helpers import LLMClientWrapper, parse_function_json, get_tater_name
from plugin_registry import plugin_registry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("xbmc")

# -------------------- Platform defaults (overridable in WebUI) --------------------
BIND_HOST = "0.0.0.0"
ENABLE_PLUGINS = True
TIMEOUT_SECONDS = 60

DEFAULT_SESSION_HISTORY_MAX = 6
DEFAULT_MAX_HISTORY_CAP = 20
DEFAULT_SESSION_TTL_SECONDS = 2 * 60 * 60  # 2h

# Redis (history + platform settings)
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

PLATFORM_SETTINGS = {
    "category": "XBMC / Original Xbox Settings",
    "required": {
        "bind_port": {
            "label": "Bind Port",
            "type": "number",
            "default": 8790,
            "description": "TCP port for the Tater ↔ XBMC bridge"
        },
        "SESSION_HISTORY_MAX": {
            "label": "Session History (turns)",
            "type": "number",
            "default": DEFAULT_SESSION_HISTORY_MAX,
            "description": "How many recent turns to include per XBMC conversation (smaller = faster)."
        },
        "MAX_HISTORY_CAP": {
            "label": "Max History Cap",
            "type": "number",
            "default": DEFAULT_MAX_HISTORY_CAP,
            "description": "Hard ceiling to prevent runaway context sizes."
        },
        "SESSION_TTL_SECONDS": {
            "label": "Session TTL",
            "type": "select",
            "options": ["5m", "30m", "1h", "2h", "6h", "24h"],
            "default": "2h",
            "description": "How long to keep an XBMC session’s history alive (5m–24h)."
        },
    }
}

# -------------------- Settings helpers --------------------
def _platform_settings() -> Dict[str, str]:
    return redis_client.hgetall("xbmc_platform_settings") or {}

def _parse_duration_seconds(val: str, default_seconds: int) -> int:
    if val is None:
        return default_seconds
    s = str(val).strip().lower()
    # raw integer seconds?
    try:
        return int(s)
    except ValueError:
        pass
    import re
    m = re.match(r"^\s*(\d+)\s*([smhd])\s*$", s)
    if not m:
        return default_seconds
    num = int(m.group(1))
    unit = m.group(2)
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return num * mult

def _get_duration_seconds_setting(name: str, default_seconds: int) -> int:
    s = _platform_settings().get(name)
    return _parse_duration_seconds(s, default_seconds)

def _get_int_platform_setting(name: str, default: int) -> int:
    s = _platform_settings().get(name)
    try:
        return int(str(s).strip()) if s is not None and str(s).strip() != "" else default
    except Exception:
        return default

# -------------------- FastAPI DTOs --------------------
class XBMCRequest(BaseModel):
    text: str
    user_id: Optional[str] = None
    device_id: Optional[str] = None
    area_id: Optional[str] = None
    session_id: Optional[str] = None  # we use this for Redis key

class XBMCResponse(BaseModel):
    response: str

# -------------------- System prompt (XBMC / Cortana) --------------------
def build_system_prompt() -> str:
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    first, last = get_tater_name()

    base_prompt = (
        f"You are {first} {last}, the core AI assistant that powers the multi-platform Tater bot.\n\n"
        "On this platform you are running on an original Xbox using XBMC4Xbox, shown on a TV screen.\n"
        "Here, you MUST roleplay as the Xbox assistant named 'Cortana':\n"
        "- Introduce yourself as Cortana.\n"
        "- Refer to yourself as Cortana in casual conversation.\n"
        "- Keep answers short, readable, and friendly — ideal for a TV at a distance.\n"
        "- Avoid long walls of text; aim for 1–3 short paragraphs at most.\n\n"
        "However, if the user explicitly asks who you REALLY are, what your REAL name is, or mentions 'Tater', "
        f"you should honestly explain that your real name is {first} {last} (nicknamed Tater), and you are just "
        "pretending to be Cortana on this original Xbox for fun.\n\n"
    )

    if ENABLE_PLUGINS:
        # Only show tools that actually support XBMC (future-proof)
        tool_instructions = "\n\n".join(
            f"Tool: {plugin.name}\n"
            f"Description: {getattr(plugin, 'description', 'No description provided.')}\n"
            f"{plugin.usage}"
            for plugin in plugin_registry.values()
            if (("xbmc" in getattr(plugin, 'platforms', [])) or ("both" in getattr(plugin, 'platforms', [])))
            and hasattr(plugin, "handle_xbmc")
        )

        behavior_guard = (
            "You can optionally call tools for actions like controlling devices, downloading content, or summarizing "
            "external resources. Only call a tool if the user clearly asks for an action; otherwise answer directly.\n"
            "When you DO call a tool, reply ONLY with a JSON object as described in the tool instructions (no extra text).\n"
        )
    else:
        tool_instructions = ""
        behavior_guard = (
            "You do NOT have access to any tools on this platform right now; always answer directly in natural language.\n"
        )

    style_guard = (
        "Avoid emoji and markdown formatting, as the UI is a simple text list.\n"
        "If the user asks for code, keep it as short as reasonably possible.\n"
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"{base_prompt}"
        f"{tool_instructions}\n\n"
        f"{behavior_guard}"
        f"{style_guard}"
    )

# -------------------- History shaping --------------------
def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    # skip explicit plugin wait markers if ever stored
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        phase = content.get("phase", "final")
        if phase != "final":
            return None
        payload = content.get("content", "")
        if isinstance(payload, str):
            txt = payload.strip()
            if len(txt) > 4000:
                txt = txt[:4000] + " …"
            return {"role": "assistant", "content": txt}
        try:
            compact = json.dumps(payload, ensure_ascii=False)
            if len(compact) > 2000:
                compact = compact[:2000] + " …"
            return {"role": "assistant", "content": compact}
        except Exception:
            return None

    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps(
            {"function": content.get("plugin"), "arguments": content.get("arguments", {})},
            indent=2
        )
        return {"role": "assistant", "content": as_text}

    if isinstance(content, str):
        return {"role": role, "content": content}

    return {"role": role, "content": str(content)}

def _enforce_user_assistant_alternation(loop_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for m in loop_messages:
        if not m:
            continue
        if not merged:
            merged.append(m)
            continue
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

# -------------------- Redis history --------------------
def _sess_key(session_id: Optional[str]) -> str:
    return f"tater:xbmc:session:{session_id or 'default'}:history"

async def _load_history(session_id: Optional[str], limit: int) -> List[Dict[str, Any]]:
    key = _sess_key(session_id)
    raw = redis_client.lrange(key, -limit, -1)
    loop_messages: List[Dict[str, Any]] = []
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
    ttl = _get_duration_seconds_setting("SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS)
    pipe.expire(key, ttl)
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
app = FastAPI(title="Tater XBMC Bridge", version="1.0")

_llm: Optional[LLMClientWrapper] = None

@app.on_event("startup")
async def _on_startup():
    global _llm
    llm_host = os.getenv("LLM_HOST", "127.0.0.1")
    llm_port = os.getenv("LLM_PORT", "11434")
    _llm = LLMClientWrapper(host=f"http://{llm_host}:{llm_port}")
    logger.info(f"[XBMC Bridge] LLM client → http://{llm_host}:{llm_port}")

@app.get("/tater-xbmc/v1/health")
async def health():
    return {"ok": True, "version": "1.0"}

# -------------------- Main XBMC chat endpoint --------------------
@app.post("/tater-xbmc/v1/message", response_model=XBMCResponse)
async def handle_message(payload: XBMCRequest):
    """
    XBMC bridge:
    - Builds a Cortana-flavored system prompt, aware it's on OG Xbox / XBMC4Xbox
    - Shapes loop history
    - (Optionally) executes plugins that implement handle_xbmc (currently disabled)
    - Returns simple text for the XBMC script to show
    """
    if _llm is None:
        raise HTTPException(status_code=503, detail="LLM backend not initialized")

    text_in = (payload.text or "").strip()
    if not text_in:
        return XBMCResponse(response="(no text provided)")

    session_history_max = _get_int_platform_setting("SESSION_HISTORY_MAX", DEFAULT_SESSION_HISTORY_MAX)
    max_history_cap = _get_int_platform_setting("MAX_HISTORY_CAP", DEFAULT_MAX_HISTORY_CAP)
    history_max = min(max(session_history_max, 0), max_history_cap)

    # Save user turn
    await _save_message(payload.session_id, "user", text_in, history_max)

    system_prompt = build_system_prompt()
    loop_messages = await _load_history(payload.session_id, history_max)
    messages_list = [{"role": "system", "content": system_prompt}] + loop_messages

    try:
        response = await _llm.chat(messages_list, timeout=TIMEOUT_SECONDS)
        text = (response.get("message", {}) or {}).get("content", "") if isinstance(response, dict) else ""

        if not text:
            await _save_message(payload.session_id, "assistant", "", history_max)
            return XBMCResponse(response="Sorry, I didn't catch that.")

        # In the future, if you enable tools:
        fn = parse_function_json(text) if ENABLE_PLUGINS else None
        if fn and ENABLE_PLUGINS:
            func = fn.get("function")
            args = fn.get("arguments", {}) or {}

            await _save_message(
                payload.session_id,
                "assistant",
                {"marker": "plugin_call", "plugin": func, "arguments": args},
                history_max
            )

            plugin = plugin_registry.get(func)
            is_xbmc_plugin = plugin and (
                ("xbmc" in getattr(plugin, "platforms", [])) or ("both" in getattr(plugin, "platforms", []))
            )
            if not plugin or not is_xbmc_plugin or not hasattr(plugin, "handle_xbmc"):
                msg = f"Function `{func}` is not available for XBMC."
                await _save_message(payload.session_id, "assistant", msg, history_max)
                return XBMCResponse(response=msg)

            try:
                result = await plugin.handle_xbmc(args, _llm)
                final_text = _flatten_to_text(result).strip() or f"Done with {func}."
                if len(final_text) > 4000:
                    final_text = final_text[:4000] + "…"

                await _save_message(
                    payload.session_id,
                    "assistant",
                    {"marker": "plugin_response", "phase": "final", "content": final_text},
                    history_max
                )
                return XBMCResponse(response=final_text)
            except Exception:
                logger.exception(f"[XBMC Bridge] Plugin '{func}' error")
                msg = f"I tried to run {func} but hit an error."
                await _save_message(payload.session_id, "assistant", msg, history_max)
                return XBMCResponse(response=msg)

        # Plain-text answer
        final_text = text.strip()
        if len(final_text) > 4000:
            final_text = final_text[:4000] + "…"
        await _save_message(payload.session_id, "assistant", final_text, history_max)
        return XBMCResponse(response=final_text)

    except Exception:
        logger.exception("[XBMC Bridge] LLM error")
        msg = "Sorry, I ran into a problem processing that."
        await _save_message(payload.session_id, "assistant", msg, history_max)
        return XBMCResponse(response=msg)

# -------------------- Runner (WebUI-style) --------------------
def run(stop_event: Optional[threading.Event] = None):
    """Match other platforms’ run signature and graceful stop behavior."""
    raw_port = redis_client.hget("xbmc_platform_settings", "bind_port")
    try:
        port = int(raw_port) if raw_port is not None else 8790
    except (TypeError, ValueError):
        logger.warning(f"[XBMC Bridge] Invalid bind_port value '{raw_port}', defaulting to 8790")
        port = 8790

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
    logger.info(f"[XBMC Bridge] Listening on http://{BIND_HOST}:{port}")