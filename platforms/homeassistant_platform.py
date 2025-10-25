# platforms/homeassistant_platform.py
import json
import os
import asyncio
import logging
import threading
import time
from pydantic import Field
from datetime import datetime
from typing import Optional, Dict, Any, List
import redis
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import uvicorn
import requests
import re

from helpers import LLMClientWrapper, parse_function_json, get_tater_name
from plugin_registry import plugin_registry

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("homeassistant")

# -------------------- Platform defaults (overridable in WebUI) --------------------
BIND_HOST = "0.0.0.0"
ENABLE_PLUGINS = True
TIMEOUT_SECONDS = 60  # LLM request timeout in seconds

# Defaults; users can override these in WebUI platform settings
DEFAULT_SESSION_HISTORY_MAX = 6
DEFAULT_MAX_HISTORY_CAP = 20
DEFAULT_SESSION_TTL_SECONDS = 2 * 60 * 60  # 2h

# Redis (history + plugin toggles + notifications)
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# Notification keys
REDIS_NOTIF_LIST = "tater:ha:notifications"  # LPUSH new, LRANGE read, then clear

PLATFORM_SETTINGS = {
    "category": "Home Assistant Settings",
    "required": {
        "bind_port": {
            "label": "Bind Port",
            "type": "number",
            "default": 8787,
            "description": "TCP port for the Tater ↔ HA bridge"
        },

        # --- NEW History and TTL controls ---
        "SESSION_HISTORY_MAX": {
            "label": "Session History (turns)",
            "type": "number",
            "default": DEFAULT_SESSION_HISTORY_MAX,
            "description": "How many recent turns to include per HA conversation (smaller = faster)."
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
            "description": "How long to keep a voice session’s history alive (5m–24h)."
        },

        # --- Existing Voice PE fields ---
        "VOICE_PE_ENTITY_1": {
            "label": "Voice PE entity #1",
            "type": "string",
            "default": "",
            "description": "Entity ID of a Voice PE light/LED (e.g., light.voice_pe_office)"
        },
        "VOICE_PE_ENTITY_2": {
            "label": "Voice PE entity #2",
            "type": "string",
            "default": "",
            "description": "Entity ID of a Voice PE light/LED (e.g., light.voice_pe_office)"
        },
        "VOICE_PE_ENTITY_3": {
            "label": "Voice PE entity #3",
            "type": "string",
            "default": "",
            "description": "Entity ID of a Voice PE light/LED (e.g., light.voice_pe_office)"
        },
        "VOICE_PE_ENTITY_4": {
            "label": "Voice PE entity #4",
            "type": "string",
            "default": "",
            "description": "Entity ID of a Voice PE light/LED (e.g., light.voice_pe_office)"
        },
        "VOICE_PE_ENTITY_5": {
            "label": "Voice PE entity #5",
            "type": "string",
            "default": "",
            "description": "Entity ID of a Voice PE light/LED (e.g., light.voice_pe_office)"
        },
    }
}


# --- Duration parsing (supports "5m", "2h", "24h", or raw seconds like "7200") ---
def _parse_duration_seconds(val: str, default_seconds: int) -> int:
    if val is None:
        return default_seconds
    s = str(val).strip().lower()
    # raw integer seconds?
    try:
        return int(s)
    except ValueError:
        pass
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
class HARequest(BaseModel):
    text: str
    user_id: Optional[str] = None
    device_id: Optional[str] = None
    area_id: Optional[str] = None
    session_id: Optional[str] = None  # Usually HA's conversation_id

class HAResponse(BaseModel):
    response: str

# Notifications DTOs
class NotificationIn(BaseModel):
    source: str = Field(..., description="Logical source/plugin, e.g., 'doorbell_alert'")
    title: str = Field(..., description="Short notification title")
    type: Optional[str] = Field(None, description="Category: doorbell, motion, etc.")
    message: Optional[str] = Field(None, description="Human-readable notification body")
    entity_id: Optional[str] = Field(None, description="Primary HA entity related to this notification")
    ha_time: Optional[str] = Field(None, description="Timestamp string provided by HA (e.g., sensor.date_time_iso)")
    level: Optional[str] = Field("info", description="info|warn|error (free-form)")
    data: Optional[Dict[str, Any]] = Field(None, description="Arbitrary structured extras")

class NotificationsOut(BaseModel):
    notifications: List[Dict[str, Any]]

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
        "Never call a tool in response to casual or friendly messages like 'thanks', 'lol', or 'cool'.\n"
        "Keep normal (non-tool) replies brief and TTS-friendly.\n"
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"{base_prompt}\n\n"
        f"{tool_instructions}\n\n"
        f"{behavior_guard}"
        "If no function is needed, reply normally. Do not use emoji's in your reply"
    )

# -------------------- History shaping (Discord-style alternation) --------------------
def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    # --- Skip waiting lines from tools (future-proof) ---
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    # --- Include final plugin responses in context (defaults to final if missing for backward compat) ---
    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        phase = content.get("phase", "final")
        if phase != "final":
            return None
        payload = content.get("content", "")

        # We mostly store a string here already (thanks to _flatten_to_text).
        if isinstance(payload, str):
            txt = payload.strip()
            if len(txt) > 4000:
                txt = txt[:4000] + " …"
            return {"role": "assistant", "content": txt}

        # Fallback: compact any structured payload to JSON string
        try:
            compact = json.dumps(payload, ensure_ascii=False)
            if len(compact) > 2000:
                compact = compact[:2000] + " …"
            return {"role": "assistant", "content": compact}
        except Exception:
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

# -------------------- Minimal HA client (platform local) --------------------
class _HA:
    def __init__(self):
        # Reuse Home Assistant plugin settings for base URL & token
        s = redis_client.hgetall("plugin_settings: Home Assistant") or redis_client.hgetall("plugin_settings:Home Assistant")
        self.base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        token = s.get("HA_TOKEN")
        if not token:
            raise ValueError("HA_TOKEN missing in 'Home Assistant' plugin settings.")
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _req(self, method: str, path: str, json=None, timeout=10):
        r = requests.request(method, f"{self.base}{path}", headers=self.headers, json=json, timeout=timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
        try:
            return r.json()
        except Exception:
            return r.text

    def get_state(self, entity_id: str):
        return self._req("GET", f"/api/states/{entity_id}")

    def call_service(self, domain: str, service: str, data: dict):
        return self._req("POST", f"/api/services/{domain}/{service}", json=data)

# -------------------- Voice PE helpers --------------------
def _platform_settings() -> Dict[str, str]:
    return redis_client.hgetall("homeassistant_platform_settings") or {}

def _voice_pe_entities() -> List[str]:
    s = _platform_settings()
    ids = [
        (s.get("VOICE_PE_ENTITY_1") or "").strip(),
        (s.get("VOICE_PE_ENTITY_2") or "").strip(),
        (s.get("VOICE_PE_ENTITY_3") or "").strip(),
        (s.get("VOICE_PE_ENTITY_4") or "").strip(),
        (s.get("VOICE_PE_ENTITY_5") or "").strip(),
    ]
    return [e for e in ids if e]

def _ring_on():
    """Turn ON all configured Voice PE LED entities."""
    ents = _voice_pe_entities()
    if not ents:
        return
    ha = _HA()
    for eid in ents:
        try:
            ha.call_service("light", "turn_on", {"entity_id": eid})
        except Exception as e:
            logger.warning(f"[notify] failed to turn on ring {eid}: {e}")

def _ring_off():
    """Turn OFF all configured Voice PE LED entities."""
    ents = _voice_pe_entities()
    if not ents:
        return
    ha = _HA()
    for eid in ents:
        try:
            ha.call_service("light", "turn_off", {"entity_id": eid})
        except Exception as e:
            logger.warning(f"[notify] failed to turn off ring {eid}: {e}")

# -------------------- App + LLM client --------------------
app = FastAPI(title="Tater Home Assistant Bridge", version="1.5")  # 🔼 simplified ring logic

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
    return {"ok": True, "version": "1.5"}

# -------------------- Notifications API --------------------
@app.post("/tater-ha/v1/notifications/add")
async def add_notification(n: NotificationIn):
    item = {
        "source": n.source.strip(),
        "title": n.title.strip(),
        "type": (n.type or "").strip(),
        "message": (n.message or "").strip(),
        "entity_id": (n.entity_id or "").strip(),
        "ha_time": (n.ha_time or "").strip(),
        "level": (n.level or "info").strip(),
        "data": n.data or {},
        "ts": int(time.time()),  # server epoch seconds
    }

    # Push newest first
    redis_client.lpush(REDIS_NOTIF_LIST, json.dumps(item))

    # Light up rings
    try:
        _ring_on()
    except Exception:
        logger.exception("[notify] ring_on failed")

    return {"ok": True, "queued": True}

@app.get("/tater-ha/v1/notifications", response_model=NotificationsOut)
async def pull_notifications(background_tasks: BackgroundTasks):
    """
    Return all pending notifications (most recent first) and clear the queue.
    Turn the rings OFF afterward (async) or immediately if queue is empty.
    """
    raw = redis_client.lrange(REDIS_NOTIF_LIST, 0, -1) or []
    notifications = []
    for r in raw:
        try:
            notifications.append(json.loads(r))
        except Exception:
            continue

    # Clear the queue
    try:
        redis_client.delete(REDIS_NOTIF_LIST)
    except Exception:
        logger.warning("[notify] failed to clear notification list")

    # If there were any notifications, turn rings OFF asynchronously
    if notifications:
        background_tasks.add_task(_ring_off)
    else:
        # No notifications: ensure ring is off right away
        try:
            _ring_off()
        except Exception:
            logger.exception("[notify] ring_off failed")

    return {"notifications": notifications}

# -------------------- Main HA chat endpoint (unchanged logic) --------------------
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

    session_history_max = _get_int_platform_setting("SESSION_HISTORY_MAX", DEFAULT_SESSION_HISTORY_MAX)
    max_history_cap = _get_int_platform_setting("MAX_HISTORY_CAP", DEFAULT_MAX_HISTORY_CAP)
    history_max = min(max(session_history_max, 0), max_history_cap)

    # Save the user turn (raw)
    await _save_message(payload.session_id, "user", text_in, history_max)

    # Build the messages list: system + shaped history
    system_prompt = build_system_prompt()
    loop_messages = await _load_history(payload.session_id, history_max)
    messages_list = [{"role": "system", "content": system_prompt}] + loop_messages

    try:
        # Ask the LLM
        response = await _llm.chat(messages_list, timeout=TIMEOUT_SECONDS)
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
                    {"marker": "plugin_response", "phase": "final", "content": final_text},
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