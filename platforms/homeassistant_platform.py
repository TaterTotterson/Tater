# platforms/homeassistant_platform.py
import json
import os
import asyncio
import logging
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import redis
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import uvicorn
import requests
import re
import inspect
import aiohttp

from helpers import (
    get_llm_client_from_env,
    parse_function_json,
    get_tater_name,
    get_tater_personality,
)
import plugin_registry

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("homeassistant")

# -------------------- Platform defaults (overridable in WebUI) --------------------
BIND_HOST = "0.0.0.0"
TIMEOUT_SECONDS = 60  # LLM request timeout in seconds

# Defaults; users can override these in WebUI platform settings
DEFAULT_SESSION_HISTORY_MAX = 6
DEFAULT_MAX_HISTORY_CAP = 20
DEFAULT_SESSION_TTL_SECONDS = 2 * 60 * 60  # 2h

# Continued chat (auto follow-up) defaults
DEFAULT_CONTINUED_CHAT_ENABLED = False

# Follow-up defaults
DEFAULT_FOLLOWUP_IDLE_TIMEOUT_S = 12.0
DEFAULT_SATELLITE_MAP_CACHE_TTL_S = 3600  # 1h

# Redis (history + plugin toggles + notifications)
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# Notification keys
REDIS_NOTIF_LIST = "tater:ha:notifications"  # LPUSH new, LRANGE read, then clear

# Cache keys
REDIS_SATELLITE_MAP_KEY = "tater:ha:assist_satellite_map:v1"  # json map: area_id -> entity_id

PLATFORM_SETTINGS = {
    "category": "Home Assistant Settings",
    "required": {
        "bind_port": {
            "label": "Bind Port",
            "type": "number",
            "default": 8787,
            "description": "TCP port for the Tater ↔ HA bridge",
        },

        # --- History and TTL controls ---
        "SESSION_HISTORY_MAX": {
            "label": "Session History (turns)",
            "type": "number",
            "default": DEFAULT_SESSION_HISTORY_MAX,
            "description": "How many recent turns to include per HA conversation (smaller = faster).",
        },
        "MAX_HISTORY_CAP": {
            "label": "Max History Cap",
            "type": "number",
            "default": DEFAULT_MAX_HISTORY_CAP,
            "description": "Hard ceiling to prevent runaway context sizes.",
        },
        "SESSION_TTL_SECONDS": {
            "label": "Session TTL",
            "type": "select",
            "options": ["5m", "30m", "1h", "2h", "6h", "24h"],
            "default": "2h",
            "description": "How long to keep a voice session’s history alive (5m–24h).",
        },

        # --- Continued chat toggle ---
        "CONTINUED_CHAT_ENABLED": {
            "label": "Continued chat (auto re-open mic)",
            "type": "select",
            "options": ["true", "false"],
            "default": "false",
            "description": "If enabled, Tater automatically re-opens the Assist satellite mic when it ends with a question.",
        },

        # --- Follow-up behavior ---
        "FOLLOWUP_IDLE_TIMEOUT_S": {
            "label": "Follow-up idle wait (seconds)",
            "type": "number",
            "default": int(DEFAULT_FOLLOWUP_IDLE_TIMEOUT_S),
            "description": "How long to wait for the satellite to return to idle before re-opening the mic.",
        },

        # --- Assist satellite resolution ---
        "SATELLITE_MAP_CACHE_TTL_S": {
            "label": "Assist satellite map cache TTL (seconds)",
            "type": "number",
            "default": DEFAULT_SATELLITE_MAP_CACHE_TTL_S,
            "description": "How long to cache the area→assist_satellite mapping (registry lookups).",
        },

        # --- Existing Voice PE ring fields (optional) ---
        "VOICE_PE_ENTITY_1": {
            "label": "Voice PE entity #1",
            "type": "string",
            "default": "",
            "description": "Entity ID of a Voice PE light/LED (e.g., light.voice_pe_office)",
        },
        "VOICE_PE_ENTITY_2": {
            "label": "Voice PE entity #2",
            "type": "string",
            "default": "",
            "description": "Entity ID of a Voice PE light/LED (e.g., light.voice_pe_office)",
        },
        "VOICE_PE_ENTITY_3": {
            "label": "Voice PE entity #3",
            "type": "string",
            "default": "",
            "description": "Entity ID of a Voice PE light/LED (e.g., light.voice_pe_office)",
        },
        "VOICE_PE_ENTITY_4": {
            "label": "Voice PE entity #4",
            "type": "string",
            "default": "",
            "description": "Entity ID of a Voice PE light/LED (e.g., light.voice_pe_office)",
        },
        "VOICE_PE_ENTITY_5": {
            "label": "Voice PE entity #5",
            "type": "string",
            "default": "",
            "description": "Entity ID of a Voice PE light/LED (e.g., light.voice_pe_office)",
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

def _platform_settings() -> Dict[str, str]:
    return redis_client.hgetall("homeassistant_platform_settings") or {}

def _get_duration_seconds_setting(name: str, default_seconds: int) -> int:
    s = _platform_settings().get(name)
    return _parse_duration_seconds(s, default_seconds)

def _get_int_platform_setting(name: str, default: int) -> int:
    s = _platform_settings().get(name)
    try:
        return int(str(s).strip()) if s is not None and str(s).strip() != "" else default
    except Exception:
        return default

def _get_float_platform_setting(name: str, default: float) -> float:
    s = _platform_settings().get(name)
    try:
        return float(str(s).strip()) if s is not None and str(s).strip() != "" else default
    except Exception:
        return default

def _get_bool_platform_setting(name: str, default: bool) -> bool:
    s = _platform_settings().get(name)
    if s is None:
        return default
    v = str(s).strip().lower()
    if v in ("1", "true", "yes", "y", "on", "enabled"):
        return True
    if v in ("0", "false", "no", "n", "off", "disabled"):
        return False
    return default

# -------------------- FastAPI DTOs --------------------
class HAContext(BaseModel):
    device_id: Optional[str] = None
    device_name: Optional[str] = None
    area_id: Optional[str] = None
    area_name: Optional[str] = None
    language: Optional[str] = None

class HARequest(BaseModel):
    text: str
    user_id: Optional[str] = None
    device_id: Optional[str] = None
    area_id: Optional[str] = None
    session_id: Optional[str] = None  # Usually HA's conversation_id (NOT stable across start_conversation)
    context: Optional[HAContext] = None

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

# -------------------- Stable conversation key (CRITICAL for continued chat) --------------------
def _conv_key(payload: HARequest, ctx: Dict[str, Any]) -> str:
    """
    Home Assistant's conversation_id/session_id can change when we call
    assist_satellite.start_conversation. To keep chat history consistent,
    we use a stable key.

    Priority:
      1) device_id (best for a specific satellite)
      2) ctx.device_id
      3) area_id (room fallback)
      4) session_id (last resort)
      5) default
    """
    did = (payload.device_id or "").strip() or (ctx.get("device_id") or "").strip()
    if did:
        return f"device:{did}"

    aid = (payload.area_id or "").strip() or (ctx.get("area_id") or "").strip()
    if aid:
        return f"area:{aid}"

    sid = (payload.session_id or "").strip()
    if sid:
        return f"session:{sid}"

    return "default"

# -------------------- System prompt (Discord/IRC style, HA scoped) --------------------
def build_system_prompt(ctx: Optional[Dict[str, Any]] = None) -> str:
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    first, last = get_tater_name()
    personality = get_tater_personality()

    # ---- Voice / room context ----
    room_clause = ""
    if ctx:
        area_name = (ctx.get("area_name") or "").strip()
        device_name = (ctx.get("device_name") or "").strip()
        if area_name or device_name:
            room_clause = (
                "VOICE CONTEXT:\n"
                f"- Device: {device_name or '(unknown)'}\n"
                f"- Area/Room: {area_name or '(unknown)'}\n\n"
                "DEFAULT ROOM RULE:\n"
                "If the user asks to control lights/switches/fans/etc and does NOT specify a room, "
                "assume they mean the Area/Room shown above.\n\n"
            )

    persona_clause = ""
    if personality:
        persona_clause = (
            f"You should speak and behave like {personality} "
            "while still being helpful, concise, and easy to understand. "
            "Keep the style subtle rather than over-the-top. "
            "Even while staying in character, you must strictly follow the tool-calling rules below.\n\n"
        )

    base_prompt = (
        f"You are {first} {last}, a Home Assistant–savvy AI assistant with access to various tools and plugins.\n\n"
        f"{persona_clause}"
        f"{room_clause}"
        "When a user requests one of these actions, reply ONLY with a JSON object in one of the following formats (and nothing else):\n\n"
    )

    available_plugins = [
        plugin for plugin in plugin_registry.plugin_registry.values()
        if (("homeassistant" in getattr(plugin, "platforms", [])) or ("both" in getattr(plugin, "platforms", [])))
        and get_plugin_enabled(plugin.name)
        and hasattr(plugin, "handle_homeassistant")
    ]
    logger.debug(
        "[Home Assistant] Number of plugins visible: %s | Number of enabled tools: %s",
        len(plugin_registry.plugin_registry),
        len(available_plugins),
    )
    tool_instructions = "\n\n".join(
        f"Tool: {plugin.name}\n"
        f"Description: {getattr(plugin, 'description', 'No description provided.')}\n"
        f"{plugin.usage}"
        for plugin in available_plugins
    )

    behavior_guard = (
        "Only call a tool if the user's latest message clearly requests an action — such as 'generate', 'summarize', or 'download'.\n"
        "Never call a tool in response to casual or friendly messages like 'thanks', 'lol', or 'cool' — reply normally instead.\n"
    )

    no_emoji_guard = (
        "IMPORTANT:\n"
        "- Do not use emojis.\n"
        "- Do not include markdown formatting.\n"
        "- Keep replies concise and easy to understand.\n"
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"{base_prompt}\n\n"
        f"{tool_instructions}\n\n"
        f"{behavior_guard}\n"
        f"{no_emoji_guard}"
        "If no function is needed, reply normally."
    )

# -------------------- History shaping (Discord-style alternation) --------------------
def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    # Skip waiting lines from tools
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    # Do NOT inject HA context markers into the LLM prompt
    if isinstance(content, dict) and content.get("marker") == "ha_context":
        return None

    # Include final plugin responses in context
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

    # Represent plugin calls as plain text (so history still makes sense)
    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps(
            {"function": content.get("plugin"), "arguments": content.get("arguments", {})},
            indent=2,
        )
        return {"role": "assistant", "content": as_text}

    # Text + fallback
    if isinstance(content, str):
        return {"role": role, "content": content}

    return {"role": role, "content": str(content)}

def _enforce_user_assistant_alternation(loop_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep the history compact and readable by merging consecutive messages
    with the same role.

    IMPORTANT:
    We intentionally do NOT insert a blank user message at the beginning.
    Some LLM backends/models can respond with empty completions when they
    see an empty user turn (content="").
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
            if isinstance(a, str) and isinstance(b, str):
                merged[-1]["content"] = (a + "\n\n" + b).strip()
            else:
                merged[-1]["content"] = (str(a) + "\n\n" + str(b)).strip()
        else:
            merged.append(m)

    return merged

# -------------------- Redis history + context helpers --------------------
def _sess_key(conv_key: str) -> str:
    return f"tater:ha:session:{conv_key}:history"

def _ctx_key(conv_key: str) -> str:
    return f"tater:ha:session:{conv_key}:ctx"

def _load_ctx(conv_key: str) -> Dict[str, Any]:
    raw = redis_client.get(_ctx_key(conv_key))
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

def _save_ctx(conv_key: str, ctx: Dict[str, Any]) -> None:
    if not ctx:
        return
    ttl = _get_duration_seconds_setting("SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS)
    try:
        redis_client.setex(_ctx_key(conv_key), ttl, json.dumps(ctx))
    except Exception:
        pass

async def _load_history(conv_key: str, limit: int) -> List[Dict[str, Any]]:
    key = _sess_key(conv_key)
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

async def _save_message(conv_key: str, role: str, content: Any, max_store: int):
    key = _sess_key(conv_key)
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

# -------------------- Minimal HA client (platform local) --------------------
class _HA:
    def __init__(self):
        # Prefer shared Home Assistant settings first
        shared = redis_client.hgetall("homeassistant_settings") or {}
        base = (shared.get("HA_BASE_URL") or "").strip()
        token = (shared.get("HA_TOKEN") or "").strip()

        # Backward-compatible fallback (legacy storage)
        if not base or not token:
            legacy = (
                redis_client.hgetall("plugin_settings: Home Assistant")
                or redis_client.hgetall("plugin_settings:Home Assistant")
                or {}
            )
            base = base or (legacy.get("HA_BASE_URL") or "").strip()
            token = token or (legacy.get("HA_TOKEN") or "").strip()

        self.base = (base or "http://homeassistant.local:8123").rstrip("/")
        if not token:
            raise ValueError("HA_TOKEN missing in Home Assistant settings.")
        self.token = token
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _req(self, method: str, path: str, json_body=None, timeout=10):
        r = requests.request(method, f"{self.base}{path}", headers=self.headers, json=json_body, timeout=timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
        try:
            return r.json()
        except Exception:
            return r.text

    def get_state(self, entity_id: str):
        return self._req("GET", f"/api/states/{entity_id}")

    def call_service(self, domain: str, service: str, data: dict, return_response: bool = False):
        qs = "?return_response=true" if return_response else ""
        return self._req("POST", f"/api/services/{domain}/{service}{qs}", json_body=data)

    def ws_url(self) -> str:
        # http://host:8123 -> ws://host:8123
        if self.base.startswith("https://"):
            return self.base.replace("https://", "wss://", 1) + "/api/websocket"
        return self.base.replace("http://", "ws://", 1) + "/api/websocket"

# -------------------- Voice PE ring helpers --------------------
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
    ents = _voice_pe_entities()
    if not ents:
        return
    ha = _HA()
    for eid in ents:
        try:
            ha.call_service("light", "turn_off", {"entity_id": eid})
        except Exception as e:
            logger.warning(f"[notify] failed to turn off ring {eid}: {e}")

# -------------------- Assist satellite resolver (area→entity) --------------------
_satellite_refresh_lock = asyncio.Lock()
_satellite_map_mem: Dict[str, str] = {}
_satellite_map_mem_ts: float = 0.0

async def _ha_ws_call(session: aiohttp.ClientSession, ws: aiohttp.ClientWebSocketResponse, msg: dict, expect_id: int, timeout: float = 20.0) -> Any:
    await ws.send_json(msg)
    end = time.time() + timeout
    while time.time() < end:
        m = await ws.receive(timeout=timeout)
        if m.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(m.data)
            if data.get("type") == "result" and data.get("id") == expect_id:
                if not data.get("success"):
                    raise RuntimeError(f"HA WS call failed: {data}")
                return data.get("result")
        elif m.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
            break
    raise TimeoutError("Timed out waiting for HA WS result")

async def _fetch_satellite_map_from_ha() -> Dict[str, str]:
    """
    Build a map of:
      area_id -> assist_satellite.entity_id
    using HA entity registry + device registry.
    """
    ha = _HA()
    ws_url = ha.ws_url()

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url, heartbeat=30) as ws:
            # auth_required
            first = await ws.receive(timeout=10)
            if first.type != aiohttp.WSMsgType.TEXT:
                raise RuntimeError("Unexpected HA WS message during auth_required")
            hello = json.loads(first.data)
            if hello.get("type") != "auth_required":
                raise RuntimeError(f"Unexpected HA WS hello: {hello}")

            await ws.send_json({"type": "auth", "access_token": ha.token})
            auth_resp = await ws.receive(timeout=10)
            if auth_resp.type != aiohttp.WSMsgType.TEXT:
                raise RuntimeError("Unexpected HA WS message during auth")
            auth_data = json.loads(auth_resp.data)
            if auth_data.get("type") != "auth_ok":
                raise RuntimeError(f"HA WS auth failed: {auth_data}")

            entity_reg = await _ha_ws_call(
                session, ws,
                {"id": 1, "type": "config/entity_registry/list"},
                expect_id=1,
                timeout=30,
            )
            device_reg = await _ha_ws_call(
                session, ws,
                {"id": 2, "type": "config/device_registry/list"},
                expect_id=2,
                timeout=30,
            )

    # device_id -> area_id
    dev_area: Dict[str, str] = {}
    if isinstance(device_reg, list):
        for d in device_reg:
            try:
                did = d.get("id")
                aid = d.get("area_id")
                if did and aid:
                    dev_area[str(did)] = str(aid)
            except Exception:
                continue

    # area_id -> assist_satellite entity_id
    area_sat: Dict[str, str] = {}
    if isinstance(entity_reg, list):
        for e in entity_reg:
            try:
                ent = (e.get("entity_id") or "").strip()
                if not ent.startswith("assist_satellite."):
                    continue
                did = e.get("device_id")
                if not did:
                    continue
                aid = dev_area.get(str(did))
                if not aid:
                    continue
                if aid not in area_sat:
                    area_sat[aid] = ent
            except Exception:
                continue

    return area_sat

def _satellite_cache_ttl_s() -> int:
    return _get_int_platform_setting("SATELLITE_MAP_CACHE_TTL_S", DEFAULT_SATELLITE_MAP_CACHE_TTL_S)

def _load_satellite_map_from_redis() -> Tuple[Dict[str, str], bool]:
    """
    Returns (map, ok). ok=False means missing/invalid.
    """
    try:
        raw = redis_client.get(REDIS_SATELLITE_MAP_KEY)
        if not raw:
            return {}, False
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return {}, False
        clean = {str(k): str(v) for k, v in obj.items() if k and v}
        return clean, True
    except Exception:
        return {}, False

def _save_satellite_map_to_redis(m: Dict[str, str]) -> None:
    try:
        ttl = _satellite_cache_ttl_s()
        redis_client.setex(REDIS_SATELLITE_MAP_KEY, ttl, json.dumps(m))
    except Exception:
        pass

async def _get_satellite_map(force_refresh: bool = False) -> Dict[str, str]:
    """
    Uses:
      memory -> redis -> HA WS
    """
    global _satellite_map_mem, _satellite_map_mem_ts

    ttl = float(_satellite_cache_ttl_s())

    if not force_refresh and _satellite_map_mem and (time.time() - _satellite_map_mem_ts) < ttl:
        return _satellite_map_mem

    async with _satellite_refresh_lock:
        if not force_refresh and _satellite_map_mem and (time.time() - _satellite_map_mem_ts) < ttl:
            return _satellite_map_mem

        if not force_refresh:
            cached, ok = _load_satellite_map_from_redis()
            if ok and cached:
                _satellite_map_mem = cached
                _satellite_map_mem_ts = time.time()
                return cached

        try:
            fresh = await _fetch_satellite_map_from_ha()
            if fresh:
                _satellite_map_mem = fresh
                _satellite_map_mem_ts = time.time()
                _save_satellite_map_to_redis(fresh)
                logger.info(f"[followup] refreshed assist satellite map ({len(fresh)} areas)")
                return fresh
        except Exception as e:
            logger.warning(f"[followup] failed refreshing satellite map: {e}")

        cached, ok = _load_satellite_map_from_redis()
        if ok:
            _satellite_map_mem = cached
            _satellite_map_mem_ts = time.time()
            return cached

        return {}

async def _resolve_assist_satellite_entity(ctx: Dict[str, Any]) -> Optional[str]:
    if not ctx:
        return None
    area_id = (ctx.get("area_id") or "").strip()
    if not area_id:
        return None

    m = await _get_satellite_map(force_refresh=False)
    ent = (m.get(area_id) or "").strip()
    if ent:
        return ent

    m2 = await _get_satellite_map(force_refresh=True)
    ent2 = (m2.get(area_id) or "").strip()
    return ent2 or None

async def _wait_for_satellite_idle(entity_id: str, timeout_s: float) -> bool:
    idle_like = {"idle", "off", "ready", "standby"}
    busy_like = {"listening", "processing", "responding", "speaking", "replying", "playing", "on"}

    ha = _HA()
    end = time.time() + max(1.0, timeout_s)

    while time.time() < end:
        try:
            st = ha.get_state(entity_id)
            state = str(st.get("state") or "").strip().lower()

            if state in ("unknown", "unavailable", ""):
                await asyncio.sleep(0.35)
                continue

            if state in idle_like:
                return True

            if state in busy_like:
                await asyncio.sleep(0.35)
                continue

            if state not in busy_like:
                return True

        except Exception:
            await asyncio.sleep(0.35)

    return False

async def _generate_followup_question(assistant_text: str) -> str:
    """
    Generate a VERY short spoken follow-up cue for start_conversation.start_message.

    IMPORTANT:
    - This MUST NOT be a question (no '?'), to avoid creating weird loops.
    - This text is NOT saved into Redis history and is NOT passed to _should_follow_up().
    """
    t = (assistant_text or "").strip()
    tail = t[-300:] if len(t) > 300 else t

    prompt = (
        "Write a very short spoken cue that tells the user the mic is open.\n"
        "Rules:\n"
        "- 1 to 4 words\n"
        "- Exactly one sentence fragment (no extra punctuation)\n"
        "- MUST NOT be a question (do not use '?')\n"
        "- MUST NOT include emojis\n"
        "- MUST NOT mention devices, rooms, or names\n"
        "- MUST NOT introduce new actions or topics\n"
        "- Avoid filler like 'Okay' if possible\n"
        "Good examples:\n"
        "- Go ahead.\n"
        "- I'm listening.\n"
        "- Tell me.\n"
        "- Say it.\n"
        "Bad examples:\n"
        "- Just say yes or no?\n"
        "- Which one?\n\n"
        f"Context (assistant just spoke):\n{tail!r}\n"
    )

    try:
        r = await _llm.chat(
            [
                {"role": "system", "content": "You write ultra-short voice cues. No emojis. No questions."},
                {"role": "user", "content": prompt},
            ],
            timeout=6,
        )
        text = (r.get("message", {}) or {}).get("content", "") if isinstance(r, dict) else ""
        text = (text or "").strip()
        text = text.split("\n")[0].strip()
        text = text[:40].strip()

        # Hard safety: never allow a question mark
        text = text.replace("?", "").strip()

        # Ensure it ends with a period for TTS cadence (optional but nice)
        if text and text[-1].isalnum():
            text += "."

        # Fallbacks
        if not text or len(text) < 2:
            return "I'm listening."

        return text
    except Exception:
        return "I'm listening."

def _start_satellite_followup(entity_id: str, start_message: str) -> None:
    msg = (start_message or "").strip()
    msg = msg.replace("?", "").strip()
    if not msg:
        msg = "I'm listening."

    ha = _HA()
    ha.call_service(
        "assist_satellite",
        "start_conversation",
        {
            "entity_id": entity_id,
            "start_message": msg,
            "preannounce": False,
        },
    )

def _should_follow_up(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    tail = t[-200:]
    return "?" in tail and tail.rstrip().endswith("?")

async def _maybe_reopen_listening(conv_key: str, ctx: Dict[str, Any], assistant_text: str):
    """
    If assistant ended in a question, and we have area context,
    reopen listening on the correct satellite entity for that area.

    Flow:
    - Trigger ONLY based on assistant_text (the real assistant reply)
    - Wait for satellite idle
    - Call assist_satellite.start_conversation with a short AI-generated *cue*
      that is NOT a question (no '?') so it can't cause weird loops.

    NOTE:
    conv_key is only used for logging/clarity (history continuity is handled by _conv_key()).
    """
    if not _should_follow_up(assistant_text):
        return

    sat = await _resolve_assist_satellite_entity(ctx)
    if not sat:
        logger.info("[followup] skip (no assist satellite found for area)")
        return

    idle_timeout = _get_float_platform_setting("FOLLOWUP_IDLE_TIMEOUT_S", DEFAULT_FOLLOWUP_IDLE_TIMEOUT_S)

    ok = await _wait_for_satellite_idle(sat, idle_timeout)
    if not ok:
        logger.info(f"[followup] skip (satellite not idle within {idle_timeout}s): {sat}")
        return

    try:
        cue = await _generate_followup_question(assistant_text)

        # Hard safety: never allow a question mark in start_message
        cue = (cue or "").strip().replace("?", "").strip()
        if not cue:
            cue = "I'm listening."

        # IMPORTANT:
        # - We DO NOT pass `cue` into _should_follow_up()
        # - We DO NOT save `cue` into history
        await asyncio.to_thread(_start_satellite_followup, sat, cue)

        logger.info(f"[followup] start_conversation on {sat} (conv_key={conv_key}, cue={cue!r})")
    except Exception as e:
        logger.warning(f"[followup] failed to start_conversation on {sat}: {e}")

# -------------------- App + LLM client --------------------
app = FastAPI(title="Tater Home Assistant Bridge", version="2.0")  # stable conv_key for continued chat

_llm = None

@app.on_event("startup")
async def _on_startup():
    global _llm
    _llm = get_llm_client_from_env()

@app.get("/tater-ha/v1/health")
async def health():
    return {"ok": True, "version": "2.0"}

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
        "ts": int(time.time()),
    }

    redis_client.lpush(REDIS_NOTIF_LIST, json.dumps(item))

    try:
        _ring_on()
    except Exception:
        logger.exception("[notify] ring_on failed")

    return {"ok": True, "queued": True}

@app.get("/tater-ha/v1/notifications", response_model=NotificationsOut)
async def pull_notifications(background_tasks: BackgroundTasks):
    raw = redis_client.lrange(REDIS_NOTIF_LIST, 0, -1) or []
    notifications = []
    for r in raw:
        try:
            notifications.append(json.loads(r))
        except Exception:
            continue

    try:
        redis_client.delete(REDIS_NOTIF_LIST)
    except Exception:
        logger.warning("[notify] failed to clear notification list")

    if notifications:
        background_tasks.add_task(_ring_off)
    else:
        try:
            _ring_off()
        except Exception:
            logger.exception("[notify] ring_off failed")

    return {"notifications": notifications}

# -------------------- Main HA chat endpoint --------------------
@app.post("/tater-ha/v1/message", response_model=HAResponse)
async def handle_message(payload: HARequest):
    """
    Home Assistant bridge:
    - Builds a Discord/IRC-style system prompt (HA-scoped)
    - Injects voice room/device context via system prompt only
    - Shapes loop history (real user/assistant turns only)
    - Executes ONLY plugins that implement handle_homeassistant
    - Passes room/device context to plugins if they accept context=...
    - If assistant ends with a question and continued chat is enabled:
      wait for the satellite to become idle, then call assist_satellite.start_conversation
      with a short AI-generated spoken cue (NOT a question) to re-open the mic.

    IMPORTANT:
    We key history/ctx by a stable conv_key so follow-up mic re-open does NOT reset context.
    """
    if _llm is None:
        raise HTTPException(status_code=503, detail="LLM backend not initialized")

    text_in = (payload.text or "").strip()
    if not text_in:
        return HAResponse(response="(no text provided)")

    session_history_max = _get_int_platform_setting("SESSION_HISTORY_MAX", DEFAULT_SESSION_HISTORY_MAX)
    max_history_cap = _get_int_platform_setting("MAX_HISTORY_CAP", DEFAULT_MAX_HISTORY_CAP)
    history_max = min(max(session_history_max, 0), max_history_cap)

    # ---- canonical context merge (ctx stored separately; not in chat history) ----
    incoming_ctx: Dict[str, Any] = {}
    if payload.context:
        incoming_ctx = payload.context.model_dump(exclude_none=True)

    # Start with stored ctx from the STABLE conv_key (not HA's session_id)
    conv_key = _conv_key(payload, incoming_ctx)
    ctx: Dict[str, Any] = _load_ctx(conv_key)

    # Merge incoming context
    if isinstance(incoming_ctx, dict) and incoming_ctx:
        ctx.update(incoming_ctx)

    # Backward compatible fallbacks
    if payload.device_id and not ctx.get("device_id"):
        ctx["device_id"] = payload.device_id
    if payload.area_id and not ctx.get("area_id"):
        ctx["area_id"] = payload.area_id

    if ctx:
        _save_ctx(conv_key, ctx)

    logger.debug(
        f"[HA Bridge] conv_key={conv_key} session_id={payload.session_id} device_id={payload.device_id} area_id={payload.area_id}"
    )

    # Save the user turn
    await _save_message(conv_key, "user", text_in, history_max)

    # Build the messages list: system + shaped history
    system_prompt = build_system_prompt(ctx if ctx else None)
    loop_messages = await _load_history(conv_key, history_max)
    messages_list = [{"role": "system", "content": system_prompt}] + loop_messages

    # Hard guard: ensure last turn is user
    if not messages_list or messages_list[-1].get("role") != "user":
        messages_list.append({"role": "user", "content": text_in})

    logger.debug(f"[HA Bridge] LLM last role = {messages_list[-1]['role']} (conv_key={conv_key})")

    try:
        response = await _llm.chat(messages_list, timeout=TIMEOUT_SECONDS)
        text = (response.get("message", {}) or {}).get("content", "") if isinstance(response, dict) else ""
        text = (text or "").strip()

        if not text:
            await _save_message(conv_key, "assistant", "", history_max)
            return HAResponse(response="Sorry, I didn't catch that.")

        fn = parse_function_json(text)
        if fn:
            func = fn.get("function")
            args = fn.get("arguments", {}) or {}

            await _save_message(
                conv_key,
                "assistant",
                {"marker": "plugin_call", "plugin": func, "arguments": args},
                history_max,
            )

            plugin = plugin_registry.plugin_registry.get(func)
            is_ha_plugin = plugin and (
                ("homeassistant" in getattr(plugin, "platforms", [])) or ("both" in getattr(plugin, "platforms", []))
            )
            if not plugin or not get_plugin_enabled(func) or not is_ha_plugin:
                msg = f"Function `{func}` is not available for Home Assistant."
                await _save_message(conv_key, "assistant", msg, history_max)
                return HAResponse(response=msg)

            if not hasattr(plugin, "handle_homeassistant"):
                msg = f"Function `{func}` is not supported in this platform."
                await _save_message(conv_key, "assistant", msg, history_max)
                return HAResponse(response=msg)

            try:
                handle = plugin.handle_homeassistant
                try:
                    sig = inspect.signature(handle)
                except Exception:
                    sig = None

                if sig and "context" in sig.parameters:
                    result = await handle(args, _llm, context=ctx)
                else:
                    result = await handle(args, _llm)

                final_text = _flatten_to_text(result).strip() or f"Done with {func}."
                if len(final_text) > 4000:
                    final_text = final_text[:4000] + "…"

                await _save_message(
                    conv_key,
                    "assistant",
                    {"marker": "plugin_response", "phase": "final", "content": final_text},
                    history_max,
                )

                if _get_bool_platform_setting("CONTINUED_CHAT_ENABLED", DEFAULT_CONTINUED_CHAT_ENABLED):
                    asyncio.create_task(_maybe_reopen_listening(conv_key, ctx, final_text))

                return HAResponse(response=final_text)

            except Exception:
                logger.exception(f"[HA Bridge] Plugin '{func}' error")
                msg = f"I tried to run {func} but hit an error."
                await _save_message(conv_key, "assistant", msg, history_max)
                return HAResponse(response=msg)

        # Plain text answer
        final_text = text
        if len(final_text) > 4000:
            final_text = final_text[:4000] + "…"

        await _save_message(conv_key, "assistant", final_text, history_max)

        if _get_bool_platform_setting("CONTINUED_CHAT_ENABLED", DEFAULT_CONTINUED_CHAT_ENABLED):
            asyncio.create_task(_maybe_reopen_listening(conv_key, ctx, final_text))

        return HAResponse(response=final_text)

    except Exception:
        logger.exception("[HA Bridge] LLM error")
        msg = "Sorry, I ran into a problem processing that."
        await _save_message(conv_key, "assistant", msg, history_max)
        return HAResponse(response=msg)

def run(stop_event: Optional[threading.Event] = None):
    """Match your other platforms’ run signature and graceful stop behavior."""
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
