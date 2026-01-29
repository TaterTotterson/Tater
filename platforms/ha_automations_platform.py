# platforms/ha_automations_platform.py
import os
import json
import asyncio
import logging
import threading
import time
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from dotenv import load_dotenv

import plugin_registry
from helpers import get_llm_client_from_env, build_llm_host_from_env

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ha_automations")

# -------------------- Platform constants --------------------
BIND_HOST = "0.0.0.0"
APP_VERSION = "1.7"  # direct tool calls only (no AI router)

# -------------------- Redis --------------------
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# -------------------- Platform settings (WebUI can write this hash) --------------------
PLATFORM_SETTINGS = {
    "category": "Automation Settings",
    "required": {
        "bind_port": {
            "label": "Bind Port",
            "type": "number",
            "default": 8788,
            "description": "TCP port for the Tater ↔ Home Assistant Automations bridge"
        },
        "events_retention": {
            "label": "Events Retention",
            "type": "select",
            "options": ["2d", "7d", "30d", "forever"],
            "default": "7d",
            "description": "How long to keep events (by ha_time only)."
        },
    }
}

def _get_bind_port() -> int:
    raw = redis_client.hget("ha_automations_platform_settings", "bind_port")
    try:
        return int(raw) if raw is not None else PLATFORM_SETTINGS["required"]["bind_port"]["default"]
    except (TypeError, ValueError):
        logger.warning(
            f"[Automations] Invalid bind_port '{raw}', defaulting to {PLATFORM_SETTINGS['required']['bind_port']['default']}"
        )
        return PLATFORM_SETTINGS["required"]["bind_port"]["default"]

def _get_events_retention_seconds() -> Optional[int]:
    raw = redis_client.hget("ha_automations_platform_settings", "events_retention")
    val = (raw or PLATFORM_SETTINGS["required"]["events_retention"]["default"]).strip().lower()
    mapping = {
        "2d": 2 * 24 * 60 * 60,
        "7d": 7 * 24 * 60 * 60,
        "30d": 30 * 24 * 60 * 60,
        "forever": None,
    }
    return mapping.get(val, mapping["7d"])

# -------------------- Time helpers (naive ISO) --------------------
def _parse_iso_naive(s: Optional[str]) -> Optional[datetime]:
    """Parse ISO string like '2025-10-19T20:07:00' (no timezone)."""
    if not s:
        return None
    s = s.strip()
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    except Exception:
        try:
            dt = datetime.fromisoformat(s)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except Exception:
            return None

def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

# -------------------- Events storage (time-based retention by ha_time) --------------------
EVENTS_LIST_PREFIX = "tater:automations:events:"  # newest-first list per source

def _events_key(source: str) -> str:
    src = (source or "general").strip() or "general"
    return f"{EVENTS_LIST_PREFIX}{src}"

def _trim_events_by_time(source: str) -> None:
    """Keep only items whose ha_time >= now - retention_seconds. Newest-first list."""
    retention = _get_events_retention_seconds()
    if retention is None:
        return  # forever
    cutoff_dt = datetime.now() - timedelta(seconds=retention)
    key = _events_key(source)
    try:
        raw = redis_client.lrange(key, 0, -1) or []
        keep = []
        for r in raw:
            try:
                item = json.loads(r)
                dt = _parse_iso_naive(item.get("ha_time"))
                if dt and dt >= cutoff_dt:
                    keep.append(r)
            except Exception:
                continue
        pipe = redis_client.pipeline()
        pipe.delete(key)
        if keep:
            pipe.rpush(key, *keep)
        pipe.execute()
    except Exception:
        logger.exception("[Automations] time-trim failed for %s", key)

def _append_event(source: str, item: Dict[str, Any]) -> None:
    key = _events_key(source)
    try:
        redis_client.lpush(key, json.dumps(item))
        _trim_events_by_time(source)
    except Exception:
        logger.exception("[Automations] Failed to append event for %s", key)

# -------------------- Pydantic DTOs --------------------
class ToolCallRequest(BaseModel):
    arguments: Dict[str, Any] = Field(default_factory=dict, description="Arguments for the tool/plugin call")

class EventIn(BaseModel):
    """
    Generic house event payload posted by plugins or automations.
    NOTE: ha_time is REQUIRED (naive ISO like 2025-10-19T20:07:00). No epoch.
    """
    source: str = Field(..., description="Logical source/area, e.g., 'front_yard'")
    title: str = Field(..., description="Short event title")
    ha_time: str = Field(..., description="ISO local-naive time, e.g., 2025-10-19T20:07:00")
    type: Optional[str] = Field(None, description="Category: doorbell, motion, garage, scene, etc.")
    message: Optional[str] = Field(None, description="Human-readable description/body")
    entity_id: Optional[str] = Field(None, description="Primary HA entity related to this event")
    level: Optional[str] = Field("info", description="info|warn|error (free-form)")
    data: Optional[Dict[str, Any]] = Field(None, description="Arbitrary structured extras")

class EventsOut(BaseModel):
    source: str
    items: List[Dict[str, Any]]

# -------------------- Gating helpers --------------------
def _plugin_enabled(name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", name)
    return bool(enabled and enabled.lower() == "true")

def _is_automation_plugin(p) -> bool:
    platforms = getattr(p, "platforms", [])
    return isinstance(platforms, list) and ("automation" in platforms) and hasattr(p, "handle_automation")

# -------------------- Function name normalizer (defensive) --------------------
def _normalize_func_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.strip().lower()
    if s.startswith("run_"):
        s = s[4:]
    s = s.replace(" ", "_").replace("-", "_")
    return s

# -------------------- FastAPI app --------------------
app = FastAPI(title="Tater Automations Bridge", version=APP_VERSION)

_llm = None

@app.on_event("startup")
async def _on_startup():
    global _llm
    _llm = get_llm_client_from_env()
    logger.info(f"[Automations] LLM client → {build_llm_host_from_env()}")

@app.get("/tater-ha/v1/health")
async def health():
    return {"ok": True, "version": APP_VERSION}

# -------------------- Events APIs --------------------
@app.post("/tater-ha/v1/events/add")
async def add_event(ev: EventIn):
    ha_dt = _parse_iso_naive(ev.ha_time)
    if ha_dt is None:
        raise HTTPException(status_code=422, detail="ha_time must be ISO like 2025-10-19T20:07:00 (no timezone)")

    item = {
        "source": (ev.source or "").strip(),
        "title": (ev.title or "").strip(),
        "type": (ev.type or "").strip(),
        "message": (ev.message or "").strip(),
        "entity_id": (ev.entity_id or "").strip(),
        "ha_time": _iso(ha_dt),
        "level": (ev.level or "info").strip(),
        "data": ev.data or {},
    }
    _append_event(item["source"], item)
    return {"ok": True, "stored": True}

@app.get("/tater-ha/v1/events/search", response_model=EventsOut)
async def events_search(
    source: str = Query("general", description="Event source/area bucket, e.g., 'front_yard'"),
    limit: int = Query(25, ge=1, le=1000, description="Max number of items to return (newest first)"),
    since: Optional[str] = Query(None, description="Naive ISO start (YYYY-MM-DDTHH:MM:SS)"),
    until: Optional[str] = Query(None, description="Naive ISO end (YYYY-MM-DDTHH:MM:SS)"),
):
    try:
        _trim_events_by_time(source)
    except Exception:
        logger.exception("[Automations] time-trim failed during search for %s", source)

    since_dt = _parse_iso_naive(since) if since else None
    until_dt = _parse_iso_naive(until) if until else None

    key = _events_key(source)
    raw = redis_client.lrange(key, 0, limit - 1) or []
    items: List[Dict[str, Any]] = []
    for r in raw:
        try:
            ev = json.loads(r)
            ev_dt = _parse_iso_naive(ev.get("ha_time"))
            if ev_dt:
                if since_dt and ev_dt < since_dt:
                    continue
                if until_dt and ev_dt > until_dt:
                    continue
            items.append(ev)
        except Exception:
            continue
    return {"source": source, "items": items}

# -------------------- Direct tool endpoint (NO AI ROUTING) --------------------
@app.post("/tater-ha/v1/tools/{tool_name}")
async def call_tool(tool_name: str, payload: ToolCallRequest):
    """
    Direct tool call endpoint for Home Assistant automations.

    POST /tater-ha/v1/tools/<tool_name>
    Body: {"arguments": {...}}

    Returns: {"result": <plugin_result>}
    """
    if _llm is None:
        raise HTTPException(status_code=503, detail="LLM backend not initialized")

    func_name = _normalize_func_name(tool_name)
    if not func_name:
        raise HTTPException(status_code=400, detail="Missing tool_name")

    plugin = plugin_registry.plugin_registry.get(func_name)
    if not plugin:
        raise HTTPException(status_code=404, detail=f"Tool '{func_name}' not found")

    if not _plugin_enabled(func_name):
        raise HTTPException(status_code=404, detail=f"Tool '{func_name}' is disabled")

    if not _is_automation_plugin(plugin):
        raise HTTPException(status_code=404, detail=f"Tool '{func_name}' is not available on 'automation' platform")

    args = payload.arguments or {}

    try:
        result = await plugin.handle_automation(args, _llm)
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=f"Validation error: {ve}")
    except Exception as e:
        logger.exception(f"[Automations] Tool '{func_name}' error")
        raise HTTPException(status_code=500, detail=f"Plugin error: {e}")

    return {"result": result}

# -------------------- Runner (mirrors other platforms’ graceful stop) --------------------
def run(stop_event: Optional[threading.Event] = None):
    port = _get_bind_port()
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
    logger.info(f"[Automations] Listening on http://{BIND_HOST}:{port}")
