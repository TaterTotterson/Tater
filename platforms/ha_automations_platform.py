# platforms/ha_automations_platform.py
import os
import json
import asyncio
import logging
import threading
import time
from typing import Optional, Dict, Any, List

import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Response, Query
from pydantic import BaseModel, Field

from dotenv import load_dotenv
load_dotenv()

from helpers import LLMClientWrapper, parse_function_json, get_tater_name
from plugin_registry import plugin_registry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ha_automations")

# -------------------- Platform constants --------------------
BIND_HOST = "0.0.0.0"
TIMEOUT_SECONDS = 60  # LLM request timeout in seconds
APP_VERSION = "1.1"  # events endpoints + time-based retention

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
            "description": "How long to keep events (by time only)."
        },
    }
}

def _get_bind_port() -> int:
    """Read port directly from Redis, fallback to default."""
    raw = redis_client.hget("ha_automations_platform_settings", "bind_port")
    try:
        return int(raw) if raw is not None else PLATFORM_SETTINGS["required"]["bind_port"]["default"]
    except (TypeError, ValueError):
        logger.warning(
            f"[Automations] Invalid bind_port '{raw}', defaulting to {PLATFORM_SETTINGS['required']['bind_port']['default']}"
        )
        return PLATFORM_SETTINGS["required"]["bind_port"]["default"]


def _get_events_retention_seconds() -> Optional[int]:
    """Read retention from Redis, fallback to default."""
    raw = redis_client.hget("ha_automations_platform_settings", "events_retention")
    val = (raw or PLATFORM_SETTINGS["required"]["events_retention"]["default"]).strip().lower()
    mapping = {
        "2d": 2 * 24 * 60 * 60,
        "7d": 7 * 24 * 60 * 60,
        "30d": 30 * 24 * 60 * 60,
        "forever": None,
    }
    return mapping.get(val, mapping["7d"])

# -------------------- Events storage (time-based retention) --------------------
EVENTS_LIST_PREFIX = "tater:automations:events:"  # newest-first list per source

def _events_key(source: str) -> str:
    src = (source or "general").strip() or "general"
    return f"{EVENTS_LIST_PREFIX}{src}"

def _trim_events_by_time(source: str) -> None:
    """Keep only items whose 'ts' >= now - retention_seconds. Newest-first list."""
    retention = _get_events_retention_seconds()
    if retention is None:
        return  # forever
    cutoff = int(time.time()) - retention
    key = _events_key(source)
    try:
        raw = redis_client.lrange(key, 0, -1) or []
        keep = []
        for r in raw:
            try:
                item = json.loads(r)
                if int(item.get("ts", 0)) >= cutoff:
                    keep.append(r)
            except Exception:
                continue
        pipe = redis_client.pipeline()
        pipe.delete(key)
        if keep:
            pipe.rpush(key, *keep)  # keep newest-first at index 0
        pipe.execute()
    except Exception:
        logger.exception("[Automations] time-trim failed for %s", key)

def _append_event(source: str, item: Dict[str, Any]) -> None:
    key = _events_key(source)
    try:
        redis_client.lpush(key, json.dumps(item))
        _trim_events_by_time(source)  # prune old entries after append
    except Exception:
        logger.exception("[Automations] Failed to append event for %s", key)

# -------------------- Pydantic DTOs --------------------
class AutomationsRequest(BaseModel):
    text: str  # plain user instruction, e.g. "turn the office lights green"

class EventIn(BaseModel):
    """
    Generic house event payload posted by plugins or automations.
    All fields except 'source' and 'title' are optional; we add 'ts' server-side.
    """
    source: str = Field(..., description="Logical source/plugin, e.g., 'doorbell_alert'")
    title: str = Field(..., description="Short event title")
    type: Optional[str] = Field(None, description="Category: doorbell, motion, garage, scene, etc.")
    message: Optional[str] = Field(None, description="Human-readable description/body")
    entity_id: Optional[str] = Field(None, description="Primary HA entity related to this event")
    ha_time: Optional[str] = Field(None, description="Timestamp string provided by HA (e.g., sensor.date_time_iso)")
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
    # Only plugins that declare platforms = ["automation"] and implement handle_automation
    platforms = getattr(p, "platforms", [])
    return isinstance(platforms, list) and platforms == ["automation"] and hasattr(p, "handle_automation")

# -------------------- System prompt --------------------
def build_system_prompt() -> str:
    """
    Strict router: MUST return exactly one tool call as JSON; no chat text allowed.
    Only includes automation plugins that are enabled and implement handle_automation.
    """
    first, last = get_tater_name()

    header = (
        f"You are {first} {last}, an automation-only tool router.\n"
        "You MUST respond with a single JSON object describing exactly one tool call.\n"
        "NEVER write normal prose or explanations. If you cannot pick a tool with confident arguments, "
        "return the JSON: {\"error\":\"no_tool\"}.\n"
        "Valid tool call format:\n"
        "{\n"
        '  "function": "<tool_name>",\n'
        '  "arguments": { }\n'
        "}\n"
    )

    tool_blocks = []
    for plugin in plugin_registry.values():
        if not _is_automation_plugin(plugin):
            continue
        if not _plugin_enabled(plugin.name):
            continue
        desc = getattr(plugin, "description", "No description provided.")
        usage = getattr(plugin, "usage", "").strip()
        block = (
            f"Tool: {plugin.name}\n"
            f"Description: {desc}\n"
            f"{usage}\n"
        )
        tool_blocks.append(block)

    tool_section = "\n".join(tool_blocks) if tool_blocks else "No tools are currently available."

    guardrails = (
        "Rules:\n"
        "- Only return the JSON for one tool call; no Markdown, no backticks, no extra keys.\n"
        "- Choose a tool ONLY if the user's latest message clearly requests an action that the tool can perform.\n"
        "- If the tool requires structured arguments, construct them carefully from the user's message.\n"
        "- If uncertain, return {\"error\":\"no_tool\"}.\n"
    )

    return f"{header}\n{tool_section}\n\n{guardrails}"

# -------------------- FastAPI app --------------------
app = FastAPI(title="Tater Automations Bridge", version=APP_VERSION)

_llm: Optional[LLMClientWrapper] = None

@app.on_event("startup")
async def _on_startup():
    global _llm
    llm_host = os.getenv("LLM_HOST", "127.0.0.1")
    llm_port = os.getenv("LLM_PORT", "11434")
    _llm = LLMClientWrapper(host=f"http://{llm_host}:{llm_port}")
    logger.info(f"[Automations] LLM client → http://{llm_host}:{llm_port}")

@app.get("/tater-ha/v1/health")
async def health():
    return {"ok": True, "version": APP_VERSION}

# -------------------- Events APIs --------------------
@app.post("/tater-ha/v1/events/add")
async def add_event(ev: EventIn):
    """
    Append a house event to a per-source Redis list (newest first).
    This keeps events durable and queryable, separate from notifications.
    """
    item = {
        "source": ev.source.strip(),
        "title": ev.title.strip(),
        "type": (ev.type or "").strip(),
        "message": (ev.message or "").strip(),
        "entity_id": (ev.entity_id or "").strip(),
        "ha_time": (ev.ha_time or "").strip(),
        "level": (ev.level or "info").strip(),
        "data": ev.data or {},
        "ts": int(time.time()),  # server epoch seconds
    }
    _append_event(item["source"], item)
    return {"ok": True, "stored": True}

@app.get("/tater-ha/v1/events/search", response_model=EventsOut)
async def events_search(
    source: str = Query("general", description="Event source/plugin, e.g., 'doorbell_alert'"),
    limit: int = Query(25, ge=1, le=1000, description="Max number of items to return (newest first)"),
):
    """
    Return the most recent events for a given source (newest first).
    Applies time-based trimming opportunistically on read.
    """
    try:
        _trim_events_by_time(source)
    except Exception:
        logger.exception("[Automations] time-trim failed during search for %s", source)

    key = _events_key(source)
    raw = redis_client.lrange(key, 0, limit - 1) or []
    items: List[Dict[str, Any]] = []
    for r in raw:
        try:
            items.append(json.loads(r))
        except Exception:
            continue
    return {"source": source, "items": items}

# -------------------- Main automations message endpoint --------------------
@app.post("/tater-ha/v1/message")
async def handle_message(payload: AutomationsRequest):
    """
    Strict tool-only router:
    - Builds an automation-scoped system prompt
    - Calls LLM with: [system, user]
    - Requires a valid single tool call JSON
    - Executes plugin.handle_automation(args, llm_client)
    - On success: returns 204 No Content (no chat)
    """
    if _llm is None:
        raise HTTPException(status_code=503, detail="LLM backend not initialized")

    text = (payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Missing 'text'")

    system_prompt = build_system_prompt()
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": text},
    ]

    # Ask LLM (router behavior)
    try:
        resp = await _llm.chat(messages, timeout=TIMEOUT_SECONDS)
        llm_text = (resp.get("message", {}) or {}).get("content", "") if isinstance(resp, dict) else ""
    except Exception as e:
        logger.exception("[Automations] LLM error")
        raise HTTPException(status_code=503, detail=f"LLM error: {e}")

    if not llm_text:
        raise HTTPException(status_code=422, detail="Empty LLM response")

    # Parse function JSON
    fn = parse_function_json(llm_text)
    if not fn:
        # Detect explicit no_tool error
        try:
            obj = json.loads(llm_text)
            if isinstance(obj, dict) and obj.get("error") == "no_tool":
                raise HTTPException(status_code=422, detail="No suitable tool selected")
        except Exception:
            pass
        raise HTTPException(status_code=422, detail="Invalid tool call JSON")

    func_name = fn.get("function")
    args = fn.get("arguments", {}) or {}

    # Resolve plugin
    plugin = plugin_registry.get(func_name)
    if not plugin:
        raise HTTPException(status_code=404, detail=f"Tool '{func_name}' not found")

    if not _plugin_enabled(func_name):
        raise HTTPException(status_code=404, detail=f"Tool '{func_name}' is disabled")

    if not _is_automation_plugin(plugin):
        raise HTTPException(status_code=404, detail=f"Tool '{func_name}' is not available on 'automation' platform")

    # Execute
    try:
        await plugin.handle_automation(args, _llm)
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=f"Validation error: {ve}")
    except Exception as e:
        logger.exception(f"[Automations] Plugin '{func_name}' error")
        raise HTTPException(status_code=500, detail=f"Plugin error: {e}")

    # Success: no content
    return Response(status_code=204)

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