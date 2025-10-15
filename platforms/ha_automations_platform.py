# platforms/ha_automations_platform.py
import os
import json
import asyncio
import logging
import threading
import time
from typing import Optional, Dict, Any

import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

from dotenv import load_dotenv
load_dotenv()

from helpers import LLMClientWrapper, parse_function_json, get_tater_name
from plugin_registry import plugin_registry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ha_automations")

# -------------------- Platform constants --------------------
BIND_HOST = "0.0.0.0"
TIMEOUT_MS = 60_000  # LLM request timeout (ms)
APP_VERSION = "1.0"

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
    }
}

# Namespace: "automation_platform_settings"
PLATFORM_SETTINGS_HASH = "automation_platform_settings"

def _get_bind_port() -> int:
    raw = redis_client.hget("automation_platform_settings", "bind_port")
    try:
        return int(raw) if raw is not None else PLATFORM_SETTINGS["required"]["bind_port"]["default"]
    except (TypeError, ValueError):
        logger.warning(f"[Automations] Invalid bind_port '{raw}', defaulting to {PLATFORM_SETTINGS['required']['bind_port']['default']}")
        return PLATFORM_SETTINGS["required"]["bind_port"]["default"]

# -------------------- Pydantic DTOs --------------------
class AutomationsRequest(BaseModel):
    text: str  # plain user instruction, e.g. "turn the office lights green"

# -------------------- Gating helpers --------------------
def _plugin_enabled(name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", name)
    return bool(enabled and enabled.lower() == "true")

def _is_automation_plugin(p) -> bool:
    # Per your requirement: only plugins that explicitly declare platforms = ["automation"]
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
        '  "arguments": { /* JSON object with the tool arguments */ }\n'
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

    if not tool_blocks:
        # If nothing qualifies, make it explicit; the platform will fail with 404 later anyway.
        tool_section = "No tools are currently available."
    else:
        tool_section = "\n".join(tool_blocks)

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
        resp = await _llm.chat(messages, timeout_ms=TIMEOUT_MS)
        llm_text = (resp.get("message", {}) or {}).get("content", "") if isinstance(resp, dict) else ""
    except Exception as e:
        logger.exception("[Automations] LLM error")
        raise HTTPException(status_code=503, detail=f"LLM error: {e}")

    if not llm_text:
        raise HTTPException(status_code=422, detail="Empty LLM response")

    # Parse function JSON
    fn = parse_function_json(llm_text)
    if not fn:
        # The router failed or returned prose or error marker
        # Try to detect explicit error marker
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
        # Plugins may raise for validation errors; we bubble those up as 422
        result = await plugin.handle_automation(args, _llm)
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