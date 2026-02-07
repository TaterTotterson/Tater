# plugins/unifi_protect.py
import asyncio
import base64
import json
import logging
import mimetypes
import os
import re
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

import requests
import urllib3

from plugin_base import ToolPlugin
from plugin_result import action_failure
from helpers import redis_client, get_tater_name, get_tater_personality

def _build_media_metadata(binary: bytes, *, media_type: str, name: str, mimetype: str) -> dict:
    if not isinstance(binary, (bytes, bytearray)):
        raise TypeError("binary must be bytes")
    return {
        "type": media_type,
        "name": name,
        "mimetype": mimetype,
        "size": len(binary),
        "bytes": bytes(binary),
    }

logger = logging.getLogger("unifi_protect")
logger.setLevel(logging.INFO)


# ----------------------------
# Small helpers
# ----------------------------
def _coerce_str(x) -> str:
    return "" if x is None else str(x)


def _ms_to_local_str(ms: Any) -> str:
    if not ms:
        return "unknown"
    try:
        dt = datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).astimezone()
        return dt.strftime("%Y-%m-%d %I:%M %p")
    except Exception:
        return "unknown"


def _c_to_f(c: Any) -> Optional[float]:
    if c is None:
        return None
    try:
        return (float(c) * 9.0 / 5.0) + 32.0
    except Exception:
        return None


def _strip_code_fences(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.MULTILINE).strip()
    if s.endswith("```"):
        s = re.sub(r"\s*```$", "", s, flags=re.MULTILINE).strip()
    return s


def _to_data_url(image_bytes: bytes, filename: str = "image.jpg") -> str:
    mime = mimetypes.guess_type(filename)[0] or "image/jpeg"
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    return f"data:{mime};base64,{b64}"


def _extract_context_text(context: Optional[dict]) -> str:
    if not isinstance(context, dict):
        return ""
    for key in ("raw_message", "request_text", "user_text", "task_prompt", "body"):
        val = context.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    msg = context.get("message")
    if msg is not None:
        for attr in ("content", "text", "message"):
            val = getattr(msg, attr, None)
            if isinstance(val, str) and val.strip():
                return val.strip()

    update = context.get("update")
    if isinstance(update, dict):
        text = ((update.get("message") or {}).get("text") or "")
        if isinstance(text, str) and text.strip():
            return text.strip()

    ctx = context.get("context")
    if isinstance(ctx, dict):
        for key in ("raw_message", "text", "message"):
            val = ctx.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()

    return ""


def _split_action_and_target(raw_action: str) -> Tuple[str, str]:
    raw = (raw_action or "").strip().lower()
    if not raw:
        return "", ""
    raw = raw.replace("-", "_")
    if raw in {"describe area", "describe camera", "list cameras", "sensor detail", "sensors status"}:
        raw = raw.replace(" ", "_")
        return raw, ""

    tokens = raw.split()
    if len(tokens) >= 2:
        candidate = f"{tokens[0]}_{tokens[1]}"
        if candidate in {"sensors_status", "sensor_detail", "list_cameras", "describe_camera", "describe_area"}:
            return candidate, " ".join(tokens[2:]).strip()
        return tokens[0], " ".join(tokens[1:]).strip()
    return raw, ""


def _looks_like_area(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in [
        "front yard", "back yard", "yard", "porch", "driveway", "patio", "garage", "front porch", "back porch",
    ])


def _guess_action_from_text(text: str, target: str = "") -> str:
    t = f"{text} {target}".lower().strip()
    if not t:
        return ""

    if "list" in t and "camera" in t:
        return "list_cameras"

    if any(k in t for k in ["sensor", "sensors", "battery", "humidity", "temperature", "temp"]):
        if any(k in t for k in ["front door", "back door", "garage door", "doorbell"]):
            return "sensor_detail"
        return "sensors_status"

    if any(k in t for k in ["snapshot", "photo", "picture", "image"]):
        return "describe_camera" if target or "camera" in t or "doorbell" in t else "describe_area"

    if any(k in t for k in ["what's happening", "whats happening", "what is happening", "show me", "look", "see", "view", "describe"]):
        if _looks_like_area(t):
            return "describe_area"
        if any(k in t for k in ["camera", "cam", "doorbell", "front door", "back door"]):
            return "describe_camera"
        return "describe_area"

    if _looks_like_area(t):
        return "describe_area"

    if any(k in t for k in ["door", "window"]):
        return "sensor_detail" if target else "sensors_status"

    return ""


def _platform_supports_media(platform: str) -> bool:
    return (platform or "").strip().lower() in {"webui", "discord", "telegram", "matrix"}


# ----------------------------
# UniFi Protect REST helper (Integration API via console proxy)
# ----------------------------
class ProtectClient:
    def __init__(self):
        settings = redis_client.hgetall("plugin_settings:UniFi Protect") or {}

        base_url = (settings.get("UNIFI_PROTECT_BASE_URL") or "https://10.4.20.127").strip()
        api_key = (settings.get("UNIFI_PROTECT_API_KEY") or "").strip()

        # Vision endpoint settings (OpenAI-compatible)
        vision_api_base = (settings.get("VISION_API_BASE") or "http://127.0.0.1:1234").strip().rstrip("/")
        vision_model = (settings.get("VISION_MODEL") or "qwen2.5-vl-7b-instruct").strip()
        vision_api_key = (settings.get("VISION_API_KEY") or os.getenv("OPENAI_API_KEY", "") or "").strip()

        # Defaults (as requested)
        self.verify_ssl = False
        self.timeout = 20

        if not base_url:
            base_url = "https://10.4.20.127"

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

        self.vision_api_base = vision_api_base
        self.vision_model = vision_model
        self.vision_api_key = vision_api_key

        if not self.api_key:
            raise ValueError("UniFi Protect API key (UNIFI_PROTECT_API_KEY) not set in plugin settings.")

        self.headers = {
            "X-API-KEY": self.api_key,
            "Accept": "application/json",
        }

        # Silence warnings if verify_ssl is False (your environment uses -k in curl)
        if not self.verify_ssl:
            try:
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:
                pass

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def _req(self, method: str, path: str, *, params=None, json_body=None, headers=None, stream=False) -> Any:
        url = self._url(path)
        hdrs = dict(self.headers)
        if headers:
            hdrs.update(headers)

        resp = requests.request(
            method,
            url,
            headers=hdrs,
            params=params,
            json=json_body,
            timeout=self.timeout,
            verify=self.verify_ssl,
            stream=stream,
        )

        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

        # stream responses (snapshots) return bytes
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if stream or "image/" in ctype:
            return resp.content, resp.headers

        try:
            return resp.json()
        except Exception:
            return resp.text

    # ---- Sensors
    def list_sensors(self) -> List[dict]:
        return self._req("GET", "/proxy/protect/integration/v1/sensors") or []

    def get_sensor(self, sensor_id: str) -> dict:
        return self._req("GET", f"/proxy/protect/integration/v1/sensors/{sensor_id}")

    # ---- Cameras
    def list_cameras(self) -> List[dict]:
        return self._req("GET", "/proxy/protect/integration/v1/cameras") or []

    def get_camera_snapshot(self, camera_id: str) -> Tuple[bytes, str]:
        """
        Returns (image_bytes, mimetype).
        Tries multiple endpoints because Protect snapshot routes differ between releases/models.
        """
        candidates = [
            f"/proxy/protect/integration/v1/cameras/{camera_id}/snapshot",
            f"/proxy/protect/integration/v1/cameras/{camera_id}/snapshot.jpg",
            f"/proxy/protect/integration/v1/cameras/{camera_id}/snapshot?format=jpeg",
            f"/proxy/protect/integration/v1/cameras/{camera_id}/snapshot?force=true",
            f"/proxy/protect/integration/v1/cameras/{camera_id}/snapshot?force=true&format=jpeg",
        ]

        last_err = None
        for path in candidates:
            try:
                data, headers = self._req(
                    "GET",
                    path,
                    headers={"Accept": "image/jpeg,image/png,image/*,*/*"},
                    stream=True,
                )
                ctype = (headers.get("Content-Type") or "").split(";")[0].strip().lower()
                if not ctype:
                    ctype = "image/jpeg"
                if isinstance(data, (bytes, bytearray)) and len(data) > 1000:
                    return bytes(data), ctype
            except Exception as e:
                last_err = e
                continue

        raise RuntimeError(f"Snapshot not available for camera {camera_id}. Last error: {last_err}")

    # ---- Vision (OpenAI-compatible)
    def call_vision(self, image_bytes: bytes, *, prompt: str, filename: str = "image.jpg") -> str:
        """
        Call OpenAI-compatible vision endpoint (separate from UniFi).
        """
        url = f"{self.vision_api_base}/v1/chat/completions"
        data_url = _to_data_url(image_bytes, filename)

        payload = {
            "model": self.vision_model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt or "Describe this image."},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
            "temperature": 0.2,
            "max_tokens": 500,
        }

        headers = {"Content-Type": "application/json"}
        if self.vision_api_key:
            headers["Authorization"] = f"Bearer {self.vision_api_key}"

        resp = requests.post(url, json=payload, headers=headers, timeout=90)
        if resp.status_code != 200:
            return f"(vision error {resp.status_code}) {resp.text[:200]}"

        j = resp.json()
        try:
            return (j["choices"][0]["message"]["content"] or "").strip()
        except Exception:
            return "(vision error) Unexpected response"


# ----------------------------
# Plugin
# ----------------------------
class UniFiProtectPlugin(ToolPlugin):
    """
    UniFi Protect (Integration API) plugin.

    What it can do (with what we know works right now):
    - Sensors:
      - "are any doors open?"
      - "when was the garage door last opened?"
      - "how are the sensors doing?"
      - "any low battery sensors?"
    - Cameras:
      - "list cameras"
      - "what's going on in the front yard?" (takes snapshots from matching cameras and runs Vision model)
      - "describe the doorbell" (snapshot + vision summary)

    Notes:
    - Some event endpoints (subscribe/events, smartDetects) are NOT exposed on your console right now
      and return "Entity 'endpoint' not found". This plugin does NOT rely on those.
    """

    name = "unifi_protect"
    plugin_name = "UniFi Protect"
    version = "1.0.5"
    min_tater_version = "50"
    pretty_name = "UniFi Protect"
    settings_category = "UniFi Protect"

    platforms = ["webui", "homeassistant", "homekit", "xbmc", "discord", "telegram", "matrix", "irc"]

    usage = (
        "{\n"
        '  "function": "unifi_protect",\n'
        '  "arguments": {\n'
        '    "action": "Optional: sensors_status|sensor_detail|list_cameras|describe_camera|describe_area.",\n'
        '    "target": "Optional name hint (front door, garage, doorbell, front yard).",\n'
        '    "camera": "Optional camera name (alias of target for describe_camera).",\n'
        '    "area": "Optional area name (alias of target for describe_area).",\n'
        '    "query": "Optional: user request text; plugin will infer action/target if omitted."\n'
        "  }\n"
        "}\n"
    )

    description = (
        "Query UniFi Protect sensors (door/motion/temp/humidity/battery) and cameras "
        "(list + snapshot vision summaries). Uses the UniFi Protect Integration API via your console proxy."
    )
    plugin_dec = "Get UniFi Protect sensor status and camera snapshot vision summaries."
    when_to_use = (
        "Use for UniFi Protect camera snapshots or descriptions (e.g., 'what does the front yard look like', "
        "'what's happening in the back yard', 'describe the porch/driveway/garage'), plus sensor status or camera lists."
    )
    common_needs = ["camera or area name (optional if already in the request)"]
    required_args = []
    optional_args = ["action", "target", "camera", "area", "query"]
    missing_info_prompts = [
        "Which camera or area should I describe (for example: back yard, front door, garage)?",
    ]

    argument_schema = {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["sensors_status", "sensor_detail", "list_cameras", "describe_camera", "describe_area"],
                "description": "Optional action (plugin can infer from query).",
            },
            "target": {
                "type": "string",
                "description": "Name hint for sensor/camera/area (front door, garage, back yard).",
            },
            "camera": {
                "type": "string",
                "description": "Camera name (alias of target for describe_camera).",
            },
            "area": {
                "type": "string",
                "description": "Area name (alias of target for describe_area).",
            },
            "query": {
                "type": "string",
                "description": "Optional: user request text to infer action/target.",
            },
        },
        "required": [],
    }

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re checking UniFi Protect now. "
        "Only output that message."
    )

    required_settings = {
        # Protect
        "UNIFI_PROTECT_BASE_URL": {
            "label": "UniFi Console Base URL",
            "type": "text",
            "default": "https://10.4.20.127",
            "description": "Base URL of your UniFi console (example: https://10.4.20.127).",
        },
        "UNIFI_PROTECT_API_KEY": {
            "label": "UniFi Protect Integration API Key",
            "type": "text",
            "default": "",
            "description": "API key used as X-API-KEY header for Protect Integration endpoints.",
        },
        # Vision (OpenAI-compatible)
        "VISION_API_BASE": {
            "label": "Vision API Base URL",
            "type": "text",
            "default": "http://127.0.0.1:1234",
            "description": "OpenAI-compatible base URL for vision (example: http://127.0.0.1:1234).",
        },
        "VISION_MODEL": {
            "label": "Vision Model",
            "type": "text",
            "default": "qwen2.5-vl-7b-instruct",
            "description": "OpenAI-compatible vision model name (example: qwen2.5-vl-7b-instruct).",
        },
        "VISION_API_KEY": {
            "label": "Vision API Key (optional)",
            "type": "text",
            "default": "",
            "description": "Optional. If blank, OPENAI_API_KEY env var will be used if present.",
        },
    }

    # ----------------------------
    # Settings / client
    # ----------------------------
    def _get_client(self) -> Optional[ProtectClient]:
        try:
            return ProtectClient()
        except Exception as e:
            logger.error(f"[unifi_protect] Failed to init client: {e}")
            return None

    # ----------------------------
    # LLM: decide action
    # ----------------------------
    async def _decide_action(self, user_query: str, llm_client) -> dict:
        """
        Ask LLM to choose a small action plan.
        """
        first, last = get_tater_name()
        assistant_name = f"{first} {last}".strip() or "Tater"

        system = (
            f"You are {assistant_name}, routing a UniFi Protect request.\n"
            "Return STRICT JSON only.\n"
            "Schema:\n"
            "{\n"
            '  "action": "sensors_status|sensor_detail|list_cameras|describe_camera|describe_area",\n'
            '  "target": "optional name hint like front door, garage, doorbell, front yard"\n'
            "}\n"
            "Rules:\n"
            "- If user asks about doors/windows/sensors/battery/temp/humidity: use sensors_status.\n"
            "- If user asks about a specific sensor (front door/back door/garage door): use sensor_detail and set target.\n"
            "- If user asks to list cameras: list_cameras.\n"
            "- If user asks what's going on in a place (front yard/front door/porch/back yard): describe_area and set target.\n"
            "- If user asks to describe a specific camera (doorbell/front door/garage): describe_camera and set target.\n"
        )

        resp = await llm_client.chat(messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": (user_query or "").strip()},
        ])
        content = _strip_code_fences((resp.get("message", {}) or {}).get("content", ""))
        try:
            data = json.loads(content)
            if isinstance(data, dict) and data.get("action"):
                return data
        except Exception:
            pass

        # fallback
        q = (user_query or "").lower()
        if any(w in q for w in ["camera", "cameras"]):
            return {"action": "list_cameras", "target": ""}
        return {"action": "sensors_status", "target": ""}

    # ----------------------------
    # Data shaping: sensors
    # ----------------------------
    def _compact_sensors(self, sensors: List[dict]) -> dict:
        """
        Make a compact facts blob so LLM is fast + less likely to hallucinate.
        """
        items = []
        open_sensors = []
        low_batt = []

        for s in sensors or []:
            if not isinstance(s, dict):
                continue
            name = _coerce_str(s.get("name") or "Unknown")
            state = _coerce_str(s.get("state") or "")
            mount = _coerce_str(s.get("mountType") or "")

            batt = (s.get("batteryStatus") or {})
            batt_pct = batt.get("percentage")
            batt_low = batt.get("isLow")

            is_open = bool(s.get("isOpened"))
            open_changed = s.get("openStatusChangedAt")

            stats = (s.get("stats") or {})
            temp_c = ((stats.get("temperature") or {}).get("value"))
            hum = ((stats.get("humidity") or {}).get("value"))

            item = {
                "id": s.get("id"),
                "name": name,
                "state": state,
                "mountType": mount,
                "isOpened": is_open,
                "openStatusChangedAt": _ms_to_local_str(open_changed),
                "batteryPct": batt_pct,
                "batteryLow": batt_low,
                "tempC": temp_c,
                "tempF": _c_to_f(temp_c),
                "humidity": hum,
            }
            items.append(item)

            if is_open:
                open_sensors.append({"name": name, "changed": _ms_to_local_str(open_changed)})
            if batt_low is True or (isinstance(batt_pct, (int, float)) and batt_pct <= 20):
                low_batt.append({"name": name, "batteryPct": batt_pct})

        return {
            "total": len(items),
            "open": open_sensors[:10],
            "lowBattery": low_batt[:10],
            "items": items,  # keep full list for follow-up questions (still compact)
        }

    def _find_best_named(self, items: List[dict], name_hint: str) -> Optional[dict]:
        """
        Find a sensor/camera by fuzzy name match.
        """
        hint = (name_hint or "").strip().lower()
        if not hint:
            return None

        # exact contains match
        for it in items:
            n = (it.get("name") or "").strip().lower()
            if hint == n:
                return it
        for it in items:
            n = (it.get("name") or "").strip().lower()
            if hint in n:
                return it

        # loose keyword match
        hint_words = [w for w in re.split(r"\s+", hint) if w]
        best = None
        best_score = 0
        for it in items:
            n = (it.get("name") or "").strip().lower()
            score = sum(1 for w in hint_words if w in n)
            if score > best_score:
                best_score = score
                best = it
        return best if best_score > 0 else None

    # ----------------------------
    # Data shaping: cameras
    # ----------------------------
    def _compact_cameras(self, cams: List[dict]) -> List[dict]:
        out = []
        for c in cams or []:
            if not isinstance(c, dict):
                continue
            out.append({
                "id": c.get("id"),
                "name": c.get("name"),
                "state": c.get("state"),
                "modelKey": c.get("modelKey"),
                "hasSpeaker": ((c.get("featureFlags") or {}).get("hasSpeaker")),
                "smartDetectTypes": ((c.get("featureFlags") or {}).get("smartDetectTypes")) or [],
            })
        return out

    def _area_keywords(self, target: str) -> List[str]:
        t = (target or "").lower().strip()
        if not t:
            return []

        # Normalize common variants
        t = t.replace("backyard", "back yard").replace("frontyard", "front yard")

        mapping = {
            # --- Front ---
            "front yard": ["front yard", "frontyard", "front"],
            "front door": ["front door", "frontdoor"],
            "doorbell": ["doorbell"],

            # Keep "porch" specific so it doesn't match both sides
            "front porch": ["front porch", "frontporch", "porch front", "front porch cam"],
            "porch": ["porch"],  # neutral porch keyword only, see selection logic below

            # --- Back ---
            "back yard": ["back yard", "backyard", "back"],  # removed generic "yard"
            "back porch": ["back porch", "backporch", "porch back", "back patio", "patio back"],
            "patio": ["patio"],

            # --- Other ---
            "garage": ["garage"],
            "door": ["door", "front door", "back door", "doorbell"],
        }

        # Exact mapping key match
        if t in mapping:
            return mapping[t]

        # If user said "back porch" or "front porch" in a longer phrase, detect it
        if "back porch" in t or ("porch" in t and "back" in t):
            return mapping["back porch"]
        if "front porch" in t or ("porch" in t and "front" in t):
            return mapping["front porch"]

        # If user says "back yard" or "front yard" inside a longer phrase
        if "back yard" in t or ("yard" in t and "back" in t):
            return mapping["back yard"]
        if "front yard" in t or ("yard" in t and "front" in t):
            return mapping["front yard"]

        # Default: use the literal phrase as a keyword
        return [t]

    def _pick_cameras_for_area(self, cameras: List[dict], target: str) -> List[dict]:
        keys = self._area_keywords(target)
        if not keys:
            return cameras[:3]

        picked = []
        t = (target or "").lower()

        wants_front = "front" in t
        wants_back = "back" in t
        wants_garage = "garage" in t

        for c in cameras:
            name = (c.get("name") or "").lower()

            # ---- Direction bias for porch ----
            if "porch" in keys and (wants_front or wants_back):
                if wants_front and "front" not in name:
                    continue
                if wants_back and "back" not in name:
                    continue

            # ---- Garage logic ----
            if "garage" in name:
                if wants_garage:
                    picked.append(c)
                    continue
                if wants_front and not wants_back:
                    picked.append(c)  # treat garage as front
                    continue
                if wants_back:
                    continue  # exclude garage from back queries

            # ---- Normal keyword match ----
            if any(k in name for k in keys):
                picked.append(c)

        return picked

    # ----------------------------
    # Vision prompt
    # ----------------------------
    def _vision_prompt_for_query(self, user_query: str, camera_name: str) -> str:
        """
        Keep it factual and useful for a spoken summary.
        """
        q = (user_query or "").strip()
        cam = (camera_name or "Camera").strip()
        return (
            f"You are a careful visual assistant.\n"
            f"Camera: {cam}\n"
            f"User asked: {q}\n\n"
            "Describe ONLY what you can actually see in the image.\n"
            "Mention notable people, vehicles, packages, animals, doors open/closed, and obvious activity.\n"
            "If nothing notable is happening, say so.\n"
            "Be concise (2-6 sentences)."
        )

    # ----------------------------
    # LLM: final answer using facts
    # ----------------------------
    async def _answer_with_facts(self, user_query: str, facts: Any, llm_client) -> str:
        first, last = get_tater_name()
        personality = get_tater_personality() or ""

        assistant_name = f"{(first or '').strip()} {(last or '').strip()}".strip() or "Tater"

        system = (
            f"You are {assistant_name}, a helpful home-lab assistant.\n"
            "You must answer using ONLY the provided FACTS.\n"
            "If the facts do not contain the requested detail, say what is missing.\n"
            "Keep the response short, spoken-friendly, and avoid technical API terms.\n"
            "If you are given camera_descriptions, base your answer primarily on those.\n"
        )
        if personality.strip():
            system += f"\nStyle/personality:\n{personality.strip()}\n"

        user = json.dumps({
            "user_query": user_query,
            "facts": facts,
        }, ensure_ascii=False)

        resp = await llm_client.chat(messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ])
        return _coerce_str((resp.get("message", {}) or {}).get("content", "")).strip() or "Done."

    # ----------------------------
    # Platform handlers
    # ----------------------------
    async def handle_webui(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="webui", context=context)

    async def handle_homeassistant(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="homeassistant", context=context)

    async def handle_homekit(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="homekit", context=context)

    async def handle_xbmc(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="xbmc", context=context)

    async def handle_discord(self, message, args, llm_client, context=None):
        ctx = context or {}
        if "message" not in ctx:
            ctx["message"] = message
        return await self._handle(args, llm_client, platform="discord", context=ctx)

    async def handle_telegram(self, update, args, llm_client, context=None):
        ctx = context or {}
        if "update" not in ctx:
            ctx["update"] = update
        return await self._handle(args, llm_client, platform="telegram", context=ctx)

    async def handle_matrix(self, client, room, sender, body, args, llm_client, context=None):
        ctx = context or {}
        if "body" not in ctx:
            ctx["body"] = body
        return await self._handle(args, llm_client, platform="matrix", context=ctx)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client, context=None):
        ctx = context or {}
        if "raw_message" not in ctx:
            ctx["raw_message"] = raw_message
        return await self._handle(args, llm_client, platform="irc", context=ctx)

    # ----------------------------
    # Core
    # ----------------------------
    async def _handle(self, args, llm_client, platform: str = "webui", context: Optional[dict] = None):
        client = self._get_client()
        if not client:
            return (
                "UniFi Protect is not configured. "
                "Please set UNIFI_PROTECT_BASE_URL and UNIFI_PROTECT_API_KEY in plugin settings."
            )

        args = args or {}
        context = context or {}
        action_raw = (args.get("action") or "").strip()
        target = (
            args.get("target")
            or args.get("camera")
            or args.get("area")
            or args.get("camera_name")
            or args.get("area_name")
            or ""
        )
        target = str(target).strip()
        query = (args.get("query") or "").strip()
        if not query:
            query = _extract_context_text(context)

        action, action_target = _split_action_and_target(action_raw)
        if action_target and not target:
            target = action_target

        allowed_actions = {"sensors_status", "sensor_detail", "list_cameras", "describe_camera", "describe_area"}

        if action and action not in allowed_actions:
            guessed = _guess_action_from_text(action, target)
            if guessed:
                action = guessed
            else:
                action = ""

        if not action:
            if query:
                action = _guess_action_from_text(query, target)
            if not action and query:
                plan = await self._decide_action(query, llm_client)
                action = (plan.get("action") or "").strip().lower()
                target = target or (plan.get("target") or "").strip()

        if not action:
            return action_failure(
                code="invalid_action",
                message="I couldn't determine which UniFi Protect action to run.",
                needs=[
                    "Do you want sensors status, list cameras, or a camera/area description?",
                    "Which camera or area should I describe (for example: back yard, front door, garage)?",
                ],
                say_hint="Explain that more detail is needed and ask the follow-up questions.",
            )

        if not query:
            query = f"{action} {target}".strip()

        if action == "sensors_status" and target:
            action = "sensor_detail"

        if action == "describe_camera" and not target and _looks_like_area(query):
            action = "describe_area"

        # ---- Sensors status / detail
        if action in ("sensors_status", "sensor_detail"):
            try:
                sensors = client.list_sensors()
            except Exception as e:
                logger.error(f"[unifi_protect] list_sensors error: {e}")
                return f"I couldn't reach UniFi Protect sensors. {e}"

            compact = self._compact_sensors(sensors)

            if action == "sensor_detail":
                hit = self._find_best_named(compact.get("items") or [], target or query)
                if not hit:
                    facts = {"sensors": compact, "note": f"No specific sensor matched: {target or query}"}
                    return await self._answer_with_facts(query, facts, llm_client)

                # Optionally fetch full detail
                try:
                    full = client.get_sensor(_coerce_str(hit.get("id")))
                except Exception:
                    full = None

                facts = {"sensor": hit, "sensor_detail": full}
                return await self._answer_with_facts(query, facts, llm_client)

            facts = {"sensors": compact}
            return await self._answer_with_facts(query, facts, llm_client)

        # ---- Cameras list
        if action == "list_cameras":
            try:
                cams = client.list_cameras()
            except Exception as e:
                logger.error(f"[unifi_protect] list_cameras error: {e}")
                return f"I couldn't list cameras. {e}"

            facts = {"cameras": self._compact_cameras(cams), "total": len(cams or [])}
            return await self._answer_with_facts(query, facts, llm_client)

        # ---- Describe a specific camera (snapshot + vision + summary)
        if action == "describe_camera":
            try:
                cams = client.list_cameras()
            except Exception as e:
                logger.error(f"[unifi_protect] list_cameras error: {e}")
                return f"I couldn't list cameras. {e}"

            compact_cams = self._compact_cameras(cams)
            hit = self._find_best_named(compact_cams, target or query)
            if not hit:
                facts = {"cameras": compact_cams, "note": f"No camera matched: {target or query}"}
                return await self._answer_with_facts(query, facts, llm_client)

            cam_id = _coerce_str(hit.get("id"))
            cam_name = _coerce_str(hit.get("name") or "Camera")

            try:
                img_bytes, mimetype = client.get_camera_snapshot(cam_id)
            except Exception as e:
                logger.error(f"[unifi_protect] snapshot error: {e}")
                facts = {"camera": hit, "note": f"Snapshot not available for that camera: {e}"}
                return await self._answer_with_facts(query, facts, llm_client)

            # Vision describe (run in thread; requests is blocking)
            vision_prompt = self._vision_prompt_for_query(query, cam_name)
            try:
                vision_text = await asyncio.to_thread(
                    client.call_vision, img_bytes, prompt=vision_prompt, filename=f"{cam_name}.jpg"
                )
            except Exception as e:
                vision_text = f"(vision error) {e}"

            facts = {
                "camera": hit,
                "vision_description": vision_text,
                "snapshot_meta": {"mimetype": mimetype, "bytes": len(img_bytes)},
            }

            text = await self._answer_with_facts(query, facts, llm_client)

            # WebUI can render image blobs. Others: return text only (spoken-friendly).
            if _platform_supports_media(platform):
                return [
                    _build_media_metadata(
                        img_bytes,
                        media_type="image",
                        name=f"{cam_name}.jpg",
                        mimetype=mimetype,
                    ),
                    text,
                ]

            return text

        # ---- Describe an area (multiple snapshots + per-cam vision + combined summary)
        if action == "describe_area":
            try:
                cams = client.list_cameras()
            except Exception as e:
                logger.error(f"[unifi_protect] list_cameras error: {e}")
                return f"I couldn't list cameras. {e}"

            compact_cams = self._compact_cameras(cams)
            picked = self._pick_cameras_for_area(compact_cams, target or query)
            if not picked:
                facts = {"cameras": compact_cams, "note": f"No cameras matched area: {target or query}"}
                return await self._answer_with_facts(query, facts, llm_client)

            # We'll do up to 6 snapshots max (keeps it reasonable)
            picked = picked[:6]

            images_out = []
            camera_descriptions = []
            snapshot_results = []

            for cam in picked:
                cam_id = _coerce_str(cam.get("id"))
                cam_name = _coerce_str(cam.get("name") or "Camera")

                try:
                    img_bytes, mimetype = client.get_camera_snapshot(cam_id)

                    # Vision describe
                    vision_prompt = self._vision_prompt_for_query(query, cam_name)
                    try:
                        vision_text = await asyncio.to_thread(
                            client.call_vision, img_bytes, prompt=vision_prompt, filename=f"{cam_name}.jpg"
                        )
                    except Exception as e:
                        vision_text = f"(vision error) {e}"

                    camera_descriptions.append({
                        "camera": cam_name,
                        "state": cam.get("state"),
                        "vision": vision_text,
                    })
                    snapshot_results.append({
                        "camera": cam_name,
                        "mimetype": mimetype,
                        "bytes": len(img_bytes),
                        "ok": True,
                    })

                    if _platform_supports_media(platform):
                        images_out.append(
                            _build_media_metadata(
                                img_bytes,
                                media_type="image",
                                name=f"{cam_name}.jpg",
                                mimetype=mimetype,
                            )
                        )

                except Exception as e:
                    logger.info(f"[unifi_protect] snapshot/vision failed for {cam_name}: {e}")
                    camera_descriptions.append({
                        "camera": cam_name,
                        "state": cam.get("state"),
                        "vision": f"(snapshot error) {e}",
                    })
                    snapshot_results.append({
                        "camera": cam_name,
                        "ok": False,
                        "error": str(e),
                        "state": cam.get("state"),
                    })

            facts = {
                "area": target or "",
                "cameras_used": [c.get("name") for c in picked],
                "camera_descriptions": camera_descriptions,
                "snapshot_results": snapshot_results,
            }

            text = await self._answer_with_facts(query, facts, llm_client)

            if _platform_supports_media(platform):
                return images_out + [text]
            return text

        # fallback
        return "I couldn't figure out what to do with that request."


plugin = UniFiProtectPlugin()
