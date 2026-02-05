# plugins/camera_event.py
import json
import base64
import logging
import time
import re
from typing import Any, Dict, Optional
from urllib.parse import quote
from datetime import datetime

import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("camera_event")
logger.setLevel(logging.INFO)


class CameraEventPlugin(ToolPlugin):
    """
    Automation-only: On motion trigger, fetch a snapshot from a Home Assistant camera,
    analyze with a Vision LLM to produce a short description, and record it as a
    durable event via the Automations /events/add endpoint for later retrieval.
    Includes a per-camera cooldown (configured in settings).

    Stores events per-area (e.g., 'front_yard', 'back_yard') and stamps with HA local-naive ISO time.
    """
    name = "camera_event"
    plugin_name = "Camera Event"
    version = "1.0.1"
    min_tater_version = "50"
    description = "Camera event tool for when the user requests or says to run camera event."
    plugin_dec = "Capture a Home Assistant camera snapshot, describe it with vision AI, and log the event."
    usage = (
        "{\n"
        '  "function": "camera_event",\n'
        '  "arguments": {\n'
        '    "area": "front yard | back yard | garage | ...",\n'
        '    "camera": "camera.front_door_high"\n'
        "  }\n"
        "}\n"
    )

    platforms = ["automation"]

    settings_category = "Camera Event"
    required_settings = {
        "TIME_SENSOR_ENTITY": {
            "label": "Time Sensor (ISO)",
            "type": "string",
            "default": "sensor.date_time_iso",
            "description": "Sensor with local-naive ISO time (e.g., 2025-10-19T20:07:00)."
        },

        # ---- Vision LLM ----
        "VISION_API_BASE": {
            "label": "Vision API Base URL",
            "type": "string",
            "default": "http://127.0.0.1:1234",
            "description": "OpenAI-compatible base (e.g., http://127.0.0.1:1234)."
        },
        "VISION_MODEL": {
            "label": "Vision Model",
            "type": "string",
            "default": "gemma3-27b-abliterated-dpo",
            "description": "OpenAI-compatible model name (e.g., qwen2.5-vl-7b-instruct)."
        },
        "VISION_API_KEY": {
            "label": "Vision API Key",
            "type": "string",
            "default": "",
            "description": "Optional; leave blank for local stacks."
        },

        # ---- Behavior ----
        "DEFAULT_COOLDOWN_SECONDS": {
            "label": "Per-Camera Cooldown (seconds)",
            "type": "number",
            "default": 30,
            "description": "Skip logging if the same camera fired more recently than this."
        },
    }

    # ---------- Internal helpers ----------
    def _get_settings(self) -> Dict[str, str]:
        s = redis_client.hgetall(f"plugin_settings:{self.settings_category}") or \
            redis_client.hgetall(f"plugin_settings: {self.settings_category}")
        return s or {}

    def _ha(self, s: Dict[str, str]) -> Dict[str, str]:
        ha_settings = redis_client.hgetall("homeassistant_settings") or {}
        base = (ha_settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        token = (ha_settings.get("HA_TOKEN") or "").strip()
        if not token:
            raise ValueError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )
        time_sensor = (s.get("TIME_SENSOR_ENTITY") or "sensor.date_time_iso").strip()
        return {"base": base, "token": token, "time_sensor": time_sensor}

    def _vision(self, s: Dict[str, str]) -> Dict[str, Optional[str]]:
        api_base = (s.get("VISION_API_BASE") or "http://127.0.0.1:1234").rstrip("/")
        model = s.get("VISION_MODEL") or "gemma3-27b-abliterated-dpo"
        api_key = s.get("VISION_API_KEY") or None
        return {"api_base": api_base, "model": model, "api_key": api_key}

    def _automation_base_url(self) -> str:
        """
        Resolve Automations bridge base URL via Redis:
          hash: 'ha_automations_platform_settings' -> 'bind_port'
        """
        try:
            raw_port = redis_client.hget("ha_automations_platform_settings", "bind_port")
            port = int(raw_port) if raw_port is not None else 8788
        except Exception:
            port = 8788
        return f"http://localhost:{port}"

    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    @staticmethod
    def _slug(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"\s+", "_", s)
        s = re.sub(r"[^a-z0-9_:-]", "", s)
        return s or "unknown"

    @staticmethod
    def _format_iso_naive(dt: datetime) -> str:
        """Ensure strict ISO without timezone or microseconds: YYYY-MM-DDTHH:MM:SS"""
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    def _ha_now(self, ha_base: str, token: str, sensor_entity: str) -> datetime:
        """
        Use HA's reported time exactly as local-naive time.
        If HA includes a timezone offset, strip it. Fallback to local system time.
        """
        try:
            url = f"{ha_base}/api/states/{sensor_entity}"
            r = requests.get(url, headers=self._ha_headers(token), timeout=5)
            if r.status_code < 400:
                state = (r.json() or {}).get("state", "")
                if state and state not in ("unknown", "unavailable"):
                    dt = datetime.fromisoformat(state)
                    return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except Exception:
            logger.info("[camera_event] HA time sensor fetch failed; using local system time")
        return datetime.now()

    def _get_camera_jpeg(self, ha_base: str, token: str, camera_entity: str) -> bytes:
        """
        GET /api/camera_proxy/<entity_id>  → current still image (JPEG/PNG).
        """
        url = f"{ha_base}/api/camera_proxy/{quote(camera_entity, safe='')}"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        if r.status_code >= 400:
            raise RuntimeError(f"camera_proxy HTTP {r.status_code}: {r.text[:200]}")
        return r.content

    def _vision_describe(self, image_bytes: bytes, api_base: str, model: str, api_key: Optional[str]) -> str:
        """
        OpenAI-compatible /v1/chat/completions with an image URL (data URL).
        Returns a concise, natural sentence suitable for logs/reading.
        """
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        data_url = f"data:image/jpeg;base64,{b64}"

        prompt = (
            "Write one clear sentence describing what the camera sees. "
            "If a person is visible, note clothing or uniform and packages. "
            "If no person, describe relevant scene elements like vehicles, pets, or packages. "
            "No prefaces, no meta text—just the sentence."
        )

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a concise vision assistant."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
            ],
            "temperature": 0.2,
            "max_tokens": 120,
        }

        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        url = f"{api_base}/v1/chat/completions"
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"Vision HTTP {r.status_code}: {r.text[:200]}")
        res = r.json()
        text = (res.get("choices", [{}])[0].get("message", {}).get("content", "") or "").strip()
        return text or "Motion event detected."

    # -------- Cooldown tracking (per camera) --------
    def _cooldown_key(self, camera_entity: str) -> str:
        return f"tater:camera_event:last_ts:{camera_entity}"

    def _within_cooldown(self, camera_entity: str, cooldown_seconds: int) -> bool:
        try:
            last = redis_client.get(self._cooldown_key(camera_entity))
            if not last:
                return False
            last_ts = int(last)
            return (int(time.time()) - last_ts) < max(0, int(cooldown_seconds))
        except Exception:
            return False

    def _mark_fired(self, camera_entity: str) -> None:
        try:
            redis_client.set(self._cooldown_key(camera_entity), str(int(time.time())))
        except Exception:
            logger.warning("[camera_event] failed to set cooldown key for %s", camera_entity)

    # -------- Events endpoint --------
    def _post_event(self, *, source: str, title: str, message: str, entity_id: str, area: Optional[str], ha_time: str) -> None:
        base = self._automation_base_url()
        url = f"{base}/tater-ha/v1/events/add"
        payload = {
            "source": source,  # per-area slug (e.g., 'front_yard')
            "title": title,
            "type": "camera_motion",
            "message": message,
            "entity_id": entity_id,
            "level": "info",
            "ha_time": ha_time,  # strict local-naive ISO: YYYY-MM-DDTHH:MM:SS
            "data": {"area": (area or "").strip()},
        }
        try:
            r = requests.post(url, json=payload, timeout=5)
            if r.status_code >= 400:
                logger.warning("[camera_event] events/add failed %s: %s", r.status_code, r.text[:200])
        except Exception as e:
            logger.warning("[camera_event] events/add error: %s", e)

    # ---------- Automation entrypoint ----------
    async def handle_automation(self, args: Dict[str, Any], llm_client) -> Any:
        """
        Expected args:
        {
          "area": "front yard",
          "camera": "camera.front_door_high"
        }
        """
        area = (args.get("area") or "").strip()
        camera = (args.get("camera") or "").strip()

        if not area:
            raise ValueError("Missing 'area' (e.g., 'front yard').")
        if not camera:
            raise ValueError("Missing 'camera' (e.g., 'camera.front_door_high').")

        s = self._get_settings()
        ha = self._ha(s)
        vis = self._vision(s)

        # Cooldown from settings ONLY
        try:
            cooldown_seconds = int(s.get("DEFAULT_COOLDOWN_SECONDS", 30))
        except Exception:
            cooldown_seconds = 30

        if self._within_cooldown(camera, cooldown_seconds):
            logger.info("[camera_event] Skipping due to cooldown for %s (%ss)", camera, cooldown_seconds)
            return {
                "ok": True,
                "skipped": "cooldown",
                "camera": camera,
                "area": area,
                "cooldown_seconds": cooldown_seconds
            }

        # Current HA local time (naive) for event stamping
        now_local = self._ha_now(ha["base"], ha["token"], ha["time_sensor"])
        now_iso = self._format_iso_naive(now_local)

        # Fetch snapshot
        try:
            jpeg = self._get_camera_jpeg(ha["base"], ha["token"], camera)
        except Exception:
            logger.exception("[camera_event] Failed to fetch camera snapshot")
            # Still log an event with generic text so timeline remains useful
            generic = "Motion event detected, but no snapshot was available."
            title = f"{area.title()} motion"
            self._post_event(
                source=self._slug(area),   # <-- per-area source
                title=title,
                message=generic,
                entity_id=camera,
                area=area,
                ha_time=now_iso,
            )
            self._mark_fired(camera)
            return {"ok": True, "event": "logged_generic_no_snapshot", "camera": camera, "area": area}

        # Vision description
        try:
            desc = self._vision_describe(jpeg, vis["api_base"], vis["model"], vis["api_key"])
        except Exception:
            logger.exception("[camera_event] Vision analysis failed")
            desc = "Motion event detected."

        # Store event (per-area source)
        title = f"{area.title()} motion"
        self._post_event(
            source=self._slug(area),   # <-- per-area source
            title=title,
            message=desc,
            entity_id=camera,
            area=area,
            ha_time=now_iso,
        )

        # Mark cooldown
        self._mark_fired(camera)

        # Platform ignores the return; for logs/tracing only
        return {"ok": True, "stored": True, "camera": camera, "area": area, "cooldown_seconds": cooldown_seconds}

plugin = CameraEventPlugin()
