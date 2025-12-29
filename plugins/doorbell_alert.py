# plugins/doorbell_alert.py
import json
import base64
import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote
from datetime import datetime

import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("doorbell_alert")
logger.setLevel(logging.INFO)


class DoorbellAlertPlugin(ToolPlugin):
    """
    Automation-only: When triggered, fetch a snapshot from a Home Assistant camera,
    ask a Vision LLM to describe it briefly, speak via TTS, and (optionally) post
    notifications + durable events.

    Changes:
    - Events are stored per-area by setting 'source' to a slug of the area
      (e.g., 'front door' -> 'front_door').
    - ha_time is strict naive ISO 'YYYY-MM-DDTHH:MM:SS' (no timezone).
    """
    name = "doorbell_alert"
    plugin_name = "Doorbell Alert"
    description = "Doorbell alert tool for when the user requests or says to run a doorbell alert."
    plugin_dec = "Handle doorbell events: snapshot, describe with vision, announce, and log notifications."
    usage = (
        "{\n"
        '  "function": "doorbell_alert",\n'
        '  "arguments": {\n'
        '    // optional overrides\n'
        '    "camera": "camera.doorbell_high",\n'
        '    "players": ["media_player.kitchen"],\n'
        '    "tts_entity": "tts.piper",\n'
        '    "notifications": true,\n'
        '    "area": "front door"\n'
        "  }\n"
        "}\n"
    )

    platforms = ["automation"]

    settings_category = "Doorbell Alert"
    required_settings = {
        # ---- Home Assistant ----
        "HA_BASE_URL": {
            "label": "Home Assistant Base URL",
            "type": "string",
            "default": "http://homeassistant.local:8123",
            "description": "Base URL of your Home Assistant instance."
        },
        "HA_TOKEN": {
            "label": "Home Assistant Long-Lived Token",
            "type": "string",
            "default": "",
            "description": "Create in HA: Profile → Long-Lived Access Tokens."
        },
        "TTS_ENTITY": {
            "label": "TTS Entity",
            "type": "string",
            "default": "tts.piper",
            "description": "TTS entity to use (e.g., tts.piper)."
        },

        # Prefer TIME_SENSOR_ENTITY; legacy HA_TIME_ENTITY still accepted.
        "TIME_SENSOR_ENTITY": {
            "label": "Time Sensor (ISO)",
            "type": "string",
            "default": "sensor.date_time_iso",
            "description": "Entity that provides a local-naive ISO string like 2025-10-19T20:07:00."
        },

        # ---- Defaults ----
        "CAMERA_ENTITY": {
            "label": "Camera Entity",
            "type": "string",
            "default": "camera.doorbell_high",
            "description": "Default camera entity for doorbell snapshots."
        },
        "MEDIA_PLAYERS": {
            "label": "Media Players",
            "type": "text",
            "default": "media_player.living_room\nmedia_player.kitchen",
            "description": "One media_player entity per line (newline or comma separated)."
        },
        "NOTIFICATIONS_ENABLED": {
            "label": "Enable Notifications by Default",
            "type": "boolean",
            "default": False,
            "description": "If true, also post alerts to the HA notification queue and events."
        },
        "AREA_LABEL": {
            "label": "Area Label",
            "type": "string",
            "default": "front door",
            "description": "Area tag saved with events (e.g., 'front door', 'porch')."
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
            "description": "OpenAI-compatible model name (qwen2.5-vl-7b-instruct, etc.)."
        },
        "VISION_API_KEY": {
            "label": "Vision API Key",
            "type": "string",
            "default": "",
            "description": "Optional; leave blank for local stacks."
        },
    }

    # ---------- Utils ----------
    @staticmethod
    def _slug(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"\s+", "_", s)
        s = re.sub(r"[^a-z0-9_:-]", "", s)
        return s or "unknown"

    @staticmethod
    def _iso_naive_now() -> str:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # ---------- Settings / HA ----------
    def _get_settings(self) -> Dict[str, str]:
        s = redis_client.hgetall(f"plugin_settings:{self.settings_category}") or \
            redis_client.hgetall(f"plugin_settings: {self.settings_category}")
        return s or {}

    def _ha(self, s: Dict[str, str]) -> Dict[str, str]:
        base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        token = s.get("HA_TOKEN") or ""
        if not token:
            raise ValueError("HA_TOKEN is missing in Doorbell Alert settings.")
        tts_entity = (s.get("TTS_ENTITY") or "tts.piper").strip() or "tts.piper"
        time_entity = (s.get("TIME_SENSOR_ENTITY") or s.get("HA_TIME_ENTITY") or "sensor.date_time_iso").strip()
        return {"base": base, "token": token, "tts_entity": tts_entity, "time_entity": time_entity}

    def _vision(self, s: Dict[str, str]) -> Dict[str, Optional[str]]:
        api_base = (s.get("VISION_API_BASE") or "http://127.0.0.1:1234").rstrip("/")
        model = s.get("VISION_MODEL") or "gemma3-27b-abliterated-dpo"
        api_key = s.get("VISION_API_KEY") or None
        return {"api_base": api_base, "model": model, "api_key": api_key}

    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _parse_players_setting(self, players_str: str) -> List[str]:
        if not players_str:
            return []
        raw = players_str.replace(",", "\n").split("\n")
        return [p.strip() for p in raw if isinstance(p, str) and p.strip()]

    def _get_camera_jpeg(self, ha_base: str, token: str, camera_entity: str) -> bytes:
        url = f"{ha_base}/api/camera_proxy/{quote(camera_entity, safe='')}"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        if r.status_code >= 400:
            raise RuntimeError(f"camera_proxy HTTP {r.status_code}: {r.text[:200]}")
        return r.content

    def _get_ha_time(self, ha_base: str, token: str, time_entity: str) -> str:
        """
        Fetch a local-naive ISO time string (YYYY-MM-DDTHH:MM:SS) from HA.
        If HA provides a value with tz/offset, strip it to naive. If fetch fails,
        fallback to local system time in the same naive format.
        """
        try:
            if time_entity:
                url = f"{ha_base}/api/states/{quote(time_entity, safe='')}"
                r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=5)
                if r.status_code < 400:
                    state = (r.json() or {}).get("state", "")
                    if isinstance(state, str) and state.strip():
                        try:
                            dt = datetime.fromisoformat(state.strip())
                            if dt.tzinfo:
                                dt = dt.replace(tzinfo=None)
                            return dt.strftime("%Y-%m-%dT%H:%M:%S")
                        except Exception:
                            # If it already looks like naive ISO, accept as-is after validating shape
                            s = state.strip()
                            try:
                                datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
                                return s
                            except Exception:
                                pass
        except Exception:
            logger.debug("[doorbell_alert] HA time entity fetch failed", exc_info=True)

        # Local fallback (naive)
        return self._iso_naive_now()

    # ---------- Bridges ----------
    def _notify_ha_bridge(
        self,
        *,
        source: str,
        title: str,
        message: str,
        level: str = "info",
        notif_type: str = "doorbell",
        entity_id: Optional[str] = None,
        ha_time: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        POST to HA notifications: /tater-ha/v1/notifications/add
        """
        try:
            raw_port = redis_client.hget("homeassistant_platform_settings", "bind_port")
            port = int(raw_port) if raw_port is not None else 8787
        except Exception:
            port = 8787

        url = f"http://127.0.0.1:{port}/tater-ha/v1/notifications/add"
        payload = {
            "source": source,
            "title": title,
            "type": notif_type,
            "message": message,
            "entity_id": entity_id or "",
            "ha_time": ha_time or "",
            "level": level,
            "data": data or {},
        }
        try:
            r = requests.post(url, json=payload, timeout=5)
            if r.status_code >= 400:
                logger.warning("[doorbell_alert] notify post failed %s: %s", r.status_code, r.text[:200])
        except Exception as e:
            logger.warning("[doorbell_alert] notify post error: %s", e)

    def _post_automation_event(
        self,
        *,
        source: str,
        title: str,
        message: str,
        event_type: str = "doorbell",
        entity_id: Optional[str] = None,
        ha_time: Optional[str] = None,
        level: str = "info",
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        POST to Automations events: /tater-ha/v1/events/add
        """
        try:
            raw_port = redis_client.hget("ha_automations_platform_settings", "bind_port")
            port = int(raw_port) if raw_port is not None else 8788
        except Exception:
            port = 8788

        url = f"http://127.0.0.1:{port}/tater-ha/v1/events/add"
        payload = {
            "source": source,  # area slug
            "title": title,
            "type": event_type,
            "message": message,
            "entity_id": entity_id or "",
            "ha_time": ha_time or "",
            "level": level,
            "data": data or {},
        }
        try:
            r = requests.post(url, json=payload, timeout=5)
            if r.status_code >= 400:
                logger.warning("[doorbell_alert] events post failed %s: %s", r.status_code, r.text[:200])
        except Exception as e:
            logger.warning("[doorbell_alert] events post error: %s", e)

    # ---------- Vision / TTS ----------
    def _vision_describe(self, image_bytes: bytes, api_base: str, model: str, api_key: Optional[str]) -> str:
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        data_url = f"data:image/jpeg;base64,{b64}"

        prompt = (
            "You are writing a single spoken doorbell alert sentence.\n"
            "Start the sentence with 'Someone is at the door'.\n"
            "If a person is visible, describe briefly (count if >1, clothing color/uniform, package). "
            "If no person is visible, still start the sentence, then note no one is seen and describe the scene.\n"
            "One concise sentence, friendly, suitable for TTS."
        )

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a helpful vision assistant."},
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
        return text or "Someone is at the door."

    def _tts_speak(self, ha_base: str, token: str, tts_entity: str, players: List[str], message: str) -> None:
        svc_url = f"{ha_base}/api/services/tts/speak"
        headers = self._ha_headers(token)

        for mp in players:
            data = {
                "entity_id": tts_entity,
                "media_player_entity_id": mp,
                "message": message,
                "cache": True,
            }
            r = requests.post(svc_url, headers=headers, json=data, timeout=15)
            if r.status_code >= 400:
                fallback_url = f"{ha_base}/api/services/tts/piper_say"
                r2 = requests.post(fallback_url, headers=headers, json=data, timeout=15)
                if r2.status_code >= 400:
                    raise RuntimeError(
                        f"TTS failed (speak:{r.status_code}, piper_say:{r2.status_code})"
                    )

    # ---------- Automation entrypoint ----------
    async def handle_automation(self, args: Dict[str, Any], llm_client) -> Any:
        """
        Optional overrides:
          {
            "camera": "camera.some_other_cam",
            "players": ["media_player.one", "media_player.two"],
            "tts_entity": "tts.piper",
            "notifications": true,
            "area": "front door"
          }
        """
        s = self._get_settings()
        ha = self._ha(s)
        vis = self._vision(s)

        # Defaults from settings
        camera_default = (s.get("CAMERA_ENTITY") or "").strip()
        players_default = self._parse_players_setting(s.get("MEDIA_PLAYERS", ""))
        notif_default = str(s.get("NOTIFICATIONS_ENABLED", "false")).strip().lower() in ("1", "true", "yes", "on")
        tts_default = ha["tts_entity"]
        area_default = (s.get("AREA_LABEL") or "front door").strip()

        # Optional overrides
        camera = (args.get("camera") or camera_default).strip()
        tts_entity = (args.get("tts_entity") or tts_default).strip()
        area = (args.get("area") or area_default).strip()

        if "players" in args and isinstance(args["players"], list):
            players = [p.strip() for p in args["players"] if isinstance(p, str) and p.strip()]
        else:
            players = players_default

        notifications = bool(args.get("notifications")) if "notifications" in args else notif_default

        # Validate requireds
        if not camera:
            raise ValueError("Missing camera entity — set CAMERA_ENTITY in plugin settings or pass 'camera' in args.")
        if not players:
            raise ValueError("No media players configured — set MEDIA_PLAYERS in settings or pass 'players' in args.")

        # HA local-naive ISO time
        ha_time = self._get_ha_time(ha["base"], ha["token"], ha["time_entity"])

        # Area slug for storage (per-area bucket)
        area_slug = self._slug(area)

        # 1) Snapshot
        try:
            jpeg = self._get_camera_jpeg(ha["base"], ha["token"], camera)
        except Exception:
            logger.exception("[doorbell_alert] Failed to fetch camera snapshot; using generic line")
            generic = "Someone is at the door."
            self._tts_speak(ha["base"], ha["token"], tts_entity, players, generic)

            if notifications:
                extra = {"players": players, "tts_entity": tts_entity, "area": area}
                # notifications can keep a feature/source label
                self._notify_ha_bridge(
                    source="doorbell_alert",
                    title="Doorbell",
                    message=generic,
                    notif_type="doorbell",
                    entity_id=camera,
                    ha_time=ha_time,
                    level="info",
                    data=extra,
                )
                # events stored per-area
                self._post_automation_event(
                    source=area_slug,  # <-- per-area storage
                    title="Doorbell",
                    message=generic,
                    event_type="doorbell",
                    entity_id=camera,
                    ha_time=ha_time,
                    level="info",
                    data={"area": area, **extra},
                )
            return {"ok": True, "note": "snapshot_failed_generic_alert_spoken", "players": players, "area": area}

        # 2) Vision brief
        try:
            desc = self._vision_describe(jpeg, vis["api_base"], vis["model"], vis["api_key"])
        except Exception:
            logger.exception("[doorbell_alert] Vision analysis failed; using generic line")
            desc = "Someone is at the door."

        # 3) Speak
        self._tts_speak(ha["base"], ha["token"], tts_entity, players, desc)

        # 4) Notifications + Events (optional)
        if notifications:
            extra = {"players": players, "tts_entity": tts_entity, "area": area}
            # notifications: keep logical feature source
            self._notify_ha_bridge(
                source="doorbell_alert",
                title="Doorbell",
                message=desc,
                notif_type="doorbell",
                entity_id=camera,
                ha_time=ha_time,
                level="info",
                data=extra,
            )
            # events: per-area source
            self._post_automation_event(
                source=area_slug,  # <-- per-area storage
                title="Doorbell",
                message=desc,
                event_type="doorbell",
                entity_id=camera,
                ha_time=ha_time,
                level="info",
                data={"area": area, **extra},
            )

        # Platform ignores the return; for logs/tracing only
        return {"ok": True, "spoken": True, "players": players, "area": area}

plugin = DoorbellAlertPlugin()
