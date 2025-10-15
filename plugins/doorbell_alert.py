# plugins/doorbell_alert.py
import json
import base64
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import quote

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
    ask a Vision LLM to describe it briefly, and speak the result via Piper TTS
    on the provided media_player entities.
    """
    name = "doorbell_alert"
    description = (
        "door bell alert tool for when the user requests a doorbell alert. "
    )
    # Router-only usage (concise; no examples prose)
    usage = (
        "{\n"
        '  "function": "doorbell_alert",\n'
        '  "arguments": {\n'
        '    "camera": "camera.doorbell_high",\n'
        '    "players": ["media_player.living_room", "media_player.kitchen"],\n'
        '    "tts_entity": "tts.piper (optional)",\n'
        '    "notifications": false (optional)\n'
        "  }\n"
        "}\n"
    )

    # IMPORTANT: automation-only
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

    # ---------- Internal helpers ----------
    def _get_settings(self) -> Dict[str, str]:
        s = redis_client.hgetall(f"plugin_settings:{self.settings_category}") or \
            redis_client.hgetall(f"plugin_settings: {self.settings_category}")
        return s

    def _ha(self, s: Dict[str, str]) -> Dict[str, str]:
        base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        token = s.get("HA_TOKEN") or ""
        if not token:
            raise ValueError("HA_TOKEN is missing in Doorbell Alert settings.")
        tts_entity = (s.get("TTS_ENTITY") or "tts.piper").strip() or "tts.piper"
        return {"base": base, "token": token, "tts_entity": tts_entity}

    def _vision(self, s: Dict[str, str]) -> Dict[str, Optional[str]]:
        api_base = (s.get("VISION_API_BASE") or "http://127.0.0.1:1234").rstrip("/")
        model = s.get("VISION_MODEL") or "gemma3-27b-abliterated-dpo"
        api_key = s.get("VISION_API_KEY") or None
        return {"api_base": api_base, "model": model, "api_key": api_key}

    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _get_camera_jpeg(self, ha_base: str, token: str, camera_entity: str) -> bytes:
        """
        GET /api/camera_proxy/<entity_id>  → current still image (JPEG/PNG).
        """
        url = f"{ha_base}/api/camera_proxy/{quote(camera_entity, safe='')}"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        if r.status_code >= 400:
            raise RuntimeError(f"camera_proxy HTTP {r.status_code}: {r.text[:200]}")
        return r.content

    def _notify_ha_bridge(self, title: str, body: str, level: str = "info", source: str = "doorbell_alert") -> None:
        """
        Posts to the Home Assistant platform's notifications endpoint:
          POST /tater-ha/v1/notifications/add
        Uses the HA-bridge bind port from Redis: 'homeassistant_platform_settings' -> 'bind_port'
        """
        try:
            raw_port = redis_client.hget("homeassistant_platform_settings", "bind_port")
            port = int(raw_port) if raw_port is not None else 8787
        except Exception:
            port = 8787

        url = f"http://127.0.0.1:{port}/tater-ha/v1/notifications/add"
        try:
            r = requests.post(
                url,
                json={
                    "title": title,
                    "body": body,
                    "level": level,
                    "source": source,
                },
                timeout=5,
            )
            if r.status_code >= 400:
                logger.warning("[doorbell_alert] notify post failed %s: %s", r.status_code, r.text[:200])
        except Exception as e:
            logger.warning("[doorbell_alert] notify post error: %s", e)

    def _vision_describe(self, image_bytes: bytes, api_base: str, model: str, api_key: Optional[str]) -> str:
        """
        OpenAI-compatible /v1/chat/completions with an image URL (data URL).
        Returns a single concise sentence for TTS.
        """
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        data_url = f"data:image/jpeg;base64,{b64}"

        prompt = (
            "You are writing a single spoken doorbell alert sentence.\n"
            "Do NOT include introductions like 'Here is an alert' or 'Okay'.\n"
            "Start the sentence with 'Someone rang the doorbell' regardless of the scene.\n"
            "\n"
            "Rules:\n"
            "1) If a person is visible, describe them briefly: count (if >1), clothing color or uniform, "
            "and whether they appear to be delivering or carrying a package.\n"
            "2) If no person is visible, still start with 'Someone is at the door' but continue with a note "
            "that no one is seen, then describe the scene — vehicles, packages, pets, or motion if relevant.\n"
            "3) Keep it natural, concise, and friendly — one clear sentence, suitable for text-to-speech."
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
        text = (
            res.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
        return text or "Someone is at the door."

    def _tts_speak(self, ha_base: str, token: str, tts_entity: str, players: List[str], message: str) -> None:
        """
        Prefer tts.speak (modern HA). Loop per-player for compatibility.
        Fallback to tts.piper_say if needed.
        """
        svc_url = f"{ha_base}/api/services/tts/speak"
        headers = self._ha_headers(token)

        for mp in players:
            data = {
                "entity_id": tts_entity,                # e.g., 'tts.piper'
                "media_player_entity_id": mp,           # call per player
                "message": message,
                "cache": True,
            }
            r = requests.post(svc_url, headers=headers, json=data, timeout=15)
            if r.status_code >= 400:
                # Fallback endpoint for some setups
                fallback_url = f"{ha_base}/api/services/tts/piper_say"
                r2 = requests.post(fallback_url, headers=headers, json=data, timeout=15)
                if r2.status_code >= 400:
                    raise RuntimeError(
                        f"TTS failed (speak:{r.status_code}, piper_say:{r2.status_code})"
                    )

    # ---------- Automation entrypoint ----------
    async def handle_automation(self, args: Dict[str, Any], llm_client) -> Any:
        """
        Args expected:
          {
            "camera": "camera.doorbell_high",
            "players": ["media_player.living_room", "media_player.kitchen"],
            // optional:
            "tts_entity": "tts.piper",
            "notifications": false
          }
        """
        camera = (args.get("camera") or "").strip()
        players_raw = args.get("players") or []
        tts_entity_override = (args.get("tts_entity") or "").strip()
        notifications = bool(args.get("notifications", False))

        if not camera:
            raise ValueError("Missing 'camera' (e.g., camera.doorbell_high)")
        if not isinstance(players_raw, list) or not players_raw:
            raise ValueError('Missing "players" list (e.g., ["media_player.living_room"])')

        players = [p.strip() for p in players_raw if isinstance(p, str) and p.strip()]
        if not players:
            raise ValueError("Provided 'players' list is empty after normalization.")

        s = self._get_settings()
        ha = self._ha(s)
        vis = self._vision(s)
        tts_entity = tts_entity_override or ha["tts_entity"]

        # 1) Snapshot
        try:
            jpeg = self._get_camera_jpeg(ha["base"], ha["token"], camera)
        except Exception:
            logger.exception("[doorbell_alert] Failed to fetch camera snapshot; using generic line")
            generic = "Someone is at the door."
            self._tts_speak(ha["base"], ha["token"], tts_entity, players, generic)
            if notifications:
                self._notify_ha_bridge(title="Doorbell", body=generic, level="info", source="doorbell_alert")
            return {"ok": True, "note": "snapshot_failed_generic_alert_spoken", "players": players}

        # 2) Vision brief
        try:
            desc = self._vision_describe(jpeg, vis["api_base"], vis["model"], vis["api_key"])
        except Exception:
            logger.exception("[doorbell_alert] Vision analysis failed; using generic line")
            desc = "Someone is at the door."

        # 3) Speak
        self._tts_speak(ha["base"], ha["token"], tts_entity, players, desc)

        # 4) Optional notification to HA bridge
        if notifications:
            self._notify_ha_bridge(title="Doorbell", body=desc, level="info", source="doorbell_alert")

        # Platform ignores the return; this is for logs/tracing only
        return {"ok": True, "spoken": True, "players": players}

plugin = DoorbellAlertPlugin()