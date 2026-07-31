import base64
import json
import logging
import mimetypes
import os
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from helpers import extract_json
from integration_registry import (
    get_integration_device_registry,
    get_integration_devices_by_capability,
    run_integration_device_action,
)
from verba_base import ToolVerba
from verba_result import action_failure, action_success


logger = logging.getLogger("category_device_control")
logger.setLevel(logging.INFO)

CAMERA_SNAPSHOT_ARTIFACT_TTL_SEC = int(os.getenv("TATER_CAMERA_SNAPSHOT_ARTIFACT_TTL_SEC", str(60 * 60 * 24 * 14)))

_DEVICE_CATEGORY_QUERY_TERMS: Dict[str, Tuple[str, ...]] = {
    "light": ("light", "lights", "lamp", "lamps", "bulb", "bulbs", "dimmer", "dimmers"),
    "switch": ("switch", "switches", "relay", "relays"),
    "plug": ("plug", "plugs", "outlet", "outlets"),
    "fan": ("fan", "fans"),
    "garage_door": ("garage", "garage door", "garage doors", "garage opener"),
    "cover": ("cover", "covers", "shade", "shades", "blind", "blinds", "curtain", "curtains"),
    "lock": ("lock", "locks"),
    "camera": ("camera", "cameras", "doorbell", "doorbells"),
    "climate": ("climate", "thermostat", "thermostats", "hvac"),
    "media_player": ("media player", "media players", "speaker", "speakers", "television", "tv"),
    "remote": ("remote", "remotes"),
    "scene": ("scene", "scenes"),
    "script": ("script", "scripts"),
}
_COLOR_NAMES = {
    "red",
    "orange",
    "amber",
    "yellow",
    "green",
    "blue",
    "cyan",
    "teal",
    "purple",
    "violet",
    "magenta",
    "pink",
    "white",
    "daylight",
    "warm white",
    "cool white",
}


class CategoryDeviceControlBase(ToolVerba):
    name = "category_device_control"
    verba_name = "Category Device Control"
    pretty_name = "Category Device Control"
    version = "1.0.0"
    min_tater_version = "59"
    settings_category = "Device Control"
    platforms = [
        "voice_core",
        "homeassistant",
        "webui",
        "little_spud",
        "macos",
        "xbmc",
        "homekit",
        "discord",
        "telegram",
        "matrix",
        "irc",
        "meshtastic",
    ]
    tags = ["device", "integration"]
    how_to_use = (
        "Pass one natural-language request in query. Include a room/area name or a specific device name when needed, "
        "such as 'turn off the game room switch' or 'is there motion in the hallway'."
    )
    common_needs = ["Device category, action, and room/area or device target when the request is not for all devices."]
    missing_info_prompts = ["Which room, area, or device should I use?"]

    category_id = "device"
    category_label = "devices"
    singular_label = "device"
    action_label = "device"
    max_candidates_setting = "DEVICE_MAX_CANDIDATES"
    allowed_actions = {"list", "status"}
    control_actions = set()
    ignored_target_words = {"device", "devices", "the", "my", "all"}
    action_aliases: Dict[str, str] = {}
    needs_target_for_actions = True
    inventory_scope = "category"

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are checking or controlling devices now. "
        "Only output that message."
    )

    required_settings = {
        "DEVICE_MAX_CANDIDATES": {
            "label": "Max Device Candidates",
            "type": "number",
            "default": 180,
            "description": "Maximum integrated device candidates sent to chooser LLM calls.",
        },
    }

    @staticmethod
    def _text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _decode_map(raw: Optional[dict]) -> dict:
        out: dict = {}
        for key, value in (raw or {}).items():
            k = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
            if isinstance(value, (bytes, bytearray)):
                out[k] = value.decode("utf-8", "ignore")
            elif value is None:
                out[k] = ""
            else:
                out[k] = str(value)
        return out

    @staticmethod
    def _normalize_handler_args(args: Any) -> Dict[str, Any]:
        if isinstance(args, dict):
            payload = dict(args)
            nested = payload.get("arguments")
            if isinstance(nested, dict):
                merged = dict(nested)
                for key, value in payload.items():
                    if key != "arguments":
                        merged[key] = value
                return merged
            return payload
        if isinstance(args, str):
            text = str(args or "").strip()
            if not text:
                return {}
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    nested = parsed.get("arguments")
                    if isinstance(nested, dict):
                        merged = dict(nested)
                        for key, value in parsed.items():
                            if key != "arguments":
                                merged[key] = value
                        return merged
                    return parsed
            except Exception:
                pass
            return {"query": text}
        return {}

    def _get_plugin_settings(self) -> dict:
        try:
            from helpers import redis_client

            merged = {}
            for key in ("verba_settings:Device Control", "verba_settings: Device Control"):
                merged.update(self._decode_map(redis_client.hgetall(key) or {}))
            return merged
        except Exception:
            return {}

    def _get_int_setting(self, key: str, default: int, minimum: int, maximum: int) -> int:
        raw = self._get_plugin_settings().get(key)
        try:
            value = int(float(str(raw).strip()))
        except Exception:
            value = int(default)
        return max(minimum, min(maximum, value))

    def _json_object_from_text(self, text: str) -> Dict[str, Any]:
        clean = self._text(text)
        clean = re.sub(r"^```(?:json)?\s*", "", clean, flags=re.I)
        clean = re.sub(r"\s*```$", "", clean).strip()
        try:
            parsed = json.loads(clean)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            pass
        try:
            blob = extract_json(clean) or ""
            if blob:
                parsed = json.loads(blob)
                return parsed if isinstance(parsed, dict) else {}
        except Exception:
            pass
        match = re.search(r"\{.*\}", clean, flags=re.S)
        if not match:
            return {}
        try:
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    async def _llm_json(self, *, llm_client, system: str, user_payload: dict, max_tokens: int = 260) -> dict:
        if llm_client is None:
            return {}
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
                max_tokens=max_tokens,
                temperature=0.0,
            )
            return self._json_object_from_text(self._text((resp.get("message", {}) or {}).get("content", "")))
        except Exception as exc:
            logger.debug("[%s] llm_json failed: %s", self.name, exc)
            return {}

    def _normalize_number(self, value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _normalize_int(self, value: Any) -> Optional[int]:
        number = self._normalize_number(value)
        if number is None:
            return None
        return int(round(number))

    def _percent_from_text(self, text: Any) -> Optional[int]:
        raw = self._text(text).lower()
        for pattern in (
            r"\b(\d{1,3})\s*(?:%|\bpercent\b)",
            r"\b(?:to|at|level|position|volume|brightness|speed)\s+(\d{1,3})\b",
        ):
            match = re.search(pattern, raw)
            if not match:
                continue
            try:
                return max(0, min(100, int(round(float(match.group(1))))))
            except Exception:
                continue
        return None

    def _normalize_rgb(self, value: Any) -> Optional[List[int]]:
        if not isinstance(value, (list, tuple)) or len(value) != 3:
            return None
        rgb: List[int] = []
        for item in value:
            parsed = self._normalize_int(item)
            if parsed is None:
                return None
            rgb.append(max(0, min(255, parsed)))
        return rgb

    def _rgb_from_text(self, text: Any) -> Optional[List[int]]:
        raw = self._text(text)
        rgb_match = re.search(
            r"\brgb\s*(?:\(\s*|[:=]?\s*)(\d{1,3})\s*[, ]\s*(\d{1,3})\s*[, ]\s*(\d{1,3})\s*\)?",
            raw,
            flags=re.I,
        )
        if rgb_match:
            return [max(0, min(255, int(rgb_match.group(index)))) for index in (1, 2, 3)]
        hex_match = re.search(r"#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})\b", raw)
        if not hex_match:
            return None
        token = hex_match.group(1)
        if len(token) == 3:
            token = "".join(char * 2 for char in token)
        return [int(token[0:2], 16), int(token[2:4], 16), int(token[4:6], 16)]

    def _color_name_from_text(self, text: Any) -> str:
        raw = self._text(text).lower()
        for color in sorted(_COLOR_NAMES, key=len, reverse=True):
            if re.search(rf"\b{re.escape(color)}\b", raw):
                return color
        return ""

    def _requested_categories(self, query: Any) -> List[str]:
        raw = self._text(query).lower().replace("_", " ").replace("-", " ")
        out: List[str] = []
        for category_id, terms in _DEVICE_CATEGORY_QUERY_TERMS.items():
            if any(re.search(rf"\b{re.escape(term)}\b", raw) for term in terms):
                out.append(category_id)
        return out

    def _temperature_from_text(self, text: Any) -> Optional[float]:
        raw = self._text(text).lower()
        match = re.search(r"\b(\d{2,3}(?:\.\d+)?)\s*(?:degrees?|deg|f|c)?\b", raw)
        if not match:
            return None
        try:
            return float(match.group(1))
        except Exception:
            return None

    def _url_from_text(self, text: Any) -> str:
        match = re.search(r"https?://[^\s\"']+", self._text(text))
        return match.group(0) if match else ""

    def _hvac_mode_from_text(self, text: Any) -> str:
        raw = f" {self._text(text).lower()} "
        for mode in ("heat_cool", "auto", "cool", "heat", "off", "dry", "fan_only"):
            words = mode.replace("_", " ")
            if f" {mode} " in raw or f" {words} " in raw:
                return mode
        return ""

    def _normalize_action(self, value: Any, query: str) -> str:
        explicit = self._text(value).lower().replace("-", "_").replace(" ", "_")
        aliases = {
            "state": "status",
            "get_state": "status",
            "check": "status",
            "inventory": "list",
            "devices": "list",
            "show": "list",
            "on": "turn_on",
            "off": "turn_off",
            "power_on": "turn_on",
            "power_off": "turn_off",
            "open_garage": "open",
            "close_garage": "close",
            "snapshot": "camera_snapshot",
            "photo": "camera_snapshot",
            "picture": "camera_snapshot",
            "set_temp": "set_temperature",
            "temperature": "set_temperature",
            "mode": "set_hvac_mode",
            "hvac_mode": "set_hvac_mode",
            "play_pause": "playpause",
            "toggle_playback": "playpause",
            "volume": "set_volume",
            "speed": "set_percentage",
            "fan_speed": "set_percentage",
            "percentage": "set_percentage",
            "brightness": "set_brightness",
            "dim": "set_brightness",
            "color": "set_color",
            "colour": "set_color",
            "lock_door": "lock",
            "unlock_door": "unlock",
            "run_scene": "activate",
            "start_scene": "activate",
            "run_script": "run",
            "start_script": "run",
            "press": "send_command",
            "button": "send_command",
        }
        aliases.update(self.action_aliases or {})
        action = aliases.get(explicit, explicit)
        if action in self.allowed_actions:
            return action

        compact = re.sub(r"\s+", " ", f" {self._text(query).lower()} ")
        if re.search(r"\b(list|show|inventory|what)\b.*\b(devices?|all|available|rooms?)\b", compact):
            return "list" if "list" in self.allowed_actions else "status"
        if re.search(r"\b(status|state|check|is|are|what|whether|how|show me)\b", compact) or "?" in compact:
            return "status"
        requested_categories = set(self._requested_categories(compact))
        if "activate" in self.allowed_actions and "scene" in requested_categories and re.search(
            r"\b(activate|run|start|turn on)\b",
            compact,
        ):
            return "activate"
        if "run" in self.allowed_actions and "script" in requested_categories and re.search(
            r"\b(run|start|execute|turn on)\b",
            compact,
        ):
            return "run"
        if (
            "set_hvac_mode" in self.allowed_actions
            and "climate" in requested_categories
            and re.search(r"\b(turn|switch|set)\b.*\boff\b", compact)
        ):
            return "set_hvac_mode"
        if "turn_on" in self.allowed_actions and re.search(r"\b(turn|switch|power|set)\s+(?:[\w -]+?\s+)?on\b|\benable\b", compact):
            return "turn_on"
        if "turn_off" in self.allowed_actions and re.search(r"\b(turn|switch|power|shut|set)\s+(?:[\w -]+?\s+)?off\b|\b(disable|deactivate)\b", compact):
            return "turn_off"
        if "toggle" in self.allowed_actions and re.search(r"\b(toggle|flip)\b", compact):
            return "toggle"
        percent = self._percent_from_text(compact)
        percent_actions = {
            action_name
            for action_name in ("set_brightness", "set_percentage", "set_position")
            if action_name in self.allowed_actions
        }
        if percent is not None:
            if "set_brightness" in percent_actions and re.search(
                r"\b(light|lights|lamp|lamps|bulb|bulbs|brightness|brighten|dim|dimmer|level)\b",
                compact,
            ):
                return "set_brightness"
            if "set_percentage" in percent_actions and re.search(r"\b(fan|fans|speed)\b", compact):
                return "set_percentage"
            if "set_position" in percent_actions and re.search(
                r"\b(cover|covers|shade|shades|blind|blinds|curtain|curtains|position)\b",
                compact,
            ):
                return "set_position"
            if len(percent_actions) == 1:
                return next(iter(percent_actions))
        if "set_color" in self.allowed_actions and (
            self._rgb_from_text(compact)
            or self._color_name_from_text(compact)
            or re.search(r"\b(colou?r|hue)\b", compact)
        ):
            return "set_color"
        if "unlock" in self.allowed_actions and re.search(r"\b(unlock|open lock)\b", compact):
            return "unlock"
        if "lock" in self.allowed_actions and re.search(r"\block\b", compact) and not re.search(r"\bunlock\b", compact):
            return "lock"
        if "open" in self.allowed_actions and re.search(r"\b(open|raise|up)\b", compact):
            return "open"
        if "close" in self.allowed_actions and re.search(r"\b(close|shut|lower|down)\b", compact):
            return "close"
        if "stop" in self.allowed_actions and re.search(r"\b(stop|halt)\b", compact):
            return "stop"
        if "activate" in self.allowed_actions and re.search(r"\bactivate\b", compact):
            return "activate"
        if "run" in self.allowed_actions and re.search(r"\b(run|execute)\b", compact):
            return "run"
        if "send_command" in self.allowed_actions and (
            "remote" in requested_categories
            or re.search(r"\b(command|press|button|home|back|menu|select|ok|left|right)\b", compact)
        ) and re.search(
            r"\b(command|press|button|mute|volume|home|back|menu|select|ok|play|pause|stop|up|down|left|right)\b",
            compact,
        ):
            return "send_command"
        if "camera_snapshot" in self.allowed_actions and re.search(r"\b(snapshot|picture|photo|image|look|see)\b", compact):
            return "camera_snapshot"
        if "set_temperature" in self.allowed_actions and (
            re.search(r"\b(temp|temperature|thermostat|heat|cool)\b", compact) and self._temperature_from_text(compact) is not None
        ):
            return "set_temperature"
        if "set_hvac_mode" in self.allowed_actions and self._hvac_mode_from_text(compact):
            return "set_hvac_mode"
        if "playpause" in self.allowed_actions and re.search(r"\b(play\s*pause|toggle)\b", compact):
            return "playpause"
        if "play" in self.allowed_actions and re.search(r"\b(play|resume)\b", compact):
            return "play"
        if "pause" in self.allowed_actions and re.search(r"\bpause\b", compact):
            return "pause"
        if "stop" in self.allowed_actions and re.search(r"\bstop\b", compact):
            return "stop"
        if "next" in self.allowed_actions and re.search(r"\b(next|skip)\b", compact):
            return "next"
        if "previous" in self.allowed_actions and re.search(r"\b(previous|back)\b", compact):
            return "previous"
        if "mute" in self.allowed_actions and re.search(r"\bmute\b", compact):
            return "mute"
        if "unmute" in self.allowed_actions and re.search(r"\bunmute\b", compact):
            return "unmute"
        if "set_volume" in self.allowed_actions and re.search(r"\b(volume|louder|quieter)\b", compact):
            if self._percent_from_text(compact) is not None:
                return "set_volume"
            if "volume_up" in self.allowed_actions and re.search(r"\b(up|louder|increase)\b", compact):
                return "volume_up"
            if "volume_down" in self.allowed_actions and re.search(r"\b(down|quieter|decrease)\b", compact):
                return "volume_down"
        if "announce" in self.allowed_actions and re.search(r"\b(announce|announcement|say)\b", compact):
            return "announce"
        if "play_media" in self.allowed_actions and self._url_from_text(compact):
            return "play_media"
        return ""

    def _clean_target(self, value: Any) -> str:
        text = self._text(value).lower()
        text = re.sub(
            r"\b(turn|switch|power|set|shut|enable|disable|activate|deactivate|toggle|flip|status|state|check|is|are|what|whether|show|list|open|close|stop|raise|lower|play|pause|next|previous|mute|unmute|volume|temperature|mode|snapshot|photo|picture|image|lock|unlock|run|execute|press|button|command|speed|percentage|percent|brightness|brighten|dim|color|colour|hue|on|off|to|at|in|inside|from|for|of|with|near|currently|right|now|please|can|you|could|would)\b",
            " ",
            text,
        )
        ignored = [re.escape(item) for item in sorted(self.ignored_target_words, key=len, reverse=True)]
        if ignored:
            text = re.sub(r"\b(" + "|".join(ignored) + r")\b", " ", text)
        text = re.sub(r"https?://\S+", " ", text)
        text = re.sub(r"\d{1,3}(?:\.\d+)?\s*(?:%|degrees?|deg|f|c)?", " ", text)
        text = re.sub(r"#[0-9a-fA-F]{3,6}\b", " ", text)
        text = re.sub(r"\brgb\s*\([^)]*\)", " ", text, flags=re.I)
        return " ".join(re.findall(r"[a-z0-9]+", text))

    async def _interpret_query(self, payload: dict, query: str, llm_client) -> dict:
        action = self._normalize_action(payload.get("action"), query)
        target = self._text(payload.get("target") or payload.get("room") or payload.get("device") or payload.get("name"))
        position = self._normalize_int(payload.get("position", payload.get("position_pct")))
        volume = self._normalize_int(payload.get("volume", payload.get("volume_pct")))
        percentage = self._normalize_int(payload.get("percentage", payload.get("speed", payload.get("speed_pct"))))
        brightness = self._normalize_int(
            payload.get("brightness_pct", payload.get("brightness", payload.get("level")))
        )
        rgb_color = self._normalize_rgb(payload.get("rgb_color")) or self._rgb_from_text(query)
        color_name = self._text(payload.get("color_name") or payload.get("color")).lower()
        temperature = self._normalize_number(payload.get("temperature", payload.get("target_temperature")))
        hvac_mode = self._text(payload.get("mode") or payload.get("hvac_mode")).lower().replace(" ", "_")
        source_url = self._text(payload.get("source_url") or payload.get("url") or payload.get("media_url")) or self._url_from_text(query)
        command = self._text(payload.get("command") or payload.get("button"))
        requested_categories = self._requested_categories(query)

        if position is None and action == "set_position":
            position = self._percent_from_text(query)
        if volume is None and action == "set_volume":
            volume = self._percent_from_text(query)
        if percentage is None and action == "set_percentage":
            percentage = self._percent_from_text(query)
        if brightness is None and action in {"set_brightness", "turn_on"}:
            brightness = self._percent_from_text(query)
        if not color_name:
            color_name = self._color_name_from_text(query)
        if temperature is None and action == "set_temperature":
            temperature = self._temperature_from_text(query)
        if not hvac_mode:
            hvac_mode = self._hvac_mode_from_text(query)
        if not command and action == "send_command":
            command = self._remote_command_from_text(query)

        if not action or (not target and self.needs_target_for_actions):
            system = (
                f"Interpret one {self.category_label} request across multiple home integrations.\n"
                "Return STRICT JSON only with keys: action, target, position, volume, percentage, brightness_pct, "
                "color_name, rgb_color, temperature, hvac_mode, source_url, command.\n"
                f"Allowed action values: {', '.join(sorted(self.allowed_actions))}, or empty string.\n"
                "Rules:\n"
                "- target is the named room or specific device, without action words.\n"
                "- list/status may have an empty target when the user asks for all devices.\n"
                "- position, volume, percentage, and brightness_pct are integers 0-100 or null.\n"
                "- color_name is a named light color or empty string; rgb_color is [r,g,b] or null.\n"
                "- temperature is a number or null.\n"
                "- source_url is a URL or empty string.\n"
                "- command is a remote/button command such as mute, volume_up, home, back, select, play, or pause.\n"
            )
            ai = await self._llm_json(llm_client=llm_client, system=system, user_payload={"query": query}, max_tokens=220)
            if ai:
                action = action or self._normalize_action(ai.get("action"), query)
                target = target or self._text(ai.get("target"))
                if position is None:
                    position = self._normalize_int(ai.get("position"))
                if volume is None:
                    volume = self._normalize_int(ai.get("volume"))
                if percentage is None:
                    percentage = self._normalize_int(ai.get("percentage"))
                if brightness is None:
                    brightness = self._normalize_int(ai.get("brightness_pct"))
                if rgb_color is None:
                    rgb_color = self._normalize_rgb(ai.get("rgb_color"))
                if not color_name:
                    color_name = self._text(ai.get("color_name")).lower()
                if temperature is None:
                    temperature = self._normalize_number(ai.get("temperature"))
                if not hvac_mode:
                    hvac_mode = self._text(ai.get("hvac_mode")).lower().replace(" ", "_")
                if not source_url:
                    source_url = self._text(ai.get("source_url"))
                if not command:
                    command = self._text(ai.get("command"))

        if not target:
            target_query = query
            if action == "set_color":
                for color in sorted(_COLOR_NAMES, key=len, reverse=True):
                    target_query = re.sub(rf"\b{re.escape(color)}\b", " ", target_query, flags=re.I)
            target = self._clean_target(target_query)
        return {
            "action": action,
            "target": target,
            "position": max(0, min(100, position)) if position is not None else None,
            "volume": max(0, min(100, volume)) if volume is not None else None,
            "percentage": max(0, min(100, percentage)) if percentage is not None else None,
            "brightness_pct": max(0, min(100, brightness)) if brightness is not None else None,
            "color_name": color_name,
            "rgb_color": rgb_color,
            "temperature": temperature,
            "hvac_mode": hvac_mode,
            "source_url": source_url,
            "command": command,
            "requested_categories": requested_categories,
        }

    def _remote_command_from_text(self, text: Any) -> str:
        raw = f" {self._text(text).lower()} "
        phrases = [
            ("volume up", "volume_up"),
            ("vol up", "volume_up"),
            ("volume down", "volume_down"),
            ("vol down", "volume_down"),
            ("page up", "page_up"),
            ("page down", "page_down"),
            ("play pause", "play_pause"),
            ("fast forward", "fast_forward"),
            ("rewind", "rewind"),
        ]
        for phrase, command in phrases:
            if f" {phrase} " in raw:
                return command
        for command in (
            "mute",
            "home",
            "back",
            "menu",
            "select",
            "ok",
            "enter",
            "play",
            "pause",
            "stop",
            "up",
            "down",
            "left",
            "right",
            "power",
        ):
            if re.search(rf"\b{re.escape(command)}\b", raw):
                return command
        return ""

    def _device_actions(self, device: dict) -> set:
        return {self._text(item).lower() for item in (device.get("actions") or []) if self._text(item)}

    def _device_caps(self, device: dict) -> set:
        out = set()
        for raw in (device.get("capabilities"), device.get("category_ids")):
            values = raw if isinstance(raw, (list, tuple, set)) else [raw]
            for item in values:
                token = self._text(item).lower()
                if token:
                    out.add(token)
        return out

    def _action_supported(self, device: dict, action: str) -> bool:
        if action in {"list", "status"}:
            return True
        actions = self._device_actions(device)
        caps = self._device_caps(device)
        if action in actions:
            return True
        if action == "camera_snapshot" and "camera" in caps:
            return True
        if action in {"turn_on", "turn_off", "toggle"} and caps.intersection(
            {"switch", "plug", "light", "fan", "media_player"}
        ):
            return True
        if action in {"open", "close"} and caps.intersection({"cover", "garage_door", "open_close"}):
            return True
        if action == "stop" and caps.intersection({"cover", "garage_door", "media_player"}):
            return True
        if action == "set_position" and caps.intersection({"cover", "garage_door"}):
            return True
        if action == "set_brightness" and caps.intersection({"dimmable", "brightness"}):
            return True
        if action == "set_color" and caps.intersection({"color", "rgb", "color_temperature"}):
            return True
        if action == "set_percentage" and "fan" in caps:
            return True
        if action in {"set_temperature", "set_hvac_mode"} and "climate" in caps:
            return True
        if action in {"lock", "unlock"} and "lock" in caps:
            return True
        if action == "activate" and "scene" in caps:
            return True
        if action == "run" and "script" in caps:
            return True
        if action == "send_command" and "remote" in caps:
            return True
        if action in {
            "playpause",
            "play",
            "pause",
            "next",
            "previous",
            "mute",
            "unmute",
            "set_volume",
            "volume_up",
            "volume_down",
            "announce",
            "play_media",
        } and "media_player" in caps:
            return True
        return False

    def _device_alias_values(self, device: dict) -> List[str]:
        details = device.get("details") if isinstance(device.get("details"), dict) else {}
        values: List[str] = []
        seen = set()

        def add(value: Any) -> None:
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    add(item)
                return
            text = self._text(value)
            token = text.casefold()
            if not text or token in seen:
                return
            seen.add(token)
            values.append(text)

        for value in (
            device.get("aliases"),
            device.get("reported_name"),
            device.get("reported_device_name"),
            details.get("aliases"),
            details.get("friendly_name"),
            details.get("alias"),
            details.get("alternate_names"),
        ):
            add(value)
        return values

    def _device_alias_text(self, device: dict) -> str:
        details = device.get("details") if isinstance(device.get("details"), dict) else {}
        bits = [
            device.get("id"),
            device.get("ref"),
            device.get("name"),
            device.get("room"),
            device.get("area"),
            device.get("integration_name"),
            device.get("integration_id"),
            device.get("type"),
            details.get("friendly_name"),
            details.get("alias"),
            details.get("device_id"),
            details.get("model"),
            details.get("host"),
            details.get("ip"),
            *self._device_alias_values(device),
        ]
        return " ".join(self._text(bit).lower() for bit in bits if self._text(bit))

    def _tokens(self, value: Any) -> List[str]:
        return [token for token in re.split(r"[^a-z0-9]+", self._text(value).lower()) if token]

    def _device_matches_requested_categories(self, device: dict, requested_categories: List[str]) -> bool:
        requested = {self._text(item).lower() for item in requested_categories if self._text(item)}
        if not requested:
            return True
        if requested.intersection(self._device_caps(device)):
            return True
        semantic_text = " ".join(
            [
                self._text(device.get("name")).lower(),
                *(self._text(value).lower() for value in self._device_alias_values(device)),
            ]
        )
        for category_id in requested:
            terms = _DEVICE_CATEGORY_QUERY_TERMS.get(category_id) or ()
            if any(re.search(rf"\b{re.escape(term)}\b", semantic_text) for term in terms):
                return True
        return False

    def _device_has_exact_name_or_alias(self, target: str, device: dict) -> bool:
        target_text = self._clean_target(target)
        if not target_text:
            return False
        if target_text == self._clean_target(device.get("name")):
            return True
        return any(
            target_text == self._clean_target(alias_value)
            for alias_value in self._device_alias_values(device)
        )

    def _score_device(self, target: str, device: dict, requested_categories: Optional[List[str]] = None) -> int:
        target_text = self._clean_target(target)
        if not target_text:
            return 0
        name = self._text(device.get("name")).lower()
        room = self._text(device.get("room") or device.get("area")).lower()
        alias = self._device_alias_text(device)
        if self._device_has_exact_name_or_alias(target_text, device):
            return 1000
        score = 0
        if target_text in self._clean_target(name):
            score += 520 + len(target_text)
        alias_names = [self._clean_target(value) for value in self._device_alias_values(device)]
        if any(target_text and target_text in alias_name for alias_name in alias_names):
            score += 540 + len(target_text)
        if target_text and target_text == self._clean_target(room):
            score += 300
        elif target_text in alias:
            score += 220 + len(target_text)
        target_tokens = self._tokens(target_text)
        alias_tokens = set(self._tokens(alias))
        for token in target_tokens:
            if token in alias_tokens:
                score += 75
            elif len(token) >= 4 and any(part.startswith(token) for part in alias_tokens):
                score += 30
        if requested_categories and self._device_matches_requested_categories(device, requested_categories):
            score += 40
        return score

    def _room_matches(
        self,
        target: str,
        devices: List[dict],
        requested_categories: Optional[List[str]] = None,
    ) -> List[dict]:
        clean_target = self._clean_target(target)
        if not clean_target:
            return []
        matches: List[dict] = []
        for device in devices:
            room = self._clean_target(device.get("room") or device.get("area"))
            if room and clean_target == room:
                matches.append(device)
        if requested_categories:
            matches = [
                device
                for device in matches
                if self._device_matches_requested_categories(device, requested_categories)
            ]
        return matches

    async def _ai_choose_device(self, *, query: str, intent: dict, candidates: List[dict], llm_client) -> str:
        if llm_client is None or not candidates:
            return ""
        limit = self._get_int_setting(self.max_candidates_setting, 180, 5, 800)
        shortlist = candidates[:limit]
        compact = [
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "room": row.get("room") or row.get("area"),
                "integration": row.get("integration_name") or row.get("integration_id"),
                "state": row.get("state") or row.get("status"),
                "actions": row.get("actions") or [],
                "capabilities": row.get("category_ids") or row.get("capabilities") or [],
                "aliases": self._device_alias_values(row),
            }
            for row in shortlist
        ]
        valid_ids = {self._text(row.get("id")) for row in shortlist}
        system = (
            f"Choose the best {self.singular_label} for this request.\n"
            "Return STRICT JSON only: {\"device_id\":\"<id from candidates or empty>\"}.\n"
            f"Pick exactly one id only if the request clearly identifies a single {self.singular_label}. "
            "Use user-facing names, aliases, rooms, and the requested action. The candidates are already "
            "filtered to devices that support the action. Do not infer a device's technical type from words "
            "such as light, switch, or plug. Return an empty id when the choice is unclear, and never invent ids."
        )
        payload = await self._llm_json(
            llm_client=llm_client,
            system=system,
            user_payload={"query": query, "intent": intent, "candidates": compact},
            max_tokens=220,
        )
        picked = self._text(payload.get("device_id"))
        return picked if picked in valid_ids else ""

    async def _select_devices(self, *, devices: List[dict], payload: dict, query: str, intent: dict, llm_client) -> Tuple[List[dict], List[str]]:
        action = self._text(intent.get("action"))
        candidates = [device for device in devices if self._action_supported(device, action)]
        if not candidates:
            return [], [f"No {self.category_label} support {action}."]

        explicit_id = self._text(payload.get("device_id") or payload.get("id") or payload.get("ref"))
        if explicit_id:
            for device in candidates:
                if self._text(device.get("id")) == explicit_id or self._text(device.get("ref")) == explicit_id:
                    return [device], []
            return [], [f"No {self.singular_label} matched id {explicit_id}."]

        target = self._text(intent.get("target")) or self._clean_target(query)
        if not target:
            if action == "status":
                return candidates, []
            if len(candidates) == 1:
                return [candidates[0]], []
            return [], [f"Choose a room or {self.singular_label} name."]

        requested_categories = [
            self._text(item).lower()
            for item in (intent.get("requested_categories") or [])
            if self._text(item)
        ]
        scored = [
            (self._score_device(target, device, requested_categories), device)
            for device in candidates
        ]
        exact_matches = [
            device
            for device in candidates
            if self._device_has_exact_name_or_alias(target, device)
        ]
        if len(exact_matches) == 1:
            return exact_matches, []

        room_matches = self._room_matches(target, candidates, requested_categories)
        if room_matches:
            return room_matches, []

        scored.sort(
            key=lambda item: (item[0], self._text(item[1].get("name")).lower()),
            reverse=True,
        )
        ranked_candidates = [device for _score, device in scored]
        picked_id = await self._ai_choose_device(
            query=query,
            intent=intent,
            candidates=ranked_candidates,
            llm_client=llm_client,
        )
        if picked_id:
            for device in candidates:
                if self._text(device.get("id")) == picked_id:
                    return [device], []

        if exact_matches:
            return [], [
                f"That name or alias matched multiple {self.category_label}: "
                + self._format_device_choices(exact_matches[:10])
            ]
        likely_matches = [device for score, device in scored if score > 0]
        choices = likely_matches or ranked_candidates
        return [], [
            f"I could not confidently match '{target}' to a {self.singular_label}. "
            f"Available matches: {self._format_device_choices(choices[:12])}"
        ]

    def _format_device_choices(self, devices: List[dict]) -> str:
        parts: List[str] = []
        for device in devices:
            name = self._text(device.get("name")) or self._text(device.get("id")) or self.singular_label
            room = self._text(device.get("room") or device.get("area"))
            state = self._text(device.get("state") or device.get("status"))
            provider = self._text(device.get("integration_name") or device.get("integration_id"))
            meta = ", ".join(part for part in (room, state, provider) if part)
            parts.append(f"{name} ({meta})" if meta else name)
        return ", ".join(parts) if parts else "none"

    def _list_summary(self, devices: List[dict]) -> str:
        if not devices:
            return f"No {self.category_label} were found."
        by_room: Dict[str, int] = {}
        for device in devices:
            room = self._text(device.get("room") or device.get("area")) or "Unassigned"
            by_room[room] = by_room.get(room, 0) + 1
        rooms = ", ".join(f"{room} ({count})" for room, count in sorted(by_room.items())[:8])
        suffix = f", and {len(by_room) - 8} more rooms" if len(by_room) > 8 else ""
        return f"Found {len(devices)} {self.category_label}: {rooms}{suffix}."

    def _status_summary(self, devices: List[dict]) -> str:
        if len(devices) == 1:
            device = devices[0]
            name = self._text(device.get("name")) or self.singular_label.title()
            state = self._text(device.get("state") or device.get("status")) or "unknown"
            room = self._text(device.get("room") or device.get("area"))
            return f"{name}{f' in {room}' if room else ''} is {state}."
        shown = ", ".join(
            f"{self._text(device.get('name')) or self.singular_label.title()} ({self._text(device.get('state') or device.get('status') or 'unknown')})"
            for device in devices[:10]
        )
        suffix = f", and {len(devices) - 10} more" if len(devices) > 10 else ""
        return f"{shown}{suffix}."

    def _action_payload(self, intent: dict, payload: dict) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if intent.get("position") is not None:
            out["position"] = intent.get("position")
            out["position_pct"] = intent.get("position")
        if intent.get("volume") is not None:
            out["volume"] = intent.get("volume")
            out["volume_pct"] = intent.get("volume")
            out["level"] = intent.get("volume")
        if intent.get("percentage") is not None:
            out["percentage"] = intent.get("percentage")
            out["speed"] = intent.get("percentage")
            out["speed_pct"] = intent.get("percentage")
        if intent.get("brightness_pct") is not None:
            out["brightness"] = intent.get("brightness_pct")
            out["brightness_pct"] = intent.get("brightness_pct")
        if self._text(intent.get("action")) in {"set_color", "turn_on"}:
            if self._text(intent.get("color_name")):
                out["color_name"] = self._text(intent.get("color_name"))
            rgb_color = self._normalize_rgb(intent.get("rgb_color"))
            if rgb_color is not None:
                out["rgb_color"] = rgb_color
        if intent.get("temperature") is not None:
            out["temperature"] = intent.get("temperature")
            out["target_temperature"] = intent.get("temperature")
            out["temperature_unit"] = self._text(payload.get("temperature_unit") or "F")
        if self._text(intent.get("hvac_mode")):
            out["mode"] = self._text(intent.get("hvac_mode"))
            out["hvac_mode"] = self._text(intent.get("hvac_mode"))
        if self._text(intent.get("source_url")):
            out["source_url"] = self._text(intent.get("source_url"))
            out["url"] = self._text(intent.get("source_url"))
            out["media_url"] = self._text(intent.get("source_url"))
        if self._text(intent.get("command")):
            out["command"] = self._text(intent.get("command"))
        for key in ("message", "text", "media_content_id", "media_content_type"):
            if self._text(payload.get(key)):
                out[key] = payload.get(key)
        return out

    def _image_mimetype(self, value: Any, fallback: str = "image/jpeg") -> str:
        mimetype = self._text(value).split(";", 1)[0].strip().lower()
        if mimetype.startswith("image/"):
            return mimetype
        return fallback

    def _decode_snapshot_data(self, value: Any) -> Optional[bytes]:
        if isinstance(value, (bytes, bytearray, memoryview)):
            raw = bytes(value)
            return raw if raw else None
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if text.startswith("data:") and "," in text:
            text = text.split(",", 1)[1]
        pad = len(text) % 4
        if pad:
            text += "=" * (4 - pad)
        try:
            raw = base64.b64decode(text)
        except Exception:
            return None
        return raw if raw else None

    def _snapshot_bytes_and_mime(self, result: Any) -> Tuple[Optional[bytes], str]:
        mimetype = "image/jpeg"
        if isinstance(result, dict):
            mimetype = self._image_mimetype(
                result.get("content_type")
                or result.get("mimetype")
                or result.get("mime")
                or result.get("media_content_type"),
                mimetype,
            )
            for key in ("bytes", "data", "content", "image_bytes", "image_data"):
                raw = self._decode_snapshot_data(result.get(key))
                if raw:
                    return raw, mimetype
            return None, mimetype
        if isinstance(result, (tuple, list)) and result:
            raw = self._decode_snapshot_data(result[0])
            if len(result) > 1:
                mimetype = self._image_mimetype(result[1], mimetype)
            return raw, mimetype
        raw = self._decode_snapshot_data(result)
        return raw, mimetype

    def _snapshot_filename(self, device: dict, mimetype: str, index: int) -> str:
        label = self._text(device.get("name") or device.get("id") or f"{self.singular_label}-{index + 1}")
        stem = re.sub(r"[^a-zA-Z0-9._-]+", "-", label).strip("-._") or f"{self.singular_label}-{index + 1}"
        ext = mimetypes.guess_extension(mimetype or "image/jpeg") or ".jpg"
        if ext == ".jpe":
            ext = ".jpg"
        if not ext.startswith("."):
            ext = f".{ext}"
        return f"{stem}-snapshot{ext}"

    def _store_snapshot_blob(self, raw: bytes) -> str:
        try:
            from helpers import redis_blob_client

            key = f"tater:blob:camera_snapshot:{uuid.uuid4().hex}"
            redis_blob_client.set(key, bytes(raw or b""))
            if CAMERA_SNAPSHOT_ARTIFACT_TTL_SEC > 0:
                redis_blob_client.expire(key, CAMERA_SNAPSHOT_ARTIFACT_TTL_SEC)
            return key
        except Exception as exc:
            logger.debug("[%s] failed to store camera snapshot artifact blob: %s", self.name, exc)
            return ""

    def _camera_snapshot_artifact(self, *, result: Any, device: dict, index: int) -> Optional[Dict[str, Any]]:
        if isinstance(result, dict):
            existing_ref = {
                key: self._text(result.get(key))
                for key in ("path", "blob_key", "url", "file_id")
                if self._text(result.get(key))
            }
            if existing_ref:
                mimetype = self._image_mimetype(result.get("content_type") or result.get("mimetype"))
                filename = self._text(result.get("name") or result.get("filename")) or self._snapshot_filename(device, mimetype, index)
                return {
                    "artifact_id": f"camera_snapshot_{uuid.uuid4().hex[:8]}",
                    "type": "image",
                    "name": filename,
                    "mimetype": mimetype,
                    "source": self.name,
                    "device_name": self._text(device.get("name")),
                    "integration_id": self._text(device.get("integration_id")),
                    "device_id": self._text(device.get("id") or device.get("ref")),
                    **existing_ref,
                }

        raw, mimetype = self._snapshot_bytes_and_mime(result)
        if not raw:
            return None
        filename = self._snapshot_filename(device, mimetype, index)
        blob_key = self._store_snapshot_blob(raw)
        artifact: Dict[str, Any] = {
            "artifact_id": f"camera_snapshot_{uuid.uuid4().hex[:8]}",
            "type": "image",
            "name": filename,
            "mimetype": mimetype,
            "source": self.name,
            "device_name": self._text(device.get("name")),
            "integration_id": self._text(device.get("integration_id")),
            "device_id": self._text(device.get("id") or device.get("ref")),
            "size": len(raw),
        }
        if blob_key:
            artifact["blob_key"] = blob_key
        else:
            artifact["bytes"] = raw
        return artifact

    def _safe_result_value(self, value: Any, *, depth: int = 0) -> Any:
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {"omitted": "binary_media", "size": len(value)}
        if isinstance(value, str):
            if depth <= 1 and len(value) > 1600:
                return value[:1600] + "..."
            if depth > 1 and len(value) > 700:
                return value[:700] + "..."
            return value
        if depth >= 5:
            return self._text(value)[:240]
        if isinstance(value, dict):
            out: Dict[str, Any] = {}
            for raw_key, raw_item in value.items():
                key = self._text(raw_key)
                key_norm = key.lower()
                if key_norm in {"bytes", "data", "content", "image_bytes", "image_data", "base64", "image_b64"}:
                    marker: Dict[str, Any] = {"omitted": "binary_media"}
                    if isinstance(raw_item, (bytes, bytearray, memoryview)):
                        marker["size"] = len(raw_item)
                    out[key] = marker
                    continue
                out[key] = self._safe_result_value(raw_item, depth=depth + 1)
            return out
        if isinstance(value, (list, tuple, set)):
            items = list(value)
            safe_items = [self._safe_result_value(item, depth=depth + 1) for item in items[:12]]
            if len(items) > 12:
                safe_items.append({"omitted_items": len(items) - 12})
            return safe_items
        return self._text(value)

    def _validate_intent(
        self,
        action: str,
        intent: dict,
        payload: Optional[dict] = None,
    ) -> Tuple[bool, str, List[str]]:
        payload = payload or {}
        if action == "set_position" and intent.get("position") is None:
            return False, "Position requires a percentage.", ["Include a position percentage from 0 to 100."]
        if action == "set_volume" and intent.get("volume") is None:
            return False, "Volume requires a percentage.", ["Include a volume percentage from 0 to 100."]
        if action == "set_percentage" and intent.get("percentage") is None:
            return False, "Fan speed requires a percentage.", ["Include a fan speed percentage from 0 to 100."]
        if action == "set_brightness" and intent.get("brightness_pct") is None:
            return False, "Brightness requires a percentage.", ["Include a brightness percentage from 0 to 100."]
        if action == "set_color" and not (
            self._text(intent.get("color_name")) or self._normalize_rgb(intent.get("rgb_color"))
        ):
            return False, "Color requires a name or RGB value.", [
                "Include a color such as blue, warm white, #33ccff, or rgb(51,204,255)."
            ]
        if action == "set_temperature" and intent.get("temperature") is None:
            return False, "Temperature requires a target temperature.", ["Include the target temperature."]
        if action == "set_hvac_mode" and not self._text(intent.get("hvac_mode")):
            return False, "HVAC mode is required.", ["Include a mode such as heat, cool, auto, or off."]
        if action == "send_command" and not self._text(intent.get("command")):
            return False, "Remote control requires a command.", ["Include a button command such as mute, home, back, or volume up."]
        if action in {"play_media", "announce"} and not (
            self._text(intent.get("source_url")) or self._text(payload.get("media_content_id"))
        ):
            return False, "Media playback requires a source URL or media content id.", [
                "Include a media URL in source_url, a media_content_id, or the request text."
            ]
        return True, "", []

    def _inventory_devices(self) -> List[dict]:
        if self.inventory_scope == "all":
            registry = get_integration_device_registry()
            return [
                dict(row)
                for row in (registry.get("devices") or [])
                if isinstance(row, dict)
            ]
        return [
            dict(row)
            for row in get_integration_devices_by_capability(self.category_id)
            if isinstance(row, dict)
        ]

    async def _handle(self, args, llm_client=None):
        payload = self._normalize_handler_args(args)
        query = self._text(payload.get("query") or payload.get("text") or payload.get("prompt"))
        if not query and self._text(payload.get("action")):
            query = self._text(payload.get("action"))
        if not query:
            return action_failure(
                code="missing_query",
                message=f"Please provide a {self.category_label} request in query.",
                needs=[f"Ask for a {self.category_label} action such as list or status."],
                say_hint=f"Ask what {self.category_label} request the user wants.",
            )

        intent = await self._interpret_query(payload, query, llm_client)
        action = self._text(intent.get("action")).lower()
        if action not in self.allowed_actions:
            return action_failure(
                code="missing_action",
                message=f"Please ask for a supported {self.category_label} action.",
                needs=[f"Use one of: {', '.join(sorted(self.allowed_actions))}."],
                say_hint=f"Ask which {self.category_label} action the user wants.",
            )
        ok, message, needs = self._validate_intent(action, intent, payload)
        if not ok:
            return action_failure(
                code="missing_action_detail",
                message=message,
                needs=needs,
                say_hint="Ask for the missing detail.",
            )

        try:
            devices = self._inventory_devices()
        except Exception as exc:
            return action_failure(
                code="device_inventory_failed",
                message=f"Could not read integrated {self.category_label}: {exc}",
                needs=["Check enabled integrations and their settings."],
                say_hint=f"Explain that Tater could not read the {self.category_label} inventory.",
            )

        if action == "list":
            return action_success(
                facts={"action": "list", "category": self.category_id, "device_count": len(devices)},
                data={"devices": devices},
                summary_for_user=self._list_summary(devices),
                say_hint=f"Briefly summarize available {self.category_label} by room.",
            )

        if not devices:
            return action_failure(
                code="no_devices",
                message=f"No {self.category_label} were found in enabled integrations.",
                needs=[f"Enable an integration that exposes {self.category_label}, then refresh devices."],
                say_hint=f"Explain that no {self.category_label} are currently available.",
            )

        selected, needs = await self._select_devices(
            devices=devices,
            payload=payload,
            query=query,
            intent=intent,
            llm_client=llm_client,
        )
        if not selected:
            return action_failure(
                code="device_selection_failed",
                message=f"Could not select a {self.singular_label} for this request.",
                needs=needs,
                say_hint=f"Ask which {self.singular_label} or room to use.",
            )

        if action == "status":
            return action_success(
                facts={"action": "status", "category": self.category_id, "device_count": len(selected)},
                data={"devices": selected, "intent": intent},
                summary_for_user=self._status_summary(selected),
                say_hint=f"Report the {self.category_label} status briefly.",
            )

        action_payload = self._action_payload(intent, payload)
        results: List[dict] = []
        failures: List[str] = []
        artifacts: List[Dict[str, Any]] = []
        for device in selected:
            integration_id = self._text(device.get("integration_id"))
            device_id = self._text(device.get("id") or device.get("ref"))
            if not integration_id or not device_id:
                failures.append(f"{self._text(device.get('name')) or self.singular_label} is missing integration or device id.")
                continue
            try:
                result = run_integration_device_action(integration_id, action, device_id, action_payload)
                results.append(
                    {
                        "integration_id": integration_id,
                        "device_id": device_id,
                        "device_name": device.get("name"),
                        "result": self._safe_result_value(result),
                    }
                )
                if action == "camera_snapshot":
                    artifact = self._camera_snapshot_artifact(result=result, device=device, index=len(artifacts))
                    if artifact:
                        artifacts.append(artifact)
            except Exception as exc:
                failures.append(f"{self._text(device.get('name')) or device_id}: {exc}")

        if failures and not results:
            return action_failure(
                code="device_action_failed",
                message=f"{self.singular_label.title()} action failed: " + "; ".join(failures[:4]),
                needs=[f"Check the owning integration settings and whether the {self.singular_label} supports this action."],
                data={"devices": selected, "intent": intent, "failures": failures},
                say_hint=f"Explain that the {self.singular_label} action failed.",
            )

        action_text = action.replace("_", " ")
        target_label = (
            self._text(selected[0].get("room") or selected[0].get("area"))
            if len(selected) > 1 and len({self._text(row.get("room") or row.get("area")) for row in selected}) == 1
            else self._format_device_choices(selected[:5])
        )
        if action == "camera_snapshot" and artifacts:
            summary = f"Captured {len(artifacts)} camera snapshot{'s' if len(artifacts) != 1 else ''}"
        else:
            summary = f"Ran {action_text} on {len(results)} {self.singular_label}{'s' if len(results) != 1 else ''}"
        if target_label:
            summary += f" for {target_label}"
        summary += "."
        if failures:
            summary += f" {len(failures)} {self.singular_label}{'s' if len(failures) != 1 else ''} failed."

        return action_success(
            facts={
                "action": action,
                "category": self.category_id,
                "requested_count": len(selected),
                "success_count": len(results),
                "failure_count": len(failures),
                "artifact_count": len(artifacts),
            },
            data={"devices": selected, "intent": intent, "results": results, "failures": failures},
            summary_for_user=summary,
            say_hint=(
                "If the user asked what the camera shows, use image_describe with the returned image artifact before answering. "
                "Otherwise confirm the camera snapshot briefly and mention failures if any."
                if action == "camera_snapshot"
                else f"Confirm the {self.category_label} action briefly and mention failures if any."
            ),
            artifacts=artifacts,
        )

    async def handle_webui(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_voice_core(self, args, llm_client, context=None):
        return await self._handle(args, llm_client)

    async def handle_macos(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_little_spud(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_xbmc(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_homekit(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_discord(self, message, args, llm_client):
        payload = self._normalize_handler_args(args or {})
        if not payload.get("query"):
            content = getattr(message, "content", "")
            if content:
                payload["query"] = content
        return await self._handle(payload, llm_client)

    async def handle_telegram(self, update, context, args, llm_client):
        payload = self._normalize_handler_args(args or {})
        if not payload.get("query"):
            message = getattr(update, "message", None)
            text = self._text(getattr(message, "text", ""))
            if text:
                payload["query"] = text
        return await self._handle(payload, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        payload = self._normalize_handler_args(args or {})
        if not payload.get("query") and body:
            payload["query"] = body
        return await self._handle(payload, llm_client)

    async def handle_irc(self, bot, channel, user, message, args, llm_client):
        payload = self._normalize_handler_args(args or {})
        if not payload.get("query") and message:
            payload["query"] = message
        return await self._handle(payload, llm_client)

    async def handle_meshtastic(self, packet, args, llm_client):
        return await self._handle(args or {}, llm_client)
