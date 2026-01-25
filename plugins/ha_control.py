# plugins/ha_control_plugin.py
import logging
import re
import json as _json
import time
import requests
from typing import Any, Dict, List, Optional, Set, Tuple

from plugin_base import ToolPlugin
from helpers import redis_client

logger = logging.getLogger("ha_control")
logger.setLevel(logging.INFO)


class HAClient:
    """Simple Home Assistant REST API helper (settings from Redis)."""

    def __init__(self):
        settings = redis_client.hgetall("plugin_settings:Home Assistant Control") or {}

        self.base_url = (settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        self.token = settings.get("HA_TOKEN")
        if not self.token:
            raise ValueError("Home Assistant token (HA_TOKEN) not set in plugin settings.")

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _req(self, method: str, path: str, json=None, timeout=15):
        url = f"{self.base_url}{path}"
        resp = requests.request(method, url, headers=self.headers, json=json, timeout=timeout)
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
        try:
            return resp.json()
        except Exception:
            return resp.text

    def render_template(self, template_str: str):
        return self._req("POST", "/api/template", json={"template": template_str})

    def call_service(self, domain: str, service: str, data: dict):
        return self._req("POST", f"/api/services/{domain}/{service}", json=data)

    def get_state(self, entity_id: str):
        return self._req("GET", f"/api/states/{entity_id}")

    def list_states(self):
        return self._req("GET", "/api/states") or []


class HAControlPlugin(ToolPlugin):
    name = "ha_control"
    plugin_name = "Home Assistant Control"
    pretty_name = "Home Assistant Control"

    settings_category = "Home Assistant Control"
    platforms = ["homeassistant", "webui", "xbmc", "homekit"]

    # ONLY pass the user's exact request. Plugin infers everything else.
    usage = (
        "{\n"
        '  "function": "ha_control",\n'
        '  "arguments": {\n'
        '    "query": "The user’s request in natural language. If the user uses pronouns (it/them/those/that), '
        'restate the request with the previously mentioned device or group."\n'
        "  }\n"
        "}\n"
    )

    description = (
        "Control or check Home Assistant devices like lights, switches, thermostats, locks, covers, "
        "remotes for TVs/streaming devices, temperatures, and sensors."
    )
    plugin_dec = "Control Home Assistant devices, including TV remotes, and read sensors."

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re accessing Home Assistant devices now! "
        "Only output that message."
    )

    required_settings = {
        "HA_BASE_URL": {
            "label": "Home Assistant Base URL",
            "type": "string",
            "default": "http://homeassistant.local:8123",
            "description": "Base URL of your Home Assistant instance."
        },
        "HA_TOKEN": {
            "label": "Long-Lived Access Token",
            "type": "string",
            "default": "",
            "description": "A Home Assistant long-lived token for API access."
        },
        "HA_CATALOG_CACHE_SECONDS": {
            "label": "Catalog Cache Seconds",
            "type": "string",
            "default": "60",
            "description": "How long to cache the compact entity catalog in Redis."
        },
        "HA_MAX_CANDIDATES": {
            "label": "Max Candidates Sent to LLM",
            "type": "string",
            "default": "400",
            "description": "Max candidates to send in a single LLM call (tournament chunking used above this)."
        },
        "HA_CHUNK_SIZE": {
            "label": "LLM Tournament Chunk Size",
            "type": "string",
            "default": "120",
            "description": "Chunk size for tournament selection when candidate list is very large."
        },
        "HA_INTERPRET_CACHE_SECONDS": {
            "label": "Interpret Cache Seconds",
            "type": "string",
            "default": "45",
            "description": "Cache LLM interpret results per-query (seconds). Fast-path rules still run first."
        },
        "HA_CHOOSE_CACHE_SECONDS": {
            "label": "Choose Cache Seconds",
            "type": "string",
            "default": "45",
            "description": "Cache LLM chosen entity per-query+catalog (seconds)."
        },
        "HA_FASTPATH_ENABLED": {
            "label": "Enable Fast-Path Parsing",
            "type": "string",
            "default": "true",
            "description": "If true, common commands (lights, brightness, color, thermostat set, remote buttons) skip the interpret LLM."
        },
    }

    # ----------------------------
    # Settings helpers
    # ----------------------------
    def _get_plugin_settings(self) -> dict:
        return redis_client.hgetall("plugin_settings:Home Assistant Control") or {}

    def _get_int_setting(self, key: str, default: int) -> int:
        raw = (self._get_plugin_settings().get(key) or "").strip()
        try:
            return int(float(raw))
        except Exception:
            return default

    def _get_bool_setting(self, key: str, default: bool) -> bool:
        raw = (self._get_plugin_settings().get(key) or "").strip().lower()
        if raw in ("1", "true", "yes", "on"):
            return True
        if raw in ("0", "false", "no", "off"):
            return False
        return default

    # ----------------------------
    # Internal helpers
    # ----------------------------
    def _get_client(self):
        try:
            return HAClient()
        except Exception as e:
            logger.error(f"[ha_control] Failed to initialize HA client: {e}")
            return None

    def _excluded_entities_set(self) -> set[str]:
        """
        Read up to five Voice PE entity IDs from platform settings and exclude them
        from light control calls.
        """
        plat = redis_client.hgetall("homeassistant_platform_settings") or {}
        ids = []
        keys = ("VOICE_PE_ENTITY_1", "VOICE_PE_ENTITY_2", "VOICE_PE_ENTITY_3", "VOICE_PE_ENTITY_4", "VOICE_PE_ENTITY_5")
        for k in keys:
            v = (plat.get(k) or plat.get(k.lower()) or "").strip()
            if v:
                ids.append(v.lower())
        excluded = set(ids)
        logger.debug(f"[ha_control] excluded voice PE entities: {excluded}")
        return excluded

    @staticmethod
    def _contains_any(text: str, words: List[str]) -> bool:
        t = (text or "").lower()
        return any(w in t for w in words)

    # ---- CRITICAL FIX: hard guard so "lights to blue" never routes to thermostat temperature ----
    def _is_light_color_command(self, text: str) -> bool:
        """
        True when the user is clearly changing light color.
        This must take precedence over any thermostat/set_temperature logic.
        """
        t = (text or "").lower()
        if not t:
            return False

        # must be about lights
        is_lightish = any(w in t for w in [" light", " lights", "lamp", "bulb", "led", "hue", "sconce"])
        if not is_lightish:
            return False

        # must mention a known color phrase (or "color" itself)
        has_color_word = bool(re.search(
            r"\b(red|orange|yellow|green|cyan|blue|purple|magenta|pink|white|warm white|cool white)\b",
            t
        )) or (" color" in t)

        if not has_color_word:
            return False

        # if they explicitly say thermostat/hvac, it's not a light command
        if any(w in t for w in ["thermostat", "hvac", "heat", "cool", "setpoint", "climate"]):
            return False

        return True

    def _parse_color_name_from_text(self, text: str) -> Optional[str]:
        if not text:
            return None
        m = re.search(
            r"\b(red|orange|yellow|green|cyan|blue|purple|magenta|pink|white|warm white|cool white)\b",
            text,
            re.I,
        )
        return m.group(1).lower() if m else None

    def _parse_brightness_pct_from_text(self, text: str) -> Optional[int]:
        """
        Matches:
          - "to 50%" / "at 50%" / "50 percent"
          - "brightness 50"
        """
        if not text:
            return None
        m = re.search(r"\b(\d{1,3})\s*(%|percent)\b", text, re.I)
        if m:
            try:
                v = int(m.group(1))
                if 0 <= v <= 100:
                    return v
            except Exception:
                pass
        m2 = re.search(r"\bbrightness\s*(\d{1,3})\b", text, re.I)
        if m2:
            try:
                v = int(m2.group(1))
                if 0 <= v <= 100:
                    return v
            except Exception:
                pass
        return None

    def _parse_temperature_from_text(self, text: str) -> Optional[float]:
        """
        Matches:
          - "set to 74"
          - "to 74 degrees"
          - "74°"
        """
        if not text:
            return None
        m = re.search(r"\b(?:to|set to|set)\s*(\d{2,3})(?:\s*(?:degrees|°|deg))?\b", text, re.I)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                return None
        m2 = re.search(r"\b(\d{2,3})\s*(?:degrees|°|deg)\b", text, re.I)
        if m2:
            try:
                return float(m2.group(1))
            except Exception:
                return None
        return None

    # ----------------------------
    # Fast-path intent parsing (skip interpret LLM for common commands)
    # ----------------------------
    def _fast_intent_from_text(self, query: str) -> Optional[dict]:
        """
        Returns a full intent dict when we're confident.
        Otherwise returns None to fall back to the LLM interpreter.
        """
        q = (query or "").strip()
        t = q.lower()

        def mk(intent: str, action: str, domain_hint: str, desired: Optional[dict] = None, scope: str = "unknown") -> dict:
            d = desired or {}
            # ensure desired keys exist
            out = {
                "intent": intent,
                "action": action,
                "scope": scope,
                "domain_hint": domain_hint,
                "desired": {
                    "temperature": d.get("temperature"),
                    "brightness_pct": d.get("brightness_pct"),
                    "color_name": d.get("color_name"),
                    "activity": d.get("activity"),
                    "command": d.get("command"),
                }
            }
            return out

        # Remote button presses
        cmd = self._normalize_remote_command(q)
        if cmd:
            return mk("control", "send_command", "remote", {"command": cmd})

        # Light color changes
        if self._is_light_color_command(q):
            cn = self._parse_color_name_from_text(q) or "white"
            return mk("control", "turn_on", "light", {"color_name": cn})

        # Light brightness changes
        bp = self._parse_brightness_pct_from_text(q)
        if bp is not None and any(w in t for w in ["light", "lights", "lamp", "bulb", "led", "hue", "sconce"]):
            return mk("control", "turn_on", "light", {"brightness_pct": bp})

        # Thermostat setpoint
        tp = self._parse_temperature_from_text(q)
        if tp is not None and any(w in t for w in ["thermostat", "hvac", "climate", "heat", "cool"]):
            return mk("set_temperature", "set_temperature", "climate", {"temperature": tp})

        # Generic on/off lights
        if ("turn on" in t or "turn off" in t) and any(w in t for w in ["light", "lights", "lamp", "bulb", "led", "hue", "sconce"]):
            act = "turn_on" if "turn on" in t else "turn_off"
            return mk("control", act, "light")

        return None

    # ----------------------------
    # Power intent helpers (TVs usually live under media_player/switch)
    # ----------------------------
    def _is_power_request(self, action: str, query: str) -> bool:
        """
        Only treat as a "power" request when the text implies AV gear.
        Do NOT treat generic turn_on/turn_off as "power" automatically.
        """
        q = (query or "").lower()
        a = (action or "").lower().strip()

        power_words = ("turn on", "turn off", "power on", "power off")
        if not any(p in q for p in power_words) and a not in ("turn_on", "turn_off"):
            return False

        tv_words = ("tv", "television", "roku", "apple tv", "appletv", "shield", "fire tv", "chromecast", "receiver", "soundbar")
        return any(w in q for w in tv_words)

    @staticmethod
    def _state_key(st: Any) -> str:
        """
        Build a stable-ish representation of state to detect changes.
        Useful because many remotes report activity/current_activity rather than state.
        """
        if not isinstance(st, dict):
            return str(st or "")
        attrs = st.get("attributes") or {}
        if not isinstance(attrs, dict):
            attrs = {}
        return "|".join([
            str(st.get("state", "") or ""),
            str(attrs.get("current_activity") or attrs.get("activity") or ""),
            str(attrs.get("source") or ""),
            str(attrs.get("app_id") or ""),
        ])

    def _find_related_media_player(self, catalog: List[dict], remote_friendly: str, query: str) -> Optional[str]:
        """
        Best-effort: find a media_player that looks like the same device as a remote.
        This is a fallback when remote.turn_on doesn't actually power the device.
        """
        rf = (remote_friendly or "").strip().lower()
        q = (query or "").strip().lower()

        mps = [c for c in catalog if (c.get("domain") == "media_player" and c.get("entity_id"))]
        if not mps:
            return None

        def score(c: dict) -> int:
            name = (c.get("name") or "").strip().lower()
            eid = (c.get("entity_id") or "").strip().lower()
            blob = f"{name} {eid}"

            s = 0
            if rf and (name == rf or rf in blob or name in rf):
                s += 50
            for w in ["tv", "television", "oled", "lg", "samsung", "sony", "roku", "apple tv", "appletv", "shield", "receiver", "soundbar"]:
                if w in q and w in blob:
                    s += 10
            if "tv" in q and "tv" in blob:
                s += 5
            return s

        best = None
        best_score = -1
        for c in mps:
            sc = score(c)
            if sc > best_score:
                best_score = sc
                best = c

        if best and best_score >= 10:
            return best.get("entity_id")
        return None

    # ----------------------------
    # Remote helpers (broad compatibility)
    # ----------------------------
    def _normalize_remote_command(self, text: str) -> Optional[str]:
        """
        Convert common voice phrases into a "base command".
        We'll expand it into multiple variants later to match different integrations.
        """
        t = (text or "").lower().strip()
        if not t:
            return None

        mapping = [
            (["volume up", "vol up", "turn it up", "louder"], "volume_up"),
            (["volume down", "vol down", "turn it down", "quieter"], "volume_down"),
            (["unmute"], "mute"),
            (["mute"], "mute"),
            (["pause"], "pause"),
            (["play"], "play"),
            (["stop"], "stop"),
            (["back", "go back"], "back"),
            (["home"], "home"),
            (["menu"], "menu"),
            (["select", "ok", "okay", "enter"], "select"),
            (["up"], "up"),
            (["down"], "down"),
            (["left"], "left"),
            (["right"], "right"),
            (["rewind"], "rewind"),
            (["fast forward", "fast-forward", "forward"], "fast_forward"),
        ]

        for phrases, cmd in mapping:
            if any(p in t for p in phrases):
                return cmd
        return None

    def _command_variants(self, base_or_raw: str) -> List[str]:
        """
        Return a list of likely command-name variants to try with remote.send_command.
        This is the key to "works with most remotes".
        """
        raw = (base_or_raw or "").strip()
        if not raw:
            return []

        variants: List[str] = []

        def add(v: str):
            v = (v or "").strip()
            if v and v not in variants:
                variants.append(v)

        add(raw)

        base = raw.strip().lower()
        base = base.replace("-", "_").replace(" ", "_")

        camel_to_snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", raw).lower().replace("-", "_").replace(" ", "_")
        if camel_to_snake and camel_to_snake != base:
            base = camel_to_snake

        common = {
            "volume_up": ["VolumeUp", "VOLUMEUP", "volume_up", "volumeUp", "KEY_VOLUMEUP", "KEY_VOLUP", "VOLUP"],
            "volume_down": ["VolumeDown", "VOLUMEDOWN", "volume_down", "volumeDown", "KEY_VOLUMEDOWN", "KEY_VOLDOWN", "VOLDOWN"],
            "mute": ["Mute", "MUTE", "mute", "KEY_MUTE", "VOLUME_MUTE"],
            "play": ["Play", "PLAY", "play", "KEY_PLAY"],
            "pause": ["Pause", "PAUSE", "pause", "KEY_PAUSE"],
            "stop": ["Stop", "STOP", "stop", "KEY_STOP"],
            "home": ["Home", "HOME", "home", "KEY_HOME"],
            "menu": ["Menu", "MENU", "menu", "KEY_MENU"],
            "back": ["Back", "BACK", "back", "KEY_BACK", "RETURN"],
            "select": ["Select", "SELECT", "select", "OK", "Ok", "ok", "ENTER", "Enter", "enter", "KEY_OK", "KEY_ENTER"],
            "up": ["Up", "UP", "up", "DirectionUp", "DIR_UP", "KEY_UP"],
            "down": ["Down", "DOWN", "down", "DirectionDown", "DIR_DOWN", "KEY_DOWN"],
            "left": ["Left", "LEFT", "left", "DirectionLeft", "DIR_LEFT", "KEY_LEFT"],
            "right": ["Right", "RIGHT", "right", "DirectionRight", "DIR_RIGHT", "KEY_RIGHT"],
            "rewind": ["Rewind", "REWIND", "rewind", "KEY_REWIND"],
            "fast_forward": ["FastForward", "FASTFORWARD", "fast_forward", "fastForward", "FF", "KEY_FASTFORWARD"],
        }

        if base in common:
            for v in common[base]:
                add(v)
        else:
            add(raw.upper())
            add(raw.lower())
            add(base)
            parts = [p for p in base.split("_") if p]
            if parts:
                camel = "".join(p[:1].upper() + p[1:] for p in parts)
                add(camel)

        return variants

    def _guess_activity_from_text(self, text: str) -> Optional[str]:
        t = (text or "").lower()
        if not t:
            return None

        if "roku" in t:
            return "Roku"
        if "apple tv" in t or "appletv" in t:
            return "Apple TV"
        if "watch tv" in t or "tv" in t:
            return "Watch TV"
        if "ps5" in t or "playstation" in t:
            return "Play PS5"
        if "xbox" in t:
            return "Play Xbox"
        if "music" in t:
            return "Listen to Music"
        return None

    def _best_match_from_list(self, wanted: str, options: List[str]) -> Optional[str]:
        if not wanted or not options:
            return None
        w = wanted.strip().lower()
        for o in options:
            if isinstance(o, str) and o.strip().lower() == w:
                return o
        for o in options:
            if isinstance(o, str) and w in o.strip().lower():
                return o
        return None

    # ----------------------------
    # Catalog (grounding)
    # ----------------------------
    def _catalog_cache_key(self) -> str:
        return "ha_control:catalog:v4"

    def _build_catalog_from_states(self, states: List[dict]) -> List[dict]:
        catalog: List[dict] = []
        for s in states:
            if not isinstance(s, dict):
                continue
            eid = s.get("entity_id", "")
            if "." not in eid:
                continue

            dom = eid.split(".", 1)[0]
            attrs = s.get("attributes") or {}
            if not isinstance(attrs, dict):
                attrs = {}

            catalog.append({
                "entity_id": eid,
                "domain": dom,
                "name": attrs.get("friendly_name") or eid,
                "device_class": attrs.get("device_class"),
                "unit": attrs.get("unit_of_measurement"),
            })
        return catalog

    def _get_catalog_cached(self, client: HAClient) -> List[dict]:
        cache_seconds = self._get_int_setting("HA_CATALOG_CACHE_SECONDS", 60)
        key = self._catalog_cache_key()

        try:
            raw = redis_client.get(key)
            if raw:
                data = _json.loads(raw)
                if isinstance(data, dict) and "ts" in data and "catalog" in data:
                    ts = float(data["ts"])
                    if time.time() - ts <= max(5, cache_seconds):
                        cat = data["catalog"]
                        if isinstance(cat, list):
                            return cat
        except Exception:
            pass

        states = client.list_states()
        catalog = self._build_catalog_from_states(states)

        try:
            redis_client.set(key, _json.dumps({"ts": time.time(), "catalog": catalog}, ensure_ascii=False))
            try:
                redis_client.expire(key, max(10, cache_seconds))
            except Exception:
                pass
        except Exception:
            pass

        return catalog

    def _catalog_ts(self) -> Optional[float]:
        """
        Read catalog timestamp from cache if available (used for choose-cache keying).
        """
        key = self._catalog_cache_key()
        try:
            raw = redis_client.get(key)
            if raw:
                data = _json.loads(raw)
                if isinstance(data, dict) and "ts" in data:
                    return float(data["ts"])
        except Exception:
            pass
        return None

    # ----------------------------
    # LLM caching helpers
    # ----------------------------
    def _cache_get_json(self, key: str) -> Optional[dict]:
        try:
            raw = redis_client.get(key)
            if not raw:
                return None
            data = _json.loads(raw)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _cache_set_json(self, key: str, value: dict, ttl_seconds: int):
        try:
            redis_client.set(key, _json.dumps(value, ensure_ascii=False))
            if ttl_seconds and ttl_seconds > 0:
                try:
                    redis_client.expire(key, int(ttl_seconds))
                except Exception:
                    pass
        except Exception:
            pass

    # ----------------------------
    # Step 1: LLM interprets query → intent (with cache)
    # ----------------------------
    async def _interpret_query(self, query: str, llm_client) -> dict:
        allowed_domain = "light,switch,climate,sensor,binary_sensor,cover,lock,fan,media_player,scene,script,select,remote"

        cache_ttl = self._get_int_setting("HA_INTERPRET_CACHE_SECONDS", 45)
        cache_key = f"ha_control:interpret:v1:{query.strip().lower()}"
        if cache_ttl > 0:
            cached = self._cache_get_json(cache_key)
            if cached and isinstance(cached, dict) and cached.get("intent"):
                return cached

        system = (
            "You are interpreting a smart-home request for Home Assistant.\n"
            "Return STRICT JSON only. No explanation.\n"
            "Schema:\n"
            "{\n"
            '  "intent": "get_temp|get_state|control|set_temperature",\n'
            '  "action": "turn_on|turn_off|open|close|get_state|set_temperature|send_command",\n'
            '  "scope": "inside|outside|area:<name>|device:<phrase>|unknown",\n'
            f'  "domain_hint": "one of: {allowed_domain}",\n'
            '  "desired": {\n'
            '     "temperature": <number or null>,\n'
            '     "brightness_pct": <int 0-100 or null>,\n'
            '     "color_name": <string or null>,\n'
            '     "activity": <string or null>,\n'
            '     "command": <string or null>\n'
            "  }\n"
            "}\n"
            "Rules:\n"
            "- If user asks 'what's the temp inside' or 'temp in the kitchen', intent=get_temp, action=get_state.\n"
            "- If user asks 'thermostat set to' or 'thermostat temp', intent=get_state, domain_hint=climate.\n"
            "- If user says 'set thermostat to 74', intent=set_temperature, action=set_temperature, domain_hint=climate.\n"
            "- For lights, domain_hint=light and action turn_on/turn_off accordingly.\n"
            "- If user says 'set lights to blue' / 'turn lights blue', that's lights (domain_hint=light), NOT thermostat.\n"
            "- If user asks to set lights to a percent (brightness), you MUST use intent=control and action=turn_on,\n"
            "  and put the percent into desired.brightness_pct. Do NOT use actions like set_brightness.\n"
            "- For remotes, domain_hint=remote.\n"
            "- 'turn on the tv/roku/apple tv' usually means device power; it may be media_player.turn_on.\n"
            "- 'mute', 'volume up', 'pause', 'play', 'home', 'back', 'menu' means action=send_command and desired.command.\n"
            "- If scope is a room/area (kitchen, living room), use scope=area:<name>.\n"
            "- If it's a named device (christmas tree lights), use scope=device:<phrase>.\n"
        )

        resp = await llm_client.chat(messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": query.strip()},
        ])
        content = (resp.get("message", {}) or {}).get("content", "").strip()
        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
        data = _json.loads(content)
        if not isinstance(data, dict):
            raise ValueError("LLM interpret_query did not return JSON object")

        if cache_ttl > 0:
            self._cache_set_json(cache_key, data, cache_ttl)

        return data

    # ----------------------------
    # Step 2: Candidate building (grounded)
    # ----------------------------
    def _candidates_temperature(self, catalog: List[dict], scope: str) -> List[dict]:
        temps = [
            c for c in catalog
            if (c.get("domain") == "sensor" and (c.get("device_class") or "").lower() == "temperature")
        ]
        if not temps:
            return []

        scope_l = (scope or "").lower()

        if scope_l == "inside":
            outdoor_words = ("outside", "outdoor", "yard", "back yard", "backyard", "front yard", "porch", "patio", "driveway")
            filtered = []
            for c in temps:
                name = (c.get("name") or "").lower()
                eid = (c.get("entity_id") or "").lower()
                blob = f"{name} {eid}"
                if any(w in blob for w in outdoor_words):
                    continue
                filtered.append(c)
            return filtered if filtered else temps

        return temps

    def _candidates_for_domains(self, catalog: List[dict], domains: Set[str]) -> List[dict]:
        doms = {d.lower().strip() for d in (domains or set()) if d}
        return [c for c in catalog if (c.get("domain") or "").lower() in doms]

    def _domains_for_control(self, domain_hint: str, action: str, query: str) -> Set[str]:
        """
        Domain prioritization:
        - If we KNOW it's lights, always stay in light.
        - Remote is for button presses/activities.
        - "Power" routing only for TV-ish requests.
        """
        dh = (domain_hint or "").lower().strip()

        # ✅ Never override a light intent
        if dh == "light":
            return {"light"}

        # Remote requests stay remote-first
        if dh == "remote":
            return {"remote", "media_player"}

        # Only TV-ish "power" requests prefer media_player/switch/remote
        if self._is_power_request(action, query):
            return {"media_player", "switch", "remote"}

        # Respect other domain hints
        if dh:
            return {dh}

        return {"light", "switch", "fan", "media_player", "scene", "script", "cover", "lock", "remote"}

    # ----------------------------
    # Step 3: LLM chooser (grounded) + tournament chunking + cache + single-candidate shortcut
    # ----------------------------
    async def _choose_entity_llm(self, query: str, intent: dict, candidates: List[dict], llm_client) -> Optional[str]:
        if not candidates:
            return None

        # ✅ If only one candidate, skip the LLM entirely
        if len(candidates) == 1 and candidates[0].get("entity_id"):
            return candidates[0]["entity_id"]

        mini = [{
            "entity_id": c.get("entity_id"),
            "domain": c.get("domain"),
            "name": c.get("name"),
            "device_class": c.get("device_class"),
            "unit": c.get("unit"),
        } for c in candidates if c.get("entity_id")]

        if not mini:
            return None

        candidate_set = {c["entity_id"] for c in mini if c.get("entity_id")}

        # ✅ Choose-cache keyed by query + (approx) catalog timestamp
        cache_ttl = self._get_int_setting("HA_CHOOSE_CACHE_SECONDS", 45)
        cat_ts = self._catalog_ts()
        cache_key = None
        if cache_ttl > 0:
            cache_key = f"ha_control:choose:v2:{(query or '').strip().lower()}:{str(cat_ts or 'none')}:{_json.dumps(intent, sort_keys=True, ensure_ascii=False)}"
            try:
                cached = self._cache_get_json(cache_key)
                if cached and isinstance(cached, dict):
                    ceid = cached.get("entity_id")
                    if isinstance(ceid, str) and ceid in candidate_set:
                        return ceid
            except Exception:
                pass

        system = (
            "Pick the SINGLE best Home Assistant entity for this user request.\n"
            "You MUST choose an entity_id from the provided candidates (no inventions).\n"
            "Return strict JSON only: {\"entity_id\":\"...\"}. No explanation.\n"
            "Use the user's exact words to match rooms/devices.\n"
            "If the request is temperature inside, do NOT pick obvious outside/outdoor sensors.\n"
            "If the request is to TURN ON/OFF a TV/device, prefer a matching media_player or switch first.\n"
            "Use remote.* mainly for button presses (volume/mute/home/back/menu/select) or starting activities.\n"
        )

        async def ask_pick(chunk: List[dict]) -> Optional[str]:
            user = _json.dumps({
                "query": query,
                "intent": intent,
                "candidates": chunk
            }, ensure_ascii=False)
            resp = await llm_client.chat(messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ])
            content = (resp.get("message", {}) or {}).get("content", "").strip()
            content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
            data = _json.loads(content)
            eid = data.get("entity_id")
            if isinstance(eid, str) and eid.strip():
                eid = eid.strip()
                return eid if eid in candidate_set else None
            return None

        max_single = self._get_int_setting("HA_MAX_CANDIDATES", 400)
        chunk_size = self._get_int_setting("HA_CHUNK_SIZE", 120)

        # Prefer single-shot when reasonable
        picked: Optional[str] = None
        if len(mini) <= max_single:
            try:
                picked = await ask_pick(mini)
            except Exception as e:
                logger.warning(f"[ha_control] LLM choose failed single-shot: {e}")

        # Tournament fallback
        if not picked:
            try:
                winners: List[dict] = []
                for i in range(0, len(mini), chunk_size):
                    chunk = mini[i:i + chunk_size]
                    eid = await ask_pick(chunk)
                    if eid:
                        winners.append(next(c for c in chunk if c["entity_id"] == eid))

                if winners:
                    eid = await ask_pick(winners)
                    picked = eid or winners[0]["entity_id"]
                else:
                    picked = next(iter(candidate_set), None)
            except Exception as e:
                logger.warning(f"[ha_control] LLM choose failed tournament: {e}")
                picked = next(iter(candidate_set), None)

        if picked and cache_key and cache_ttl > 0:
            self._cache_set_json(cache_key, {"entity_id": picked}, cache_ttl)

        return picked

    # ----------------------------
    # Service mapping + confirmation
    # ----------------------------
    def _service_for_action(self, action: str, entity_domain: str) -> Optional[Tuple[str, dict]]:
        a = (action or "").lower().strip()
        d = (entity_domain or "").lower().strip()

        # ✅ (A) Brightness is implemented via light.turn_on with brightness_pct
        if a in ("set_brightness", "brightness", "dim", "set_level") and d == "light":
            return "turn_on", {}

        if a in ("turn_on", "turn_off"):
            return a, {}

        if a == "open":
            if d == "cover":
                return "open_cover", {}
            if d == "lock":
                return "unlock", {}
            return "open", {}

        if a == "close":
            if d == "cover":
                return "close_cover", {}
            if d == "lock":
                return "lock", {}
            return "close", {}

        if a == "set_temperature" and d == "climate":
            return "set_temperature", {}

        # ✅ Remote mapping (Harmony/Apple TV/etc.)
        if d == "remote":
            if a in ("turn_on", "start", "power_on", "on"):
                return "turn_on", {}
            if a in ("turn_off", "stop", "power_off", "off"):
                return "turn_off", {}
            if a in ("send_command", "command", "press", "mute", "volume_up", "volume_down", "pause", "play", "home", "back", "select", "ok", "menu"):
                return "send_command", {}

        return None

    async def _speak_response_state(self, user_query: str, friendly: str, value: str, unit: str, llm_client) -> str:
        system = (
            "You are a smart home voice assistant.\n"
            "Write exactly ONE short, natural spoken response.\n"
            "- No emojis. No technical wording. No entity IDs.\n"
            "- If the value is numeric and a unit is provided, include it naturally.\n\n"
            f"User asked: {user_query}\n"
            f"Entity: {friendly}\n"
            f"Value: {value}\n"
            f"Unit: {unit}\n"
        )
        try:
            resp = await llm_client.chat(messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": "Say it now."},
            ])
            msg = (resp.get("message", {}) or {}).get("content", "").strip()
            return msg or f"{friendly} is {value}{(' ' + unit) if unit else ''}."
        except Exception:
            return f"{friendly} is {value}{(' ' + unit) if unit else ''}."

    async def _speak_response_confirm(self, user_query: str, friendly: str, action_spoken: str, extras: str, llm_client) -> str:
        system = (
            "You are a smart home voice assistant.\n"
            "Write exactly ONE short, natural confirmation sentence.\n"
            "Constraints:\n"
            "- Sound conversational and spoken aloud.\n"
            "- Mention the device name naturally.\n"
            "- Include extra details only if provided.\n"
            "- No emojis. No technical wording. No entity IDs.\n\n"
            f"User asked: {user_query}\n"
            f"Result: {action_spoken} {friendly}.\n"
            f"Extras: {extras}\n"
        )
        try:
            resp = await llm_client.chat(messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": "Say it now."},
            ])
            msg = (resp.get("message", {}) or {}).get("content", "").strip()
            return msg or f"Okay, {action_spoken} {friendly}."
        except Exception:
            return f"Okay, {action_spoken} {friendly}."

    # ----------------------------
    # Handlers
    # ----------------------------
    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_webui(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_xbmc(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_homekit(self, args, llm_client):
        return await self._handle(args, llm_client)

    # ----------------------------
    # Core logic
    # ----------------------------
    async def _handle(self, args, llm_client):
        client = self._get_client()
        if not client:
            return "Home Assistant is not configured. Please set HA_BASE_URL and HA_TOKEN in the plugin settings."

        query = (args.get("query") or "").strip()
        if not query:
            return "Please provide 'query' with the user's exact request."

        excluded = self._excluded_entities_set()

        try:
            catalog = self._get_catalog_cached(client)
        except Exception as e:
            logger.error(f"[ha_control] catalog build failed: {e}")
            return "I couldn't access Home Assistant states."

        # ✅ Fast-path: try deterministic intent parsing first (skips interpret LLM for common commands)
        intent: Optional[dict] = None
        if self._get_bool_setting("HA_FASTPATH_ENABLED", True):
            try:
                intent = self._fast_intent_from_text(query)
            except Exception:
                intent = None

        if not intent:
            try:
                intent = await self._interpret_query(query, llm_client)
            except Exception as e:
                logger.error(f"[ha_control] interpret_query failed: {e}")
                return "I couldn't understand that request."

        intent_type = (intent.get("intent") or "").strip()
        action = (intent.get("action") or "").strip()
        scope = (intent.get("scope") or "").strip()
        domain_hint = (intent.get("domain_hint") or "").strip()

        desired = intent.get("desired") or {}
        if not isinstance(desired, dict):
            desired = {}

        # Fill missing "desired" fields from the raw query
        if desired.get("color_name") in (None, "", "null"):
            cn = self._parse_color_name_from_text(query)
            if cn:
                desired["color_name"] = cn
        if desired.get("brightness_pct") in (None, "", "null"):
            bp = self._parse_brightness_pct_from_text(query)
            if bp is not None:
                desired["brightness_pct"] = bp
        if desired.get("temperature") in (None, "", "null"):
            tp = self._parse_temperature_from_text(query)
            if tp is not None:
                desired["temperature"] = tp

        # Remote fallbacks
        if desired.get("command") in (None, "", "null"):
            cmd = self._normalize_remote_command(query)
            if cmd:
                desired["command"] = cmd
        if desired.get("activity") in (None, "", "null"):
            act = self._guess_activity_from_text(query)
            if act:
                desired["activity"] = act

        # Light color hard-guard (extra safety)
        if self._is_light_color_command(query):
            intent_type = "control"
            action = "turn_on"
            domain_hint = "light"
            if not scope:
                scope = "unknown"
            if not desired.get("color_name"):
                desired["color_name"] = self._parse_color_name_from_text(query) or "white"

        is_temp_question = self._contains_any(query, ["temp", "temperature", "degrees"])
        if intent_type == "get_temp" or (is_temp_question and intent_type in ("get_state", "control", "set_temperature")):
            scope_l = (scope or "").lower()
            if not scope_l or scope_l == "unknown":
                if self._contains_any(query, ["outside", "outdoor"]):
                    scope = "outside"
                elif self._contains_any(query, ["inside", "in the house", "indoors"]):
                    scope = "inside"

            candidates = self._candidates_temperature(catalog, scope.lower() if scope else "unknown")
            if excluded:
                candidates = [c for c in candidates if (c.get("entity_id") or "").lower() not in excluded]

            entity_id = await self._choose_entity_llm(query, {"intent": "get_temp", "scope": scope}, candidates, llm_client)
            if not entity_id:
                return "I couldn’t find a temperature sensor for that."

            try:
                st = client.get_state(entity_id)
                val = st.get("state", "unknown") if isinstance(st, dict) else str(st)
                attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                friendly = (attrs.get("friendly_name") or entity_id)
                unit = (attrs.get("unit_of_measurement") or "")
                return await self._speak_response_state(query, friendly, str(val), str(unit), llm_client)
            except Exception as e:
                logger.error(f"[ha_control] temp get_state error: {e}")
                return f"Error reading {entity_id}: {e}"

        wants_thermostat = self._contains_any(query, ["thermostat", "hvac"])
        if wants_thermostat and intent_type in ("get_state", "control") and action == "get_state":
            climate_candidates = self._candidates_for_domains(catalog, {"climate"})
            if excluded:
                climate_candidates = [c for c in climate_candidates if (c.get("entity_id") or "").lower() not in excluded]

            entity_id = await self._choose_entity_llm(query, {"intent": "get_state", "domain_hint": "climate"}, climate_candidates, llm_client)
            if not entity_id:
                return "I couldn’t find a thermostat."

            try:
                st = client.get_state(entity_id)
                attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                friendly = (attrs.get("friendly_name") or entity_id)

                if self._contains_any(query, ["temp set", "temperature set", "set to", "setpoint"]):
                    temp_val = attrs.get("temperature")
                    unit = attrs.get("unit_of_measurement") or "°F"
                    if temp_val is not None:
                        return await self._speak_response_state(query, friendly, str(temp_val), str(unit), llm_client)

                val = st.get("state", "unknown") if isinstance(st, dict) else str(st)
                return await self._speak_response_state(query, friendly, str(val), "", llm_client)
            except Exception as e:
                logger.error(f"[ha_control] thermostat read error: {e}")
                return f"Error reading {entity_id}: {e}"

        if intent_type == "set_temperature" or action == "set_temperature":
            candidates = self._candidates_for_domains(catalog, {"climate"})
            if excluded:
                candidates = [c for c in candidates if (c.get("entity_id") or "").lower() not in excluded]

            entity_id = await self._choose_entity_llm(query, intent, candidates, llm_client)
            if not entity_id:
                return "I couldn’t find a thermostat to set."

            temperature = desired.get("temperature")
            try:
                temperature = float(temperature) if temperature is not None else None
            except Exception:
                temperature = None
            if temperature is None:
                return "Tell me what temperature you want, like 'set the thermostat to 74'."

            payload = {"entity_id": entity_id, "temperature": temperature}
            try:
                client.call_service("climate", "set_temperature", payload)
                st = client.get_state(entity_id)
                attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                friendly = (attrs.get("friendly_name") or entity_id)
                return await self._speak_response_confirm(query, friendly, f"set to {int(temperature)} degrees", "", llm_client)
            except Exception as e:
                logger.error(f"[ha_control] set_temperature error: {e}")
                return f"Error setting {entity_id}: {e}"

        if intent_type == "control":
            domains = self._domains_for_control(domain_hint, action, query)
            candidates = self._candidates_for_domains(catalog, domains)

            if excluded and "light" in {d.lower() for d in domains}:
                candidates = [
                    c for c in candidates
                    if not ((c.get("domain") or "").lower() == "light" and (c.get("entity_id") or "").lower() in excluded)
                ]

            entity_id = await self._choose_entity_llm(query, intent, candidates, llm_client)
            if not entity_id:
                return "I couldn’t find a device matching that."

            if "." not in entity_id:
                return "I couldn’t find a valid Home Assistant entity to control."

            entity_domain = entity_id.split(".", 1)[0].lower()
            mapped = self._service_for_action(action, entity_domain)
            if not mapped:
                return f"The action '{action}' is not supported for {entity_domain}."

            service, extra = mapped

            payload = {"entity_id": entity_id}
            payload.update(extra)

            extras_txt_parts: List[str] = []

            # Light extras
            if entity_domain == "light" and service in ("turn_on", "turn_off"):
                if desired.get("color_name"):
                    payload["color_name"] = str(desired["color_name"])
                    extras_txt_parts.append(f"color {payload['color_name']}")
                if desired.get("brightness_pct") is not None:
                    try:
                        payload["brightness_pct"] = int(desired["brightness_pct"])
                        extras_txt_parts.append(f"brightness {payload['brightness_pct']} percent")
                    except Exception:
                        pass

            # Remote (Harmony/Apple TV/etc.) extras/payload with compatibility retries
            if entity_domain == "remote":
                # Snapshot state before (to detect "no-op")
                try:
                    st_before = client.get_state(entity_id)
                except Exception:
                    st_before = None
                before_key = self._state_key(st_before)

                # Grab current attributes (for activity_list matching + friendly name)
                attrs_now = (st_before.get("attributes") or {}) if isinstance(st_before, dict) else {}
                if not isinstance(attrs_now, dict):
                    attrs_now = {}

                activity_list = attrs_now.get("activity_list") or attrs_now.get("activities") or []
                if not isinstance(activity_list, list):
                    activity_list = []

                desired_activity = desired.get("activity") if isinstance(desired.get("activity"), str) else ""
                desired_command = desired.get("command") if isinstance(desired.get("command"), str) else ""

                desired_activity = (desired_activity or "").strip()
                desired_command = (desired_command or "").strip()

                # Align guessed activity to real activity list when available
                if desired_activity and activity_list:
                    match = self._best_match_from_list(desired_activity, [a for a in activity_list if isinstance(a, str)])
                    if match:
                        desired_activity = match

                try:
                    if service == "turn_on":
                        did_work = False

                        # Try remote.turn_on first (with activity if any)
                        try:
                            if desired_activity:
                                payload_with_activity = dict(payload)
                                payload_with_activity["activity"] = desired_activity
                                client.call_service("remote", "turn_on", payload_with_activity)
                                extras_txt_parts.append(desired_activity)
                            else:
                                client.call_service("remote", "turn_on", payload)

                            # Detect effect
                            try:
                                st_after = client.get_state(entity_id)
                                after_key = self._state_key(st_after)
                                did_work = bool(after_key and after_key != before_key)
                            except Exception:
                                did_work = True  # we successfully called the service; assume ok

                        except Exception as e:
                            logger.info(f"[ha_control] remote.turn_on failed: {e}")

                        # If the remote didn't show any change, try powering a related media_player
                        if not did_work:
                            remote_friendly = attrs_now.get("friendly_name") or entity_id
                            mp_eid = self._find_related_media_player(catalog, str(remote_friendly), query)
                            if mp_eid:
                                try:
                                    client.call_service("media_player", "turn_on", {"entity_id": mp_eid})
                                    extras_txt_parts.append("power")
                                    did_work = True
                                except Exception as e:
                                    logger.info(f"[ha_control] media_player.turn_on fallback failed: {e}")

                        if not did_work:
                            return (
                                "I tried to turn it on, but that remote didn’t report any change. "
                                "If you tell me which Home Assistant entity actually powers that TV on, I can use it."
                            )

                    elif service == "turn_off":
                        # remote.turn_off sometimes works; if not, we can fall back to media_player.turn_off
                        did_work = False
                        try:
                            client.call_service("remote", "turn_off", payload)
                            try:
                                st_after = client.get_state(entity_id)
                                after_key = self._state_key(st_after)
                                did_work = bool(after_key and after_key != before_key)
                            except Exception:
                                did_work = True
                        except Exception as e:
                            logger.info(f"[ha_control] remote.turn_off failed: {e}")

                        if not did_work:
                            remote_friendly = attrs_now.get("friendly_name") or entity_id
                            mp_eid = self._find_related_media_player(catalog, str(remote_friendly), query)
                            if mp_eid:
                                try:
                                    client.call_service("media_player", "turn_off", {"entity_id": mp_eid})
                                    extras_txt_parts.append("power")
                                    did_work = True
                                except Exception as e:
                                    logger.info(f"[ha_control] media_player.turn_off fallback failed: {e}")

                        if not did_work:
                            return (
                                "I tried to turn it off, but that remote didn’t report any change. "
                                "If you tell me which Home Assistant entity actually powers that TV off, I can use it."
                            )

                    elif service == "send_command":
                        if not desired_command:
                            desired_command = self._normalize_remote_command(query) or ""
                        if not desired_command:
                            return "Tell me what button to press, like 'mute' or 'volume up'."

                        tried: List[str] = []
                        variants = self._command_variants(desired_command)

                        last_error: Optional[Exception] = None
                        for cmd in variants:
                            tried.append(cmd)
                            payload_cmd = dict(payload)
                            payload_cmd["command"] = [cmd]
                            try:
                                client.call_service("remote", "send_command", payload_cmd)
                                extras_txt_parts.append(cmd)
                                last_error = None
                                break
                            except Exception as e:
                                last_error = e
                                continue

                        if last_error is not None:
                            logger.info(f"[ha_control] remote.send_command failed. Tried: {tried}. Last error: {last_error}")
                            return (
                                "I tried a few command names for that remote, but none worked. "
                                "If you tell me what the button is called in Home Assistant, I can use it."
                            )
                    else:
                        client.call_service(entity_domain, service, payload)

                except Exception as e:
                    logger.error(f"[ha_control] remote control error: {e}")
                    return f"Error performing {service} on {entity_id}: {e}"

                # Confirm
                try:
                    st = client.get_state(entity_id)
                    attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                    friendly = (attrs.get("friendly_name") or entity_id)

                    if service == "turn_on":
                        spoken_action = "turned on"
                    elif service == "turn_off":
                        spoken_action = "turned off"
                    elif service == "send_command":
                        spoken_action = "sent"
                    else:
                        spoken_action = service.replace("_", " ")

                    extras_txt = ", ".join(extras_txt_parts) if extras_txt_parts else ""
                    return await self._speak_response_confirm(query, friendly, spoken_action, extras_txt, llm_client)
                except Exception as e:
                    logger.error(f"[ha_control] remote post-state error: {e}")
                    return "Done."

            # Generic non-remote control path
            try:
                client.call_service(entity_domain, service, payload)
                st = client.get_state(entity_id)
                attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                friendly = (attrs.get("friendly_name") or entity_id)

                spoken_action = service.replace("_", " ")
                if spoken_action == "turn on":
                    spoken_action = "turned on"
                elif spoken_action == "turn off":
                    spoken_action = "turned off"

                extras_txt = ", ".join(extras_txt_parts) if extras_txt_parts else ""
                return await self._speak_response_confirm(query, friendly, spoken_action, extras_txt, llm_client)

            except Exception as e:
                logger.error(f"[ha_control] control error: {e}")
                return f"Error performing {service} on {entity_id}: {e}"

        if intent_type == "get_state" or action == "get_state":
            allowed = {"sensor", "binary_sensor", "lock", "cover", "light", "switch", "fan", "media_player", "climate", "remote", "select"}
            candidates = self._candidates_for_domains(catalog, allowed)
            if excluded:
                candidates = [c for c in candidates if (c.get("entity_id") or "").lower() not in excluded]

            entity_id = await self._choose_entity_llm(query, intent, candidates, llm_client)
            if not entity_id:
                return "I couldn’t find a device or sensor matching that."

            try:
                st = client.get_state(entity_id)
                val = st.get("state", "unknown") if isinstance(st, dict) else str(st)
                attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                friendly = (attrs.get("friendly_name") or entity_id)
                unit = (attrs.get("unit_of_measurement") or "")

                if (entity_id or "").startswith("remote.") and isinstance(attrs, dict):
                    current_activity = attrs.get("current_activity") or attrs.get("activity")
                    if isinstance(current_activity, str) and current_activity.strip():
                        val = current_activity.strip()

                return await self._speak_response_state(query, friendly, str(val), str(unit), llm_client)
            except Exception as e:
                logger.error(f"[ha_control] get_state error: {e}")
                return f"Error reading {entity_id}: {e}"

        return "I couldn't understand that request."


plugin = HAControlPlugin()