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
        "inside temperatures, outside temperatures, room temperatures, and sensors."
    )
    plugin_dec = "Control or check Home Assistant devices like lights, thermostats, and sensors."

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
    # Catalog (grounding)
    # ----------------------------
    def _catalog_cache_key(self) -> str:
        return "ha_control:catalog:v4"

    def _build_catalog_from_states(self, states: List[dict]) -> List[dict]:
        """
        Build a compact catalog that the LLM can choose from.
        NOTE: we intentionally do NOT rely on 'state' to decide temperature sensors, etc.
        """
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

    # ----------------------------
    # Step 1: LLM interprets query → intent
    # ----------------------------
    async def _interpret_query(self, query: str, llm_client) -> dict:
        """
        Turn the user's raw query into:
          - intent: get_temp | get_state | control | set_temperature
          - action: turn_on|turn_off|open|close|get_state|set_temperature
          - scope: inside|outside|area:<name>|device:<phrase>|unknown
          - domain_hint: light|switch|climate|sensor|cover|lock|fan|media_player|scene|script|binary_sensor
          - desired: {temperature,color_name,brightness_pct,hvac_mode}
        """
        allowed_domain = "light,switch,climate,sensor,binary_sensor,cover,lock,fan,media_player,scene,script,select"
        system = (
            "You are interpreting a smart-home request for Home Assistant.\n"
            "Return STRICT JSON only. No explanation.\n"
            "Schema:\n"
            "{\n"
            '  "intent": "get_temp|get_state|control|set_temperature",\n'
            '  "action": "turn_on|turn_off|open|close|get_state|set_temperature",\n'
            '  "scope": "inside|outside|area:<name>|device:<phrase>|unknown",\n'
            f'  "domain_hint": "one of: {allowed_domain}",\n'
            '  "desired": {\n'
            '     "temperature": <number or null>,\n'
            '     "brightness_pct": <int 0-100 or null>,\n'
            '     "color_name": <string or null>\n'
            "  }\n"
            "}\n"
            "Rules:\n"
            "- If user asks 'what's the temp inside' or 'temp in the kitchen', intent=get_temp, action=get_state.\n"
            "- If user asks 'thermostat set to' or 'thermostat temp', intent=get_state, domain_hint=climate.\n"
            "- If user says 'set thermostat to 74', intent=set_temperature, action=set_temperature, domain_hint=climate.\n"
            "- For lights, domain_hint=light and action turn_on/turn_off accordingly.\n"
            "- If user says 'set lights to blue' / 'turn lights blue', that's lights (domain_hint=light), NOT thermostat.\n"
            "- ✅ If user asks to set lights to a percent (brightness), you MUST use intent=control and action=turn_on,\n"
            "  and put the percent into desired.brightness_pct. Do NOT use actions like set_brightness.\n"
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

    def _domains_for_control(self, domain_hint: str) -> Set[str]:
        dh = (domain_hint or "").lower().strip()
        if dh:
            return {dh}
        return {"light", "switch", "fan", "media_player", "scene", "script", "cover", "lock"}

    # ----------------------------
    # Step 3: LLM chooser (grounded) + tournament chunking
    # ----------------------------
    async def _choose_entity_llm(self, query: str, intent: dict, candidates: List[dict], llm_client) -> Optional[str]:
        if not candidates:
            return None

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

        system = (
            "Pick the SINGLE best Home Assistant entity for this user request.\n"
            "You MUST choose an entity_id from the provided candidates (no inventions).\n"
            "Return strict JSON only: {\"entity_id\":\"...\"}. No explanation.\n"
            "Use the user's exact words to match rooms/devices.\n"
            "If the request is temperature inside, do NOT pick obvious outside/outdoor sensors.\n"
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

        if len(mini) <= max_single:
            try:
                eid = await ask_pick(mini)
                if eid:
                    return eid
            except Exception as e:
                logger.warning(f"[ha_control] LLM choose failed single-shot: {e}")

        try:
            winners: List[dict] = []
            for i in range(0, len(mini), chunk_size):
                chunk = mini[i:i + chunk_size]
                eid = await ask_pick(chunk)
                if eid:
                    winners.append(next(c for c in chunk if c["entity_id"] == eid))

            if not winners:
                return next(iter(candidate_set), None)

            eid = await ask_pick(winners)
            if eid:
                return eid

            return winners[0]["entity_id"]
        except Exception as e:
            logger.warning(f"[ha_control] LLM choose failed tournament: {e}")
            return next(iter(candidate_set), None)

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
            domains = self._domains_for_control(domain_hint)
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

            extras_txt_parts = []
            if entity_domain == "light" and action in ("turn_on", "turn_off"):
                if desired.get("color_name"):
                    payload["color_name"] = str(desired["color_name"])
                    extras_txt_parts.append(f"color {payload['color_name']}")
                if desired.get("brightness_pct") is not None:
                    try:
                        payload["brightness_pct"] = int(desired["brightness_pct"])
                        extras_txt_parts.append(f"brightness {payload['brightness_pct']} percent")
                    except Exception:
                        pass

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
            allowed = {"sensor", "binary_sensor", "lock", "cover", "light", "switch", "fan", "media_player", "climate"}
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
                return await self._speak_response_state(query, friendly, str(val), str(unit), llm_client)
            except Exception as e:
                logger.error(f"[ha_control] get_state error: {e}")
                return f"Error reading {entity_id}: {e}"

        return "I couldn't understand that request."


plugin = HAControlPlugin()