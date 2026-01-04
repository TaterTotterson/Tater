# plugins/ha_control_plugin.py
import logging
import re
import json as _json
import time
import requests
from typing import Any, Dict, List, Optional

from plugin_base import ToolPlugin
from helpers import redis_client

logger = logging.getLogger("ha_control")
logger.setLevel(logging.INFO)


class HAClient:
    """Simple Home Assistant REST API helper (settings from Redis)."""

    def __init__(self):
        # IMPORTANT: WebUI saves to "plugin_settings:{category}" (no space after colon)
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
        """Returns full state list for grounding."""
        return self._req("GET", "/api/states") or []


class HAControlPlugin(ToolPlugin):
    name = "ha_control"
    plugin_name = "Home Assistant Control"
    usage = (
        "{\n"
        '  "function": "ha_control",\n'
        '  "arguments": {\n'
        '    "action": "turn_on | turn_off | open | close | set_temperature | get_state",\n'
        '    "target": "office lights | game room lights | thermostat | temp outside",\n'
        '    "data": {\n'
        '      "brightness_pct": 80,\n'
        '      "color_name": "blue",\n'
        '      "temperature": 72\n'
        '    }\n'
        "  }\n"
        "}\n"
    )
    description = (
        "Call this when the user wants to control or check a Home Assistant device, "
        "such as turning lights on or off, setting temperatures, or checking a sensor state."
    )
    plugin_dec = "Control or check Home Assistant devices like lights, thermostats, and sensors."
    pretty_name = "Home Assistant Control"

    # ✅ This must exactly match what you see in the WebUI settings header
    # and the WebUI will save under: plugin_settings:Home Assistant Control
    settings_category = "Home Assistant Control"

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re controlling their Home Assistant devices now! "
        "Only output that message."
    )
    platforms = ["homeassistant", "webui", "xbmc", "homekit"]

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
            "description": "How long to cache the compact entity catalog in Redis (recommended: 30-120)."
        },
        "HA_MAX_CANDIDATES": {
            "label": "Max Candidates Sent to LLM",
            "type": "string",
            "default": "120",
            "description": "Upper bound for candidates passed to the LLM (recommended: 60-150)."
        },
    }

    # ----------------------------
    # Settings helpers (ONE category only)
    # ----------------------------
    def _get_plugin_settings(self) -> dict:
        return redis_client.hgetall("plugin_settings:Home Assistant Control") or {}

    def _get_int_setting(self, key: str, default: int) -> int:
        s = self._get_plugin_settings()
        raw = (s.get(key) or "").strip()
        try:
            return int(raw)
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
        from light control calls. Handles both UPPER and lower keys.
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

    def _normalize_action_and_data(self, action: str, target: str, data: dict) -> tuple[str, dict]:
        a = (action or "").strip().lower()
        d = dict(data or {})
        t = (target or "").lower()

        a_norm = a.replace("-", "_").replace(" ", "_")
        colorish = {"set_color", "change_color", "set_colour", "change_colour", "color", "colour"}
        power_on = {"switch_on", "power_on", "enable", "brighten", "dim"}
        power_off = {"switch_off", "power_off", "disable"}

        if a_norm in colorish:
            a = "turn_on"
        elif a_norm in power_on:
            a = "turn_on"
        elif a_norm in power_off:
            a = "turn_off"

        for k in ("color", "colour"):
            if k in d and isinstance(d[k], str) and d[k].strip():
                d["color_name"] = d.pop(k).strip().lower()

        if isinstance(d.get("hs_color"), str):
            parts = [p.strip() for p in d["hs_color"].split(",")]
            if len(parts) == 2:
                try:
                    d["hs_color"] = [float(parts[0]), float(parts[1])]
                except Exception:
                    d.pop("hs_color", None)

        if isinstance(d.get("rgb_color"), str):
            parts = [p.strip() for p in d["rgb_color"].split(",")]
            if len(parts) == 3:
                try:
                    d["rgb_color"] = [int(parts[0]), int(parts[1]), int(parts[2])]
                except Exception:
                    d.pop("rgb_color", None)

        if isinstance(d.get("brightness"), (int, float)) and 0 <= d["brightness"] <= 100:
            d["brightness_pct"] = int(d.pop("brightness"))
        if "brightness_percent" in d and "brightness_pct" not in d:
            try:
                d["brightness_pct"] = int(d.pop("brightness_percent"))
            except Exception:
                d.pop("brightness_percent", None)

        if not d.get("color_name"):
            m = re.search(
                r"\b(red|orange|yellow|green|cyan|blue|purple|magenta|pink|white|warm white|cool white)\b",
                t,
                re.I,
            )
            if m:
                d["color_name"] = m.group(1).lower()

        if a not in ("turn_on", "turn_off", "open", "close", "set_temperature", "get_state"):
            if d.get("color_name") or d.get("hs_color") or d.get("rgb_color"):
                a = "turn_on"

        return a, d

    def _extract_temperature(self, data: dict) -> float | None:
        if not isinstance(data, dict):
            return None
        for k in ("temperature", "temp", "setpoint", "degrees"):
            v = data.get(k)
            try:
                if v is None:
                    continue
                return float(v)
            except Exception:
                continue
        return None

    def _service_for_action(self, action: str, entity_domain: str) -> tuple[str, dict] | None:
        a = action.lower()
        d = entity_domain.lower()
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

    # ----------------------------
    # Catalog (grounding) + filtering
    # ----------------------------
    def _catalog_cache_key(self) -> str:
        return "ha_control:catalog:v2"

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

            name = attrs.get("friendly_name") or eid
            catalog.append({
                "entity_id": eid,
                "domain": dom,
                "name": name,
                "device_class": attrs.get("device_class"),
                "unit": attrs.get("unit_of_measurement"),
                "state_class": attrs.get("state_class"),
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

    def _intent_filters(self, phrase: str, action: str, data: dict) -> Dict[str, Any]:
        p = (phrase or "").lower()
        a = (action or "").lower()

        wants_temp = any(w in p for w in ("temp", "temperature"))
        wants_humidity = "humidity" in p
        wants_lights = "light" in p or "lights" in p or "lamp" in p

        allowed_domains: Optional[set] = None

        if a == "set_temperature":
            allowed_domains = {"climate"}
        elif a in ("open", "close"):
            if "lock" in p or "door" in p:
                allowed_domains = {"lock"}
            else:
                allowed_domains = {"cover", "lock"}
        elif a in ("turn_on", "turn_off"):
            if wants_lights:
                allowed_domains = {"light", "switch"}
            else:
                allowed_domains = {"light", "switch", "fan", "media_player", "scene", "script", "cover", "lock"}
        elif a == "get_state":
            if wants_temp or wants_humidity:
                allowed_domains = {"sensor", "binary_sensor", "climate"}
            else:
                allowed_domains = {"sensor", "binary_sensor", "climate", "light", "switch", "fan", "media_player", "cover", "lock"}

        return {
            "allowed_domains": allowed_domains,
            "wants_temp": wants_temp,
            "wants_humidity": wants_humidity,
            "wants_lights": wants_lights,
        }

    def _filter_catalog(self, catalog: List[dict], phrase: str, action: str, data: dict) -> List[dict]:
        max_candidates = self._get_int_setting("HA_MAX_CANDIDATES", 120)
        intent = self._intent_filters(phrase, action, data)

        allowed_domains = intent["allowed_domains"]
        p = (phrase or "").lower()
        tokens = [t for t in re.split(r"[\s_\-]+", p) if t]

        def token_hit(text: str) -> bool:
            if not tokens:
                return True
            tl = (text or "").lower()
            return any(t in tl for t in tokens)

        filtered = []
        for c in catalog:
            dom = (c.get("domain") or "").lower()
            if allowed_domains and dom not in allowed_domains:
                continue
            filtered.append(c)

        if action.lower() == "get_state":
            if intent["wants_temp"]:
                temp_sensors = [
                    c for c in filtered
                    if (c.get("domain") == "sensor" and (c.get("device_class") or "").lower() == "temperature")
                ]
                if len(temp_sensors) >= 3:
                    filtered = temp_sensors + [c for c in filtered if c.get("domain") == "climate"]

            if intent["wants_humidity"]:
                hum_sensors = [
                    c for c in filtered
                    if (c.get("domain") == "sensor" and (c.get("device_class") or "").lower() == "humidity")
                ]
                if len(hum_sensors) >= 3:
                    filtered = hum_sensors

        if len(filtered) > max_candidates:
            keyword_filtered = []
            for c in filtered:
                blob = f'{c.get("entity_id","")} {c.get("name","")}'
                if token_hit(blob):
                    keyword_filtered.append(c)
            if len(keyword_filtered) >= 10:
                filtered = keyword_filtered

        if len(filtered) > max_candidates:
            filtered = filtered[:max_candidates]

        return filtered

    # ----------------------------
    # LLM chooser (grounded)
    # ----------------------------
    async def _choose_entity_llm(self, phrase: str, action: str, candidates: List[dict], llm_client) -> Optional[str]:
        if not candidates:
            return None

        mini = []
        for c in candidates:
            mini.append({
                "entity_id": c.get("entity_id"),
                "domain": c.get("domain"),
                "name": c.get("name"),
                "device_class": c.get("device_class"),
                "unit": c.get("unit"),
            })

        system = (
            "You select the single best Home Assistant entity for the user's request.\n"
            "You MUST choose an entity_id from the provided candidates (no inventions).\n"
            "Selection rules:\n"
            "- For temperature readings (what's the temp, outside temp, current temperature), prefer domain 'sensor' with device_class 'temperature'.\n"
            "- For humidity readings, prefer sensor device_class 'humidity'.\n"
            "- For requests that mention 'lights', prefer domain 'light' over 'switch' when both exist.\n"
            "- For set_temperature, choose a 'climate' entity (thermostat).\n"
            "Return strict JSON only: {\"entity_id\":\"...\"}. No explanation."
        )

        user = _json.dumps({"phrase": phrase, "action": action, "candidates": mini}, ensure_ascii=False)
        candidate_set = {c.get("entity_id") for c in candidates if c.get("entity_id")}

        async def _call(sys_text: str) -> Optional[str]:
            resp = await llm_client.chat(messages=[
                {"role": "system", "content": sys_text},
                {"role": "user", "content": user},
            ])
            content = (resp.get("message", {}) or {}).get("content", "").strip()
            content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
            data = _json.loads(content)
            eid = data.get("entity_id")
            if isinstance(eid, str) and eid.strip():
                return eid.strip()
            return None

        try:
            eid = await _call(system)
            if eid in candidate_set:
                return eid
        except Exception as e:
            logger.warning(f"[ha_control] LLM choose failed (attempt 1): {e}")

        try:
            stronger = system + "\nIMPORTANT: entity_id must be exactly one of the candidates."
            eid = await _call(stronger)
            if eid in candidate_set:
                return eid
        except Exception as e:
            logger.warning(f"[ha_control] LLM choose failed (attempt 2): {e}")

        return next(iter(candidate_set), None)

    # ----------------------------
    # Target classifier (area vs entity)
    # ----------------------------
    async def _classify_target(self, target: str, action: str, llm_client) -> dict:
        target = (target or "").strip()
        if not target:
            return {}

        allowed = "light, switch, media_player, fan, climate, cover, lock, scene, script, sensor, binary_sensor"
        system = (
            "You classify a Home Assistant request into either area control or a single entity selection.\n"
            "Return strict JSON with ONE of these forms:\n"
            f'  {{"mode":"area","area":"<Area Name>","domain":"<one of: {allowed}>"}}, OR\n'
            f'  {{"mode":"entity","phrase":"<what to match>","domain_hint":"<optional domain from: {allowed}>"}}, OR\n'
            '  {"mode":"entity","phrase":"<what to match>"}\n'
            "Rules:\n"
            "- If the user says '<area> lights' or '<area> covers', prefer mode=area.\n"
            "- If it sounds like a named decoration/device (e.g. 'christmas lights', 'tree', 'blow ups'), prefer mode=entity.\n"
            "- If action is get_state for temperature/humidity, prefer mode=entity.\n"
            "No explanation."
        )

        user = _json.dumps({"target": target, "action": action}, ensure_ascii=False)

        try:
            resp = await llm_client.chat(messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ])
            content = (resp.get("message", {}) or {}).get("content", "").strip()
            content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
            data = _json.loads(content)
            if isinstance(data, dict):
                mode = (data.get("mode") or "").strip().lower()
                if mode == "area" and isinstance(data.get("area"), str) and isinstance(data.get("domain"), str):
                    return {"mode": "area", "area": data["area"].strip(), "domain": data["domain"].strip()}
                if mode == "entity" and isinstance(data.get("phrase"), str):
                    out = {"mode": "entity", "phrase": data["phrase"].strip()}
                    if isinstance(data.get("domain_hint"), str) and data["domain_hint"].strip():
                        out["domain_hint"] = data["domain_hint"].strip()
                    return out
        except Exception as e:
            logger.warning(f"[ha_control] target classification failed, falling back: {e}")

        tl = target.lower()
        if action in ("turn_on", "turn_off") and (" lights" in tl or tl.endswith(" lights") or " light" in tl):
            area_guess = tl.replace("lights", "").replace("light", "").strip().title()
            if area_guess:
                return {"mode": "area", "area": area_guess, "domain": "light"}
        return {"mode": "entity", "phrase": target}

    # ----------------------------
    # Improved confirmation prompt builder (AREA)
    # ----------------------------
    def _build_confirmation_prompt_area(
        self,
        user_target: str,
        domain: str,
        area_name: str,
        entity_count: int,
        payload: dict,
        service_used: str,
    ) -> str:
        action_spoken = service_used.replace("_", " ").strip()
        if action_spoken in ("turn on", "turn off"):
            action_spoken = "turned on" if "on" in action_spoken else "turned off"
        elif action_spoken.startswith("open"):
            action_spoken = "opened"
        elif action_spoken.startswith("close"):
            action_spoken = "closed"

        extras = []
        if isinstance(payload, dict):
            if payload.get("color_name"):
                extras.append(f"color {payload.get('color_name')}")
            if payload.get("brightness_pct") is not None:
                try:
                    extras.append(f"brightness {int(payload.get('brightness_pct'))} percent")
                except Exception:
                    pass

        extras_text = ""
        if extras:
            extras_text = " with " + " and ".join(extras[:2])

        domain_label = domain
        if not domain_label.endswith("s") and entity_count != 1:
            domain_label += "s"

        system_msg = (
            "You are a smart home voice assistant.\n"
            "Write exactly ONE short, natural confirmation sentence.\n"
            "Constraints:\n"
            "- Sound conversational and spoken aloud.\n"
            "- Mention the area name naturally.\n"
            "- Mention what was controlled (device type) and how many, if plural.\n"
            "- Include extra details only if provided (like color or brightness).\n"
            "- No emojis. No technical wording. No entity IDs. No quotes.\n\n"
            f"User request: {user_target}\n"
            f"Result: {action_spoken} {entity_count} {domain_label} in {area_name}{extras_text}.\n"
            "Now produce the single confirmation sentence."
        )
        return system_msg

    # ----------------------------
    # Platform handlers
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
    # Core handler
    # ----------------------------
    async def _handle(self, args, llm_client):
        client = self._get_client()
        if not client:
            return "Home Assistant is not configured. Please set HA_BASE_URL and HA_TOKEN in the plugin settings."

        action = (args.get("action") or "").strip()
        target = (args.get("target") or "").strip()
        data = args.get("data", {}) or {}
        action, data = self._normalize_action_and_data(action, target, data)

        if not action:
            return "Missing 'action'. Use turn_on, turn_off, open, close, set_temperature, or get_state."
        if not target:
            return "Please provide a 'target' (e.g., 'office lights' or 'temp outside')."

        excluded = self._excluded_entities_set()
        info = await self._classify_target(target, action, llm_client)

        # get_state (entity lane)
        if action == "get_state":
            phrase = (info.get("phrase") or target).strip()

            try:
                catalog = self._get_catalog_cached(client)
            except Exception as e:
                logger.error(f"[ha_control] catalog build failed: {e}")
                return "I couldn't access Home Assistant states."

            candidates = self._filter_catalog(catalog, phrase, action, data)

            dom_hint = (info.get("domain_hint") or "").strip().lower()
            if dom_hint:
                hinted = [c for c in candidates if (c.get("domain") or "").lower() == dom_hint]
                if hinted:
                    candidates = hinted

            if excluded:
                candidates = [c for c in candidates if (c.get("entity_id") or "").lower() not in excluded]

            entity_id = await self._choose_entity_llm(phrase, action, candidates, llm_client)
            if not entity_id:
                return "I couldn’t find a device or sensor matching that."

            try:
                st = client.get_state(entity_id)
                val = st.get("state", "unknown") if isinstance(st, dict) else str(st)

                friendly = entity_id
                try:
                    friendly = (st.get("attributes", {}) or {}).get("friendly_name") or entity_id
                except Exception:
                    pass

                system_msg = (
                    f"The user asked: {target}\n"
                    f"You checked: {friendly}\n"
                    f"Current value/state: {val}\n"
                    "Write exactly ONE short, natural spoken response.\n"
                    "If it's a measurement, include units naturally if obvious.\n"
                    "No emojis. No technical wording. No entity IDs."
                )

                msg = ""
                try:
                    resp = await llm_client.chat(messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": "Say it now."},
                    ])
                    msg = (resp.get("message", {}) or {}).get("content", "").strip()
                except Exception:
                    msg = ""

                if not msg:
                    msg = f"The {friendly} is currently {val}."
                return msg

            except Exception as e:
                logger.error(f"[ha_control] get_state error: {e}")
                return f"Error reading {entity_id}: {e}"

        # Control path
        if action not in ("turn_on", "turn_off", "open", "close", "set_temperature"):
            return f"Unsupported action: {action}"

        # AREA lane
        if info.get("mode") == "area" and info.get("area") and info.get("domain"):
            area_arg = info["area"]
            domain = info["domain"].lower()

            if action in ("open", "close") and domain != "cover":
                return f"'{action}' is only supported for area control when domain is 'cover'."
            if action == "set_temperature":
                return "Setting temperature requires a specific thermostat/device, not an area."

            try:
                jinja = "{{ (area_entities(%r) | select('match', '^%s\\\\.') | list) | tojson }}" % (area_arg, domain)
                rendered = client.render_template(jinja)

                entities = []
                if isinstance(rendered, list):
                    entities = rendered
                elif isinstance(rendered, str):
                    r = rendered.strip()
                    if r.startswith("["):
                        try:
                            entities = _json.loads(r)
                        except Exception:
                            entities = []
                    if not entities and r:
                        entities = [e.strip() for e in r.split(",") if e.strip()]

                if not entities:
                    return f"I couldn’t find any {domain} entities in '{area_arg}'."

                if domain == "light" and excluded:
                    entities = [e for e in entities if e.lower() not in excluded]
                    if not entities:
                        return f"Nothing to control — all lights in {area_arg} are excluded."

                service = action
                if action in ("open", "close"):
                    service = "open_cover" if action == "open" else "close_cover"

                payload = {"entity_id": entities}
                if isinstance(data, dict) and data:
                    payload.update(data)

                client.call_service(domain, service, payload)

                system_msg = self._build_confirmation_prompt_area(
                    user_target=target,
                    domain=domain,
                    area_name=area_arg,
                    entity_count=len(entities),
                    payload=payload,
                    service_used=service,
                )

                msg = ""
                try:
                    resp = await llm_client.chat(messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": "Say it now."},
                    ])
                    msg = (resp.get("message", {}) or {}).get("content", "").strip()
                except Exception:
                    msg = ""

                if not msg:
                    verb = service.replace("_", " ")
                    msg = f"Okay, {verb} the {domain} in {area_arg}."
                return msg

            except Exception as e:
                logger.error(f"[ha_control] area control error: {e}")
                return f"Error performing {action} in area '{area_arg}': {e}"

        # ENTITY lane
        phrase = (info.get("phrase") or target).strip()
        try:
            catalog = self._get_catalog_cached(client)
        except Exception as e:
            logger.error(f"[ha_control] catalog build failed: {e}")
            return "I couldn't access Home Assistant states."

        candidates = self._filter_catalog(catalog, phrase, action, data)

        dom_hint = (info.get("domain_hint") or "").strip().lower()
        if dom_hint:
            hinted = [c for c in candidates if (c.get("domain") or "").lower() == dom_hint]
            if hinted:
                candidates = hinted

        if excluded and action in ("turn_on", "turn_off"):
            candidates = [
                c for c in candidates
                if not ((c.get("domain") or "").lower() == "light" and (c.get("entity_id") or "").lower() in excluded)
            ]

        entity_id = await self._choose_entity_llm(phrase, action, candidates, llm_client)
        if not entity_id:
            return "I couldn’t find a device matching that."

        return await self._execute_on_entity_id(client, action, target, entity_id, data, llm_client, excluded)

    async def _execute_on_entity_id(
        self,
        client: HAClient,
        action: str,
        user_target: str,
        entity_id: str,
        data: dict,
        llm_client,
        excluded: set,
    ) -> str:
        if not entity_id or "." not in entity_id:
            return "I couldn’t find a valid Home Assistant entity to control."

        entity_domain = entity_id.split(".", 1)[0].lower()

        if entity_domain == "light" and entity_id.lower() in excluded and action in ("turn_on", "turn_off"):
            return "That light is excluded from control."

        mapped = self._service_for_action(action, entity_domain)
        if not mapped:
            return f"The action '{action}' is not supported for {entity_domain}."

        service, extra = mapped

        if action == "set_temperature":
            temperature = self._extract_temperature(data)
            if temperature is None:
                return "Please provide a temperature (e.g., data.temperature=72)."
            payload = {"entity_id": entity_id, "temperature": temperature}
            for k in ("hvac_mode", "target_temp_high", "target_temp_low"):
                if isinstance(data, dict) and k in data:
                    payload[k] = data[k]
        else:
            payload = {"entity_id": entity_id}
            payload.update(extra)
            if isinstance(data, dict) and data:
                payload.update(data)

        try:
            client.call_service(entity_domain, service, payload)

            try:
                st = client.get_state(entity_id)
                friendly = (st.get("attributes", {}) or {}).get("friendly_name") or entity_id
            except Exception:
                friendly = entity_id

            nice_action = (
                f"set to {payload.get('temperature')} degrees"
                if action == "set_temperature"
                else service.replace("_", " ")
            )

            system_msg = (
                "You are a smart home voice assistant.\n"
                "Write exactly ONE short, natural confirmation sentence.\n"
                "Constraints:\n"
                "- Sound conversational and spoken aloud.\n"
                "- Mention the device name naturally.\n"
                "- Include extra details only if provided (color/brightness/temperature).\n"
                "- No emojis. No technical wording. No entity IDs.\n\n"
                f"User request: {user_target}\n"
                f"Result: {nice_action} for {friendly}.\n"
                f"Extras: color_name={payload.get('color_name','')}, brightness_pct={payload.get('brightness_pct','')}\n"
                "Now produce the single confirmation sentence."
            )

            msg = ""
            try:
                resp = await llm_client.chat(messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Say it now."},
                ])
                msg = (resp.get("message", {}) or {}).get("content", "").strip()
            except Exception:
                msg = ""

            if not msg:
                msg = f"Okay, {nice_action} {friendly}."
            return msg

        except Exception as e:
            logger.error(f"[ha_control] entity control error: {e}")
            return f"Error performing {service} on {entity_id}: {e}"


plugin = HAControlPlugin()