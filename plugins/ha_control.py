# plugins/ha_control_plugin.py
import logging
import re
import json as _json
import requests
from plugin_base import ToolPlugin
from helpers import redis_client

logger = logging.getLogger("ha_control")
logger.setLevel(logging.INFO)


class HAClient:
    """Simple Home Assistant REST API helper (settings from Redis)."""

    def __init__(self):
        # Be tolerant of a stray space in the settings hash key.
        settings = (
            redis_client.hgetall("plugin_settings: Home Assistant")
            or redis_client.hgetall("plugin_settings:Home Assistant")
        )
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

    def list_areas(self):
        return self._req("GET", "/api/areas") or []

    def list_states(self):
        """Returns full state list for fuzzy matching (entity_id + attributes.friendly_name)."""
        return self._req("GET", "/api/states") or []


class HAControlPlugin(ToolPlugin):
    """
    One simple tool for Home Assistant control. The model calls it whenever the
    user wants to control or check devices.
    """
    name = "ha_control"
    usage = (
        "{\n"
        '  "function": "ha_control",\n'
        '  "arguments": {\n'
        '    "action": "turn_on | turn_off | open | close | set_temperature | get_state",\n'
        '    "target": "office lights",\n'
        '    "data": {"brightness_pct": 80}\n'
        "  }\n"
        "}\n"
    )
    description = "Call this when the user wants to control or check a Home Assistant device, such as turning lights on or off, setting temperatures, or checking a sensor state."
    pretty_name = "Home Assistant Control"
    settings_category = "Home Assistant"
    waiting_prompt_template = "Write a friendly message telling {mention} you’re controlling their Home Assistant devices now! Only output that message."
    platforms = ["homeassistant", "webui"]

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
    }

    # ----------------------------
    # Internal helpers
    # ----------------------------
    def _get_client(self):
        try:
            return HAClient()
        except Exception as e:
            logger.error(f"[ha_control] Failed to initialize HA client: {e}")
            return None

    @staticmethod
    def _infer_domain_hint(text: str, action: str | None = None) -> str | None:
        """Light heuristic to help shortlist candidates for 'object' resolution."""
        t = (text or "").lower()
        a = (action or "").lower()
        # Action-driven hints:
        if a == "set_temperature":
            return "climate"
        if a in ("open", "close"):
            # If they said 'lock' explicitly, prefer lock; otherwise assume cover.
            if "lock" in t:
                return "lock"
            return "cover"
        # Text-driven hints:
        if any(w in t for w in ("light", "lamp", "bulb")): return "light"
        if any(w in t for w in ("switch", "plug", "outlet")): return "switch"
        if any(w in t for w in ("speaker", "tv", "media", "cast")): return "media_player"
        if any(w in t for w in ("thermostat", "temperature", "heat", "cool", "ac", "climate")): return "climate"
        if "fan" in t: return "fan"
        if any(w in t for w in ("cover", "blind", "shade", "garage")): return "cover"
        if "lock" in t: return "lock"
        if "scene" in t: return "scene"
        if "script" in t: return "script"
        if any(w in t for w in ("sensor", "humidity", "feels like", "feels-like", "pressure", "rain", "uv")): return "sensor"
        return None

    async def _classify_target(self, target: str, llm_client):
        """
        Decide whether `target` refers to:
          - an area+domain     -> {"area": "<Area Name>", "domain":"<domain>"}
          - a single object    -> {"object":"<free-form name>","domain":"<optional domain hint>"}
        Output must be strict JSON with exactly one of those shapes.
        """
        target = (target or "").strip()
        if not target:
            return {}

        allowed_domains = "light, switch, media_player, fan, climate, cover, lock, scene, script, sensor"
        system = (
            "You classify smart-home targets for Home Assistant control. "
            "Return strict JSON in ONE of these forms:\n"
            '  {"area":"<Area Name>","domain":"<one of: ' + allowed_domains + '>"}\n'
            '  {"object":"<single device or sensor name>","domain":"<optional domain hint>"}\n'
            "Do NOT include explanations."
        )
        user = f'Target: "{target}"'

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user}
                ]
            )
            content = (resp.get("message", {}) or {}).get("content", "").strip()
            content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
            data = _json.loads(content)
            if isinstance(data, dict):
                if "area" in data and "domain" in data and isinstance(data["area"], str) and isinstance(data["domain"], str):
                    return {"area": data["area"].strip(), "domain": data["domain"].strip()}
                if "object" in data and isinstance(data["object"], str):
                    dom = data.get("domain")
                    return {"object": data["object"].strip(), **({"domain": dom.strip()} if isinstance(dom, str) else {})}
        except Exception as e:
            logger.warning(f"[ha_control] target classification failed, falling back: {e}")

        # Last resort heuristic: phrases that include 'light(s)' → area+light
        if "light" in target.lower():
            return {"area": target.replace("lights", "").replace("light", "").strip().title(), "domain": "light"}

        # Otherwise treat as an object with a domain guess
        hint = self._infer_domain_hint(target)
        return {"object": target, **({"domain": hint} if hint else {})}

    def _shortlist_entities(self, states: list, phrase: str, domain_hint: str | None, limit: int = 12):
        """Quick heuristic shortlist before asking LLM to pick the single best entity."""
        phrase_l = (phrase or "").lower()
        tokens = {t for t in re.split(r"[\s_\-]+", phrase_l) if t}
        candidates = []

        for s in states:
            if not isinstance(s, dict):
                continue
            eid = s.get("entity_id", "")
            if "." not in eid:
                continue
            dom, _ = eid.split(".", 1)
            if domain_hint and dom != domain_hint:
                continue

            name = ""
            attrs = s.get("attributes") or {}
            if isinstance(attrs, dict):
                name = (attrs.get("friendly_name") or "").lower()

            text_blob = f"{eid.lower()} {name}"
            score = 0
            if phrase_l and phrase_l in text_blob:
                score += 3
            overlap = len(tokens & set(re.split(r"[\s_\-\.]+", text_blob)))
            score += min(overlap, 3)
            score += max(0, 2 - int(len(text_blob) / 30))

            if score > 0:
                candidates.append({
                    "entity_id": eid,
                    "name": attrs.get("friendly_name") or eid,
                    "domain": dom,
                    "score": score
                })

        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:limit] if candidates else []

    async def _pick_entity_with_llm(self, phrase: str, shortlist: list, llm_client):
        """Ask LLM to choose the single best entity_id from a small candidate set."""
        if not shortlist:
            return None
        mini = [{"entity_id": c["entity_id"], "name": c["name"]} for c in shortlist]
        system = (
            "Pick the SINGLE best Home Assistant entity that matches the user's phrase. "
            "Return strict JSON: {\"entity_id\":\"...\"} with no explanation."
        )
        user = _json.dumps({"phrase": phrase, "candidates": mini}, ensure_ascii=False)
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user}
                ]
            )
            content = (resp.get("message", {}) or {}).get("content", "").strip()
            content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
            data = _json.loads(content)
            eid = data.get("entity_id")
            if isinstance(eid, str) and eid:
                return eid
        except Exception as e:
            logger.warning(f"[ha_control] LLM entity pick failed: {e}")
        return shortlist[0]["entity_id"]

    def _extract_temperature(self, data: dict) -> float | None:
        """Accept common keys for temperature setpoint."""
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
        """
        Map high-level actions to HA service names for a given entity domain.
        Returns (service, extra_payload) or None if unsupported.
        """
        a = action.lower()
        d = entity_domain.lower()

        if a in ("turn_on", "turn_off"):
            return a, {}

        if a == "open":
            if d == "cover":
                return "open_cover", {}
            if d == "lock":
                return "unlock", {}
            # fallback: try 'open' if domain supports a same-named service
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
    # Platform handlers
    # ----------------------------
    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_webui(self, args, llm_client):
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

        if not action:
            return "Missing 'action'. Use turn_on, turn_off, open, close, set_temperature, or get_state."
        if not target:
            return "Please provide a 'target' (e.g., 'office lights' or 'thermostat set to 72')."

        # 1) Ask LLM: area+domain OR object(+domain hint)
        info = await self._classify_target(target, llm_client)

        # 2) get_state → must resolve to a single entity
        if action == "get_state":
            domain_hint = info.get("domain") or self._infer_domain_hint(target, action)
            try:
                states = client.list_states()
            except Exception as e:
                logger.error(f"[ha_control] list_states failed: {e}")
                return "I couldn't access Home Assistant states."

            phrase = info.get("object") or target
            shortlist = self._shortlist_entities(states, phrase, domain_hint)
            if not shortlist:
                return "I couldn’t find a device or sensor matching that."
            entity_id = await self._pick_entity_with_llm(phrase, shortlist, llm_client)
            try:
                state = client.get_state(entity_id)
                val = state.get("state", "unknown") if isinstance(state, dict) else str(state)

                # Try to get friendly name
                friendly = entity_id
                try:
                    friendly = (state.get("attributes", {}) or {}).get("friendly_name") or entity_id
                except Exception:
                    pass

                # Ask LLM for a natural spoken confirmation
                system_msg = (
                    f"The user asked for the current value of {friendly}, which is {val}. "
                    "Respond naturally like a smart home assistant, in one short sentence. "
                    "Do not include emojis or technical details."
                )
                try:
                    resp = await llm_client.chat(
                        messages=[
                            {"role": "system", "content": system_msg},
                            {"role": "user", "content": "Generate that short spoken response now."}
                        ]
                    )
                    msg = (resp.get('message', {}) or {}).get('content', '').strip()
                except Exception:
                    msg = ""

                if not msg:
                    msg = f"The {friendly} is currently {val}."
                return msg

            except Exception as e:
                logger.error(f"[ha_control] get_state error: {e}")
                return f"Error reading {entity_id}: {e}"

        # 3) Control path
        if action not in ("turn_on", "turn_off", "open", "close", "set_temperature"):
            return f"Unsupported action: {action}"

        # AREA lane
        if "area" in info and "domain" in info:
            area_arg = info["area"]
            domain = info["domain"].lower()

            if action in ("open", "close") and domain != "cover":
                return f"'{action}' is only supported for area control when domain is 'cover'."

            if action == "set_temperature":
                return "Setting temperature requires a specific thermostat/device, not an area."

            try:
                # Resolve entities for the area+domain
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

                # Map action to service
                service = action
                extra = {}
                if action in ("open", "close"):
                    service = "open_cover" if action == "open" else "close_cover"

                payload = {"entity_id": entities}
                payload.update(extra)
                if isinstance(data, dict) and data:
                    payload.update(data)

                client.call_service(domain, service, payload)

                # Friendly LLM confirmation (no emojis)
                domain_label = domain + ("s" if not domain.endswith("s") else "")
                verb_phrase = service.replace("_cover", "").replace("_", " ") if domain == "cover" else action.replace("_", " ")
                system_msg = (
                    f"The user asked to {verb_phrase} the {domain_label} in the {area_arg} area. "
                    "Write a short, natural confirmation sentence as if spoken by a smart home assistant. "
                    "Do not include emojis, entity counts, or technical language."
                )
                try:
                    resp = await llm_client.chat(
                        messages=[
                            {"role": "system", "content": system_msg},
                            {"role": "user", "content": "Generate that confirmation now."}
                        ]
                    )
                    msg = (resp.get('message', {}) or {}).get('content', '').strip()
                except Exception:
                    msg = ""
                if not msg:
                    msg = f"Okay, {verb_phrase} the {domain_label} in {area_arg}."
                return msg

            except Exception as e:
                logger.error(f"[ha_control] area control error: {e}")
                return f"Error performing {action} in area '{area_arg}': {e}"

        # OBJECT lane
        phrase = info.get("object") or target
        domain_hint = info.get("domain") or self._infer_domain_hint(target, action)

        try:
            states = client.list_states()
        except Exception as e:
            logger.error(f"[ha_control] list_states failed: {e}")
            return "I couldn't access Home Assistant states."

        shortlist = self._shortlist_entities(states, phrase, domain_hint)
        if not shortlist:
            return "I couldn’t find a device matching that."

        entity_id = await self._pick_entity_with_llm(phrase, shortlist, llm_client)
        entity_domain = entity_id.split(".", 1)[0].lower()

        # Map action to service
        mapped = self._service_for_action(action, entity_domain)
        if not mapped:
            return f"The action '{action}' is not supported for {entity_domain}."
        service, extra = mapped

        # Special handling for set_temperature
        if action == "set_temperature":
            temperature = self._extract_temperature(data)
            if temperature is None:
                return "Please provide a temperature (e.g., data.temperature=72)."
            payload = {"entity_id": entity_id, "temperature": temperature}
            for k in ("hvac_mode", "target_temp_high", "target_temp_low"):
                if k in data:
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
                f"The user asked to {nice_action} for {friendly}. "
                "Write a short, natural confirmation as if spoken by a smart home assistant. "
                "Do not include emojis or technical details."
            )
            try:
                resp = await llm_client.chat(
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": "Generate that confirmation now."}
                    ]
                )
                msg = (resp.get('message', {}) or {}).get('content', '').strip()
            except Exception:
                msg = ""
            if not msg:
                msg = f"Okay, {nice_action} {friendly}."
            return msg

        except Exception as e:
            logger.error(f"[ha_control] entity control error: {e}")
            return f"Error performing {service} on {entity_id}: {e}"

plugin = HAControlPlugin()