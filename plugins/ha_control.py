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
    name = "ha_control"
    usage = (
        "{\n"
        '  "function": "ha_control",\n'
        '  "arguments": {\n'
        '    "action": "turn_on | turn_off | open | close | set_temperature | get_state",\n'
        '    "target": "office lights | game room lights | thermostat",\n'
        '    "data": {\n'
        '      "brightness_pct": 80,\n'
        '      "color_name": "blue",\n'
        '      "temperature": 72\n'
        '    }\n'
        "  }\n"
        "}\n"
    )
    description = "Call this when the user wants to control or check a Home Assistant device, such as turning lights on or off, setting temperatures, or checking a sensor state."
    plugin_dec = "Control or check Home Assistant devices like lights, thermostats, and sensors."
    pretty_name = "Home Assistant Control"
    settings_category = "Home Assistant"
    waiting_prompt_template = "Write a friendly message telling {mention} you’re controlling their Home Assistant devices now! Only output that message."
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
    }

    # ----------------------------
    # Internal helpers
    # ----------------------------
    def _normalize_action_and_data(self, action: str, target: str, data: dict) -> tuple[str, dict]:
        """
        Map common color/brightness phrases to HA-friendly forms.
        - Turn 'set_color', 'set colour', 'change-color', etc. -> 'turn_on'
        - Map 'color'/'colour' -> 'color_name'
        - Parse hs/rgb strings -> lists
        - Infer simple color from target text if missing
        """
        a = (action or "").strip().lower()
        d = dict(data or {})
        t = (target or "").lower()

        # --- Action synonyms -> supported actions ---
        # normalize separators
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
        else:
            a = a  # leave as-is (turn_on/turn_off/open/close/set_temperature okay)

        # --- Key normalization for HA lights ---
        for k in ("color", "colour"):
            if k in d and isinstance(d[k], str) and d[k].strip():
                d["color_name"] = d.pop(k).strip().lower()

        # hs/rgb strings -> lists
        if isinstance(d.get("hs_color"), str):
            parts = [p.strip() for p in d["hs_color"].split(",")]
            if len(parts) == 2:
                try:
                    d["hs_color"] = [float(parts[0]), float(parts[1])]
                except:
                    d.pop("hs_color", None)

        if isinstance(d.get("rgb_color"), str):
            parts = [p.strip() for p in d["rgb_color"].split(",")]
            if len(parts) == 3:
                try:
                    d["rgb_color"] = [int(parts[0]), int(parts[1]), int(parts[2])]
                except:
                    d.pop("rgb_color", None)

        # brightness aliases
        if isinstance(d.get("brightness"), (int, float)) and 0 <= d["brightness"] <= 100:
            d["brightness_pct"] = int(d.pop("brightness"))
        if "brightness_percent" in d and "brightness_pct" not in d:
            d["brightness_pct"] = int(d.pop("brightness_percent"))

        # Infer simple color from the phrase if none provided
        if not d.get("color_name"):
            m = re.search(
                r"\b(red|orange|yellow|green|cyan|blue|purple|magenta|pink|white|warm white|cool white)\b",
                t, re.I
            )
            if m:
                d["color_name"] = m.group(1).lower()

        # Final safety: if user clearly asked for a color (via data or text),
        # and action still isn't supported, coerce to 'turn_on' for lights.
        if a not in ("turn_on", "turn_off", "open", "close", "set_temperature"):
            if d.get("color_name") or d.get("hs_color") or d.get("rgb_color"):
                a = "turn_on"

        return a, d

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
        # Helpful for debugging
        logger.debug(f"[ha_control] excluded voice PE entities: {excluded}")
        return excluded

    @staticmethod
    def _infer_domain_hint(text: str, action: str | None = None) -> str | None:
        t = (text or "").lower()
        a = (action or "").lower()
        if a == "set_temperature": return "climate"
        if a in ("open", "close"):
            if "lock" in t: return "lock"
            return "cover"
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
        target = (target or "").strip()
        if not target:
            return {}

        allowed = "light, switch, media_player, fan, climate, cover, lock, scene, script, sensor"
        system = (
            "You classify smart-home targets for Home Assistant control. "
            "Return strict JSON in ONE of these forms:\n"
            f'  {{"area":"<Area Name>","domain":"<one of: {allowed}>"}}, or\n'
            '  {"object":"<single device or sensor name>","domain":"<optional domain hint>"}\n'
            "Do NOT include explanations."
        )
        user = f'Target: \"{target}\"'

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

        if "light" in target.lower():
            return {"area": target.replace("lights", "").replace("light", "").strip().title(), "domain": "light"}

        hint = self._infer_domain_hint(target)
        return {"object": target, **({"domain": hint} if hint else {})}

    def _shortlist_entities(self, states: list, phrase: str, domain_hint: str | None, limit: int = 12):
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
            if d == "cover": return "open_cover", {}
            if d == "lock":  return "unlock", {}
            return "open", {}
        if a == "close":
            if d == "cover": return "close_cover", {}
            if d == "lock":  return "lock", {}
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
            return "Please provide a 'target' (e.g., 'office lights' or 'thermostat set to 72')."

        # Ask LLM: area+domain OR object(+domain hint)
        info = await self._classify_target(target, llm_client)

        # get_state → must resolve to a single entity
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

                friendly = entity_id
                try:
                    friendly = (state.get("attributes", {}) or {}).get("friendly_name") or entity_id
                except Exception:
                    pass

                system_msg = (
                    f"The user said: '{target}', and you checked the current state of {friendly}, "
                    f"which is {val}. "
                    "Write one short, friendly spoken-style response as if you were a smart home assistant. "
                    "Use natural phrasing that fits everyday conversation, for example: "
                    "'The living room temperature is 72 degrees right now.' or "
                    "'The front door is locked.' "
                    "Include units or room names naturally if they are part of the device name or context. "
                    "Do not include emojis or technical language."
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

        excluded = self._excluded_entities_set()

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

                # Exclude configured Voice PE entities for LIGHT domain
                if domain == "light" and excluded:
                    entities = [e for e in entities if e.lower() not in excluded]
                    if not entities:
                        return f"Nothing to control — all lights in {area_arg} are excluded."

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

                # Friendly confirmation
                domain_label = domain + ("s" if not domain.endswith("s") else "")
                verb_phrase = service.replace("_cover", "").replace("_", " ") if domain == "cover" else action.replace("_", " ")
                system_msg = (
                    f"The user said: '{target}', and you successfully performed the action '{verb_phrase}' "
                    f"on {len(entities)} {domain_label} in the {area_arg} area. "
                    f"The color requested (if any) was '{payload.get('color_name', '')}'. "
                    "Write one short, friendly confirmation sentence as if spoken by a smart home assistant. "
                    "Include the number of lights controlled and the area name naturally. "
                    "If a color was requested, include it naturally in your response. "
                    "Do not include emojis or technical language."
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

        # If we're doing light on/off, filter out excluded Voice PE lights from candidates
        if action in ("turn_on", "turn_off") and shortlist:
            shortlist = [c for c in shortlist if c["entity_id"].split(".",1)[0].lower() != "light" or c["entity_id"].lower() not in excluded]

        if not shortlist:
            return "I couldn’t find a device matching that."

        # Pick the first non-excluded entity (extra safety)
        chosen_id = None
        # Try LLM choice, but if it's excluded, fall back to next
        candidate_id = await self._pick_entity_with_llm(phrase, shortlist, llm_client)
        if candidate_id and (candidate_id.lower() not in excluded or candidate_id.split(".",1)[0].lower() != "light"):
            chosen_id = candidate_id
        else:
            for c in shortlist:
                eid = c["entity_id"]
                if eid.split(".",1)[0].lower() != "light" or eid.lower() not in excluded:
                    chosen_id = eid
                    break

        if not chosen_id:
            return "The matching light is excluded from control."

        entity_id = chosen_id
        entity_domain = entity_id.split(".", 1)[0].lower()

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
                f"The user said: '{target}', and you successfully performed the action '{nice_action}' "
                f"for {friendly}. "
                "Write one short, friendly confirmation sentence as if spoken by a smart home assistant. "
                "If the user's phrase or data included a color or location, include that naturally "
                "(for example: 'I turned the lamp blue for you in the living room.'). "
                "Do not include emojis or technical language."
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
