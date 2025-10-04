# plugins/ha_control_plugin.py
import logging
import requests
from plugin_base import ToolPlugin
from helpers import redis_client
import json as _json

logger = logging.getLogger("ha_control")
logger.setLevel(logging.INFO)


class HAClient:
    """Simple Home Assistant REST API helper (settings from Redis)."""

    def __init__(self):
        settings = redis_client.hgetall("plugin_settings:Home Assistant")
        self.base_url = settings.get("HA_BASE_URL", "http://homeassistant.local:8123").rstrip("/")
        self.token = settings.get("HA_TOKEN")
        if not self.token:
            raise ValueError("Home Assistant token (HA_TOKEN) not set in plugin settings.")

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _req(self, method: str, path: str, json=None, timeout=10):
        url = f"{self.base_url}{path}"
        resp = requests.request(method, url, headers=self.headers, json=json, timeout=timeout)
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
        # Prefer JSON, fallback to text
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
        # Area Registry API
        return self._req("GET", "/api/areas") or []

    def resolve_area_id(self, area: str | None) -> str | None:
        """Accepts an area name (case-insensitive) or an area_id and returns a valid area_id."""
        if not area:
            return None
        areas = self.list_areas()
        # Exact id match?
        for a in areas:
            if a.get("area_id") == area:
                return area
        # Name match (case-insensitive)
        target = area.strip().lower()
        for a in areas:
            if (a.get("name") or "").strip().lower() == target:
                return a.get("area_id")
        return None


class HAControlPlugin(ToolPlugin):
    name = "ha_control"
    usage = (
        "{\n"
        '  "function": "ha_control",\n'
        '  "arguments": {\n'
        '    "action": "turn_on | turn_off | get_state",\n'
        '    "entity_id": "climate.downstairs",\n'
        '    "data": {"temperature": 72}\n'
        "  }\n"
        "}\n"
        "or area targeting:\n"
        "{\n"
        '  "function": "ha_control",\n'
        '  "arguments": {\n'
        '    "action": "turn_on | turn_off",\n'
        '    "domain": "light | switch | media_player",\n'
        '    "area": "Office"  // area name or area_id\n'
        "  }\n"
        "}\n"
    )
    description = "Control Home Assistant devices via domain/service/entity. Examples: turn lights on, toggle switches, set temperatures."
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

    def _get_client(self):
        try:
            return HAClient()
        except Exception as e:
            logger.error(f"[ha_control] Failed to initialize HA client: {e}")
            return None

    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args)

    async def handle_webui(self, args, llm_client):
        return await self._handle(args)

    async def _handle(self, args):
        client = self._get_client()
        if not client:
            return "Home Assistant is not configured. Please set HA_BASE_URL and HA_TOKEN in the plugin settings."

        action = (args.get("action") or "").strip()
        entity_id = (args.get("entity_id") or "").strip()
        domain = (args.get("domain") or "").strip()
        area_arg = (args.get("area") or "").strip()  # area name or area_id
        data = args.get("data", {}) or {}

        if not action:
            return "Missing 'action'. Use turn_on, turn_off, or get_state."

        # ----- Query path -----
        if action == "get_state":
            if not entity_id:
                return "Please provide an 'entity_id' for get_state."
            try:
                state = client.get_state(entity_id)
                return f"{entity_id} is {state.get('state', 'unknown')}."
            except Exception as e:
                logger.error(f"[ha_control] get_state error: {e}")
                return f"Error reading {entity_id}: {e}"

        # ----- Control path (turn_on/turn_off) -----
        if action not in ("turn_on", "turn_off"):
            return f"Unsupported action: {action}"

        # CASE 1: Direct entity control
        if entity_id:
            try:
                domain_from_entity = entity_id.split(".")[0]
                client.call_service(domain_from_entity, action, {"entity_id": entity_id, **data})
                return f"OK, {action.replace('_', ' ')} {entity_id}."
            except Exception as e:
                logger.error(f"[ha_control] entity control error: {e}")
                return f"Error performing {action} on {entity_id}: {e}"

        # CASE 2: Area targeting (requires domain)
        if area_arg:
            if not domain:
                return "When using 'area', also provide a 'domain' (e.g., light, switch)."

            try:
                # Render a JSON array of all entities in area matching the domain
                # Example output: ["light.office_ceiling","light.office_lamp"]
                jinja = "{{ (area_entities(%r) | select('match', '^%s\\\\.') | list) | tojson }}" % (area_arg, domain)
                rendered = client.render_template(jinja)

                # Normalize to a Python list
                entities = []
                if isinstance(rendered, list):
                    entities = rendered
                elif isinstance(rendered, str):
                    rendered = rendered.strip()
                    if rendered.startswith("["):
                        # Looks like JSON array
                        try:
                            entities = _json.loads(rendered)
                        except Exception:
                            pass
                    if not entities and rendered:
                        # Fallback: comma/whitespace separated
                        entities = [e.strip() for e in rendered.split(",") if e.strip()]

                if not entities:
                    return f"I couldn’t find any {domain} entities in '{area_arg}'."

                payload = {"entity_id": entities}
                if isinstance(data, dict) and data:
                    payload.update(data)

                client.call_service(domain, action, payload)
                return f"OK, {action.replace('_', ' ')} {len(entities)} {domain} entity(ies) in {area_arg}."
            except Exception as e:
                logger.error(f"[ha_control] area control error: {e}")
                return f"Error performing {action} in area '{area_arg}': {e}"

        # If neither entity nor area provided
        return "Please provide either an 'entity_id' or an 'area' (with a 'domain')."


plugin = HAControlPlugin()