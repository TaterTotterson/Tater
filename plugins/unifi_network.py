# plugins/unifi_network.py
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client, get_tater_name, get_tater_personality

load_dotenv()
logger = logging.getLogger("unifi_network")
logger.setLevel(logging.INFO)


class UnifiNetworkPlugin(ToolPlugin):
    """
    UniFi Network (Official Integration API) plugin.

    Goals:
    - Fetch full data from UniFi (correct totals)
    - Precompute counts + highlights in Python (fast + accurate)
    - Send ONLY compact/top items to LLM by default (fast)
    - Still allow "list all" / "show all" style requests

    API:
      https://<console>/proxy/network/integration/v1/...
      Auth: X-API-KEY header
    """

    name = "unifi_network"
    plugin_name = "UniFi Network"
    pretty_name = "UniFi Network"
    description = "Answer questions about your UniFi Network (clients, devices, site status) using the official UniFi Integration API."
    plugin_dec = "Fetch UniFi Network sites/clients/devices via the official API and let the LLM answer questions from computed facts + compact lists."
    settings_category = "UniFi Network"
    platforms = ["webui", "homeassistant", "homekit", "xbmc"]

    usage = (
        "{\n"
        '  "function": "unifi_network",\n'
        '  "arguments": {\n'
        '    "query": "User request in natural language (e.g., \\"how\\\'s the network?\\", \\"who is online\\", \\"find john\\", \\"list devices\\")"\n'
        "  }\n"
        "}\n"
    )

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re checking the UniFi network now. "
        "Only output that message."
    )

    required_settings = {
        "UNIFI_BASE_URL": {
            "label": "UniFi Console Base URL",
            "type": "string",
            "default": "https://10.4.20.1",
            "description": "Base URL of your UniFi console (UDM/CloudKey), e.g. https://10.4.20.1",
        },
        "UNIFI_API_KEY": {
            "label": "UniFi API Key",
            "type": "string",
            "default": "",
            "description": "Create in UniFi Network → Settings → Control Plane → Integrations (API Key).",
        },
    }

    # ---- internal defaults (not exposed as settings) ----
    _DEFAULT_VERIFY_SSL = False
    _DEFAULT_TIMEOUT = 20

    # paging: we fetch ALL pages, but do it in chunks for reliability
    _PAGE_LIMIT = 200

    # LLM context caps (we still FETCH all; we just don't SEND all)
    _TOP_CLIENTS_DEFAULT = 25
    _TOP_DEVICES_DEFAULT = 25
    _TOP_MATCHES_DEFAULT = 10

    # -------------------------
    # Settings / HTTP helpers
    # -------------------------
    def _get_settings(self) -> Dict[str, str]:
        s = (
            redis_client.hgetall(f"plugin_settings:{self.pretty_name}") or
            redis_client.hgetall(f"plugin_settings:{self.settings_category}") or
            redis_client.hgetall(f"plugin_settings: {self.pretty_name}") or
            redis_client.hgetall(f"plugin_settings: {self.settings_category}") or
            {}
        )
        return s or {}

    def _base(self, s: Dict[str, str]) -> str:
        return (s.get("UNIFI_BASE_URL") or "https://10.4.20.1").rstrip("/")

    def _api_key(self, s: Dict[str, str]) -> str:
        key = (s.get("UNIFI_API_KEY") or "").strip()
        if not key:
            raise ValueError("UNIFI_API_KEY is missing in UniFi Network plugin settings.")
        return key

    def _headers(self, api_key: str) -> Dict[str, str]:
        return {"X-API-KEY": api_key, "Accept": "application/json"}

    def _request(self, method: str, url: str, *, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Any:
        verify = self._DEFAULT_VERIFY_SSL
        timeout = self._DEFAULT_TIMEOUT

        # Silence urllib3 "InsecureRequestWarning" when verify=False
        if not verify:
            try:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:
                pass

        r = requests.request(method, url, headers=headers, params=params, timeout=timeout, verify=verify)
        if r.status_code >= 400:
            snippet = (r.text or "")[:300]
            raise RuntimeError(f"UniFi HTTP {r.status_code} calling {url} params={params}: {snippet}")
        try:
            return r.json()
        except Exception:
            return r.text

    # -------------------------
    # UniFi Integration API calls
    # -------------------------
    def _integration_url(self, base: str, path: str) -> str:
        path = path if path.startswith("/") else f"/{path}"
        return f"{base}/proxy/network/integration{path}"

    def _get_sites(self, base: str, headers: Dict[str, str]) -> Dict[str, Any]:
        url = self._integration_url(base, "/v1/sites")
        return self._request("GET", url, headers=headers)

    def _pick_site(self, sites_payload: Dict[str, Any]) -> Tuple[str, str]:
        data = (sites_payload or {}).get("data") or []
        if not isinstance(data, list) or not data:
            raise RuntimeError("No UniFi sites returned from /v1/sites.")
        first = data[0] or {}
        site_id = (first.get("id") or "").strip()
        site_name = (first.get("name") or first.get("internalReference") or "Unknown").strip()
        if not site_id:
            raise RuntimeError("UniFi sites response missing site id.")
        return site_id, site_name or "Unknown"

    def _get_paged(self, *, base: str, headers: Dict[str, str], path: str) -> Dict[str, Any]:
        """
        Page through endpoints shaped like:
          {"offset":0,"limit":25,"count":25,"totalCount":66,"data":[...]}
        and return a combined payload with all data.
        """
        url = self._integration_url(base, path)

        all_items: List[Any] = []
        offset = 0
        total: Optional[int] = None
        max_pages = 2000  # safety guard

        for _ in range(max_pages):
            params = {"offset": str(offset), "limit": str(self._PAGE_LIMIT)}
            payload = self._request("GET", url, headers=headers, params=params)

            if not isinstance(payload, dict):
                raise RuntimeError(f"Unexpected response type from {url}: {type(payload)}")

            page_data = payload.get("data") or []
            if isinstance(page_data, list) and page_data:
                all_items.extend(page_data)

            if total is None:
                try:
                    total = int(payload.get("totalCount"))
                except Exception:
                    total = None

            try:
                count = int(payload.get("count"))
            except Exception:
                count = len(page_data) if isinstance(page_data, list) else 0

            # Stop conditions
            if total is not None and len(all_items) >= total:
                break
            if count < self._PAGE_LIMIT:
                break

            offset += self._PAGE_LIMIT

        return {
            "offset": 0,
            "limit": self._PAGE_LIMIT,
            "count": len(all_items),
            "totalCount": total if total is not None else len(all_items),
            "data": all_items,
        }

    def _get_clients_all(self, base: str, headers: Dict[str, str], site_id: str) -> Dict[str, Any]:
        return self._get_paged(base=base, headers=headers, path=f"/v1/sites/{site_id}/clients")

    def _get_devices_all(self, base: str, headers: Dict[str, str], site_id: str) -> Dict[str, Any]:
        return self._get_paged(base=base, headers=headers, path=f"/v1/sites/{site_id}/devices")

    # -------------------------
    # Big-list optimization helpers
    # -------------------------
    @staticmethod
    def _q_norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip().lower())

    def _query_wants_full_list(self, q: str) -> bool:
        """
        If the user explicitly asks to list/show everything, include the full compact list.
        Otherwise we keep LLM context small for speed.
        """
        t = self._q_norm(q)
        triggers = (
            "list all", "show all", "everything", "full list", "all clients", "all devices",
            "dump", "print all"
        )
        return any(x in t for x in triggers)

    def _query_kind(self, q: str) -> str:
        """
        Lightweight intent detection to:
          - answer some questions without LLM (fast)
          - decide how to build compact context
        """
        t = self._q_norm(q)

        # "find" style
        if any(w in t for w in ("find ", "search ", "look up", "lookup", "where is", "ip address", "mac address")):
            return "find"

        # list style
        if any(w in t for w in ("list clients", "show clients", "who is online", "who's online", "clients online")):
            return "clients"
        if any(w in t for w in ("list devices", "show devices", "devices", "aps", "access points", "switches", "udm", "ups")):
            return "devices"

        # quick health
        if any(w in t for w in ("health", "status", "how's the network", "hows the network", "network ok", "internet", "wan")):
            return "health"

        # fallback
        return "general"

    def _client_link_counts(self, clients_payload: Dict[str, Any]) -> Dict[str, int]:
        """
        Count wired/wireless using reliable client fields.
        Prefer explicit type values like 'WIRED' / 'WIRELESS'.
        """
        data = (clients_payload or {}).get("data") or []
        wired = 0
        wireless = 0
        unknown = 0

        for c in data:
            if not isinstance(c, dict):
                continue

            t = (c.get("type") or c.get("connectionType") or c.get("linkType") or "").strip().upper()

            if t == "WIRED":
                wired += 1
                continue
            if t == "WIRELESS":
                wireless += 1
                continue

            # fallbacks some builds use
            if c.get("isWireless") is True:
                wireless += 1
                continue
            if c.get("isWired") is True:
                wired += 1
                continue

            unknown += 1

        return {
            "wired": wired,
            "wireless": wireless,
            "unknown": unknown,
            "total_listed": wired + wireless + unknown,
        }

    def _device_status_counts(self, devices_payload: Dict[str, Any]) -> Dict[str, int]:
        """
        Try to count online/offline for devices. Different builds expose different fields.
        We'll do best-effort and keep 'unknown' if we can't tell.
        """
        data = (devices_payload or {}).get("data") or []
        online = 0
        offline = 0
        unknown = 0

        for d in data:
            if not isinstance(d, dict):
                continue

            # Common-ish fields (best effort)
            state = (d.get("state") or d.get("status") or "").strip().upper()
            if state in ("ONLINE", "CONNECTED", "ADOPTED"):
                online += 1
                continue
            if state in ("OFFLINE", "DISCONNECTED", "ISOLATED"):
                offline += 1
                continue

            if d.get("isOnline") is True or d.get("online") is True:
                online += 1
                continue
            if d.get("isOnline") is False or d.get("online") is False:
                offline += 1
                continue

            unknown += 1

        return {"online": online, "offline": offline, "unknown": unknown, "total_listed": online + offline + unknown}

    @staticmethod
    def _safe_str(v: Any) -> str:
        return "" if v is None else str(v)

    def _compact_client(self, c: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": c.get("name") or c.get("hostname") or c.get("displayName") or "",
            "type": c.get("type") or c.get("connectionType") or c.get("linkType") or "",
            "ip": c.get("ipAddress") or c.get("ip") or "",
            "mac": c.get("macAddress") or c.get("mac") or "",
            "connectedAt": c.get("connectedAt") or c.get("firstSeen") or c.get("lastSeen") or "",
            "uplinkDeviceId": c.get("uplinkDeviceId") or c.get("uplink") or "",
            "id": c.get("id") or "",
        }

    def _compact_device(self, d: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": d.get("name") or d.get("displayName") or "",
            "model": d.get("model") or d.get("type") or "",
            "state": d.get("state") or d.get("status") or "",
            "ip": d.get("ipAddress") or d.get("ip") or "",
            "mac": d.get("macAddress") or d.get("mac") or "",
            "version": d.get("version") or d.get("firmwareVersion") or "",
            "id": d.get("id") or "",
        }

    def _search_clients(self, clients_payload: Dict[str, Any], needle: str, max_hits: int) -> List[Dict[str, Any]]:
        n = self._q_norm(needle)
        if not n:
            return []

        hits: List[Tuple[int, Dict[str, Any]]] = []
        for c in (clients_payload or {}).get("data") or []:
            if not isinstance(c, dict):
                continue

            name = self._q_norm(self._safe_str(c.get("name") or c.get("hostname") or c.get("displayName")))
            ip = self._q_norm(self._safe_str(c.get("ipAddress") or c.get("ip")))
            mac = self._q_norm(self._safe_str(c.get("macAddress") or c.get("mac")))

            blob = f"{name} {ip} {mac}".strip()

            score = 0
            if n == name or n == ip or n == mac:
                score = 100
            elif n and n in blob:
                score = 50
                if name.startswith(n):
                    score += 10

            if score > 0:
                hits.append((score, c))

        hits.sort(key=lambda x: x[0], reverse=True)
        return [self._compact_client(c) for _, c in hits[:max_hits]]

    def _build_compact_context(
        self,
        *,
        query: str,
        site_name: str,
        sites: Dict[str, Any],
        clients: Dict[str, Any],
        devices: Dict[str, Any],
    ) -> Dict[str, Any]:
        wants_full = self._query_wants_full_list(query)
        kind = self._query_kind(query)

        client_counts = self._client_link_counts(clients)
        device_counts = self._device_status_counts(devices)

        all_clients = (clients or {}).get("data") or []
        all_devices = (devices or {}).get("data") or []

        compact_clients = [self._compact_client(c) for c in all_clients if isinstance(c, dict)]
        compact_devices = [self._compact_device(d) for d in all_devices if isinstance(d, dict)]

        top_clients = compact_clients[:self._TOP_CLIENTS_DEFAULT]
        top_devices = compact_devices[:self._TOP_DEVICES_DEFAULT]

        if wants_full:
            top_clients = compact_clients
            top_devices = compact_devices

        matches: List[Dict[str, Any]] = []
        if kind == "find":
            matches = self._search_clients(clients, query, self._TOP_MATCHES_DEFAULT)

        anomalies: List[str] = []
        if device_counts.get("offline", 0) > 0:
            anomalies.append(f"{device_counts['offline']} UniFi device(s) appear OFFLINE.")
        if (clients or {}).get("error"):
            anomalies.append(f"Clients fetch error: {(clients.get('error') or '')[:120]}")
        if (devices or {}).get("error"):
            anomalies.append(f"Devices fetch error: {(devices.get('error') or '')[:120]}")
        if client_counts.get("unknown", 0) > 0:
            anomalies.append(f"{client_counts['unknown']} client(s) did not report wired/wireless type.")

        facts = {
            "site": site_name,
            "kind": kind,
            "wants_full_list": wants_full,
            "counts": {
                "clients_total": int((clients or {}).get("totalCount") or len(compact_clients)),
                "clients_wired": client_counts["wired"],
                "clients_wireless": client_counts["wireless"],
                "clients_unknown_link": client_counts["unknown"],
                "devices_total": int((devices or {}).get("totalCount") or len(compact_devices)),
                "devices_online": device_counts["online"],
                "devices_offline": device_counts["offline"],
                "devices_unknown_status": device_counts["unknown"],
            },
            "anomalies": anomalies,
            "matches": matches,
            "top_clients": top_clients,
            "top_devices": top_devices,
            "note": (
                "Counts are computed from full fetched lists. "
                "top_clients/top_devices may be capped unless user asked for full lists."
            ),
        }

        return {"question": query, "facts": facts, "sites": sites}

    # -------------------------
    # LLM response
    # -------------------------
    @staticmethod
    def _strip_code_fences(s: str) -> str:
        s = (s or "").strip()
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.I)
        s = re.sub(r"\s*```$", "", s)
        return s.strip()

    def _assistant_identity(self) -> Tuple[str, str]:
        first, last = get_tater_name()
        assistant_name = f"{(first or '').strip()} {(last or '').strip()}".strip() or "Tater"
        personality = (get_tater_personality() or "").strip()
        return assistant_name, personality

    def _direct_answer_if_simple(self, query: str, compact_ctx: Dict[str, Any]) -> Optional[str]:
        q = self._q_norm(query)
        facts = (compact_ctx or {}).get("facts") or {}
        counts = facts.get("counts") or {}

        if any(x in q for x in ("how many clients", "number of clients", "client count")):
            return f"There are {counts.get('clients_total', 0)} clients in the UniFi client list right now."
        if any(x in q for x in ("how many wireless", "wireless clients")):
            return f"There are {counts.get('clients_wireless', 0)} wireless clients (and {counts.get('clients_wired', 0)} wired, {counts.get('clients_unknown_link', 0)} unknown)."
        if any(x in q for x in ("how many wired", "wired clients")):
            return f"There are {counts.get('clients_wired', 0)} wired clients (and {counts.get('clients_wireless', 0)} wireless, {counts.get('clients_unknown_link', 0)} unknown)."
        if any(x in q for x in ("any devices offline", "devices offline", "offline devices")) and "how" not in q:
            off = counts.get("devices_offline", 0)
            if off:
                return f"Yes — {off} UniFi device(s) appear offline."
            return "No — I don’t see any UniFi devices reported as offline."

        return None

    async def _llm_answer(self, *, compact_ctx: Dict[str, Any], llm_client) -> str:
        assistant_name, personality = self._assistant_identity()

        system = (
            f"You are {assistant_name}, a helpful home network assistant.\n"
            "You will be given compact, precomputed UniFi facts and small top-lists.\n"
            "Answer the user's question using ONLY the provided data.\n"
            "Be concise and practical.\n"
        )

        if personality:
            system += (
                "\nPersonality / style guidelines:\n"
                f"{personality}\n"
            )

        system += (
            "\nIMPORTANT RULES:\n"
            "- Do NOT guess counts. Use facts.counts.* exactly.\n"
            "- If you mention wired vs wireless, use facts.counts.clients_wired / clients_wireless / clients_unknown_link.\n"
            "- If the user asks to 'show all', you may present more items, but keep it readable.\n"
            "- If facts.matches is present and non-empty, prefer those results for 'find' queries.\n"
            "- If a detail is missing, say you can’t confirm it from the data.\n"
        )

        resp = await llm_client.chat(messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(compact_ctx, ensure_ascii=False)},
        ])
        content = (resp.get("message", {}) or {}).get("content", "").strip()
        content = self._strip_code_fences(content)
        return content or "I couldn’t find anything to report."

    # -------------------------
    # Platform entrypoints
    # -------------------------
    async def handle_webui(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    async def handle_homeassistant(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    # ✅ New: HomeKit + XBMC handlers (same core logic)
    async def handle_homekit(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    async def handle_xbmc(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    async def _handle(self, args: Dict[str, Any], llm_client):
        query = (args.get("query") or "").strip()
        if not query:
            return "Please provide a 'query' like: “how’s the network?” or “who is online?”"

        s = self._get_settings()
        try:
            base = self._base(s)
            api_key = self._api_key(s)
        except Exception as e:
            return f"UniFi Network is not configured: {e}"

        headers = self._headers(api_key)

        # Sites
        try:
            sites = self._get_sites(base, headers)
            site_id, site_name = self._pick_site(sites)
        except Exception as e:
            logger.exception("[unifi_network] sites fetch failed")
            return f"I couldn't reach UniFi to list sites: {e}"

        # Fetch ALL clients + ALL devices (paged)
        try:
            clients = self._get_clients_all(base, headers, site_id)
        except Exception as e:
            logger.exception("[unifi_network] clients fetch failed")
            clients = {"error": str(e), "data": [], "totalCount": 0, "count": 0, "offset": 0, "limit": self._PAGE_LIMIT}

        try:
            devices = self._get_devices_all(base, headers, site_id)
        except Exception as e:
            logger.exception("[unifi_network] devices fetch failed")
            devices = {"error": str(e), "data": [], "totalCount": 0, "count": 0, "offset": 0, "limit": self._PAGE_LIMIT}

        compact_ctx = self._build_compact_context(
            query=query,
            site_name=site_name,
            sites=sites,
            clients=clients,
            devices=devices,
        )

        direct = self._direct_answer_if_simple(query, compact_ctx)
        if direct:
            return direct

        try:
            return await self._llm_answer(compact_ctx=compact_ctx, llm_client=llm_client)
        except Exception as e:
            logger.exception("[unifi_network] llm answer failed")
            facts = (compact_ctx or {}).get("facts") or {}
            counts = facts.get("counts") or {}
            return (
                f"I pulled UniFi data for site “{site_name}”, but summarization failed.\n"
                f"Clients: {counts.get('clients_total', 0)}, Devices: {counts.get('devices_total', 0)}.\n"
                f"Error: {e}"
            )


plugin = UnifiNetworkPlugin()