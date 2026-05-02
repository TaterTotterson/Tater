from __future__ import annotations

import warnings
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from helpers import redis_client

UNIFI_NETWORK_BASE_URL_KEY = "tater:unifi_network:base_url"
UNIFI_NETWORK_API_KEY_KEY = "tater:unifi_network:api_key"
UNIFI_NETWORK_DEFAULT_BASE_URL = "https://10.4.20.1"
UNIFI_NETWORK_DEFAULT_VERIFY_SSL = False
UNIFI_NETWORK_DEFAULT_TIMEOUT = 20
UNIFI_NETWORK_PAGE_LIMIT = 200

INTEGRATION = {
    "id": "unifi_network",
    "name": "UniFi Network",
    "description": "UniFi Network API key for client and device inventory actions.",
    "badge": "NET",
    "order": 60,
    "fields": [
        {
            "key": "unifi_network_base_url",
            "label": "Console Base URL",
            "type": "text",
            "default": UNIFI_NETWORK_DEFAULT_BASE_URL,
            "placeholder": UNIFI_NETWORK_DEFAULT_BASE_URL,
        },
        {
            "key": "unifi_network_api_key",
            "label": "API Key",
            "type": "password",
            "default": "",
        },
    ],
    "actions": [
        {
            "id": "test",
            "label": "Test UniFi Network",
            "status": "Checks the Network integration API and reads the first site.",
        },
    ],
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def read_unifi_network_settings(client: Any = None) -> Dict[str, str]:
    store = client or redis_client
    base_url = _text(store.get(UNIFI_NETWORK_BASE_URL_KEY) or UNIFI_NETWORK_DEFAULT_BASE_URL).rstrip("/")
    api_key = _text(store.get(UNIFI_NETWORK_API_KEY_KEY))
    return {
        "UNIFI_BASE_URL": base_url or UNIFI_NETWORK_DEFAULT_BASE_URL,
        "UNIFI_API_KEY": api_key,
    }


def save_unifi_network_settings(
    *,
    base_url: Any = None,
    api_key: Any = None,
    client: Any = None,
) -> Dict[str, str]:
    store = client or redis_client
    current = read_unifi_network_settings(store)
    next_base = _text(current.get("UNIFI_BASE_URL") if base_url is None else base_url) or UNIFI_NETWORK_DEFAULT_BASE_URL
    next_api_key = _text(current.get("UNIFI_API_KEY") if api_key is None else api_key)
    store.set(UNIFI_NETWORK_BASE_URL_KEY, next_base.rstrip("/"))
    store.set(UNIFI_NETWORK_API_KEY_KEY, next_api_key)
    return read_unifi_network_settings(store)


def unifi_network_base(settings: Dict[str, str]) -> str:
    return _text((settings or {}).get("UNIFI_BASE_URL") or UNIFI_NETWORK_DEFAULT_BASE_URL).rstrip("/")


def unifi_network_api_key(settings: Dict[str, str]) -> str:
    key = _text((settings or {}).get("UNIFI_API_KEY"))
    if not key:
        raise ValueError("UNIFI API key is missing. Set it in WebUI Settings -> Integrations -> UniFi Network.")
    return key


def unifi_network_headers(api_key: str) -> Dict[str, str]:
    return {"X-API-KEY": api_key, "Accept": "application/json"}


def unifi_network_integration_url(base: str, path: str) -> str:
    url_path = path if _text(path).startswith("/") else f"/{path}"
    return f"{_text(base).rstrip('/')}/proxy/network/integration{url_path}"


def unifi_network_request(
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    timeout: int = UNIFI_NETWORK_DEFAULT_TIMEOUT,
    verify_ssl: bool = UNIFI_NETWORK_DEFAULT_VERIFY_SSL,
) -> Any:
    if not verify_ssl:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", InsecureRequestWarning)
            response = requests.request(method, url, headers=headers, params=params, timeout=timeout, verify=verify_ssl)
    else:
        response = requests.request(method, url, headers=headers, params=params, timeout=timeout, verify=verify_ssl)
    if response.status_code >= 400:
        snippet = (response.text or "")[:300]
        raise RuntimeError(f"UniFi HTTP {response.status_code} calling {url} params={params}: {snippet}")
    try:
        return response.json()
    except Exception:
        return response.text


def get_unifi_sites(base: str, headers: Dict[str, str]) -> Dict[str, Any]:
    return unifi_network_request("GET", unifi_network_integration_url(base, "/v1/sites"), headers=headers)


def pick_unifi_site(sites_payload: Dict[str, Any]) -> Tuple[str, str]:
    data = (sites_payload or {}).get("data") or []
    if not isinstance(data, list) or not data:
        raise RuntimeError("No UniFi sites returned from /v1/sites.")
    first = data[0] or {}
    site_id = _text(first.get("id"))
    site_name = _text(first.get("name") or first.get("internalReference") or "Unknown")
    if not site_id:
        raise RuntimeError("UniFi sites response missing site id.")
    return site_id, site_name or "Unknown"


def get_unifi_paged(
    *,
    base: str,
    headers: Dict[str, str],
    path: str,
    page_limit: int = UNIFI_NETWORK_PAGE_LIMIT,
) -> Dict[str, Any]:
    url = unifi_network_integration_url(base, path)
    all_items: List[Any] = []
    offset = 0
    total: Optional[int] = None
    max_pages = 2000

    for _ in range(max_pages):
        params = {"offset": str(offset), "limit": str(page_limit)}
        payload = unifi_network_request("GET", url, headers=headers, params=params)

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

        if total is not None and len(all_items) >= total:
            break
        if count < page_limit:
            break

        offset += page_limit

    return {
        "offset": 0,
        "limit": page_limit,
        "count": len(all_items),
        "totalCount": total if total is not None else len(all_items),
        "data": all_items,
    }


def get_unifi_clients_all(base: str, headers: Dict[str, str], site_id: str, *, page_limit: int = UNIFI_NETWORK_PAGE_LIMIT) -> Dict[str, Any]:
    return get_unifi_paged(base=base, headers=headers, path=f"/v1/sites/{site_id}/clients", page_limit=page_limit)


def get_unifi_devices_all(base: str, headers: Dict[str, str], site_id: str, *, page_limit: int = UNIFI_NETWORK_PAGE_LIMIT) -> Dict[str, Any]:
    return get_unifi_paged(base=base, headers=headers, path=f"/v1/sites/{site_id}/devices", page_limit=page_limit)


def _first_text(row: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(row.get(key))
        if value:
            return value
    return ""


def _network_status(row: Dict[str, Any]) -> str:
    for key in ("status", "state", "connectionState", "connection_state"):
        value = _text(row.get(key))
        if value:
            return value
    for key in ("isConnected", "is_connected", "connected", "online"):
        if key in row:
            return "online" if bool(row.get(key)) else "offline"
    return ""


def _details(row: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            out[key] = value
    return out


def integration_devices() -> Dict[str, Any]:
    settings = read_unifi_network_settings()
    api_key = _text(settings.get("UNIFI_API_KEY"))
    if not api_key:
        return {"devices": [], "message": "UniFi Network is not configured."}
    base = unifi_network_base(settings)
    headers = unifi_network_headers(api_key)
    sites = get_unifi_sites(base, headers)
    site_id, site_name = pick_unifi_site(sites)
    devices_payload = get_unifi_devices_all(base, headers, site_id)
    clients_payload = get_unifi_clients_all(base, headers, site_id)
    rows: List[Dict[str, Any]] = []

    for device in devices_payload.get("data") or []:
        if not isinstance(device, dict):
            continue
        device_id = _first_text(device, "id", "macAddress", "mac", "serial")
        name = _first_text(device, "name", "displayName", "model", "macAddress", "mac") or device_id
        rows.append(
            {
                "id": device_id or name,
                "name": name or "Network Device",
                "type": _first_text(device, "type", "model", "deviceType") or "network_device",
                "status": _network_status(device),
                "state": _network_status(device),
                "area": site_name,
                "details": _details(
                    device,
                    [
                        "ipAddress",
                        "ip",
                        "macAddress",
                        "mac",
                        "model",
                        "version",
                        "firmwareVersion",
                        "adopted",
                        "uptime",
                    ],
                ),
            }
        )

    for client in clients_payload.get("data") or []:
        if not isinstance(client, dict):
            continue
        client_id = _first_text(client, "id", "macAddress", "mac")
        name = _first_text(client, "name", "hostname", "displayName", "ipAddress", "ip", "macAddress", "mac") or client_id
        rows.append(
            {
                "id": client_id or name,
                "name": name or "Client",
                "type": "client",
                "status": _network_status(client),
                "state": _network_status(client),
                "area": site_name,
                "details": _details(
                    client,
                    [
                        "ipAddress",
                        "ip",
                        "macAddress",
                        "mac",
                        "network",
                        "wired",
                        "wifiExperience",
                        "signal",
                        "rxRate",
                        "txRate",
                        "uplinkDeviceName",
                        "lastSeen",
                    ],
                ),
            }
        )
    return {"devices": rows, "message": f"UniFi Network returned {len(rows)} devices and clients from {site_name}."}


def read_integration_settings() -> Dict[str, Any]:
    settings = read_unifi_network_settings()
    return {
        "unifi_network_base_url": settings.get("UNIFI_BASE_URL") or UNIFI_NETWORK_DEFAULT_BASE_URL,
        "unifi_network_api_key": settings.get("UNIFI_API_KEY", ""),
    }


def save_integration_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    saved = save_unifi_network_settings(
        base_url=(payload or {}).get("unifi_network_base_url"),
        api_key=(payload or {}).get("unifi_network_api_key"),
    )
    return {
        "unifi_network_base_url": saved.get("UNIFI_BASE_URL") or UNIFI_NETWORK_DEFAULT_BASE_URL,
        "unifi_network_api_key": saved.get("UNIFI_API_KEY", ""),
    }


def run_integration_action(action_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if _text(action_id) != "test":
        raise KeyError(f"Unsupported UniFi Network action: {action_id}")
    current = read_integration_settings()
    base = _text((payload or {}).get("unifi_network_base_url") or current.get("unifi_network_base_url"))
    api_key = _text((payload or {}).get("unifi_network_api_key") or current.get("unifi_network_api_key"))
    if not api_key:
        raise ValueError("UniFi Network API key is required.")
    headers = unifi_network_headers(api_key)
    sites = get_unifi_sites(base, headers)
    site_id, site_name = pick_unifi_site(sites)
    return {
        "ok": True,
        "site_id": site_id,
        "site_name": site_name,
        "message": f"UniFi Network connection worked. First site: {site_name}.",
    }
