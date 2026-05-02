from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

import requests

from helpers import redis_client

ALADDIN_SETTINGS_KEY = "aladdin_settings"
ALADDIN_DEFAULT_API_BASE_URL = "https://api.smartgarage.systems"
ALADDIN_DEFAULT_AUTH_HOST = "cognito-idp.us-east-2.amazonaws.com"
ALADDIN_DEFAULT_CLIENT_ID = "27iic8c3bvslqngl3hso83t74b"
ALADDIN_DEFAULT_CLIENT_SECRET = "7bokto0ep96055k42fnrmuth84k7jdcjablestb7j53o8lp63v5"
ALADDIN_DEFAULT_TIMEOUT_SECONDS = 5

INTEGRATION = {
    "id": "aladdin",
    "name": "Aladdin Connect",
    "description": "Direct Genie/Aladdin garage door login used by Tater garage door actions.",
    "badge": "ALD",
    "order": 30,
    "fields": [
        {
            "key": "aladdin_username",
            "label": "Username / Email",
            "type": "text",
            "default": "",
        },
        {
            "key": "aladdin_password",
            "label": "Password",
            "type": "password",
            "default": "",
        },
        {
            "key": "aladdin_timeout_seconds",
            "label": "Timeout Seconds",
            "type": "number",
            "default": ALADDIN_DEFAULT_TIMEOUT_SECONDS,
            "min": 2,
            "max": 120,
        },
    ],
    "actions": [
        {
            "id": "test",
            "label": "Test Aladdin",
            "status": "Checks the direct Aladdin Connect login and lists doors.",
        },
    ],
}

ALADDIN_DOOR_STATUS = {
    0: "unknown",
    1: "open",
    2: "opening",
    3: "timeout_opening",
    4: "closed",
    5: "closing",
    6: "timeout_closing",
    7: "not_configured",
}
ALADDIN_DOOR_LINK_STATUS = {
    0: "Unknown",
    1: "NotConfigured",
    2: "Paired",
    3: "Connected",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(_text(value)))
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def normalize_aladdin_api_base(value: Any) -> str:
    text = _text(value) or ALADDIN_DEFAULT_API_BASE_URL
    if "://" not in text:
        text = f"https://{text}"
    parsed = urlparse(text)
    netloc = parsed.netloc or parsed.path
    if not netloc:
        return ALADDIN_DEFAULT_API_BASE_URL
    return urlunparse((parsed.scheme or "https", netloc, "", "", "", "")).rstrip("/")


def _legacy_settings(client: Any = None) -> Dict[str, str]:
    store = client or redis_client
    merged: Dict[str, str] = {}
    for key in (
        "verba_settings:Aladdin Connect",
        "verba_settings: Aladdin Connect",
    ):
        try:
            raw = store.hgetall(key) or {}
        except Exception:
            continue
        for field, value in raw.items():
            if _text(value) or field not in merged:
                merged[str(field)] = _text(value)
    return merged


def read_aladdin_settings(client: Any = None) -> Dict[str, str]:
    store = client or redis_client
    try:
        shared = store.hgetall(ALADDIN_SETTINGS_KEY) or {}
    except Exception:
        shared = {}
    legacy = _legacy_settings(store)

    def pick(*keys: str) -> str:
        for source in (shared, legacy):
            for key in keys:
                value = _text(source.get(key))
                if value:
                    return value
        return ""

    timeout = _bounded_int(
        pick("ALADDIN_TIMEOUT_SECONDS", "TIMEOUT_SECONDS"),
        default=ALADDIN_DEFAULT_TIMEOUT_SECONDS,
        minimum=2,
        maximum=120,
    )
    return {
        "ALADDIN_USERNAME": pick("ALADDIN_USERNAME", "ALADDIN_EMAIL", "USERNAME", "EMAIL"),
        "ALADDIN_PASSWORD": pick("ALADDIN_PASSWORD", "PASSWORD"),
        "ALADDIN_API_BASE_URL": normalize_aladdin_api_base(
            pick("ALADDIN_API_BASE_URL", "API_BASE_URL") or ALADDIN_DEFAULT_API_BASE_URL
        ),
        "ALADDIN_TIMEOUT_SECONDS": str(timeout),
    }


def save_aladdin_settings(
    *,
    username: Any = None,
    password: Any = None,
    api_base_url: Any = None,
    timeout_seconds: Any = None,
    client: Any = None,
) -> Dict[str, str]:
    store = client or redis_client
    current = read_aladdin_settings(store)
    next_settings = {
        "ALADDIN_USERNAME": _text(current.get("ALADDIN_USERNAME") if username is None else username),
        "ALADDIN_PASSWORD": _text(current.get("ALADDIN_PASSWORD") if password is None else password),
        "ALADDIN_API_BASE_URL": normalize_aladdin_api_base(
            current.get("ALADDIN_API_BASE_URL") if api_base_url is None else api_base_url
        ),
        "ALADDIN_TIMEOUT_SECONDS": str(
            _bounded_int(
                current.get("ALADDIN_TIMEOUT_SECONDS") if timeout_seconds is None else timeout_seconds,
                default=ALADDIN_DEFAULT_TIMEOUT_SECONDS,
                minimum=2,
                maximum=120,
            )
        ),
    }
    store.hset(ALADDIN_SETTINGS_KEY, mapping=next_settings)
    return read_aladdin_settings(store)


class AladdinConnectClient:
    def __init__(
        self,
        *,
        username: Any = None,
        password: Any = None,
        api_base_url: Any = None,
        timeout_seconds: Any = None,
        session: Optional[requests.Session] = None,
    ):
        settings = read_aladdin_settings()
        self.username = _text(username if username is not None else settings.get("ALADDIN_USERNAME"))
        self.password = _text(password if password is not None else settings.get("ALADDIN_PASSWORD"))
        self.api_base_url = normalize_aladdin_api_base(
            api_base_url if api_base_url is not None else settings.get("ALADDIN_API_BASE_URL")
        )
        self.timeout = _bounded_int(
            timeout_seconds if timeout_seconds is not None else settings.get("ALADDIN_TIMEOUT_SECONDS"),
            default=ALADDIN_DEFAULT_TIMEOUT_SECONDS,
            minimum=2,
            maximum=120,
        )
        self.session = session or requests.Session()
        self.access_token = ""
        if not self.username or not self.password:
            raise ValueError("Aladdin Connect username and password are missing in Tater Settings > Integrations.")

    def _secret_hash(self) -> str:
        message = f"{self.username}{ALADDIN_DEFAULT_CLIENT_ID}".encode("utf-8")
        digest = hmac.new(
            ALADDIN_DEFAULT_CLIENT_SECRET.encode("utf-8"),
            msg=message,
            digestmod=hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _error_message(self, response: requests.Response, prefix: str) -> str:
        detail = _text(response.text)
        try:
            payload = response.json()
            if isinstance(payload, dict):
                detail = _text(
                    payload.get("message")
                    or payload.get("error_description")
                    or payload.get("error")
                    or payload.get("__type")
                    or detail
                )
        except Exception:
            pass
        return f"{prefix}: HTTP {response.status_code}{(': ' + detail) if detail else ''}"

    def login(self) -> str:
        payload = {
            "AuthFlow": "USER_PASSWORD_AUTH",
            "AuthParameters": {
                "USERNAME": self.username,
                "PASSWORD": self.password,
                "SECRET_HASH": self._secret_hash(),
            },
            "ClientId": ALADDIN_DEFAULT_CLIENT_ID,
        }
        response = self.session.post(
            f"https://{ALADDIN_DEFAULT_AUTH_HOST}",
            headers={
                "Content-Type": "application/x-amz-json-1.1",
                "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
            },
            json=payload,
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise RuntimeError(self._error_message(response, "Aladdin Connect login failed"))
        data = response.json()
        auth = data.get("AuthenticationResult") if isinstance(data, dict) else {}
        token = _text((auth or {}).get("AccessToken"))
        if not token:
            raise RuntimeError("Aladdin Connect login failed: no access token returned.")
        self.access_token = token
        return token

    def _request(self, method: str, path: str, *, json_body: Optional[dict] = None) -> Any:
        if not self.access_token:
            self.login()

        response = self.session.request(
            method,
            f"{self.api_base_url}{path}",
            headers={"Authorization": f"Bearer {self.access_token}"},
            json=json_body,
            timeout=self.timeout,
        )
        if response.status_code in {401, 403}:
            self.login()
            response = self.session.request(
                method,
                f"{self.api_base_url}{path}",
                headers={"Authorization": f"Bearer {self.access_token}"},
                json=json_body,
                timeout=self.timeout,
            )
        if response.status_code >= 400:
            raise RuntimeError(self._error_message(response, f"Aladdin Connect API call failed for {path}"))
        if not _text(response.text):
            return None
        try:
            return response.json()
        except Exception:
            return response.text

    def list_doors(self) -> List[Dict[str, Any]]:
        data = self._request("GET", "/devices")
        rows: List[Dict[str, Any]] = []
        devices = data.get("devices") if isinstance(data, dict) else []
        for device in devices if isinstance(devices, list) else []:
            if not isinstance(device, dict):
                continue
            device_id = device.get("id")
            serial = _text(device.get("serial_number") or device.get("serial"))
            ownership = _text(device.get("ownership"))
            for door in device.get("doors", []) if isinstance(device.get("doors"), list) else []:
                if not isinstance(door, dict):
                    continue
                status_code = door.get("status", 0)
                link_code = door.get("link_status", 0)
                rows.append(
                    {
                        "device_id": device_id,
                        "door_id": _text(door.get("id")),
                        "door_number": door.get("door_index"),
                        "name": _text(door.get("name")) or f"Garage Door {door.get('door_index')}",
                        "status": ALADDIN_DOOR_STATUS.get(status_code, "unknown"),
                        "status_code": status_code,
                        "link_status": ALADDIN_DOOR_LINK_STATUS.get(link_code, "Unknown"),
                        "link_status_code": link_code,
                        "battery_level": door.get("battery_level", 0),
                        "rssi": device.get("rssi", 0),
                        "serial": serial,
                        "vendor": _text(device.get("vendor")),
                        "model": _text(device.get("model")),
                        "ownership": ownership,
                        "fault": bool(door.get("fault")),
                        "ble_strength": door.get("ble_strength", 0),
                    }
                )
        return rows

    def command_door(self, device_id: Any, door_number: Any, action: str) -> bool:
        normalized = _text(action).lower()
        if normalized not in {"open", "close"}:
            raise ValueError(f"Unsupported Aladdin Connect door action: {action}")
        command = "OPEN_DOOR" if normalized == "open" else "CLOSE_DOOR"
        try:
            self._request("POST", f"/command/devices/{device_id}/doors/{door_number}", json_body={"command": command})
        except RuntimeError as exc:
            text = str(exc).lower()
            if f"already {normalized}" in text:
                return True
            raise
        return True


def test_aladdin_connection(
    *,
    username: Any = None,
    password: Any = None,
    api_base_url: Any = None,
    timeout_seconds: Any = None,
) -> Dict[str, Any]:
    client = AladdinConnectClient(
        username=username,
        password=password,
        api_base_url=api_base_url,
        timeout_seconds=timeout_seconds,
    )
    doors = client.list_doors()
    return {"ok": True, "door_count": len(doors), "doors": doors}


def integration_devices() -> Dict[str, Any]:
    settings = read_aladdin_settings()
    if not _text(settings.get("ALADDIN_USERNAME")) or not _text(settings.get("ALADDIN_PASSWORD")):
        return {"devices": [], "message": "Aladdin Connect is not configured."}
    client = AladdinConnectClient(
        username=settings.get("ALADDIN_USERNAME"),
        password=settings.get("ALADDIN_PASSWORD"),
        api_base_url=settings.get("ALADDIN_API_BASE_URL"),
        timeout_seconds=settings.get("ALADDIN_TIMEOUT_SECONDS"),
    )
    doors = client.list_doors()
    devices: List[Dict[str, Any]] = []
    for door in doors:
        if not isinstance(door, dict):
            continue
        door_id = _text(door.get("door_id")) or f"{door.get('device_id')}:{door.get('door_number')}"
        devices.append(
            {
                "id": door_id,
                "name": _text(door.get("name")) or "Garage Door",
                "type": "garage_door",
                "status": _text(door.get("status")),
                "state": _text(door.get("status")),
                "details": {
                    "device_id": door.get("device_id"),
                    "door_number": door.get("door_number"),
                    "link_status": door.get("link_status"),
                    "battery_level": door.get("battery_level"),
                    "rssi": door.get("rssi"),
                    "model": door.get("model"),
                    "serial": door.get("serial"),
                    "fault": door.get("fault"),
                },
            }
        )
    return {"devices": devices, "message": f"Aladdin Connect returned {len(devices)} doors."}


def read_integration_settings() -> Dict[str, Any]:
    settings = read_aladdin_settings()
    return {
        "aladdin_username": settings.get("ALADDIN_USERNAME", ""),
        "aladdin_password": settings.get("ALADDIN_PASSWORD", ""),
        "aladdin_timeout_seconds": int(settings.get("ALADDIN_TIMEOUT_SECONDS") or ALADDIN_DEFAULT_TIMEOUT_SECONDS),
    }


def save_integration_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    saved = save_aladdin_settings(
        username=(payload or {}).get("aladdin_username"),
        password=(payload or {}).get("aladdin_password"),
        timeout_seconds=(payload or {}).get("aladdin_timeout_seconds"),
    )
    return {
        "aladdin_username": saved.get("ALADDIN_USERNAME", ""),
        "aladdin_password": saved.get("ALADDIN_PASSWORD", ""),
        "aladdin_timeout_seconds": int(saved.get("ALADDIN_TIMEOUT_SECONDS") or ALADDIN_DEFAULT_TIMEOUT_SECONDS),
    }


def run_integration_action(action_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if _text(action_id) != "test":
        raise KeyError(f"Unsupported Aladdin Connect action: {action_id}")
    result = test_aladdin_connection(
        username=(payload or {}).get("aladdin_username"),
        password=(payload or {}).get("aladdin_password"),
        timeout_seconds=(payload or {}).get("aladdin_timeout_seconds"),
    )
    count = int(result.get("door_count") or 0)
    result["message"] = f"Aladdin Connect login worked. Found {count} door{'s' if count != 1 else ''}."
    return result
