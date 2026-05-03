from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import requests

from helpers import redis_client

WEATHER_API_SETTINGS_KEY = "weather_api_settings"
WEATHER_API_BASE_URL = "https://api.weatherapi.com/v1"
WEATHER_API_DEFAULT_LOCATION = "60614"
WEATHER_API_DEFAULT_DAYS = 3
WEATHER_API_DEFAULT_UNITS = "us"
WEATHER_API_DEFAULT_INCLUDE_AQI = True
WEATHER_API_DEFAULT_INCLUDE_POLLEN = True
WEATHER_API_DEFAULT_INCLUDE_ALERTS = True
WEATHER_API_DEFAULT_SHOW_HOURLY_PEEK = 6
WEATHER_API_DEFAULT_MAX_RESPONSE_CHARS = 650
WEATHER_API_DEFAULT_TIMEOUT_SECONDS = 12

INTEGRATION = {
    "id": "weather_api",
    "name": "WeatherAPI.com",
    "description": "WeatherAPI.com key and defaults used by weather forecast tools.",
    "badge": "WX",
    "order": 70,
    "fields": [
        {
            "key": "weatherapi_key",
            "label": "WeatherAPI.com API Key",
            "type": "password",
            "default": "",
        },
        {
            "key": "weatherapi_default_location",
            "label": "Default Location",
            "type": "text",
            "default": WEATHER_API_DEFAULT_LOCATION,
            "placeholder": "City, ZIP, or lat,lon",
        },
        {
            "key": "weatherapi_default_days",
            "label": "Default Forecast Days",
            "type": "number",
            "default": WEATHER_API_DEFAULT_DAYS,
            "min": 1,
            "max": 14,
        },
        {
            "key": "weatherapi_default_units",
            "label": "Default Units",
            "type": "select",
            "options": ["us", "metric"],
            "default": WEATHER_API_DEFAULT_UNITS,
        },
        {
            "key": "weatherapi_include_aqi",
            "label": "Include Air Quality",
            "type": "select",
            "options": ["true", "false"],
            "default": "true",
        },
        {
            "key": "weatherapi_include_pollen",
            "label": "Include Pollen",
            "type": "select",
            "options": ["true", "false"],
            "default": "true",
        },
        {
            "key": "weatherapi_include_alerts",
            "label": "Include Alerts",
            "type": "select",
            "options": ["true", "false"],
            "default": "true",
        },
        {
            "key": "weatherapi_show_hourly_peek",
            "label": "Show next N hours",
            "type": "number",
            "default": WEATHER_API_DEFAULT_SHOW_HOURLY_PEEK,
            "min": 0,
            "max": 48,
        },
        {
            "key": "weatherapi_max_response_chars",
            "label": "Max Response Characters",
            "type": "number",
            "default": WEATHER_API_DEFAULT_MAX_RESPONSE_CHARS,
            "min": 120,
            "max": 4000,
        },
        {
            "key": "weatherapi_timeout_seconds",
            "label": "HTTP Timeout Seconds",
            "type": "number",
            "default": WEATHER_API_DEFAULT_TIMEOUT_SECONDS,
            "min": 2,
            "max": 60,
        },
    ],
    "actions": [
        {
            "id": "test",
            "label": "Test WeatherAPI",
            "status": "Checks the WeatherAPI.com forecast endpoint using the default location.",
        },
    ],
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    token = _text(value).lower()
    if token in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(_text(value)))
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def _normalize_units(value: Any) -> str:
    token = _text(value).lower()
    return token if token in {"us", "metric"} else WEATHER_API_DEFAULT_UNITS


def _read_raw(client: Any = None) -> Dict[str, Any]:
    store = client or redis_client
    try:
        raw = store.hgetall(WEATHER_API_SETTINGS_KEY) or {}
    except Exception:
        raw = {}
    return raw if isinstance(raw, dict) else {}


def read_weatherapi_settings(client: Any = None) -> Dict[str, Any]:
    raw = _read_raw(client)
    return {
        "WEATHERAPI_KEY": _text(raw.get("WEATHERAPI_KEY")),
        "DEFAULT_LOCATION": _text(raw.get("DEFAULT_LOCATION")) or WEATHER_API_DEFAULT_LOCATION,
        "DEFAULT_DAYS": _bounded_int(
            raw.get("DEFAULT_DAYS"),
            default=WEATHER_API_DEFAULT_DAYS,
            minimum=1,
            maximum=14,
        ),
        "DEFAULT_UNITS": _normalize_units(raw.get("DEFAULT_UNITS")),
        "INCLUDE_AQI": _bool(raw.get("INCLUDE_AQI"), WEATHER_API_DEFAULT_INCLUDE_AQI),
        "INCLUDE_POLLEN": _bool(raw.get("INCLUDE_POLLEN"), WEATHER_API_DEFAULT_INCLUDE_POLLEN),
        "INCLUDE_ALERTS": _bool(raw.get("INCLUDE_ALERTS"), WEATHER_API_DEFAULT_INCLUDE_ALERTS),
        "SHOW_HOURLY_PEEK": _bounded_int(
            raw.get("SHOW_HOURLY_PEEK"),
            default=WEATHER_API_DEFAULT_SHOW_HOURLY_PEEK,
            minimum=0,
            maximum=48,
        ),
        "MAX_RESPONSE_CHARS": _bounded_int(
            raw.get("MAX_RESPONSE_CHARS"),
            default=WEATHER_API_DEFAULT_MAX_RESPONSE_CHARS,
            minimum=120,
            maximum=4000,
        ),
        "TIMEOUT_SECONDS": _bounded_int(
            raw.get("TIMEOUT_SECONDS"),
            default=WEATHER_API_DEFAULT_TIMEOUT_SECONDS,
            minimum=2,
            maximum=60,
        ),
    }


def save_weatherapi_settings(
    *,
    api_key: Any = None,
    default_location: Any = None,
    default_days: Any = None,
    default_units: Any = None,
    include_aqi: Any = None,
    include_pollen: Any = None,
    include_alerts: Any = None,
    show_hourly_peek: Any = None,
    max_response_chars: Any = None,
    timeout_seconds: Any = None,
    client: Any = None,
) -> Dict[str, Any]:
    store = client or redis_client
    current = read_weatherapi_settings(store)
    next_settings = {
        "WEATHERAPI_KEY": _text(current.get("WEATHERAPI_KEY") if api_key is None else api_key),
        "DEFAULT_LOCATION": _text(
            current.get("DEFAULT_LOCATION") if default_location is None else default_location
        )
        or WEATHER_API_DEFAULT_LOCATION,
        "DEFAULT_DAYS": str(
            _bounded_int(
                current.get("DEFAULT_DAYS") if default_days is None else default_days,
                default=WEATHER_API_DEFAULT_DAYS,
                minimum=1,
                maximum=14,
            )
        ),
        "DEFAULT_UNITS": _normalize_units(current.get("DEFAULT_UNITS") if default_units is None else default_units),
        "INCLUDE_AQI": "true" if _bool(current.get("INCLUDE_AQI") if include_aqi is None else include_aqi, True) else "false",
        "INCLUDE_POLLEN": "true"
        if _bool(current.get("INCLUDE_POLLEN") if include_pollen is None else include_pollen, True)
        else "false",
        "INCLUDE_ALERTS": "true"
        if _bool(current.get("INCLUDE_ALERTS") if include_alerts is None else include_alerts, True)
        else "false",
        "SHOW_HOURLY_PEEK": str(
            _bounded_int(
                current.get("SHOW_HOURLY_PEEK") if show_hourly_peek is None else show_hourly_peek,
                default=WEATHER_API_DEFAULT_SHOW_HOURLY_PEEK,
                minimum=0,
                maximum=48,
            )
        ),
        "MAX_RESPONSE_CHARS": str(
            _bounded_int(
                current.get("MAX_RESPONSE_CHARS") if max_response_chars is None else max_response_chars,
                default=WEATHER_API_DEFAULT_MAX_RESPONSE_CHARS,
                minimum=120,
                maximum=4000,
            )
        ),
        "TIMEOUT_SECONDS": str(
            _bounded_int(
                current.get("TIMEOUT_SECONDS") if timeout_seconds is None else timeout_seconds,
                default=WEATHER_API_DEFAULT_TIMEOUT_SECONDS,
                minimum=2,
                maximum=60,
            )
        ),
    }
    store.hset(WEATHER_API_SETTINGS_KEY, mapping=next_settings)
    return read_weatherapi_settings(store)


def weatherapi_configured(client: Any = None) -> bool:
    return bool(_text(read_weatherapi_settings(client).get("WEATHERAPI_KEY")))


def weatherapi_request(
    endpoint: str,
    params: Dict[str, Any],
    *,
    api_key: Any = None,
    timeout_seconds: Optional[int] = None,
    client: Any = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    settings = read_weatherapi_settings(client)
    request_api_key = _text(settings.get("WEATHERAPI_KEY") if api_key is None else api_key)
    if not request_api_key:
        return None, "WeatherAPI is not configured. Set the API key in Settings > Integrations > WeatherAPI.com."

    endpoint_name = _text(endpoint)
    if not endpoint_name:
        return None, "WeatherAPI endpoint is required."
    url = f"{WEATHER_API_BASE_URL}/{endpoint_name.lstrip('/')}"
    request_params = dict(params or {})
    request_params["key"] = request_api_key
    timeout = _bounded_int(
        timeout_seconds if timeout_seconds is not None else settings.get("TIMEOUT_SECONDS"),
        default=WEATHER_API_DEFAULT_TIMEOUT_SECONDS,
        minimum=2,
        maximum=60,
    )
    headers = {"User-Agent": "TaterTotterson/WeatherAPIIntegration"}
    try:
        response = requests.get(url, params=request_params, headers=headers, timeout=timeout)
        if response.status_code >= 400:
            try:
                payload = response.json()
                message = (payload.get("error") or {}).get("message") if isinstance(payload, dict) else ""
                message = _text(message) or _text(response.text)
            except Exception:
                message = _text(response.text)
            return None, f"WeatherAPI error (HTTP {response.status_code}): {message}"
        payload = response.json()
    except Exception as exc:
        return None, f"Weather request failed: {exc}"

    return payload if isinstance(payload, dict) else {"data": payload}, None


def fetch_weatherapi_forecast(
    *,
    location: Any,
    days: Any = None,
    api_key: Any = None,
    include_aqi: Optional[bool] = None,
    include_pollen: Optional[bool] = None,
    include_alerts: Optional[bool] = None,
    timeout_seconds: Optional[int] = None,
    client: Any = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    settings = read_weatherapi_settings(client)
    q = _text(location)
    if not q:
        return None, "WeatherAPI location is required."
    forecast_days = _bounded_int(
        days if days is not None else settings.get("DEFAULT_DAYS"),
        default=WEATHER_API_DEFAULT_DAYS,
        minimum=1,
        maximum=14,
    )
    params = {
        "q": q,
        "days": forecast_days,
        "aqi": "yes" if _bool(include_aqi, bool(settings.get("INCLUDE_AQI"))) else "no",
        "alerts": "yes" if _bool(include_alerts, bool(settings.get("INCLUDE_ALERTS"))) else "no",
        "pollen": "yes" if _bool(include_pollen, bool(settings.get("INCLUDE_POLLEN"))) else "no",
    }
    return weatherapi_request(
        "forecast.json",
        params,
        api_key=api_key,
        timeout_seconds=timeout_seconds,
        client=client,
    )


def integration_status() -> Dict[str, Any]:
    settings = read_weatherapi_settings()
    configured = bool(_text(settings.get("WEATHERAPI_KEY")))
    return {
        "configured": configured,
        "message": "WeatherAPI.com is configured." if configured else "WeatherAPI.com API key is missing.",
    }


def integration_devices() -> Dict[str, Any]:
    return {
        "devices": [],
        "message": "WeatherAPI.com provides forecast data and does not expose local devices.",
    }


def read_integration_settings() -> Dict[str, Any]:
    settings = read_weatherapi_settings()
    return {
        "weatherapi_key": _text(settings.get("WEATHERAPI_KEY")),
        "weatherapi_default_location": _text(settings.get("DEFAULT_LOCATION")) or WEATHER_API_DEFAULT_LOCATION,
        "weatherapi_default_days": int(settings.get("DEFAULT_DAYS") or WEATHER_API_DEFAULT_DAYS),
        "weatherapi_default_units": _normalize_units(settings.get("DEFAULT_UNITS")),
        "weatherapi_include_aqi": "true" if bool(settings.get("INCLUDE_AQI")) else "false",
        "weatherapi_include_pollen": "true" if bool(settings.get("INCLUDE_POLLEN")) else "false",
        "weatherapi_include_alerts": "true" if bool(settings.get("INCLUDE_ALERTS")) else "false",
        "weatherapi_show_hourly_peek": int(
            settings.get("SHOW_HOURLY_PEEK") or WEATHER_API_DEFAULT_SHOW_HOURLY_PEEK
        ),
        "weatherapi_max_response_chars": int(
            settings.get("MAX_RESPONSE_CHARS") or WEATHER_API_DEFAULT_MAX_RESPONSE_CHARS
        ),
        "weatherapi_timeout_seconds": int(
            settings.get("TIMEOUT_SECONDS") or WEATHER_API_DEFAULT_TIMEOUT_SECONDS
        ),
    }


def save_integration_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload or {}
    saved = save_weatherapi_settings(
        api_key=data.get("weatherapi_key"),
        default_location=data.get("weatherapi_default_location"),
        default_days=data.get("weatherapi_default_days"),
        default_units=data.get("weatherapi_default_units"),
        include_aqi=data.get("weatherapi_include_aqi"),
        include_pollen=data.get("weatherapi_include_pollen"),
        include_alerts=data.get("weatherapi_include_alerts"),
        show_hourly_peek=data.get("weatherapi_show_hourly_peek"),
        max_response_chars=data.get("weatherapi_max_response_chars"),
        timeout_seconds=data.get("weatherapi_timeout_seconds"),
    )
    return {
        "weatherapi_key": _text(saved.get("WEATHERAPI_KEY")),
        "weatherapi_default_location": _text(saved.get("DEFAULT_LOCATION")) or WEATHER_API_DEFAULT_LOCATION,
        "weatherapi_default_days": int(saved.get("DEFAULT_DAYS") or WEATHER_API_DEFAULT_DAYS),
        "weatherapi_default_units": _normalize_units(saved.get("DEFAULT_UNITS")),
        "weatherapi_include_aqi": "true" if bool(saved.get("INCLUDE_AQI")) else "false",
        "weatherapi_include_pollen": "true" if bool(saved.get("INCLUDE_POLLEN")) else "false",
        "weatherapi_include_alerts": "true" if bool(saved.get("INCLUDE_ALERTS")) else "false",
        "weatherapi_show_hourly_peek": int(saved.get("SHOW_HOURLY_PEEK") or WEATHER_API_DEFAULT_SHOW_HOURLY_PEEK),
        "weatherapi_max_response_chars": int(
            saved.get("MAX_RESPONSE_CHARS") or WEATHER_API_DEFAULT_MAX_RESPONSE_CHARS
        ),
        "weatherapi_timeout_seconds": int(saved.get("TIMEOUT_SECONDS") or WEATHER_API_DEFAULT_TIMEOUT_SECONDS),
    }


def run_integration_action(action_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if _text(action_id) != "test":
        raise KeyError(f"Unsupported WeatherAPI action: {action_id}")

    current = read_integration_settings()
    data = payload or {}
    api_key = _text(data.get("weatherapi_key", current.get("weatherapi_key")))
    default_location = _text(
        data.get("weatherapi_default_location", current.get("weatherapi_default_location"))
    ) or WEATHER_API_DEFAULT_LOCATION
    default_days = _bounded_int(
        data.get("weatherapi_default_days", current.get("weatherapi_default_days")),
        default=WEATHER_API_DEFAULT_DAYS,
        minimum=1,
        maximum=14,
    )
    timeout_seconds = _bounded_int(
        data.get("weatherapi_timeout_seconds", current.get("weatherapi_timeout_seconds")),
        default=WEATHER_API_DEFAULT_TIMEOUT_SECONDS,
        minimum=2,
        maximum=60,
    )
    if not api_key:
        raise ValueError("WeatherAPI.com API key is required.")

    forecast, error = fetch_weatherapi_forecast(
        location=default_location,
        days=min(default_days, 1),
        api_key=api_key,
        include_aqi=_bool(data.get("weatherapi_include_aqi", current.get("weatherapi_include_aqi")), True),
        include_pollen=_bool(data.get("weatherapi_include_pollen", current.get("weatherapi_include_pollen")), True),
        include_alerts=_bool(data.get("weatherapi_include_alerts", current.get("weatherapi_include_alerts")), True),
        timeout_seconds=timeout_seconds,
    )
    if error:
        raise RuntimeError(error)
    location = forecast.get("location") if isinstance(forecast, dict) else {}
    name = _text((location or {}).get("name")) or default_location
    region = _text((location or {}).get("region"))
    label = ", ".join([part for part in (name, region) if part])
    return {
        "ok": True,
        "location": label or name,
        "message": f"WeatherAPI.com connection worked. Forecast loaded for {label or name}.",
    }
