from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import logging
import re
import threading
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from helpers import redis_client

ECOBEE_HOMEKIT_SETTINGS_KEY = "ecobee_homekit_settings"
ECOBEE_HOMEKIT_PAIRINGS_KEY = "ecobee_homekit_pairings"
ECOBEE_HOMEKIT_DEFAULT_ALIAS = "ecobee"
ECOBEE_HOMEKIT_DEFAULT_TIMEOUT_SECONDS = 10

SERVICE_THERMOSTAT = "0000004A-0000-1000-8000-0026BB765291"
SERVICE_ACCESSORY_INFORMATION = "0000003E-0000-1000-8000-0026BB765291"

CHAR_NAME = "00000023-0000-1000-8000-0026BB765291"
CHAR_MANUFACTURER = "00000020-0000-1000-8000-0026BB765291"
CHAR_MODEL = "00000021-0000-1000-8000-0026BB765291"
CHAR_SERIAL_NUMBER = "00000030-0000-1000-8000-0026BB765291"
CHAR_CURRENT_HEATING_COOLING_STATE = "0000000F-0000-1000-8000-0026BB765291"
CHAR_TARGET_HEATING_COOLING_STATE = "00000033-0000-1000-8000-0026BB765291"
CHAR_CURRENT_TEMPERATURE = "00000011-0000-1000-8000-0026BB765291"
CHAR_TARGET_TEMPERATURE = "00000035-0000-1000-8000-0026BB765291"
CHAR_TEMPERATURE_UNITS = "00000036-0000-1000-8000-0026BB765291"
CHAR_CURRENT_RELATIVE_HUMIDITY = "00000010-0000-1000-8000-0026BB765291"
CHAR_TARGET_RELATIVE_HUMIDITY = "00000034-0000-1000-8000-0026BB765291"
CHAR_COOLING_THRESHOLD_TEMPERATURE = "0000000D-0000-1000-8000-0026BB765291"
CHAR_HEATING_THRESHOLD_TEMPERATURE = "00000012-0000-1000-8000-0026BB765291"

HOMEKIT_MODE_VALUES = {
    "off": 0,
    "heat": 1,
    "cool": 2,
    "auto": 3,
    "heat_cool": 3,
}
HOMEKIT_TARGET_MODE_NAMES = {
    0: "off",
    1: "heat",
    2: "cool",
    3: "auto",
}


class _AioHomeKitBackgroundDisconnectFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = str(record.getMessage() or "")
        if record.name != "aiohomekit.utils" or not message.startswith("Failure running background task"):
            return True
        exc = record.exc_info[1] if record.exc_info else None
        if exc is not None and exc.__class__.__name__ == "AccessoryDisconnectedError":
            return False
        return True


def _install_aiohomekit_log_filter() -> None:
    log = logging.getLogger("aiohomekit.utils")
    if getattr(log, "_tater_homekit_disconnect_filter", False):
        return
    log.addFilter(_AioHomeKitBackgroundDisconnectFilter())
    setattr(log, "_tater_homekit_disconnect_filter", True)


_install_aiohomekit_log_filter()
HOMEKIT_CURRENT_MODE_NAMES = {
    0: "off",
    1: "heating",
    2: "cooling",
}

INTEGRATION = {
    "id": "ecobee_homekit",
    "name": "Ecobee (HomeKit)",
    "description": "Pair an Ecobee thermostat directly through HomeKit using the setup code.",
    "badge": "ECO",
    "order": 35,
    "fields": [
        {
            "key": "ecobee_homekit_alias",
            "label": "Pairing Alias",
            "type": "text",
            "default": ECOBEE_HOMEKIT_DEFAULT_ALIAS,
            "placeholder": ECOBEE_HOMEKIT_DEFAULT_ALIAS,
            "description": "A local name Tater uses for this HomeKit pairing.",
        },
        {
            "key": "ecobee_homekit_setup_code",
            "label": "HomeKit Setup Code",
            "type": "password",
            "default": "",
            "placeholder": "123-45-678",
            "description": "Use the 8-digit HomeKit code from the thermostat. Tater does not save this after pairing.",
        },
        {
            "key": "ecobee_homekit_device_id",
            "label": "HomeKit Device ID",
            "type": "text",
            "default": "",
            "placeholder": "AA:BB:CC:DD:EE:FF",
            "description": "Optional. Discover devices first, then paste the thermostat id here if more than one appears.",
            "full_width": True,
        },
        {
            "key": "ecobee_homekit_discovery_timeout_seconds",
            "label": "Discovery Timeout Seconds",
            "type": "number",
            "default": ECOBEE_HOMEKIT_DEFAULT_TIMEOUT_SECONDS,
            "min": 3,
            "max": 60,
        },
    ],
    "actions": [
        {
            "id": "discover",
            "label": "Discover HomeKit",
            "status": "Looking for HomeKit accessories on the local network.",
        },
        {
            "id": "pair",
            "label": "Pair Thermostat",
            "status": "Pairing the selected Ecobee thermostat with the HomeKit setup code.",
        },
        {
            "id": "list_thermostats",
            "label": "List Thermostats",
            "status": "Reading thermostats from the saved HomeKit pairing.",
        },
        {
            "id": "forget_pairing",
            "label": "Forget Saved Pairing",
            "status": "Removing Tater's saved HomeKit pairing data for this alias.",
        },
    ],
}


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "ignore").strip()
    return str(value or "").strip()


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(_text(value)))
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def _normalize_alias(value: Any) -> str:
    alias = _text(value) or ECOBEE_HOMEKIT_DEFAULT_ALIAS
    alias = re.sub(r"[^A-Za-z0-9_.-]+", "_", alias).strip("._-")
    return alias or ECOBEE_HOMEKIT_DEFAULT_ALIAS


def _normalize_device_id(value: Any) -> str:
    return _text(value).lower()


def _normalize_setup_code(value: Any) -> str:
    text = _text(value)
    digits = re.sub(r"\D+", "", text)
    if len(digits) == 8:
        return f"{digits[:3]}-{digits[3:5]}-{digits[5:]}"
    if re.match(r"^\d{3}-\d{2}-\d{3}$", text):
        return text
    return ""


def _normalize_uuid(value: Any) -> str:
    return _text(value).lower()


def _uuid_matches(value: Any, expected: str) -> bool:
    return _normalize_uuid(value) == expected.lower()


def _json_loads(value: Any, default: Any = None) -> Any:
    try:
        return json.loads(_text(value))
    except Exception:
        return default


def homekit_dependency_available() -> bool:
    return importlib.util.find_spec("aiohomekit") is not None and importlib.util.find_spec("zeroconf") is not None


def _require_homekit_dependencies() -> Dict[str, Any]:
    try:
        from aiohomekit.controller.controller import Controller
        from aiohomekit.zeroconf import HAP_TYPE_TCP, HAP_TYPE_UDP, ZeroconfServiceListener
        from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf
    except Exception as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "aiohomekit and zeroconf are required for Ecobee HomeKit pairing. "
            "Install Tater requirements, then restart Tater."
        ) from exc
    return {
        "Controller": Controller,
        "AsyncZeroconf": AsyncZeroconf,
        "AsyncServiceBrowser": AsyncServiceBrowser,
        "ZeroconfServiceListener": ZeroconfServiceListener,
        "HAP_TYPE_TCP": HAP_TYPE_TCP,
        "HAP_TYPE_UDP": HAP_TYPE_UDP,
    }


def _run_sync(coro: Any) -> Any:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is None:
        return asyncio.run(coro)

    if not loop.is_running():
        return loop.run_until_complete(coro)

    result: Dict[str, Any] = {}

    def runner() -> None:
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - thread handoff
            result["error"] = exc

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join()
    if "error" in result:
        raise result["error"]
    return result.get("value")


async def _with_controller(callback: Callable[[Any], Any]) -> Any:
    deps = _require_homekit_dependencies()
    zc = deps["AsyncZeroconf"]()
    listener = deps["ZeroconfServiceListener"]()
    browser = deps["AsyncServiceBrowser"](zc.zeroconf, [deps["HAP_TYPE_TCP"], deps["HAP_TYPE_UDP"]], listener=listener)
    controller = deps["Controller"](async_zeroconf_instance=zc)
    try:
        await controller.async_start()
        return await callback(controller)
    finally:
        with contextlib.suppress(Exception):
            await controller.async_stop()
        with contextlib.suppress(Exception):
            await browser.async_cancel()
        with contextlib.suppress(Exception):
            await zc.async_close()


def read_ecobee_homekit_settings(client: Any = None) -> Dict[str, str]:
    store = client or redis_client
    try:
        raw = store.hgetall(ECOBEE_HOMEKIT_SETTINGS_KEY) or {}
    except Exception:
        raw = {}
    timeout = _bounded_int(
        raw.get("ECOBEE_HOMEKIT_DISCOVERY_TIMEOUT_SECONDS"),
        default=ECOBEE_HOMEKIT_DEFAULT_TIMEOUT_SECONDS,
        minimum=3,
        maximum=60,
    )
    return {
        "ECOBEE_HOMEKIT_ALIAS": _normalize_alias(raw.get("ECOBEE_HOMEKIT_ALIAS")),
        "ECOBEE_HOMEKIT_DEVICE_ID": _normalize_device_id(raw.get("ECOBEE_HOMEKIT_DEVICE_ID")),
        "ECOBEE_HOMEKIT_DISCOVERY_TIMEOUT_SECONDS": str(timeout),
    }


def save_ecobee_homekit_settings(
    *,
    alias: Any = None,
    device_id: Any = None,
    discovery_timeout_seconds: Any = None,
    client: Any = None,
) -> Dict[str, str]:
    store = client or redis_client
    current = read_ecobee_homekit_settings(store)
    next_settings = {
        "ECOBEE_HOMEKIT_ALIAS": _normalize_alias(current.get("ECOBEE_HOMEKIT_ALIAS") if alias is None else alias),
        "ECOBEE_HOMEKIT_DEVICE_ID": _normalize_device_id(
            current.get("ECOBEE_HOMEKIT_DEVICE_ID") if device_id is None else device_id
        ),
        "ECOBEE_HOMEKIT_DISCOVERY_TIMEOUT_SECONDS": str(
            _bounded_int(
                current.get("ECOBEE_HOMEKIT_DISCOVERY_TIMEOUT_SECONDS")
                if discovery_timeout_seconds is None
                else discovery_timeout_seconds,
                default=ECOBEE_HOMEKIT_DEFAULT_TIMEOUT_SECONDS,
                minimum=3,
                maximum=60,
            )
        ),
    }
    store.hset(ECOBEE_HOMEKIT_SETTINGS_KEY, mapping=next_settings)
    return read_ecobee_homekit_settings(store)


def _load_pairings(client: Any = None) -> Dict[str, Dict[str, Any]]:
    store = client or redis_client
    try:
        raw = store.hgetall(ECOBEE_HOMEKIT_PAIRINGS_KEY) or {}
    except Exception:
        raw = {}
    pairings: Dict[str, Dict[str, Any]] = {}
    for alias, blob in raw.items():
        name = _normalize_alias(alias)
        data = _json_loads(blob, {})
        if name and isinstance(data, dict):
            pairings[name] = data
    return pairings


def _load_pairing(alias: Any = None, *, required: bool = True, client: Any = None) -> Dict[str, Any]:
    name = _normalize_alias(alias or read_ecobee_homekit_settings(client).get("ECOBEE_HOMEKIT_ALIAS"))
    data = _load_pairings(client).get(name)
    if not data and required:
        raise ValueError("Ecobee HomeKit is not paired. Open Tater Settings > Integrations > Ecobee (HomeKit).")
    return dict(data or {})


def _save_pairing(alias: Any, pairing_data: Dict[str, Any], client: Any = None) -> None:
    name = _normalize_alias(alias)
    if not name:
        raise ValueError("Pairing alias is required.")
    (client or redis_client).hset(ECOBEE_HOMEKIT_PAIRINGS_KEY, name, json.dumps(pairing_data, sort_keys=True))


def forget_ecobee_homekit_pairing(alias: Any = None, client: Any = None) -> bool:
    name = _normalize_alias(alias or read_ecobee_homekit_settings(client).get("ECOBEE_HOMEKIT_ALIAS"))
    try:
        return bool((client or redis_client).hdel(ECOBEE_HOMEKIT_PAIRINGS_KEY, name))
    except Exception:
        return False


def _discovery_row(discovery: Any) -> Dict[str, Any]:
    desc = getattr(discovery, "description", None)
    category = getattr(desc, "category", "")
    try:
        category_id = int(category)
    except Exception:
        category_id = None
    category_label = _text(getattr(category, "name", "") or str(category)).replace("Categories.", "").lower()
    return {
        "id": _normalize_device_id(getattr(desc, "id", "")),
        "name": _text(getattr(desc, "name", "")),
        "model": _text(getattr(desc, "model", "")),
        "category": category_label,
        "category_id": category_id,
        "paired": bool(getattr(discovery, "paired", False)),
        "address": _text(getattr(desc, "address", "")),
        "port": int(getattr(desc, "port", 0) or 0),
    }


def _is_ecobee_candidate(row: Dict[str, Any]) -> bool:
    haystack = f"{row.get('name', '')} {row.get('model', '')} {row.get('category', '')}".lower()
    return "ecobee" in haystack or "thermostat" in haystack or row.get("category_id") == 9


async def _discover_homekit_accessories_async(timeout_seconds: int) -> List[Dict[str, Any]]:
    async def work(controller: Any) -> List[Dict[str, Any]]:
        await asyncio.sleep(max(0.5, float(timeout_seconds)))
        rows: List[Dict[str, Any]] = []
        seen: set[str] = set()
        async for discovery in controller.async_discover():
            row = _discovery_row(discovery)
            if not row.get("id") or row["id"] in seen:
                continue
            seen.add(row["id"])
            rows.append(row)
        rows.sort(key=lambda item: (_text(item.get("name")).casefold(), _text(item.get("id"))))
        return rows

    return await _with_controller(work)


def discover_homekit_accessories(timeout_seconds: Any = None) -> List[Dict[str, Any]]:
    timeout = _bounded_int(
        timeout_seconds or read_ecobee_homekit_settings().get("ECOBEE_HOMEKIT_DISCOVERY_TIMEOUT_SECONDS"),
        default=ECOBEE_HOMEKIT_DEFAULT_TIMEOUT_SECONDS,
        minimum=3,
        maximum=60,
    )
    return _run_sync(_discover_homekit_accessories_async(timeout))


async def _pair_ecobee_homekit_async(*, alias: str, setup_code: str, device_id: str, timeout_seconds: int) -> Dict[str, Any]:
    async def work(controller: Any) -> Dict[str, Any]:
        target_discovery = None
        if device_id:
            target_discovery = await controller.async_find(device_id, timeout=timeout_seconds)
        else:
            await asyncio.sleep(max(0.5, float(timeout_seconds)))
            candidates: List[Any] = []
            async for discovery in controller.async_discover():
                row = _discovery_row(discovery)
                if discovery.paired:
                    continue
                if _is_ecobee_candidate(row):
                    candidates.append(discovery)
            if not candidates:
                raise ValueError("No unpaired Ecobee/HomeKit thermostat was found. Use Discover HomeKit and enter the Device ID if needed.")
            if len(candidates) > 1:
                choices = ", ".join(f"{_discovery_row(item).get('name') or 'HomeKit device'} ({_discovery_row(item).get('id')})" for item in candidates)
                raise ValueError(f"Multiple HomeKit thermostat candidates were found. Enter one Device ID first: {choices}")
            target_discovery = candidates[0]

        row = _discovery_row(target_discovery)
        if target_discovery.paired:
            raise ValueError(
                f"{row.get('name') or row.get('id')} already reports as paired. "
                "Put the thermostat in HomeKit pairing mode or remove the existing HomeKit pairing first."
            )

        finish_pairing = await target_discovery.async_start_pairing(alias)
        pairing = await finish_pairing(setup_code)
        pairing_data = dict(getattr(pairing, "pairing_data", {}) or {})
        if not pairing_data:
            raise RuntimeError("HomeKit pairing completed but no pairing data was returned.")
        return {
            "alias": alias,
            "device": row,
            "pairing_data": pairing_data,
        }

    return await _with_controller(work)


def pair_ecobee_homekit(
    *,
    alias: Any = None,
    setup_code: Any = None,
    device_id: Any = None,
    timeout_seconds: Any = None,
) -> Dict[str, Any]:
    name = _normalize_alias(alias or read_ecobee_homekit_settings().get("ECOBEE_HOMEKIT_ALIAS"))
    pin = _normalize_setup_code(setup_code)
    if not pin:
        raise ValueError("Enter a valid HomeKit setup code in the form 123-45-678.")
    timeout = _bounded_int(
        timeout_seconds or read_ecobee_homekit_settings().get("ECOBEE_HOMEKIT_DISCOVERY_TIMEOUT_SECONDS"),
        default=ECOBEE_HOMEKIT_DEFAULT_TIMEOUT_SECONDS,
        minimum=3,
        maximum=60,
    )
    device = _normalize_device_id(device_id)
    result = _run_sync(
        _pair_ecobee_homekit_async(alias=name, setup_code=pin, device_id=device, timeout_seconds=timeout)
    )
    _save_pairing(name, result["pairing_data"])
    paired_device = result.get("device") if isinstance(result.get("device"), dict) else {}
    save_ecobee_homekit_settings(alias=name, device_id=paired_device.get("id") or device, discovery_timeout_seconds=timeout)
    return {
        "alias": name,
        "device": paired_device,
    }


def _char_by_type(service: Dict[str, Any], char_type: str) -> Optional[Dict[str, Any]]:
    for char in service.get("characteristics") or []:
        if isinstance(char, dict) and _uuid_matches(char.get("type"), char_type):
            return char
    return None


def _char_value(service: Dict[str, Any], char_type: str, default: Any = None) -> Any:
    char = _char_by_type(service, char_type)
    if not isinstance(char, dict):
        return default
    return char.get("value", default)


def _accessory_info(accessory: Dict[str, Any]) -> Dict[str, Any]:
    info: Dict[str, Any] = {}
    for service in accessory.get("services") or []:
        if not isinstance(service, dict) or not _uuid_matches(service.get("type"), SERVICE_ACCESSORY_INFORMATION):
            continue
        info = {
            "name": _text(_char_value(service, CHAR_NAME)),
            "manufacturer": _text(_char_value(service, CHAR_MANUFACTURER)),
            "model": _text(_char_value(service, CHAR_MODEL)),
            "serial_number": _text(_char_value(service, CHAR_SERIAL_NUMBER)),
        }
        break
    return info


def _c_to_f(value: Any) -> Optional[float]:
    try:
        return round((float(value) * 9.0 / 5.0) + 32.0, 1)
    except Exception:
        return None


def _f_to_c(value: Any) -> float:
    return round((float(value) - 32.0) * 5.0 / 9.0, 1)


def _round_temperature(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        rounded = round(float(value), 1)
        if rounded.is_integer():
            return int(rounded)
        return rounded
    except Exception:
        return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _temperature_payload(value_c: Any, unit: str) -> Tuple[Optional[float], Optional[float], str]:
    value = _coerce_float(value_c)
    c = _round_temperature(value) if value is not None else None
    f = _round_temperature(_c_to_f(value)) if value is not None else None
    if unit.upper() == "F":
        return c, f, "F"
    return c, f, "C"


def _mode_name(value: Any, names: Dict[int, str]) -> str:
    parsed = _coerce_float(value)
    if parsed is None:
        return ""
    try:
        index = int(parsed)
    except Exception:
        return ""
    return names.get(index, str(index))


def _thermostat_row(alias: str, accessory: Dict[str, Any], service: Dict[str, Any]) -> Dict[str, Any]:
    aid = int(accessory.get("aid") or 0)
    service_iid = int(service.get("iid") or 0)
    info = _accessory_info(accessory)
    name = _text(_char_value(service, CHAR_NAME)) or info.get("name") or f"Thermostat {aid}"
    unit_value = _char_value(service, CHAR_TEMPERATURE_UNITS, 1)
    display_unit = "F" if str(unit_value) == "1" else "C"

    current_c, current_f, _ = _temperature_payload(_char_value(service, CHAR_CURRENT_TEMPERATURE), display_unit)
    target_c, target_f, _ = _temperature_payload(_char_value(service, CHAR_TARGET_TEMPERATURE), display_unit)
    heat_c, heat_f, _ = _temperature_payload(_char_value(service, CHAR_HEATING_THRESHOLD_TEMPERATURE), display_unit)
    cool_c, cool_f, _ = _temperature_payload(_char_value(service, CHAR_COOLING_THRESHOLD_TEMPERATURE), display_unit)

    target_mode = _char_value(service, CHAR_TARGET_HEATING_COOLING_STATE)
    current_mode = _char_value(service, CHAR_CURRENT_HEATING_COOLING_STATE)
    current_mode_char = _char_by_type(service, CHAR_CURRENT_HEATING_COOLING_STATE) or {}
    current_temp_char = _char_by_type(service, CHAR_CURRENT_TEMPERATURE) or {}
    current_humidity_char = _char_by_type(service, CHAR_CURRENT_RELATIVE_HUMIDITY) or {}
    target_char = _char_by_type(service, CHAR_TARGET_HEATING_COOLING_STATE) or {}
    target_temp_char = _char_by_type(service, CHAR_TARGET_TEMPERATURE) or {}
    heat_temp_char = _char_by_type(service, CHAR_HEATING_THRESHOLD_TEMPERATURE) or {}
    cool_temp_char = _char_by_type(service, CHAR_COOLING_THRESHOLD_TEMPERATURE) or {}

    return {
        "id": f"{alias}:{aid}:{service_iid}",
        "alias": alias,
        "aid": aid,
        "service_iid": service_iid,
        "name": name,
        "manufacturer": info.get("manufacturer", ""),
        "model": info.get("model", ""),
        "serial_number": info.get("serial_number", ""),
        "temperature_unit": display_unit,
        "current_temperature_c": current_c,
        "current_temperature_f": current_f,
        "target_temperature_c": target_c,
        "target_temperature_f": target_f,
        "heating_threshold_c": heat_c,
        "heating_threshold_f": heat_f,
        "cooling_threshold_c": cool_c,
        "cooling_threshold_f": cool_f,
        "target_hvac_mode": _mode_name(target_mode, HOMEKIT_TARGET_MODE_NAMES),
        "current_hvac_state": _mode_name(current_mode, HOMEKIT_CURRENT_MODE_NAMES),
        "current_humidity": _char_value(service, CHAR_CURRENT_RELATIVE_HUMIDITY),
        "target_humidity": _char_value(service, CHAR_TARGET_RELATIVE_HUMIDITY),
        "current_mode_iid": current_mode_char.get("iid"),
        "current_temperature_iid": current_temp_char.get("iid"),
        "current_humidity_iid": current_humidity_char.get("iid"),
        "target_mode_iid": target_char.get("iid"),
        "target_temperature_iid": target_temp_char.get("iid"),
        "heating_threshold_iid": heat_temp_char.get("iid"),
        "cooling_threshold_iid": cool_temp_char.get("iid"),
        "target_mode_writable": "pw" in (target_char.get("perms") or []),
        "target_temperature_writable": "pw" in (target_temp_char.get("perms") or []),
    }


def _extract_thermostats(alias: str, accessories: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for accessory in accessories or []:
        if not isinstance(accessory, dict):
            continue
        for service in accessory.get("services") or []:
            if isinstance(service, dict) and _uuid_matches(service.get("type"), SERVICE_THERMOSTAT):
                rows.append(_thermostat_row(alias, accessory, service))
    rows.sort(key=lambda item: (_text(item.get("name")).casefold(), int(item.get("aid") or 0), int(item.get("service_iid") or 0)))
    return rows


async def _with_pairing(alias: str, callback: Callable[[Any], Any]) -> Any:
    pairing_data = _load_pairing(alias, required=True)

    async def work(controller: Any) -> Any:
        pairing = controller.load_pairing(alias, dict(pairing_data))
        try:
            return await callback(pairing)
        finally:
            with contextlib.suppress(Exception):
                await pairing.close()

    return await _with_controller(work)


async def _list_homekit_thermostats_async(alias: str) -> List[Dict[str, Any]]:
    async def work(pairing: Any) -> List[Dict[str, Any]]:
        accessories = await pairing.list_accessories_and_characteristics()
        return _extract_thermostats(alias, accessories if isinstance(accessories, list) else [])

    return await _with_pairing(alias, work)


def list_homekit_thermostats(alias: Any = None) -> List[Dict[str, Any]]:
    name = _normalize_alias(alias or read_ecobee_homekit_settings().get("ECOBEE_HOMEKIT_ALIAS"))
    return _run_sync(_list_homekit_thermostats_async(name))


def ecobee_homekit_paired(alias: Any = None, client: Any = None) -> bool:
    name = _normalize_alias(alias or read_ecobee_homekit_settings(client).get("ECOBEE_HOMEKIT_ALIAS"))
    return name in _load_pairings(client)


_WATCHED_THERMOSTAT_CHARS = {
    CHAR_CURRENT_HEATING_COOLING_STATE: "current_hvac_state",
    CHAR_TARGET_HEATING_COOLING_STATE: "target_hvac_mode",
    CHAR_CURRENT_TEMPERATURE: "current_temperature",
    CHAR_TARGET_TEMPERATURE: "target_temperature",
    CHAR_HEATING_THRESHOLD_TEMPERATURE: "heating_threshold_temperature",
    CHAR_COOLING_THRESHOLD_TEMPERATURE: "cooling_threshold_temperature",
    CHAR_CURRENT_RELATIVE_HUMIDITY: "current_humidity",
    CHAR_TARGET_RELATIVE_HUMIDITY: "target_humidity",
    CHAR_TEMPERATURE_UNITS: "temperature_unit",
}


def _homekit_event_pairs(alias: str, accessories: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[Tuple[int, int], Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    watched: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for accessory in accessories or []:
        if not isinstance(accessory, dict):
            continue
        aid = int(accessory.get("aid") or 0)
        for service in accessory.get("services") or []:
            if not isinstance(service, dict) or not _uuid_matches(service.get("type"), SERVICE_THERMOSTAT):
                continue
            row = _thermostat_row(alias, accessory, service)
            rows.append(row)
            service_iid = int(service.get("iid") or 0)
            for char in service.get("characteristics") or []:
                if not isinstance(char, dict):
                    continue
                char_type = _normalize_uuid(char.get("type"))
                char_name = _WATCHED_THERMOSTAT_CHARS.get(char_type)
                if not char_name:
                    continue
                perms = char.get("perms") if isinstance(char.get("perms"), list) else []
                if perms and "ev" not in perms:
                    continue
                iid = int(char.get("iid") or 0)
                if not aid or not iid:
                    continue
                watched[(aid, iid)] = {
                    "thermostat_id": row.get("id"),
                    "thermostat_name": row.get("name"),
                    "service_iid": service_iid,
                    "characteristic": char_name,
                    "characteristic_type": char_type,
                    "value": char.get("value"),
                }
    rows.sort(key=lambda item: (_text(item.get("name")).casefold(), int(item.get("aid") or 0), int(item.get("service_iid") or 0)))
    return rows, watched


def _homekit_event_entries(raw: Any) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    if isinstance(raw, dict):
        for key, value in raw.items():
            aid = 0
            iid = 0
            if isinstance(key, tuple) and len(key) >= 2:
                aid = int(key[0] or 0)
                iid = int(key[1] or 0)
            elif isinstance(key, str) and "." in key:
                left, right = key.split(".", 1)
                aid = int(left or 0)
                iid = int(right or 0)
            if isinstance(value, dict):
                aid = int(value.get("aid") or aid or 0)
                iid = int(value.get("iid") or iid or 0)
                val = value.get("value")
            else:
                val = value
            if aid and iid:
                entries.append({"aid": aid, "iid": iid, "value": val})
        return entries
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            aid = int(item.get("aid") or 0)
            iid = int(item.get("iid") or 0)
            if aid and iid:
                entries.append({"aid": aid, "iid": iid, "value": item.get("value")})
    return entries


async def watch_homekit_thermostats(
    *,
    alias: Any = None,
    on_update: Callable[[Dict[str, Any]], Any],
    stop_event: Any = None,
) -> None:
    name = _normalize_alias(alias or read_ecobee_homekit_settings().get("ECOBEE_HOMEKIT_ALIAS"))
    pairing_data = _load_pairing(name, required=True)
    deps = _require_homekit_dependencies()
    zc = deps["AsyncZeroconf"]()
    listener = deps["ZeroconfServiceListener"]()
    browser = deps["AsyncServiceBrowser"](zc.zeroconf, [deps["HAP_TYPE_TCP"], deps["HAP_TYPE_UDP"]], listener=listener)
    controller = deps["Controller"](async_zeroconf_instance=zc)
    pairing = None
    unsubscribe_callback = None
    pairs: List[Tuple[int, int]] = []

    async def emit(payload: Dict[str, Any]) -> None:
        result = on_update(payload)
        if asyncio.iscoroutine(result):
            await result

    try:
        await controller.async_start()
        pairing = controller.load_pairing(name, dict(pairing_data))
        accessories = await pairing.list_accessories_and_characteristics()
        thermostat_rows, watched = _homekit_event_pairs(name, accessories if isinstance(accessories, list) else [])
        pairs = sorted(watched.keys())
        await emit({"type": "snapshot", "alias": name, "thermostats": thermostat_rows, "watched": list(watched.values())})
        if not pairs:
            raise RuntimeError("No event-capable thermostat characteristics were found in the Ecobee HomeKit pairing.")

        queue: asyncio.Queue[Any] = asyncio.Queue()
        loop = asyncio.get_running_loop()

        def callback(data: Any) -> None:
            with contextlib.suppress(Exception):
                loop.call_soon_threadsafe(queue.put_nowait, data)

        dispatcher_connect = getattr(pairing, "dispatcher_connect", None)
        if not callable(dispatcher_connect):
            raise RuntimeError("The installed aiohomekit pairing does not expose event callbacks.")
        unsubscribe_callback = dispatcher_connect(callback)
        result = await pairing.subscribe(pairs)
        if isinstance(result, dict):
            failures = {}
            for key, value in result.items():
                if not isinstance(value, dict):
                    continue
                try:
                    status = int(value.get("status") or 0)
                except Exception:
                    status = 0
                if status != 0:
                    failures[key] = value
            if failures and len(failures) >= len(pairs):
                raise RuntimeError(f"HomeKit thermostat subscription failed: {failures}")

        while not (stop_event is not None and stop_event.is_set()):
            try:
                raw_update = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            changed: List[Dict[str, Any]] = []
            for entry in _homekit_event_entries(raw_update):
                meta = watched.get((int(entry["aid"]), int(entry["iid"])))
                if not meta:
                    continue
                row = dict(meta)
                row["value"] = entry.get("value")
                changed.append(row)
            accessories = await pairing.list_accessories_and_characteristics()
            thermostat_rows, watched = _homekit_event_pairs(name, accessories if isinstance(accessories, list) else [])
            await emit(
                {
                    "type": "update",
                    "alias": name,
                    "changed": changed,
                    "raw": raw_update,
                    "thermostats": thermostat_rows,
                }
            )
    finally:
        if pairs:
            with contextlib.suppress(Exception):
                if pairing is not None:
                    await pairing.unsubscribe(pairs)
        if callable(unsubscribe_callback):
            with contextlib.suppress(Exception):
                unsubscribe_callback()
        if pairing is not None:
            with contextlib.suppress(Exception):
                await pairing.close()
        with contextlib.suppress(Exception):
            await controller.async_stop()
        with contextlib.suppress(Exception):
            await browser.async_cancel()
        with contextlib.suppress(Exception):
            await zc.async_close()


def _find_thermostat(rows: List[Dict[str, Any]], thermostat_id: Any = None, target: Any = None) -> Dict[str, Any]:
    if not rows:
        raise ValueError("No thermostats were found in the saved Ecobee HomeKit pairing.")
    wanted_id = _text(thermostat_id)
    if wanted_id:
        for row in rows:
            if _text(row.get("id")) == wanted_id:
                return row
        raise ValueError(f"No HomeKit thermostat matched id {wanted_id}.")
    target_text = _text(target).lower()
    if target_text:
        target_tokens = {token for token in re.split(r"[^a-z0-9]+", target_text) if token}
        scored: List[Tuple[int, Dict[str, Any]]] = []
        for row in rows:
            haystack = f"{row.get('name', '')} {row.get('model', '')}".lower()
            if target_text in haystack:
                scored.append((100 + len(target_text), row))
                continue
            name_tokens = {token for token in re.split(r"[^a-z0-9]+", haystack) if token}
            score = len(target_tokens & name_tokens)
            if score:
                scored.append((score, row))
        if scored:
            scored.sort(key=lambda item: item[0], reverse=True)
            if len(scored) == 1 or scored[0][0] > scored[1][0]:
                return scored[0][1]
    if len(rows) == 1:
        return rows[0]
    choices = ", ".join(_text(row.get("name")) or _text(row.get("id")) for row in rows)
    raise ValueError(f"Multiple Ecobee HomeKit thermostats are available. Choose one: {choices}")


def _normalize_hvac_mode(value: Any) -> str:
    text = _text(value).lower().replace("-", "_").replace(" ", "_")
    if text in {"off", "heat", "cool", "auto", "heat_cool"}:
        return text
    if text in {"heating", "heater"}:
        return "heat"
    if text in {"cooling", "ac", "air_conditioning"}:
        return "cool"
    if text in {"automatic", "on"}:
        return "auto"
    raise ValueError("HVAC mode must be one of off, heat, cool, or auto.")


def _put_result_errors(results: Any) -> List[str]:
    errors: List[str] = []
    if not isinstance(results, dict):
        return errors
    for key, value in results.items():
        if not isinstance(value, dict):
            continue
        status = int(value.get("status") or 0)
        if status != 0:
            errors.append(f"{key}: {_text(value.get('description')) or status}")
    return errors


async def _set_thermostat_values_async(
    *,
    alias: str,
    thermostat_id: str = "",
    target: str = "",
    mode: str = "",
    temperature: Optional[float] = None,
    temperature_unit: str = "F",
) -> Dict[str, Any]:
    async def work(pairing: Any) -> Dict[str, Any]:
        accessories = await pairing.list_accessories_and_characteristics()
        rows = _extract_thermostats(alias, accessories if isinstance(accessories, list) else [])
        thermostat = _find_thermostat(rows, thermostat_id=thermostat_id, target=target)

        updates: List[Tuple[int, int, Any]] = []
        aid = int(thermostat.get("aid") or 0)
        if mode:
            normalized_mode = _normalize_hvac_mode(mode)
            iid = int(thermostat.get("target_mode_iid") or 0)
            if not iid:
                raise ValueError(f"{thermostat.get('name') or thermostat.get('id')} does not expose a writable HVAC mode.")
            updates.append((aid, iid, HOMEKIT_MODE_VALUES[normalized_mode]))
        if temperature is not None:
            iid = int(thermostat.get("target_temperature_iid") or 0)
            if not iid:
                raise ValueError(f"{thermostat.get('name') or thermostat.get('id')} does not expose a writable target temperature.")
            unit = _text(temperature_unit).upper()
            value_c = float(temperature) if unit == "C" else _f_to_c(float(temperature))
            updates.append((aid, iid, round(value_c, 1)))

        if not updates:
            raise ValueError("No thermostat update was requested.")
        results = await pairing.put_characteristics(updates)
        errors = _put_result_errors(results)
        if errors:
            raise RuntimeError("HomeKit thermostat update failed: " + "; ".join(errors))

        refreshed = await pairing.list_accessories_and_characteristics()
        refreshed_rows = _extract_thermostats(alias, refreshed if isinstance(refreshed, list) else [])
        return _find_thermostat(refreshed_rows, thermostat_id=thermostat.get("id"))

    return await _with_pairing(alias, work)


def set_homekit_thermostat_mode(mode: Any, *, thermostat_id: Any = None, target: Any = None, alias: Any = None) -> Dict[str, Any]:
    name = _normalize_alias(alias or read_ecobee_homekit_settings().get("ECOBEE_HOMEKIT_ALIAS"))
    return _run_sync(
        _set_thermostat_values_async(
            alias=name,
            thermostat_id=_text(thermostat_id),
            target=_text(target),
            mode=_normalize_hvac_mode(mode),
        )
    )


def set_homekit_thermostat_temperature(
    temperature: Any,
    *,
    temperature_unit: str = "F",
    mode: Any = None,
    thermostat_id: Any = None,
    target: Any = None,
    alias: Any = None,
) -> Dict[str, Any]:
    name = _normalize_alias(alias or read_ecobee_homekit_settings().get("ECOBEE_HOMEKIT_ALIAS"))
    try:
        temp_value = float(temperature)
    except Exception as exc:
        raise ValueError("Target temperature must be a number.") from exc
    return _run_sync(
        _set_thermostat_values_async(
            alias=name,
            thermostat_id=_text(thermostat_id),
            target=_text(target),
            mode=_normalize_hvac_mode(mode) if _text(mode) else "",
            temperature=temp_value,
            temperature_unit=temperature_unit,
        )
    )


def read_integration_settings() -> Dict[str, Any]:
    settings = read_ecobee_homekit_settings()
    return {
        "ecobee_homekit_alias": settings.get("ECOBEE_HOMEKIT_ALIAS") or ECOBEE_HOMEKIT_DEFAULT_ALIAS,
        "ecobee_homekit_setup_code": "",
        "ecobee_homekit_device_id": settings.get("ECOBEE_HOMEKIT_DEVICE_ID", ""),
        "ecobee_homekit_discovery_timeout_seconds": int(
            settings.get("ECOBEE_HOMEKIT_DISCOVERY_TIMEOUT_SECONDS") or ECOBEE_HOMEKIT_DEFAULT_TIMEOUT_SECONDS
        ),
    }


def integration_status() -> Dict[str, Any]:
    settings = read_ecobee_homekit_settings()
    alias = settings.get("ECOBEE_HOMEKIT_ALIAS") or ECOBEE_HOMEKIT_DEFAULT_ALIAS
    paired = alias in _load_pairings()
    if not homekit_dependency_available():
        return {
            "configured": paired,
            "dependency_available": False,
            "message": "Ecobee HomeKit needs aiohomekit installed from Tater requirements.",
        }
    return {
        "configured": paired,
        "dependency_available": True,
        "message": f"Ecobee HomeKit is paired as {alias}." if paired else "Ecobee HomeKit is not paired yet.",
    }


def integration_devices() -> Dict[str, Any]:
    settings = read_ecobee_homekit_settings()
    alias = settings.get("ECOBEE_HOMEKIT_ALIAS") or ECOBEE_HOMEKIT_DEFAULT_ALIAS
    if alias not in _load_pairings():
        return {"devices": [], "message": "Ecobee HomeKit is not paired yet."}
    rows = list_homekit_thermostats(alias)
    devices: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        current_temp = row.get("current_temperature_f")
        state = f"{current_temp} F" if current_temp not in (None, "") else _text(row.get("current_hvac_state"))
        devices.append(
            {
                "id": _text(row.get("id")) or _text(row.get("name")),
                "name": _text(row.get("name")) or "Ecobee Thermostat",
                "type": "thermostat",
                "status": _text(row.get("current_hvac_state")),
                "state": state,
                "details": {
                    "current_temperature_f": row.get("current_temperature_f"),
                    "current_humidity": row.get("current_humidity"),
                    "target_temperature_f": row.get("target_temperature_f"),
                    "heating_threshold_f": row.get("heating_threshold_f"),
                    "cooling_threshold_f": row.get("cooling_threshold_f"),
                    "target_hvac_mode": row.get("target_hvac_mode"),
                    "current_hvac_state": row.get("current_hvac_state"),
                    "model": row.get("model"),
                    "manufacturer": row.get("manufacturer"),
                },
            }
        )
    return {"devices": devices, "message": f"Ecobee HomeKit returned {len(devices)} thermostats."}


def save_integration_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    saved = save_ecobee_homekit_settings(
        alias=(payload or {}).get("ecobee_homekit_alias"),
        device_id=(payload or {}).get("ecobee_homekit_device_id"),
        discovery_timeout_seconds=(payload or {}).get("ecobee_homekit_discovery_timeout_seconds"),
    )
    return {
        "ecobee_homekit_alias": saved.get("ECOBEE_HOMEKIT_ALIAS") or ECOBEE_HOMEKIT_DEFAULT_ALIAS,
        "ecobee_homekit_setup_code": "",
        "ecobee_homekit_device_id": saved.get("ECOBEE_HOMEKIT_DEVICE_ID", ""),
        "ecobee_homekit_discovery_timeout_seconds": int(
            saved.get("ECOBEE_HOMEKIT_DISCOVERY_TIMEOUT_SECONDS") or ECOBEE_HOMEKIT_DEFAULT_TIMEOUT_SECONDS
        ),
    }


def _accessory_summary(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "No HomeKit accessories were found."
    pieces = []
    for row in rows[:5]:
        paired = "paired" if row.get("paired") else "unpaired"
        pieces.append(f"{row.get('name') or 'HomeKit device'} ({row.get('category') or 'device'}, {row.get('id')}, {paired})")
    suffix = "" if len(rows) <= 5 else f" and {len(rows) - 5} more"
    return "Found " + ", ".join(pieces) + suffix + "."


def run_integration_action(action_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    action = _text(action_id)
    if payload:
        save_integration_settings(payload)
    current = read_integration_settings()
    alias = current.get("ecobee_homekit_alias") or ECOBEE_HOMEKIT_DEFAULT_ALIAS
    timeout = current.get("ecobee_homekit_discovery_timeout_seconds") or ECOBEE_HOMEKIT_DEFAULT_TIMEOUT_SECONDS

    if action == "discover":
        rows = discover_homekit_accessories(timeout)
        values: Dict[str, Any] = {}
        candidates = [row for row in rows if not row.get("paired") and _is_ecobee_candidate(row)]
        if len(candidates) == 1:
            values["ecobee_homekit_device_id"] = candidates[0].get("id", "")
            save_ecobee_homekit_settings(device_id=candidates[0].get("id", ""))
        return {
            "ok": True,
            "accessory_count": len(rows),
            "accessories": rows,
            "values": values,
            "message": _accessory_summary(rows),
        }

    if action == "pair":
        result = pair_ecobee_homekit(
            alias=alias,
            setup_code=(payload or {}).get("ecobee_homekit_setup_code"),
            device_id=current.get("ecobee_homekit_device_id"),
            timeout_seconds=timeout,
        )
        device = result.get("device") if isinstance(result.get("device"), dict) else {}
        name = device.get("name") or device.get("id") or alias
        return {
            "ok": True,
            "paired": True,
            "device": device,
            "values": {
                "ecobee_homekit_alias": alias,
                "ecobee_homekit_setup_code": "",
                "ecobee_homekit_device_id": device.get("id") or current.get("ecobee_homekit_device_id", ""),
                "ecobee_homekit_discovery_timeout_seconds": timeout,
            },
            "message": f"Ecobee HomeKit pairing established for {name}.",
        }

    if action == "list_thermostats":
        rows = list_homekit_thermostats(alias)
        names = ", ".join(row.get("name") or row.get("id") for row in rows) or "none"
        return {
            "ok": True,
            "thermostat_count": len(rows),
            "thermostats": rows,
            "message": f"Found {len(rows)} HomeKit thermostat{'s' if len(rows) != 1 else ''}: {names}.",
        }

    if action == "forget_pairing":
        removed = forget_ecobee_homekit_pairing(alias)
        return {
            "ok": True,
            "removed": removed,
            "message": f"Forgot saved Ecobee HomeKit pairing for {alias}." if removed else f"No saved Ecobee HomeKit pairing existed for {alias}.",
        }

    raise KeyError(f"Unsupported Ecobee HomeKit action: {action_id}")
