"""Narrow, validated thermostat surface for Checkers' local touch screen.

Only an unambiguous, writable thermostat is selectable. The satellite never
receives integration credentials, and every write re-reads the current device.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List

import requests

from tateros import integration_store


def _text(value: Any) -> str:
    return str(value or "").strip()


def _homekit() -> Any:
    return integration_store.integration_module("homekit", auto_restore=False)


def _homeassistant() -> Any:
    return integration_store.integration_module("homeassistant", auto_restore=False)


def _ha_request(method: str, path: str, body: Dict[str, Any] | None = None) -> Any:
    module = _homeassistant()
    if module is None:
        if method != "GET":
            raise ValueError("Home Assistant integration is unavailable")
        return None
    config = module.load_homeassistant_config(required=False)
    base, token = _text(config.get("base")).rstrip("/"), _text(config.get("token"))
    if not base or not token:
        if method != "GET":
            raise ValueError("Home Assistant is not configured")
        return None
    response = requests.request(
        method, base + path,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
        json=body, timeout=5,
    )
    response.raise_for_status()
    return response.json() if response.content else {}


def _ha_rows() -> List[Dict[str, Any]]:
    states = _ha_request("GET", "/api/states")
    if not isinstance(states, list):
        return []
    config = _ha_request("GET", "/api/config") or {}
    unit = _text((config.get("unit_system") or {}).get("temperature")).upper()
    unit = "C" if "C" in unit else "F"
    rows = []
    for state in states:
        if not isinstance(state, dict) or not _text(state.get("entity_id")).startswith("climate."):
            continue
        if _text(state.get("state")).lower() in {"unavailable", "unknown"}:
            continue
        attributes = state.get("attributes") or {}
        if not isinstance(attributes, dict):
            continue
        modes = attributes.get("hvac_modes") or []
        if not isinstance(modes, list):
            modes = []
        rows.append({
            "_source": "ha", "id": _text(state.get("entity_id")),
            "name": _text(attributes.get("friendly_name")) or _text(state.get("entity_id")),
            "temperature_unit": unit,
            "current_temperature_" + unit.lower(): attributes.get("current_temperature"),
            "target_temperature_" + unit.lower(): attributes.get("temperature"),
            "target_hvac_mode": "auto" if state.get("state") == "heat_cool" else _text(state.get("state")),
            "target_mode_writable": bool(modes),
            "target_temperature_writable": attributes.get("temperature") is not None,
            "_hvac_modes": modes,
        })
    return rows


def _rows() -> List[Dict[str, Any]]:
    module = _homekit()
    if module is not None and callable(getattr(module, "list_homekit_thermostats", None)):
        try:
            rows = module.list_homekit_thermostats()
            if isinstance(rows, list) and rows:
                return [{**row, "_source": "homekit"} for row in rows if isinstance(row, dict)]
        except Exception:
            pass
    return _ha_rows()


def _choose(rows: List[Dict[str, Any]], room: str) -> Dict[str, Any] | None:
    if len(rows) == 1:
        return rows[0]
    room = room.casefold().strip()
    if not room:
        return None
    exact = [row for row in rows if _text(row.get("name")).casefold() == room]
    return exact[0] if len(exact) == 1 else None


def _number(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def summary(*, environment_installed: bool, room: str = "") -> Dict[str, Any]:
    if not environment_installed:
        return {"available": False, "message": "Please install Environment Core"}
    try:
        rows = _rows()
    except Exception:
        return {"available": False, "message": "Thermostat unavailable"}
    row = _choose(rows, room)
    if row is None:
        message = "No thermostat connected" if not rows else "Multiple thermostats found"
        return {"available": False, "message": message}
    unit = "C" if _text(row.get("temperature_unit")).upper() == "C" else "F"
    suffix = unit.lower()
    return {
        "available": True,
        "id": _text(row.get("_source")) + ":" + _text(row.get("id")),
        "name": _text(row.get("name")) or "Thermostat",
        "current": _number(row.get("current_temperature_" + suffix)),
        "target": _number(row.get("target_temperature_" + suffix)),
        "unit": unit,
        "mode": _text(row.get("target_hvac_mode")).lower() or "off",
        "writable": bool(row.get("target_mode_writable") or row.get("target_temperature_writable")),
        "mode_writable": bool(row.get("target_mode_writable")),
        "target_writable": bool(row.get("target_temperature_writable")),
    }


def set_target(payload: Dict[str, Any], *, room: str = "", environment_installed: bool) -> Dict[str, Any]:
    if not environment_installed:
        raise ValueError("Environment Core is not installed")
    row = _choose(_rows(), room)
    if row is None:
        raise ValueError("No unambiguous thermostat is available")
    expected_id = _text(row.get("_source")) + ":" + _text(row.get("id"))
    if _text(payload.get("thermostat_id")) != expected_id:
        raise ValueError("Thermostat selection changed")
    mode = _text(payload.get("mode")).lower()
    target = payload.get("target")
    if mode and mode not in {"heat", "cool", "auto", "off"}:
        raise ValueError("Unsupported thermostat mode")
    if target is None and not mode:
        raise ValueError("No thermostat change was requested")
    unit = "C" if _text(row.get("temperature_unit")).upper() == "C" else "F"
    if mode and not row.get("target_mode_writable"):
        raise ValueError("Thermostat mode is read-only")
    if target is not None and not row.get("target_temperature_writable"):
        raise ValueError("Thermostat setpoint is read-only")
    if row.get("_source") == "ha":
        modes = row.get("_hvac_modes") or []
        ha_mode = "heat_cool" if mode == "auto" else mode
        if mode and ha_mode not in modes:
            raise ValueError("Thermostat does not support that mode")
        if target is not None and row.get("target_hvac_mode") == "auto" and not mode:
            raise ValueError("Select heat or cool before changing the setpoint")
    else:
        module = _homekit()
        if module is None:
            raise ValueError("HomeKit integration is unavailable")
    if target is not None:
        if isinstance(target, bool):
            raise ValueError("Invalid target temperature")
        try:
            target = float(target)
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid target temperature") from exc
        low, high = (10, 32) if unit == "C" else (50, 90)
        if not math.isfinite(target) or not low <= target <= high or _text(payload.get("unit")).upper() != unit:
            raise ValueError("Target temperature or unit is out of range")
        if row.get("_source") == "ha":
            body = {"entity_id": row["id"], "temperature": target}
            if mode:
                body["hvac_mode"] = ha_mode
            _ha_request("POST", "/api/services/climate/set_temperature", body)
            result = {"target_hvac_mode": mode or row.get("target_hvac_mode")}
        else:
            result = module.set_homekit_thermostat_temperature(
                target, temperature_unit=unit, mode=mode or None, thermostat_id=row["id"]
            )
    else:
        if row.get("_source") == "ha":
            _ha_request("POST", "/api/services/climate/set_hvac_mode", {"entity_id": row["id"], "hvac_mode": ha_mode})
            result = {"target_hvac_mode": mode}
        else:
            result = module.set_homekit_thermostat_mode(mode, thermostat_id=row["id"])
    return {"ok": True, "thermostat_id": expected_id, "target_hvac_mode": result.get("target_hvac_mode")}
