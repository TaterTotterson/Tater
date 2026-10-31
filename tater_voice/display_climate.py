from __future__ import annotations

from typing import Any, Dict, List

from helpers import redis_client
from integration_registry import (
    get_integration_device_registry,
    run_integration_device_action,
)
from little_spud_home import (
    CONTROL_ACTIONS,
    build_satellite_climate_snapshot,
    home_action_payload,
    select_satellite_thermostat,
)

from . import display_feed


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _room_sensor_refs(profile: Dict[str, Any], maximum_rooms: int) -> List[str]:
    slots = profile.get("slots") if isinstance(profile.get("slots"), dict) else {}
    return [
        _text(slots.get(f"room_temp_{index}"))
        for index in range(1, maximum_rooms + 1)
        if _text(slots.get(f"room_temp_{index}"))
    ]


def build_climate_summary(
    *,
    selector: str = "",
    preferred_room: str = "",
    client: Any = None,
) -> Dict[str, Any]:
    redis_obj = client or redis_client
    profile, _found = display_feed._saved_display_profile_from_query(
        {"selector": selector, "target": selector},
        redis_obj,
    )
    maximum_rooms = 3 if _text(profile.get("screen_target")).lower() == "rook" else 6
    registry = get_integration_device_registry(redis_obj)
    return build_satellite_climate_snapshot(
        registry,
        room_sensor_refs=_room_sensor_refs(profile, maximum_rooms),
        thermostat_ref=_text(profile.get("thermostat")),
        preferred_room=preferred_room,
        maximum_rooms=maximum_rooms,
    )


def run_climate_action(
    payload: Dict[str, Any],
    *,
    selector: str = "",
    preferred_room: str = "",
    client: Any = None,
) -> Dict[str, Any]:
    action = _text(payload.get("action")).lower()
    if action not in {"set_temperature", "set_hvac_mode"}:
        raise ValueError("Unsupported climate action.")
    redis_obj = client or redis_client
    profile, _found = display_feed._saved_display_profile_from_query(
        {"selector": selector, "target": selector},
        redis_obj,
    )
    registry = get_integration_device_registry(redis_obj)
    thermostat = select_satellite_thermostat(
        registry,
        thermostat_ref=_text(profile.get("thermostat")),
        preferred_room=preferred_room or _text(payload.get("room_id")),
    )
    if thermostat is None:
        raise RuntimeError("No thermostat is assigned to this satellite.")
    raw_actions = thermostat.get("actions") or []
    if not isinstance(raw_actions, (list, tuple, set)):
        raw_actions = [raw_actions]
    actions = {
        _text(value).lower().replace("-", "_").replace(" ", "_")
        for value in raw_actions
    }
    if action not in actions or action not in CONTROL_ACTIONS["climate"]:
        raise RuntimeError("The assigned thermostat does not support this action.")
    action_payload = home_action_payload(
        action,
        payload.get("value"),
        mode=payload.get("mode"),
        temperature_unit=payload.get("temperature_unit") or "F",
    )
    integration_id = _text(thermostat.get("integration_id"))
    device_id = _text(thermostat.get("id") or thermostat.get("ref"))
    if not integration_id or not device_id:
        raise RuntimeError("The assigned thermostat is missing its integration target.")
    result = run_integration_device_action(integration_id, action, device_id, action_payload)
    if isinstance(result, dict) and result.get("ok") is False:
        raise RuntimeError(_text(result.get("error")) or "The thermostat rejected the action.")
    return {
        "ok": True,
        "action": action,
        "succeeded": 1,
        "failed": 0,
        "warning": "",
    }


def apply_action_to_summary(summary: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """Keep the display on the accepted value while provider state catches up."""
    updated = dict(summary or {})
    action = _text(payload.get("action")).lower()
    if action == "set_temperature":
        try:
            value = float(payload.get("value"))
        except (TypeError, ValueError):
            return updated
        source_unit = "C" if _text(payload.get("temperature_unit")).upper() == "C" else "F"
        display_unit = "C" if _text(updated.get("temperature_unit")).upper() == "C" else "F"
        if source_unit != display_unit:
            value = value * 9.0 / 5.0 + 32.0 if source_unit == "C" else (value - 32.0) * 5.0 / 9.0
        updated["target_temperature"] = round(value, 1)
    elif action == "set_hvac_mode":
        mode = _text(payload.get("mode")).lower().replace("-", "_").replace(" ", "_")
        if mode:
            updated["hvac_mode"] = mode
    return updated
