from __future__ import annotations

from typing import Any, Dict, List

from helpers import redis_client
from integration_registry import (
    get_integration_device_registry,
    run_integration_device_action,
)
from little_spud_home import (
    build_satellite_climate_snapshot,
    home_action_payload,
    resolve_home_action_targets,
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
        preferred_room=preferred_room,
        maximum_rooms=maximum_rooms,
    )


def run_climate_action(payload: Dict[str, Any], *, client: Any = None) -> Dict[str, Any]:
    action = _text(payload.get("action")).lower()
    if action not in {"set_temperature", "set_hvac_mode"}:
        raise ValueError("Unsupported climate action.")
    registry = get_integration_device_registry(client or redis_client)
    targets = resolve_home_action_targets(
        registry,
        room_id=payload.get("room_id"),
        category_id="climate",
        action=action,
    )
    action_payload = home_action_payload(
        action,
        payload.get("value"),
        mode=payload.get("mode"),
        temperature_unit=payload.get("temperature_unit") or "F",
    )
    succeeded = 0
    failures: List[str] = []
    for device in targets:
        integration_id = _text(device.get("integration_id"))
        device_id = _text(device.get("id") or device.get("ref"))
        if not integration_id or not device_id:
            failures.append("A thermostat is missing its integration target.")
            continue
        try:
            result = run_integration_device_action(integration_id, action, device_id, action_payload)
            if isinstance(result, dict) and result.get("ok") is False:
                failures.append(_text(result.get("error")) or "The thermostat rejected the action.")
            else:
                succeeded += 1
        except Exception as exc:
            failures.append(_text(exc) or "The thermostat action failed.")
    if succeeded <= 0:
        raise RuntimeError(failures[0] if failures else "No thermostat accepted the action.")
    return {
        "ok": True,
        "action": action,
        "succeeded": succeeded,
        "failed": len(failures),
        "warning": failures[0] if failures else "",
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
