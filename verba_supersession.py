from __future__ import annotations

from typing import Any, Callable, Dict, Optional


LEGACY_VERBA_REPLACEMENTS: Dict[str, str] = {
    "aladdin_connect": "garage_door_control",
    "ecobee_homekit_thermostat": "climate_control",
    "ha_climate": "climate_control",
    "ha_covers": "cover_control",
    "ha_fans": "fan_control",
    "ha_lights": "light_control",
    "ha_locks": "lock_control",
    "ha_remotes": "remote_control",
    "ha_scenes": "scene_control",
    "ha_scripts": "script_control",
    "ha_sensors": "sensor_status",
    "ha_switches": "switch_control",
    "ha_temperature": "temperature_status",
    "hue_lights": "light_control",
    "shelly_covers": "cover_control",
    "shelly_lights": "light_control",
    "shelly_sensors": "sensor_status",
    "shelly_switches": "switch_control",
    "unifi_network_clients": "presence_status",
    "unifi_network_devices": "network_device_status",
    "unifi_protect_camera": "camera_control",
    "unifi_protect_camera_info": "camera_control",
    "unifi_protect_sensors": "sensor_status",
}


def replacement_for_verba(verba_id: Any, plugin: Any = None) -> str:
    explicit = str(getattr(plugin, "superseded_by", "") or "").strip() if plugin is not None else ""
    if explicit:
        return explicit
    return LEGACY_VERBA_REPLACEMENTS.get(str(verba_id or "").strip(), "")


def is_verba_superseded(
    verba_id: Any,
    plugin: Any,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]] = None,
) -> bool:
    replacement = replacement_for_verba(verba_id, plugin)
    if not replacement or replacement == str(verba_id or "").strip():
        return False
    if replacement not in (registry or {}):
        return False
    if callable(enabled_predicate) and not enabled_predicate(replacement):
        return False
    return True
