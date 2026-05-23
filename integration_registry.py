from __future__ import annotations

import importlib
import logging
import os
import pkgutil
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List

from tateros import integration_store as integration_store_module


INTEGRATION_PACKAGE = "integrations"
INTEGRATION_DIR = Path(os.getenv("TATER_INTEGRATION_DIR", "integrations"))
logger = logging.getLogger("integration_registry")
integration_registry_errors: List[str] = []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if value in (None, ""):
        return []
    return [value]


def _normalize_token(value: Any) -> str:
    return _text(value).lower().replace(" ", "_").replace("-", "_")


def _normalize_capabilities(value: Any) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in _as_list(value):
        token = _normalize_token(item)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _infer_device_capabilities(device_type: str, source: Dict[str, Any], details: Dict[str, Any]) -> List[str]:
    capabilities = _normalize_capabilities(source.get("capabilities"))
    for token in (device_type, details.get("device_class"), details.get("resource_type"), details.get("sensor_type")):
        normalized = _normalize_token(token)
        if normalized and normalized not in capabilities:
            capabilities.append(normalized)

    haystack = " ".join(
        _text(value).lower()
        for value in (
            source.get("id"),
            source.get("name"),
            source.get("type"),
            details.get("device_class"),
            details.get("resource_type"),
            details.get("sensor_type"),
        )
    )

    def add(token: str) -> None:
        if token not in capabilities:
            capabilities.append(token)

    sensorish = device_type in {
        "sensor",
        "binary_sensor",
        "contact",
        "entry_sensor",
        "garage",
        "garage_door",
        "thermostat",
        "temperature",
        "humidity",
        "device",
    }
    detail_tokens = {
        _normalize_token(details.get("device_class")),
        _normalize_token(details.get("resource_type")),
        _normalize_token(details.get("sensor_type")),
    }

    if device_type == "camera" or "camera" in haystack:
        add("camera")
        add("snapshot")
    if "doorbell" in haystack or "ring" in haystack:
        add("doorbell")
    if (
        device_type in {"contact", "entry_sensor", "garage_door"}
        or detail_tokens.intersection({"door", "window", "garage", "garage_door", "contact", "opening"})
        or (
            sensorish
            and device_type != "camera"
            and any(token in haystack for token in ("door", "window", "garage", "contact", "opening"))
        )
    ):
        add("entry_sensor")
    if "motion" in haystack:
        add("motion")
    if device_type in {"temperature", "thermostat"} or (
        sensorish and any(token in haystack for token in ("temperature", "temp"))
    ):
        add("temperature")
    if device_type in {"humidity"} or (sensorish and "humidity" in haystack):
        add("humidity")
    return capabilities


def _ensure_import_context() -> None:
    parent = str(INTEGRATION_DIR.resolve().parent)
    if parent and parent not in sys.path:
        sys.path.insert(0, parent)

    package = sys.modules.get(INTEGRATION_PACKAGE)
    if package is not None and not isinstance(package, ModuleType):
        sys.modules.pop(INTEGRATION_PACKAGE, None)
        package = None

    importlib.invalidate_caches()
    if package is None:
        package = importlib.import_module(INTEGRATION_PACKAGE)

    package_paths = getattr(package, "__path__", None)
    if package_paths is not None:
        expected = str(INTEGRATION_DIR.resolve())
        normalized = {str(Path(path).resolve()) for path in package_paths}
        if expected not in normalized:
            package_paths.append(expected)


def _integration_modules() -> List[Any]:
    _ensure_import_context()
    package = importlib.import_module(INTEGRATION_PACKAGE)
    modules: List[Any] = []
    errors: List[str] = []
    package_paths = getattr(package, "__path__", None)
    if package_paths is None:
        return modules

    for item in pkgutil.iter_modules(package_paths):
        name = _text(item.name)
        if not name or name.startswith("_"):
            continue
        if not integration_store_module.get_integration_enabled(name):
            continue
        module_name = f"{INTEGRATION_PACKAGE}.{name}"
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            errors.append(f"{module_name}: {exc}")
            continue
        definition = getattr(module, "INTEGRATION", None)
        if isinstance(definition, dict) and _text(definition.get("id")):
            modules.append(module)
    integration_registry_errors.clear()
    integration_registry_errors.extend(errors)
    if errors:
        logger.warning("Integration registry load issues: %s", "; ".join(errors))
    return modules


def _module_for_integration(integration_id: str) -> Any:
    target = _text(integration_id)
    if not target:
        raise KeyError("Integration id is required.")
    for module in _integration_modules():
        definition = getattr(module, "INTEGRATION", None)
        if isinstance(definition, dict) and _text(definition.get("id")) == target:
            return module
    raise KeyError(f"Unknown integration: {target}")


def _coerce_definition(module: Any) -> Dict[str, Any]:
    definition = dict(getattr(module, "INTEGRATION", {}) or {})
    integration_id = _text(definition.get("id"))
    if not integration_id:
        return {}

    fields = definition.get("fields")
    if not isinstance(fields, list):
        fields = []
    actions = definition.get("actions")
    if not isinstance(actions, list):
        actions = []
    capabilities = _normalize_capabilities(definition.get("capabilities"))

    return {
        "id": integration_id,
        "name": _text(definition.get("name")) or integration_id.replace("_", " ").title(),
        "description": _text(definition.get("description")),
        "badge": _text(definition.get("badge")) or integration_id[:3].upper(),
        "order": int(definition.get("order") or 1000),
        "capabilities": capabilities,
        "fields": [dict(field) for field in fields if isinstance(field, dict)],
        "actions": [dict(action) for action in actions if isinstance(action, dict)],
    }


def _read_values(module: Any) -> Dict[str, Any]:
    reader = getattr(module, "read_integration_settings", None)
    if not callable(reader):
        return {}
    try:
        values = reader()
    except Exception as exc:
        return {"_error": str(exc)}
    return values if isinstance(values, dict) else {}


def _read_status(module: Any) -> Dict[str, Any]:
    reader = getattr(module, "integration_status", None)
    if not callable(reader):
        return {}
    try:
        status = reader()
    except Exception as exc:
        return {"error": str(exc), "message": str(exc)}
    return status if isinstance(status, dict) else {}


def _coerce_device_row(integration_id: str, row: Dict[str, Any]) -> Dict[str, Any]:
    source = dict(row or {})
    details = source.get("details") if isinstance(source.get("details"), dict) else {}
    if not details:
        details = {
            key: value
            for key, value in source.items()
            if key not in {"id", "name", "type", "kind", "status", "state", "area", "details"}
        }
    device_id = _text(source.get("id") or source.get("device_id") or source.get("entity_id") or source.get("mac"))
    name = _text(source.get("name") or source.get("label") or source.get("friendly_name") or device_id)
    device_type = _normalize_token(source.get("type") or source.get("kind") or source.get("domain") or "device")
    ref = _text(source.get("ref") or source.get("resource_ref"))
    if not ref and device_id:
        ref = f"{device_type or 'device'}:{device_id}"
    capabilities = _infer_device_capabilities(device_type, source, details)
    return {
        "integration_id": integration_id,
        "id": device_id or name,
        "name": name or device_id or "Device",
        "type": device_type or "device",
        "ref": ref or device_id or name,
        "capabilities": capabilities,
        "actions": _normalize_capabilities(source.get("actions")),
        "event_sources": [dict(item) for item in _as_list(source.get("event_sources")) if isinstance(item, dict)],
        "status": _text(source.get("status")),
        "state": _text(source.get("state")),
        "area": _text(source.get("area")),
        "details": dict(details),
    }


def _read_devices(module: Any, integration_id: str) -> Dict[str, Any]:
    reader = getattr(module, "integration_devices", None)
    if not callable(reader):
        return {"devices": []}
    try:
        rows = reader()
    except Exception as exc:
        return {"devices": [], "error": str(exc), "message": str(exc)}
    if isinstance(rows, dict):
        raw_rows = rows.get("devices") if isinstance(rows.get("devices"), list) else []
        out = {key: value for key, value in rows.items() if key != "devices"}
    else:
        raw_rows = rows if isinstance(rows, list) else []
        out = {}
    devices = [
        _coerce_device_row(integration_id, row)
        for row in raw_rows
        if isinstance(row, dict)
    ]
    devices.sort(key=lambda item: (_text(item.get("name")).casefold(), _text(item.get("type")).casefold(), _text(item.get("id")).casefold()))
    out["devices"] = devices
    return out


def _device_group(module: Any, definition: Dict[str, Any]) -> Dict[str, Any]:
    integration_id = _text(definition.get("id"))
    status = _read_status(module)
    device_result = _read_devices(module, integration_id)
    devices = device_result.get("devices") if isinstance(device_result.get("devices"), list) else []
    error = _text(device_result.get("error"))
    return {
        "id": integration_id,
        "name": _text(definition.get("name")) or integration_id,
        "badge": _text(definition.get("badge")),
        "order": int(definition.get("order") or 1000),
        "status": status,
        "devices": devices,
        "device_count": len(devices),
        "error": error,
        "message": _text(device_result.get("message")),
    }


def get_integration_catalog() -> List[Dict[str, Any]]:
    catalog: List[Dict[str, Any]] = []
    for module in _integration_modules():
        definition = _coerce_definition(module)
        if not definition:
            continue
        row = dict(definition)
        values = _read_values(module)
        status = _read_status(module)
        if values.get("_error") and not status.get("message"):
            status = {"error": values.get("_error"), "message": values.get("_error")}
        row["values"] = values
        row["status"] = status
        catalog.append(row)
    catalog.sort(key=lambda item: (int(item.get("order") or 1000), _text(item.get("name")).casefold(), _text(item.get("id"))))
    return catalog


def get_integration_devices() -> Dict[str, Any]:
    groups: List[Dict[str, Any]] = []
    total = 0
    errors: List[Dict[str, str]] = []
    for module in _integration_modules():
        definition = _coerce_definition(module)
        if not definition:
            continue
        group = _device_group(module, definition)
        devices = group.get("devices") if isinstance(group.get("devices"), list) else []
        total += len(devices)
        error = _text(group.get("error"))
        if error:
            errors.append({"integration_id": _text(group.get("id")), "name": _text(group.get("name")), "error": error})
        groups.append(group)
    groups.sort(key=lambda item: (int(item.get("order") or 1000), _text(item.get("name")).casefold(), _text(item.get("id"))))
    return {"groups": groups, "total": total, "errors": errors}


def get_integration_device_group(integration_id: str) -> Dict[str, Any]:
    module = _module_for_integration(integration_id)
    definition = _coerce_definition(module)
    if not definition:
        raise KeyError(f"Unknown integration: {_text(integration_id)}")
    group = _device_group(module, definition)
    return {"group": group}


def get_integration_devices_by_capability(capability: str) -> List[Dict[str, Any]]:
    token = _normalize_token(capability)
    out: List[Dict[str, Any]] = []
    if not token:
        return out
    groups = get_integration_devices().get("groups") or []
    for group in groups if isinstance(groups, list) else []:
        devices = group.get("devices") if isinstance(group, dict) else []
        for device in devices if isinstance(devices, list) else []:
            if not isinstance(device, dict):
                continue
            caps = set(_normalize_capabilities(device.get("capabilities")))
            if token in caps:
                row = dict(device)
                row.setdefault("integration_name", _text(group.get("name")) if isinstance(group, dict) else "")
                out.append(row)
    return out


def save_integration_settings(integration_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    module = _module_for_integration(integration_id)
    saver = getattr(module, "save_integration_settings", None)
    if not callable(saver):
        raise KeyError(f"{integration_id} does not support settings saves.")
    values = saver(dict(payload or {}))
    return {
        "ok": True,
        "id": _text(integration_id),
        "values": values if isinstance(values, dict) else _read_values(module),
        "status": _read_status(module),
    }


def run_integration_action(integration_id: str, action_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    module = _module_for_integration(integration_id)
    runner = getattr(module, "run_integration_action", None)
    if not callable(runner):
        raise KeyError(f"{integration_id} does not support actions.")
    result = runner(_text(action_id), dict(payload or {}))
    if result is None:
        result = {}
    if not isinstance(result, dict):
        result = {"result": result}
    result.setdefault("ok", True)
    result.setdefault("id", _text(integration_id))
    result.setdefault("action", _text(action_id))
    result["status"] = _read_status(module)
    return result


def run_integration_device_action(integration_id: str, action_id: str, device_id: str, payload: Dict[str, Any] | None = None) -> Any:
    module = _module_for_integration(integration_id)
    action = _text(action_id)
    device = _text(device_id)
    if not action:
        raise KeyError("Device action is required.")
    if not device:
        raise KeyError("Device id is required.")

    runner = getattr(module, "run_integration_device_action", None)
    if callable(runner):
        return runner(action, device, dict(payload or {}))

    runner = getattr(module, "integration_device_action", None)
    if callable(runner):
        return runner(action, device, dict(payload or {}))

    if action in {"camera_snapshot", "snapshot"}:
        snapshotter = getattr(module, "get_camera_snapshot", None)
        if callable(snapshotter):
            return snapshotter(device)

    raise KeyError(f"{integration_id} does not support device action {action}.")


def get_integration_registry_errors() -> List[str]:
    return list(integration_registry_errors)
