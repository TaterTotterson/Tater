from __future__ import annotations

import importlib
import pkgutil
from typing import Any, Dict, List


INTEGRATION_PACKAGE = "integrations"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _integration_modules() -> List[Any]:
    package = importlib.import_module(INTEGRATION_PACKAGE)
    modules: List[Any] = []
    package_paths = getattr(package, "__path__", None)
    if package_paths is None:
        return modules

    for item in pkgutil.iter_modules(package_paths):
        name = _text(item.name)
        if not name or name.startswith("_"):
            continue
        module = importlib.import_module(f"{INTEGRATION_PACKAGE}.{name}")
        definition = getattr(module, "INTEGRATION", None)
        if isinstance(definition, dict) and _text(definition.get("id")):
            modules.append(module)
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

    return {
        "id": integration_id,
        "name": _text(definition.get("name")) or integration_id.replace("_", " ").title(),
        "description": _text(definition.get("description")),
        "badge": _text(definition.get("badge")) or integration_id[:3].upper(),
        "order": int(definition.get("order") or 1000),
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
    device_type = _text(source.get("type") or source.get("kind") or source.get("domain") or "device")
    return {
        "integration_id": integration_id,
        "id": device_id or name,
        "name": name or device_id or "Device",
        "type": device_type or "device",
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
