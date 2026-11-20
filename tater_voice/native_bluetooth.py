"""Bounded Bluetooth discovery and saved proxy bonds; no room presence tracking."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from pathlib import Path
import re
import secrets
import tempfile
import threading
import time
import uuid
from typing import Any

_lock = threading.RLock()
_observations: dict[tuple[str, str], dict[str, Any]] = {}
_scan_until: dict[str, float] = {}
_status: dict[str, dict[str, Any]] = {}
_bonds: dict[str, dict[str, Any]] | None = None
MAX_OBSERVATIONS = 500


def address(value: Any) -> str:
    raw = re.sub(r"[^0-9a-fA-F]", "", str(value or ""))
    if len(raw) != 12:
        raise ValueError("Choose a valid Bluetooth address")
    return ":".join(raw[i:i+2] for i in range(0, 12, 2)).lower()


def _bond_path() -> Path:
    return Path(os.environ.get("TATER_BLUETOOTH_STATE_PATH") or Path.home() / ".taterassistant" / "bluetooth_devices.json")


def _load_bonds() -> dict[str, dict[str, Any]]:
    global _bonds
    if _bonds is None:
        try:
            saved = json.loads(_bond_path().read_text())
            if not isinstance(saved, dict):
                raise ValueError("Invalid Bluetooth device registry")
            _bonds = saved
        except FileNotFoundError:
            _bonds = {}
    return _bonds


def _save_bonds(rows: dict[str, dict[str, Any]]) -> None:
    global _bonds
    path = _bond_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".bluetooth-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(rows, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _bonds = rows
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)


def _decode(data: str) -> tuple[str, list[str]]:
    try:
        raw = bytes.fromhex(data)
    except ValueError:
        return "", []
    name, services, offset = "", [], 0
    while offset < len(raw):
        size = raw[offset]
        if not size or offset + size >= len(raw):
            break
        kind, value = raw[offset + 1], raw[offset + 2:offset + size + 1]
        if kind in (8, 9):
            name = value.decode("utf-8", errors="replace")
        width = 2 if kind in (2, 3) else 16 if kind in (6, 7) else 0
        if width:
            for start in range(0, len(value) - width + 1, width):
                item = value[start:start + width]
                services.append(f"0000{int.from_bytes(item, 'little'):04x}-0000-1000-8000-00805f9b34fb" if width == 2 else str(uuid.UUID(bytes=item[::-1])))
        offset += size + 1
    return name, services


def ingest_advertisements(selector: str, payload: dict[str, Any], *, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    now = time.time()
    metadata = metadata or {}
    with _lock:
        # Old firmware must not silently keep the removed presence service alive.
        if _scan_until.get(selector, 0) <= now:
            return {"scanning": False, "observations": 0}
        adverts = payload.get("adverts")
        if not isinstance(adverts, list):
            return {"scanning": True, "observations": len(_observations)}
        for item in adverts[:100]:
            if not isinstance(item, dict):
                continue
            try:
                addr = address(item.get("address"))
                rssi = int(item.get("rssi", -127))
                addr_type = int(item.get("address_type", 0))
            except (ValueError, TypeError):
                continue
            if not -127 <= rssi <= 20 or addr_type not in (0, 1):
                continue
            raw = str(item.get("data") or "")[:1024]
            name, services = _decode(raw)
            decoded_services = item.get("service_uuids")
            if isinstance(decoded_services, list):
                for service in decoded_services[:64]:
                    try:
                        services.append(str(uuid.UUID(str(service))))
                    except (ValueError, TypeError, AttributeError):
                        continue
            _observations[(selector, addr)] = {
                "selector": selector, "address": addr, "address_type": addr_type,
                "rssi": rssi, "data": raw, "received_ts": now,
                "advertised_name": str(item.get("name") or name)[:128],
                "service_uuids": list(dict.fromkeys(services))[:64],
                "device_name": str(metadata.get("device_name") or selector),
                "room": str(metadata.get("room") or ""),
            }
        stale = sorted(_observations, key=lambda key: _observations[key]["received_ts"], reverse=True)[MAX_OBSERVATIONS:]
        for key in stale:
            _observations.pop(key, None)
        return {"scanning": True, "observations": len(_observations)}


def ingest_event(selector: str, payload: dict[str, Any]) -> None:
    if payload.get("t") in {"slots", "status"}:
        with _lock:
            _status[selector] = {key: payload[key] for key in ("enabled", "scanning", "free", "limit", "addrs") if key in payload}


def disconnected(selector: str) -> None:
    """Retire discovery and volatile status; saved bonds remain intact."""
    with _lock:
        _scan_until.pop(selector, None)
        _status.pop(selector, None)


def snapshot(*, selector: str = "", max_age_s: float = 60, include_observations: bool = True, limit: int = 500, **_unused: Any) -> dict[str, Any]:
    from . import native_satellite
    clients = native_satellite.status_snapshot_sync().get("clients", {})
    now = time.time()
    with _lock:
        rows = [dict(row) for row in _observations.values() if now - row["received_ts"] <= min(120, max(1, max_age_s)) and (not selector or row["selector"] == selector)]
        rows.sort(key=lambda row: row["rssi"], reverse=True)
        devices: dict[str, dict[str, Any]] = {}
        for row in rows:
            if row["address"] not in devices:
                devices[row["address"]] = {**row, "strongest_selector": row["selector"], "strongest_rssi": row["rssi"]}
        scanners = []
        for key, client in clients.items():
            caps = client.get("capabilities") or {}
            if caps.get("ble_gatt") and client.get("connected") and (not selector or key == selector):
                scanners.append({"selector": key, "name": client.get("name") or client.get("device_name") or key,
                                 "on_demand": bool(caps.get("ble_on_demand")),
                                 **_status.get(key, {}), "scanning": _scan_until.get(key, 0) > now,
                                 "remaining_s": max(0, int(_scan_until.get(key, 0) - now))})
        return {"scanners": scanners, "devices": list(devices.values())[:limit],
                "observations": rows[:limit] if include_observations else [],
                "paired": list(_load_bonds().values()), "scanning": any(row["scanning"] for row in scanners)}


async def request(selector: str, action: str, **payload: Any) -> dict[str, Any]:
    from . import native_satellite
    result = await native_satellite.send_request(selector, "ble.gatt", {"t": action, "req": secrets.randbelow(2**31 - 1) + 1, **payload}, timeout_s=42 if action in {"connect", "pair"} else 12)
    if not result.get("ok") and not (action == "connect" and result.get("error") == "already_connected"):
        raise RuntimeError(str(result.get("detail") or result.get("error") or "Bluetooth request failed"))
    return result


async def scan(*, selector: str = "", duration_s: int = 15) -> dict[str, Any]:
    if not isinstance(duration_s, int) or isinstance(duration_s, bool) or not 5 <= duration_s <= 60:
        raise ValueError("Discovery must last 5–60 seconds")
    scanners = [row for row in snapshot(selector=selector)["scanners"] if row["on_demand"]]
    if not scanners:
        raise ValueError("Connect a satellite with updated Bluetooth proxy firmware")
    errors = []
    for row in scanners:
        key = row["selector"]
        with _lock:
            _scan_until[key] = time.time() + duration_s
            for item in [item for item in _observations if item[0] == key]:
                _observations.pop(item, None)
        try:
            await request(key, "scan", duration_s=duration_s)
        except Exception as exc:
            with _lock:
                _scan_until.pop(key, None)
            errors.append(str(exc))
    if len(errors) == len(scanners):
        raise RuntimeError("; ".join(errors))
    return {"ok": True, "warnings": errors, **snapshot(selector=selector)}


async def stop_scan(selector: str = "") -> dict[str, Any]:
    with _lock:
        keys = [key for key in _scan_until if not selector or key == selector]
    results = await asyncio.gather(*(request(key, "stop_scan") for key in keys), return_exceptions=True)
    with _lock:
        for key, result in zip(keys, results):
            if not isinstance(result, BaseException):
                _scan_until.pop(key, None)
    errors = [str(value) for value in results if isinstance(value, BaseException)]
    if errors:
        raise RuntimeError("; ".join(errors))
    return {"ok": True}


async def pair(selector: str, address_value: Any, pin: Any) -> dict[str, Any]:
    addr, pin = address(address_value), str(pin or "")
    if len(pin) != 6 or not pin.isascii() or not pin.isdigit():
        raise ValueError("Enter the device's six-digit PIN")
    with _lock:
        nearby = dict(_observations.get((selector, addr)) or {})
    if not nearby or time.time() - nearby["received_ts"] > 120:
        raise ValueError("Scan for this device through the selected satellite first")
    result = await request(selector, "connect", addr=addr, addr_type=nearby["address_type"])
    opened = result.get("error") != "already_connected"
    try:
        result = await request(selector, "pair", addr=addr, pin=pin)
        if not all(result.get(key) is True for key in ("bonded", "encrypted", "authenticated")):
            raise RuntimeError("Pairing did not establish an authenticated encrypted bond")
        with _lock:
            record = {"selector": selector, "address": addr, "name": nearby["advertised_name"] or addr, "satellite_name": nearby["device_name"]}
            _save_bonds({**_load_bonds(), f"{selector}|{addr}": record})
        return {"ok": True, "device": record}
    finally:
        if opened:
            with contextlib.suppress(Exception):
                await request(selector, "disconnect", addr=addr)


async def unpair(selector: str, address_value: Any) -> dict[str, Any]:
    addr = address(address_value)
    await request(selector, "forget", addr=addr)
    with _lock:
        rows = dict(_load_bonds())
        rows.pop(f"{selector}|{addr}", None)
        _save_bonds(rows)
    return {"ok": True}
