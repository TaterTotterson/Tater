from __future__ import annotations

import base64
import binascii
from collections import Counter, OrderedDict
import contextlib
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import threading
import time
import uuid
from typing import Any, Dict, Iterable, List, Tuple


MAX_BATCH_ADVERTS = 32
MAX_OBSERVATIONS = 1024
DEFAULT_MAX_AGE_S = 300.0
OBSERVATION_TTL_S = 900.0
MAX_DATA_BYTES = 31
RSSI_SMOOTHING_ALPHA = 0.25
LOCATION_FRESHNESS_GRACE_S = 30.0
LOCATION_FRESHNESS_PENALTY_DB_PER_S = 1.0
ROOM_SWITCH_MARGIN_DB = 8.0
ROOM_SWITCH_DWELL_S = 8.0
ROOM_CURRENT_STALE_S = 45.0
ROOM_CHALLENGER_MAX_AGE_S = 15.0
ROOM_MIN_CHALLENGER_OBSERVATIONS = 2
ROOM_STALE_SWITCH_DWELL_S = 3.0
MAX_HISTORY_EVENTS = 2000
MAX_TRACKED_DEVICES = 512
MAX_IDENTITIES_PER_DEVICE = 16
MAX_SCANNER_CALIBRATIONS = 256
MAX_IDENTITY_CACHE = 2048
DEFAULT_HISTORY_LIMIT = 80
PERSISTENCE_VERSION = 2
TRACKER_REFRESH_INTERVAL_S = 1.0

DEFAULT_SETTINGS: Dict[str, Any] = {
    "reference_power": -59.0,
    "attenuation": 2.4,
    "home_timeout_s": 180.0,
    "max_home_radius_m": 35.0,
    "max_room_radius_m": 18.0,
    "history_days": 30,
}

_ADDRESS_RE = re.compile(r"^(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$")
_HEX_RE = re.compile(r"^[0-9a-fA-F]*$")
_lock = threading.RLock()
_observations: "OrderedDict[Tuple[str, str, int], Dict[str, Any]]" = OrderedDict()
_source_stats: Dict[str, Dict[str, Any]] = {}
_signal_state: Dict[Tuple[str, str], Dict[str, float]] = {}
_room_assignments: Dict[str, Dict[str, Any]] = {}
_registry_devices: Dict[str, Dict[str, Any]] = {}
_scanner_settings: Dict[str, Dict[str, Any]] = {}
_presence_settings: Dict[str, Any] = dict(DEFAULT_SETTINGS)
_tracker_states: Dict[str, Dict[str, Any]] = {}
_history: List[Dict[str, Any]] = []
_identity_cache: Dict[str, Tuple[str, str, float]] = {}
_state_loaded = False
_suppress_persistence = False
_revision = 0
_last_tracker_refresh_ts = 0.0
_LOGGER = logging.getLogger(__name__)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _float(value: Any, default: float, *, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        parsed = default
    if not math.isfinite(parsed):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _storage_path() -> Path:
    configured = _text(os.getenv("TATER_NATIVE_PRESENCE_PATH"))
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".taterassistant" / "native_presence.json"


def _default_state() -> Dict[str, Any]:
    return {
        "version": PERSISTENCE_VERSION,
        "settings": dict(DEFAULT_SETTINGS),
        "devices": {},
        "scanners": {},
        "trackers": {},
        "history": [],
    }


def _normalise_settings(values: Any) -> Dict[str, Any]:
    incoming = values if isinstance(values, dict) else {}
    return {
        "reference_power": _float(incoming.get("reference_power"), -59.0, minimum=-100.0, maximum=-20.0),
        "attenuation": _float(incoming.get("attenuation"), 2.4, minimum=1.0, maximum=6.0),
        "home_timeout_s": _float(incoming.get("home_timeout_s"), 180.0, minimum=15.0, maximum=3600.0),
        "max_home_radius_m": _float(incoming.get("max_home_radius_m"), 35.0, minimum=1.0, maximum=250.0),
        "max_room_radius_m": _float(incoming.get("max_room_radius_m"), 18.0, minimum=1.0, maximum=100.0),
        "history_days": int(_float(incoming.get("history_days"), 30.0, minimum=1.0, maximum=365.0)),
    }


def _normalise_irk(value: Any) -> str:
    token = _text(value).replace(" ", "").replace(":", "").replace("-", "")
    if token.lower().startswith("0x"):
        token = token[2:]
    if len(token) == 32 and _HEX_RE.fullmatch(token):
        return token.lower()
    try:
        decoded = base64.b64decode(_text(value), validate=True)
    except (ValueError, binascii.Error):
        return ""
    return decoded.hex() if len(decoded) == 16 else ""


def _normalise_ibeacon_id(value: Any) -> str:
    token = _text(value).lower().replace("_", ":")
    parts = token.split(":")
    if len(parts) < 3:
        return ""
    major_token, minor_token = parts[-2:]
    beacon_uuid = ":".join(parts[:-2]).replace(":", "").replace("-", "")
    if len(beacon_uuid) != 32 or not _HEX_RE.fullmatch(beacon_uuid):
        return ""
    try:
        major = int(major_token)
        minor = int(minor_token)
    except (TypeError, ValueError):
        return ""
    if not 0 <= major <= 65535 or not 0 <= minor <= 65535:
        return ""
    formatted_uuid = (
        f"{beacon_uuid[:8]}-{beacon_uuid[8:12]}-{beacon_uuid[12:16]}-"
        f"{beacon_uuid[16:20]}-{beacon_uuid[20:]}"
    )
    return f"{formatted_uuid}:{major}:{minor}"


def _normalise_identity(value: Any) -> Dict[str, Any] | None:
    row = value if isinstance(value, dict) else {}
    kind = _text(row.get("type") or row.get("kind")).lower()
    raw_value = row.get("value")
    if kind in {"mac", "address"}:
        address = _text(raw_value).lower().replace("-", ":")
        if not _ADDRESS_RE.fullmatch(address):
            return None
        return {"type": "address", "value": address}
    if kind == "ibeacon":
        beacon_id = _normalise_ibeacon_id(raw_value)
        return {"type": "ibeacon", "value": beacon_id} if beacon_id else None
    if kind == "irk":
        irk = _normalise_irk(raw_value)
        return {"type": "irk", "value": irk} if irk else None
    return None


def _normalise_device_record(device_id: str, value: Any) -> Dict[str, Any]:
    row = value if isinstance(value, dict) else {}
    clean_id = _text(device_id or row.get("id")) or f"device-{uuid.uuid4().hex[:12]}"
    identities: List[Dict[str, Any]] = []
    seen = set()
    raw_identities = row.get("identities") if isinstance(row.get("identities"), list) else []
    for identity in raw_identities[:MAX_IDENTITIES_PER_DEVICE]:
        clean = _normalise_identity(identity)
        if clean is None:
            continue
        key = (clean["type"], clean["value"])
        if key in seen:
            continue
        seen.add(key)
        identities.append(clean)
    reference_power = row.get("reference_power")
    max_radius = row.get("max_radius_m")
    timeout = row.get("home_timeout_s")
    return {
        "id": clean_id,
        "name": _text(row.get("name"))[:80] or "Tracked device",
        "owner": _text(row.get("owner"))[:80],
        "category": _text(row.get("category"))[:40] or "device",
        "track": _bool(row.get("track"), True),
        "reference_power": (
            _float(reference_power, -59.0, minimum=-100.0, maximum=-20.0)
            if reference_power not in {None, ""}
            else None
        ),
        "max_radius_m": (
            _float(max_radius, 18.0, minimum=1.0, maximum=100.0)
            if max_radius not in {None, ""}
            else None
        ),
        "home_timeout_s": (
            _float(timeout, 180.0, minimum=15.0, maximum=3600.0)
            if timeout not in {None, ""}
            else None
        ),
        "identities": identities,
        "created_ts": _float(row.get("created_ts"), time.time(), minimum=0.0, maximum=10_000_000_000.0),
        "updated_ts": _float(row.get("updated_ts"), time.time(), minimum=0.0, maximum=10_000_000_000.0),
    }


def _normalise_scanner_settings(values: Any) -> Dict[str, Dict[str, Any]]:
    incoming = values if isinstance(values, dict) else {}
    cleaned: Dict[str, Dict[str, Any]] = {}
    for selector, row in incoming.items():
        if len(cleaned) >= MAX_SCANNER_CALIBRATIONS:
            break
        token = _text(selector)
        if not token:
            continue
        data = row if isinstance(row, dict) else {}
        cleaned[token] = {
            "rssi_offset_db": _float(data.get("rssi_offset_db"), 0.0, minimum=-40.0, maximum=40.0),
        }
    return cleaned


def _ensure_state_loaded_locked() -> None:
    global _state_loaded, _presence_settings, _registry_devices, _scanner_settings, _tracker_states, _history
    if _state_loaded:
        return
    state = _default_state()
    try:
        loaded = json.loads(_storage_path().read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            state.update(loaded)
    except (FileNotFoundError, OSError, ValueError, TypeError):
        pass
    _presence_settings = _normalise_settings(state.get("settings"))
    device_rows = state.get("devices") if isinstance(state.get("devices"), dict) else {}
    _registry_devices = {
        _text(device_id): _normalise_device_record(_text(device_id), row)
        for device_id, row in list(device_rows.items())[:MAX_TRACKED_DEVICES]
        if _text(device_id) and isinstance(row, dict)
    }
    _scanner_settings = _normalise_scanner_settings(state.get("scanners"))
    _tracker_states = {
        _text(device_id): dict(row)
        for device_id, row in (state.get("trackers") or {}).items()
        if _text(device_id) and isinstance(row, dict)
    }
    _history = [dict(row) for row in state.get("history") or [] if isinstance(row, dict)][-MAX_HISTORY_EVENTS:]
    _state_loaded = True


def _save_state_locked() -> None:
    if _suppress_persistence:
        return
    path = _storage_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    payload = {
        "version": PERSISTENCE_VERSION,
        "updated_ts": time.time(),
        "settings": _presence_settings,
        "devices": _registry_devices,
        "scanners": _scanner_settings,
        "trackers": _tracker_states,
        "history": _history[-MAX_HISTORY_EVENTS:],
    }
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    with contextlib.suppress(OSError):
        os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def _int(value: Any, default: int = 0, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError, OverflowError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _source(selector: str, metadata: Dict[str, Any] | None) -> Dict[str, str]:
    values = metadata if isinstance(metadata, dict) else {}
    return {
        "selector": selector,
        "device_id": _text(values.get("device_id")),
        "device_name": _text(values.get("device_name") or values.get("name")),
        "board": _text(values.get("board")),
        "room": _text(values.get("room") or values.get("area_name") or values.get("room_name")),
    }


def _decode_advertisement(data_hex: str) -> Dict[str, Any]:
    try:
        payload = bytes.fromhex(data_hex)
    except ValueError:
        return {}

    advertised_name = ""
    manufacturer_id: int | None = None
    manufacturer_data = ""
    service_uuids: List[str] = []
    tx_power: int | None = None
    ibeacon: Dict[str, Any] | None = None
    offset = 0
    while offset < len(payload):
        field_length = payload[offset]
        if field_length == 0:
            break
        field_end = offset + field_length + 1
        if field_end > len(payload) or field_length < 1:
            break
        field_type = payload[offset + 1]
        value = payload[offset + 2 : field_end]
        if field_type in {0x08, 0x09} and value:
            decoded = value.decode("utf-8", errors="ignore")
            advertised_name = "".join(char for char in decoded if char.isprintable()).strip()[:64]
        elif field_type == 0xFF and len(value) >= 2:
            manufacturer_id = int.from_bytes(value[:2], "little")
            manufacturer_data = value.hex()
            # Apple iBeacon: company id 0x004C, type 0x02, length 0x15,
            # UUID (16 bytes), major, minor and calibrated RSSI at one metre.
            if manufacturer_id == 0x004C and len(value) >= 25 and value[2:4] == b"\x02\x15":
                beacon_uuid_hex = value[4:20].hex()
                beacon_uuid = (
                    f"{beacon_uuid_hex[:8]}-{beacon_uuid_hex[8:12]}-{beacon_uuid_hex[12:16]}-"
                    f"{beacon_uuid_hex[16:20]}-{beacon_uuid_hex[20:]}"
                )
                major = int.from_bytes(value[20:22], "big")
                minor = int.from_bytes(value[22:24], "big")
                measured_power = int.from_bytes(value[24:25], "big", signed=True)
                ibeacon = {
                    "uuid": beacon_uuid,
                    "major": major,
                    "minor": minor,
                    "measured_power": measured_power,
                    "id": f"{beacon_uuid}:{major}:{minor}",
                }
                tx_power = measured_power
        elif field_type in {0x02, 0x03}:
            for index in range(0, len(value) - 1, 2):
                service_uuids.append(f"{int.from_bytes(value[index:index + 2], 'little'):04x}")
        elif field_type == 0x16 and len(value) >= 2:
            service_uuids.append(f"{int.from_bytes(value[:2], 'little'):04x}")
        elif field_type == 0x0A and value:
            tx_power = int.from_bytes(value[:1], "big", signed=True)
        offset = field_end

    decoded: Dict[str, Any] = {}
    if advertised_name:
        decoded["advertised_name"] = advertised_name
    if manufacturer_id is not None:
        decoded["manufacturer_id"] = manufacturer_id
        decoded["manufacturer_id_hex"] = f"0x{manufacturer_id:04X}"
        decoded["manufacturer_data"] = manufacturer_data
    if service_uuids:
        decoded["service_uuids"] = list(dict.fromkeys(service_uuids))
    if tx_power is not None:
        decoded["tx_power"] = tx_power
    if ibeacon is not None:
        decoded["ibeacon"] = ibeacon
        decoded["ibeacon_id"] = ibeacon["id"]
    return decoded


def _identity_maps_locked() -> Dict[str, Any]:
    _ensure_state_loaded_locked()
    addresses: Dict[str, str] = {}
    ibeacons: Dict[str, str] = {}
    irks: List[Tuple[str, str]] = []
    for device_id, record in _registry_devices.items():
        for identity in record.get("identities") or []:
            kind = _text(identity.get("type"))
            value = _text(identity.get("value")).lower()
            if kind == "address" and value:
                addresses[value] = device_id
            elif kind == "ibeacon" and value:
                ibeacons[value] = device_id
            elif kind == "irk" and value:
                irks.append((value, device_id))
    return {
        "addresses": addresses,
        "ibeacons": ibeacons,
        "irks": irks,
        "devices": {key: dict(value) for key, value in _registry_devices.items()},
    }


def _irk_resolves_address(irk_hex: str, address: str) -> bool:
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    except Exception:
        return False
    try:
        address_bytes = bytes.fromhex(address.replace(":", ""))
        irk = bytes.fromhex(irk_hex)
    except ValueError:
        return False
    if len(address_bytes) != 6 or len(irk) != 16 or address_bytes[0] >> 6 != 0b01:
        return False
    prand = address_bytes[:3]
    expected_hash = address_bytes[3:]
    encryptor = Cipher(algorithms.AES(irk), modes.ECB()).encryptor()
    encrypted = encryptor.update((b"\x00" * 13) + prand) + encryptor.finalize()
    return encrypted[-3:] == expected_hash


def _resolve_identity(row: Dict[str, Any], maps: Dict[str, Any], *, now_ts: float) -> Dict[str, Any]:
    address = _text(row.get("address")).lower()
    beacon_id = _text(row.get("ibeacon_id")).lower()
    direct_device_id = _text((maps.get("addresses") or {}).get(address))
    beacon_device_id = _text((maps.get("ibeacons") or {}).get(beacon_id)) if beacon_id else ""

    if beacon_device_id:
        device_id = beacon_device_id
        identity_type = "ibeacon"
        identity_key = f"ibeacon:{beacon_id}"
    elif direct_device_id:
        device_id = direct_device_id
        identity_type = "address"
        identity_key = f"address:{address}"
    else:
        cached = _identity_cache.get(address)
        if cached and cached[2] >= now_ts:
            irk_device_id, irk_fingerprint, _ = cached
        else:
            irk_device_id = ""
            irk_fingerprint = ""
            for irk_hex, candidate_device_id in maps.get("irks") or []:
                if _irk_resolves_address(irk_hex, address):
                    irk_device_id = candidate_device_id
                    irk_fingerprint = hashlib.sha256(bytes.fromhex(irk_hex)).hexdigest()[:12]
                    break
            if len(_identity_cache) >= MAX_IDENTITY_CACHE:
                for cached_address, cached_value in list(_identity_cache.items()):
                    if cached_value[2] < now_ts:
                        _identity_cache.pop(cached_address, None)
                while len(_identity_cache) >= MAX_IDENTITY_CACHE:
                    _identity_cache.pop(next(iter(_identity_cache)))
            _identity_cache[address] = (irk_device_id, irk_fingerprint, now_ts + 900.0)
        if irk_device_id:
            device_id = irk_device_id
            identity_type = "irk"
            identity_key = f"irk:{irk_fingerprint}"
        elif beacon_id:
            digest = hashlib.sha256(beacon_id.encode("utf-8")).hexdigest()[:16]
            device_id = f"ibeacon-{digest}"
            identity_type = "ibeacon"
            identity_key = f"ibeacon:{beacon_id}"
        else:
            device_id = f"ble-{address.replace(':', '')}"
            identity_type = "address"
            identity_key = f"address:{address}"

    record = (maps.get("devices") or {}).get(device_id)
    return {
        "presence_id": device_id,
        "identity_type": identity_type,
        "identity_key": identity_key,
        "registered": isinstance(record, dict),
        "registry": dict(record) if isinstance(record, dict) else {},
    }


def _distance_metres(rssi: float, reference_power: float, attenuation: float) -> float:
    exponent = (reference_power - rssi) / (10.0 * max(1.0, attenuation))
    return max(0.01, min(999.0, 10.0 ** exponent))


def _public_identity(identity: Dict[str, Any]) -> Dict[str, Any]:
    kind = _text(identity.get("type"))
    value = _text(identity.get("value"))
    if kind == "irk":
        fingerprint = hashlib.sha256(bytes.fromhex(value)).hexdigest()[:12] if value else ""
        return {
            "type": "irk",
            "configured": bool(value),
            "fingerprint": fingerprint,
            "display": f"IRK ••••{value[-4:].upper()}" if value else "IRK",
        }
    return {"type": kind, "value": value, "display": value}


def _normalise_advert(
    selector: str,
    row: Any,
    *,
    received_ts: float,
    source: Dict[str, str],
    batch_id: int,
) -> Dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    address = _text(row.get("address") or row.get("addr")).lower()
    if not _ADDRESS_RE.fullmatch(address):
        return None
    data = _text(row.get("data") or row.get("data_hex")).lower()
    if len(data) % 2 or len(data) > MAX_DATA_BYTES * 2 or not _HEX_RE.fullmatch(data):
        return None
    age_ms = _int(row.get("age_ms"), 0, minimum=0, maximum=60_000)
    return {
        **source,
        "address": address,
        "address_type": _int(row.get("address_type", row.get("addrType")), 0, minimum=0, maximum=3),
        "rssi": _int(row.get("rssi"), -127, minimum=-127, maximum=20),
        "event_type": _int(row.get("event_type"), 0, minimum=0, maximum=255),
        "data": data,
        "batch_id": batch_id,
        "received_ts": received_ts,
        "observed_ts": max(0.0, received_ts - (age_ms / 1000.0)),
        **_decode_advertisement(data),
    }


def _prune_locked(now_ts: float) -> None:
    cutoff = now_ts - OBSERVATION_TTL_S
    stale = [key for key, row in _observations.items() if float(row.get("received_ts") or 0.0) < cutoff]
    for key in stale:
        _observations.pop(key, None)
    stale_signals = [key for key, row in _signal_state.items() if float(row.get("last_seen_ts") or 0.0) < cutoff]
    for key in stale_signals:
        _signal_state.pop(key, None)
    stale_assignments = [
        key
        for key, row in _room_assignments.items()
        if float(row.get("last_seen_ts") or 0.0) < cutoff
    ]
    for key in stale_assignments:
        _room_assignments.pop(key, None)
    while len(_observations) > MAX_OBSERVATIONS:
        _observations.popitem(last=False)


def ingest_advertisements(
    selector: Any,
    payload: Any,
    *,
    metadata: Dict[str, Any] | None = None,
    received_ts: float | None = None,
) -> Dict[str, Any]:
    global _last_tracker_refresh_ts, _revision
    token = _text(selector)
    body = payload if isinstance(payload, dict) else {}
    rows = body.get("adverts")
    if not token or not isinstance(rows, list):
        return {"ok": False, "selector": token, "accepted": 0, "invalid": 0, "dropped": 0}

    now_ts = float(received_ts if received_ts is not None else time.time())
    batch_id = _int(body.get("batch_id"), 0, minimum=0, maximum=0xFFFFFFFF)
    source = _source(token, metadata)
    accepted: List[Dict[str, Any]] = []
    invalid = 0
    limited_rows = rows[:MAX_BATCH_ADVERTS]
    for row in limited_rows:
        advert = _normalise_advert(
            token,
            row,
            received_ts=now_ts,
            source=source,
            batch_id=batch_id,
        )
        if advert is None:
            invalid += 1
        else:
            accepted.append(advert)
    dropped = max(0, len(rows) - len(limited_rows))

    with _lock:
        identity_maps = _identity_maps_locked()
        for advert in accepted:
            key = (token, advert["address"], int(advert["event_type"]))
            advert.update(_resolve_identity(advert, identity_maps, now_ts=now_ts))
            _observations[key] = advert
            _observations.move_to_end(key)
            signal_key = (_text(advert.get("presence_id")) or advert["address"], token)
            measured_rssi = float(advert.get("rssi") or -127)
            previous_signal = _signal_state.get(signal_key)
            if previous_signal is None or now_ts - float(previous_signal.get("last_seen_ts") or 0.0) > 30.0:
                smoothed_rssi = measured_rssi
            else:
                previous_rssi = float(previous_signal.get("smoothed_rssi") or measured_rssi)
                smoothed_rssi = (
                    previous_rssi * (1.0 - RSSI_SMOOTHING_ALPHA)
                    + measured_rssi * RSSI_SMOOTHING_ALPHA
                )
            _signal_state[signal_key] = {
                "smoothed_rssi": smoothed_rssi,
                "last_seen_ts": now_ts,
            }
        _prune_locked(now_ts)
        stats = _source_stats.setdefault(
            token,
            {
                "selector": token,
                "batches_received": 0,
                "adverts_received": 0,
                "invalid_adverts": 0,
                "dropped_adverts": 0,
                "last_batch_id": 0,
                "last_seen_ts": 0.0,
            },
        )
        stats["batches_received"] = int(stats["batches_received"]) + 1
        stats["adverts_received"] = int(stats["adverts_received"]) + len(accepted)
        stats["invalid_adverts"] = int(stats["invalid_adverts"]) + invalid
        stats["dropped_adverts"] = int(stats["dropped_adverts"]) + dropped
        stats["last_batch_id"] = batch_id
        stats["last_seen_ts"] = now_ts
        stats.update({key: value for key, value in source.items() if key != "selector"})
        _revision += 1
        source_snapshot = dict(stats)
        should_refresh_trackers = (
            now_ts - _last_tracker_refresh_ts >= TRACKER_REFRESH_INTERVAL_S
            and any(_bool(record.get("track"), True) for record in _registry_devices.values())
        )
        if should_refresh_trackers:
            _last_tracker_refresh_ts = now_ts

    if should_refresh_trackers:
        try:
            snapshot(
                limit=500,
                max_age_s=60.0,
                include_observations=False,
                history_limit=0,
                now_ts=now_ts,
            )
        except Exception:
            # Presence bookkeeping must never break the satellite data path.
            _LOGGER.exception("Native presence tracker refresh failed")

    return {
        "ok": True,
        "selector": token,
        "accepted": len(accepted),
        "invalid": invalid,
        "dropped": dropped,
        "source": source_snapshot,
    }


def _signal_label(rssi: int) -> str:
    if rssi >= -55:
        return "excellent"
    if rssi >= -67:
        return "good"
    if rssi >= -78:
        return "fair"
    return "weak"


def _presence_rows(
    observations: Iterable[Dict[str, Any]],
    *,
    now_ts: float,
    signal_state: Dict[Tuple[str, str], Dict[str, float]],
    identity_maps: Dict[str, Any],
    settings: Dict[str, Any],
    scanner_settings: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for source_row in observations:
        row = dict(source_row)
        identity = _resolve_identity(row, identity_maps, now_ts=now_ts)
        row.update(identity)
        grouped.setdefault(_text(identity.get("presence_id")), []).append(row)
    devices: List[Dict[str, Any]] = []
    for presence_id, rows in grouped.items():
        if not presence_id:
            continue
        registry = dict(rows[0].get("registry") or {})
        by_source: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            selector = _text(row.get("selector"))
            previous = by_source.get(selector)
            if previous is None or (
                float(row.get("received_ts") or 0.0),
                int(row.get("rssi") or -127),
            ) > (
                float(previous.get("received_ts") or 0.0),
                int(previous.get("rssi") or -127),
            ):
                by_source[selector] = row
        sources: List[Dict[str, Any]] = []
        for item in by_source.values():
            source_row = dict(item)
            age_s = max(0.0, now_ts - float(source_row.get("received_ts") or 0.0))
            rssi = int(source_row.get("rssi") or -127)
            selector = _text(source_row.get("selector"))
            signal_key = (presence_id, selector)
            smoothed_rssi = float((signal_state.get(signal_key) or {}).get("smoothed_rssi") or rssi)
            scanner_offset = float((scanner_settings.get(selector) or {}).get("rssi_offset_db") or 0.0)
            calibrated_rssi = smoothed_rssi + scanner_offset
            freshness_penalty = min(
                48.0,
                max(0.0, age_s - LOCATION_FRESHNESS_GRACE_S)
                * LOCATION_FRESHNESS_PENALTY_DB_PER_S,
            )
            advertised_power = source_row.get("tx_power")
            configured_reference = registry.get("reference_power")
            reference_power = (
                float(configured_reference)
                if configured_reference not in {None, ""}
                else _float(advertised_power, float(settings["reference_power"]), minimum=-100.0, maximum=-20.0)
                if advertised_power not in {None, ""}
                else float(settings["reference_power"])
            )
            distance_m = _distance_metres(calibrated_rssi, reference_power, float(settings["attenuation"]))
            source_row["age_s"] = round(age_s, 3)
            source_row["smoothed_rssi"] = round(smoothed_rssi, 2)
            source_row["calibrated_rssi"] = round(calibrated_rssi, 2)
            source_row["rssi_offset_db"] = round(scanner_offset, 2)
            source_row["reference_power"] = round(reference_power, 2)
            source_row["distance_m"] = round(distance_m, 2)
            source_row["location_score"] = round(calibrated_rssi - freshness_penalty, 2)
            source_row["signal"] = _signal_label(rssi)
            sources.append(source_row)
        sources.sort(key=lambda item: float(item.get("location_score") or -999.0), reverse=True)
        strongest = sources[0]
        metadata_rows = sorted(rows, key=lambda item: float(item.get("received_ts") or 0.0), reverse=True)
        advertised_name = next((_text(item.get("advertised_name")) for item in metadata_rows if _text(item.get("advertised_name"))), "")
        manufacturer_id = next((item.get("manufacturer_id") for item in metadata_rows if item.get("manufacturer_id") is not None), None)
        manufacturer_id_hex = next((_text(item.get("manufacturer_id_hex")) for item in metadata_rows if _text(item.get("manufacturer_id_hex"))), "")
        service_uuids: List[str] = []
        for item in metadata_rows:
            for service_uuid in item.get("service_uuids") or []:
                token = _text(service_uuid).lower()
                if token and token not in service_uuids:
                    service_uuids.append(token)
        last_seen_ts = max(float(item.get("received_ts") or 0.0) for item in sources)
        last_seen_age_s = max(0.0, now_ts - last_seen_ts)
        addresses = list(dict.fromkeys(_text(item.get("address")).lower() for item in metadata_rows if _text(item.get("address"))))
        address = addresses[0] if addresses else ""
        ibeacon = next((dict(item.get("ibeacon") or {}) for item in metadata_rows if item.get("ibeacon")), {})
        identity_type = _text(metadata_rows[0].get("identity_type")) or "address"
        identities = (
            [_public_identity(identity) for identity in registry.get("identities") or []]
            if registry
            else ([{"type": "ibeacon", "value": _text(ibeacon.get("id")), "display": _text(ibeacon.get("id"))}] if ibeacon else [{"type": "address", "value": address, "display": address}])
        )
        score_gap = (
            float(strongest.get("location_score") or -999.0)
            - float(sources[1].get("location_score") or -999.0)
            if len(sources) > 1
            else 99.0
        )
        confidence = "high" if score_gap >= 10.0 else "medium" if score_gap >= 4.0 else "low"
        devices.append(
            {
                "id": presence_id,
                "presence_id": presence_id,
                "address": address,
                "addresses": addresses,
                "display_name": _text(registry.get("name")) or advertised_name or f"BLE {address[-8:].upper()}",
                "advertised_name": advertised_name,
                "owner": _text(registry.get("owner")),
                "category": _text(registry.get("category")) or ("beacon" if ibeacon else "device"),
                "configured": bool(registry),
                "tracked": _bool(registry.get("track"), False) if registry else False,
                "identity_type": identity_type,
                "identities": identities,
                "ibeacon": ibeacon,
                "manufacturer_id": manufacturer_id,
                "manufacturer_id_hex": manufacturer_id_hex,
                "service_uuids": service_uuids,
                "strongest_selector": _text(strongest.get("selector")),
                "strongest_room": _text(strongest.get("room")) or "Unknown",
                "strongest_rssi": int(strongest.get("rssi") or -127),
                "calibrated_rssi": float(strongest.get("calibrated_rssi") or strongest.get("rssi") or -127),
                "distance_m": float(strongest.get("distance_m") or 0.0),
                "reference_power": float(strongest.get("reference_power") or settings["reference_power"]),
                "reference_power_override": registry.get("reference_power"),
                "attenuation": float(settings["attenuation"]),
                "max_radius_m": float(registry.get("max_radius_m") or settings["max_room_radius_m"]),
                "max_radius_m_override": registry.get("max_radius_m"),
                "home_timeout_s_override": registry.get("home_timeout_s"),
                "signal": _signal_label(int(strongest.get("rssi") or -127)),
                "confidence": confidence,
                "last_seen_ts": last_seen_ts,
                "last_seen_age_s": round(last_seen_age_s, 3),
                "present": last_seen_age_s <= 15.0,
                "home_state": "home" if last_seen_age_s <= float(registry.get("home_timeout_s") or settings["home_timeout_s"]) else "away",
                "sources": [
                    {
                        "selector": _text(item.get("selector")),
                        "device_name": _text(item.get("device_name")),
                        "room": _text(item.get("room")),
                        "rssi": int(item.get("rssi") or -127),
                        "smoothed_rssi": float(item.get("smoothed_rssi") or item.get("rssi") or -127),
                        "calibrated_rssi": float(item.get("calibrated_rssi") or item.get("rssi") or -127),
                        "rssi_offset_db": float(item.get("rssi_offset_db") or 0.0),
                        "distance_m": float(item.get("distance_m") or 0.0),
                        "signal": _text(item.get("signal")),
                        "age_s": float(item.get("age_s") or 0.0),
                        "location_score": float(item.get("location_score") or -999.0),
                        "received_ts": float(item.get("received_ts") or 0.0),
                    }
                    for item in sources
                ],
            }
        )
    devices.sort(key=lambda item: float(item.get("last_seen_ts") or 0.0), reverse=True)
    return devices


def _room_source_map(device: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    room_sources: Dict[str, Dict[str, Any]] = {}
    for source in device.get("sources") or []:
        if not isinstance(source, dict):
            continue
        room = _text(source.get("room")) or "Unknown"
        previous = room_sources.get(room)
        if previous is None or float(source.get("location_score") or -999.0) > float(
            previous.get("location_score") or -999.0
        ):
            room_sources[room] = source
    return room_sources


def _set_stable_room_fields(
    device: Dict[str, Any],
    *,
    assignment: Dict[str, Any],
    room_sources: Dict[str, Dict[str, Any]],
    raw_room: str,
    raw_selector: str,
    now_ts: float,
) -> None:
    stable_room = _text(assignment.get("room")) or raw_room
    selected = room_sources.get(stable_room) or room_sources.get(raw_room)
    if selected is None:
        return
    selected_score = float(selected.get("location_score") or -999.0)
    competing_scores = [
        float(source.get("location_score") or -999.0)
        for room, source in room_sources.items()
        if room != stable_room
    ]
    score_gap = selected_score - max(competing_scores) if competing_scores else 99.0
    device["raw_strongest_room"] = raw_room
    device["raw_strongest_selector"] = raw_selector
    device["strongest_room"] = stable_room
    device["strongest_selector"] = _text(selected.get("selector"))
    device["strongest_rssi"] = int(selected.get("rssi") or -127)
    device["calibrated_rssi"] = float(selected.get("calibrated_rssi") or selected.get("rssi") or -127)
    device["distance_m"] = float(selected.get("distance_m") or 0.0)
    device["signal"] = _signal_label(int(selected.get("rssi") or -127))
    device["confidence"] = "high" if score_gap >= 10.0 else "medium" if score_gap >= 4.0 else "low"
    device["room_changed_ts"] = float(assignment.get("changed_ts") or now_ts)
    max_radius = float(device.get("max_radius_m") or 0.0)
    within_radius = max_radius <= 0.0 or float(device.get("distance_m") or 999.0) <= max_radius
    device["within_room_radius"] = within_radius
    device["location_room"] = stable_room if within_radius else "Unknown"
    candidate_room = _text(assignment.get("candidate_room"))
    if candidate_room:
        device["candidate_room"] = candidate_room
        device["candidate_age_s"] = round(
            max(0.0, now_ts - float(assignment.get("candidate_since_ts") or now_ts)),
            3,
        )


def _stabilize_room_assignments(devices: List[Dict[str, Any]], *, now_ts: float) -> None:
    with _lock:
        for device in devices:
            address = _text(device.get("presence_id") or device.get("id") or device.get("address"))
            raw_room = _text(device.get("strongest_room")) or "Unknown"
            raw_selector = _text(device.get("strongest_selector"))
            room_sources = _room_source_map(device)
            if not address or not room_sources:
                continue

            assignment = _room_assignments.get(address)
            if assignment is None:
                assignment = {
                    "room": raw_room,
                    "changed_ts": now_ts,
                    "candidate_room": "",
                    "candidate_since_ts": 0.0,
                    "candidate_last_seen_ts": 0.0,
                    "candidate_observations": 0,
                    "last_seen_ts": now_ts,
                }
                _room_assignments[address] = assignment

            current_room = _text(assignment.get("room")) or raw_room
            assignment["last_seen_ts"] = now_ts
            if current_room not in room_sources:
                assignment.update(
                    {
                        "room": raw_room,
                        "changed_ts": now_ts,
                        "candidate_room": "",
                        "candidate_since_ts": 0.0,
                        "candidate_last_seen_ts": 0.0,
                        "candidate_observations": 0,
                    }
                )
            elif raw_room == current_room:
                assignment["candidate_room"] = ""
                assignment["candidate_since_ts"] = 0.0
                assignment["candidate_last_seen_ts"] = 0.0
                assignment["candidate_observations"] = 0
            else:
                current_source = room_sources[current_room]
                challenger = room_sources[raw_room]
                score_gap = float(challenger.get("location_score") or -999.0) - float(
                    current_source.get("location_score") or -999.0
                )
                current_stale = float(current_source.get("age_s") or 0.0) >= ROOM_CURRENT_STALE_S
                challenger_fresh = float(challenger.get("age_s") or 0.0) <= ROOM_CHALLENGER_MAX_AGE_S
                qualified = challenger_fresh and (score_gap >= ROOM_SWITCH_MARGIN_DB or current_stale)
                dwell_s = ROOM_STALE_SWITCH_DWELL_S if current_stale else ROOM_SWITCH_DWELL_S
                if not qualified:
                    assignment["candidate_room"] = ""
                    assignment["candidate_since_ts"] = 0.0
                    assignment["candidate_last_seen_ts"] = 0.0
                    assignment["candidate_observations"] = 0
                elif _text(assignment.get("candidate_room")) != raw_room:
                    assignment["candidate_room"] = raw_room
                    assignment["candidate_since_ts"] = now_ts
                    assignment["candidate_last_seen_ts"] = float(challenger.get("received_ts") or now_ts)
                    assignment["candidate_observations"] = 1
                else:
                    challenger_seen_ts = float(challenger.get("received_ts") or 0.0)
                    if challenger_seen_ts > float(assignment.get("candidate_last_seen_ts") or 0.0):
                        assignment["candidate_last_seen_ts"] = challenger_seen_ts
                        assignment["candidate_observations"] = int(assignment.get("candidate_observations") or 0) + 1
                if (
                    qualified
                    and _text(assignment.get("candidate_room")) == raw_room
                    and int(assignment.get("candidate_observations") or 0) >= ROOM_MIN_CHALLENGER_OBSERVATIONS
                    and now_ts - float(assignment.get("candidate_since_ts") or now_ts) >= dwell_s
                ):
                    assignment.update(
                        {
                            "room": raw_room,
                            "changed_ts": now_ts,
                            "candidate_room": "",
                            "candidate_since_ts": 0.0,
                            "candidate_last_seen_ts": 0.0,
                            "candidate_observations": 0,
                        }
                    )

            _set_stable_room_fields(
                device,
                assignment=assignment,
                room_sources=room_sources,
                raw_room=raw_room,
                raw_selector=raw_selector,
                now_ts=now_ts,
            )


def _apply_room_assignments(devices: List[Dict[str, Any]], *, now_ts: float) -> None:
    """Apply canonical assignments without letting API filters change them."""
    with _lock:
        for device in devices:
            address = _text(device.get("presence_id") or device.get("id") or device.get("address"))
            assignment = _room_assignments.get(address)
            room_sources = _room_source_map(device)
            stable_room = _text((assignment or {}).get("room"))
            if not assignment or not stable_room or stable_room not in room_sources:
                continue
            _set_stable_room_fields(
                device,
                assignment=assignment,
                room_sources=room_sources,
                raw_room=_text(device.get("strongest_room")) or "Unknown",
                raw_selector=_text(device.get("strongest_selector")),
                now_ts=now_ts,
            )


def _prune_history_locked(now_ts: float) -> None:
    cutoff = now_ts - (float(_presence_settings.get("history_days") or 30) * 86400.0)
    if len(_history) > MAX_HISTORY_EVENTS or (_history and float(_history[0].get("at") or 0.0) < cutoff):
        retained = [row for row in _history if float(row.get("at") or 0.0) >= cutoff]
        _history[:] = retained[-MAX_HISTORY_EVENTS:]


def _record_event_locked(
    event_type: str,
    *,
    device_id: str,
    name: str,
    at: float,
    from_value: str = "",
    to_value: str = "",
    room: str = "",
    distance_m: float | None = None,
    confidence: str = "",
) -> Dict[str, Any]:
    event = {
        "id": f"presence-{int(at * 1000)}-{uuid.uuid4().hex[:8]}",
        "type": event_type,
        "device_id": device_id,
        "name": name,
        "from": from_value,
        "to": to_value,
        "room": room,
        "distance_m": round(float(distance_m), 2) if distance_m is not None else None,
        "confidence": confidence,
        "at": at,
    }
    _history.append(event)
    _prune_history_locked(at)
    return event


def _update_tracker_states(devices: List[Dict[str, Any]], *, now_ts: float) -> List[Dict[str, Any]]:
    global _revision
    by_id = {_text(device.get("presence_id") or device.get("id")): device for device in devices}
    changed = False
    with _lock:
        _ensure_state_loaded_locked()
        for device_id, record in _registry_devices.items():
            if not _bool(record.get("track"), True):
                continue
            device = by_id.get(device_id)
            previous = dict(_tracker_states.get(device_id) or {})
            timeout = float(record.get("home_timeout_s") or _presence_settings["home_timeout_s"])
            max_home_radius = float(_presence_settings["max_home_radius_m"])
            if device is not None:
                last_seen_ts = float(device.get("last_seen_ts") or 0.0)
                distance_m = float(device.get("distance_m") or 999.0)
                is_home = now_ts - last_seen_ts <= timeout and distance_m <= max_home_radius
                room = _text(device.get("location_room") or device.get("strongest_room")) or "Unknown"
                location = room if is_home else "Away"
                state = "home" if is_home else "away"
                next_state = {
                    "device_id": device_id,
                    "state": state,
                    "location": location,
                    "room": room,
                    "last_seen_ts": last_seen_ts,
                    "last_changed_ts": float(previous.get("last_changed_ts") or now_ts),
                    "distance_m": distance_m,
                    "address": _text(device.get("address")),
                }
            else:
                last_seen_ts = float(previous.get("last_seen_ts") or 0.0)
                is_home = bool(last_seen_ts and now_ts - last_seen_ts <= timeout)
                state = "home" if is_home else "away"
                room = _text(previous.get("room")) or "Unknown"
                location = room if is_home else "Away"
                next_state = {
                    **previous,
                    "device_id": device_id,
                    "state": state,
                    "location": location,
                    "room": room,
                    "last_changed_ts": float(previous.get("last_changed_ts") or now_ts),
                }

            previous_state = _text(previous.get("state"))
            previous_location = _text(previous.get("location"))
            if previous and (previous_state != state or previous_location != location):
                next_state["last_changed_ts"] = now_ts
                if previous_state != state:
                    _record_event_locked(
                        "arrived_home" if state == "home" else "left_home",
                        device_id=device_id,
                        name=_text(record.get("name")) or "Tracked device",
                        from_value=previous_state,
                        to_value=state,
                        room=room,
                        distance_m=next_state.get("distance_m"),
                        confidence=_text((device or {}).get("confidence")),
                        at=now_ts,
                    )
                elif state == "home" and previous_location != location:
                    _record_event_locked(
                        "room_changed",
                        device_id=device_id,
                        name=_text(record.get("name")) or "Tracked device",
                        from_value=previous_location,
                        to_value=location,
                        room=location,
                        distance_m=next_state.get("distance_m"),
                        confidence=_text((device or {}).get("confidence")),
                        at=now_ts,
                    )
                changed = True
            elif not previous:
                next_state["last_changed_ts"] = now_ts
                if state == "home":
                    _record_event_locked(
                        "arrived_home",
                        device_id=device_id,
                        name=_text(record.get("name")) or "Tracked device",
                        from_value="unknown",
                        to_value="home",
                        room=room,
                        distance_m=next_state.get("distance_m"),
                        confidence=_text((device or {}).get("confidence")),
                        at=now_ts,
                    )
                changed = True
            _tracker_states[device_id] = next_state

            if device is not None:
                device["home_state"] = state
                device["location"] = location
                device["location_room"] = location
                device["home_timeout_s"] = timeout
                device["state_changed_ts"] = float(next_state.get("last_changed_ts") or now_ts)
            else:
                identities = [_public_identity(identity) for identity in record.get("identities") or []]
                devices.append(
                    {
                        "id": device_id,
                        "presence_id": device_id,
                        "address": _text(next_state.get("address")),
                        "addresses": [_text(next_state.get("address"))] if _text(next_state.get("address")) else [],
                        "display_name": _text(record.get("name")) or "Tracked device",
                        "owner": _text(record.get("owner")),
                        "category": _text(record.get("category")) or "device",
                        "configured": True,
                        "tracked": True,
                        "identities": identities,
                        "identity_type": _text((identities[0] if identities else {}).get("type")),
                        "strongest_room": room,
                        "location_room": location,
                        "location": location,
                        "strongest_selector": "",
                        "strongest_rssi": -127,
                        "calibrated_rssi": -127.0,
                        "distance_m": next_state.get("distance_m"),
                        "reference_power": float(record.get("reference_power") or _presence_settings["reference_power"]),
                        "reference_power_override": record.get("reference_power"),
                        "max_radius_m": float(record.get("max_radius_m") or _presence_settings["max_room_radius_m"]),
                        "max_radius_m_override": record.get("max_radius_m"),
                        "signal": "weak",
                        "confidence": "unknown",
                        "last_seen_ts": last_seen_ts,
                        "last_seen_age_s": max(0.0, now_ts - last_seen_ts) if last_seen_ts else None,
                        "present": False,
                        "home_state": state,
                        "home_timeout_s": timeout,
                        "home_timeout_s_override": record.get("home_timeout_s"),
                        "state_changed_ts": float(next_state.get("last_changed_ts") or now_ts),
                        "sources": [],
                        "service_uuids": [],
                    }
                )

        if changed:
            _revision += 1
            _save_state_locked()
    devices.sort(
        key=lambda item: (
            0 if _text(item.get("home_state")) == "home" else 1,
            -float(item.get("last_seen_ts") or 0.0),
        )
    )
    return devices


def _public_device_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **{key: value for key, value in record.items() if key != "identities"},
        "identities": [_public_identity(identity) for identity in record.get("identities") or []],
    }


def _irk_available() -> bool:
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher  # noqa: F401
    except Exception:
        return False
    return True


def configuration_snapshot() -> Dict[str, Any]:
    with _lock:
        _ensure_state_loaded_locked()
        return {
            "ok": True,
            "settings": dict(_presence_settings),
            "devices": [_public_device_record(row) for _, row in sorted(_registry_devices.items())],
            "scanners": {key: dict(value) for key, value in sorted(_scanner_settings.items())},
            "capabilities": {
                "ibeacon": True,
                "irk": _irk_available(),
                "calibrated_distance": True,
                "persistent_history": True,
                "native_automations": True,
            },
            "history_count": len(_history),
            "storage": {
                "path": str(_storage_path()),
                "private_permissions": True,
            },
        }


def _merge_identities(existing: Dict[str, Any], incoming: Any) -> List[Dict[str, Any]]:
    previous_irks = [row for row in existing.get("identities") or [] if _text(row.get("type")) == "irk"]
    cleaned: List[Dict[str, Any]] = []
    for row in incoming if isinstance(incoming, list) else []:
        if isinstance(row, dict) and _text(row.get("type")) == "irk" and not _text(row.get("value")):
            fingerprint = _text(row.get("fingerprint"))
            retained = next(
                (
                    item
                    for item in previous_irks
                    if not fingerprint
                    or hashlib.sha256(bytes.fromhex(_text(item.get("value")))).hexdigest()[:12] == fingerprint
                ),
                None,
            )
            if retained:
                cleaned.append(dict(retained))
            continue
        identity = _normalise_identity(row)
        if identity:
            cleaned.append(identity)
    unique: List[Dict[str, Any]] = []
    seen = set()
    for identity in cleaned:
        key = (identity["type"], identity["value"])
        if key not in seen:
            seen.add(key)
            unique.append(identity)
    return unique


def configure(action: Any, payload: Any = None, *, now_ts: float | None = None) -> Dict[str, Any]:
    global _revision
    action_name = _text(action).lower()
    body = payload if isinstance(payload, dict) else {}
    now = float(now_ts if now_ts is not None else time.time())
    with _lock:
        _ensure_state_loaded_locked()
        if action_name == "save_settings":
            _presence_settings.update(_normalise_settings({**_presence_settings, **body}))
        elif action_name == "save_scanner":
            selector = _text(body.get("selector"))
            if not selector:
                raise ValueError("Choose a satellite to calibrate.")
            if selector not in _scanner_settings and len(_scanner_settings) >= MAX_SCANNER_CALIBRATIONS:
                raise ValueError("The satellite calibration limit has been reached.")
            _scanner_settings[selector] = {
                "rssi_offset_db": _float(body.get("rssi_offset_db"), 0.0, minimum=-40.0, maximum=40.0),
            }
        elif action_name == "upsert_device":
            device_id = _text(body.get("id") or body.get("presence_id")) or f"device-{uuid.uuid4().hex[:12]}"
            existing = dict(_registry_devices.get(device_id) or {})
            if not existing and len(_registry_devices) >= MAX_TRACKED_DEVICES:
                raise ValueError("The tracked device limit has been reached.")
            incoming_identities = body.get("identities")
            if not isinstance(incoming_identities, list):
                incoming_identities = existing.get("identities") or []
            identities = _merge_identities(existing, incoming_identities)[:MAX_IDENTITIES_PER_DEVICE]
            if not identities:
                source_address = _text(body.get("address")).lower()
                source_ibeacon = _normalise_ibeacon_id(body.get("ibeacon_id"))
                if source_ibeacon:
                    identities.append({"type": "ibeacon", "value": source_ibeacon})
                elif _ADDRESS_RE.fullmatch(source_address):
                    identities.append({"type": "address", "value": source_address})
            if not identities:
                raise ValueError("Add a BLE address, iBeacon identity, or IRK before saving this device.")
            for other_id, other in _registry_devices.items():
                if other_id == device_id:
                    continue
                other_keys = {(row.get("type"), row.get("value")) for row in other.get("identities") or []}
                if any((row["type"], row["value"]) in other_keys for row in identities):
                    raise ValueError("That Bluetooth identity is already assigned to another tracked device.")
            record = _normalise_device_record(
                device_id,
                {
                    **existing,
                    **body,
                    "id": device_id,
                    "identities": identities,
                    "created_ts": existing.get("created_ts") or now,
                    "updated_ts": now,
                },
            )
            _registry_devices[device_id] = record
            _identity_cache.clear()
            _signal_state.clear()
            _room_assignments.clear()
        elif action_name == "remove_device":
            device_id = _text(body.get("id") or body.get("device_id"))
            if not device_id:
                raise ValueError("Choose a tracked device to remove.")
            _registry_devices.pop(device_id, None)
            _tracker_states.pop(device_id, None)
            _identity_cache.clear()
        elif action_name == "clear_history":
            _history.clear()
        else:
            raise ValueError("Unknown presence configuration action.")
        _revision += 1
        _save_state_locked()
    return configuration_snapshot()


def history_snapshot(*, device_id: Any = "", limit: int = DEFAULT_HISTORY_LIMIT, since_ts: float = 0.0) -> Dict[str, Any]:
    token = _text(device_id)
    row_limit = max(1, min(500, int(limit or DEFAULT_HISTORY_LIMIT)))
    with _lock:
        _ensure_state_loaded_locked()
        rows = [
            dict(row)
            for row in reversed(_history)
            if (not token or _text(row.get("device_id")) == token)
            and float(row.get("at") or 0.0) >= float(since_ts or 0.0)
        ][:row_limit]
    return {"ok": True, "events": rows, "count": len(rows)}


def snapshot(
    *,
    selector: Any = "",
    address: Any = "",
    device_id: Any = "",
    limit: int = 200,
    max_age_s: float = DEFAULT_MAX_AGE_S,
    include_observations: bool = True,
    history_limit: int = DEFAULT_HISTORY_LIMIT,
    now_ts: float | None = None,
) -> Dict[str, Any]:
    token = _text(selector)
    address_token = _text(address).lower()
    device_token = _text(device_id)
    now = float(now_ts if now_ts is not None else time.time())
    try:
        age_limit = max(1.0, min(OBSERVATION_TTL_S, float(max_age_s)))
    except (TypeError, ValueError, OverflowError):
        age_limit = DEFAULT_MAX_AGE_S
    row_limit = max(1, min(500, int(limit or 200)))

    with _lock:
        _ensure_state_loaded_locked()
        _prune_locked(now)
        canonical_rows = [
            dict(row)
            for row in reversed(_observations.values())
            if now - float(row.get("received_ts") or 0.0) <= 60.0
        ]
        rows = [
            dict(row)
            for row in reversed(_observations.values())
            if (not token or _text(row.get("selector")) == token)
            and (not address_token or _text(row.get("address")) == address_token)
            and now - float(row.get("received_ts") or 0.0) <= age_limit
        ][:row_limit]
        sources = [
            {
                **dict(row),
                "rssi_offset_db": float((_scanner_settings.get(key) or {}).get("rssi_offset_db") or 0.0),
            }
            for key, row in sorted(_source_stats.items())
            if not token or key == token
        ]
        signal_state = {key: dict(row) for key, row in _signal_state.items()}
        identity_maps = _identity_maps_locked()
        settings = dict(_presence_settings)
        scanner_settings = {key: dict(value) for key, value in _scanner_settings.items()}

    canonical_devices = _presence_rows(
        canonical_rows,
        now_ts=now,
        signal_state=signal_state,
        identity_maps=identity_maps,
        settings=settings,
        scanner_settings=scanner_settings,
    )
    _stabilize_room_assignments(canonical_devices, now_ts=now)
    devices = _presence_rows(
        rows,
        now_ts=now,
        signal_state=signal_state,
        identity_maps=identity_maps,
        settings=settings,
        scanner_settings=scanner_settings,
    )
    if not token:
        _apply_room_assignments(devices, now_ts=now)
    if not token and not address_token:
        devices = _update_tracker_states(devices, now_ts=now)
    if device_token:
        devices = [row for row in devices if _text(row.get("presence_id") or row.get("id")) == device_token]
    rooms = dict(Counter(_text(row.get("location_room") or row.get("strongest_room")) or "Unknown" for row in devices))
    trackers = Counter(_text(row.get("home_state")) or "nearby" for row in devices if row.get("tracked"))
    with _lock:
        revision = _revision
        history = [
            dict(row)
            for row in reversed(_history)
            if not device_token or _text(row.get("device_id")) == device_token
        ][
            : max(0, min(200, int(history_limit or 0)))
        ]
        registry_count = len(_registry_devices)
        irk_count = sum(
            1
            for record in _registry_devices.values()
            for identity in record.get("identities") or []
            if _text(identity.get("type")) == "irk"
        )
        history_count = len(_history)
        identity_cache_count = len(_identity_cache)

    return {
        "ok": True,
        "revision": revision,
        "generated_ts": now,
        "observations": rows if include_observations else [],
        "count": len(rows),
        "devices": devices,
        "device_count": len(devices),
        "sources": sources,
        "source_count": len(sources),
        "rooms": rooms,
        "trackers": dict(trackers),
        "tracked_count": registry_count,
        "history": history,
        "settings": settings,
        "diagnostics": {
            "identity_registry": registry_count,
            "irk_identities": irk_count,
            "irk_resolution_available": _irk_available(),
            "ibeacons_visible": sum(1 for row in devices if row.get("ibeacon")),
            "calibrated_scanners": sum(1 for row in scanner_settings.values() if float(row.get("rssi_offset_db") or 0.0) != 0.0),
            "bounded_observations": len(rows),
            "observation_capacity": MAX_OBSERVATIONS,
            "history_events": history_count,
            "history_capacity": MAX_HISTORY_EVENTS,
            "identity_cache": identity_cache_count,
            "identity_cache_capacity": MAX_IDENTITY_CACHE,
        },
        "max_age_s": age_limit,
    }


def reset_for_tests() -> None:
    global _last_tracker_refresh_ts, _revision, _state_loaded, _suppress_persistence, _presence_settings
    with _lock:
        _observations.clear()
        _source_stats.clear()
        _signal_state.clear()
        _room_assignments.clear()
        _registry_devices.clear()
        _scanner_settings.clear()
        _tracker_states.clear()
        _history.clear()
        _identity_cache.clear()
        _presence_settings = dict(DEFAULT_SETTINGS)
        _state_loaded = True
        _suppress_persistence = True
        _last_tracker_refresh_ts = 0.0
        _revision = 0
