from __future__ import annotations

from collections import Counter, OrderedDict
import re
import threading
import time
from typing import Any, Dict, Iterable, List, Tuple


MAX_BATCH_ADVERTS = 32
MAX_OBSERVATIONS = 1024
DEFAULT_MAX_AGE_S = 300.0
OBSERVATION_TTL_S = 900.0
MAX_DATA_BYTES = 31

_ADDRESS_RE = re.compile(r"^(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$")
_HEX_RE = re.compile(r"^[0-9a-fA-F]*$")
_lock = threading.RLock()
_observations: "OrderedDict[Tuple[str, str, int], Dict[str, Any]]" = OrderedDict()
_source_stats: Dict[str, Dict[str, Any]] = {}
_revision = 0


def _text(value: Any) -> str:
    return str(value or "").strip()


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
    service_uuids: List[str] = []
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
        elif field_type in {0x02, 0x03}:
            for index in range(0, len(value) - 1, 2):
                service_uuids.append(f"{int.from_bytes(value[index:index + 2], 'little'):04x}")
        elif field_type == 0x16 and len(value) >= 2:
            service_uuids.append(f"{int.from_bytes(value[:2], 'little'):04x}")
        offset = field_end

    decoded: Dict[str, Any] = {}
    if advertised_name:
        decoded["advertised_name"] = advertised_name
    if manufacturer_id is not None:
        decoded["manufacturer_id"] = manufacturer_id
        decoded["manufacturer_id_hex"] = f"0x{manufacturer_id:04X}"
    if service_uuids:
        decoded["service_uuids"] = list(dict.fromkeys(service_uuids))
    return decoded


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
    while len(_observations) > MAX_OBSERVATIONS:
        _observations.popitem(last=False)


def ingest_advertisements(
    selector: Any,
    payload: Any,
    *,
    metadata: Dict[str, Any] | None = None,
    received_ts: float | None = None,
) -> Dict[str, Any]:
    global _revision
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
        for advert in accepted:
            key = (token, advert["address"], int(advert["event_type"]))
            _observations[key] = advert
            _observations.move_to_end(key)
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


def _presence_rows(observations: Iterable[Dict[str, Any]], *, now_ts: float) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in observations:
        grouped.setdefault(_text(row.get("address")), []).append(row)
    devices: List[Dict[str, Any]] = []
    for address, rows in grouped.items():
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
            source_row["age_s"] = round(age_s, 3)
            source_row["location_score"] = round(rssi - min(48.0, age_s * 2.0), 2)
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
        score_gap = (
            float(strongest.get("location_score") or -999.0)
            - float(sources[1].get("location_score") or -999.0)
            if len(sources) > 1
            else 99.0
        )
        confidence = "high" if score_gap >= 10.0 else "medium" if score_gap >= 4.0 else "low"
        devices.append(
            {
                "address": address,
                "display_name": advertised_name or f"BLE {address[-8:].upper()}",
                "advertised_name": advertised_name,
                "manufacturer_id": manufacturer_id,
                "manufacturer_id_hex": manufacturer_id_hex,
                "service_uuids": service_uuids,
                "strongest_selector": _text(strongest.get("selector")),
                "strongest_room": _text(strongest.get("room")) or "Unknown",
                "strongest_rssi": int(strongest.get("rssi") or -127),
                "signal": _signal_label(int(strongest.get("rssi") or -127)),
                "confidence": confidence,
                "last_seen_ts": last_seen_ts,
                "last_seen_age_s": round(last_seen_age_s, 3),
                "present": last_seen_age_s <= 15.0,
                "sources": [
                    {
                        "selector": _text(item.get("selector")),
                        "device_name": _text(item.get("device_name")),
                        "room": _text(item.get("room")),
                        "rssi": int(item.get("rssi") or -127),
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


def snapshot(
    *,
    selector: Any = "",
    address: Any = "",
    limit: int = 200,
    max_age_s: float = DEFAULT_MAX_AGE_S,
    include_observations: bool = True,
    now_ts: float | None = None,
) -> Dict[str, Any]:
    token = _text(selector)
    address_token = _text(address).lower()
    now = float(now_ts if now_ts is not None else time.time())
    try:
        age_limit = max(1.0, min(OBSERVATION_TTL_S, float(max_age_s)))
    except (TypeError, ValueError, OverflowError):
        age_limit = DEFAULT_MAX_AGE_S
    row_limit = max(1, min(500, int(limit or 200)))

    with _lock:
        _prune_locked(now)
        revision = _revision
        rows = [
            dict(row)
            for row in reversed(_observations.values())
            if (not token or _text(row.get("selector")) == token)
            and (not address_token or _text(row.get("address")) == address_token)
            and now - float(row.get("received_ts") or 0.0) <= age_limit
        ][:row_limit]
        sources = [
            dict(row)
            for key, row in sorted(_source_stats.items())
            if not token or key == token
        ]

    devices = _presence_rows(rows, now_ts=now)
    rooms = dict(Counter(_text(row.get("strongest_room")) or "Unknown" for row in devices))

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
        "max_age_s": age_limit,
    }


def reset_for_tests() -> None:
    global _revision
    with _lock:
        _observations.clear()
        _source_stats.clear()
        _revision = 0
