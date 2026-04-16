from __future__ import annotations

import base64
import contextlib
import mimetypes
import os
import time
from typing import Any, Dict, List

from . import runtime as esphome_runtime

_asset_data_url_cache: Dict[str, str] = {}


def _vp():
    from . import voice_pipeline as vp

    return vp


def _as_float(value: Any, default: float = 0.0) -> float:
    return _vp()._as_float(value, default)


def _satellite_area_name(row: Any) -> str:
    data = row if isinstance(row, dict) else {}
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    for key in ("area_name", "room_name", "room", "area"):
        value = esphome_runtime.text(meta.get(key))
        if value:
            return value
    return ""


def format_ts_label(ts_value: Any) -> str:
    ts = _as_float(ts_value, 0.0)
    if ts <= 0:
        return "-"
    with contextlib.suppress(Exception):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    return "-"


def _asset_data_url(path: str) -> str:
    token = esphome_runtime.text(path)
    if not token:
        return ""
    cached = _asset_data_url_cache.get(token)
    if cached:
        return cached
    try:
        with open(token, "rb") as handle:
            raw = handle.read()
        mime = mimetypes.guess_type(token)[0] or "application/octet-stream"
        encoded = base64.b64encode(raw).decode("ascii")
        value = f"data:{mime};base64,{encoded}"
        _asset_data_url_cache[token] = value
        return value
    except Exception:
        return ""


def _default_satellite_image_src() -> str:
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "images", "tatervoice.png"))
    return _asset_data_url(path)


def _named_satellite_image_src(image_name: str) -> str:
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "images", image_name))
    return _asset_data_url(path)


def _satellite_image_src(*name_candidates: Any) -> str:
    for raw_name in name_candidates:
        token = esphome_runtime.lower(raw_name)
        if not token:
            continue
        if "tatervpe" in token:
            return _named_satellite_image_src("voicepe.png")
        if "tatersat1" in token:
            return _named_satellite_image_src("sat1.png")
    return _default_satellite_image_src()


def satellite_item_forms(status: Dict[str, Any]) -> List[Dict[str, Any]]:
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    registry = esphome_runtime.load_satellite_registry()
    rows_by_selector: Dict[str, Dict[str, Any]] = {}

    for row in registry:
        selector = esphome_runtime.text(row.get("selector"))
        if not selector:
            host = esphome_runtime.lower(row.get("host"))
            if host:
                selector = f"host:{host}"
        if not selector:
            continue
        normalized = dict(row)
        normalized["selector"] = selector
        rows_by_selector[selector] = normalized

    for selector, client_row in clients.items():
        if not isinstance(client_row, dict):
            continue
        token = esphome_runtime.text(selector)
        if not token:
            continue
        current = rows_by_selector.get(token) or {}
        host = (
            esphome_runtime.lower(current.get("host"))
            or esphome_runtime.lower(client_row.get("host"))
            or esphome_runtime.satellite_host_from_selector(token)
        )
        meta = current.get("metadata") if isinstance(current.get("metadata"), dict) else {}
        rows_by_selector[token] = {
            "selector": token,
            "host": host,
            "name": esphome_runtime.text(current.get("name")) or esphome_runtime.text(client_row.get("name")) or host or token,
            "source": esphome_runtime.text(current.get("source")) or esphome_runtime.text(client_row.get("source")) or "esphome_native",
            "metadata": dict(meta),
            "last_seen_ts": _as_float(current.get("last_seen_ts"), 0.0),
        }

    items: List[Dict[str, Any]] = []
    sortable_rows = []
    for selector, row in rows_by_selector.items():
        client_row = clients.get(selector) if isinstance(clients.get(selector), dict) else {}
        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        selected = bool(meta.get("esphome_selected"))
        connected = bool(client_row.get("connected"))
        name = esphome_runtime.text(row.get("name")) or esphome_runtime.text(row.get("host")) or selector
        host = esphome_runtime.lower(row.get("host")) or esphome_runtime.satellite_host_from_selector(selector)
        sortable_rows.append((selected, connected, esphome_runtime.lower(name or host), selector, row, client_row))

    sortable_rows.sort(key=lambda item: (0 if item[0] else 1, 0 if item[1] else 1, item[2], item[3]))
    for selected, connected, _sort_name, selector, row, client_row in sortable_rows:
        host = esphome_runtime.lower(row.get("host")) or esphome_runtime.satellite_host_from_selector(selector)
        name = esphome_runtime.text(row.get("name")) or host or selector
        source = esphome_runtime.text(row.get("source")) or "unknown"
        area_name = _satellite_area_name(row)
        last_seen = format_ts_label(row.get("last_seen_ts"))
        last_error = esphome_runtime.text(client_row.get("last_error"))
        device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
        entity_rows = list(client_row.get("entity_rows") or []) if isinstance(client_row.get("entity_rows"), list) else []
        entity_row_count = int(client_row.get("entity_row_count") or len(entity_rows) or 0)
        entity_count = int(client_row.get("entity_count") or 0)
        state_updated = format_ts_label(client_row.get("entity_state_updated_ts"))
        log_last_line = format_ts_label(client_row.get("log_last_line_ts"))
        voice_metrics = client_row.get("voice_metrics") if isinstance(client_row.get("voice_metrics"), dict) else {}
        device_name = esphome_runtime.text(device_info.get("name"))
        friendly_name = esphome_runtime.text(device_info.get("friendly_name"))
        manufacturer = esphome_runtime.text(device_info.get("manufacturer"))
        model = esphome_runtime.text(device_info.get("model"))
        project_name = esphome_runtime.text(device_info.get("project_name"))
        project_version = esphome_runtime.text(device_info.get("project_version"))
        esphome_version = esphome_runtime.text(device_info.get("esphome_version"))
        compilation_time = esphome_runtime.text(device_info.get("compilation_time"))
        mac_address = esphome_runtime.text(device_info.get("mac_address"))
        bluetooth_mac_address = esphome_runtime.text(device_info.get("bluetooth_mac_address"))
        api_audio = bool(client_row.get("voice_api_audio_supported"))
        speaker_supported = bool(client_row.get("voice_speaker_supported"))
        subtitle = f"{host or 'unknown host'} • {'selected' if selected else 'not selected'} • {'connected' if connected else 'disconnected'}"
        detail_parts = [f"source={source}"]
        if last_seen != "-":
            detail_parts.append(f"seen={last_seen}")
        if state_updated != "-":
            detail_parts.append(f"sensors={state_updated}")
        if log_last_line != "-":
            detail_parts.append(f"logs={log_last_line}")
        if last_error:
            detail_parts.append(f"error={last_error}")

        hero_badges: List[Dict[str, str]] = [
            {"label": "Selected" if selected else "Not Selected", "tone": "accent" if selected else "muted"},
            {"label": "Connected" if connected else "Offline", "tone": "success" if connected else "danger"},
            {"label": "API Audio" if api_audio else "No API Audio", "tone": "success" if api_audio else "muted"},
            {"label": "Speaker" if speaker_supported else "No Speaker", "tone": "success" if speaker_supported else "muted"},
        ]
        summary_rows: List[Dict[str, str]] = [
            {"label": "Host", "value": host or "-"},
            {"label": "Room / Area", "value": area_name or "-"},
            {"label": "Source", "value": source or "-"},
            {"label": "Last Seen", "value": last_seen},
            {"label": "Sensor Update", "value": state_updated},
            {"label": "Last Log", "value": log_last_line},
            {"label": "Entities", "value": str(entity_count)},
            {"label": "Live Entities", "value": str(entity_row_count)},
        ]
        last_outcome = esphome_runtime.text(voice_metrics.get("last_outcome"))
        last_reason = esphome_runtime.text(voice_metrics.get("last_reason"))
        if last_outcome:
            summary_rows.append({"label": "Last Outcome", "value": last_outcome.replace("_", " ")})
        if float(voice_metrics.get("avg_turn_latency_ms") or 0.0) > 0.0:
            summary_rows.append({"label": "Avg Turn", "value": f"{float(voice_metrics.get('avg_turn_latency_ms')):.1f} ms"})
        if float(voice_metrics.get("avg_stt_latency_ms") or 0.0) > 0.0:
            summary_rows.append({"label": "Avg STT", "value": f"{float(voice_metrics.get('avg_stt_latency_ms')):.1f} ms"})
        if float(voice_metrics.get("avg_tts_latency_ms") or 0.0) > 0.0:
            summary_rows.append({"label": "Avg TTS", "value": f"{float(voice_metrics.get('avg_tts_latency_ms')):.1f} ms"})
        if float(voice_metrics.get("avg_speech_s") or 0.0) > 0.0:
            summary_rows.append({"label": "Avg Speech", "value": f"{float(voice_metrics.get('avg_speech_s')):.2f} s"})
        if float(voice_metrics.get("avg_silence_s") or 0.0) > 0.0:
            summary_rows.append({"label": "Avg Silence", "value": f"{float(voice_metrics.get('avg_silence_s')):.2f} s"})
        if last_reason:
            summary_rows.append({"label": "Last Reason", "value": last_reason.replace("_", " ")})
        if device_name:
            summary_rows.append({"label": "Device Name", "value": device_name})
        if friendly_name:
            summary_rows.append({"label": "Friendly Name", "value": friendly_name})
        if manufacturer:
            summary_rows.append({"label": "Maker", "value": manufacturer})
        if model:
            summary_rows.append({"label": "Model", "value": model})
        if project_name or project_version:
            summary_rows.append(
                {"label": "Project", "value": " ".join(part for part in [project_name, project_version] if part).strip()}
            )
        if esphome_version:
            summary_rows.append({"label": "ESPHome", "value": esphome_version})
        if compilation_time:
            summary_rows.append({"label": "Build", "value": compilation_time})
        if mac_address:
            summary_rows.append({"label": "MAC", "value": mac_address})
        if bluetooth_mac_address:
            summary_rows.append({"label": "BT MAC", "value": bluetooth_mac_address})
        hero_image_src = _satellite_image_src(device_name, friendly_name, name)
        sensor_rows = [
            {
                "key": esphome_runtime.text(sensor.get("key")),
                "label": esphome_runtime.text(sensor.get("label")) or "Sensor",
                "value": esphome_runtime.text(sensor.get("value")) or "-",
                "kind": esphome_runtime.text(sensor.get("kind")),
                "meta": esphome_runtime.text(sensor.get("meta")),
                "control": dict(sensor.get("control") or {}) if isinstance(sensor.get("control"), dict) else {},
            }
            for sensor in entity_rows
            if isinstance(sensor, dict)
        ]

        fields: List[Dict[str, Any]] = [
            {
                "key": "area_name",
                "label": "Room / Area",
                "type": "text",
                "value": area_name,
                "placeholder": "Office",
                "description": "Used as the default room context for voice turns from this satellite.",
            },
        ]

        items.append(
            {
                "id": selector,
                "group": "satellite",
                "title": name,
                "subtitle": subtitle,
                "detail": " • ".join(detail_parts),
                "connected": connected,
                "hero_image_src": hero_image_src,
                "hero_image_alt": f"{name} satellite",
                "hero_badges": hero_badges,
                "summary_rows": summary_rows,
                "sensor_rows": sensor_rows,
                "sensor_title": "Live Entities" if sensor_rows else "No Entities",
                "fields": fields,
                "popup_mode": "voice-satellite-log",
                "popup_config": {
                    "selector": selector,
                    "name": name,
                    "host": host,
                },
                "popup_fields": [
                    {
                        "key": "live_log_feed",
                        "label": "Live Device Log",
                        "type": "textarea",
                        "value": "Opening live log feed...",
                        "description": "Live ESPHome logs from this satellite. New lines stream in automatically while the popup stays open.",
                    }
                ],
                "save_action": "voice_satellite_save",
                "save_label": "Save",
                "remove_action": "voice_satellite_remove",
                "remove_label": "Forget",
                "remove_confirm": f"Forget satellite {name}?",
                "run_action": "voice_disconnect" if connected else "voice_connect",
                "run_label": "Disconnect" if connected else "Connect",
                "run_confirm": "Disconnect and deselect this satellite?" if connected else "",
                "settings_title": f"{name} Live Log",
                "settings_label": "Live Log",
            }
        )

    return items
