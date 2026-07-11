from __future__ import annotations

import base64
import contextlib
import mimetypes
import os
import time
from typing import Any, Dict, List

from . import native_live_settings
from . import reply_playback
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


def _native_section_value(sections: List[Dict[str, Any]], section_title: str, row_label: str) -> str:
    wanted_section = esphome_runtime.lower(section_title)
    wanted_label = esphome_runtime.lower(row_label)
    for section in sections:
        if not isinstance(section, dict) or esphome_runtime.lower(section.get("title")) != wanted_section:
            continue
        rows = section.get("rows") if isinstance(section.get("rows"), list) else []
        for row in rows:
            if isinstance(row, dict) and esphome_runtime.lower(row.get("label")) == wanted_label:
                return esphome_runtime.text(row.get("value"))
    return ""


def _native_info_key(value: Any) -> str:
    token = esphome_runtime.lower(value).replace(" ", "_")
    safe = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in token).strip("_")
    return safe or "field"


def _native_popup_fields_from_sections(sections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fields: List[Dict[str, Any]] = []
    for section_idx, section in enumerate(sections):
        if not isinstance(section, dict):
            continue
        title = esphome_runtime.text(section.get("title"))
        rows = section.get("rows") if isinstance(section.get("rows"), list) else []
        if not title or not rows:
            continue
        fields.append(
            {
                "key": f"native_info_section_{section_idx}",
                "label": title,
                "type": "section",
            }
        )
        for row_idx, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            label = esphome_runtime.text(row.get("label"))
            value = esphome_runtime.text(row.get("value"))
            if not label or not value or value == "-":
                continue
            fields.append(
                {
                    "key": f"native_info_{section_idx}_{row_idx}_{_native_info_key(label)}",
                    "label": label,
                    "type": "readonly",
                    "value": value,
                }
            )
    return fields


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


def _satellite_image_token(value: Any) -> str:
    token = esphome_runtime.lower(value)
    if not token:
        return ""
    parts: List[str] = []
    last_separator = True
    for char in token:
        if char.isalnum():
            parts.append(char)
            last_separator = False
        elif not last_separator:
            parts.append("-")
            last_separator = True
    return "".join(parts).strip("-")


def _exact_satellite_image_src(value: Any) -> str:
    token = _satellite_image_token(value)
    if not token:
        return ""
    compact = token.replace("-", "")
    if token in {"reachy-mini", "tater-reachy", "tater-voice-sat"} or compact in {"reachymini", "taterreachy", "tatervoicesat"}:
        return _named_satellite_image_src("reachy-mini.png")
    if token in {"respeaker-lite", "seeed-respeaker-lite"} or compact in {"respeakerlite", "seeedrespeakerlite"}:
        return _named_satellite_image_src("respeaker-lite.png")
    if token in {"respeaker-xvf3800", "seeed-xvf3800", "xvf3800"} or compact in {"respeakerxvf3800", "seeedxvf3800"}:
        return _named_satellite_image_src("respeaker-xvf3800.png")
    if token in {"koala", "koala-satellite", "koala-voice-satellite"}:
        return _named_satellite_image_src("koala-satellite.png")
    if token in {"taters3box", "tater-s3box", "tater-s3box-display", "s3box", "s3-box", "esp32-s3-box", "esp32-s3-box-3"} or compact in {
        "taters3box",
        "s3box",
        "esp32s3box",
        "esp32s3box3",
    }:
        return _named_satellite_image_src("taterD.png")
    if token in {"satellite1", "satellite-1", "sat1", "sat-1", "tater-sat1", "tater-satellite1", "core-board"} or compact in {
        "satellite1",
        "sat1",
        "tatersat1",
        "tatersatellite1",
        "coreboard",
    }:
        return _named_satellite_image_src("sat1.png")
    if token in {"voice-pe", "voicepe", "tater-voice-pe", "tatervpe"} or compact in {"voicepe", "tatervpe", "tatervoicepe"}:
        return _named_satellite_image_src("voicepe.png")
    return ""


def device_image_src(*name_candidates: Any) -> str:
    for raw_name in name_candidates:
        exact_src = _exact_satellite_image_src(raw_name)
        if exact_src:
            return exact_src
        token = esphome_runtime.lower(raw_name)
        if not token:
            continue
        if any(
            part in token
            for part in (
                "taterreachy",
                "tater-reachy",
                "tater reachy",
                "tater voice sat",
                "tatervoicesat",
                "reachy-mini",
                "reachy_mini",
                "reachy mini",
                "reachymini",
            )
        ):
            return _named_satellite_image_src("reachy-mini.png")
        if any(
            part in token
            for part in (
                "respeaker-lite",
                "respeaker_lite",
                "respeaker lite",
                "respeakerlite",
                "re speaker lite",
                "seeed respeaker lite",
            )
        ):
            return _named_satellite_image_src("respeaker-lite.png")
        if any(
            part in token
            for part in (
                "respeaker-xvf3800",
                "respeaker_xvf3800",
                "respeaker xvf3800",
                "respeaker xvf 3800",
                "respeakerxvf3800",
                "seeed xvf3800",
                "xvf3800",
                "xvf 3800",
            )
        ):
            return _named_satellite_image_src("respeaker-xvf3800.png")
        if any(
            part in token
            for part in (
                "koala-satellite",
                "koala_satellite",
                "koala satellite",
                "koala voice satellite",
                "koala",
            )
        ):
            return _named_satellite_image_src("koala-satellite.png")
        if any(
            part in token
            for part in (
                "taters3box",
                "tater-s3box",
                "tater s3box",
                "tater-s3box-display",
                "s3box",
                "s3 box",
                "esp32-s3-box",
                "esp32-s3-box-3",
                "esp32 s3 box 3",
                "box-3",
                "box 3",
            )
        ):
            return _named_satellite_image_src("taterD.png")
        if any(
            part in token
            for part in (
                "tatersat1",
                "tater-sat1",
                "tater_sat1",
                "tater sat1",
                "tater satellite1",
                "tater-satellite1",
                "tater_satellite1",
                "satellite1",
                "satellite-1",
                "satellite_1",
                "satellite 1",
                "sat1",
                "sat-1",
                "sat_1",
                "sat 1",
                "core board",
            )
        ):
            return _named_satellite_image_src("sat1.png")
        if "tatervpe" in token:
            return _named_satellite_image_src("voicepe.png")
        if any(
            part in token
            for part in (
                "voice-pe",
                "voice_pe",
                "voice pe",
                "voicepe",
                "tater voice pe",
                "tater-voice-pe",
                "tater_voice_pe",
            )
        ):
            return _named_satellite_image_src("voicepe.png")
    return _default_satellite_image_src()


def _satellite_image_src(*name_candidates: Any) -> str:
    return device_image_src(*name_candidates)


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
        source = esphome_runtime.text(row.get("source"))
        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        native_device = (
            selector.startswith("native:")
            or source in {"tater_native", "native_satellite"}
            or bool(meta.get("native_selected"))
            or bool(meta.get("native_protocol"))
        )
        if not native_device:
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
        client_source = esphome_runtime.text(client_row.get("source"))
        native_device = client_source in {"tater_native", "native_satellite"} or token.startswith("native:")
        host = (
            esphome_runtime.lower(current.get("host"))
            or esphome_runtime.lower(client_row.get("host"))
            or ("" if native_device else esphome_runtime.satellite_host_from_selector(token))
        )
        meta = dict(current.get("metadata") if isinstance(current.get("metadata"), dict) else {})
        client_meta = client_row.get("metadata") if isinstance(client_row.get("metadata"), dict) else {}
        if native_device:
            meta.update({key: value for key, value in client_meta.items() if esphome_runtime.text(value) or isinstance(value, bool)})
        rows_by_selector[token] = {
            "selector": token,
            "host": host,
            "name": esphome_runtime.text(current.get("name")) or esphome_runtime.text(client_row.get("name")) or host or token,
            "source": esphome_runtime.text(current.get("source")) or client_source or "tater_native",
            "metadata": dict(meta),
            "last_seen_ts": _as_float(current.get("last_seen_ts"), 0.0) or _as_float(client_row.get("last_seen_ts"), 0.0),
        }

    items: List[Dict[str, Any]] = []
    sortable_rows = []
    for selector, row in rows_by_selector.items():
        client_row = clients.get(selector) if isinstance(clients.get(selector), dict) else {}
        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        source = esphome_runtime.text(row.get("source")) or esphome_runtime.text(client_row.get("source"))
        native_device = source in {"tater_native", "native_satellite"} or selector.startswith("native:")
        selected = bool(meta.get("native_selected") or native_device)
        connected = bool(client_row.get("connected"))
        name = esphome_runtime.text(row.get("name")) or esphome_runtime.text(row.get("host")) or selector
        host = esphome_runtime.lower(row.get("host")) or ("" if native_device else esphome_runtime.satellite_host_from_selector(selector))
        sortable_rows.append((selected, connected, esphome_runtime.lower(name or host), selector, row, client_row))

    sortable_rows.sort(key=lambda item: (0 if item[0] else 1, 0 if item[1] else 1, item[2], item[3]))
    for selected, connected, _sort_name, selector, row, client_row in sortable_rows:
        source = esphome_runtime.text(row.get("source")) or "unknown"
        native_device = source in {"tater_native", "native_satellite"} or selector.startswith("native:")
        host = esphome_runtime.lower(row.get("host")) or ("" if native_device else esphome_runtime.satellite_host_from_selector(selector))
        name = esphome_runtime.text(row.get("name")) or host or selector
        area_name = _satellite_area_name(row)
        reply_playback_target = reply_playback.resolve_reply_playback_target(row, client_row=client_row)
        reply_playback_options = reply_playback.build_reply_playback_options(reply_playback_target)
        reply_playback_label = next(
            (
                esphome_runtime.text(option.get("label"))
                for option in reply_playback_options
                if esphome_runtime.text(option.get("value")) == reply_playback_target
            ),
            reply_playback_target or "This device speaker",
        )
        last_seen = format_ts_label(row.get("last_seen_ts"))
        last_error = esphome_runtime.text(client_row.get("last_error"))
        device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
        entity_rows = list(client_row.get("entity_rows") or []) if isinstance(client_row.get("entity_rows"), list) else []
        entity_row_count = int(client_row.get("entity_row_count") or len(entity_rows) or 0)
        entity_count = int(client_row.get("entity_count") or 0)
        native_detail_sections = (
            list(client_row.get("native_detail_sections") or [])
            if native_device and isinstance(client_row.get("native_detail_sections"), list)
            else []
        )
        state_updated = format_ts_label(client_row.get("entity_state_updated_ts"))
        log_last_line = format_ts_label(client_row.get("log_last_line_ts"))
        voice_metrics = client_row.get("voice_metrics") if isinstance(client_row.get("voice_metrics"), dict) else {}
        device_name = esphome_runtime.text(device_info.get("name"))
        friendly_name = esphome_runtime.text(device_info.get("friendly_name"))
        manufacturer = esphome_runtime.text(device_info.get("manufacturer"))
        model = esphome_runtime.text(device_info.get("model"))
        project_name = esphome_runtime.text(device_info.get("project_name"))
        project_version = esphome_runtime.text(device_info.get("project_version"))
        firmware_version = esphome_runtime.text(client_row.get("firmware_version")) or project_version
        board = (model or esphome_runtime.text(meta.get("board"))) if native_device else (esphome_runtime.text(meta.get("board")) or model)
        esphome_version = esphome_runtime.text(device_info.get("esphome_version"))
        compilation_time = esphome_runtime.text(device_info.get("compilation_time"))
        mac_address = esphome_runtime.text(device_info.get("mac_address"))
        bluetooth_mac_address = esphome_runtime.text(device_info.get("bluetooth_mac_address"))
        api_audio = bool(client_row.get("voice_api_audio_supported"))
        speaker_supported = bool(client_row.get("voice_speaker_supported"))
        location_label = "native websocket" if native_device else (host or "unknown host")
        subtitle = f"{location_label} • {'selected' if selected else 'not selected'} • {'connected' if connected else 'disconnected'}"
        detail_parts: List[str] = []
        if not native_device:
            detail_parts.append(f"source={source}")
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
        info_fields: List[Dict[str, Any]] = []
        info_title = ""
        info_label = ""
        if native_device:
            connection_rows = [
                {"label": "Transport", "value": "Native WebSocket"},
                {"label": "Room / Area", "value": area_name or "-"},
                {"label": "Reply Playback", "value": reply_playback_label or "-"},
                {"label": "Source", "value": "Tater Native"},
                {"label": "Last Seen", "value": last_seen},
            ]
            if log_last_line != "-":
                connection_rows.append({"label": "Last Log", "value": log_last_line})
            project_value = " ".join(part for part in [project_name, project_version] if part).strip()
            device_rows = [
                {"label": "Device Name", "value": device_name or "-"},
                {"label": "Friendly Name", "value": friendly_name or "-"},
                {"label": "Maker", "value": manufacturer or "-"},
                {"label": "Model", "value": model or "-"},
                {"label": "Project", "value": project_value or "-"},
            ]
            full_native_detail_sections = [
                {"title": "Connection", "rows": connection_rows},
                {"title": "Device", "rows": device_rows},
                *native_detail_sections,
            ]
            overview_rows = [
                {"label": "State", "value": _native_section_value(full_native_detail_sections, "Device Info", "State") or "-"},
                {"label": "Room / Area", "value": area_name or "-"},
                {
                    "label": "Wake Model",
                    "value": _native_section_value(full_native_detail_sections, "Diagnostics", "Active Wake Model") or "-",
                },
                {"label": "Wi-Fi RSSI", "value": _native_section_value(full_native_detail_sections, "Diagnostics", "Wi-Fi RSSI") or "-"},
                {"label": "Last Reset", "value": _native_section_value(full_native_detail_sections, "Diagnostics", "Last Reset") or "-"},
                {
                    "label": "Wake Tuning",
                    "value": (
                        _native_section_value(full_native_detail_sections, "Settings", "Wake Tuning")
                        or _native_section_value(full_native_detail_sections, "Settings", "Wake Threshold / Window")
                        or "-"
                    ),
                },
                {"label": "Firmware", "value": firmware_version or "-"},
            ]
            native_detail_sections = [{"title": "Overview", "rows": overview_rows}]
            info_fields = _native_popup_fields_from_sections(full_native_detail_sections)
            info_title = f"{name} Device Info"
            info_label = "Device Info"
            summary_rows = []
        else:
            summary_rows = [
                {"label": "Host", "value": host or "-"},
                {"label": "Room / Area", "value": area_name or "-"},
                {"label": "Reply Playback", "value": reply_playback_label or "-"},
                {"label": "Source", "value": source or "-"},
                {"label": "Last Seen", "value": last_seen},
                {"label": "Sensor Update", "value": state_updated},
                {"label": "Last Log", "value": log_last_line},
                {"label": "Entities", "value": str(entity_count)},
                {"label": "Live Entities", "value": str(entity_row_count)},
            ]
        last_outcome = esphome_runtime.text(voice_metrics.get("last_outcome"))
        last_reason = esphome_runtime.text(voice_metrics.get("last_reason"))
        if last_outcome and not native_device:
            summary_rows.append({"label": "Last Outcome", "value": last_outcome.replace("_", " ")})
        if not native_device and float(voice_metrics.get("avg_turn_latency_ms") or 0.0) > 0.0:
            summary_rows.append({"label": "Avg Turn", "value": f"{float(voice_metrics.get('avg_turn_latency_ms')):.1f} ms"})
        if not native_device and float(voice_metrics.get("avg_stt_latency_ms") or 0.0) > 0.0:
            summary_rows.append({"label": "Avg STT", "value": f"{float(voice_metrics.get('avg_stt_latency_ms')):.1f} ms"})
        if not native_device and float(voice_metrics.get("avg_tts_latency_ms") or 0.0) > 0.0:
            summary_rows.append({"label": "Avg TTS", "value": f"{float(voice_metrics.get('avg_tts_latency_ms')):.1f} ms"})
        if not native_device and float(voice_metrics.get("avg_speech_s") or 0.0) > 0.0:
            summary_rows.append({"label": "Avg Speech", "value": f"{float(voice_metrics.get('avg_speech_s')):.2f} s"})
        if not native_device and float(voice_metrics.get("avg_silence_s") or 0.0) > 0.0:
            summary_rows.append({"label": "Avg Silence", "value": f"{float(voice_metrics.get('avg_silence_s')):.2f} s"})
        if last_reason and not native_device:
            summary_rows.append({"label": "Last Reason", "value": last_reason.replace("_", " ")})
        if device_name and not native_device:
            summary_rows.append({"label": "Device Name", "value": device_name})
        if friendly_name and not native_device:
            summary_rows.append({"label": "Friendly Name", "value": friendly_name})
        if manufacturer and not native_device:
            summary_rows.append({"label": "Maker", "value": manufacturer})
        if model and not native_device:
            summary_rows.append({"label": "Model", "value": model})
        if not native_device and (project_name or project_version):
            summary_rows.append(
                {"label": "Project", "value": " ".join(part for part in [project_name, project_version] if part).strip()}
            )
        if esphome_version and not native_device:
            summary_rows.append({"label": "Legacy Firmware", "value": esphome_version})
        if compilation_time and not native_device:
            summary_rows.append({"label": "Build", "value": compilation_time})
        if mac_address and not native_device:
            summary_rows.append({"label": "MAC", "value": mac_address})
        if bluetooth_mac_address and not native_device:
            summary_rows.append({"label": "BT MAC", "value": bluetooth_mac_address})
        hero_image_src = _exact_satellite_image_src(board) or _satellite_image_src(
            firmware_version,
            model,
            project_name,
            selector,
            name,
            device_name,
            friendly_name,
        )
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
        wake_engine_value = ""
        display_target = ""
        for sensor in sensor_rows:
            sensor_label = esphome_runtime.lower(sensor.get("label"))
            if sensor_label == "wake engine":
                wake_engine_value = esphome_runtime.text(sensor.get("value"))
            elif sensor_label == "tater display target":
                display_target = esphome_runtime.text(sensor.get("value"))
        if wake_engine_value:
            summary_rows.insert(3, {"label": "Wake Engine", "value": wake_engine_value})
        if not display_target and esphome_runtime.lower(project_name) == "tater.s3box_display":
            display_target = "livingroom"

        popup_mode = "voice-satellite-log"
        popup_config = {
            "selector": selector,
            "name": name,
            "host": host,
        }
        popup_fields: List[Dict[str, Any]] = [
            {
                "key": "live_log_feed",
                "label": "Live Device Log",
                "type": "textarea",
                "value": "Opening live log feed...",
                "description": "Live logs from this satellite. New lines stream in automatically while the popup stays open.",
            }
        ]
        settings_title = f"{name} Live Log"
        settings_label = "Live Log"
        settings_save_action = ""
        if native_device:
            popup_mode = ""
            popup_config = {
                "selector": selector,
                "name": name,
            }
            popup_fields = native_live_settings.settings_fields(selector, board=board)
            settings_title = f"{name} Satellite Settings"
            settings_label = "Satellite Settings"
            settings_save_action = "voice_native_satellite_settings_save"

        fields: List[Dict[str, Any]] = [
            {
                "key": "area_name",
                "label": "Room / Area",
                "type": "text",
                "value": area_name,
                "placeholder": "Office",
                "description": "Used as the default room context for voice turns from this satellite.",
            },
            {
                "key": "reply_playback_target",
                "label": "Reply Playback",
                "type": "select",
                "value": reply_playback_target,
                "options": reply_playback_options,
                "description": "Optional. Leave this on This device speaker to keep normal sat1 and VoicePE reply playback. Use Silent / display only when a device should act as a mic and screen.",
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
                "native_firmware": native_device,
                "board": board,
                "model": model,
                "project_name": project_name,
                "hero_image_src": hero_image_src,
                "hero_image_alt": f"{name} satellite",
                "hero_badges": hero_badges,
                "summary_rows": summary_rows,
                "detail_sections": native_detail_sections,
                "sensor_rows": [] if native_device else sensor_rows,
                "sensor_title": "" if native_device else ("Live Entities" if sensor_rows else "No Entities"),
                "show_entity_refresh": not native_device,
                "display_target": display_target,
                "fields": fields,
                "popup_mode": popup_mode,
                "popup_config": popup_config,
                "popup_fields": popup_fields,
                "info_fields": info_fields,
                "info_title": info_title,
                "info_label": info_label,
                "save_action": "voice_satellite_save",
                "settings_save_action": settings_save_action,
                "save_label": "Save",
                "identify_action": "voice_satellite_identify",
                "identify_label": "Identify",
                "remove_action": "" if native_device and connected else "voice_satellite_remove",
                "remove_label": "Forget",
                "remove_confirm": f"Forget satellite {name}?",
                "run_action": (
                    "voice_native_satellite_setup_mode"
                    if native_device and connected
                    else ("" if native_device else ("voice_disconnect" if connected else "voice_connect"))
                ),
                "run_label": "Setup Mode" if native_device and connected else ("" if native_device else ("Disconnect" if connected else "Connect")),
                "run_confirm": (
                    f"Put {name} into setup mode? It will clear saved provisioning, reboot, and disconnect from Tater until setup is completed again."
                    if native_device and connected
                    else ("" if native_device else ("Disconnect and deselect this satellite?" if connected else ""))
                ),
                "settings_title": settings_title,
                "settings_label": settings_label,
            }
        )

    return items
