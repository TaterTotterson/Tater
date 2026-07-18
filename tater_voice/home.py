from __future__ import annotations

import contextlib
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from . import runtime as esphome_runtime
from . import firmware as esphome_firmware
from . import native_live_settings
from . import native_satellite
from . import reply_playback as esphome_reply_playback
from . import settings as esphome_settings
from . import speaker_id as esphome_speaker_id
from . import emotion_id as esphome_emotion_id

IDENTIFY_SATELLITE_TEXT = (
    "Hey, over here. This is the satellite you're looking for. Yeah, over here. Can you hear me?"
)


def settings_hash_key() -> str:
    return esphome_settings.settings_hash_key()


def settings_fields() -> List[Dict[str, Any]]:
    rows = esphome_settings.settings_fields()
    return rows if isinstance(rows, list) else []


def settings_item_form() -> Dict[str, Any]:
    form = esphome_settings.settings_item_form()
    return form if isinstance(form, dict) else {}


def model_settings_sections() -> List[Dict[str, Any]]:
    rows = esphome_settings.model_settings_sections()
    return rows if isinstance(rows, list) else []


def save_settings_values(values: Dict[str, Any]) -> Dict[str, Any]:
    result = esphome_settings.save_settings_values(values or {})
    return result if isinstance(result, dict) else {"ok": True}


def runtime_tab_spec() -> Dict[str, Any]:
    return {
        "label": "Tater Voice",
        "core_key": "voice",
        "surface_key": "voice",
        "surface_kind": "voice",
        "order": 40,
        "requires_running": False,
        "running": is_running(),
    }


def is_running() -> bool:
    return esphome_runtime.is_running()


async def startup() -> None:
    native_satellite.bind_runtime_loop()
    await esphome_runtime.startup()


async def shutdown() -> None:
    await esphome_runtime.shutdown()


def _runtime_panel_token(panel: Any = "") -> str:
    token = esphome_runtime.lower(panel)
    return token if token in {"satellites", "firmware", "platform", "speakerid", "emotionid", "stats"} else ""


def _native_satellite_status_snapshot() -> Dict[str, Any]:
    try:
        result = native_satellite.run_on_runtime_loop(native_satellite.status(), timeout=3.0)
    except Exception:
        return {}
    return result if isinstance(result, dict) else {}


def _native_log_entries(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        seq = int(row.get("seq") or 0)
        ts_value = float(row.get("ts") or 0.0)
        level = esphome_runtime.text(row.get("level")) or "info"
        message = esphome_runtime.text(row.get("message"))
        if not message:
            continue
        timestamp = "-"
        with contextlib.suppress(Exception):
            if ts_value > 0:
                import time

                timestamp = time.strftime("%H:%M:%S", time.localtime(ts_value))
        entries.append(
            {
                "seq": seq,
                "ts": ts_value,
                "level": level,
                "message": message,
                "display": f"[{timestamp}] [{level}] {message}",
            }
        )
    return entries


def _native_logs_payload(selector: str, *, after_seq: int = 0, start: bool = False, stop: bool = False) -> Dict[str, Any]:
    if stop:
        return {"ok": True, "selector": selector, "active": False, "stopped": False, "viewer_count": 0}
    result = native_satellite.run_on_runtime_loop(
        native_satellite.logs(selector, after_seq=after_seq, limit=200),
        timeout=3.0,
    )
    if not isinstance(result, dict):
        result = {}
    rows = list(result.get("logs") or []) if isinstance(result.get("logs"), list) else []
    entries = _native_log_entries(rows)
    cursor = after_seq
    for entry in entries:
        cursor = max(cursor, int(entry.get("seq") or 0))
    if start:
        entries.insert(
            0,
            {
                "seq": 0,
                "level": "info",
                "message": "Native satellite log feed opened.",
                "display": "Native satellite log feed opened.",
            },
        )
    return {
        "ok": True,
        "selector": selector,
        "active": True,
        "connected": True,
        "cursor": cursor,
        "entries": entries,
        "viewer_count": 1,
    }


def _native_detail_row(key: str, label: str, value: Any) -> Dict[str, str]:
    if value is None:
        display = "-"
    elif isinstance(value, bool):
        display = "On" if value else "Off"
    else:
        display = str(value).strip() or "-"
    return {
        "key": key,
        "label": label,
        "value": display,
    }


def _native_queue_label(depth: Any, capacity: Any) -> Optional[str]:
    if depth is None and capacity is None:
        return None
    if capacity is None:
        return str(depth)
    return f"{depth or 0} / {capacity}"


def _native_ms_label(value: Any) -> Optional[str]:
    if value is None:
        return None
    return f"{value} ms"


def _native_client_to_runtime_row(selector: str, row: Dict[str, Any]) -> Dict[str, Any]:
    status = row.get("last_status") if isinstance(row.get("last_status"), dict) else {}
    capabilities = row.get("capabilities") if isinstance(row.get("capabilities"), dict) else {}
    voice = row.get("voice") if isinstance(row.get("voice"), dict) else {}
    auth = row.get("auth") if isinstance(row.get("auth"), dict) else {}
    live_settings = row.get("live_settings") if isinstance(row.get("live_settings"), dict) else {}
    wake_engine = status.get("wake_engine") if isinstance(status.get("wake_engine"), dict) else {}
    reset = status.get("reset") if isinstance(status.get("reset"), dict) else {}
    transport = status.get("transport") if isinstance(status.get("transport"), dict) else {}
    audio_transport = voice.get("audio_transport") if isinstance(voice.get("audio_transport"), dict) else {}
    xmos_firmware = status.get("xmos_firmware") if isinstance(status.get("xmos_firmware"), dict) else {}
    state = esphome_runtime.text(status.get("state")) or esphome_runtime.text(voice.get("state")) or "idle"
    room = esphome_runtime.text(row.get("room"))
    board = esphome_runtime.text(row.get("board")) or "native satellite"
    firmware_version = esphome_runtime.text(row.get("firmware_version"))
    name = esphome_runtime.text(row.get("device_name")) or esphome_runtime.text(row.get("device_id")) or selector

    wake_ready = "Ready" if bool(wake_engine.get("ready")) else "Not Ready"
    active_wake_model = (
        esphome_runtime.text(wake_engine.get("active_wake_label"))
        or esphome_runtime.text(wake_engine.get("active_wake_word"))
        or esphome_runtime.text(live_settings.get("wake_profile_name"))
        or esphome_runtime.text(live_settings.get("wake_word"))
    )
    wake_source = esphome_runtime.text(wake_engine.get("active_model_source")) or "embedded"
    wake_tuning = f"{live_settings.get('wake_threshold', 0.97)} / {live_settings.get('wake_sliding_window', 5)}"
    wake_sensitivity = native_live_settings.wake_sensitivity_label(live_settings.get("wake_sensitivity"))
    wake_environment = native_live_settings.wake_environment_label(live_settings.get("wake_environment"))
    wake_tuning_label = f"{wake_sensitivity} / {wake_environment}"
    capture_bits = []
    if bool(live_settings.get("capture_wake_audio")):
        capture_bits.append("good wakes")
    if bool(live_settings.get("capture_close_misses")):
        capture_bits.append("close misses")

    device_info_rows = [
        _native_detail_row("native_state", "State", state),
        _native_detail_row("native_connection", "Connection", "Connected" if bool(row.get("connected")) else "Offline"),
        _native_detail_row("native_auth", "Auth", esphome_runtime.text(auth.get("mode")) or "open"),
        _native_detail_row("native_board", "Board", board),
        _native_detail_row("native_firmware", "Firmware", firmware_version),
        _native_detail_row("native_last_message", "Last Message", row.get("last_message_type")),
    ]
    diagnostic_rows = [
        _native_detail_row("native_wake_engine", "Wake Engine", wake_ready),
        _native_detail_row("native_active_wake_model", "Active Wake Model", active_wake_model),
        _native_detail_row("native_wake_source", "Wake Source", wake_source),
        _native_detail_row("native_audio_frames", "Audio Frames", row.get("binary_frames")),
        _native_detail_row("native_audio_bytes", "Audio Bytes", row.get("binary_bytes")),
        _native_detail_row("native_logs", "Logs", row.get("log_count")),
        _native_detail_row("native_queued_commands", "Queued Commands", row.get("queued_commands")),
    ]
    if bool(wake_engine.get("custom_download_running")) or any(int(wake_engine.get(key) or 0) for key in ("custom_cache_hits", "custom_cache_writes", "custom_cache_failures", "custom_download_failures")):
        diagnostic_rows.append(
            _native_detail_row(
                "native_wake_model_cache",
                "Wake Model Cache",
                f"{wake_engine.get('custom_cache_hits') or 0} hits / {wake_engine.get('custom_cache_writes') or 0} writes / {wake_engine.get('custom_cache_failures') or 0} failures",
            )
        )
    if bool(wake_engine.get("wake_sound_download_running")) or any(int(wake_engine.get(key) or 0) for key in ("wake_sound_cache_hits", "wake_sound_cache_writes", "wake_sound_cache_failures", "wake_sound_download_failures")):
        diagnostic_rows.append(
            _native_detail_row(
                "native_wake_sound_cache",
                "Wake Sound Cache",
                f"{wake_engine.get('wake_sound_cache_hits') or 0} hits / {wake_engine.get('wake_sound_cache_writes') or 0} writes / {wake_engine.get('wake_sound_cache_failures') or 0} failures",
            )
        )
    if xmos_firmware:
        xmos_installed = esphome_runtime.text(xmos_firmware.get("installed_version")) or "unknown"
        xmos_target = esphome_runtime.text(xmos_firmware.get("target_version"))
        xmos_state = esphome_runtime.text(xmos_firmware.get("update_state")) or "unknown"
        xmos_version = xmos_installed
        if xmos_target and xmos_target != xmos_installed:
            xmos_version = f"{xmos_installed} -> {xmos_target}"
        xmos_state_label = xmos_state.replace("_", " ").title()
        if xmos_state == "running" and xmos_firmware.get("progress_percent") is not None:
            xmos_state_label = f"{xmos_state_label} ({xmos_firmware.get('progress_percent')}%)"
        elif bool(xmos_firmware.get("update_required")) and xmos_state not in {"complete", "skipped"}:
            xmos_state_label = f"{xmos_state_label} (required)"
        diagnostic_rows.append(_native_detail_row("native_xmos_firmware", "XMOS Firmware", xmos_version))
        diagnostic_rows.append(_native_detail_row("native_xmos_update", "XMOS Update", xmos_state_label))
        if xmos_state in {"running", "error"}:
            diagnostic_rows.append(
                _native_detail_row(
                    "native_xmos_dfu",
                    "XMOS DFU",
                    f"{xmos_firmware.get('dfu_state')} / {xmos_firmware.get('dfu_status')}",
                )
            )
    if status.get("wifi_rssi") is not None:
        diagnostic_rows.append(_native_detail_row("native_wifi_rssi", "Wi-Fi RSSI", f"{status.get('wifi_rssi')} dBm"))
    if status.get("free_heap") is not None:
        diagnostic_rows.append(_native_detail_row("native_free_heap", "Free Heap", status.get("free_heap")))
    if reset:
        reset_reason = esphome_runtime.text(reset.get("reason"))
        reset_code = reset.get("reason_code")
        if reset_reason:
            diagnostic_rows.append(_native_detail_row("native_reset_reason", "Last Reset", f"{reset_reason} ({reset_code})"))
        coredump_label = "Valid crash dump" if bool(reset.get("coredump_valid")) else "Present" if bool(reset.get("coredump_present")) else "None"
        if reset.get("coredump_size"):
            coredump_label = f"{coredump_label} ({reset.get('coredump_size')} bytes)"
        diagnostic_rows.append(_native_detail_row("native_coredump", "Core Dump", coredump_label))
        if esphome_runtime.text(reset.get("panic_reason")):
            diagnostic_rows.append(_native_detail_row("native_panic_reason", "Panic Reason", reset.get("panic_reason")))
        if esphome_runtime.text(reset.get("crash_task")):
            diagnostic_rows.append(_native_detail_row("native_crash_task", "Crash Task", reset.get("crash_task")))
        if reset.get("crash_pc") is not None:
            try:
                crash_pc = f"0x{int(reset.get('crash_pc')):08x}"
            except Exception:
                crash_pc = reset.get("crash_pc")
            diagnostic_rows.append(_native_detail_row("native_crash_pc", "Crash PC", crash_pc))
        if esphome_runtime.text(reset.get("backtrace")):
            diagnostic_rows.append(_native_detail_row("native_backtrace", "Backtrace", reset.get("backtrace")))
    if bool(voice.get("active")):
        diagnostic_rows.append(_native_detail_row("native_voice_session", "Voice Session", voice.get("session_id")))
    transport_rows = []
    if esphome_runtime.text(status.get("last_link_down")):
        age_ms = status.get("last_link_down_age_ms")
        try:
            age_label = f"{float(age_ms) / 1000.0:.1f}s ago"
        except Exception:
            age_label = "-"
        transport_rows.append(_native_detail_row("native_last_link_down", "Last Link Drop", status.get("last_link_down")))
        transport_rows.append(_native_detail_row("native_last_link_down_age", "Drop Age", age_label))
    if transport:
        transport_rows.extend(
            [
                _native_detail_row("native_audio_send_failures", "Audio Send Failures", transport.get("audio_send_failure_total")),
                _native_detail_row("native_last_audio_send", "Last Audio Send Result", transport.get("last_audio_send_result")),
                _native_detail_row(
                    "native_audio_tx_queue",
                    "Device Audio Queue",
                    _native_queue_label(transport.get("audio_tx_queue_depth"), transport.get("audio_tx_queue_capacity")),
                ),
                _native_detail_row("native_audio_tx_high_water", "Device Queue High Water", transport.get("audio_tx_high_water")),
                _native_detail_row("native_audio_tx_dropped", "Device Audio Drops", transport.get("audio_tx_dropped")),
                _native_detail_row("native_audio_tx_timeouts", "Device Send Timeouts", transport.get("audio_tx_send_timeouts")),
                _native_detail_row("native_audio_tx_last_ms", "Device Last Send Time", _native_ms_label(transport.get("audio_tx_last_send_ms"))),
                _native_detail_row("native_ws_error", "WS Error Type", transport.get("last_ws_error_type")),
                _native_detail_row("native_ws_socket_errno", "WS Socket Errno", transport.get("last_ws_sock_errno")),
                _native_detail_row("native_ws_http_status", "WS HTTP Status", transport.get("last_ws_http_status")),
            ]
        )
    if audio_transport:
        transport_rows.extend(
            [
                _native_detail_row(
                    "native_server_audio_queue",
                    "Tater Audio Queue",
                    _native_queue_label(audio_transport.get("queue_depth"), audio_transport.get("queue_capacity")),
                ),
                _native_detail_row("native_server_audio_high_water", "Tater Queue High Water", audio_transport.get("queue_high_water")),
                _native_detail_row("native_server_audio_drops", "Tater Audio Drops", audio_transport.get("queue_drops")),
                _native_detail_row("native_server_audio_drain_timeouts", "Tater Drain Timeouts", audio_transport.get("queue_drain_timeouts")),
            ]
        )
    settings_rows = [
        _native_detail_row("native_wake_word", "Wake Word", live_settings.get("wake_profile_name") or live_settings.get("wake_word")),
        _native_detail_row("native_wake_tuning_label", "Wake Tuning", wake_tuning_label),
        _native_detail_row("native_wake_sensitivity", "Wake Sensitivity", wake_sensitivity),
        _native_detail_row("native_wake_environment", "Wake Environment", wake_environment),
        _native_detail_row("native_wake_tuning", "Wake Threshold / Window", wake_tuning),
        _native_detail_row("native_wake_sound", "Wake Sound", live_settings.get("wake_sound") or "no_sound"),
        _native_detail_row("native_volume", "Volume", f"{live_settings.get('volume_percent', 80)}%"),
        _native_detail_row("native_led_brightness", "LED Brightness", live_settings.get("led_brightness", 64)),
        _native_detail_row("native_led_color", "LED Color", live_settings.get("led_color") or "#ff5a1f"),
        _native_detail_row(
            "native_led_animations",
            "LED Animations",
            " / ".join(
                [
                    esphome_runtime.text(live_settings.get("led_listening_animation")) or "directional",
                    esphome_runtime.text(live_settings.get("led_thinking_animation")) or "sparkle",
                    esphome_runtime.text(live_settings.get("led_tool_call_animation")) or "ping_pong",
                    esphome_runtime.text(live_settings.get("led_replying_animation")) or "voice_ring",
                ]
            ),
        ),
        _native_detail_row("native_continued_chat", "Continued Chat", "On" if bool(live_settings.get("continued_chat", True)) else "Off"),
        _native_detail_row("native_barge_in", "Barge-In", "On" if bool(live_settings.get("barge_in_enabled", False)) else "Off"),
        _native_detail_row("native_trainer_feedback", "Trainer Feedback", ", ".join(capture_bits) if capture_bits else "Off"),
    ]

    host = esphome_runtime.text(row.get("host"))

    return {
        "selector": selector,
        "connected": bool(row.get("connected")),
        "last_error": esphome_runtime.text(row.get("last_error")),
        "last_disconnect_ts": float(row.get("last_disconnect_ts") or 0.0),
        "host": host,
        "source": "tater_native",
        "name": name,
        "metadata": {
            "native_selected": True,
            "native_connected": bool(row.get("connected")),
            "board": board,
            "area_name": room,
            "room": room,
            "room_name": room,
        },
        "device_info": {
            "name": esphome_runtime.text(row.get("device_id")) or selector,
            "friendly_name": name,
            "manufacturer": "Tater",
            "model": board,
            "project_name": "tater.native_satellite",
            "project_version": firmware_version,
        },
        "voice_api_audio_supported": True,
        "voice_speaker_supported": bool(capabilities.get("speaker", True)),
        "voice_metrics": {},
        "entity_rows": [],
        "entity_row_count": 0,
        "entity_count": 0,
        "entity_state_updated_ts": 0.0,
        "native_detail_sections": [
            {"title": "Device Info", "rows": device_info_rows},
            {"title": "Diagnostics", "rows": diagnostic_rows},
            *([{"title": "Transport", "rows": transport_rows}] if transport_rows else []),
            {"title": "Settings", "rows": settings_rows},
        ],
        "log_last_line_ts": float(row.get("last_seen_ts") or 0.0) if int(row.get("log_count") or 0) > 0 else 0.0,
        "last_seen_ts": float(row.get("last_seen_ts") or 0.0),
    }


def _merge_native_satellites(status: Dict[str, Any], native_status: Dict[str, Any]) -> Dict[str, Any]:
    native_clients = native_status.get("clients") if isinstance(native_status.get("clients"), dict) else {}
    if not native_clients:
        return status
    clients = dict(status.get("clients") if isinstance(status.get("clients"), dict) else {})
    for selector, row in native_clients.items():
        token = esphome_runtime.text(selector)
        if not token or not isinstance(row, dict):
            continue
        clients[token] = _native_client_to_runtime_row(token, row)
    merged = dict(status)
    merged["clients"] = clients
    return merged


def _runtime_status_with_native(native_status: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    status = esphome_runtime.status()
    native = native_status if isinstance(native_status, dict) else _native_satellite_status_snapshot()
    return _merge_native_satellites(status, native)


def _is_native_satellite_row(row: Dict[str, Any]) -> bool:
    selector = esphome_runtime.text(row.get("selector"))
    source = esphome_runtime.text(row.get("source"))
    meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    return (
        selector.startswith("native:")
        or source in {"tater_native", "native_satellite"}
        or bool(meta.get("native_selected"))
        or bool(meta.get("native_protocol"))
    )


def get_runtime_payload(
    *,
    redis_client: Any = None,
    core_key: str = "voice",
    core_tab: Optional[Dict[str, Any]] = None,
    panel: str = "",
) -> Dict[str, Any]:
    panel_token = _runtime_panel_token(panel)
    include_satellites = panel_token in {"", "satellites"}
    include_firmware = panel_token in {"", "firmware"}
    include_speaker_id = panel_token in {"", "speakerid"}
    include_emotion_id = panel_token in {"", "emotionid"}
    include_stats = panel_token in {"", "stats"}
    native_status = _native_satellite_status_snapshot()
    status = _runtime_status_with_native(native_status)
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    voice_metrics = (
        status.get("voice_metrics")
        if isinstance(status.get("voice_metrics"), dict)
        else esphome_runtime.voice_metrics_snapshot()
    )
    satellites = [row for row in esphome_runtime.load_satellite_registry() if isinstance(row, dict) and _is_native_satellite_row(row)]
    native_clients = native_status.get("clients") if isinstance(native_status.get("clients"), dict) else {}

    connected = len([row for row in clients.values() if isinstance(row, dict) and bool(row.get("connected"))])
    selected = len(
        [row for row in native_clients.values() if isinstance(row, dict) and bool(row.get("connected"))]
    )
    known_selectors = {esphome_runtime.text(row.get("selector")) for row in satellites if isinstance(row, dict)}
    known_satellite_count = len(satellites) + len(
        [
            selector
            for selector in native_clients.keys()
            if esphome_runtime.text(selector) and esphome_runtime.text(selector) not in known_selectors
        ]
    )

    cfg = esphome_runtime.voice_config_snapshot()
    eou = cfg.get("eou") if isinstance(cfg.get("eou"), dict) else {}
    stt = cfg.get("stt") if isinstance(cfg.get("stt"), dict) else {}
    tts = cfg.get("tts") if isinstance(cfg.get("tts"), dict) else {}
    effective_stt_backend, _stt_note = esphome_runtime.resolve_stt_backend()
    effective_tts_backend, _tts_note = esphome_runtime.resolve_tts_backend()

    summary = (
        f"Selected satellites: {selected} • Connected: {connected} • "
        f"Valid turns: {int(voice_metrics.get('valid_turns') or 0)}/{int(voice_metrics.get('sessions_started') or 0)} • "
        f"False wakes: {int(voice_metrics.get('false_wake_count') or 0)} • "
        f"Low signal: {int(voice_metrics.get('low_signal_count') or 0)} • "
        f"Avg turn: {float(voice_metrics.get('avg_turn_latency_ms') or 0.0):.0f} ms • "
        "Transport: Native WebSocket • "
        f"STT: {esphome_runtime.text(stt.get('backend'))}->{effective_stt_backend} • "
        f"TTS: {esphome_runtime.text(tts.get('backend'))}->{effective_tts_backend} • "
        f"EOU: {esphome_runtime.text(eou.get('mode'))}/{esphome_runtime.text(eou.get('backend'))}"
    )
    payload = {
        "summary": summary,
        "panel": panel_token or "all",
        "header_stats": [
            {"label": "Connected", "value": connected},
            {"label": "Known Satellites", "value": known_satellite_count},
            {"label": "Transport", "value": "Native WebSocket"},
            {"label": "STT Backend", "value": effective_stt_backend},
            {"label": "TTS Backend", "value": effective_tts_backend},
        ],
        "items": [],
        "empty_message": "No native satellites connected yet. Use Add Satellite to create a pairing code, then enter it in the satellite setup page.",
        "ui": {
            "kind": "tater_native_satellite",
            "title": "Tater Satellites",
            "native_pairing": {
                "start_action": "voice_native_satellite_pairing_start",
                "status_action": "voice_native_satellite_pairing_status",
            },
        },
    }
    if include_satellites:
        item_forms = [esphome_settings.settings_item_form()]
        item_forms.extend(esphome_settings.satellite_item_forms(status))
        payload["ui"]["item_forms"] = item_forms
        payload["display_sensors"] = esphome_firmware.display_sensor_profiles_payload(status)

    if include_firmware:
        payload["firmware"] = esphome_firmware.firmware_panel_payload(status)

    if include_speaker_id:
        payload["speaker_id"] = esphome_speaker_id.panel_payload(status)

    if include_emotion_id:
        payload["emotion_id"] = esphome_emotion_id.panel_payload(status)

    if include_stats:
        voice_rows, voices_meta = esphome_runtime.load_wyoming_tts_voice_catalog()
        piper_rows, piper_meta = esphome_runtime.load_piper_tts_model_catalog()
        if effective_tts_backend == "piper":
            tts_catalog_count = len(piper_rows)
            tts_catalog_updated = piper_meta.get("updated_ts")
        elif effective_tts_backend == "wyoming":
            tts_catalog_count = len(voice_rows)
            tts_catalog_updated = voices_meta.get("updated_ts")
        else:
            tts_catalog_count = 0
            tts_catalog_updated = None
        discovery_stats = esphome_runtime.discovery_stats()
        stt_backend_rows = [
            {"backend": esphome_runtime.text(name) or "unknown", "avg_ms": f"{float(value or 0.0):.1f}"}
            for name, value in sorted(
                ((voice_metrics.get("avg_stt_latency_by_backend_ms") or {}) if isinstance(voice_metrics.get("avg_stt_latency_by_backend_ms"), dict) else {}).items(),
                key=lambda item: str(item[0]),
            )
        ]
        tts_backend_rows = [
            {"backend": esphome_runtime.text(name) or "unknown", "avg_ms": f"{float(value or 0.0):.1f}"}
            for name, value in sorted(
                ((voice_metrics.get("avg_tts_latency_by_backend_ms") or {}) if isinstance(voice_metrics.get("avg_tts_latency_by_backend_ms"), dict) else {}).items(),
                key=lambda item: str(item[0]),
            )
        ]
        device_rows = []
        for selector, row in sorted(clients.items(), key=lambda item: str(item[0])):
            if not isinstance(row, dict):
                continue
            device_info = row.get("device_info") if isinstance(row.get("device_info"), dict) else {}
            metrics_row = row.get("voice_metrics") if isinstance(row.get("voice_metrics"), dict) else {}
            title = (
                esphome_runtime.text(device_info.get("friendly_name"))
                or esphome_runtime.text(device_info.get("name"))
                or esphome_runtime.text(row.get("selector"))
                or esphome_runtime.text(selector)
            )
            device_rows.append(
                {
                    "satellite": title,
                    "host": esphome_runtime.text(row.get("host")) or "-",
                    "sessions": str(int(metrics_row.get("sessions_started") or 0)),
                    "valid": str(int(metrics_row.get("valid_turns") or 0)),
                    "no_ops": str(int(metrics_row.get("no_op_turns") or 0)),
                    "false_wakes": str(int(metrics_row.get("false_wake_count") or 0)),
                    "errors": str(int(metrics_row.get("error_count") or 0)),
                    "reconnects": str(int(metrics_row.get("reconnect_count") or 0)),
                    "avg_turn_ms": f"{float(metrics_row.get('avg_turn_latency_ms') or 0.0):.1f}",
                }
            )
        payload["stats"] = [
            {"label": "Voice Sessions", "value": int(voice_metrics.get("sessions_started") or 0)},
            {"label": "Valid Turns", "value": int(voice_metrics.get("valid_turns") or 0)},
            {"label": "No-Ops", "value": int(voice_metrics.get("no_op_turns") or 0)},
            {"label": "False Wakes", "value": int(voice_metrics.get("false_wake_count") or 0)},
            {"label": "Wake No Speech", "value": int(voice_metrics.get("wake_no_speech_count") or 0)},
            {"label": "Low Signal", "value": int(voice_metrics.get("low_signal_count") or 0)},
            {"label": "Clipped", "value": int(voice_metrics.get("clipped_ambiguous_count") or 0)},
            {"label": "Blank Wakes", "value": int(voice_metrics.get("blank_wake_count") or 0)},
            {"label": "Avg STT", "value": f"{float(voice_metrics.get('avg_stt_latency_ms') or 0.0):.0f} ms"},
            {"label": "Avg TTS", "value": f"{float(voice_metrics.get('avg_tts_latency_ms') or 0.0):.0f} ms"},
            {"label": "Continued Chat", "value": f"{float(voice_metrics.get('continued_chat_reopen_rate') or 0.0):.0f}%"},
            {"label": "STT Fallbacks", "value": int(voice_metrics.get("stt_fallback_count") or 0)},
            {"label": "TTS Fallbacks", "value": int(voice_metrics.get("tts_fallback_count") or 0)},
            {"label": "STT Backend", "value": effective_stt_backend},
            {"label": "TTS Backend", "value": effective_tts_backend},
            {"label": "TTS Catalog", "value": tts_catalog_count},
            {"label": "Last TTS Refresh", "value": esphome_runtime.text(tts_catalog_updated) or "-"},
        ]
        payload["stats_sections"] = [
            {
                "title": "Native Devices",
                "metrics": [
                    {"label": "Selected", "value": selected},
                    {"label": "Connected", "value": connected},
                    {"label": "Known Satellites", "value": known_satellite_count},
                    {"label": "Transport", "value": "Native WebSocket"},
                ],
            },
            {
                "title": "Turn Outcomes",
                "metrics": [
                    {"label": "Voice Sessions", "value": int(voice_metrics.get("sessions_started") or 0)},
                    {"label": "Valid Turns", "value": int(voice_metrics.get("valid_turns") or 0)},
                    {"label": "No-Ops", "value": int(voice_metrics.get("no_op_turns") or 0)},
                    {"label": "False Wakes", "value": int(voice_metrics.get("false_wake_count") or 0)},
                    {"label": "Wake No Speech", "value": int(voice_metrics.get("wake_no_speech_count") or 0)},
                    {"label": "Low Signal", "value": int(voice_metrics.get("low_signal_count") or 0)},
                    {"label": "Clipped", "value": int(voice_metrics.get("clipped_ambiguous_count") or 0)},
                    {"label": "Blank Wakes", "value": int(voice_metrics.get("blank_wake_count") or 0)},
                ],
            },
            {
                "title": "Latency & Conversation",
                "metrics": [
                    {"label": "Avg Turn", "value": f"{float(voice_metrics.get('avg_turn_latency_ms') or 0.0):.0f} ms"},
                    {"label": "Avg STT", "value": f"{float(voice_metrics.get('avg_stt_latency_ms') or 0.0):.0f} ms"},
                    {"label": "Avg TTS", "value": f"{float(voice_metrics.get('avg_tts_latency_ms') or 0.0):.0f} ms"},
                    {"label": "Avg Speech", "value": f"{float(voice_metrics.get('avg_speech_s') or 0.0):.2f} s"},
                    {"label": "Avg Silence", "value": f"{float(voice_metrics.get('avg_silence_s') or 0.0):.2f} s"},
                    {"label": "Continued Chat", "value": f"{float(voice_metrics.get('continued_chat_reopen_rate') or 0.0):.0f}%"},
                ],
            },
            {
                "title": "Backends & Fallbacks",
                "metrics": [
                    {"label": "STT Backend", "value": effective_stt_backend},
                    {"label": "TTS Backend", "value": effective_tts_backend},
                    {"label": "STT Fallbacks", "value": int(voice_metrics.get("stt_fallback_count") or 0)},
                    {"label": "TTS Fallbacks", "value": int(voice_metrics.get("tts_fallback_count") or 0)},
                    {"label": "TTS Catalog", "value": tts_catalog_count},
                    {"label": "Last TTS Refresh", "value": esphome_runtime.text(tts_catalog_updated) or "-"},
                ],
            },
        ]
        payload["stats_tables"] = [
            {
                "title": "STT Latency By Backend",
                "columns": [
                    {"key": "backend", "label": "Backend"},
                    {"key": "avg_ms", "label": "Avg ms"},
                ],
                "rows": stt_backend_rows,
                "empty_message": "No STT latency samples yet.",
            },
            {
                "title": "TTS Latency By Backend",
                "columns": [
                    {"key": "backend", "label": "Backend"},
                    {"key": "avg_ms", "label": "Avg ms"},
                ],
                "rows": tts_backend_rows,
                "empty_message": "No TTS latency samples yet.",
            },
            {
                "title": "Per-Satellite Voice Summary",
                "columns": [
                    {"key": "satellite", "label": "Satellite"},
                    {"key": "host", "label": "Host"},
                    {"key": "sessions", "label": "Sessions"},
                    {"key": "valid", "label": "Valid"},
                    {"key": "no_ops", "label": "No-Ops"},
                    {"key": "false_wakes", "label": "False Wakes"},
                    {"key": "errors", "label": "Errors"},
                    {"key": "reconnects", "label": "Reconnects"},
                    {"key": "avg_turn_ms", "label": "Avg Turn ms"},
                ],
                "rows": device_rows,
                "empty_message": "No satellite metrics yet.",
            },
        ]
    return payload


def _satellite_display_name(selector: str, row: Dict[str, Any], client_row: Dict[str, Any]) -> str:
    device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
    return (
        esphome_runtime.text(row.get("name"))
        or esphome_runtime.text(device_info.get("friendly_name"))
        or esphome_runtime.text(device_info.get("name"))
        or esphome_runtime.text(row.get("host"))
        or selector
    )


def _identify_satellite(selector: str, *, redis_client: Any = None) -> Dict[str, Any]:
    if not selector:
        raise ValueError("selector is required")

    status = _merge_native_satellites(esphome_runtime.status(), _native_satellite_status_snapshot())
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    client_row = clients.get(selector) if isinstance(clients.get(selector), dict) else {}
    if not client_row or not bool(client_row.get("connected")):
        raise RuntimeError("Satellite is not connected.")

    row = esphome_runtime.satellite_lookup(selector)
    name = _satellite_display_name(selector, row, client_row)

    from speech_settings import get_speech_settings
    from speech_tts import speak_announcement_targets

    speech_settings = get_speech_settings()
    try:
        ha_config = esphome_reply_playback.load_homeassistant_config(required=False, client=redis_client)
    except Exception:
        ha_config = {"base": "", "token": ""}

    backend = esphome_runtime.text(speech_settings.get("announcement_tts_backend")) or esphome_runtime.text(
        speech_settings.get("tts_backend")
    ) or "wyoming"
    result = esphome_runtime.run_async_blocking(
        speak_announcement_targets(
            text=IDENTIFY_SATELLITE_TEXT,
            backend=backend,
            ha_base=esphome_runtime.text(ha_config.get("base")),
            token=esphome_runtime.text(ha_config.get("token")),
            targets=[selector],
            public_base_url="",
            model=esphome_runtime.text(speech_settings.get("announcement_tts_model"))
            or esphome_runtime.text(speech_settings.get("tts_model")),
            voice=esphome_runtime.text(speech_settings.get("announcement_tts_voice"))
            or esphome_runtime.text(speech_settings.get("tts_voice")),
            wyoming_host=esphome_runtime.text(speech_settings.get("announcement_wyoming_tts_host"))
            or esphome_runtime.text(speech_settings.get("wyoming_tts_host")),
            wyoming_port=speech_settings.get("announcement_wyoming_tts_port") or speech_settings.get("wyoming_tts_port"),
            wyoming_voice=esphome_runtime.text(speech_settings.get("announcement_wyoming_tts_voice"))
            or esphome_runtime.text(speech_settings.get("wyoming_tts_voice")),
            openai_base_url=esphome_runtime.text(speech_settings.get("announcement_openai_tts_base_url"))
            or esphome_runtime.text(speech_settings.get("openai_tts_base_url")),
            openai_api_key=esphome_runtime.text(speech_settings.get("announcement_openai_tts_api_key"))
            or esphome_runtime.text(speech_settings.get("openai_tts_api_key")),
            chatterbox_base_url=esphome_runtime.text(speech_settings.get("announcement_chatterbox_tts_base_url"))
            or esphome_runtime.text(speech_settings.get("chatterbox_tts_base_url")),
            chatterbox_voice_mode=esphome_runtime.text(speech_settings.get("announcement_chatterbox_tts_voice_mode"))
            or esphome_runtime.text(speech_settings.get("chatterbox_tts_voice_mode")),
            chatterbox_chunk_size=speech_settings.get("announcement_chatterbox_tts_chunk_size")
            or speech_settings.get("chatterbox_tts_chunk_size"),
            chatterbox_temperature=speech_settings.get("announcement_chatterbox_tts_temperature")
            or speech_settings.get("chatterbox_tts_temperature"),
            chatterbox_exaggeration=speech_settings.get("announcement_chatterbox_tts_exaggeration")
            or speech_settings.get("chatterbox_tts_exaggeration"),
            chatterbox_cfg_weight=speech_settings.get("announcement_chatterbox_tts_cfg_weight")
            or speech_settings.get("chatterbox_tts_cfg_weight"),
            chatterbox_seed=speech_settings.get("announcement_chatterbox_tts_seed")
            or speech_settings.get("chatterbox_tts_seed"),
            chatterbox_speed_factor=speech_settings.get("announcement_chatterbox_tts_speed_factor")
            or speech_settings.get("chatterbox_tts_speed_factor"),
            chatterbox_language=esphome_runtime.text(speech_settings.get("announcement_chatterbox_tts_language"))
            or esphome_runtime.text(speech_settings.get("chatterbox_tts_language")),
            default_backend=backend,
            tts_kind="identify",
        ),
        timeout=120.0,
    )
    if not isinstance(result, dict):
        result = {}
    if not result.get("ok") and int(result.get("sent_count") or 0) <= 0:
        raise RuntimeError(esphome_runtime.text(result.get("error")) or "Identify playback failed.")

    return {
        "ok": True,
        "selector": selector,
        "sent_count": int(result.get("sent_count") or 0),
        "backend": esphome_runtime.text(result.get("backend")) or backend,
        "message": f"Identify message played on {name}.",
    }


def handle_runtime_action(*, action: str, payload: Dict[str, Any], redis_client: Any = None, core_key: str = "voice") -> Dict[str, Any]:
    action_name = esphome_runtime.lower(action)
    body = payload if isinstance(payload, dict) else {}

    firmware_result = esphome_firmware.handle_runtime_action(action_name, body)
    if isinstance(firmware_result, dict):
        return firmware_result

    runtime_status = _runtime_status_with_native()

    speaker_id_result = esphome_speaker_id.handle_runtime_action(action_name, body, runtime_status)
    if isinstance(speaker_id_result, dict):
        return speaker_id_result

    emotion_id_result = esphome_emotion_id.handle_runtime_action(action_name, body, runtime_status)
    if isinstance(emotion_id_result, dict):
        return emotion_id_result

    if action_name == "voice_settings_save":
        values = esphome_runtime.payload_values(body)
        result = esphome_settings.save_settings_values(values)
        with contextlib.suppress(Exception):
            esphome_runtime.reconcile_once(force=True, timeout=45.0)
        updated = int(result.get("updated_count") or 0)
        message = f"Saved {updated} setting(s)." if updated > 0 else "No settings changed."
        return {"ok": True, "action": action_name, "message": message, **result, "status": _runtime_status_with_native()}

    if action_name == "voice_settings_reset_defaults":
        result = esphome_settings.reset_settings_defaults()
        with contextlib.suppress(Exception):
            esphome_runtime.reconcile_once(force=True, timeout=45.0)
        updated = int(result.get("updated_count") or 0)
        message = f"Restored {updated} setting(s) to defaults." if updated > 0 else "Settings already use defaults."
        return {"ok": True, "action": action_name, "message": message, **result, "status": _runtime_status_with_native()}

    if action_name == "voice_satellite_add_manual":
        raise ValueError("Manual legacy satellite add has been removed. Use Add Satellite pairing for Tater Native firmware.")

    if action_name == "voice_native_satellite_pairing_start":
        result = native_satellite.start_pairing_session()
        result["action"] = action_name
        result["message"] = "Pairing code created."
        return result

    if action_name == "voice_native_satellite_pairing_status":
        pairing_id = esphome_runtime.text(body.get("pairing_id") or body.get("id"))
        result = native_satellite.pairing_status(pairing_id)
        result["action"] = action_name
        return result

    if action_name == "voice_native_satellite_settings_save":
        selector = esphome_runtime.payload_selector(body)
        values = esphome_runtime.payload_values(body)
        if not selector:
            raise ValueError("selector is required")
        result = native_satellite.run_on_runtime_loop(
            native_satellite.save_live_settings(values, selector=selector),
            timeout=5.0,
        )
        changed = result.get("changed_keys") if isinstance(result, dict) else []
        changed_count = len(changed or [])
        message = f"Saved {changed_count} native live setting(s)." if changed_count else "No native live settings changed."
        status = _merge_native_satellites(esphome_runtime.status(), _native_satellite_status_snapshot())
        return {
            "ok": True,
            "action": action_name,
            "selector": selector,
            "message": message,
            **(result if isinstance(result, dict) else {}),
            "status": status,
        }

    if action_name == "voice_native_satellite_setup_mode":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        result = native_satellite.run_on_runtime_loop(
            native_satellite.send_command(
                selector,
                "setup.reset",
                {"reason": "user_requested_setup_mode"},
            ),
            timeout=5.0,
        )
        return {
            "ok": True,
            "action": action_name,
            "selector": selector,
            "message": "Setup mode requested. The satellite will reboot and start its setup Wi-Fi network.",
            **(result if isinstance(result, dict) else {}),
            "status": esphome_runtime.status(),
        }

    if action_name == "voice_satellite_save":
        selector = esphome_runtime.payload_selector(body)
        values = esphome_runtime.payload_values(body)
        existing = esphome_runtime.satellite_lookup(selector) if selector else {}
        host = esphome_runtime.lower(values.get("host")) or esphome_runtime.lower(existing.get("host")) or esphome_runtime.satellite_host_from_selector(selector)
        if not selector and host:
            selector = f"host:{host}"
        if not selector:
            raise ValueError("selector is required")
        metadata = dict(existing.get("metadata") or {})
        if "area_name" in values:
            metadata["area_name"] = esphome_runtime.text(values.get("area_name"))
        if "reply_playback_target" in values:
            metadata["reply_playback_target"] = esphome_reply_playback.normalize_reply_playback_target(
                values.get("reply_playback_target")
            )
        name = esphome_runtime.text(values.get("name")) or esphome_runtime.text(existing.get("name")) or host or selector
        source = esphome_runtime.text(existing.get("source")) or "manual"
        esphome_runtime.upsert_satellite({"selector": selector, "host": host, "name": name, "source": source, "metadata": metadata})
        return {"ok": True, "action": action_name, "selector": selector, "message": f"Saved satellite {name}.", "status": esphome_runtime.status()}

    if action_name == "voice_satellite_remove":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        removed = esphome_runtime.remove_satellite(selector)
        with contextlib.suppress(Exception):
            esphome_runtime.disconnect_selector(selector, reason="manual_remove", timeout=20.0)
        return {"ok": True, "action": action_name, "selector": selector, "removed": bool(removed), "message": "Satellite removed." if removed else "Satellite was already absent.", "status": esphome_runtime.status()}

    if action_name == "voice_satellite_identify":
        selector = esphome_runtime.payload_selector(body)
        result = _identify_satellite(selector, redis_client=redis_client)
        return {"action": action_name, "status": esphome_runtime.status(), **result}

    if action_name == "voice_discover":
        return {"ok": True, "action": action_name, "count": 0, "message": "Legacy mDNS discovery is disabled. Use Add Satellite pairing.", "status": esphome_runtime.status()}

    if action_name == "voice_reconcile":
        status = esphome_runtime.reconcile_once(force=True, timeout=45.0)
        return {"ok": True, "action": action_name, "status": status, "message": "Legacy satellite reconcile is disabled."}

    if action_name == "voice_refresh":
        status = esphome_runtime.reconcile_once(force=True, timeout=45.0)
        return {
            "ok": True,
            "action": action_name,
            "count": 0,
            "status": status,
            "message": "Native satellite status refreshed. Add new devices with pairing.",
        }

    if action_name == "voice_entity_refresh":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        result = esphome_runtime.refresh_entity_catalog(selector, timeout=20.0)
        entity_rows = list(result.get("entity_rows") or []) if isinstance(result, dict) else []
        wake_engine_found = any(esphome_runtime.lower(row.get("label")) == "wake engine" for row in entity_rows if isinstance(row, dict))
        return {
            "ok": True,
            "action": action_name,
            "selector": selector,
            "entity_rows": entity_rows,
            "message": "Live entities refreshed." if wake_engine_found else "Live entities refreshed, but Wake Engine was not exposed by this firmware.",
        }

    if action_name == "voice_connect":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        esphome_runtime.set_satellite_selected(selector, True)
        status = esphome_runtime.reconcile_once(force=True, timeout=45.0)
        return {"ok": True, "action": action_name, "selector": selector, "status": status, "message": "Satellite selected and connect requested."}

    if action_name == "voice_disconnect":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        esphome_runtime.set_satellite_selected(selector, False)
        esphome_runtime.disconnect_selector(selector, reason="manual_disconnect", timeout=20.0)
        return {"ok": True, "action": action_name, "selector": selector, "status": esphome_runtime.status(), "message": "Satellite disconnected and deselected."}

    if action_name == "voice_entity_command":
        selector = esphome_runtime.payload_selector(body)
        entity_key = esphome_runtime.text(body.get("entity_key") or body.get("key"))
        command = esphome_runtime.text(body.get("command"))
        if not selector:
            raise ValueError("selector is required")
        if not entity_key:
            raise ValueError("entity_key is required")
        if not command:
            raise ValueError("command is required")
        result = esphome_runtime.command_entity(
            selector,
            entity_key=entity_key,
            command=command,
            value=body.get("value"),
            options=body,
            timeout=20.0,
        )
        entity_rows = list(result.get("entity_rows") or []) if isinstance(result, dict) else []
        return {
            "ok": True,
            "action": action_name,
            "selector": selector,
            "entity_key": entity_key,
            "command": command,
            "entity_rows": entity_rows,
            "message": f"Updated {esphome_runtime.text(body.get('entity_label')) or 'entity'}.",
        }

    if action_name == "voice_logs_start":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        if esphome_runtime.lower(selector).startswith("native:"):
            result = _native_logs_payload(selector, start=True)
            result["action"] = action_name
            return result
        result = esphome_runtime.logs_start(selector, timeout=20.0)
        result["action"] = action_name
        return result

    if action_name == "voice_logs_poll":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        after_seq = esphome_runtime.as_int(body.get("after_seq"), 0, minimum=0)
        if esphome_runtime.lower(selector).startswith("native:"):
            result = _native_logs_payload(selector, after_seq=after_seq)
            result["action"] = action_name
            return result
        result = esphome_runtime.logs_poll(selector, after_seq=after_seq, timeout=20.0)
        result["action"] = action_name
        return result

    if action_name == "voice_logs_stop":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        force = esphome_runtime.as_bool(body.get("force"), False)
        if esphome_runtime.lower(selector).startswith("native:"):
            result = _native_logs_payload(selector, stop=True)
            result["action"] = action_name
            return result
        result = esphome_runtime.logs_stop(selector, force=force, timeout=20.0)
        result["action"] = action_name
        return result

    raise ValueError(f"Unknown action: {action_name}")


def include_routes(app: Any) -> None:
    esphome_runtime.include_routes(app)


def raise_unavailable_settings_error() -> None:
    raise HTTPException(status_code=500, detail="Built-in native satellite services are unavailable.")
