from __future__ import annotations

import contextlib
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from . import runtime as esphome_runtime
from . import firmware as esphome_firmware
from . import native_live_settings
from . import native_satellite
from . import reply_playback as esphome_reply_playback
from . import settings as esphome_settings
from . import speaker_id as esphome_speaker_id
from . import stereo_pairs
from . import wake_trainer_link
from . import emotion_id as esphome_emotion_id

IDENTIFY_SATELLITE_TEXT = (
    "Hey, over here. This is the satellite you're looking for. Yeah, over here. Can you hear me?"
)


def _local_timestamp_label(value: Any) -> str:
    try:
        timestamp = float(value or 0.0)
    except Exception:
        timestamp = 0.0
    if timestamp <= 0.0:
        return "—"
    return datetime.fromtimestamp(timestamp).astimezone().strftime("%b %d, %Y %I:%M %p")


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
    esphome_settings.cleanup_removed_user_settings()
    native_satellite.bind_runtime_loop()
    await esphome_runtime.startup()


async def shutdown() -> None:
    await esphome_runtime.shutdown()


def _runtime_panel_token(panel: Any = "") -> str:
    token = esphome_runtime.lower(panel)
    return token if token in {"satellites", "firmware", "stereo", "platform", "speakerid", "emotionid", "stats"} else ""


def _native_satellite_status_snapshot() -> Dict[str, Any]:
    try:
        result = native_satellite.status_snapshot_sync()
    except Exception:
        return {}
    return result if isinstance(result, dict) else {}


def _global_satellite_settings_item_form(native_status: Dict[str, Any]) -> Dict[str, Any]:
    clients = native_status.get("clients") if isinstance(native_status.get("clients"), dict) else {}
    connected = len(
        [
            row
            for row in clients.values()
            if isinstance(row, dict) and bool(row.get("connected"))
        ]
    )
    return {
        "id": "voice_global_satellite_settings",
        "group": "global_satellite_settings",
        "title": "Voice Runtime Settings",
        "subtitle": (
            "Shared by every Tater Native satellite. Saving applies these settings immediately "
            f"to all connected satellites ({connected} connected)."
        ),
        "sections": native_live_settings.global_voice_runtime_settings_sections(),
        "save_action": "voice_global_satellite_settings_save",
        "save_label": "Apply To All Satellites",
        "remove_action": "",
    }


def _global_satellite_model_settings_item_form(native_status: Dict[str, Any]) -> Dict[str, Any]:
    clients = native_status.get("clients") if isinstance(native_status.get("clients"), dict) else {}
    connected = len(
        [
            row
            for row in clients.values()
            if isinstance(row, dict) and bool(row.get("connected"))
        ]
    )
    return {
        "id": "voice_global_satellite_model_settings",
        "group": "global_satellite_model_settings",
        "title": "Wake Word",
        "subtitle": (
            "Shared by every Tater Native satellite. Saving applies the wake model and trainer settings immediately "
            f"to all connected satellites ({connected} connected)."
        ),
        "sections": native_live_settings.global_model_settings_sections(),
        "save_action": "voice_global_satellite_settings_save",
        "save_label": "Apply To All Satellites",
        "remove_action": "",
    }


def _stereo_pair_member_options(
    native_status: Dict[str, Any],
    *,
    current_selector: str = "",
) -> List[Dict[str, str]]:
    clients = native_status.get("clients") if isinstance(native_status.get("clients"), dict) else {}
    rows: List[Dict[str, str]] = [{"value": "", "label": "Select a satellite"}]
    seen = {""}
    required = {
        "synchronized_media_sessions",
        "stereo_channel_selection",
        "media_playhead_telemetry",
        "media_drift_correction",
    }
    for selector, raw in sorted(clients.items(), key=lambda item: esphome_runtime.text(item[0])):
        if not isinstance(raw, dict) or not bool(raw.get("connected")):
            continue
        token = esphome_runtime.text(selector)
        capabilities = raw.get("capabilities") if isinstance(raw.get("capabilities"), dict) else {}
        try:
            session_version = int(float(capabilities.get("audio_session_version") or 0))
        except Exception:
            session_version = 0
        if session_version < 2 or any(not bool(capabilities.get(name)) for name in required):
            continue
        name = esphome_runtime.text(raw.get("device_name")) or token
        room = esphome_runtime.text(raw.get("room"))
        suffix = f" • {room}" if room and room.lower() != name.lower() else ""
        rows.append({"value": token, "label": f"{name}{suffix}"})
        seen.add(token)
    current = esphome_runtime.text(current_selector)
    if current and current not in seen:
        rows.append({"value": current, "label": f"{current} (offline or update required)"})
    return rows


def _stereo_pair_ready(pair: Dict[str, Any], native_status: Dict[str, Any]) -> bool:
    clients = native_status.get("clients") if isinstance(native_status.get("clients"), dict) else {}
    for selector in (
        esphome_runtime.text(pair.get("left_selector")),
        esphome_runtime.text(pair.get("right_selector")),
    ):
        row = clients.get(selector) if isinstance(clients.get(selector), dict) else {}
        capabilities = row.get("capabilities") if isinstance(row.get("capabilities"), dict) else {}
        try:
            session_version = int(float(capabilities.get("audio_session_version") or 0))
        except Exception:
            session_version = 0
        if (
            not bool(row.get("connected"))
            or session_version < 2
            or not bool(capabilities.get("synchronized_media_sessions"))
            or not bool(capabilities.get("stereo_channel_selection"))
            or not bool(capabilities.get("media_playhead_telemetry"))
            or not bool(capabilities.get("media_drift_correction"))
        ):
            return False
    return True


def _stereo_pair_fields(pair: Dict[str, Any], native_status: Dict[str, Any]) -> List[Dict[str, Any]]:
    left_selector = esphome_runtime.text(pair.get("left_selector"))
    right_selector = esphome_runtime.text(pair.get("right_selector"))
    return [
        {
            "key": "name",
            "label": "Pair Name",
            "type": "text",
            "value": esphome_runtime.text(pair.get("name")),
            "placeholder": "Master Bedroom Stereo",
            "description": "This appears as one destination throughout Tater.",
        },
        {
            "key": "left_selector",
            "label": "Left Satellite",
            "type": "select",
            "value": left_selector,
            "options": _stereo_pair_member_options(native_status, current_selector=left_selector),
        },
        {
            "key": "right_selector",
            "label": "Right Satellite",
            "type": "select",
            "value": right_selector,
            "options": _stereo_pair_member_options(native_status, current_selector=right_selector),
        },
        {
            "key": "left_volume_percent",
            "label": "Left Balance",
            "type": "number",
            "value": int(
                pair.get("left_volume_percent")
                if pair.get("left_volume_percent") is not None
                else 100
            ),
            "min": 0,
            "max": 100,
            "step": 1,
            "suffix": "%",
        },
        {
            "key": "right_volume_percent",
            "label": "Right Balance",
            "type": "number",
            "value": int(
                pair.get("right_volume_percent")
                if pair.get("right_volume_percent") is not None
                else 100
            ),
            "min": 0,
            "max": 100,
            "step": 1,
            "suffix": "%",
        },
        {
            "key": "left_delay_ms",
            "label": "Left Delay",
            "type": "number",
            "value": int(pair.get("left_delay_ms") or 0),
            "min": 0,
            "max": 250,
            "step": 1,
            "suffix": "ms",
            "description": "Optional acoustic calibration. Leave both delays at zero initially.",
        },
        {
            "key": "right_delay_ms",
            "label": "Right Delay",
            "type": "number",
            "value": int(pair.get("right_delay_ms") or 0),
            "min": 0,
            "max": 250,
            "step": 1,
            "suffix": "ms",
        },
    ]


def _stereo_pair_item_forms(native_status: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for pair in stereo_pairs.list_pairs():
        ready = _stereo_pair_ready(pair, native_status)
        rows.append(
            {
                "id": pair.get("selector"),
                "group": "stereo_pair",
                "title": pair.get("name") or "Stereo Pair",
                "subtitle": "Synchronized left/right Tater satellite pair",
                "detail": (
                    "Ready for synchronized playback"
                    if ready
                    else "Unavailable until both satellites are connected with current firmware"
                ),
                "connected": ready,
                "hero_badges": [
                    {"label": "Stereo", "tone": "active"},
                    {"label": "Ready" if ready else "Unavailable", "tone": "active" if ready else "warning"},
                ],
                "summary_rows": [
                    {"label": "Left", "value": pair.get("left_selector")},
                    {"label": "Right", "value": pair.get("right_selector")},
                ],
                "fields": _stereo_pair_fields(pair, native_status),
                "save_action": "voice_stereo_pair_save",
                "save_label": "Save Pair",
                "remove_action": "voice_stereo_pair_remove",
                "remove_label": "Delete Pair",
                "remove_confirm": f"Delete stereo pair {pair.get('name') or pair.get('id')}?",
                "show_entity_refresh": False,
            }
        )
    rows.append(
        {
            "id": "stereo:new",
            "group": "stereo_pair_create",
            "title": "Create Stereo Pair",
            "subtitle": "Combine two updated Tater Native satellites into one synchronized destination.",
            "detail": "Choose which satellite is physically on the left and right.",
            "connected": False,
            "hero_badges": [{"label": "New Pair", "tone": "muted"}],
            "fields": _stereo_pair_fields(
                {
                    "name": "",
                    "left_volume_percent": 100,
                    "right_volume_percent": 100,
                    "left_delay_ms": 0,
                    "right_delay_ms": 0,
                },
                native_status,
            ),
            "save_action": "voice_stereo_pair_save",
            "save_label": "Create Pair",
            "show_entity_refresh": False,
        }
    )
    return rows


def _wake_trainer_link_item_form() -> Dict[str, Any]:
    link = wake_trainer_link.status()
    linked = bool(link.get("linked"))
    return {
        "id": "voice_wake_trainer_link",
        "group": "wake_trainer_link",
        "title": "Wake Word Trainer",
        "subtitle": (
            "Securely link the trainer with a short pairing code. Once linked, only that trainer "
            "can publish a new wake word to every satellite."
        ),
        "linked": linked,
        "status_label": "Linked" if linked else "Not Linked",
        "trainer_name": esphome_runtime.text(link.get("trainer_name")) or "Wake Word Trainer",
        "trainer_url": esphome_runtime.text(link.get("trainer_url")),
        "publish_base_url": esphome_runtime.text(link.get("publish_base_url")),
        "linked_at": esphome_runtime.text(link.get("linked_at")),
        "last_publish_at": esphome_runtime.text(link.get("last_publish_at")),
        "last_wake_word": esphome_runtime.text(link.get("last_wake_word")),
        "start_action": "voice_wake_trainer_link_pairing_start",
        "status_action": "voice_wake_trainer_link_pairing_status",
        "unlink_action": "voice_wake_trainer_link_unlink",
    }


def _wake_verifier_item_form(native_status: Dict[str, Any]) -> Dict[str, Any]:
    try:
        from . import voice_pipeline as vp

        selected_stt_engine = esphome_runtime.text(vp._selected_stt_backend())
    except Exception:
        selected_stt_engine = ""
    clients = native_status.get("clients") if isinstance(native_status.get("clients"), dict) else {}
    voice_metrics = esphome_runtime.voice_metrics_snapshot()
    metrics_devices = voice_metrics.get("devices") if isinstance(voice_metrics.get("devices"), dict) else {}
    persistent_period = float(voice_metrics.get("period_started_ts") or 0.0) > 0.0
    retention_days = int(voice_metrics.get("retention_days") or 30)
    registry = {
        esphome_runtime.text(row.get("selector")): row
        for row in esphome_runtime.load_satellite_registry()
        if isinstance(row, dict) and _is_native_satellite_row(row) and esphome_runtime.text(row.get("selector"))
    }
    selectors = set(clients.keys()) | {
        esphome_runtime.text(selector)
        for selector in metrics_devices.keys()
        if esphome_runtime.text(selector).startswith("native:")
    }
    rows: List[Dict[str, Any]] = []
    total_checks = 0
    total_rejected = 0
    total_fail_open = 0

    for selector in sorted(selectors, key=esphome_runtime.text):
        raw_row = clients.get(selector) if isinstance(clients.get(selector), dict) else {}
        saved_row = registry.get(selector) if isinstance(registry.get(selector), dict) else {}
        server = raw_row.get("wake_verifier") if isinstance(raw_row.get("wake_verifier"), dict) else {}
        last = server.get("last") if isinstance(server.get("last"), dict) else {}
        last_status = raw_row.get("last_status") if isinstance(raw_row.get("last_status"), dict) else {}
        wake_engine = last_status.get("wake_engine") if isinstance(last_status.get("wake_engine"), dict) else {}
        device = wake_engine.get("verifier") if isinstance(wake_engine.get("verifier"), dict) else {}
        persisted = metrics_devices.get(selector) if isinstance(metrics_devices.get(selector), dict) else {}

        server_checks = int(server.get("count") or 0)
        server_rejected = int(server.get("rejections") or 0)
        device_checks = int(device.get("completed") or 0)
        device_rejected = int(device.get("rejections") or 0)
        if persistent_period:
            checks = int(persisted.get("wake_verifier_checks") or 0)
            rejected = min(checks, int(persisted.get("wake_verifier_rejections") or 0))
            fail_open = int(persisted.get("wake_verifier_fail_open") or 0)
            last = persisted.get("wake_verifier_last") if isinstance(persisted.get("wake_verifier_last"), dict) else {}
        else:
            checks = max(server_checks, device_checks)
            rejected = min(checks, max(server_rejected, device_rejected))
            fail_open = int(device.get("fail_open") or 0)
        accepted = max(0, checks - rejected)
        total_checks += checks
        total_rejected += rejected
        total_fail_open += fail_open

        connected = bool(raw_row.get("connected"))
        supported = bool(device) or bool(last)
        if connected and supported:
            status_label = "Ready"
        elif connected:
            status_label = "No verifier firmware"
        else:
            status_label = "Offline"

        if last:
            if not bool(last.get("available", True)):
                last_result = "Fail-open"
            else:
                last_result = "Accepted" if bool(last.get("accepted")) else "Rejected"
        else:
            reason = "" if persistent_period else esphome_runtime.text(device.get("last_reason"))
            last_result = reason.replace("_", " ").title() if reason else "—"

        transcript = esphome_runtime.text(last.get("transcript")) or "—"
        score = f"{float(last.get('score') or 0.0):.3f}" if last else "—"
        stt_ms = f"{float(last.get('stt_ms') or 0.0):.1f} ms" if last else "—"
        stt_engine = (
            esphome_runtime.text(last.get("stt_engine"))
            or esphome_runtime.text(last.get("stt_engine_selected"))
            or selected_stt_engine
            or "—"
        )
        name = (
            esphome_runtime.text(raw_row.get("device_name"))
            or esphome_runtime.text(raw_row.get("name"))
            or esphome_runtime.text(saved_row.get("name"))
            or esphome_runtime.text(selector)
        )
        rows.append(
            {
                "satellite": name,
                "status": status_label,
                "checks": checks,
                "accepted": accepted,
                "rejected": rejected,
                "fail_open": fail_open,
                "last_result": last_result,
                "transcript": transcript,
                "score": score,
                "stt_ms": stt_ms,
                "stt_engine": stt_engine,
            }
        )

    accepted_total = max(0, total_checks - total_rejected)
    mode = esphome_settings.wake_verifier_mode()
    return {
        "id": "global_wake_verifier",
        "group": "wake_verifier",
        "title": "STT Wake Verification",
        "subtitle": (
            "Applies one mode to every Tater Native satellite. Observe records decisions without blocking; "
            "Enabled rejects transcript mismatches and opens the mic if Tater errors or exceeds the 500 ms deadline. "
            "Verification uses the STT backend selected in Model Settings."
        ),
        "sections": [
            {
                "label": "Global Mode",
                "fields": [
                    {
                        "key": esphome_settings.VOICE_WAKE_VERIFIER_MODE_KEY,
                        "label": "Wake Verifier",
                        "type": "select",
                        "value": mode,
                        "default": "off",
                        "options": [
                            {"value": "off", "label": "Disabled"},
                            {"value": "observe", "label": "Observe"},
                            {"value": "enforce", "label": "Enabled"},
                        ],
                        "description": "The selected mode is pushed immediately to all connected satellites and is sent to satellites that connect later.",
                    },
                    {
                        "key": "wake_verifier_summary",
                        "label": "Current Results",
                        "type": "text",
                        "read_only": True,
                        "value": (
                            f"{total_checks} checks • {accepted_total} accepted • {total_rejected} rejected • "
                            f"{total_fail_open} fail-open"
                        ),
                        "description": f"Stored in Redis for a {retention_days}-day statistics period and preserved across Tater and satellite restarts.",
                    },
                    {
                        "key": "wake_verifier_stt_engine",
                        "label": "Configured STT Engine",
                        "type": "text",
                        "read_only": True,
                        "value": selected_stt_engine or "Unavailable",
                        "description": "Wake verification follows the STT backend selected in Model Settings. A runtime fallback is shown per satellite when the selected local backend is unavailable.",
                    },
                ],
            },
            {
                "label": "Results By Satellite",
                "fields": [
                    {
                        "key": "wake_verifier_results",
                        "label": "Latest verifier results",
                        "type": "table",
                        "full_width": True,
                        "columns": [
                            {"key": "satellite", "label": "Satellite"},
                            {"key": "status", "label": "Status"},
                            {"key": "checks", "label": "Checks"},
                            {"key": "accepted", "label": "Accepted"},
                            {"key": "rejected", "label": "Rejected"},
                            {"key": "fail_open", "label": "Fail-open"},
                            {"key": "last_result", "label": "Last"},
                            {"key": "transcript", "label": "Transcript"},
                            {"key": "score", "label": "Score"},
                            {"key": "stt_engine", "label": "STT Engine"},
                            {"key": "stt_ms", "label": "STT"},
                        ],
                        "rows": rows,
                        "description": "Observe mode populates this table while allowing every wake to continue normally.",
                    }
                ],
            },
        ],
        "save_action": "voice_wake_verifier_save",
        "save_label": "Apply To All Satellites",
        "reset_action": "voice_wake_verifier_stats_reset",
        "reset_label": "Reset Verification Stats",
        "reset_confirm": "Reset stored STT wake-verification statistics for every satellite?",
        "remove_action": "",
    }


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
        _native_detail_row("native_led_brightness", "LED Brightness", f"{live_settings.get('led_brightness', 80)}%"),
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
    board_token = esphome_runtime.lower(board).replace("_", "-").replace(" ", "-")
    compact_board = board_token.replace("-", "")
    is_s3_box = board_token in {"s3-box", "s3-box-3", "esp32-s3-box", "esp32-s3-box-3"} or compact_board in {
        "s3box",
        "s3box3",
        "esp32s3box",
        "esp32s3box3",
    }
    if is_s3_box:
        settings_rows = [
            item
            for item in settings_rows
            if esphome_runtime.text(item.get("key"))
            not in {"native_led_brightness", "native_led_color", "native_led_animations"}
        ]
        night_enabled = esphome_runtime.as_bool(live_settings.get("screen_night_mode_enabled"), False)
        night_label = "Off"
        if night_enabled:
            night_label = (
                f"{live_settings.get('screen_night_start') or '22:00'}–"
                f"{live_settings.get('screen_night_end') or '07:00'} at "
                f"{live_settings.get('screen_night_brightness', 10)}%"
            )
        volume_index = next(
            (
                index + 1
                for index, item in enumerate(settings_rows)
                if esphome_runtime.text(item.get("key")) == "native_volume"
            ),
            len(settings_rows),
        )
        settings_rows[volume_index:volume_index] = [
            _native_detail_row(
                "native_screen_brightness",
                "Screen Brightness",
                f"{live_settings.get('screen_brightness', 80)}%",
            ),
            _native_detail_row("native_screen_night_mode", "Night Dimming", night_label),
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


def _has_specific_native_board(value: Any) -> bool:
    return esphome_runtime.lower(value) not in {"", "unknown", "native", "native satellite"}


def _merge_saved_native_satellites(status: Dict[str, Any]) -> Dict[str, Any]:
    clients = dict(status.get("clients") if isinstance(status.get("clients"), dict) else {})
    for saved_row in esphome_runtime.load_satellite_registry():
        if not isinstance(saved_row, dict) or not _is_native_satellite_row(saved_row):
            continue
        selector = esphome_runtime.text(saved_row.get("selector"))
        if not selector:
            continue

        saved_meta = saved_row.get("metadata") if isinstance(saved_row.get("metadata"), dict) else {}
        saved_board = esphome_runtime.text(saved_meta.get("board"))
        saved_firmware = esphome_runtime.text(saved_meta.get("firmware_version"))
        saved_name = esphome_runtime.text(saved_row.get("name")) or selector
        saved_host = esphome_runtime.text(saved_row.get("host"))
        current = dict(clients.get(selector) if isinstance(clients.get(selector), dict) else {})
        current_meta = current.get("metadata") if isinstance(current.get("metadata"), dict) else {}
        current_info = current.get("device_info") if isinstance(current.get("device_info"), dict) else {}

        metadata = {**saved_meta, **current_meta}
        if saved_board and not _has_specific_native_board(metadata.get("board")):
            metadata["board"] = saved_board
        metadata.setdefault("native_selected", True)
        metadata["native_connected"] = bool(current.get("connected"))

        device_info = dict(current_info)
        current_model = device_info.get("model") or current.get("board")
        if saved_board and not _has_specific_native_board(current_model):
            device_info["model"] = saved_board
        if saved_firmware and not esphome_runtime.text(device_info.get("project_version")):
            device_info["project_version"] = saved_firmware
        device_info.setdefault("project_name", "tater.native_satellite")
        device_info.setdefault("manufacturer", "Tater")
        if not esphome_runtime.text(device_info.get("name")):
            device_info["name"] = selector
        if not esphome_runtime.text(device_info.get("friendly_name")):
            device_info["friendly_name"] = saved_name

        current.update(
            {
                "selector": selector,
                "host": esphome_runtime.text(current.get("host")) or saved_host,
                "source": esphome_runtime.text(current.get("source")) or "tater_native",
                "name": esphome_runtime.text(current.get("name")) or saved_name,
                "selected": bool(current.get("selected", metadata.get("native_selected", True))),
                "connected": bool(current.get("connected")),
                "metadata": metadata,
                "device_info": device_info,
                "last_seen_ts": float(current.get("last_seen_ts") or saved_row.get("last_seen_ts") or 0.0),
            }
        )
        clients[selector] = current

    merged = dict(status)
    merged["clients"] = clients
    return merged


def _runtime_status_with_native(native_status: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    status = esphome_runtime.status()
    native = native_status if isinstance(native_status, dict) else _native_satellite_status_snapshot()
    return _merge_saved_native_satellites(_merge_native_satellites(status, native))


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
    include_stereo_pairs = panel_token in {"", "stereo"}
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
    item_forms = [
        _global_satellite_settings_item_form(native_status),
        _global_satellite_model_settings_item_form(native_status),
        _wake_trainer_link_item_form(),
        _wake_verifier_item_form(native_status),
    ]
    if include_satellites:
        item_forms.append(esphome_settings.settings_item_form())
        item_forms.extend(esphome_settings.satellite_item_forms(status))
        payload["display_sensors"] = esphome_firmware.display_sensor_profiles_payload(status)
    if include_stereo_pairs:
        item_forms.extend(_stereo_pair_item_forms(native_status))
    payload["ui"]["item_forms"] = item_forms

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
                "title": "Statistics Period",
                "metrics": [
                    {"label": "Retention", "value": f"{int(voice_metrics.get('retention_days') or 30)} days"},
                    {"label": "Started", "value": _local_timestamp_label(voice_metrics.get("period_started_ts"))},
                    {"label": "Automatic Reset", "value": _local_timestamp_label(voice_metrics.get("period_expires_ts"))},
                    {"label": "Storage", "value": "Redis"},
                ],
            },
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
        payload["stats_controls"] = {
            "id": "voice_statistics",
            "reset_action": "voice_statistics_reset",
            "reset_label": "Reset All Voice Statistics",
            "reset_confirm": "Reset all stored voice and STT wake-verification statistics for every satellite?",
            "description": "Statistics are stored in Redis, survive restarts, and reset automatically after the retention period.",
        }
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

    if action_name == "voice_wake_trainer_link_pairing_start":
        result = wake_trainer_link.start_pairing()
        return {
            "ok": True,
            "action": action_name,
            **result,
            "wake_trainer_link": _wake_trainer_link_item_form(),
        }

    if action_name == "voice_wake_trainer_link_pairing_status":
        values = esphome_runtime.payload_values(body)
        result = wake_trainer_link.pairing_status(values.get("pairing_id"))
        return {
            "ok": True,
            "action": action_name,
            **result,
            "wake_trainer_link": _wake_trainer_link_item_form(),
        }

    if action_name == "voice_wake_trainer_link_unlink":
        result = wake_trainer_link.unlink()
        return {
            "ok": True,
            "action": action_name,
            **result,
            "wake_trainer_link": _wake_trainer_link_item_form(),
        }

    if action_name == "voice_global_satellite_settings_save":
        values = native_live_settings.resolve_wake_word_source_values(
            esphome_runtime.payload_values(body)
        )
        allowed_values = {
            key: value
            for key, value in values.items()
            if key in native_live_settings.GLOBAL_SATELLITE_CONTROL_KEYS
        }
        if not allowed_values:
            raise ValueError("No global satellite settings were provided.")
        result = native_satellite.run_on_runtime_loop(
            native_satellite.save_live_settings(allowed_values),
            timeout=15.0,
        )
        if "continued_chat" in allowed_values:
            esphome_settings.save_settings_values(
                {
                    "VOICE_CONTINUED_CHAT_ENABLED": esphome_runtime.as_bool(
                        allowed_values.get("continued_chat"),
                        True,
                    )
                }
            )
        push = result.get("push") if isinstance(result, dict) and isinstance(result.get("push"), dict) else {}
        pushed_count = int(push.get("count") or 0)
        return {
            "ok": True,
            "action": action_name,
            "message": f"Applied shared voice settings to {pushed_count} connected satellite(s).",
            **(result if isinstance(result, dict) else {}),
            "global_satellite_settings": _global_satellite_settings_item_form(
                _native_satellite_status_snapshot()
            ),
            "global_satellite_model_settings": _global_satellite_model_settings_item_form(
                _native_satellite_status_snapshot()
            ),
        }

    if action_name in {"voice_wake_verifier_stats_reset", "voice_statistics_reset"}:
        if action_name == "voice_statistics_reset":
            result = esphome_runtime.reset_voice_metrics()
            message = "All stored voice statistics were reset. A new 30-day statistics period has started."
        else:
            result = esphome_runtime.reset_wake_verifier_metrics()
            message = "Stored STT wake-verification statistics were reset for every satellite."
        try:
            native_reset = native_satellite.run_on_runtime_loop(
                native_satellite.reset_wake_verifier_runtime_stats(),
                timeout=5.0,
            )
        except Exception:
            native_reset = {"ok": False, "cleared_clients": 0}
        return {
            "ok": True,
            "action": action_name,
            "message": message,
            **result,
            "native": native_reset,
        }

    if action_name == "voice_wake_verifier_save":
        values = esphome_runtime.payload_values(body)
        mode = esphome_runtime.lower(values.get(esphome_settings.VOICE_WAKE_VERIFIER_MODE_KEY))
        if mode not in esphome_settings.VOICE_WAKE_VERIFIER_MODES:
            raise ValueError("Wake verifier mode must be Disabled, Observe, or Enabled.")
        result = esphome_settings.save_settings_values(
            {esphome_settings.VOICE_WAKE_VERIFIER_MODE_KEY: mode}
        )
        push = native_satellite.run_on_runtime_loop(
            native_satellite.push_live_settings(),
            timeout=10.0,
        )
        label = {"off": "Disabled", "observe": "Observe", "enforce": "Enabled"}[mode]
        pushed_count = int((push or {}).get("count") or 0) if isinstance(push, dict) else 0
        return {
            "ok": True,
            "action": action_name,
            "message": f"Wake verification set to {label} for {pushed_count} connected satellite(s).",
            **result,
            "push": push,
            "wake_verifier": _wake_verifier_item_form(_native_satellite_status_snapshot()),
        }

    if action_name == "voice_settings_save":
        values = esphome_runtime.payload_values(body)
        result = esphome_settings.save_settings_values(values)
        updated = int(result.get("updated_count") or 0)
        message = f"Saved {updated} setting(s)." if updated > 0 else "No settings changed."
        return {"ok": True, "action": action_name, "message": message, **result, "status": _runtime_status_with_native()}

    if action_name == "voice_settings_reset_defaults":
        result = esphome_settings.reset_settings_defaults()
        updated = int(result.get("updated_count") or 0)
        message = f"Restored {updated} setting(s) to defaults." if updated > 0 else "Settings already use defaults."
        return {"ok": True, "action": action_name, "message": message, **result, "status": _runtime_status_with_native()}

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

    if action_name == "voice_stereo_pair_save":
        selector = esphome_runtime.payload_selector(body)
        values = esphome_runtime.payload_values(body)
        compatibility = native_satellite.run_on_runtime_loop(
            native_satellite.stereo_pair_compatibility(
                esphome_runtime.text(values.get("left_selector")),
                esphome_runtime.text(values.get("right_selector")),
            ),
            timeout=8.0,
        )
        if not isinstance(compatibility, dict) or not bool(compatibility.get("ok")):
            raise ValueError(
                esphome_runtime.text((compatibility or {}).get("error"))
                or "The selected satellites are not ready for stereo pairing."
            )
        existing_id = stereo_pairs.pair_id_from_selector(selector)
        saved = stereo_pairs.save_pair(values, pair_id=existing_id)
        return {
            "ok": True,
            "action": action_name,
            "selector": saved.get("selector"),
            "pair": saved,
            "message": f"Saved stereo pair {saved.get('name')}.",
        }

    if action_name == "voice_stereo_pair_remove":
        selector = esphome_runtime.payload_selector(body)
        removed = stereo_pairs.remove_pair(selector)
        return {
            "ok": True,
            "action": action_name,
            **removed,
            "message": "Stereo pair deleted." if removed.get("removed") else "Stereo pair was already removed.",
        }

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
        return {"ok": True, "action": action_name, "selector": selector, "removed": bool(removed), "message": "Satellite removed." if removed else "Satellite was already absent.", "status": esphome_runtime.status()}

    if action_name == "voice_satellite_identify":
        selector = esphome_runtime.payload_selector(body)
        result = _identify_satellite(selector, redis_client=redis_client)
        return {"action": action_name, "status": esphome_runtime.status(), **result}

    if action_name == "voice_refresh":
        status = _runtime_status_with_native()
        return {
            "ok": True,
            "action": action_name,
            "count": 0,
            "status": status,
            "message": "Native satellite status refreshed. Add new devices with pairing.",
        }

    if action_name == "voice_logs_start":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        if not esphome_runtime.lower(selector).startswith("native:"):
            raise ValueError("Only Tater Native satellite logs are supported.")
        result = _native_logs_payload(selector, start=True)
        result["action"] = action_name
        return result

    if action_name == "voice_logs_poll":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        after_seq = esphome_runtime.as_int(body.get("after_seq"), 0, minimum=0)
        if not esphome_runtime.lower(selector).startswith("native:"):
            raise ValueError("Only Tater Native satellite logs are supported.")
        result = _native_logs_payload(selector, after_seq=after_seq)
        result["action"] = action_name
        return result

    if action_name == "voice_logs_stop":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        if not esphome_runtime.lower(selector).startswith("native:"):
            raise ValueError("Only Tater Native satellite logs are supported.")
        result = _native_logs_payload(selector, stop=True)
        result["action"] = action_name
        return result

    raise ValueError(f"Unknown action: {action_name}")


def include_routes(app: Any) -> None:
    esphome_runtime.include_routes(app)


def raise_unavailable_settings_error() -> None:
    raise HTTPException(status_code=500, detail="Built-in native satellite services are unavailable.")
