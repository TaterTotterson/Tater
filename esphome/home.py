from __future__ import annotations

import contextlib
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from . import runtime as esphome_runtime
from . import firmware as esphome_firmware
from . import settings as esphome_settings


def settings_hash_key() -> str:
    return esphome_settings.settings_hash_key()


def settings_fields() -> List[Dict[str, Any]]:
    rows = esphome_settings.settings_fields()
    return rows if isinstance(rows, list) else []


def settings_item_form() -> Dict[str, Any]:
    form = esphome_settings.settings_item_form()
    return form if isinstance(form, dict) else {}


def save_settings_values(values: Dict[str, Any]) -> Dict[str, Any]:
    result = esphome_settings.save_settings_values(values or {})
    return result if isinstance(result, dict) else {"ok": True}


def runtime_tab_spec() -> Dict[str, Any]:
    return {
        "label": "ESPHome",
        "core_key": "esphome",
        "surface_key": "esphome",
        "surface_kind": "esphome",
        "order": 40,
        "requires_running": False,
        "running": is_running(),
    }


def is_running() -> bool:
    return esphome_runtime.is_running()


async def startup() -> None:
    await esphome_runtime.startup()


async def shutdown() -> None:
    await esphome_runtime.shutdown()


def _runtime_panel_token(panel: Any = "") -> str:
    token = esphome_runtime.lower(panel)
    return token if token in {"satellites", "firmware", "platform", "stats"} else ""


def get_runtime_payload(
    *,
    redis_client: Any = None,
    core_key: str = "esphome",
    core_tab: Optional[Dict[str, Any]] = None,
    panel: str = "",
) -> Dict[str, Any]:
    panel_token = _runtime_panel_token(panel)
    include_satellites = panel_token in {"", "satellites"}
    include_firmware = panel_token in {"", "firmware"}
    include_stats = panel_token in {"", "stats"}
    status = esphome_runtime.status()
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    voice_metrics = (
        status.get("voice_metrics")
        if isinstance(status.get("voice_metrics"), dict)
        else esphome_runtime.voice_metrics_snapshot()
    )
    satellites = esphome_runtime.load_satellite_registry()

    connected = len([row for row in clients.values() if isinstance(row, dict) and bool(row.get("connected"))])
    selected = len(status.get("targets") or {})
    discovered = len([row for row in satellites if esphome_runtime.lower(row.get("source")).startswith("mdns")])

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
        f"STT: {esphome_runtime.text(stt.get('backend'))}->{effective_stt_backend} • "
        f"TTS: {esphome_runtime.text(tts.get('backend'))}->{effective_tts_backend} • "
        f"EOU: {esphome_runtime.text(eou.get('mode'))}/{esphome_runtime.text(eou.get('backend'))}"
    )
    payload = {
        "summary": summary,
        "panel": panel_token or "all",
        "header_stats": [
            {"label": "Connected", "value": connected},
            {"label": "Known Satellites", "value": len(satellites)},
            {"label": "mDNS Discovered", "value": discovered},
            {"label": "STT Backend", "value": effective_stt_backend},
            {"label": "TTS Backend", "value": effective_tts_backend},
        ],
        "items": [],
        "empty_message": "No satellites discovered yet. Use Refresh or add one manually.",
        "ui": {
            "kind": "esphome_native",
            "title": "Tater Voice",
        },
    }
    if include_satellites:
        item_forms = [esphome_settings.settings_item_form()]
        item_forms.extend(esphome_settings.satellite_item_forms(status))
        payload["ui"]["add_form"] = {
            "action": "voice_satellite_add_manual",
            "submit_label": "Add Satellite",
            "fields": [
                {
                    "key": "host",
                    "label": "Host / IP",
                    "type": "text",
                    "placeholder": "10.4.20.19",
                    "description": "Hostname or IP of an ESPHome voice satellite.",
                },
                {
                    "key": "name",
                    "label": "Name",
                    "type": "text",
                    "placeholder": "Kitchen Satellite",
                },
                {
                    "key": "area_name",
                    "label": "Room / Area",
                    "type": "text",
                    "placeholder": "Kitchen",
                    "description": "Optional default room context for voice turns from this satellite.",
                },
            ],
        }
        payload["ui"]["item_forms"] = item_forms

    if include_firmware:
        payload["firmware"] = esphome_firmware.firmware_panel_payload(status)

    if include_stats:
        voice_rows, voices_meta = esphome_runtime.load_wyoming_tts_voice_catalog()
        piper_rows, piper_meta = esphome_runtime.load_piper_tts_model_catalog()
        tts_catalog_count = len(piper_rows) if effective_tts_backend == "piper" else len(voice_rows)
        tts_catalog_updated = piper_meta.get("updated_ts") if effective_tts_backend == "piper" else voices_meta.get("updated_ts")
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
                "title": "Discovery & Devices",
                "metrics": [
                    {"label": "Selected", "value": selected},
                    {"label": "Connected", "value": connected},
                    {"label": "Known Satellites", "value": len(satellites)},
                    {"label": "mDNS Discovered", "value": discovered},
                    {"label": "Discovery Runs", "value": int(discovery_stats.get("runs") or 0)},
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


def handle_runtime_action(*, action: str, payload: Dict[str, Any], redis_client: Any = None, core_key: str = "esphome") -> Dict[str, Any]:
    action_name = esphome_runtime.lower(action)
    body = payload if isinstance(payload, dict) else {}

    firmware_result = esphome_firmware.handle_runtime_action(action_name, body)
    if isinstance(firmware_result, dict):
        return firmware_result

    if action_name == "voice_settings_save":
        values = esphome_runtime.payload_values(body)
        result = esphome_settings.save_settings_values(values)
        with contextlib.suppress(Exception):
            esphome_runtime.reconcile_once(force=True, timeout=45.0)
        updated = int(result.get("updated_count") or 0)
        message = f"Saved {updated} setting(s)." if updated > 0 else "No settings changed."
        return {"ok": True, "action": action_name, "message": message, **result, "status": esphome_runtime.status()}

    if action_name == "voice_satellite_add_manual":
        values = esphome_runtime.payload_values(body)
        host = esphome_runtime.lower(values.get("host") or body.get("host"))
        if not host:
            raise ValueError("host is required")
        selector = f"host:{host}"
        name = esphome_runtime.text(values.get("name")) or host
        area_name = esphome_runtime.text(values.get("area_name"))
        esphome_runtime.upsert_satellite(
            {
                "selector": selector,
                "host": host,
                "name": name,
                "source": "manual",
                "metadata": {
                    "esphome_selected": True,
                    "area_name": area_name,
                },
            }
        )
        with contextlib.suppress(Exception):
            esphome_runtime.reconcile_once(force=True, timeout=45.0)
        return {"ok": True, "action": action_name, "selector": selector, "message": f"Added satellite {name}.", "status": esphome_runtime.status()}

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

    if action_name == "voice_discover":
        rows = esphome_runtime.discover_once()
        for row in rows or []:
            esphome_runtime.upsert_satellite(row)
        return {"ok": True, "action": action_name, "count": len(rows or []), "message": f"Discovery completed: {len(rows or [])} satellite(s) found.", "status": esphome_runtime.status()}

    if action_name == "voice_reconcile":
        status = esphome_runtime.reconcile_once(force=True, timeout=45.0)
        return {"ok": True, "action": action_name, "status": status, "message": "Reconcile completed."}

    if action_name == "voice_refresh":
        rows = esphome_runtime.discover_once()
        for row in rows or []:
            esphome_runtime.upsert_satellite(row)
        status = esphome_runtime.reconcile_once(force=True, timeout=45.0)
        return {
            "ok": True,
            "action": action_name,
            "count": len(rows or []),
            "status": status,
            "message": f"Refresh completed: discovered {len(rows or [])} satellite(s) and reconciled selected devices.",
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
        result = esphome_runtime.logs_start(selector, timeout=20.0)
        result["action"] = action_name
        return result

    if action_name == "voice_logs_poll":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        after_seq = esphome_runtime.as_int(body.get("after_seq"), 0, minimum=0)
        result = esphome_runtime.logs_poll(selector, after_seq=after_seq, timeout=20.0)
        result["action"] = action_name
        return result

    if action_name == "voice_logs_stop":
        selector = esphome_runtime.payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        force = esphome_runtime.as_bool(body.get("force"), False)
        result = esphome_runtime.logs_stop(selector, force=force, timeout=20.0)
        result["action"] = action_name
        return result

    raise ValueError(f"Unknown action: {action_name}")


def include_routes(app: Any) -> None:
    esphome_runtime.include_routes(app)


def raise_unavailable_settings_error() -> None:
    raise HTTPException(status_code=500, detail="Built-in ESPHome services are unavailable.")
