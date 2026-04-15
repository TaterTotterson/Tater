from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from .voice_pipeline import (
    VOICE_CORE_SETTINGS_HASH_KEY,
    _discovery_stats,
    _load_piper_tts_model_catalog,
    _load_satellite_registry,
    _load_wyoming_tts_voice_catalog,
    _resolve_stt_backend,
    _resolve_tts_backend,
    _run_async_blocking,
    _save_voice_ui_settings,
    _selector_runtime,
    _set_satellite_selected,
    _sync_manual_targets,
    _text,
    _voice_config_snapshot,
    _voice_ui_satellite_item_forms,
    _voice_ui_setting_fields,
    _voice_ui_settings_item_form,
    _voice_metrics_snapshot,
    _esphome_disconnect_selector,
    _esphome_reconcile_once,
    _esphome_status,
    _lower,
    router,
    shutdown as _voice_shutdown,
    startup as _voice_startup,
)


def settings_hash_key() -> str:
    return str(VOICE_CORE_SETTINGS_HASH_KEY or "voice_core_settings")


def settings_fields() -> List[Dict[str, Any]]:
    rows = _voice_ui_setting_fields()
    return rows if isinstance(rows, list) else []


def save_settings_values(values: Dict[str, Any]) -> Dict[str, Any]:
    result = _save_voice_ui_settings(values or {})
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
    runtime = _selector_runtime("__esphome_home__")
    return bool(runtime.get("service_running"))


async def startup() -> None:
    if is_running():
        return
    await _voice_startup()
    runtime = _selector_runtime("__esphome_home__")
    runtime["service_running"] = True


async def shutdown() -> None:
    if not is_running():
        return
    await _voice_shutdown()
    runtime = _selector_runtime("__esphome_home__")
    runtime["service_running"] = False


def get_runtime_payload(*, redis_client: Any = None, core_key: str = "esphome", core_tab: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    voice_rows, voices_meta = _load_wyoming_tts_voice_catalog()
    piper_rows, piper_meta = _load_piper_tts_model_catalog()
    status = _esphome_status()
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    voice_metrics = status.get("voice_metrics") if isinstance(status.get("voice_metrics"), dict) else _voice_metrics_snapshot()
    satellites = _load_satellite_registry()

    connected = len([row for row in clients.values() if isinstance(row, dict) and bool(row.get("connected"))])
    selected = len(status.get("targets") or {})
    discovered = len([row for row in satellites if _lower(row.get("source")).startswith("mdns")])

    cfg = _voice_config_snapshot()
    eou = cfg.get("eou") if isinstance(cfg.get("eou"), dict) else {}
    stt = cfg.get("stt") if isinstance(cfg.get("stt"), dict) else {}
    tts = cfg.get("tts") if isinstance(cfg.get("tts"), dict) else {}
    effective_stt_backend, _stt_note = _resolve_stt_backend()
    effective_tts_backend, _tts_note = _resolve_tts_backend()
    tts_catalog_count = len(piper_rows) if effective_tts_backend == "piper" else len(voice_rows)
    tts_catalog_updated = piper_meta.get("updated_ts") if effective_tts_backend == "piper" else voices_meta.get("updated_ts")
    stt_backend_rows = [
        {"backend": _text(name) or "unknown", "avg_ms": f"{float(value or 0.0):.1f}"}
        for name, value in sorted(
            ((voice_metrics.get("avg_stt_latency_by_backend_ms") or {}) if isinstance(voice_metrics.get("avg_stt_latency_by_backend_ms"), dict) else {}).items(),
            key=lambda item: str(item[0]),
        )
    ]
    tts_backend_rows = [
        {"backend": _text(name) or "unknown", "avg_ms": f"{float(value or 0.0):.1f}"}
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
            _text(device_info.get("friendly_name"))
            or _text(device_info.get("name"))
            or _text(row.get("selector"))
            or _text(selector)
        )
        device_rows.append(
            {
                "satellite": title,
                "host": _text(row.get("host")) or "-",
                "sessions": str(int(metrics_row.get("sessions_started") or 0)),
                "valid": str(int(metrics_row.get("valid_turns") or 0)),
                "no_ops": str(int(metrics_row.get("no_op_turns") or 0)),
                "false_wakes": str(int(metrics_row.get("false_wake_count") or 0)),
                "errors": str(int(metrics_row.get("error_count") or 0)),
                "reconnects": str(int(metrics_row.get("reconnect_count") or 0)),
                "avg_turn_ms": f"{float(metrics_row.get('avg_turn_latency_ms') or 0.0):.1f}",
            }
        )

    summary = (
        f"Selected satellites: {selected} • Connected: {connected} • "
        f"Valid turns: {int(voice_metrics.get('valid_turns') or 0)}/{int(voice_metrics.get('sessions_started') or 0)} • "
        f"False wakes: {int(voice_metrics.get('false_wake_count') or 0)} • "
        f"Low signal: {int(voice_metrics.get('low_signal_count') or 0)} • "
        f"Avg turn: {float(voice_metrics.get('avg_turn_latency_ms') or 0.0):.0f} ms • "
        f"STT: {_text(stt.get('backend'))}->{effective_stt_backend} • "
        f"TTS: {_text(tts.get('backend'))}->{effective_tts_backend} • "
        f"EOU: {_text(eou.get('mode'))}/{_text(eou.get('backend'))}"
    )
    item_forms = [_voice_ui_settings_item_form()]
    item_forms.extend(_voice_ui_satellite_item_forms(status))
    return {
        "summary": summary,
        "header_stats": [
            {"label": "Connected", "value": connected},
            {"label": "Known Satellites", "value": len(satellites)},
            {"label": "mDNS Discovered", "value": discovered},
            {"label": "STT Backend", "value": effective_stt_backend},
            {"label": "TTS Backend", "value": effective_tts_backend},
        ],
        "stats": [
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
            {"label": "Last TTS Refresh", "value": _text(tts_catalog_updated) or "-"},
        ],
        "stats_sections": [
            {
                "title": "Discovery & Devices",
                "metrics": [
                    {"label": "Selected", "value": selected},
                    {"label": "Connected", "value": connected},
                    {"label": "Known Satellites", "value": len(satellites)},
                    {"label": "mDNS Discovered", "value": discovered},
                    {"label": "Discovery Runs", "value": int(_discovery_stats.get("runs") or 0)},
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
                    {"label": "Last TTS Refresh", "value": _text(tts_catalog_updated) or "-"},
                ],
            },
        ],
        "stats_tables": [
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
        ],
        "items": [],
        "empty_message": "No satellites discovered yet. Use Refresh or add one manually.",
        "ui": {
            "kind": "esphome_native",
            "title": "Tater Voice",
            "add_form": {
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
            },
            "item_forms": item_forms,
        },
    }


def handle_runtime_action(*, action: str, payload: Dict[str, Any], redis_client: Any = None, core_key: str = "esphome") -> Dict[str, Any]:
    from . import voice_pipeline as vp

    action_name = _lower(action)
    body = payload if isinstance(payload, dict) else {}

    if action_name == "voice_settings_save":
        values = vp._payload_values(body)
        result = _save_voice_ui_settings(values)
        _sync_manual_targets()
        with vp.contextlib.suppress(Exception):
            _run_async_blocking(vp._esphome_reconcile_once(force=True), timeout=45.0)
        updated = int(result.get("updated_count") or 0)
        message = f"Saved {updated} setting(s)." if updated > 0 else "No settings changed."
        return {"ok": True, "action": action_name, "message": message, **result, "status": _esphome_status()}

    if action_name == "voice_satellite_add_manual":
        values = vp._payload_values(body)
        host = _lower(values.get("host") or body.get("host"))
        if not host:
            raise ValueError("host is required")
        selector = f"host:{host}"
        name = _text(values.get("name")) or host
        area_name = _text(values.get("area_name"))
        vp._upsert_satellite(
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
        targets = vp._current_manual_targets()
        if host not in targets:
            targets.append(host)
            vp._save_manual_targets(targets)
        with vp.contextlib.suppress(Exception):
            _run_async_blocking(vp._esphome_reconcile_once(force=True), timeout=45.0)
        return {"ok": True, "action": action_name, "selector": selector, "message": f"Added satellite {name}.", "status": _esphome_status()}

    if action_name == "voice_satellite_save":
        selector = vp._payload_selector(body)
        values = vp._payload_values(body)
        existing = vp._satellite_lookup(selector) if selector else {}
        host = _lower(values.get("host")) or _lower(existing.get("host")) or vp._satellite_host_from_selector(selector)
        if not selector and host:
            selector = f"host:{host}"
        if not selector:
            raise ValueError("selector is required")
        metadata = dict(existing.get("metadata") or {})
        if "area_name" in values:
            metadata["area_name"] = _text(values.get("area_name"))
        name = _text(values.get("name")) or _text(existing.get("name")) or host or selector
        source = _text(existing.get("source")) or "manual"
        vp._upsert_satellite({"selector": selector, "host": host, "name": name, "source": source, "metadata": metadata})
        return {"ok": True, "action": action_name, "selector": selector, "message": f"Saved satellite {name}.", "status": _esphome_status()}

    if action_name == "voice_satellite_remove":
        selector = vp._payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        existing = vp._satellite_lookup(selector)
        host = _lower(existing.get("host")) or vp._satellite_host_from_selector(selector)
        removed = vp._remove_satellite(selector)
        if host:
            next_targets = [item for item in vp._current_manual_targets() if _lower(item) != host]
            vp._save_manual_targets(next_targets)
        with vp.contextlib.suppress(Exception):
            _run_async_blocking(vp._esphome_disconnect_selector(selector, reason="manual_remove"), timeout=20.0)
        return {"ok": True, "action": action_name, "selector": selector, "removed": bool(removed), "message": "Satellite removed." if removed else "Satellite was already absent.", "status": _esphome_status()}

    if action_name == "voice_discover":
        rows = _run_async_blocking(vp._discover_mdns_once(), timeout=30.0)
        for row in rows or []:
            vp._upsert_satellite(row)
        _sync_manual_targets()
        return {"ok": True, "action": action_name, "count": len(rows or []), "message": f"Discovery completed: {len(rows or [])} satellite(s) found.", "status": _esphome_status()}

    if action_name == "voice_reconcile":
        status = _run_async_blocking(vp._esphome_reconcile_once(force=True), timeout=45.0)
        return {"ok": True, "action": action_name, "status": status, "message": "Reconcile completed."}

    if action_name == "voice_refresh":
        rows = _run_async_blocking(vp._discover_mdns_once(), timeout=30.0)
        for row in rows or []:
            vp._upsert_satellite(row)
        _sync_manual_targets()
        status = _run_async_blocking(vp._esphome_reconcile_once(force=True), timeout=45.0)
        return {
            "ok": True,
            "action": action_name,
            "count": len(rows or []),
            "status": status,
            "message": f"Refresh completed: discovered {len(rows or [])} satellite(s) and reconciled selected devices.",
        }

    if action_name == "voice_connect":
        selector = vp._payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        _set_satellite_selected(selector, True)
        row = vp._satellite_lookup(selector)
        host = _lower(row.get("host")) or vp._satellite_host_from_selector(selector)
        if host:
            targets = vp._current_manual_targets()
            if host not in targets:
                targets.append(host)
                vp._save_manual_targets(targets)
        status = _run_async_blocking(vp._esphome_reconcile_once(force=True), timeout=45.0)
        return {"ok": True, "action": action_name, "selector": selector, "status": status, "message": "Satellite selected and connect requested."}

    if action_name == "voice_disconnect":
        selector = vp._payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        _set_satellite_selected(selector, False)
        row = vp._satellite_lookup(selector)
        host = _lower(row.get("host")) or vp._satellite_host_from_selector(selector)
        if host:
            next_targets = [item for item in vp._current_manual_targets() if _lower(item) != host]
            vp._save_manual_targets(next_targets)
        _run_async_blocking(_esphome_disconnect_selector(selector, reason="manual_disconnect"), timeout=20.0)
        return {"ok": True, "action": action_name, "selector": selector, "status": _esphome_status(), "message": "Satellite disconnected and deselected."}

    if action_name == "voice_entity_command":
        selector = vp._payload_selector(body)
        entity_key = _text(body.get("entity_key") or body.get("key"))
        command = _text(body.get("command"))
        if not selector:
            raise ValueError("selector is required")
        if not entity_key:
            raise ValueError("entity_key is required")
        if not command:
            raise ValueError("command is required")
        result = _run_async_blocking(
            vp._esphome_command_entity(
                selector,
                entity_key=entity_key,
                command=command,
                value=body.get("value"),
                options=body,
            ),
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
            "message": f"Updated {_text(body.get('entity_label')) or 'entity'}.",
        }

    if action_name == "voice_logs_start":
        selector = vp._payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        result = _run_async_blocking(vp._esphome_logs_start(selector), timeout=20.0)
        result["action"] = action_name
        return result

    if action_name == "voice_logs_poll":
        selector = vp._payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        after_seq = vp._as_int(body.get("after_seq"), 0, minimum=0)
        result = _run_async_blocking(vp._esphome_logs_poll(selector, after_seq=after_seq), timeout=20.0)
        result["action"] = action_name
        return result

    if action_name == "voice_logs_stop":
        selector = vp._payload_selector(body)
        if not selector:
            raise ValueError("selector is required")
        force = vp._as_bool(body.get("force"), False)
        result = _run_async_blocking(vp._esphome_logs_stop(selector, force=force), timeout=20.0)
        result["action"] = action_name
        return result

    raise ValueError(f"Unknown action: {action_name}")


def include_routes(app: Any) -> None:
    app.include_router(router)


def raise_unavailable_settings_error() -> None:
    raise HTTPException(status_code=500, detail="Built-in ESPHome services are unavailable.")
