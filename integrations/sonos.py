from __future__ import annotations

import contextlib
import json
import socket
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse
from xml.sax.saxutils import escape as xml_escape

import requests

from helpers import redis_client

SONOS_SETTINGS_KEY = "sonos_settings"
SONOS_TARGET_PREFIX = "sonos:"
SONOS_DEFAULT_ENABLED = True
SONOS_DEFAULT_DISCOVERY_TIMEOUT_SECONDS = 2
SONOS_DISCOVERY_CACHE_KEY = "tater:sonos:speakers:registry:v1"
SONOS_DISCOVERY_CACHE_TTL_SECONDS = 60.0
SONOS_AVTRANSPORT_SERVICE = "urn:schemas-upnp-org:service:AVTransport:1"
SONOS_DEFAULT_PLAY_TIMEOUT_SECONDS = 30.0

INTEGRATION = {
    "id": "sonos",
    "name": "Sonos",
    "description": "Sonos speaker discovery and direct playback targets for announcements.",
    "badge": "SON",
    "order": 50,
    "fields": [
        {
            "key": "sonos_enabled",
            "label": "Enable Sonos announcements",
            "type": "checkbox",
            "default": SONOS_DEFAULT_ENABLED,
        },
        {
            "key": "sonos_discovery_timeout_seconds",
            "label": "Discovery Timeout Seconds",
            "type": "number",
            "default": SONOS_DEFAULT_DISCOVERY_TIMEOUT_SECONDS,
            "min": 1,
            "max": 10,
        },
        {
            "key": "sonos_speaker_hosts",
            "label": "Manual Speaker Hosts",
            "type": "textarea",
            "default": "",
            "rows": 3,
            "placeholder": "192.168.1.42\nsonos-living-room.local",
            "description": "Optional. One host, IP, or URL per line for speakers SSDP does not discover.",
            "full_width": True,
        },
    ],
    "actions": [
        {
            "id": "discover",
            "label": "Discover Speakers",
            "status": "Runs Sonos discovery using SSDP plus manual speaker hosts.",
        },
    ],
}


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "ignore").strip()
    return str(value or "").strip()


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(_text(value)))
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def read_sonos_settings(client: Any = None) -> Dict[str, Any]:
    store = client or redis_client
    try:
        raw = store.hgetall(SONOS_SETTINGS_KEY) or {}
    except Exception:
        raw = {}
    timeout = _bounded_int(
        raw.get("SONOS_DISCOVERY_TIMEOUT_SECONDS"),
        default=SONOS_DEFAULT_DISCOVERY_TIMEOUT_SECONDS,
        minimum=1,
        maximum=10,
    )
    return {
        "SONOS_ENABLED": _as_bool(raw.get("SONOS_ENABLED"), SONOS_DEFAULT_ENABLED),
        "SONOS_DISCOVERY_TIMEOUT_SECONDS": str(timeout),
        "SONOS_SPEAKER_HOSTS": _text(raw.get("SONOS_SPEAKER_HOSTS")),
    }


def save_sonos_settings(
    *,
    enabled: Any = None,
    discovery_timeout_seconds: Any = None,
    speaker_hosts: Any = None,
    client: Any = None,
) -> Dict[str, Any]:
    store = client or redis_client
    current = read_sonos_settings(store)
    next_settings = {
        "SONOS_ENABLED": "true"
        if _as_bool(current.get("SONOS_ENABLED") if enabled is None else enabled, SONOS_DEFAULT_ENABLED)
        else "false",
        "SONOS_DISCOVERY_TIMEOUT_SECONDS": str(
            _bounded_int(
                current.get("SONOS_DISCOVERY_TIMEOUT_SECONDS")
                if discovery_timeout_seconds is None
                else discovery_timeout_seconds,
                default=SONOS_DEFAULT_DISCOVERY_TIMEOUT_SECONDS,
                minimum=1,
                maximum=10,
            )
        ),
        "SONOS_SPEAKER_HOSTS": _text(current.get("SONOS_SPEAKER_HOSTS") if speaker_hosts is None else speaker_hosts),
    }
    store.hset(SONOS_SETTINGS_KEY, mapping=next_settings)
    with contextlib.suppress(Exception):
        store.delete(SONOS_DISCOVERY_CACHE_KEY)
    return read_sonos_settings(store)


def _split_sonos_manual_hosts(value: Any) -> List[str]:
    items: List[str] = []
    seen: set[str] = set()
    for raw in _text(value).replace(",", "\n").splitlines():
        token = _text(raw)
        if not token or token.startswith("#"):
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(token)
    return items


def normalize_sonos_root(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    if "://" not in text:
        text = f"http://{text}"
    parsed = urlparse(text)
    netloc = parsed.netloc or parsed.path
    if not netloc:
        return ""
    if ":" not in netloc.rsplit("@", 1)[-1]:
        netloc = f"{netloc}:1400"
    scheme = parsed.scheme or "http"
    return urlunparse((scheme, netloc, "", "", "", "")).rstrip("/")


def _root_from_url(raw_url: Any) -> str:
    parsed = urlparse(_text(raw_url))
    if not parsed.scheme or not parsed.netloc:
        return ""
    return urlunparse((parsed.scheme, parsed.netloc, "", "", "", "")).rstrip("/")


def _description_url_for_root(root_url: Any) -> str:
    root = normalize_sonos_root(root_url)
    return f"{root}/xml/device_description.xml" if root else ""


def _xml_find_text(root: ET.Element, tag: str) -> str:
    node = root.find(f".//{{*}}{tag}")
    return _text(node.text if node is not None else "")


def _parse_sonos_description(location_url: str, *, timeout_s: float) -> Dict[str, str]:
    location = _text(location_url)
    if not location:
        return {}
    try:
        response = requests.get(location, timeout=max(1.0, float(timeout_s or 2.0)))
        response.raise_for_status()
    except Exception:
        return {}

    body = _text(response.text)
    if "sonos" not in body.lower() and "zoneplayer" not in body.lower():
        return {}

    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return {}

    udn = _text(_xml_find_text(root, "UDN"))
    speaker_id = sonos_target_id(udn) or sonos_target_id(_root_from_url(response.url) or location)
    root_url = _root_from_url(response.url) or _root_from_url(location)
    if not root_url:
        root_url = normalize_sonos_root(location)
    parsed = urlparse(root_url)
    name = (
        _xml_find_text(root, "roomName")
        or _xml_find_text(root, "friendlyName")
        or _xml_find_text(root, "displayName")
        or _text(parsed.hostname)
        or speaker_id
    )
    model = _xml_find_text(root, "modelName") or _xml_find_text(root, "modelNumber")
    if not speaker_id or not root_url:
        return {}
    return {
        "id": speaker_id,
        "udn": udn,
        "name": name,
        "model": model,
        "root_url": root_url,
        "location": response.url or location,
        "host": _text(parsed.hostname),
    }


def _fetch_sonos_speaker(root_or_location: Any, *, timeout_s: float = 2.0) -> Dict[str, str]:
    token = _text(root_or_location)
    if not token:
        return {}
    if token.lower().endswith(".xml"):
        location = token
    else:
        location = _description_url_for_root(token)
    return _parse_sonos_description(location, timeout_s=timeout_s)


def _parse_ssdp_headers(payload: bytes) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    raw = payload.decode("utf-8", "ignore")
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[_text(key).lower()] = _text(value)
    return headers


def _discover_sonos_locations_ssdp(timeout_s: float) -> List[str]:
    locations: List[str] = []
    seen: set[str] = set()
    deadline = time.time() + max(1.0, min(10.0, float(timeout_s or SONOS_DEFAULT_DISCOVERY_TIMEOUT_SECONDS)))
    message = (
        "M-SEARCH * HTTP/1.1\r\n"
        "HOST: 239.255.255.250:1900\r\n"
        'MAN: "ssdp:discover"\r\n'
        "MX: 2\r\n"
        "ST: urn:schemas-upnp-org:device:ZonePlayer:1\r\n"
        "\r\n"
    ).encode("ascii")

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.settimeout(0.5)
        sock.sendto(message, ("239.255.255.250", 1900))
    except OSError:
        with contextlib.suppress(Exception):
            sock.close()
        return []

    try:
        while time.time() < deadline:
            try:
                data, _addr = sock.recvfrom(8192)
            except socket.timeout:
                continue
            except OSError:
                break
            headers = _parse_ssdp_headers(data)
            raw_location = _text(headers.get("location"))
            if not raw_location:
                continue
            raw_payload = data.decode("utf-8", "ignore").lower()
            if "sonos" not in raw_payload and "zoneplayer" not in raw_payload:
                continue
            key = raw_location.lower()
            if key in seen:
                continue
            seen.add(key)
            locations.append(raw_location)
    finally:
        with contextlib.suppress(Exception):
            sock.close()

    return locations


def _cache_rows(rows: List[Dict[str, str]]) -> None:
    payload = {"ts": time.time(), "rows": rows}
    with contextlib.suppress(Exception):
        redis_client.set(SONOS_DISCOVERY_CACHE_KEY, json.dumps(payload, ensure_ascii=False))


def _cached_rows() -> Optional[List[Dict[str, str]]]:
    try:
        raw = redis_client.get(SONOS_DISCOVERY_CACHE_KEY)
        payload = json.loads(raw) if raw else {}
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        ts = float(payload.get("ts") or 0.0)
    except Exception:
        ts = 0.0
    if ts <= 0.0 or (time.time() - ts) > SONOS_DISCOVERY_CACHE_TTL_SECONDS:
        return None
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return None
    return [dict(item) for item in rows if isinstance(item, dict)]


def _dedupe_sonos_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        speaker_id = sonos_target_id(row.get("id") or row.get("udn") or row.get("root_url"))
        root_url = normalize_sonos_root(row.get("root_url"))
        key = (speaker_id or root_url).lower()
        if not key or key in seen:
            continue
        next_row = dict(row)
        next_row["id"] = speaker_id
        next_row["root_url"] = root_url
        seen.add(key)
        out.append(next_row)
    out.sort(key=lambda item: (_text(item.get("name")).lower(), _text(item.get("host")).lower()))
    return out


def discover_sonos_speakers(*, force: bool = False, timeout_s: Any = None) -> List[Dict[str, str]]:
    settings = read_sonos_settings()
    if not _as_bool(settings.get("SONOS_ENABLED"), SONOS_DEFAULT_ENABLED):
        return []
    if not force:
        cached = _cached_rows()
        if cached is not None:
            return _dedupe_sonos_rows(cached)

    timeout = _bounded_int(
        timeout_s if timeout_s is not None else settings.get("SONOS_DISCOVERY_TIMEOUT_SECONDS"),
        default=SONOS_DEFAULT_DISCOVERY_TIMEOUT_SECONDS,
        minimum=1,
        maximum=10,
    )
    candidates = _discover_sonos_locations_ssdp(float(timeout))
    candidates.extend(_split_sonos_manual_hosts(settings.get("SONOS_SPEAKER_HOSTS")))

    rows: List[Dict[str, str]] = []
    seen_candidates: set[str] = set()
    unique_candidates: List[str] = []
    for item in candidates:
        token = _text(item)
        key = token.lower()
        if not token or key in seen_candidates:
            continue
        seen_candidates.add(key)
        unique_candidates.append(token)

    if unique_candidates:
        with ThreadPoolExecutor(max_workers=min(12, max(1, len(unique_candidates)))) as executor:
            future_map = {
                executor.submit(_fetch_sonos_speaker, candidate, timeout_s=max(1.0, float(timeout))): candidate
                for candidate in unique_candidates
            }
            for future in as_completed(future_map):
                try:
                    row = future.result()
                except Exception:
                    row = {}
                if row:
                    rows.append(row)

    rows = _dedupe_sonos_rows(rows)
    _cache_rows(rows)
    return rows


def sonos_target_id(value: Any) -> str:
    token = _text(value)
    if not token:
        return ""
    lower = token.lower()
    if lower.startswith(SONOS_TARGET_PREFIX):
        token = _text(token[len(SONOS_TARGET_PREFIX) :])
    if token.lower().startswith("uuid:"):
        token = _text(token[5:])
    return token


def resolve_sonos_target(target: Any) -> Dict[str, str]:
    token = sonos_target_id(target)
    if not token:
        return {}
    token_l = token.lower()
    for row in discover_sonos_speakers():
        aliases = {
            _text(row.get("id")).lower(),
            sonos_target_id(row.get("udn")).lower(),
            _text(row.get("root_url")).lower(),
            _text(row.get("host")).lower(),
            _text(row.get("name")).lower(),
        }
        if token_l in aliases:
            return row
    if token_l.startswith(("http://", "https://")) or "." in token_l or ":" in token_l:
        return _fetch_sonos_speaker(token, timeout_s=2.0)
    return {}


def _sonos_soap_action(
    root_url: str,
    *,
    action: str,
    inner_xml: str,
    timeout_s: float,
) -> None:
    root = normalize_sonos_root(root_url)
    if not root:
        raise RuntimeError("Sonos speaker root URL is missing.")
    body = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
        's:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">'
        "<s:Body>"
        f'<u:{action} xmlns:u="{SONOS_AVTRANSPORT_SERVICE}">'
        f"{inner_xml}"
        f"</u:{action}>"
        "</s:Body>"
        "</s:Envelope>"
    )
    response = requests.post(
        f"{root}/MediaRenderer/AVTransport/Control",
        data=body.encode("utf-8"),
        headers={
            "Content-Type": 'text/xml; charset="utf-8"',
            "SOAPACTION": f'"{SONOS_AVTRANSPORT_SERVICE}#{action}"',
        },
        timeout=max(2.0, float(timeout_s or SONOS_DEFAULT_PLAY_TIMEOUT_SECONDS)),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Sonos {action} HTTP {response.status_code}: {_text(response.text)[:200]}")


def sonos_play_url_sync(
    *,
    speaker: Dict[str, Any],
    source_url: Any,
    timeout_s: float = SONOS_DEFAULT_PLAY_TIMEOUT_SECONDS,
) -> None:
    root_url = _text(speaker.get("root_url")) if isinstance(speaker, dict) else ""
    url = _text(source_url)
    if not root_url:
        raise RuntimeError("Sonos speaker root URL is missing.")
    if not url:
        raise RuntimeError("Sonos source URL is missing.")
    escaped_url = xml_escape(url, {'"': "&quot;"})
    _sonos_soap_action(
        root_url,
        action="SetAVTransportURI",
        inner_xml=(
            "<InstanceID>0</InstanceID>"
            f"<CurrentURI>{escaped_url}</CurrentURI>"
            "<CurrentURIMetaData></CurrentURIMetaData>"
        ),
        timeout_s=timeout_s,
    )
    _sonos_soap_action(
        root_url,
        action="Play",
        inner_xml="<InstanceID>0</InstanceID><Speed>1</Speed>",
        timeout_s=timeout_s,
    )


def sonos_play_media_sync(
    *,
    speakers: List[str],
    source_url: Any,
    timeout_s: float = SONOS_DEFAULT_PLAY_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    clean_speakers = [sonos_target_id(item) for item in list(speakers or []) if sonos_target_id(item)]
    if not clean_speakers:
        return {"ok": False, "sent_count": 0, "error": "No Sonos speakers selected."}
    url = _text(source_url)
    if not url:
        return {"ok": False, "sent_count": 0, "error": "Sonos announcement URL is missing."}

    sent_count = 0
    failures: List[str] = []
    for target in clean_speakers:
        try:
            speaker = resolve_sonos_target(target)
            if not speaker:
                raise RuntimeError("speaker was not found")
            sonos_play_url_sync(speaker=speaker, source_url=url, timeout_s=timeout_s)
            sent_count += 1
        except Exception as exc:
            failures.append(f"{target} ({exc})")

    if sent_count:
        result: Dict[str, Any] = {"ok": True, "sent_count": sent_count}
        if failures:
            result["warnings"] = failures
        return result
    return {"ok": False, "sent_count": 0, "error": "; ".join(failures) or "Sonos playback failed."}


def read_integration_settings() -> Dict[str, Any]:
    settings = read_sonos_settings()
    return {
        "sonos_enabled": _as_bool(settings.get("SONOS_ENABLED"), SONOS_DEFAULT_ENABLED),
        "sonos_discovery_timeout_seconds": int(
            settings.get("SONOS_DISCOVERY_TIMEOUT_SECONDS") or SONOS_DEFAULT_DISCOVERY_TIMEOUT_SECONDS
        ),
        "sonos_speaker_hosts": settings.get("SONOS_SPEAKER_HOSTS", ""),
    }


def integration_status() -> Dict[str, Any]:
    settings = read_sonos_settings()
    enabled = _as_bool(settings.get("SONOS_ENABLED"), SONOS_DEFAULT_ENABLED)
    return {
        "enabled": enabled,
        "message": "Sonos announcements are enabled." if enabled else "Sonos announcements are disabled.",
    }


def integration_devices() -> Dict[str, Any]:
    settings = read_sonos_settings()
    if not _as_bool(settings.get("SONOS_ENABLED"), SONOS_DEFAULT_ENABLED):
        return {"devices": [], "message": "Sonos announcements are disabled."}
    speakers = discover_sonos_speakers(force=False)
    devices: List[Dict[str, Any]] = []
    for speaker in speakers:
        if not isinstance(speaker, dict):
            continue
        devices.append(
            {
                "id": _text(speaker.get("id")) or _text(speaker.get("udn")),
                "name": _text(speaker.get("name")) or _text(speaker.get("host")) or "Sonos Speaker",
                "type": "speaker",
                "status": "available",
                "state": "available",
                "details": {
                    "model": speaker.get("model"),
                    "host": speaker.get("host"),
                    "root_url": speaker.get("root_url"),
                    "udn": speaker.get("udn"),
                },
            }
        )
    return {"devices": devices, "message": f"Sonos returned {len(devices)} speakers."}


def save_integration_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    saved = save_sonos_settings(
        enabled=(payload or {}).get("sonos_enabled"),
        discovery_timeout_seconds=(payload or {}).get("sonos_discovery_timeout_seconds"),
        speaker_hosts=(payload or {}).get("sonos_speaker_hosts"),
    )
    return {
        "sonos_enabled": _as_bool(saved.get("SONOS_ENABLED"), SONOS_DEFAULT_ENABLED),
        "sonos_discovery_timeout_seconds": int(
            saved.get("SONOS_DISCOVERY_TIMEOUT_SECONDS") or SONOS_DEFAULT_DISCOVERY_TIMEOUT_SECONDS
        ),
        "sonos_speaker_hosts": saved.get("SONOS_SPEAKER_HOSTS", ""),
    }


def run_integration_action(action_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if _text(action_id) != "discover":
        raise KeyError(f"Unsupported Sonos action: {action_id}")
    if payload:
        save_integration_settings(payload)
    rows = discover_sonos_speakers(force=True)
    count = len(rows)
    return {
        "ok": True,
        "speaker_count": count,
        "speakers": rows,
        "message": f"Sonos discovery found {count} speaker{'s' if count != 1 else ''}.",
    }
