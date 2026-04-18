from __future__ import annotations

import contextlib
import copy
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import parse as urllib_parse, request as urllib_request

import yaml

from helpers import redis_client

from . import runtime as esphome_runtime

FIRMWARE_PROFILE_HASH_KEY = "tater:esphome:firmware:profiles:v1"
FIRMWARE_AGENT_LABS_ROOT = Path(__file__).resolve().parents[1] / "agent_lab" / "esphome"
FIRMWARE_CONFIG_ROOT = FIRMWARE_AGENT_LABS_ROOT / "firmware_configs"
FIRMWARE_BUILD_ROOT = FIRMWARE_AGENT_LABS_ROOT / "firmware_builds"
FIRMWARE_RUNNER_ROOT = FIRMWARE_AGENT_LABS_ROOT / "runner"
FIRMWARE_PLATFORMIO_ROOT = FIRMWARE_AGENT_LABS_ROOT / "platformio"
FIRMWARE_HOME_ROOT = FIRMWARE_AGENT_LABS_ROOT / "home"
FIRMWARE_CACHE_ROOT = FIRMWARE_AGENT_LABS_ROOT / "cache"
FIRMWARE_BUILD_TIMEOUT_SECONDS = 60 * 60
_CLI_STATUS_CACHE_TTL_SECONDS = 30.0
_CLI_STATUS_CACHE: Dict[str, Any] = {"ts": 0.0, "status": {}}
_CLI_STATUS_LOCK = threading.Lock()
_REMOTE_TEMPLATE_FETCH_TIMEOUT_SECONDS = 3.0
_REMOTE_TEMPLATE_CACHE_TTL_SECONDS = 60.0
_REMOTE_TEMPLATE_CACHE: Dict[str, Dict[str, Any]] = {}
_REMOTE_TEMPLATE_LOCK = threading.Lock()
_REMOTE_JSON_CACHE_TTL_SECONDS = 15 * 60.0
_REMOTE_JSON_CACHE: Dict[str, Dict[str, Any]] = {}
_REMOTE_JSON_LOCK = threading.Lock()
_FIRMWARE_SESSION_MAX_ENTRIES = 4000
_FIRMWARE_SESSION_TTL_SECONDS = 45 * 60.0
_FIRMWARE_DEVICE_LOG_RETRY_SECONDS = 2.5
_FIRMWARE_SESSIONS: Dict[str, Dict[str, Any]] = {}
_FIRMWARE_SESSION_LOCK = threading.Lock()
_ANSI_ESCAPE_RE = re.compile(r"\x1B(?:\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])")
_WAKE_WORD_GITHUB_OWNER = "TaterTotterson"
_WAKE_WORD_GITHUB_REPO = "microWakeWords"
_WAKE_WORD_GITHUB_REF = "main"
_WAKE_WORD_MANIFEST_URLS: tuple[str, ...] = (
    f"https://raw.githubusercontent.com/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/{_WAKE_WORD_GITHUB_REF}/wake_word_manifest.json",
    f"https://raw.githubusercontent.com/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/{_WAKE_WORD_GITHUB_REF}/wake-word-manifest.json",
)
_WAKE_WORD_SOURCE_SPECS: tuple[Dict[str, str], ...] = (
    {"key": "microWakeWords", "label": "microWakeWords"},
    {"key": "microWakeWordsV2", "label": "microWakeWordsV2"},
    {"key": "microWakeWordsV3", "label": "microWakeWordsV3"},
)
_WAKE_WORD_CATALOG_CACHE_TTL_SECONDS = 10 * 60.0
_WAKE_WORD_CATALOG_CACHE: Dict[str, Any] = {"ts": 0.0, "payload": {}}
_WAKE_WORD_CATALOG_LOCK = threading.Lock()

_TEMPLATE_SPECS: tuple[Dict[str, Any], ...] = (
    {
        "key": "voicepe",
        "label": "VoicePE",
        "source_urls": [
            "https://github.com/TaterTotterson/microWakeWords/raw/refs/heads/main/voicePE-TaterTimer.yaml",
        ],
        "candidates": [
            ("microWakeWords", "voicePE-TaterTimer.yaml"),
            ("VoicePE-ESPHome", "voicePE-TaterTimer.yaml"),
        ],
        "fallback_path": "firmware_templates/voicePE-TaterTimer.yaml",
        "fixed_keys": {"device_name"},
        "auto_keys": {"ha_voice_ip"},
        "match_tokens": {
            "voicepe",
            "voice pe",
            "tatervpe",
            "vpe",
        },
    },
    {
        "key": "satellite1",
        "label": "Satellite1",
        "source_urls": [
            "https://github.com/TaterTotterson/microWakeWords/raw/refs/heads/main/satellite1-TaterTimer.yaml",
        ],
        "candidates": [
            ("microWakeWords", "satellite1-TaterTimer.yaml"),
            ("Satellite1-ESPHome", "satellite1-TaterTimer.yaml"),
        ],
        "fallback_path": "firmware_templates/satellite1-TaterTimer.yaml",
        "fixed_keys": {"node_name"},
        "auto_keys": {"ha_voice_ip"},
        "match_tokens": {
            "satellite1",
            "sat 1",
            "sat1",
            "tatersat1",
            "tater_sat1",
            "tater sat1",
            "core board",
        },
    },
)


class _FirmwareYamlLoader(yaml.SafeLoader):
    pass


class _FirmwareYamlDumper(yaml.SafeDumper):
    pass


class _TaggedYamlValue:
    __slots__ = ("tag", "value")

    def __init__(self, tag: str, value: Any) -> None:
        self.tag = _text(tag)
        self.value = value


def _construct_secret(loader: yaml.SafeLoader, node: yaml.Node) -> Dict[str, str]:
    return {"__secret__": loader.construct_scalar(node)}


_FirmwareYamlLoader.add_constructor("!secret", _construct_secret)


def _construct_tagged_yaml(loader: yaml.SafeLoader, tag_suffix: str, node: yaml.Node) -> _TaggedYamlValue:
    tag = f"!{tag_suffix}"
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    elif isinstance(node, yaml.MappingNode):
        value = loader.construct_mapping(node, deep=True)
    else:
        value = loader.construct_object(node, deep=True)
    return _TaggedYamlValue(tag, value)


def _represent_tagged_yaml(dumper: yaml.SafeDumper, value: _TaggedYamlValue) -> yaml.Node:
    payload = value.value
    if isinstance(payload, dict):
        return dumper.represent_mapping(value.tag, payload)
    if isinstance(payload, list):
        return dumper.represent_sequence(value.tag, payload)
    return dumper.represent_scalar(value.tag, "" if payload is None else str(payload))


_FirmwareYamlLoader.add_multi_constructor("!", _construct_tagged_yaml)
_FirmwareYamlDumper.add_representer(_TaggedYamlValue, _represent_tagged_yaml)


def _text(value: Any) -> str:
    return esphome_runtime.text(value)


def _lower(value: Any) -> str:
    return esphome_runtime.lower(value)


def _as_bool(value: Any, default: bool = False) -> bool:
    return esphome_runtime.as_bool(value, default)


def _repo_siblings_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _bundled_template_root() -> Path:
    return Path(__file__).resolve().parent


def _sanitize_token(value: Any) -> str:
    token = _text(value)
    if not token:
        return "device"
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", token)
    return clean.strip("._-") or "device"


def _humanize_key(key: str) -> str:
    token = _text(key)
    if not token:
        return "Value"
    label = token.replace("_", " ").strip()
    special = {
        "ha": "HA",
        "ip": "IP",
        "id": "ID",
        "ssid": "SSID",
        "wifi": "Wi-Fi",
        "xmos": "XMOS",
        "fw": "FW",
    }
    parts = []
    for raw in label.split():
        lower = raw.lower()
        parts.append(special.get(lower, raw.capitalize()))
    return " ".join(parts) or token


def _clean_terminal_text(value: Any) -> str:
    text_value = _text(value)
    if not text_value:
        return ""
    clean = _ANSI_ESCAPE_RE.sub("", text_value).replace("\r", "")
    clean = "".join(ch for ch in clean if ch == "\t" or ord(ch) >= 32)
    return clean.strip()


def _ensure_agent_labs_dirs() -> None:
    for path in (
        FIRMWARE_AGENT_LABS_ROOT,
        FIRMWARE_CONFIG_ROOT,
        FIRMWARE_BUILD_ROOT,
        FIRMWARE_RUNNER_ROOT,
        FIRMWARE_PLATFORMIO_ROOT,
        FIRMWARE_HOME_ROOT,
        FIRMWARE_CACHE_ROOT,
    ):
        path.mkdir(parents=True, exist_ok=True)


def _runner_env_overrides() -> Dict[str, str]:
    _ensure_agent_labs_dirs()
    return {
        "HOME": str(FIRMWARE_HOME_ROOT),
        "XDG_CACHE_HOME": str(FIRMWARE_CACHE_ROOT),
        "PLATFORMIO_CORE_DIR": str(FIRMWARE_PLATFORMIO_ROOT),
        "PLATFORMIO_CACHE_DIR": str(FIRMWARE_PLATFORMIO_ROOT / "cache"),
    }


def _template_default_string(raw_value: Any) -> str:
    if isinstance(raw_value, dict) and raw_value.get("__secret__"):
        return ""
    if isinstance(raw_value, bool):
        return "true" if raw_value else "false"
    if raw_value is None:
        return ""
    return _text(raw_value)


def _secret_name(raw_value: Any) -> str:
    if isinstance(raw_value, dict):
        return _text(raw_value.get("__secret__"))
    return ""


def _remote_json(url: str, *, force_refresh: bool = False) -> Any:
    target = _text(url)
    if not target:
        raise RuntimeError("Remote JSON URL is missing.")

    now = time.time()
    if not force_refresh:
        with _REMOTE_JSON_LOCK:
            cached = _REMOTE_JSON_CACHE.get(target)
            cached_ts = float(cached.get("ts") or 0.0) if isinstance(cached, dict) else 0.0
            if isinstance(cached, dict) and (now - cached_ts) < _REMOTE_JSON_CACHE_TTL_SECONDS:
                if "data" in cached:
                    return copy.deepcopy(cached.get("data"))
                error_value = _text(cached.get("error"))
                if error_value:
                    raise RuntimeError(error_value)

    req = urllib_request.Request(
        target,
        headers={
            "User-Agent": "Tater/1.0",
            "Accept": "application/vnd.github+json, application/json, text/plain, */*",
        },
    )
    try:
        with urllib_request.urlopen(req, timeout=_REMOTE_TEMPLATE_FETCH_TIMEOUT_SECONDS) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            payload = json.loads(response.read().decode(charset, errors="replace"))
    except urllib_request.HTTPError as exc:
        message = f"Failed to fetch remote JSON from {target}: HTTP {int(exc.code or 0)}."
        with _REMOTE_JSON_LOCK:
            _REMOTE_JSON_CACHE[target] = {"ts": now, "error": message}
        raise RuntimeError(message) from exc
    except Exception as exc:
        message = f"Failed to fetch remote JSON from {target}: {_text(exc) or exc.__class__.__name__}."
        with _REMOTE_JSON_LOCK:
            _REMOTE_JSON_CACHE[target] = {"ts": now, "error": message}
        raise RuntimeError(message) from exc

    with _REMOTE_JSON_LOCK:
        _REMOTE_JSON_CACHE[target] = {"ts": now, "data": copy.deepcopy(payload)}
    return payload


def _wake_word_label_from_slug(slug: str) -> str:
    token = _text(slug).strip()
    if not token:
        return "Wake Word"
    parts = [part for part in re.split(r"[_\-\s]+", token) if part]
    if not parts:
        return token
    rendered: List[str] = []
    for part in parts:
        if len(part) <= 3 and part.isascii():
            rendered.append(part.upper())
        else:
            rendered.append(part.capitalize())
    return " ".join(rendered)


def _wake_word_source_version_tag(source_key: Any) -> str:
    token = _lower(source_key)
    if token == "microwakewordsv2":
        return "V2"
    if token == "microwakewordsv3":
        return "V3"
    if token == "microwakewords":
        return "V1"
    return ""


def _wake_word_source_display_label(source_key: Any, source_label: Any = "") -> str:
    tag = _wake_word_source_version_tag(source_key)
    label = _text(source_label).strip() or _text(source_key).strip() or "Wake Words"
    if not tag:
        return label
    return f"{tag} - {label}"


def _wake_word_option_label(label: Any, slug: Any, source_key: Any) -> str:
    base = _text(label).strip() or _text(slug).strip() or "Wake Word"
    tag = _wake_word_source_version_tag(source_key)
    if not tag:
        return base
    return f"{base} [{tag}]"


def _wake_word_slug_from_url(url: str) -> str:
    token = _text(url).strip()
    if not token:
        return ""
    name = Path(token.split("?", 1)[0]).name
    if name.lower().endswith(".json"):
        name = name[:-5]
    return _sanitize_token(name).lower()


def _wake_word_raw_url(path: str) -> str:
    clean = _text(path).strip().lstrip("/")
    if not clean:
        return ""
    quoted = "/".join(urllib_parse.quote(part) for part in clean.split("/") if part)
    return f"https://raw.githubusercontent.com/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/{_WAKE_WORD_GITHUB_REF}/{quoted}"


def _wake_word_contents_api_url(path: str) -> str:
    clean = _text(path).strip().lstrip("/")
    quoted = urllib_parse.quote(clean, safe="/")
    return (
        f"https://api.github.com/repos/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/contents/{quoted}"
        f"?ref={urllib_parse.quote(_WAKE_WORD_GITHUB_REF)}"
    )


def _wake_word_entry(
    *,
    source_key: str,
    source_label: str,
    slug: str,
    url: str,
    label: str = "",
    path: str = "",
) -> Optional[Dict[str, str]]:
    slug_token = _text(slug).strip()
    url_token = _text(url).strip()
    if not slug_token or not url_token:
        return None
    return {
        "id": f"{_text(source_key)}:{slug_token}",
        "slug": slug_token,
        "label": _text(label).strip() or _wake_word_label_from_slug(slug_token),
        "url": url_token,
        "path": _text(path).strip(),
        "source_key": _text(source_key).strip(),
        "source_label": _text(source_label).strip() or _text(source_key).strip(),
    }


def _wake_word_entries_from_manifest(payload: Any) -> List[Dict[str, str]]:
    rows: List[Any] = []
    if isinstance(payload, list):
        rows = list(payload)
    elif isinstance(payload, dict):
        for key in ("entries", "wake_words", "words", "models", "items"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                rows = list(candidate)
                break
        if not rows:
            for source_key, candidate in payload.items():
                if isinstance(candidate, list):
                    for item in candidate:
                        if isinstance(item, dict):
                            enriched = dict(item)
                            enriched.setdefault("source", source_key)
                            rows.append(enriched)

    entries: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_key = _text(row.get("source") or row.get("folder") or row.get("group"))
        source_spec = next((spec for spec in _WAKE_WORD_SOURCE_SPECS if _lower(spec.get("key")) == _lower(source_key)), None)
        source_key = _text((source_spec or {}).get("key")) or source_key or "custom"
        source_label = _text((source_spec or {}).get("label")) or source_key or "Custom"
        url = (
            _text(row.get("url"))
            or _text(row.get("json_url"))
            or _text(row.get("download_url"))
            or _text(row.get("model_url"))
            or _text(row.get("wake_word_model_url"))
        )
        path = _text(row.get("path"))
        if not url and path:
            url = _wake_word_raw_url(path)
        slug = _text(row.get("slug") or row.get("name") or row.get("key")) or _wake_word_slug_from_url(url)
        entry = _wake_word_entry(
            source_key=source_key,
            source_label=source_label,
            slug=slug,
            url=url,
            label=_text(row.get("label") or row.get("title")),
            path=path,
        )
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def _wake_word_entries_from_source_folder(source_spec: Dict[str, str], *, force_refresh: bool = False) -> List[Dict[str, str]]:
    path = _text(source_spec.get("key"))
    if not path:
        return []
    try:
        payload = _remote_json(_wake_word_contents_api_url(path), force_refresh=force_refresh)
    except RuntimeError as exc:
        if "HTTP 404" in _text(exc):
            return []
        raise

    rows = payload if isinstance(payload, list) else payload.get("entries")
    if not isinstance(rows, list):
        return []

    entries: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _lower(row.get("type")) != "file":
            continue
        name = _text(row.get("name")).strip()
        if not name.lower().endswith(".json"):
            continue
        slug = name[:-5]
        entry = _wake_word_entry(
            source_key=path,
            source_label=_text(source_spec.get("label")) or path,
            slug=slug,
            url=_wake_word_raw_url(f"{path}/{name}"),
            path=f"{path}/{name}",
        )
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def _sorted_wake_word_entries(entries: List[Dict[str, str]]) -> List[Dict[str, str]]:
    source_order = {_text(spec.get("key")): index for index, spec in enumerate(_WAKE_WORD_SOURCE_SPECS)}
    unique: Dict[str, Dict[str, str]] = {}
    for row in entries:
        if not isinstance(row, dict):
            continue
        url = _text(row.get("url"))
        if not url:
            continue
        unique[url] = dict(row)
    return sorted(
        unique.values(),
        key=lambda row: (
            source_order.get(_text(row.get("source_key")), 999),
            _lower(row.get("label")),
            _lower(row.get("slug")),
        ),
    )


def _load_wake_word_catalog(*, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.time()
    if not force_refresh:
        with _WAKE_WORD_CATALOG_LOCK:
            cached_ts = float(_WAKE_WORD_CATALOG_CACHE.get("ts") or 0.0)
            cached_payload = _WAKE_WORD_CATALOG_CACHE.get("payload")
            if isinstance(cached_payload, dict) and (now - cached_ts) < _WAKE_WORD_CATALOG_CACHE_TTL_SECONDS:
                return copy.deepcopy(cached_payload)

    warnings: List[str] = []

    for manifest_url in _WAKE_WORD_MANIFEST_URLS:
        try:
            manifest_payload = _remote_json(manifest_url, force_refresh=force_refresh)
            entries = _sorted_wake_word_entries(_wake_word_entries_from_manifest(manifest_payload))
            if entries:
                payload = {
                    "entries": entries,
                    "source_kind": "manifest",
                    "source_label": manifest_url,
                    "warning": "",
                }
                with _WAKE_WORD_CATALOG_LOCK:
                    _WAKE_WORD_CATALOG_CACHE["ts"] = now
                    _WAKE_WORD_CATALOG_CACHE["payload"] = copy.deepcopy(payload)
                return payload
        except RuntimeError as exc:
            if "HTTP 404" not in _text(exc):
                warnings.append(_text(exc))

    entries: List[Dict[str, str]] = []
    for source_spec in _WAKE_WORD_SOURCE_SPECS:
        try:
            entries.extend(_wake_word_entries_from_source_folder(source_spec, force_refresh=force_refresh))
        except RuntimeError as exc:
            warnings.append(_text(exc))

    payload = {
        "entries": _sorted_wake_word_entries(entries),
        "source_kind": "repo_contents",
        "source_label": _text(_WAKE_WORD_GITHUB_REPO),
        "warning": _text(warnings[0] if warnings else ""),
    }
    with _WAKE_WORD_CATALOG_LOCK:
        _WAKE_WORD_CATALOG_CACHE["ts"] = now
        _WAKE_WORD_CATALOG_CACHE["payload"] = copy.deepcopy(payload)
    return payload


def _wake_word_picker_options(catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries = catalog.get("entries") if isinstance(catalog.get("entries"), list) else []
    rows: List[Dict[str, str]] = []
    for row in entries:
        if not isinstance(row, dict):
            continue
        source_key = _text(row.get("source_key"))
        url = _text(row.get("url"))
        if not url:
            continue
        rows.append(
            {
                "value": url,
                "label": _wake_word_option_label(row.get("label"), row.get("slug"), source_key),
            }
        )
    rows.sort(key=lambda option: (_lower(option.get("label")), _text(option.get("value"))))
    return [{"value": "__custom__", "label": "Custom URL"}, *rows]


def _extract_substitution_sections(raw_text: str) -> Dict[str, str]:
    section_map: Dict[str, str] = {}
    in_substitutions = False
    current_section = "Firmware"

    for line in raw_text.splitlines():
        if not in_substitutions:
            if re.match(r"^\s*substitutions:\s*$", line):
                in_substitutions = True
            continue

        if line and not line.startswith((" ", "\t")):
            break

        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            comment = stripped[1:].strip()
            if not comment or set(comment) <= {"-"}:
                continue
            if len(comment) <= 40 and re.search(r"[A-Za-z]", comment):
                current_section = comment.title() if comment.isupper() else comment
            continue

        match = re.match(r"^([A-Za-z0-9_]+)\s*:", stripped)
        if match:
            section_map[match.group(1)] = current_section

    return section_map


def _resolve_template_source(spec: Dict[str, Any], *, force_remote_refresh: bool = False) -> Optional[Dict[str, Any]]:
    last_error = ""
    for url in list(spec.get("source_urls") or []):
        try:
            return {
                "repo_root": None,
                "template_path": None,
                "raw_text": _remote_template_text(_text(url), force_refresh=force_remote_refresh),
                "source_kind": "remote",
                "source_label": _text(url),
            }
        except Exception as exc:
            last_error = _text(exc) or f"Failed to load template from {_text(url)}."

    fallback_path = _bundled_template_root() / _text(spec.get("fallback_path"))
    if fallback_path.is_file():
        return {
            "repo_root": None,
            "template_path": fallback_path,
            "source_kind": "bundled",
            "source_label": str(fallback_path),
            "source_warning": last_error,
        }
    if last_error:
        raise RuntimeError(last_error)
    return None


def _template_spec_by_key(template_key: str) -> Optional[Dict[str, Any]]:
    token = _lower(template_key)
    for spec in _TEMPLATE_SPECS:
        if _lower(spec.get("key")) == token:
            return dict(spec)
    return None


def _load_template_context(spec: Dict[str, Any], *, force_remote_refresh: bool = False) -> Dict[str, Any]:
    resolved = _resolve_template_source(spec, force_remote_refresh=force_remote_refresh)
    if not isinstance(resolved, dict):
        raise RuntimeError(f"Firmware template for {spec.get('label') or spec.get('key')} is unavailable.")

    template_path = Path(resolved["template_path"]) if resolved.get("template_path") else None
    raw_text = _text(resolved.get("raw_text"))
    if not raw_text:
        if not isinstance(template_path, Path):
            raise RuntimeError(f"Firmware template for {spec.get('label') or spec.get('key')} is unavailable.")
        raw_text = template_path.read_text(encoding="utf-8")
    parsed = yaml.load(raw_text, Loader=_FirmwareYamlLoader)
    if not isinstance(parsed, dict):
        template_name = template_path.name if isinstance(template_path, Path) else _text(resolved.get("source_label")) or "template"
        raise RuntimeError(f"Firmware template {template_name} did not parse into a YAML mapping.")

    substitutions = parsed.get("substitutions") if isinstance(parsed.get("substitutions"), dict) else {}
    sections = _extract_substitution_sections(raw_text)
    return {
        "spec": dict(spec),
        "repo_root": Path(resolved["repo_root"]) if resolved.get("repo_root") else None,
        "template_path": template_path,
        "template_doc": parsed,
        "substitutions": dict(substitutions),
        "sections": sections,
        "source_kind": _text(resolved.get("source_kind")),
        "source_label": _text(resolved.get("source_label")),
        "source_warning": _text(resolved.get("source_warning")),
    }


def _profile_storage_key(template_key: str) -> str:
    token = _lower(template_key)
    return f"template:{token}" if token else ""


def _profile_load(template_key: str, selector: str = "") -> Dict[str, str]:
    tokens = [_profile_storage_key(template_key)]
    legacy_selector = _text(selector)
    if legacy_selector:
        tokens.append(legacy_selector)

    for token in [item for item in tokens if _text(item)]:
        with contextlib.suppress(Exception):
            raw = redis_client.hget(FIRMWARE_PROFILE_HASH_KEY, token)
            if raw:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return {str(key): _text(value) for key, value in parsed.items() if _text(key)}
    return {}


def _profile_save(template_key: str, values: Dict[str, Any]) -> None:
    token = _profile_storage_key(template_key)
    if not token:
        return
    clean = {str(key): _text(value) for key, value in (values or {}).items() if _text(key)}
    redis_client.hset(FIRMWARE_PROFILE_HASH_KEY, token, json.dumps(clean, ensure_ascii=False))


def _match_template_spec(selector: str, client_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
    haystack = " ".join(
        part
        for part in [
            selector,
            client_row.get("selector"),
            client_row.get("host"),
            client_row.get("source"),
            device_info.get("name"),
            device_info.get("friendly_name"),
            device_info.get("manufacturer"),
            device_info.get("model"),
            device_info.get("project_name"),
        ]
        if _text(part)
    ).lower()

    for spec in _TEMPLATE_SPECS:
        tokens = {_lower(token) for token in set(spec.get("match_tokens") or set()) if _text(token)}
        if any(token and token in haystack for token in tokens):
            return dict(spec)
    return None


def _checkbox_like_key(key: str, raw_value: Any) -> bool:
    token = _lower(key)
    if token in {"hidden_ssid"}:
        return True
    raw_text = _lower(_template_default_string(raw_value))
    return raw_text in {"true", "false"}


def _build_device_context(
    selector: str,
    client_row: Dict[str, Any],
    template_spec: Dict[str, Any],
    *,
    force_remote_refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    if not isinstance(client_row, dict) or not bool(client_row.get("connected")):
        return None

    if not isinstance(template_spec, dict):
        return None

    template_ctx = _load_template_context(template_spec, force_remote_refresh=force_remote_refresh)
    substitutions = template_ctx["substitutions"]
    field_order = [key for key in substitutions.keys() if _text(key)]
    if not field_order:
        return None

    selector_token = _text(selector)
    host = _text(client_row.get("host")) or esphome_runtime.satellite_host_from_selector(selector_token)
    device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
    template_key = _text(template_spec.get("key"))
    profile = _profile_load(template_key, selector_token)
    fixed_keys = set(template_spec.get("fixed_keys") or set())
    auto_keys = set(template_spec.get("auto_keys") or set())
    wake_word_catalog = _load_wake_word_catalog()

    display_name = (
        _text(device_info.get("friendly_name"))
        or _text(device_info.get("name"))
        or _text(client_row.get("selector"))
        or selector_token
    )

    sections_ui: List[Dict[str, Any]] = []
    fields_meta: Dict[str, Dict[str, Any]] = {}
    section_lookup: Dict[str, List[Dict[str, Any]]] = {}

    for key in field_order:
        raw_value = substitutions.get(key)
        template_default = _template_default_string(raw_value)
        secret_hint = _secret_name(raw_value)
        saved_value = _text(profile.get(key))
        section_title = _text(template_ctx["sections"].get(key)) or "Firmware"
        if key in {"wake_word_name", "wake_word_model_url"}:
            section_title = "Micro Wake Word"
        if section_title not in section_lookup:
            fields: List[Dict[str, Any]] = []
            section_lookup[section_title] = fields
            sections_ui.append({"title": section_title, "fields": fields})
        fields = section_lookup[section_title]

        resolved_value = saved_value or template_default
        if key == "friendly_name":
            resolved_value = saved_value or _text(device_info.get("friendly_name")) or display_name or template_default
        if key in auto_keys and host:
            resolved_value = host
        if key in fixed_keys:
            resolved_value = template_default or resolved_value

        field_type = "checkbox" if _checkbox_like_key(key, raw_value) else "text"
        field_value: Any = resolved_value
        description_parts: List[str] = []
        placeholder = ""
        read_only = key in fixed_keys or key in auto_keys

        if field_type == "checkbox":
            field_value = _as_bool(resolved_value, _as_bool(template_default, False))
        elif key == "wifi_password":
            field_type = "password"
            field_value = ""
            placeholder = "Leave blank to keep saved Wi-Fi password" if saved_value else "Enter Wi-Fi password"
            if saved_value:
                description_parts.append("Leave blank to keep the saved Wi-Fi password in Tater.")
            else:
                description_parts.append("Required before build or flash.")
        elif key == "wifi_ssid" and secret_hint:
            placeholder = secret_hint
            if not saved_value:
                description_parts.append("Required before build or flash.")
        elif key == "wake_word_name":
            placeholder = placeholder or "hey_tater"
            description_parts.append("Auto-filled when you choose a prebuilt wake word, but you can still edit it.")
        elif key == "wake_word_model_url":
            description_parts.append("Pick a prebuilt wake word above or paste any custom JSON model URL.")

        if key in fixed_keys:
            description_parts.append("Locked to the firmware template for this device family.")
        elif key in auto_keys:
            description_parts.append("Auto-filled from the currently connected satellite IP.")
        elif secret_hint and key not in {"wifi_password", "wifi_ssid"}:
            placeholder = placeholder or secret_hint

        field_row = {
            "key": key,
            "label": _humanize_key(key),
            "type": field_type,
            "value": field_value,
            "read_only": read_only,
        }
        if placeholder and not read_only:
            field_row["placeholder"] = placeholder
        if description_parts:
            field_row["description"] = " ".join(part for part in description_parts if part)
        fields.append(field_row)

        fields_meta[key] = {
            "type": field_type,
            "template_default": template_default,
            "secret_hint": secret_hint,
            "read_only": read_only,
            "resolved_value": resolved_value,
            "required": key in {"wifi_ssid", "wifi_password"},
        }

    wake_word_section = section_lookup.get("Micro Wake Word") if isinstance(section_lookup.get("Micro Wake Word"), list) else None
    if isinstance(wake_word_section, list) and "wake_word_model_url" in fields_meta:
        wake_word_entries = wake_word_catalog.get("entries") if isinstance(wake_word_catalog.get("entries"), list) else []
        current_wake_word_url = _text(
            (
                fields_meta.get("wake_word_model_url", {}).get("resolved_value")
                if isinstance(fields_meta.get("wake_word_model_url"), dict)
                else ""
            )
        )
        available_urls = {_text(row.get("url")) for row in wake_word_entries if isinstance(row, dict)}
        picker_value = current_wake_word_url if current_wake_word_url in available_urls else "__custom__"
        catalog_description = (
            f"Choose from {len(wake_word_entries)} prebuilt wake words, "
            "or leave this on Custom URL and paste your own model URL below. If you need a new wake word, request it from the "
            "microWakeWords repo link below and this list will update after it is added."
            if wake_word_entries
            else "Prebuilt wake-word catalog is unavailable right now. You can still paste any custom model URL below. "
            "If you need a new wake word, request it from the microWakeWords repo link below and this list will update after it is added."
        )
        catalog_warning = _text(wake_word_catalog.get("warning"))
        if catalog_warning and not wake_word_entries:
            catalog_description = f"{catalog_description} {_text(catalog_warning)}".strip()
        wake_word_section.insert(
            0,
            {
                "key": "wake_word_catalog",
                "label": "Prebuilt Wake Word",
                "type": "select",
                "value": picker_value,
                "options": _wake_word_picker_options(wake_word_catalog),
                "description": catalog_description,
            },
        )

    cli_status = esphome_cli_status()
    links = [
        {"label": "Template YAML", "href": _text((template_spec.get("source_urls") or [""])[0])},
        {"label": "Wake Word Requests", "href": f"https://github.com/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}"},
    ]

    model = _text(device_info.get("model"))
    project_name = _text(device_info.get("project_name"))
    detail_parts = [part for part in [host, model or project_name] if part]

    item = {
        "id": selector_token,
        "selector": selector_token,
        "template_key": _text(template_spec.get("key")),
        "title": display_name,
        "subtitle": " • ".join(part for part in [host, _text(template_spec.get("label"))] if part),
        "detail": " • ".join(detail_parts),
        "template_label": _text(template_spec.get("label")),
        "template_url": _text((template_spec.get("source_urls") or [""])[0]),
        "connected": True,
        "sections": sections_ui,
        "links": [row for row in links if _text(row.get("href"))],
        "cli_available": bool(cli_status.get("available")),
        "cli_reason": _text(cli_status.get("detail")),
        "host": host,
    }

    return {
        "selector": selector_token,
        "host": host,
        "display_name": display_name,
        "template_key": template_key,
        "template_label": _text(template_spec.get("label")),
        "template_spec": template_spec,
        "template_ctx": template_ctx,
        "profile": profile,
        "field_order": field_order,
        "fields_meta": fields_meta,
        "item": item,
    }


def _summarize_process_output(stdout: str, stderr: str, *, max_lines: int = 16) -> str:
    joined = "\n".join(part for part in [_text(stdout), _text(stderr)] if _text(part))
    if not joined:
        return ""
    lines = [line.rstrip() for line in joined.splitlines() if line.strip()]
    return "\n".join(lines[-max_lines:])


def _remote_template_text(url: str, *, force_refresh: bool = False) -> str:
    target = _text(url)
    if not target:
        raise RuntimeError("Firmware template URL is missing.")

    now = time.time()
    if not force_refresh:
        with _REMOTE_TEMPLATE_LOCK:
            cached = _REMOTE_TEMPLATE_CACHE.get(target)
            cached_ts = float(cached.get("ts") or 0.0) if isinstance(cached, dict) else 0.0
            if isinstance(cached, dict) and (now - cached_ts) < _REMOTE_TEMPLATE_CACHE_TTL_SECONDS:
                text_value = _text(cached.get("text"))
                if text_value:
                    return text_value
                error_value = _text(cached.get("error"))
                if error_value:
                    raise RuntimeError(error_value)

    req = urllib_request.Request(
        target,
        headers={
            "User-Agent": "Tater/1.0",
            "Accept": "text/plain, text/yaml, application/yaml, */*",
        },
    )
    try:
        with urllib_request.urlopen(req, timeout=_REMOTE_TEMPLATE_FETCH_TIMEOUT_SECONDS) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            text_value = response.read().decode(charset, errors="replace")
    except Exception as exc:
        message = f"Failed to fetch firmware template from {target}: {_text(exc) or exc.__class__.__name__}."
        with _REMOTE_TEMPLATE_LOCK:
            _REMOTE_TEMPLATE_CACHE[target] = {"ts": now, "error": message}
        raise RuntimeError(message) from exc

    with _REMOTE_TEMPLATE_LOCK:
        _REMOTE_TEMPLATE_CACHE[target] = {"ts": now, "text": text_value}
    return text_value


def _probe_cli_executable(path_token: str) -> Dict[str, Any]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.update(_runner_env_overrides())
    proc = subprocess.run(
        [path_token, "version"],
        cwd=str(FIRMWARE_RUNNER_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if proc.returncode == 0:
        return {
            "available": True,
            "label": path_token,
            "detail": "Using ESPHome from PATH.",
            "argv": [path_token],
            "cwd": str(FIRMWARE_RUNNER_ROOT),
            "env": _runner_env_overrides(),
        }
    return {
        "available": False,
        "label": path_token,
        "detail": _summarize_process_output(proc.stdout, proc.stderr) or f"`{path_token} version` failed.",
    }


def _probe_source_checkout() -> Dict[str, Any]:
    source_root = _repo_siblings_root() / "esphome"
    if not source_root.is_dir():
        return {"available": False, "label": "Source checkout", "detail": "No ESPHome source checkout was found."}

    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.update(_runner_env_overrides())
    existing_pythonpath = _text(env.get("PYTHONPATH"))
    env["PYTHONPATH"] = (
        f"{source_root}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(source_root)
    )
    argv = [sys.executable, "-m", "esphome"]
    proc = subprocess.run(
        [*argv, "version"],
        cwd=str(FIRMWARE_RUNNER_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if proc.returncode == 0:
        return {
            "available": True,
            "label": f"{Path(sys.executable).name} -m esphome",
            "detail": f"Using ESPHome source checkout in {source_root} with isolated runner workspace.",
            "argv": argv,
            "cwd": str(FIRMWARE_RUNNER_ROOT),
            "env": {
                **_runner_env_overrides(),
                "PYTHONPATH": env["PYTHONPATH"],
            },
        }
    return {
        "available": False,
        "label": "Source checkout",
        "detail": _summarize_process_output(proc.stdout, proc.stderr)
        or f"ESPHome source checkout in {source_root} is not runnable in the current Python environment.",
    }


def esphome_cli_status(*, force: bool = False) -> Dict[str, Any]:
    now = time.time()
    with _CLI_STATUS_LOCK:
        cached = _CLI_STATUS_CACHE.get("status")
        cached_ts = float(_CLI_STATUS_CACHE.get("ts") or 0.0)
        if not force and isinstance(cached, dict) and (now - cached_ts) < _CLI_STATUS_CACHE_TTL_SECONDS:
            return dict(cached)

    status = {"available": False, "label": "Unavailable", "detail": "ESPHome CLI is not available."}
    path_cli = shutil.which("esphome")
    if path_cli:
        status = _probe_cli_executable(path_cli)
    if not bool(status.get("available")):
        source_status = _probe_source_checkout()
        if bool(source_status.get("available")):
            status = source_status
        elif not path_cli:
            status = source_status
        else:
            status["detail"] = _text(status.get("detail")) or _text(source_status.get("detail")) or status["detail"]

    with _CLI_STATUS_LOCK:
        _CLI_STATUS_CACHE["ts"] = now
        _CLI_STATUS_CACHE["status"] = dict(status)
    return dict(status)


def _connected_device_option(selector: str, client_row: Dict[str, Any]) -> Optional[Dict[str, str]]:
    if not isinstance(client_row, dict) or not bool(client_row.get("connected")):
        return None

    selector_token = _text(selector)
    device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
    host = _text(client_row.get("host")) or esphome_runtime.satellite_host_from_selector(selector_token)
    title = (
        _text(device_info.get("friendly_name"))
        or _text(device_info.get("name"))
        or _text(client_row.get("selector"))
        or selector_token
    )
    model = _text(device_info.get("model")) or _text(device_info.get("project_name"))
    label_parts = [part for part in [title, host] if part]
    label = " • ".join(label_parts) or selector_token
    detail = " • ".join(part for part in [host, model] if part)
    return {
        "value": selector_token,
        "label": label,
        "title": title,
        "host": host,
        "detail": detail,
    }


def firmware_panel_payload(status: Dict[str, Any]) -> Dict[str, Any]:
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    cli_status = esphome_cli_status()

    template_options = [
        {"value": _text(spec.get("key")), "label": _text(spec.get("label")) or _text(spec.get("key"))}
        for spec in _TEMPLATE_SPECS
        if _text(spec.get("key"))
    ]
    devices: List[Dict[str, str]] = []
    variants: Dict[str, Dict[str, Dict[str, Any]]] = {row["value"]: {} for row in template_options if _text(row.get("value"))}
    warnings: List[str] = []
    seen_warnings: set[str] = set()

    for selector, client_row in sorted(clients.items(), key=lambda item: _lower(item[0])):
        selector_token = _text(selector)
        row = client_row if isinstance(client_row, dict) else {}
        device_option = _connected_device_option(selector_token, row)
        if not isinstance(device_option, dict):
            continue
        devices.append(device_option)

        for spec in _TEMPLATE_SPECS:
            template_key = _text(spec.get("key"))
            if not template_key:
                continue
            try:
                context = _build_device_context(selector_token, row, dict(spec))
            except Exception as exc:
                message = f"{_text(spec.get('label')) or template_key}: {_text(exc) or 'Firmware template is unavailable.'}"
                if message not in seen_warnings:
                    seen_warnings.add(message)
                    warnings.append(message)
                continue
            if isinstance(context, dict):
                variants.setdefault(template_key, {})[selector_token] = context["item"]

    active_selector = _text((devices[0] or {}).get("value")) if devices else ""
    active_template_key = _text((template_options[0] or {}).get("value")) if template_options else ""

    empty_message = "No connected ESPHome devices are available for firmware actions."
    if not devices:
        empty_message = "No connected ESPHome devices are available for firmware actions."
    elif warnings and not any(bool(rows) for rows in variants.values()):
        empty_message = warnings[0]

    payload = {
        "cli": cli_status,
        "devices": devices,
        "templates": template_options,
        "variants": variants,
        "active_selector": active_selector,
        "active_template_key": active_template_key,
        "empty_message": empty_message,
        "wifi_note": (
            "Wi-Fi SSID is stored per device in Tater. "
            "Leave the Wi-Fi password blank to keep the saved password for that device."
        ),
    }
    if warnings:
        payload["warnings"] = warnings[:6]
    return payload


def _normalize_profile_values(context: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, str]:
    incoming = values if isinstance(values, dict) else {}
    existing = context.get("profile") if isinstance(context.get("profile"), dict) else {}
    normalized: Dict[str, str] = dict(existing)

    for key in list(context.get("field_order") or []):
        meta = context.get("fields_meta", {}).get(key) if isinstance(context.get("fields_meta"), dict) else {}
        field_type = _text(meta.get("type"))
        current_value = _text(meta.get("resolved_value"))
        if bool(meta.get("read_only")):
            normalized[key] = current_value
            continue

        raw_value = incoming.get(key)
        if field_type == "checkbox":
            normalized[key] = "true" if _as_bool(raw_value, False) else "false"
            continue

        if key == "wifi_password":
            token = _text(raw_value)
            normalized[key] = token or _text(existing.get(key))
            continue

        normalized[key] = _text(raw_value)

    wake_word_catalog_value = _text(incoming.get("wake_word_catalog"))
    if wake_word_catalog_value and wake_word_catalog_value != "__custom__" and "wake_word_model_url" in normalized:
        normalized["wake_word_model_url"] = wake_word_catalog_value
        if "wake_word_name" in normalized and not _text(normalized.get("wake_word_name")):
            normalized["wake_word_name"] = _wake_word_slug_from_url(wake_word_catalog_value)

    if _text(context.get("host")) and "ha_voice_ip" in normalized:
        normalized["ha_voice_ip"] = _text(context.get("host"))

    return {key: _text(value) for key, value in normalized.items() if _text(key)}


def _validate_profile_values(context: Dict[str, Any], values: Dict[str, str]) -> None:
    required: List[str] = []
    if "wifi_ssid" in values and not _text(values.get("wifi_ssid")):
        required.append("Wi-Fi SSID")
    if "wifi_password" in values and not _text(values.get("wifi_password")):
        required.append("Wi-Fi password")
    if "ha_voice_ip" in values and not _text(values.get("ha_voice_ip")):
        required.append("connected device IP")
    if required:
        raise RuntimeError(f"Missing required firmware values: {', '.join(required)}.")


def _rewrite_local_packages(config: Dict[str, Any], repo_root: Optional[Path]) -> None:
    if not isinstance(repo_root, Path):
        return
    packages = config.get("packages")
    if not isinstance(packages, dict):
        return

    new_packages: Dict[str, Any] = {}
    changed = False
    for package_name, package_value in packages.items():
        if not isinstance(package_value, dict):
            new_packages[_text(package_name)] = package_value
            continue
        files = package_value.get("files")
        if not isinstance(files, list):
            new_packages[_text(package_name)] = package_value
            continue

        changed = True
        for index, entry in enumerate(files, start=1):
            file_path = ""
            file_vars: Dict[str, Any] = {}
            if isinstance(entry, dict):
                file_path = _text(entry.get("path"))
                file_vars = entry.get("vars") if isinstance(entry.get("vars"), dict) else {}
            else:
                file_path = _text(entry)
            if not file_path:
                continue
            absolute_path = repo_root / file_path
            package_row: Dict[str, Any] = {"file": str(absolute_path)}
            if file_vars:
                package_row["vars"] = dict(file_vars)
            new_packages[f"{_text(package_name) or 'package'}.{index}"] = package_row

    if changed and new_packages:
        config["packages"] = new_packages


def _render_config_text(context: Dict[str, Any], values: Dict[str, str]) -> str:
    config = copy.deepcopy(context["template_ctx"]["template_doc"])
    substitutions = config.get("substitutions") if isinstance(config.get("substitutions"), dict) else {}
    for key in list(context.get("field_order") or []):
        substitutions[key] = _text(values.get(key))
    config["substitutions"] = substitutions
    esphome_block = config.get("esphome") if isinstance(config.get("esphome"), dict) else {}
    esphome_block["build_path"] = str(
        FIRMWARE_BUILD_ROOT / _sanitize_token(context.get("selector")) / _sanitize_token(context.get("template_key"))
    )
    config["esphome"] = esphome_block
    _rewrite_local_packages(config, context["template_ctx"].get("repo_root"))
    return yaml.dump(config, Dumper=_FirmwareYamlDumper, sort_keys=False, allow_unicode=True)


def _prepare_config_path(context: Dict[str, Any], values: Dict[str, str]) -> Path:
    _ensure_agent_labs_dirs()
    selector_dir = FIRMWARE_CONFIG_ROOT / _sanitize_token(context.get("selector"))
    selector_dir.mkdir(parents=True, exist_ok=True)
    config_path = selector_dir / f"{_sanitize_token(context.get('template_key'))}.yaml"
    config_path.write_text(_render_config_text(context, values), encoding="utf-8")
    return config_path


def _runner_env(status: Dict[str, Any]) -> Dict[str, str]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.update(_runner_env_overrides())
    runner_env = status.get("env") if isinstance(status.get("env"), dict) else {}
    for key, value in runner_env.items():
        env[str(key)] = _text(value)
    return env


def _run_esphome_command(argv: List[str], *, cwd: str = "", env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd or None,
        env=env or None,
        capture_output=True,
        text=True,
        timeout=FIRMWARE_BUILD_TIMEOUT_SECONDS,
        check=False,
    )


def _entry_time_text(ts_value: Optional[float] = None) -> str:
    stamp = float(ts_value or time.time())
    return time.strftime("%H:%M:%S", time.localtime(stamp))


def _session_entries_after_locked(session: Dict[str, Any], after_seq: int = 0) -> List[Dict[str, Any]]:
    rows = session.get("entries") if isinstance(session.get("entries"), list) else []
    threshold = max(0, int(after_seq or 0))
    return [dict(row) for row in rows if int(row.get("seq") or 0) > threshold]


def _append_session_entry_locked(
    session: Dict[str, Any],
    *,
    level: str = "info",
    message: Any = "",
    ts_value: Optional[float] = None,
    time_text: str = "",
    source: str = "cli",
    display: str = "",
) -> Optional[Dict[str, Any]]:
    text_value = _clean_terminal_text(display) or _clean_terminal_text(message)
    if not text_value:
        return None
    entries = session.get("entries")
    if not isinstance(entries, list):
        entries = []
        session["entries"] = entries
    seq = int(session.get("cursor") or 0) + 1
    row = {
        "seq": seq,
        "time": _text(time_text) or _entry_time_text(ts_value),
        "level": _lower(level) or "info",
        "message": _clean_terminal_text(message) or text_value,
        "display": text_value,
        "source": _text(source) or "cli",
    }
    entries.append(row)
    overflow = len(entries) - _FIRMWARE_SESSION_MAX_ENTRIES
    if overflow > 0:
        del entries[:overflow]
    session["cursor"] = seq
    session["updated_ts"] = time.time()
    return dict(row)


def _append_session_passthrough_locked(session: Dict[str, Any], entry: Dict[str, Any], *, source: str) -> Optional[Dict[str, Any]]:
    return _append_session_entry_locked(
        session,
        level=_text(entry.get("level")) or "info",
        message=_text(entry.get("message") or entry.get("display")),
        time_text=_text(entry.get("time")),
        source=source,
        display=_text(entry.get("display") or entry.get("message")),
    )


def _phase_status_text(phase: str, display_name: str = "") -> str:
    name = _text(display_name) or "device"
    token = _lower(phase)
    if token == "starting":
        return f"Preparing firmware flash for {name}..."
    if token == "building":
        return f"Building firmware for {name}..."
    if token == "uploading":
        return f"Uploading firmware to {name}..."
    if token == "awaiting_device_logs":
        return f"Upload finished. Waiting for {name} to reconnect for live logs..."
    if token == "live_logs":
        return f"Streaming live logs from {name}."
    if token == "failed":
        return f"Firmware flash failed for {name}."
    if token == "cancelled":
        return f"Firmware flash stopped for {name}."
    if token == "completed":
        return f"Firmware flash completed for {name}."
    return f"Firmware session active for {name}."


def _set_session_phase_locked(session: Dict[str, Any], phase: str) -> None:
    token = _text(phase)
    if token:
        session["phase"] = token
    session["status_text"] = _phase_status_text(_text(session.get("phase")), _text(session.get("display_name")))
    session["updated_ts"] = time.time()


def _cli_line_level(line: str) -> str:
    text_value = _clean_terminal_text(line)
    if not text_value:
        return "info"
    upper = text_value.upper()
    if re.search(r"\[[^\]]+\]\[E\]", text_value):
        return "error"
    if re.search(r"\[[^\]]+\]\[W\]", text_value):
        return "warn"
    if re.search(r"\[[^\]]+\]\[[DV]\]", text_value):
        return "debug"
    if any(token in upper for token in ["ERROR", "FAILED", "EXCEPTION", "TRACEBACK"]):
        return "error"
    if "WARN" in upper:
        return "warn"
    if "DEBUG" in upper or "VERBOSE" in upper:
        return "debug"
    return "info"


def _cli_line_phase(current_phase: str, line: str) -> str:
    token = _lower(current_phase)
    text_value = _lower(_clean_terminal_text(line))
    upload_markers = (
        "uploading",
        "espota.py",
        "sending invitation to",
        "ota",
        "writing at",
        "hard resetting via",
    )
    build_markers = (
        "dependency graph",
        "compiling ",
        "linking ",
        "archiving ",
        "building ",
        ".pioenvs",
    )
    if any(marker in text_value for marker in upload_markers):
        return "uploading"
    if token in {"starting", ""} and any(marker in text_value for marker in build_markers):
        return "building"
    return token or "building"


def _final_session_phase(session: Dict[str, Any]) -> str:
    phase = _lower(session.get("phase"))
    if phase in {"failed", "cancelled"}:
        return phase
    if phase == "live_logs":
        return "live_logs" if bool(session.get("active")) else "completed"
    if int(session.get("returncode") or 0) == 0:
        return "completed"
    if bool(session.get("stop_requested")):
        return "cancelled"
    return "failed"


def _session_payload_locked(session: Dict[str, Any], *, after_seq: int = 0) -> Dict[str, Any]:
    phase = _text(session.get("phase"))
    final_phase = _final_session_phase(session)
    active = bool(session.get("active"))
    return {
        "ok": True,
        "session_id": _text(session.get("id")),
        "selector": _text(session.get("selector")),
        "template_key": _text(session.get("template_key")),
        "display_name": _text(session.get("display_name")),
        "host": _text(session.get("host")),
        "phase": phase,
        "status_text": _text(session.get("status_text")) or _phase_status_text(phase, _text(session.get("display_name"))),
        "active": active,
        "completed": not active and final_phase in {"completed", "failed", "cancelled"},
        "cursor": int(session.get("cursor") or 0),
        "entries": _session_entries_after_locked(session, after_seq),
        "error": _text(session.get("error")),
        "message": _text(session.get("message")),
        "command": list(session.get("command") or []),
        "config_path": _text(session.get("config_path")),
    }


def _stop_device_logs_if_needed(session: Dict[str, Any]) -> None:
    if not bool(session.get("device_logs_started")):
        return
    selector = _text(session.get("selector"))
    if not selector:
        return
    with contextlib.suppress(Exception):
        esphome_runtime.logs_stop(selector, force=False, timeout=20.0)


def _prune_firmware_sessions() -> None:
    now = time.time()
    stale_sessions: List[Dict[str, Any]] = []
    with _FIRMWARE_SESSION_LOCK:
        for session_id, session in list(_FIRMWARE_SESSIONS.items()):
            if not isinstance(session, dict):
                _FIRMWARE_SESSIONS.pop(session_id, None)
                continue
            proc = session.get("proc")
            running = isinstance(proc, subprocess.Popen) and proc.poll() is None
            updated_ts = float(session.get("updated_ts") or session.get("created_ts") or 0.0)
            if running:
                continue
            if updated_ts <= 0 or (now - updated_ts) < _FIRMWARE_SESSION_TTL_SECONDS:
                continue
            stale_sessions.append(session)
            _FIRMWARE_SESSIONS.pop(session_id, None)
    for session in stale_sessions:
        _stop_device_logs_if_needed(session)


def _active_flash_for_selector(selector: str) -> Optional[Dict[str, Any]]:
    token = _text(selector)
    if not token:
        return None
    with _FIRMWARE_SESSION_LOCK:
        for session in _FIRMWARE_SESSIONS.values():
            if not isinstance(session, dict):
                continue
            if _text(session.get("selector")) != token:
                continue
            if bool(session.get("active")):
                return dict(session)
    return None


def _firmware_session_worker(session_id: str) -> None:
    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(session, dict):
            return
        command = list(session.get("command") or [])
        cwd = _text(session.get("cwd"))
        env = session.get("env") if isinstance(session.get("env"), dict) else None
        _set_session_phase_locked(session, "building")

    proc: Optional[subprocess.Popen[str]] = None
    try:
        proc = subprocess.Popen(
            command,
            cwd=cwd or None,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except Exception as exc:
        with _FIRMWARE_SESSION_LOCK:
            session = _FIRMWARE_SESSIONS.get(session_id)
            if isinstance(session, dict):
                session["active"] = False
                session["error"] = _text(exc) or exc.__class__.__name__
                _set_session_phase_locked(session, "failed")
                _append_session_entry_locked(
                    session,
                    level="error",
                    message=f"Failed to start ESPHome CLI: {_text(exc) or exc.__class__.__name__}.",
                    source="cli",
                )
        return

    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(session, dict):
            with contextlib.suppress(Exception):
                proc.terminate()
            return
        session["proc"] = proc
        session["pid"] = int(proc.pid or 0)
        _append_session_entry_locked(
            session,
            level="debug",
            message=f"ESPHome process started (pid {int(proc.pid or 0)}).",
            source="cli",
        )

    try:
        if proc.stdout is not None:
            for raw_line in proc.stdout:
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
                with _FIRMWARE_SESSION_LOCK:
                    session = _FIRMWARE_SESSIONS.get(session_id)
                    if not isinstance(session, dict):
                        continue
                    next_phase = _cli_line_phase(_text(session.get("phase")), line)
                    if next_phase != _text(session.get("phase")):
                        _set_session_phase_locked(session, next_phase)
                    _append_session_entry_locked(
                        session,
                        level=_cli_line_level(line),
                        message=line,
                        source="cli",
                    )
    finally:
        with contextlib.suppress(Exception):
            if proc.stdout is not None:
                proc.stdout.close()
        returncode = proc.wait()

    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(session, dict):
            return
        session["proc"] = None
        session["returncode"] = int(returncode)
        stop_requested = bool(session.get("stop_requested"))
        if stop_requested:
            session["active"] = False
            session["message"] = "Firmware flash stopped."
            _set_session_phase_locked(session, "cancelled")
            _append_session_entry_locked(session, level="warn", message="Firmware flash cancelled.", source="cli")
            return
        if returncode != 0:
            session["active"] = False
            session["error"] = f"ESPHome exited with code {int(returncode)}."
            session["message"] = "Firmware flash failed."
            _set_session_phase_locked(session, "failed")
            _append_session_entry_locked(
                session,
                level="error",
                message=f"ESPHome CLI exited with code {int(returncode)}.",
                source="cli",
            )
            return
        session["returncode"] = 0
        session["message"] = "Firmware uploaded successfully. Waiting for live device logs."
        session["device_log_next_retry_ts"] = time.time()
        session["device_log_retry_count"] = 0
        _set_session_phase_locked(session, "awaiting_device_logs")
        _append_session_entry_locked(
            session,
            level="info",
            message="Build and upload finished. Waiting for the device to reconnect so live logs can continue here.",
            source="session",
        )


def _pump_session_device_logs(session_id: str) -> None:
    start_selector = ""
    start_after_seq = 0
    should_start = False
    should_poll = False
    retry_ts = 0.0
    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(session, dict):
            return
        if not bool(session.get("active")):
            return
        phase = _lower(session.get("phase"))
        if phase not in {"awaiting_device_logs", "live_logs"}:
            return
        start_selector = _text(session.get("selector"))
        start_after_seq = int(session.get("device_log_cursor") or 0)
        retry_ts = float(session.get("device_log_next_retry_ts") or 0.0)
        should_poll = bool(session.get("device_logs_started"))
        should_start = not should_poll and time.time() >= retry_ts

    if not start_selector:
        return

    if should_start:
        try:
            result = esphome_runtime.logs_start(start_selector, timeout=20.0)
        except Exception as exc:
            with _FIRMWARE_SESSION_LOCK:
                session = _FIRMWARE_SESSIONS.get(session_id)
                if isinstance(session, dict):
                    attempts = int(session.get("device_log_retry_count") or 0) + 1
                    session["device_log_retry_count"] = attempts
                    session["device_log_next_retry_ts"] = time.time() + _FIRMWARE_DEVICE_LOG_RETRY_SECONDS
                    session["device_log_error"] = _text(exc) or exc.__class__.__name__
                    session["status_text"] = _phase_status_text("awaiting_device_logs", _text(session.get("display_name")))
        else:
            with _FIRMWARE_SESSION_LOCK:
                session = _FIRMWARE_SESSIONS.get(session_id)
                if isinstance(session, dict):
                    session["device_logs_started"] = True
                    session["device_log_cursor"] = int(result.get("cursor") or 0)
                    session["device_log_error"] = ""
                    session["message"] = "Firmware uploaded successfully. Streaming live device logs."
                    _set_session_phase_locked(session, "live_logs")
                    _append_session_entry_locked(
                        session,
                        level="info",
                        message="Connected to device logs. Streaming live output below.",
                        source="session",
                    )
                    for entry in list(result.get("entries") or []):
                        if isinstance(entry, dict):
                            _append_session_passthrough_locked(session, entry, source="device")
            return

    if not should_poll:
        return

    try:
        result = esphome_runtime.logs_poll(start_selector, after_seq=start_after_seq, timeout=5.0)
    except Exception as exc:
        with _FIRMWARE_SESSION_LOCK:
            session = _FIRMWARE_SESSIONS.get(session_id)
            if isinstance(session, dict):
                session["device_logs_started"] = False
                session["device_log_next_retry_ts"] = time.time() + _FIRMWARE_DEVICE_LOG_RETRY_SECONDS
                session["device_log_error"] = _text(exc) or exc.__class__.__name__
                _set_session_phase_locked(session, "awaiting_device_logs")
        return

    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(session, dict):
            return
        session["device_log_cursor"] = int(result.get("cursor") or session.get("device_log_cursor") or 0)
        error_text = _text(result.get("error"))
        if error_text:
            session["device_log_error"] = error_text
        if not bool(result.get("active")):
            session["device_logs_started"] = False
            session["device_log_next_retry_ts"] = time.time() + _FIRMWARE_DEVICE_LOG_RETRY_SECONDS
            _set_session_phase_locked(session, "awaiting_device_logs")
            return
        _set_session_phase_locked(session, "live_logs")
        session["device_log_error"] = ""
        for entry in list(result.get("entries") or []):
            if isinstance(entry, dict):
                _append_session_passthrough_locked(session, entry, source="device")


def _start_flash_session(context: Dict[str, Any], profile_values: Dict[str, str], cli_status: Dict[str, Any]) -> Dict[str, Any]:
    _prune_firmware_sessions()
    selector = _text(context.get("selector"))
    active_session = _active_flash_for_selector(selector)
    if isinstance(active_session, dict):
        raise RuntimeError(
            f"A firmware flash session is already active for {_text(context.get('display_name')) or selector}."
        )

    config_path = _prepare_config_path(context, profile_values)
    host = _text(context.get("host"))
    argv = list(cli_status.get("argv") or [])
    if not argv:
        raise RuntimeError("ESPHome CLI runner is not configured.")
    command = [*argv, "run", str(config_path), "--no-logs", "--device", host or "OTA"]
    session_id = f"fw_{uuid.uuid4().hex}"
    session = {
        "id": session_id,
        "selector": selector,
        "template_key": _text(context.get("template_key")),
        "display_name": _text(context.get("display_name")) or selector,
        "host": host,
        "config_path": str(config_path),
        "command": command,
        "cwd": _text(cli_status.get("cwd")),
        "env": _runner_env(cli_status),
        "created_ts": time.time(),
        "updated_ts": time.time(),
        "cursor": 0,
        "entries": [],
        "phase": "starting",
        "status_text": _phase_status_text("starting", _text(context.get("display_name")) or selector),
        "active": True,
        "error": "",
        "message": f"Streaming build, upload, and live device logs for {_text(context.get('display_name')) or selector}.",
        "returncode": None,
        "proc": None,
        "stop_requested": False,
        "device_logs_started": False,
        "device_log_cursor": 0,
        "device_log_next_retry_ts": 0.0,
        "device_log_retry_count": 0,
        "device_log_error": "",
    }
    with _FIRMWARE_SESSION_LOCK:
        _FIRMWARE_SESSIONS[session_id] = session
        _append_session_entry_locked(
            session,
            level="info",
            message=(
                f"Preparing {_text(context.get('template_label')) or 'firmware'} for "
                f"{_text(context.get('display_name')) or selector}."
            ),
            source="session",
        )
        _append_session_entry_locked(
            session,
            level="debug",
            message=f"Config written to {str(config_path)}",
            source="session",
        )
        _append_session_entry_locked(
            session,
            level="debug",
            message="Command: " + " ".join(command),
            source="session",
        )

    worker = threading.Thread(target=_firmware_session_worker, args=(session_id,), daemon=True)
    with _FIRMWARE_SESSION_LOCK:
        live_session = _FIRMWARE_SESSIONS.get(session_id)
        if isinstance(live_session, dict):
            live_session["worker"] = worker
    worker.start()

    with _FIRMWARE_SESSION_LOCK:
        live_session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(live_session, dict):
            raise RuntimeError("Firmware session was not created.")
        return _session_payload_locked(live_session, after_seq=0)


def _poll_flash_session(session_id: str, *, after_seq: int = 0) -> Dict[str, Any]:
    _prune_firmware_sessions()
    _pump_session_device_logs(session_id)
    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(_text(session_id))
        if not isinstance(session, dict):
            raise RuntimeError("Firmware log session is no longer available.")
        return _session_payload_locked(session, after_seq=after_seq)


def _stop_flash_session(session_id: str) -> Dict[str, Any]:
    _prune_firmware_sessions()
    session_token = _text(session_id)
    proc: Optional[subprocess.Popen[str]] = None
    selector = ""
    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_token)
        if not isinstance(session, dict):
            return {"ok": True, "session_id": session_token, "stopped": True}
        session["stop_requested"] = True
        session["active"] = False
        selector = _text(session.get("selector"))
        proc = session.get("proc") if isinstance(session.get("proc"), subprocess.Popen) else None
        if proc is None:
            _set_session_phase_locked(session, _final_session_phase(session))
            _append_session_entry_locked(
                session,
                level="info",
                message="Firmware log viewer closed.",
                source="session",
            )

    if isinstance(proc, subprocess.Popen) and proc.poll() is None:
        with contextlib.suppress(Exception):
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            with contextlib.suppress(Exception):
                proc.kill()

    if selector:
        with contextlib.suppress(Exception):
            esphome_runtime.logs_stop(selector, force=False, timeout=20.0)

    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_token)
        if not isinstance(session, dict):
            return {"ok": True, "session_id": session_token, "stopped": True}
        session["device_logs_started"] = False
        if _lower(session.get("phase")) not in {"failed", "cancelled"}:
            _set_session_phase_locked(session, _final_session_phase(session))
        if _lower(session.get("phase")) == "completed":
            session["message"] = "Firmware flash completed."
        elif _lower(session.get("phase")) == "cancelled":
            session["message"] = "Firmware flash stopped."
        return {
            "ok": True,
            "session_id": session_token,
            "selector": selector,
            "stopped": True,
            "phase": _text(session.get("phase")),
            "message": _text(session.get("message")) or "Firmware log viewer closed.",
        }


def handle_runtime_action(action_name: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if action_name == "voice_firmware_flash_poll":
        body = payload if isinstance(payload, dict) else {}
        session_id = _text(body.get("session_id") or body.get("id"))
        after_seq = esphome_runtime.as_int(body.get("after_seq"), 0, minimum=0)
        if not session_id:
            raise ValueError("session_id is required")
        result = _poll_flash_session(session_id, after_seq=after_seq)
        result["action"] = action_name
        return result

    if action_name == "voice_firmware_flash_stop":
        body = payload if isinstance(payload, dict) else {}
        session_id = _text(body.get("session_id") or body.get("id"))
        if not session_id:
            raise ValueError("session_id is required")
        result = _stop_flash_session(session_id)
        result["action"] = action_name
        return result

    if action_name not in {"voice_firmware_save", "voice_firmware_build", "voice_firmware_flash", "voice_firmware_flash_start"}:
        return None

    body = payload if isinstance(payload, dict) else {}
    selector = esphome_runtime.payload_selector(body)
    template_key = _text(body.get("template_key"))
    if not selector:
        raise ValueError("selector is required")
    if not template_key:
        raise ValueError("template_key is required")

    client_row = esphome_runtime.client_row_snapshot_sync(selector)
    if not isinstance(client_row, dict) or not bool(client_row.get("connected")):
        raise RuntimeError(f"ESPHome device {selector} is not currently connected.")

    template_spec = _template_spec_by_key(template_key)
    if not isinstance(template_spec, dict):
        raise RuntimeError(f"Firmware template {template_key} is not supported.")

    force_remote_refresh = action_name in {"voice_firmware_build", "voice_firmware_flash", "voice_firmware_flash_start"}
    context = _build_device_context(
        selector,
        client_row,
        template_spec,
        force_remote_refresh=force_remote_refresh,
    )
    if not isinstance(context, dict):
        raise RuntimeError(f"Connected ESPHome device {selector} is not available for firmware actions.")

    values = esphome_runtime.payload_values(body)
    profile_values = _normalize_profile_values(context, values)

    if action_name == "voice_firmware_save":
        _profile_save(_text(context.get("template_key")), profile_values)
        return {
            "ok": True,
            "action": action_name,
            "selector": selector,
            "template_key": context.get("template_key"),
            "message": f"Saved firmware substitutions for {context.get('display_name') or selector}.",
        }

    _validate_profile_values(context, profile_values)
    _profile_save(_text(context.get("template_key")), profile_values)
    cli_status = esphome_cli_status(force=True)
    if not bool(cli_status.get("available")):
        raise RuntimeError(_text(cli_status.get("detail")) or "ESPHome CLI is unavailable.")

    if action_name == "voice_firmware_flash_start":
        result = _start_flash_session(context, profile_values, cli_status)
        result["action"] = action_name
        return result

    config_path = _prepare_config_path(context, profile_values)
    host = _text(context.get("host"))
    argv = list(cli_status.get("argv") or [])
    if not argv:
        raise RuntimeError("ESPHome CLI runner is not configured.")

    if action_name == "voice_firmware_build":
        command = [*argv, "compile", str(config_path)]
    else:
        command = [*argv, "run", str(config_path), "--no-logs", "--device", host or "OTA"]

    proc = _run_esphome_command(
        command,
        cwd=_text(cli_status.get("cwd")),
        env=_runner_env(cli_status),
    )
    summary = _summarize_process_output(proc.stdout, proc.stderr)
    if proc.returncode != 0:
        verb = "flash" if action_name == "voice_firmware_flash" else "build"
        raise RuntimeError(
            f"ESPHome {verb} failed for {context.get('display_name') or selector}.\n\n{summary or 'No CLI output was captured.'}"
        )

    if action_name == "voice_firmware_flash":
        message = f"Built and flashed {context.get('display_name') or selector}."
    else:
        message = f"Built firmware for {context.get('display_name') or selector}."
    return {
        "ok": True,
        "action": action_name,
        "selector": selector,
        "template_key": context.get("template_key"),
        "config_path": str(config_path),
        "command": command,
        "message": message,
        "output_tail": summary,
    }
